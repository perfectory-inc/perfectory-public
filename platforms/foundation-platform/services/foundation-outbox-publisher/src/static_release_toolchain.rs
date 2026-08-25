use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs::File,
    io::Read as _,
    path::{Path, PathBuf},
    process::Output,
    time::Duration,
};

use anyhow::{bail, ensure, Context as _};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio::{process::Command, time};

const CONTRACT_JSON: &str = include_str!("../../../config/static-release-toolchain.contract.json");

#[derive(Debug, Deserialize)]
struct Contract {
    schema_version: u32,
    tools: BTreeMap<String, Tool>,
    distributions: BTreeMap<String, Distribution>,
}

#[derive(Debug, Deserialize)]
struct Tool {
    version: String,
    version_command: Vec<String>,
    banner_prefix: String,
    banner_suffix: String,
    distribution: String,
}

#[derive(Debug, Deserialize)]
struct Distribution {
    platforms: BTreeMap<String, Artifact>,
}

#[derive(Debug, Deserialize)]
struct Artifact {
    executables: BTreeMap<String, Executable>,
}

#[derive(Debug, Deserialize)]
struct Executable {
    filename: String,
    sha256: String,
}

#[derive(Debug)]
struct VerifiedExecutable {
    path: PathBuf,
    sha256: String,
}

#[derive(Debug)]
pub(crate) struct VerifiedToolchain {
    executables: BTreeMap<String, VerifiedExecutable>,
}

impl VerifiedToolchain {
    pub(crate) async fn run<I, S>(
        &self,
        tool_name: &str,
        arguments: I,
        timeout: Duration,
        cwd: Option<&Path>,
    ) -> anyhow::Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let executable = self.executable(tool_name)?;
        verify_executable_hash(&executable.path, &executable.sha256)?;
        let output = run_bounded(&executable.path, arguments, timeout, cwd).await?;
        verify_executable_hash(&executable.path, &executable.sha256)?;
        Ok(output)
    }

    fn executable(&self, tool_name: &str) -> anyhow::Result<&VerifiedExecutable> {
        self.executables
            .get(tool_name)
            .with_context(|| format!("verified toolchain does not contain {tool_name}"))
    }
}

pub(crate) async fn verify(timeout: Duration) -> anyhow::Result<VerifiedToolchain> {
    ensure!(!timeout.is_zero(), "external-tool timeout must be positive");
    let contract = embedded_contract()?;
    let platform = platform_key(env::consts::OS, env::consts::ARCH)?;
    let mut verified = BTreeMap::new();

    for (tool_name, tool) in &contract.tools {
        let distribution = contract
            .distributions
            .get(&tool.distribution)
            .with_context(|| format!("{tool_name} names an unknown distribution"))?;
        let artifact = distribution
            .platforms
            .get(&platform)
            .with_context(|| format!("toolchain has no artifact for platform {platform}"))?;
        let executable = artifact
            .executables
            .get(tool_name)
            .with_context(|| format!("toolchain artifact does not contain {tool_name}"))?;
        let path = resolve_on_path(&executable.filename)
            .with_context(|| format!("required external tool {tool_name} is unavailable"))?;

        verify_executable_hash(&path, &executable.sha256)?;
        let output = run_bounded(&path, &tool.version_command, timeout, None).await?;
        let actual = output_banner(tool_name, &output)?;
        verify_banner(tool_name, &expected_banner(tool), &actual)?;
        verify_executable_hash(&path, &executable.sha256)?;
        verified.insert(
            tool_name.clone(),
            VerifiedExecutable {
                path,
                sha256: executable.sha256.clone(),
            },
        );
    }
    Ok(VerifiedToolchain {
        executables: verified,
    })
}

fn embedded_contract() -> anyhow::Result<Contract> {
    let contract: Contract = serde_json::from_str(CONTRACT_JSON)
        .context("embedded toolchain contract is invalid JSON")?;
    ensure!(
        contract.schema_version == 1,
        "unsupported toolchain contract schema"
    );
    ensure!(
        contract
            .tools
            .keys()
            .map(String::as_str)
            .eq(["martin-cp", "mbtiles", "pmtiles"]),
        "embedded toolchain contract has an incomplete tool set"
    );
    for (tool_name, tool) in &contract.tools {
        ensure!(
            !tool.version_command.is_empty(),
            "{tool_name} has no version command"
        );
        ensure!(
            contract.distributions.contains_key(&tool.distribution),
            "{tool_name} names an unknown distribution"
        );
    }
    Ok(contract)
}

fn platform_key(os: &str, architecture: &str) -> anyhow::Result<String> {
    ensure!(
        matches!(os, "linux" | "windows") && architecture == "x86_64",
        "unsupported static-release toolchain platform {os}-{architecture}"
    );
    Ok(format!("{os}-{architecture}"))
}

fn expected_banner(tool: &Tool) -> String {
    format!(
        "{}{}{}",
        tool.banner_prefix, tool.version, tool.banner_suffix
    )
}

fn resolve_on_path(filename: &str) -> anyhow::Result<PathBuf> {
    ensure!(
        Path::new(filename).file_name() == Some(OsStr::new(filename)),
        "contracted executable filename must be a basename"
    );
    let path = env::var_os("PATH").context("PATH is unavailable")?;
    let current_dir = env::current_dir().context("current directory is unavailable")?;
    resolve_on_search_path(filename, &path, &current_dir)
}

fn resolve_on_search_path(
    filename: &str,
    search_path: &OsStr,
    current_dir: &Path,
) -> anyhow::Result<PathBuf> {
    for directory in env::split_paths(search_path) {
        let directory = if directory.is_absolute() {
            directory
        } else {
            current_dir.join(directory)
        };
        let candidate = directory.join(filename);
        if candidate.is_file() {
            let canonical = candidate.canonicalize().with_context(|| {
                format!(
                    "could not canonicalize contracted executable {}",
                    candidate.display()
                )
            })?;
            ensure!(
                canonical.is_absolute(),
                "contracted executable did not resolve to an absolute path"
            );
            return Ok(canonical);
        }
    }
    bail!("{filename} was not found on PATH")
}

fn verify_executable_hash(path: &Path, expected: &str) -> anyhow::Result<()> {
    let actual = sha256_file(path)?;
    ensure!(
        actual == expected,
        "{} executable SHA-256 mismatch",
        path.display()
    );
    Ok(())
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut source = File::open(path)
        .with_context(|| format!("could not open contracted executable {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .with_context(|| format!("could not hash contracted executable {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

async fn run_bounded<I, S>(
    path: &Path,
    arguments: I,
    timeout: Duration,
    cwd: Option<&Path>,
) -> anyhow::Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(path);
    command.args(arguments).kill_on_drop(true);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    time::timeout(timeout, command.output())
        .await
        .with_context(|| {
            format!(
                "external tool {} command exceeded {}ms",
                path.display(),
                timeout.as_millis()
            )
        })?
        .with_context(|| format!("could not execute contracted tool {}", path.display()))
}

fn output_banner(tool_name: &str, output: &Output) -> anyhow::Result<String> {
    if !output.status.success() {
        bail!(
            "{tool_name} version command failed with status {}",
            output.status
        );
    }
    let stdout = String::from_utf8(output.stdout.clone())
        .with_context(|| format!("{tool_name} version stdout is not UTF-8"))?;
    let stderr = String::from_utf8(output.stderr.clone())
        .with_context(|| format!("{tool_name} version stderr is not UTF-8"))?;
    let stdout = stdout.trim();
    let stderr = stderr.trim();
    ensure!(
        stdout.is_empty() || stderr.is_empty(),
        "{tool_name} version command wrote to both stdout and stderr"
    );
    Ok(if stdout.is_empty() { stderr } else { stdout }.to_owned())
}

fn verify_banner(tool_name: &str, expected: &str, actual: &str) -> anyhow::Result<()> {
    ensure!(
        actual == expected,
        "{tool_name} version banner does not match the embedded contract"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_file(label: &str, contents: &[u8]) -> anyhow::Result<PathBuf> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = env::temp_dir().join(format!(
            "perfectory-toolchain-{label}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::write(&path, contents)?;
        Ok(path)
    }

    #[test]
    fn embedded_contract_names_the_complete_toolchain() -> anyhow::Result<()> {
        let contract = embedded_contract()?;
        assert_eq!(
            contract
                .tools
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["martin-cp", "mbtiles", "pmtiles"]
        );
        assert!(contract
            .distributions
            .values()
            .all(|distribution| distribution.platforms.contains_key("windows-x86_64")));
        assert!(contract
            .distributions
            .values()
            .all(|distribution| distribution.platforms.contains_key("linux-x86_64")));
        Ok(())
    }

    #[test]
    fn expected_banner_is_composed_from_the_contract_fields() -> anyhow::Result<()> {
        let contract = embedded_contract()?;
        let tool = contract.tools.get("mbtiles").expect("contracted tool");
        assert_eq!(
            expected_banner(tool),
            format!(
                "{}{}{}",
                tool.banner_prefix, tool.version, tool.banner_suffix
            )
        );
        Ok(())
    }

    #[test]
    fn executable_hash_mismatch_fails_closed() -> anyhow::Result<()> {
        let executable = temp_file("hash-mismatch", b"different bytes")?;
        let error = verify_executable_hash(&executable, &"0".repeat(64))
            .expect_err("wrong bytes must fail");
        std::fs::remove_file(executable)?;
        assert!(error.to_string().contains("SHA-256 mismatch"));
        Ok(())
    }

    #[test]
    fn stale_embedded_contract_rejects_a_new_path_executable() -> anyhow::Result<()> {
        let executable = temp_file("stale-contract", b"new release bytes")?;
        let embedded_hash = sha256_hex(b"embedded release bytes");
        let result = verify_executable_hash(&executable, &embedded_hash);
        std::fs::remove_file(executable)?;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn banner_match_is_exact() {
        assert!(verify_banner("tool", "expected", "expected").is_ok());
        assert!(verify_banner("tool", "expected", "expected with suffix").is_err());
    }

    #[test]
    fn unsupported_host_platform_is_rejected() {
        assert!(platform_key("plan9", "mips").is_err());
    }

    #[tokio::test]
    async fn version_execution_is_bounded() -> anyhow::Result<()> {
        let current_executable = env::current_exe()?;
        let arguments = [
            "--ignored".to_owned(),
            "--exact".to_owned(),
            "static_release_toolchain::tests::version_command_stall_helper".to_owned(),
        ];
        let error = run_bounded(
            &current_executable,
            &arguments,
            Duration::from_millis(100),
            None,
        )
        .await
        .expect_err("stalled version command must time out");
        assert!(error.to_string().contains("exceeded"));
        Ok(())
    }

    #[test]
    fn verified_toolchain_keeps_the_verified_path_and_rejects_replacement() -> anyhow::Result<()> {
        let verified_path = temp_file("verified-path", b"verified bytes")?;
        let other_path = temp_file("later-path-candidate", b"other bytes")?;
        let toolchain = VerifiedToolchain {
            executables: BTreeMap::from([(
                "demo".to_owned(),
                VerifiedExecutable {
                    path: verified_path.clone(),
                    sha256: sha256_hex(b"verified bytes"),
                },
            )]),
        };

        assert_eq!(toolchain.executable("demo")?.path, verified_path);
        assert_ne!(toolchain.executable("demo")?.path, other_path);
        std::fs::write(&verified_path, b"replaced after preflight")?;
        assert!(verify_executable_hash(
            &toolchain.executable("demo")?.path,
            &toolchain.executable("demo")?.sha256,
        )
        .is_err());
        std::fs::remove_file(verified_path)?;
        std::fs::remove_file(other_path)?;
        Ok(())
    }

    #[test]
    fn relative_search_path_is_frozen_as_a_canonical_absolute_path() -> anyhow::Result<()> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = env::temp_dir().join(format!(
            "perfectory-toolchain-relative-path-{}-{nonce}",
            std::process::id()
        ));
        let directory = root.join("bin");
        std::fs::create_dir_all(&directory)?;
        let executable = directory.join("demo");
        std::fs::write(&executable, b"demo")?;

        let resolved = resolve_on_search_path("demo", OsStr::new("bin"), &root)?;
        assert!(resolved.is_absolute());
        assert_eq!(resolved, executable.canonicalize()?);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[ignore = "spawned by the bounded version-command test"]
    fn version_command_stall_helper() {
        std::thread::sleep(Duration::from_secs(5));
    }
}
