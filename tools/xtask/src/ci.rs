//! `cargo xtask ci <command>`: CI steps that were `scripts/ci/*.sh` (shell inventory, root ADR-0270).
//!
//! - `install-cargo-deny` puts the cargo-deny release pinned in `tools/cargo-deny.env` on the GitHub
//!   Actions job's PATH; the one pinned fetcher (`scripts/ci/fetch_pinned.py`, root ADR-0197) downloads
//!   and checks the archive before anything is extracted (root ADR-0155).
//! - `osv-gate [--report-path <path>] [repository]` scans every tracked `Cargo.lock` and `pnpm-lock.yaml`
//!   with the pinned OSV-Scanner and holds the findings to `tools/osv-vulnerability-baseline.tsv`
//!   (root ADR-0028). Threat model: honest-mistake detection, not unpublished flaws or a compromised OSV.
//! - `postgres-production-shape <database url> <base commit>` (root ADR-0259 §8): when the change touches
//!   Foundation's migrations or this gate, a self-test (a planted nullable column applies onto seeded
//!   rows, a planted required one fails), then the change's new migrations onto main's schema holding
//!   production's rows, each in its own new database.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str =
    "usage: cargo xtask ci <install-cargo-deny | osv-gate [--report-path <path>] [repository] | \
postgres-production-shape <database url> <base commit>>";

pub(crate) fn run(root: &Path, args: &[String]) -> Result<(), String> {
    match (args.first().map(String::as_str), &args[args.len().min(1)..]) {
        (Some("install-cargo-deny"), []) => install_cargo_deny(root),
        (Some("osv-gate"), rest) => {
            let (report, target) = match rest {
                [flag, report, target @ ..] if flag == "--report-path" => {
                    (Some(PathBuf::from(report)), target)
                }
                [flag] if flag == "--report-path" => return Err(USAGE.to_owned()),
                target => (None, target),
            };
            let target = match target {
                [] => root.to_path_buf(),
                [one] => PathBuf::from(one),
                _ => return Err(USAGE.to_owned()),
            };
            if osv_gate(root, &target, report.as_deref())? {
                Ok(())
            } else {
                Err("the OSV findings are not the baseline's (see above)".to_owned())
            }
        }
        (Some("postgres-production-shape"), [url, base]) => {
            postgres_production_shape(root, url, base)
        }
        (Some("postgres-production-shape"), [url]) => postgres_production_shape(root, url, ""),
        _ => Err(USAGE.to_owned()),
    }
}

fn install_cargo_deny(root: &Path) -> Result<(), String> {
    let github_path = std::env::var("GITHUB_PATH").map_err(|_| {
        "GITHUB_PATH is unset; this command configures a GitHub Actions job".to_owned()
    })?;
    let runner_temp =
        PathBuf::from(std::env::var("RUNNER_TEMP").map_err(|_| "RUNNER_TEMP is unset".to_owned())?);
    let policy = read_policy(&root.join("tools/cargo-deny.env"))?;
    let version = policy
        .get("CARGO_DENY_VERSION")
        .cloned()
        .unwrap_or_default();
    if !is_version(&version) {
        return Err("unreadable version in tools/cargo-deny.env".to_owned());
    }
    let name = format!("cargo-deny-{version}-x86_64-unknown-linux-musl");
    let archive = runner_temp.join(format!("{name}.tar.gz"));
    let bin = runner_temp.join("cargo-deny").join("bin");
    fs::create_dir_all(&bin).map_err(|error| error.to_string())?;
    succeed(
        Command::new("python3")
            .arg(root.join("scripts/ci/fetch_pinned.py"))
            .args(["tool", "cargo-deny", "--output"])
            .arg(&archive),
        "fetch_pinned.py tool cargo-deny",
    )?;
    succeed(
        Command::new("tar")
            .arg("xzf")
            .arg(&archive)
            .arg("--strip-components=1")
            .arg("-C")
            .arg(&bin)
            .arg(format!("{name}/cargo-deny")),
        "tar (cargo-deny)",
    )?;
    let mut path_file = fs::OpenOptions::new()
        .append(true)
        .open(&github_path)
        .map_err(|error| format!("cannot open GITHUB_PATH: {error}"))?;
    std::io::Write::write_all(&mut path_file, format!("{}\n", bin.display()).as_bytes())
        .map_err(|error| error.to_string())?;
    succeed(
        Command::new(bin.join("cargo-deny")).arg("--version"),
        "cargo-deny --version",
    )
}

/// Runs the OSV ratchet over `target`'s tracked lockfiles; whether the findings are the baseline's.
pub(crate) fn osv_gate(root: &Path, target: &Path, report: Option<&Path>) -> Result<bool, String> {
    let policy = read_policy(&root.join("tools/osv-scanner.env"))?;
    let (asset_key, executable) = osv_platform(std::env::consts::OS, std::env::consts::ARCH)?;
    check_osv_policy(&policy, &asset_key)?;
    let target = std::path::absolute(target)
        .map_err(|error| format!("cannot resolve {}: {error}", target.display()))?;
    let listing = Command::new("git")
        .arg("-C")
        .arg(&target)
        .args(["ls-files", "-z", "--", "*Cargo.lock", "*pnpm-lock.yaml"])
        .output()
        .map_err(|error| format!("cannot run git: {error}"))?;
    if !listing.status.success() {
        return Err(format!(
            "target is not a Git repository: {}",
            target.display()
        ));
    }
    let lockfiles: Vec<String> = String::from_utf8_lossy(&listing.stdout)
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect();
    if lockfiles.is_empty() {
        return Err("no tracked Cargo.lock or pnpm-lock.yaml files".to_owned());
    }
    let scan = std::env::temp_dir().join(format!("xtask-osv-{}", std::process::id()));
    fs::create_dir_all(&scan).map_err(|error| error.to_string())?;
    let result = scan_and_check(root, &target, report, &lockfiles, &scan.join(executable));
    let _ = fs::remove_dir_all(&scan);
    result
}

fn scan_and_check(
    root: &Path,
    target: &Path,
    report: Option<&Path>,
    lockfiles: &[String],
    binary: &Path,
) -> Result<bool, String> {
    // Downloaded, checksum-verified and cached by the one pinned fetcher (root ADR-0197).
    succeed(
        Command::new("python3")
            .arg(root.join("scripts/ci/fetch_pinned.py"))
            .args(["tool", "osv-scanner", "--output"])
            .arg(binary),
        "fetch_pinned.py tool osv-scanner",
    )?;
    make_executable(binary)?;
    let report = match report {
        None => binary.with_file_name("osv-report.json"),
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => target.join(path),
    };
    if let Some(parent) = report.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let mut scan = Command::new(binary);
    scan.args([
        "scan",
        "--format",
        "json",
        "--verbosity",
        "error",
        "--output-file",
    ])
    .arg(&report);
    for lockfile in lockfiles {
        scan.arg("-L").arg(target.join(lockfile));
    }
    let status = scan
        .status()
        .map_err(|error| format!("cannot run OSV-Scanner: {error}"))?;
    // 1 means findings; the baseline check below decides whether they are allowed.
    if !matches!(status.code(), Some(0 | 1)) {
        return Err(format!("OSV-Scanner exited {status}"));
    }
    println!(
        "INFO osv-vulnerability-gate: scanner_rc={} lockfiles={} report={}",
        status.code().unwrap_or(-1),
        lockfiles.len(),
        report.display()
    );
    Ok(Command::new("python3")
        .arg(root.join("scripts/guard/check-osv-vulnerability-baseline.py"))
        .arg("--baseline")
        .arg(root.join("tools/osv-vulnerability-baseline.tsv"))
        .arg("--report")
        .arg(&report)
        .status()
        .map_err(|error| format!("cannot run the baseline check: {error}"))?
        .success())
}

fn postgres_production_shape(root: &Path, url: &str, base: &str) -> Result<(), String> {
    let gate = root.join("scripts/ci/foundation_postgres_production_shape.py");
    succeed(
        Command::new("python3")
            .arg(root.join("scripts/ci/test_foundation_postgres_production_shape.py")),
        "the gate's unit tests",
    )?;
    let touched = if base.is_empty() || base.bytes().all(|b| b == b'0') {
        println!("postgres-production-shape: no base commit (a manual run); the change cannot be told apart");
        "all".to_owned()
    } else {
        let present = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["cat-file", "-e", &format!("{base}^{{commit}}")])
            .status();
        if !present.map(|status| status.success()).unwrap_or(false) {
            succeed(
                Command::new("git").arg("-C").arg(root).args([
                    "fetch",
                    "-q",
                    "--no-tags",
                    "--depth=1",
                    "origin",
                    base,
                ]),
                "git fetch (base)",
            )?;
        }
        stdout(
            Command::new("git")
                .arg("-C")
                .arg(root)
                .args(["diff", "--name-only", base, "HEAD", "--"])
                .args(SHAPE_INPUTS),
        )?
    };
    let base = if touched == "all" { "HEAD" } else { base };
    if touched.trim().is_empty() {
        println!(
            "postgres-production-shape: no migration or seed change; nothing to apply onto rows"
        );
        return Ok(());
    }
    let server = url
        .rsplit_once('/')
        .map(|(server, _)| server)
        .ok_or("the database url has no database")?;
    succeed(
        Command::new("psql")
            .args([
                url,
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "create database shape_self_test",
                "-c",
            ])
            .arg("create database shape_change"),
        "psql create database",
    )?;
    let work = std::env::temp_dir().join(format!("xtask-shape-{}", std::process::id()));
    fs::create_dir_all(&work).map_err(|error| error.to_string())?;
    let result = (|| {
        let nullable = work.join("planted_nullable.sql");
        let required = work.join("planted_required.sql");
        fs::write(
            &nullable,
            "ALTER TABLE catalog.industrial_complex ADD COLUMN planted_note text;\n",
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            &required,
            "ALTER TABLE catalog.industrial_complex ADD COLUMN planted_required text NOT NULL;\n",
        )
        .map_err(|error| error.to_string())?;
        let self_test = Command::new("python3")
            .arg(&gate)
            .args([
                "--database-url",
                &format!("{server}/shape_self_test"),
                "--base",
                "HEAD",
                "--try",
            ])
            .arg(&nullable)
            .arg("--try")
            .arg(&required)
            .output()
            .map_err(|error| format!("cannot run the gate: {error}"))?;
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&self_test.stdout),
            String::from_utf8_lossy(&self_test.stderr)
        );
        print!("{log}");
        if !self_test_refused_the_planted_column(self_test.status.success(), &log) {
            return Err(
                "FAILED: the self-test's required column did not fail on the seeded rows"
                    .to_owned(),
            );
        }
        println!("postgres-production-shape: self-test OK");
        succeed(
            Command::new("python3").arg(&gate).args([
                "--database-url",
                &format!("{server}/shape_change"),
                "--base",
                base,
            ]),
            "the production-shape gate",
        )
    })();
    let _ = fs::remove_dir_all(&work);
    result
}

/// What a change must touch for the production-shape step to apply anything onto rows.
const SHAPE_INPUTS: &[&str] = &[
    "platforms/foundation-platform/migrations",
    "platforms/foundation-platform/config/postgres-production-shape.json",
    "platforms/foundation-platform/config/postgres-seed-hints.json",
    "scripts/ci/foundation_postgres_production_shape.py",
    "tools/xtask/src/ci.rs",
];

/// The self-test passed only when the gate failed as a whole and its log shows the nullable column
/// applied and the required one refused: a seed that filled nothing cannot pass.
fn self_test_refused_the_planted_column(gate_succeeded: bool, log: &str) -> bool {
    !gate_succeeded
        && log.contains("planted_nullable.sql applied onto rows")
        && log.contains("planted_required.sql fails on tables that hold rows")
}

/// The policy's checksum key and the executable name for this platform.
fn osv_platform(os: &str, arch: &str) -> Result<(String, &'static str), String> {
    let architecture = match arch {
        "x86_64" => "AMD64",
        "aarch64" => "ARM64",
        other => return Err(format!("unsupported architecture {other}")),
    };
    let (platform, executable) = match os {
        "linux" => ("LINUX", "osv-scanner"),
        "macos" => ("DARWIN", "osv-scanner"),
        "windows" => ("WINDOWS", "osv-scanner.exe"),
        other => return Err(format!("unsupported operating system {other}")),
    };
    Ok((
        format!("OSV_SCANNER_{platform}_{architecture}_SHA256"),
        executable,
    ))
}

/// The pinned version and every platform's checksum are well formed, and this platform has one.
fn check_osv_policy(
    policy: &std::collections::BTreeMap<String, String>,
    asset_key: &str,
) -> Result<(), String> {
    if !is_version(policy.get("OSV_SCANNER_VERSION").map_or("", String::as_str)) {
        return Err("malformed pinned release policy".to_owned());
    }
    for platform in [
        "LINUX_AMD64",
        "LINUX_ARM64",
        "DARWIN_AMD64",
        "DARWIN_ARM64",
        "WINDOWS_AMD64",
        "WINDOWS_ARM64",
    ] {
        let key = format!("OSV_SCANNER_{platform}_SHA256");
        if !is_sha256(policy.get(&key).map_or("", String::as_str)) {
            return Err(format!("malformed checksum policy for {key}"));
        }
    }
    if !is_sha256(policy.get(asset_key).map_or("", String::as_str)) {
        return Err(format!("missing checksum {asset_key}"));
    }
    Ok(())
}

/// A scalar-only `KEY=value` policy file (comments and blank lines skipped).
fn read_policy(path: &Path) -> Result<std::collections::BTreeMap<String, String>, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("missing {}: {error}", path.display()))?;
    Ok(parse_policy(&text))
}

fn parse_policy(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| {
            (
                key.trim().to_owned(),
                value.trim().trim_matches('"').to_owned(),
            )
        })
        .collect()
}

fn is_version(text: &str) -> bool {
    let parts: Vec<&str> = text.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

fn is_sha256(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).map_err(|error| error.to_string())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<(), String> {
    Ok(())
}

fn stdout(command: &mut Command) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("cannot run {command:?}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{command:?} failed ({})", output.status));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn succeed(command: &mut Command, what: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("cannot run {what}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{what} failed: {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(text: &str) -> std::collections::BTreeMap<String, String> {
        parse_policy(text)
    }

    fn sha(digit: char) -> String {
        std::iter::repeat_n(digit, 64).collect()
    }

    fn full_policy() -> String {
        let mut text = "# pinned\nOSV_SCANNER_VERSION=2.2.1\n".to_owned();
        for platform in [
            "LINUX_AMD64",
            "LINUX_ARM64",
            "DARWIN_AMD64",
            "DARWIN_ARM64",
            "WINDOWS_AMD64",
            "WINDOWS_ARM64",
        ] {
            text.push_str(&format!("OSV_SCANNER_{platform}_SHA256={}\n", sha('a')));
        }
        text
    }

    #[test]
    fn the_scanner_for_this_platform_is_named_by_os_and_architecture() {
        assert_eq!(
            osv_platform("linux", "x86_64"),
            Ok(("OSV_SCANNER_LINUX_AMD64_SHA256".to_owned(), "osv-scanner"))
        );
        assert_eq!(
            osv_platform("windows", "aarch64"),
            Ok((
                "OSV_SCANNER_WINDOWS_ARM64_SHA256".to_owned(),
                "osv-scanner.exe"
            ))
        );
        assert_eq!(
            osv_platform("macos", "aarch64").map(|(key, _)| key),
            Ok("OSV_SCANNER_DARWIN_ARM64_SHA256".to_owned())
        );
        assert!(osv_platform("freebsd", "x86_64").is_err());
        assert!(osv_platform("linux", "riscv64").is_err());
    }

    #[test]
    fn the_policy_needs_a_version_and_every_platforms_checksum() {
        let key = "OSV_SCANNER_LINUX_AMD64_SHA256";
        assert_eq!(check_osv_policy(&policy(&full_policy()), key), Ok(()));
        let bad_version = full_policy().replace("2.2.1", "2.2");
        assert!(check_osv_policy(&policy(&bad_version), key).is_err());
        let bad_sum = full_policy().replace(
            &format!("WINDOWS_ARM64_SHA256={}", sha('a')),
            "WINDOWS_ARM64_SHA256=ABC",
        );
        assert!(check_osv_policy(&policy(&bad_sum), key)
            .unwrap_err()
            .contains("WINDOWS_ARM64"));
        let upper = full_policy().replace(
            &format!("LINUX_AMD64_SHA256={}", sha('a')),
            &format!("LINUX_AMD64_SHA256={}", sha('A')),
        );
        assert!(
            check_osv_policy(&policy(&upper), key).is_err(),
            "checksums are lower-case hex"
        );
    }

    #[test]
    fn a_policy_file_is_scalar_key_value_lines() {
        let read = policy("# comment\n\nCARGO_DENY_VERSION=0.18.4\r\nQUOTED=\"x\"\n");
        assert_eq!(
            read.get("CARGO_DENY_VERSION").map(String::as_str),
            Some("0.18.4")
        );
        assert_eq!(read.get("QUOTED").map(String::as_str), Some("x"));
        assert!(
            is_version("0.18.4")
                && !is_version("0.18")
                && !is_version("v0.18.4")
                && !is_version("0.18.x")
        );
    }

    #[test]
    fn the_shape_self_test_passes_only_when_the_required_column_was_refused_on_rows() {
        let both = "planted_nullable.sql applied onto rows\nplanted_required.sql fails on tables that hold rows\n";
        assert!(self_test_refused_the_planted_column(false, both));
        assert!(
            !self_test_refused_the_planted_column(true, both),
            "a gate that passed refused nothing"
        );
        assert!(!self_test_refused_the_planted_column(
            false,
            "planted_nullable.sql applied onto rows\n"
        ));
        assert!(!self_test_refused_the_planted_column(
            false,
            "planted_required.sql fails on tables that hold rows\n"
        ));
    }

    #[test]
    fn the_shape_step_reruns_when_its_own_code_changes() {
        assert!(SHAPE_INPUTS.contains(&"tools/xtask/src/ci.rs"));
        assert!(SHAPE_INPUTS.contains(&"platforms/foundation-platform/migrations"));
    }

    #[test]
    fn an_unknown_or_incomplete_command_prints_the_usage() {
        let run = |args: &[&str]| {
            run(
                Path::new("."),
                &args.iter().map(|a| (*a).to_owned()).collect::<Vec<_>>(),
            )
        };
        assert_eq!(run(&[]), Err(USAGE.to_owned()));
        assert_eq!(run(&["install-cargo-deny", "extra"]), Err(USAGE.to_owned()));
        assert_eq!(run(&["osv-gate", "--report-path"]), Err(USAGE.to_owned()));
        assert_eq!(run(&["osv-gate", "a", "b"]), Err(USAGE.to_owned()));
        assert_eq!(run(&["postgres-production-shape"]), Err(USAGE.to_owned()));
    }
}
