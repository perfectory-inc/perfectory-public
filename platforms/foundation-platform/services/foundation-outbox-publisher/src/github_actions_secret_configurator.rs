use std::{
    collections::BTreeSet,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use serde::Serialize;

use crate::public_data_control_support::{
    env_path, git_head, optional_env_value, repo_relative_path,
    required_secrets_for_mode as shared_required_secrets_for_mode, resolve_repo_path, utc_now,
    write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.github_actions_secrets_configuration.v1";

const ROOT_ENV: &str = "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_ROOT";
const OWNER_REPO_ENV: &str = "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_OWNER_REPO";
const OUTPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_OUTPUT_PATH";
const ENV_FILE_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_ENV_FILE_PATH";
const SECRET_NAMES_ENV: &str =
    "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_SECRET_NAMES";
const GH_EXE_ENV: &str = "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_GH_EXE";
const EXECUTE_ENV: &str = "FOUNDATION_PLATFORM_GITHUB_ACTIONS_SECRET_CONFIGURATOR_EXECUTE";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_CONFIRM_GITHUB_ACTIONS_SECRET_CONFIGURATION";

pub(crate) fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if config.execute && !config.confirm {
        bail!("Execute requires -ConfirmGitHubActionsSecretConfiguration");
    }

    if let Some(env_file_path) = &config.env_file_path {
        import_dotenv_file(env_file_path)?;
    }

    let secret_names = if config.secret_names.is_empty() {
        required_secrets_for_mode("All")
    } else {
        config.secret_names.clone()
    };
    if secret_names.is_empty() {
        bail!("SecretNames must contain at least one secret name");
    }
    for name in &secret_names {
        if !secret_name_is_valid(name) {
            bail!("SecretNames must use uppercase environment variable names: {name}");
        }
    }

    let mut environment = Vec::<EnvironmentState>::new();
    let mut missing_environment_variables = Vec::<String>::new();
    for name in &secret_names {
        let present = env::var(name)
            .ok()
            .is_some_and(|value| !value.trim().is_empty());
        if !present {
            missing_environment_variables.push(name.clone());
        }
        environment.push(EnvironmentState {
            name: name.clone(),
            present,
        });
    }

    let mut configured_secrets = Vec::<SecretCommandResult>::new();
    let mut failed_secrets = Vec::<SecretCommandResult>::new();
    let mut status = if missing_environment_variables.is_empty() {
        "ready".to_owned()
    } else {
        "blocked".to_owned()
    };

    if config.execute {
        if !missing_environment_variables.is_empty() {
            status = "blocked".to_owned();
        } else {
            let gh_exe = resolve_gh_exe(config.gh_exe.as_deref())?;
            for name in &secret_names {
                let value = env::var(name).unwrap_or_default();
                let result = invoke_gh_secret_set(&gh_exe, &config.owner_repo, name, &value)?;
                if result.exit_code == 0 {
                    configured_secrets.push(result);
                } else {
                    failed_secrets.push(result);
                }
            }
            status = if failed_secrets.is_empty() {
                "configured".to_owned()
            } else {
                "failed".to_owned()
            };
        }
    }

    let report = Report {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        scope: "github_actions_secret_configuration_only",
        completion_claim_allowed: false,
        status: status.clone(),
        execute: config.execute,
        owner_repo: config.owner_repo.clone(),
        env_file_loaded: config.env_file_path.is_some(),
        env_file_path: config
            .env_file_path
            .as_ref()
            .map(|path| {
                if path.starts_with(&config.root) {
                    repo_relative_path(&config.root, path)
                } else {
                    "<outside-root>".to_owned()
                }
            })
            .unwrap_or_default(),
        secret_names,
        environment,
        missing_environment_variables,
        configured_secret_count: configured_secrets.len(),
        configured_secrets,
        failed_secret_count: failed_secrets.len(),
        failed_secrets,
        evidence_limitations: vec![
            "does_not_print_secret_values",
            "does_not_create_completion_artifacts",
            "does_not_dispatch_workflows",
            "does_not_validate_runtime_secret_values",
        ],
    };

    write_json_file(&config.output_path, &report)?;
    if config.execute && report.status == "blocked" {
        println!(
            "github-actions-secrets-configuration-ok status=blocked missing_env={} report={}",
            report.missing_environment_variables.len(),
            config.output_path.display()
        );
        bail!("GitHub Actions secret configuration blocked by missing environment variables");
    }
    if report.status == "failed" {
        println!(
            "github-actions-secrets-configuration-failed failed={} report={}",
            report.failed_secret_count,
            config.output_path.display()
        );
        bail!("GitHub Actions secret configuration failed");
    }

    println!(
        "github-actions-secrets-configuration-ok status={} configured={} missing_env={} report={}",
        report.status,
        report.configured_secret_count,
        report.missing_environment_variables.len(),
        config.output_path.display()
    );
    Ok(())
}

struct Config {
    root: PathBuf,
    owner_repo: String,
    output_path: PathBuf,
    env_file_path: Option<PathBuf>,
    secret_names: Vec<String>,
    gh_exe: Option<PathBuf>,
    execute: bool,
    confirm: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path(ROOT_ENV, ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("Root does not exist: {}", root.display()))?;
        if !root.is_dir() {
            bail!("Root does not exist: {}", root.display());
        }
        let owner_repo = optional_env_value(OWNER_REPO_ENV)?
            .map(Ok)
            .unwrap_or_else(|| owner_repo_from_git_origin(&root))?;
        if !owner_repo_is_valid(&owner_repo) {
            bail!("OwnerRepo must use owner/repo format");
        }

        let output_path = resolve_repo_path(
            &root,
            &env_path(
                OUTPUT_PATH_ENV,
                "target/audit/github-actions-secrets-configuration.json",
            )?,
            "OutputPath",
        )
        .map_err(|_| anyhow::anyhow!("OutputPath must stay within Root"))?;
        let env_file_path = optional_env_value(ENV_FILE_PATH_ENV)?
            .map(PathBuf::from)
            .map(|path| resolve_input_path(&root, &path, "EnvFilePath"))
            .transpose()?;
        let secret_names = parse_secret_names(optional_env_value(SECRET_NAMES_ENV)?.as_deref());
        let gh_exe = optional_env_value(GH_EXE_ENV)?.map(PathBuf::from);

        Ok(Self {
            root,
            owner_repo,
            output_path,
            env_file_path,
            secret_names,
            gh_exe,
            execute: env_bool(EXECUTE_ENV)?,
            confirm: env_bool(CONFIRM_ENV)?,
        })
    }
}

#[derive(Serialize)]
struct Report {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    scope: &'static str,
    completion_claim_allowed: bool,
    status: String,
    execute: bool,
    owner_repo: String,
    env_file_loaded: bool,
    env_file_path: String,
    secret_names: Vec<String>,
    environment: Vec<EnvironmentState>,
    missing_environment_variables: Vec<String>,
    configured_secrets: Vec<SecretCommandResult>,
    configured_secret_count: usize,
    failed_secrets: Vec<SecretCommandResult>,
    failed_secret_count: usize,
    evidence_limitations: Vec<&'static str>,
}

#[derive(Serialize)]
struct EnvironmentState {
    name: String,
    present: bool,
}

#[derive(Serialize)]
struct SecretCommandResult {
    name: String,
    exit_code: i32,
}

fn import_dotenv_file(path: &Path) -> anyhow::Result<()> {
    if !path.is_file() {
        bail!("EnvFilePath does not exist");
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read EnvFilePath {}", path.display()))?;
    for line in content.lines() {
        let trimmed = line.trim().trim_start_matches('\u{feff}');
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((name, value)) = trimmed.split_once('=') else {
            bail!("EnvFilePath contains an invalid line");
        };
        let name = name.trim();
        if !secret_name_is_valid(name) {
            bail!("EnvFilePath contains an invalid variable name");
        }
        let value = value.trim().trim_matches('"').trim_matches('\'').to_owned();
        env::set_var(name, value);
    }
    Ok(())
}

fn invoke_gh_secret_set(
    gh_exe: &Path,
    owner_repo: &str,
    secret_name: &str,
    secret_value: &str,
) -> anyhow::Result<SecretCommandResult> {
    let gh_args = [
        "secret",
        "set",
        secret_name,
        "--repo",
        owner_repo,
        "--app",
        "actions",
    ];
    let mut command = ProcessCommand::new(gh_exe);
    command.args(gh_args);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| "failed to start gh secret set")?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(secret_value.as_bytes())
            .with_context(|| "failed to write secret value to gh stdin")?;
    }

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child
            .try_wait()
            .with_context(|| "failed to wait for gh secret set")?
        {
            return Ok(SecretCommandResult {
                name: secret_name.to_owned(),
                exit_code: status.code().unwrap_or(1),
            });
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(SecretCommandResult {
                name: secret_name.to_owned(),
                exit_code: 124,
            });
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn resolve_gh_exe(explicit: Option<&Path>) -> anyhow::Result<PathBuf> {
    if let Some(explicit) = explicit {
        if explicit.as_os_str().is_empty() {
            return resolve_gh_exe(None);
        }
        return Ok(explicit.to_path_buf());
    }
    if let Some(path) = find_gh_on_path() {
        return Ok(path);
    }
    let candidate = PathBuf::from("C:/Program Files/GitHub CLI/gh.exe");
    if candidate.is_file() {
        return Ok(candidate);
    }
    Ok(PathBuf::from("gh"))
}

fn find_gh_on_path() -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    let candidates = if cfg!(windows) {
        vec!["gh.exe", "gh.cmd", "gh.bat", "gh"]
    } else {
        vec!["gh", "gh.exe", "gh.cmd", "gh.bat"]
    };
    env::split_paths(&path).find_map(|directory| {
        candidates
            .iter()
            .map(|name| directory.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn required_secrets_for_mode(mode: &str) -> Vec<String> {
    shared_required_secrets_for_mode(mode)
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn parse_secret_names(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or_default()
        .split([';', ','])
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn owner_repo_from_git_origin(root: &Path) -> anyhow::Result<String> {
    let output = ProcessCommand::new("git")
        .args(["-C", &root.to_string_lossy(), "remote", "get-url", "origin"])
        .output()
        .with_context(|| "OwnerRepo is required when git origin remote is missing")?;
    if !output.status.success() {
        bail!("OwnerRepo is required when git origin remote is missing");
    }
    let remote = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let Some(rest) = remote.strip_prefix("https://github.com/") else {
        bail!("origin remote must be an HTTPS GitHub repository URL");
    };
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    if !owner_repo_is_valid(rest) {
        bail!("origin remote must be an HTTPS GitHub repository URL");
    }
    Ok(rest.to_owned())
}

fn resolve_input_path(root: &Path, path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("{label} is required");
    }
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    })
}

fn owner_repo_is_valid(value: &str) -> bool {
    let Some((owner, repo)) = value.split_once('/') else {
        return false;
    };
    !owner.is_empty()
        && !repo.is_empty()
        && value.split('/').count() == 2
        && owner.chars().all(owner_repo_char_is_valid)
        && repo.chars().all(owner_repo_char_is_valid)
}

fn owner_repo_char_is_valid(value: char) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, '_' | '.' | '-')
}

fn secret_name_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
}

fn env_bool(name: &str) -> anyhow::Result<bool> {
    let Some(raw) = optional_env_value(name)? else {
        return Ok(false);
    };
    Ok(matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    ))
}

#[cfg(test)]
mod tests {
    use super::{owner_repo_is_valid, parse_secret_names, required_secrets_for_mode};

    #[test]
    fn comma_and_semicolon_secret_names_are_deduped_and_sorted() {
        assert_eq!(
            parse_secret_names(Some(
                "FOUNDATION_PLATFORM_DATABASE_URL,FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET;FOUNDATION_PLATFORM_DATABASE_URL"
            )),
            vec![
                "FOUNDATION_PLATFORM_DATABASE_URL".to_owned(),
                "FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET".to_owned(),
            ]
        );
    }

    #[test]
    fn required_secrets_are_selected_by_dispatch_mode() {
        let all = required_secrets_for_mode("All");
        assert!(all.contains(&"FOUNDATION_PLATFORM_DATABASE_URL".to_owned()));
        assert!(all.contains(&"FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET".to_owned()));
        assert_eq!(
            required_secrets_for_mode("ConsumerReceiverE2E"),
            vec!["FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET".to_owned()]
        );
        assert!(required_secrets_for_mode("SupplyChainReleaseGates").is_empty());
    }

    #[test]
    fn owner_repo_shape_is_strict() {
        assert!(owner_repo_is_valid("acme/foundation-platform"));
        assert!(!owner_repo_is_valid(
            "https://github.com/acme/foundation-platform"
        ));
        assert!(!owner_repo_is_valid("acme/platform/core"));
    }
}
