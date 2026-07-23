use std::{env, fs, path::PathBuf};

use anyhow::{bail, Context};

use crate::public_data_control_support::{env_path, repo_relative_path, resolve_repo_path};

const DEFAULT_MANIFEST_PATH: &str = "target/audit/national-data-collection-shard-manifest.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/national-data-collection-plan.json";
const DEFAULT_LEDGER_PATH: &str = "target/audit/national-data-collection-execution-ledger.jsonl";

pub(super) struct CompileConfig {
    pub(super) root: PathBuf,
    pub(super) manifest_path: PathBuf,
    pub(super) output_path: PathBuf,
    pub(super) ledger_path: PathBuf,
    pub(super) collection_snapshot_id: Option<String>,
}

impl CompileConfig {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        if !env_bool("FOUNDATION_PLATFORM_CONFIRM_NATIONAL_PLAN_COMPILE", false)? {
            bail!(
                "ConfirmNationalPlanCompile is required before compiling national collection plan"
            );
        }

        let manifest_path = env_repo_path(
            &root,
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SHARD_MANIFEST_PATH",
            DEFAULT_MANIFEST_PATH,
            "ManifestPath",
        )?;
        let output_path = env_repo_path(
            &root,
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_PLAN_OUTPUT_PATH",
            DEFAULT_OUTPUT_PATH,
            "OutputPath",
        )?;
        let ledger_path = env_repo_path(
            &root,
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_EXECUTION_LEDGER_PATH",
            DEFAULT_LEDGER_PATH,
            "LedgerPath",
        )?;

        if !manifest_path.is_file() {
            bail!(
                "National shard manifest missing: {}",
                repo_relative_path(&root, &manifest_path)
            );
        }
        if output_path.is_file() {
            bail!(
                "national data collection plan already exists: {}",
                repo_relative_path(&root, &output_path)
            );
        }
        if ledger_path.is_file() {
            bail!(
                "national data collection execution ledger already exists: {}",
                repo_relative_path(&root, &ledger_path)
            );
        }

        Ok(Self {
            collection_snapshot_id: env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SNAPSHOT_ID",
                "",
            )?
            .filter(|value| !value.is_empty()),
            root,
            manifest_path,
            output_path,
            ledger_path,
        })
    }
}

fn env_repo_path(
    root: &std::path::Path,
    name: &str,
    default: &str,
    label: &str,
) -> anyhow::Result<PathBuf> {
    resolve_repo_path(root, &env_path(name, default)?, label)
}

fn env_string(name: &str, default: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => Ok(Some(raw.trim().to_owned())),
        // Present-but-empty behaves like unset (PowerShell wrappers cannot delete env vars
        // portably).
        Ok(_) | Err(env::VarError::NotPresent) => Ok(Some(default.to_owned())),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    let value = env_string(name, "")?.unwrap_or_default();
    if value.is_empty() {
        return Ok(default);
    }
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("{name} must be a boolean"),
    }
}
