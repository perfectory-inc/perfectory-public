//! CLI adapter for provider-neutral Lakehouse quality-rule evaluation.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use lakehouse_domain::{evaluate_lakehouse_quality_rules, LakehouseQualityRules, SparkRunSummary};

use crate::public_data_control_support::read_json;

const DEFAULT_RULES_PATH: &str = "docs/data-quality/lakehouse-quality-rules.v1.example.json";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let summary =
        SparkRunSummary::from_json_value(read_json(&config.summary_path, "Spark run summary")?)
            .map_err(anyhow::Error::new)
            .context("invalid Spark run summary json")?;
    let rules_document = LakehouseQualityRules::from_json_value(read_json(
        &config.rules_path,
        "Lakehouse quality rules",
    )?)
    .map_err(anyhow::Error::new)
    .context("invalid Lakehouse quality rules json")?;

    let outcome =
        evaluate_lakehouse_quality_rules(&summary, &rules_document).map_err(anyhow::Error::new)?;
    if outcome.is_blocked() {
        for violation in outcome.violations {
            eprintln!("{violation}");
        }
        bail!("lakehouse quality evaluation blocked");
    }

    println!(
        "lakehouse-quality-evaluation-ok table={} rules={}",
        outcome.table, outcome.evaluated_rule_count
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    summary_path: PathBuf,
    rules_path: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = optional_env("FOUNDATION_PLATFORM_REPO_ROOT")?.unwrap_or_else(|| ".".to_owned());
        let root = fs::canonicalize(PathBuf::from(root))
            .context("failed to resolve foundation-platform repo root")?;
        let summary_raw =
            required_env("FOUNDATION_PLATFORM_LAKEHOUSE_QUALITY_EVALUATION_SUMMARY_PATH")?;
        let rules_raw =
            optional_env("FOUNDATION_PLATFORM_LAKEHOUSE_QUALITY_EVALUATION_RULES_PATH")?
                .unwrap_or_else(|| DEFAULT_RULES_PATH.to_owned());

        Ok(Self {
            summary_path: resolve_input_path(&root, Path::new(&summary_raw), "Spark run summary")?,
            rules_path: resolve_input_path(
                &root,
                Path::new(&rules_raw),
                "Lakehouse quality rules",
            )?,
        })
    }
}

fn resolve_input_path(root: &Path, path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    if !resolved.is_file() {
        bail!("{label} not found: {}", resolved.display());
    }
    Ok(resolved)
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.map_or_else(|| bail!("{name} is required"), Ok)
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}
