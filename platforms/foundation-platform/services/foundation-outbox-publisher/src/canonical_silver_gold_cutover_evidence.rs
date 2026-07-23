use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{env_path, read_json, utc_now, write_json_file};

const EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.cutover_canonical_silver_gold_live_write.v1";
const RUN_SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.spark_run_summary.v1";
const PREFIX: &str = "FOUNDATION_PLATFORM_CANONICAL_SILVER_GOLD_CUTOVER_EVIDENCE";
const DEFAULT_SILVER_SUMMARY_PATH: &str =
    "target/lakehouse/smoke/summaries/industrial_complexes_iceberg.json";
const DEFAULT_GOLD_SUMMARY_PATH: &str =
    "target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg.json";
const DEFAULT_OUTPUT_PATH: &str = "target/cutover/canonical-silver-gold-live-write.json";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if config.readback_status.trim().is_empty() {
        bail!("ReadbackStatus is required");
    }
    if config.readback_status != "passed" {
        bail!("ReadbackStatus must be passed");
    }

    let silver = read_summary(
        &config.silver_summary_path,
        "industrial_complex_bronze_to_silver",
        "silver.industrial_complexes",
        "r2.silver.industrial_complexes",
    )?;
    let gold = read_summary(
        &config.gold_summary_path,
        "industrial_complex_silver_to_gold",
        "gold.complex_catalog",
        "r2.gold.complex_catalog",
    )?;
    assert_gold_input_from_canonical_silver(&gold, &config.gold_summary_path)?;

    let source_snapshot_ids = unique_snapshot_ids(
        silver
            .source_snapshot_ids
            .iter()
            .chain(&gold.source_snapshot_ids),
    );
    let source_snapshot_id = source_snapshot_ids
        .first()
        .context("source_snapshot_ids must contain at least one id")?
        .clone();

    let resolved_output_path = config.output_path;
    if resolved_output_path.is_file() {
        bail!(
            "cutover evidence already exists: {}",
            resolved_output_path.display()
        );
    }

    let evidence = json!({
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "silver": {
            "table": "silver.industrial_complexes",
            "qualified_table": silver.qualified_table,
            "write_mode": silver.write_mode,
            "write_disposition": silver.write_disposition,
            "write_status": "passed",
            "row_count": silver.persisted_row_count,
            "quality_metrics": silver.quality_metrics,
        },
        "gold": {
            "table": "gold.complex_catalog",
            "qualified_table": gold.qualified_table,
            "source_table": "silver.industrial_complexes",
            "qualified_source_table": gold.qualified_input_table,
            "write_mode": gold.write_mode,
            "write_disposition": gold.write_disposition,
            "write_status": "passed",
            "row_count": gold.persisted_row_count,
            "quality_metrics": gold.quality_metrics,
        },
        "readback": {
            "status": config.readback_status,
        },
        "source_snapshot_id": source_snapshot_id,
        "source_snapshot_ids": source_snapshot_ids,
    });
    write_json_file(&resolved_output_path, &evidence)?;

    println!(
        "canonical-silver-gold-cutover-evidence-written path={}",
        resolved_output_path.display()
    );
    Ok(())
}

struct Config {
    silver_summary_path: PathBuf,
    gold_summary_path: PathBuf,
    readback_status: String,
    output_path: PathBuf,
}

struct RunSummary {
    qualified_table: String,
    qualified_input_table: String,
    write_mode: String,
    write_disposition: String,
    persisted_row_count: i64,
    source_snapshot_ids: Vec<String>,
    quality_metrics: JsonValue,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );

        let silver_summary_path = env_path(
            &format!("{PREFIX}_SILVER_SUMMARY_PATH"),
            DEFAULT_SILVER_SUMMARY_PATH,
        )?;
        let gold_summary_path = env_path(
            &format!("{PREFIX}_GOLD_SUMMARY_PATH"),
            DEFAULT_GOLD_SUMMARY_PATH,
        )?;
        let output_path = env_path(&format!("{PREFIX}_OUTPUT_PATH"), DEFAULT_OUTPUT_PATH)?;

        Ok(Self {
            silver_summary_path: resolve_input_path(&root, &silver_summary_path)?,
            gold_summary_path: resolve_input_path(&root, &gold_summary_path)?,
            readback_status: env_string(&format!("{PREFIX}_READBACK_STATUS"), "")?,
            output_path: resolve_output_path(&root, &output_path)?,
        })
    }
}

fn read_summary(
    path: &Path,
    expected_job_name: &str,
    expected_contract: &str,
    expected_qualified_table: &str,
) -> anyhow::Result<RunSummary> {
    if !path.is_file() {
        bail!("run summary missing: {}", path.display());
    }
    let summary = read_json(path, "Spark run summary")?;
    if string_property(&summary, "schema_version") != RUN_SUMMARY_SCHEMA_VERSION {
        bail!("unexpected run summary schema_version: {}", path.display());
    }
    if string_property(&summary, "job_name") != expected_job_name {
        bail!("unexpected run summary job_name: {}", path.display());
    }
    if string_property(&summary, "contract") != expected_contract {
        bail!("unexpected run summary contract: {}", path.display());
    }

    let target = summary
        .get("target")
        .context("canonical cutover summary target missing")?;
    if string_property(target, "kind") != "iceberg" {
        bail!(
            "canonical cutover summary target.kind must be iceberg: {}",
            path.display()
        );
    }
    let qualified_table = string_property(target, "qualified_table");
    if qualified_table.trim().is_empty() || qualified_table.ends_with("_smoke") {
        bail!("canonical table required for cutover evidence: {qualified_table}");
    }
    if qualified_table != expected_qualified_table {
        bail!("unexpected canonical table for cutover evidence: {qualified_table}");
    }

    let write_mode = string_property(&summary, "write_mode");
    if write_mode != "iceberg" {
        bail!(
            "canonical cutover summary write_mode must be iceberg: {}",
            path.display()
        );
    }
    let write_disposition = string_property(&summary, "write_disposition");
    if !matches!(
        write_disposition.as_str(),
        "iceberg_append" | "iceberg_overwrite"
    ) {
        bail!(
            "canonical cutover summary write_disposition must be an Iceberg write: {}",
            path.display()
        );
    }

    let row_count = i64_property(&summary, "row_count", 0);
    let persisted_row_count = i64_property(&summary, "persisted_row_count", 0);
    if persisted_row_count < 1 {
        bail!("persisted_row_count must be positive: {}", path.display());
    }
    if row_count != persisted_row_count {
        bail!(
            "row_count must match persisted_row_count: {}",
            path.display()
        );
    }
    if bool_property(&summary, "source_snapshot_truncated", true) {
        bail!("source snapshots must not be truncated: {}", path.display());
    }

    let source_snapshot_ids = string_list_property(&summary, "source_snapshot_ids");
    if source_snapshot_ids.is_empty() || source_snapshot_ids[0].trim().is_empty() {
        bail!(
            "source_snapshot_ids must contain at least one id: {}",
            path.display()
        );
    }
    if source_snapshot_ids
        .iter()
        .any(|snapshot_id| snapshot_id.trim().is_empty())
    {
        bail!(
            "source_snapshot_ids must not contain blank id: {}",
            path.display()
        );
    }

    let quality_metrics = summary
        .get("quality_metrics")
        .cloned()
        .unwrap_or(JsonValue::Null);
    let quality_row_count = positive_i64_metric(&quality_metrics, "row_count", path)?;
    let quality_persisted_row_count =
        positive_i64_metric(&quality_metrics, "persisted_row_count", path)?;
    if quality_row_count != quality_persisted_row_count {
        bail!(
            "quality_metrics.row_count must match quality_metrics.persisted_row_count: {}",
            path.display()
        );
    }
    if quality_row_count != row_count || quality_persisted_row_count != persisted_row_count {
        bail!(
            "quality_metrics must match summary row counts: {}",
            path.display()
        );
    }

    let input = summary.get("input").unwrap_or(&JsonValue::Null);
    Ok(RunSummary {
        qualified_table,
        qualified_input_table: string_property(input, "qualified_table"),
        write_mode,
        write_disposition,
        persisted_row_count,
        source_snapshot_ids,
        quality_metrics,
    })
}

fn assert_gold_input_from_canonical_silver(
    summary: &RunSummary,
    path: &Path,
) -> anyhow::Result<()> {
    let gold_json = read_json(path, "Spark run summary")?;
    let input = gold_json
        .get("input")
        .context("Gold cutover summary input missing")?;
    if string_property(input, "kind") != "iceberg" {
        bail!(
            "Gold cutover summary input.kind must be iceberg: {}",
            path.display()
        );
    }
    let qualified_input_table = string_property(input, "qualified_table");
    if qualified_input_table.trim().is_empty()
        || qualified_input_table.ends_with("_smoke")
        || qualified_input_table != "r2.silver.industrial_complexes"
    {
        bail!(
            "Gold cutover summary input must be canonical silver.industrial_complexes: {qualified_input_table}"
        );
    }
    if summary.qualified_input_table != qualified_input_table {
        bail!(
            "Gold cutover summary input must be canonical silver.industrial_complexes: {}",
            summary.qualified_input_table
        );
    }
    Ok(())
}

fn unique_snapshot_ids<'a>(values: impl Iterator<Item = &'a String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            output.push(value.clone());
        }
    }
    output
}

fn resolve_input_path(root: &Path, path: &Path) -> anyhow::Result<PathBuf> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    Ok(resolved)
}

fn resolve_output_path(root: &Path, path: &Path) -> anyhow::Result<PathBuf> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    if !resolved.starts_with(root) {
        bail!("OutputPath must stay within repository root");
    }
    Ok(resolved)
}

fn positive_i64_metric(value: &JsonValue, name: &str, path: &Path) -> anyhow::Result<i64> {
    let metric = value.get(name).and_then(json_i64).unwrap_or(0);
    if metric < 1 {
        bail!(
            "quality_metrics.{name} must be positive: {}",
            path.display()
        );
    }
    Ok(metric)
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        // Present-but-empty behaves like unset: PowerShell wrappers cannot delete env vars
        // portably ($env:X = "" removes on Windows but sets empty on Linux).
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn string_property(value: &JsonValue, name: &str) -> String {
    value
        .get(name)
        .map(|property| match property {
            JsonValue::String(text) => text.clone(),
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(number) => number.to_string(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn string_list_property(value: &JsonValue, name: &str) -> Vec<String> {
    match value.get(name) {
        Some(JsonValue::Array(items)) => items
            .iter()
            .map(|item| match item {
                JsonValue::String(text) => text.clone(),
                JsonValue::Null => String::new(),
                JsonValue::Bool(flag) => flag.to_string(),
                JsonValue::Number(number) => number.to_string(),
                other => other.to_string(),
            })
            .collect(),
        Some(other) => vec![match other {
            JsonValue::String(text) => text.clone(),
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(number) => number.to_string(),
            other => other.to_string(),
        }],
        None => Vec::new(),
    }
}

fn i64_property(value: &JsonValue, name: &str, default: i64) -> i64 {
    value.get(name).and_then(json_i64).unwrap_or(default)
}

fn json_i64(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Number(number) => number.as_i64(),
        JsonValue::String(text) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn bool_property(value: &JsonValue, name: &str, default: bool) -> bool {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Bool(flag) => Some(*flag),
            JsonValue::String(text) => text.trim().parse::<bool>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let value = path.to_string_lossy();
        if let Some(rest) = value.strip_prefix("\\\\?\\UNC\\") {
            return PathBuf::from(format!("\\\\{rest}"));
        }
        if let Some(rest) = value.strip_prefix("\\\\?\\") {
            return PathBuf::from(rest);
        }
    }
    path
}
