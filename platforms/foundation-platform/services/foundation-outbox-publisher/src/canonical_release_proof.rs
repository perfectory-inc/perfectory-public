//! Validates one bounded Bronze-to-Gold-to-tile release evidence bundle.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde_json::{json, Value};

use crate::public_data_control_support::{env_path, read_json, utc_now};

const PREFIX: &str = "FOUNDATION_PLATFORM_CANONICAL_RELEASE_PROOF";
const DEFAULT_SILVER_SUMMARY_PATH: &str =
    "target/lakehouse/smoke/summaries/industrial_complexes_iceberg.json";
const DEFAULT_GOLD_SUMMARY_PATH: &str =
    "target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg.json";
const DEFAULT_TILE_MANIFEST_PATH: &str = "target/canonical/vector-tile-manifest.json";
const DEFAULT_OUTPUT_PATH: &str = "target/canonical/canonical-release-proof.json";

/// Runs the bounded release proof against existing summary and manifest artifacts.
pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let silver = read_json(&config.silver_summary_path, "Silver run summary")?;
    validate_run_summary(
        &silver,
        "industrial_complex_bronze_to_silver",
        "silver.industrial_complexes",
        "r2.silver.industrial_complexes",
        None,
    )?;
    let gold = read_json(&config.gold_summary_path, "Gold run summary")?;
    validate_run_summary(
        &gold,
        "industrial_complex_silver_to_gold",
        "gold.complex_catalog",
        "r2.gold.complex_catalog",
        Some("r2.silver.industrial_complexes"),
    )?;
    let tiles = read_json(&config.tile_manifest_path, "vector tile manifest")?;
    let evidence = build_release_proof(&silver, &gold, &tiles)?;
    write_atomic_json(&config.output_path, &evidence)?;
    println!(
        "canonical-release-proof-written path={} release_id={}",
        config.output_path.display(),
        evidence["release_id"].as_str().unwrap_or("<unknown>")
    );
    Ok(())
}

struct Config {
    silver_summary_path: PathBuf,
    gold_summary_path: PathBuf,
    tile_manifest_path: PathBuf,
    output_path: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repository root {}", root.display()))?;
        let silver_summary_path = resolve_input(
            &root,
            env_path(
                &format!("{PREFIX}_SILVER_SUMMARY_PATH"),
                DEFAULT_SILVER_SUMMARY_PATH,
            )?,
        );
        let gold_summary_path = resolve_input(
            &root,
            env_path(
                &format!("{PREFIX}_GOLD_SUMMARY_PATH"),
                DEFAULT_GOLD_SUMMARY_PATH,
            )?,
        );
        let tile_manifest_path = resolve_input(
            &root,
            env_path(
                &format!("{PREFIX}_TILE_MANIFEST_PATH"),
                DEFAULT_TILE_MANIFEST_PATH,
            )?,
        );
        let output_path = resolve_output(
            &root,
            env_path(&format!("{PREFIX}_OUTPUT_PATH"), DEFAULT_OUTPUT_PATH)?,
        )?;
        Ok(Self {
            silver_summary_path,
            gold_summary_path,
            tile_manifest_path,
            output_path,
        })
    }
}

fn resolve_input(root: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

fn resolve_output(root: &Path, path: PathBuf) -> anyhow::Result<PathBuf> {
    let resolved = resolve_input(root, path);
    if !resolved.starts_with(root) {
        bail!("canonical release proof output must stay inside repository root");
    }
    Ok(resolved)
}

fn validate_run_summary(
    summary: &Value,
    expected_job: &str,
    expected_contract: &str,
    expected_target: &str,
    expected_input: Option<&str>,
) -> anyhow::Result<()> {
    if summary["schema_version"] != "foundation-platform.spark_run_summary.v1" {
        bail!("run summary schema_version is not the canonical Spark summary");
    }
    if summary["job_name"] != expected_job || summary["contract"] != expected_contract {
        bail!("run summary job/contract does not match canonical release proof");
    }
    if summary["target"]["kind"] != "iceberg"
        || summary["target"]["qualified_table"] != expected_target
    {
        bail!("run summary target must be canonical Iceberg table {expected_target}");
    }
    if let Some(expected_input) = expected_input {
        if summary["input"]["qualified_table"] != expected_input {
            bail!("Gold input must be canonical Silver table {expected_input}");
        }
    }
    if summary["write_mode"] != "iceberg"
        || !matches!(
            summary["write_disposition"].as_str(),
            Some("iceberg_append") | Some("iceberg_overwrite")
        )
    {
        bail!("canonical release summary must describe an Iceberg write");
    }
    if summary["source_snapshot_truncated"] == true {
        bail!("source snapshot list is truncated");
    }
    let snapshots = snapshot_ids(summary)?;
    if snapshots.iter().any(|id| is_placeholder(id)) {
        bail!("source snapshot id must be a real immutable id");
    }
    if summary["persisted_row_count"].as_i64().unwrap_or_default() < 1 {
        bail!("canonical release summary must contain persisted rows");
    }
    Ok(())
}

fn snapshot_ids(summary: &Value) -> anyhow::Result<Vec<String>> {
    let Some(values) = summary["source_snapshot_ids"].as_array() else {
        bail!("source_snapshot_ids must be an array");
    };
    if values.is_empty() {
        bail!("source_snapshot_ids must not be empty");
    }
    let ids = values
        .iter()
        .map(|value| value.as_str().unwrap_or_default().trim().to_owned())
        .collect::<Vec<_>>();
    if ids.iter().any(String::is_empty) {
        bail!("source_snapshot_ids must not contain blank values");
    }
    Ok(ids)
}

fn build_release_proof(silver: &Value, gold: &Value, tiles: &Value) -> anyhow::Result<Value> {
    let _silver_ids = snapshot_ids(silver)?;
    let gold_ids = snapshot_ids(gold)?;
    let tile_snapshot_id = tiles["source_snapshot_id"]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("tile manifest source_snapshot_id is required")?;
    if is_placeholder(tile_snapshot_id) {
        bail!("tile manifest source_snapshot_id must be a real immutable id");
    }
    if !gold_ids.iter().any(|id| id == tile_snapshot_id) {
        bail!(
            "tile manifest source_snapshot_id {tile_snapshot_id} is not present in Gold source_snapshot_ids"
        );
    }
    let release_id = tiles["current_version"]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("tile manifest current_version is required")?;
    if is_placeholder(release_id) {
        bail!("tile manifest current_version must be a real immutable release id");
    }
    let artifacts = tiles["artifacts"]
        .as_object()
        .filter(|artifacts| !artifacts.is_empty())
        .context("tile manifest artifacts must not be empty")?;
    for (layer, artifact) in artifacts {
        if artifact["source_layer"].as_str().unwrap_or_default().trim() != layer {
            bail!("tile artifact source_layer must match logical layer {layer}");
        }
        if artifact["object_key_prefix"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            bail!("tile artifact {layer} object_key_prefix is required");
        }
    }
    Ok(json!({
        "schema_version": "foundation-platform.canonical_release_proof.v1",
        "generated_at_utc": utc_now(),
        "release_id": release_id,
        "source_snapshot_id": tile_snapshot_id,
        "gold_source_snapshot_ids": gold_ids,
        "tile_layers": artifacts.keys().collect::<Vec<_>>(),
    }))
}

fn is_placeholder(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    ["fixture", "synthetic", "dev-none", "_smoke"]
        .iter()
        .any(|token| lower.contains(token))
}

fn write_atomic_json(path: &Path, value: &Value) -> anyhow::Result<()> {
    if path.exists() {
        bail!("canonical release proof already exists: {}", path.display());
    }
    let parent = path
        .parent()
        .context("canonical release proof output must have a parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&temporary, bytes)?;
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "failed to atomically publish canonical release proof {}",
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_release_proof;
    use serde_json::json;

    #[test]
    fn rejects_tile_manifest_from_a_different_source_snapshot() {
        let silver = json!({"source_snapshot_ids": ["iceberg:gold-001"]});
        let gold = json!({"source_snapshot_ids": ["iceberg:gold-001"]});
        let tiles = json!({"current_version": "tile-001", "source_snapshot_id": "iceberg:gold-002", "artifacts": {"parcels": {}}});

        let error = build_release_proof(&silver, &gold, &tiles)
            .expect_err("mismatched source snapshots must fail closed");
        assert!(error.to_string().contains("source_snapshot_id"));
    }

    #[test]
    fn emits_one_release_identity_for_matching_snapshot() {
        let silver = json!({"source_snapshot_ids": ["iceberg:gold-001"]});
        let gold = json!({"source_snapshot_ids": ["iceberg:gold-001"]});
        let tiles = json!({"current_version": "tile-001", "source_snapshot_id": "iceberg:gold-001", "artifacts": {"parcels": {"source_layer": "parcels", "object_key_prefix": "gold/tile-001/parcels/"}}});

        let evidence = build_release_proof(&silver, &gold, &tiles).expect("matching release");
        assert_eq!(evidence["release_id"], "tile-001");
        assert_eq!(evidence["source_snapshot_id"], "iceberg:gold-001");
    }
}
