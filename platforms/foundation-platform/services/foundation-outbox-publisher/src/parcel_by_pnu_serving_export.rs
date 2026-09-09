//! Parcel by-PNU serving object export (root ADR-0096).
//!
//! Reads `gold.parcel_panel` from the Iceberg catalog and writes one serving JSON object per
//! parcel under a caller-named generation directory. Its summary is the input
//! `publish-parcel-by-pnu-serving-manifest` needs: the generation, the snapshot it represents,
//! and every object key with its checksum.
//!
//! The command does not write the manifest. Baking the objects and pointing the gateway at them
//! are separate failures, and were separated so a half-run cannot leave the pointer aimed at a
//! generation that is not fully there.
//!
//! Objects are written create-only. A re-export of the same snapshot into the same generation is
//! an idempotent re-run; different bytes at an existing key are refused unless the caller
//! explicitly states the delta re-bake intent with the overwrite flag.

mod parcel_document;

use std::{
    collections::BTreeSet,
    env,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_domain::GOLD_PARCEL_PANEL;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
use crate::parcel_by_pnu_serving_store::{local_root, ParcelServingObjectStore};
use crate::r2_layout::parcel_by_pnu_serving_object_key;
use parcel_document::{
    GoldSnapshotProvenance, ParcelServingArtifact, PARCEL_DOCUMENT_SCHEMA_VERSION,
};

const SUMMARY_SCHEMA_VERSION: &str = "foundation-platform.parcel_by_pnu_serving_export_summary.v1";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_CONFIRM_EXPORT";
const OUTPUT_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_STORAGE_DRIVER";
const OUTPUT_ROOT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_OUTPUT_ROOT";
const TARGET_GENERATION_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_TARGET_GENERATION";
const EXPECTED_ROW_COUNT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_EXPECTED_ROW_COUNT";
const MAX_CONCURRENCY_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MAX_CONCURRENCY";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_SUMMARY_PATH";
const ALLOW_OVERWRITE_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_ALLOW_OVERWRITE";
const PNU_ALLOWLIST_PATH_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_PNU_ALLOWLIST_PATH";
const DEFAULT_MAX_CONCURRENCY: usize = 8;
const MAX_CONCURRENCY: usize = 32;
/// `scan_snapshot_rows` holds every scanned row in memory. National scale (39.8M parcels) needs
/// a streaming export; this cap makes that boundary an explicit refusal instead of an OOM.
const MAX_ROWS_PER_RUN: usize = 2_000_000;

/// Runs the parcel by-PNU serving export.
pub async fn run() -> anyhow::Result<()> {
    let config = ServingExportConfig::from_env()?;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(GOLD_PARCEL_PANEL.table_name)
        .await
        .context("failed to resolve the Gold parcel panel snapshot")?
        .with_context(|| {
            format!(
                "{} has no current Iceberg snapshot to export",
                GOLD_PARCEL_PANEL.table_name
            )
        })?;

    let lakehouse = LakehouseObjectReader::from_env()?;
    let output = ParcelServingObjectStore::open(&config.output)?;
    let summary = export(&config, &lakehouse, &output, &snapshot).await?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, &summary)?;
    }

    tracing::info!(
        output_bucket = summary.output_bucket.as_deref().unwrap_or("(local)"),
        gold_table = %summary.gold_table,
        gold_iceberg_snapshot_id = %summary.gold_iceberg_snapshot_id,
        target_generation = summary.target_generation,
        scanned_row_count = summary.scanned_row_count,
        exported_row_count = summary.exported_row_count,
        created_object_count = summary.created_object_count,
        reused_object_count = summary.reused_object_count,
        overwritten_object_count = summary.overwritten_object_count,
        output_storage_driver = summary.output_storage_driver,
        "parcel by-PNU serving export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServingExportConfig {
    output: ProfileStoreConfig,
    target_generation: u64,
    expected_row_count: Option<u64>,
    max_concurrency: usize,
    summary_path: Option<PathBuf>,
    allow_overwrite: bool,
    pnu_allowlist: Option<BTreeSet<String>>,
}

#[derive(Debug, Serialize)]
struct ServingExportSummary {
    schema_version: &'static str,
    document_schema_version: &'static str,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    gold_metadata_location: String,
    gold_manifest_list_location: String,
    target_generation: u64,
    data_file_count: u64,
    scanned_row_count: u64,
    exported_row_count: u64,
    output_storage_driver: &'static str,
    output_bucket: Option<String>,
    created_object_count: u64,
    reused_object_count: u64,
    overwritten_object_count: u64,
    artifacts: Vec<ServingExportEntry>,
}

/// One baked object, named the way `publish-parcel-by-pnu-serving-manifest` reads it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ServingExportEntry {
    pnu: String,
    object_key: String,
    object_size_bytes: u64,
    object_checksum_sha256: String,
    write_outcome: &'static str,
}

impl ServingExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env(CONFIRM_ENV)?.unwrap_or_default();
        ensure!(
            confirm.eq_ignore_ascii_case("true"),
            "{CONFIRM_ENV} must be true"
        );

        let target_generation = optional_env(TARGET_GENERATION_ENV)?
            .with_context(|| format!("{TARGET_GENERATION_ENV} is required"))?
            .parse::<u64>()
            .with_context(|| format!("{TARGET_GENERATION_ENV} must be a positive integer"))?;
        ensure!(
            target_generation >= 1,
            "{TARGET_GENERATION_ENV} must be at least 1"
        );

        let pnu_allowlist = optional_env(PNU_ALLOWLIST_PATH_ENV)?
            .map(|raw| read_pnu_allowlist(Path::new(raw.as_str())))
            .transpose()?;

        Ok(Self {
            output: ProfileStoreConfig::parse(
                optional_env(OUTPUT_STORAGE_DRIVER_ENV)?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(optional_env(OUTPUT_ROOT_ENV)?),
            )
            .with_context(|| format!("{OUTPUT_STORAGE_DRIVER_ENV}/{OUTPUT_ROOT_ENV}"))?,
            target_generation,
            expected_row_count: optional_env(EXPECTED_ROW_COUNT_ENV)?
                .map(|value| parse_positive_u64(value.as_str(), EXPECTED_ROW_COUNT_ENV))
                .transpose()?,
            max_concurrency: optional_env(MAX_CONCURRENCY_ENV)?
                .map(|value| parse_max_concurrency(value.as_str()))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
            allow_overwrite: optional_env(ALLOW_OVERWRITE_ENV)?
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            pnu_allowlist,
        })
    }
}

/// Reads the one-PNU-per-line allowlist that scopes a rehearsal or a single-parcel proof run.
fn read_pnu_allowlist(path: &Path) -> anyhow::Result<BTreeSet<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the PNU allowlist {}", path.display()))?;
    let allowlist = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    ensure!(
        !allowlist.is_empty(),
        "the PNU allowlist {} names no parcels",
        path.display()
    );
    Ok(allowlist)
}

async fn export(
    config: &ServingExportConfig,
    lakehouse: &LakehouseObjectReader,
    output: &ParcelServingObjectStore,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ServingExportSummary> {
    let provenance = GoldSnapshotProvenance {
        table: snapshot.table_name.clone(),
        iceberg_snapshot_id: snapshot.snapshot_id.to_string(),
        metadata_location: snapshot.metadata_location.clone(),
        manifest_list_location: snapshot.manifest_list_location.clone(),
    };

    let rows = scan_snapshot_rows(&GOLD_PARCEL_PANEL, lakehouse, snapshot).await?;
    let data_file_count = rows.data_file_count;
    let scanned_row_count = u64::try_from(rows.rows.len()).context("scanned row count overflow")?;
    ensure!(
        rows.rows.len() <= MAX_ROWS_PER_RUN,
        "{} snapshot {} holds {} rows; this in-memory export refuses more than {MAX_ROWS_PER_RUN} \
         — national scale needs the streaming export follow-up, not a bigger heap",
        snapshot.table_name,
        snapshot.snapshot_id,
        rows.rows.len()
    );
    if let Some(expected_row_count) = config.expected_row_count {
        ensure!(
            expected_row_count == scanned_row_count,
            "{} snapshot {} holds {scanned_row_count} rows but {expected_row_count} were expected",
            snapshot.table_name,
            snapshot.snapshot_id
        );
    }
    ensure!(
        scanned_row_count == rows.manifest_record_count,
        "scanned {scanned_row_count} rows but the manifests declared {} rows",
        rows.manifest_record_count
    );

    let selected = select_rows(&rows.rows, config.pnu_allowlist.as_ref())?;
    let entries = write_artifacts(config, output, &provenance, &selected).await?;

    let created_object_count = count_outcome(&entries, "created")?;
    let reused_object_count = count_outcome(&entries, "reused")?;
    let overwritten_object_count = count_outcome(&entries, "overwritten")?;
    Ok(ServingExportSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        document_schema_version: PARCEL_DOCUMENT_SCHEMA_VERSION,
        gold_table: provenance.table.clone(),
        gold_iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        gold_metadata_location: provenance.metadata_location.clone(),
        gold_manifest_list_location: provenance.manifest_list_location.clone(),
        target_generation: config.target_generation,
        data_file_count,
        scanned_row_count,
        exported_row_count: u64::try_from(entries.len()).context("exported row count overflow")?,
        output_storage_driver: output.storage_driver(),
        output_bucket: output.bucket().map(ToOwned::to_owned),
        created_object_count,
        reused_object_count,
        overwritten_object_count,
        artifacts: entries,
    })
}

/// Applies the allowlist and refuses duplicate PNUs — one parcel must resolve to one object.
fn select_rows<'a>(
    rows: &'a [JsonMap<String, JsonValue>],
    allowlist: Option<&BTreeSet<String>>,
) -> anyhow::Result<Vec<&'a JsonMap<String, JsonValue>>> {
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    for row in rows {
        let pnu = row
            .get("pnu")
            .and_then(JsonValue::as_str)
            .context("gold.parcel_panel row is missing pnu")?;
        ensure!(
            seen.insert(pnu.to_owned()),
            "Gold snapshot carries more than one row for parcel {pnu}"
        );
        if allowlist.is_none_or(|list| list.contains(pnu)) {
            selected.push(row);
        }
    }
    if let Some(list) = allowlist {
        let missing = list
            .iter()
            .filter(|pnu| !seen.contains(*pnu))
            .collect::<Vec<_>>();
        ensure!(
            missing.is_empty(),
            "the PNU allowlist names parcels absent from the Gold snapshot: {missing:?}"
        );
    }
    Ok(selected)
}

async fn write_artifacts(
    config: &ServingExportConfig,
    output: &ParcelServingObjectStore,
    provenance: &GoldSnapshotProvenance,
    rows: &[&JsonMap<String, JsonValue>],
) -> anyhow::Result<Vec<ServingExportEntry>> {
    let mut writes = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        writes.push(write_artifact(config, output, provenance, row, index));
    }
    let mut indexed = stream::iter(writes)
        .buffer_unordered(config.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    indexed.sort_by_key(|(index, _)| *index);
    Ok(indexed.into_iter().map(|(_, entry)| entry).collect())
}

async fn write_artifact(
    config: &ServingExportConfig,
    output: &ParcelServingObjectStore,
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
    index: usize,
) -> anyhow::Result<(usize, ServingExportEntry)> {
    let artifact = parcel_document::build(provenance, row)?;
    let object_key = parcel_by_pnu_serving_object_key(config.target_generation, &artifact.pnu)?;
    let write_outcome = write_with_policy(config, output, &object_key, &artifact).await?;
    Ok((
        index,
        ServingExportEntry {
            pnu: artifact.pnu.clone(),
            object_key,
            object_size_bytes: u64::try_from(artifact.body.len())
                .context("serving artifact size overflow")?,
            object_checksum_sha256: artifact.checksum_sha256.clone(),
            write_outcome,
        },
    ))
}

/// Create-only by default; the overwrite flag turns a byte-collision into a stated delta re-bake.
async fn write_with_policy(
    config: &ServingExportConfig,
    output: &ParcelServingObjectStore,
    object_key: &str,
    artifact: &ParcelServingArtifact,
) -> anyhow::Result<&'static str> {
    if !config.allow_overwrite {
        return Ok(
            if output
                .write_object_create_only(object_key, &artifact.body, &artifact.checksum_sha256)
                .await?
            {
                "created"
            } else {
                "reused"
            },
        );
    }
    match output
        .write_object_create_only(object_key, &artifact.body, &artifact.checksum_sha256)
        .await
    {
        Ok(true) => Ok("created"),
        Ok(false) => Ok("reused"),
        Err(_) => {
            output
                .write_object_overwrite(object_key, &artifact.body, &artifact.checksum_sha256)
                .await?;
            Ok("overwritten")
        }
    }
}

fn count_outcome(entries: &[ServingExportEntry], outcome: &str) -> anyhow::Result<u64> {
    u64::try_from(
        entries
            .iter()
            .filter(|entry| entry.write_outcome == outcome)
            .count(),
    )
    .context("write outcome tally overflow")
}

fn write_summary(path: &Path, summary: &ServingExportSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(summary)
        .context("failed to serialize the serving export summary")?;
    std::fs::write(path, payload)
        .with_context(|| format!("failed to write the summary {}", path.display()))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_positive_u64(value: &str, name: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(parsed > 0, "{name} must be greater than zero");
    Ok(parsed)
}

fn parse_max_concurrency(value: &str) -> anyhow::Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{MAX_CONCURRENCY_ENV} must be a positive integer"))?;
    ensure!(
        (1..=MAX_CONCURRENCY).contains(&parsed),
        "{MAX_CONCURRENCY_ENV} must be between 1 and {MAX_CONCURRENCY}"
    );
    Ok(parsed)
}

#[cfg(test)]
mod tests;
