//! Industrial-complex Gold profile artifact export.
//!
//! Reads `gold.complex_catalog` from the Iceberg catalog and writes one immutable profile object
//! per industrial complex, create-only. Its summary is the input
//! `publish-industrial-complex-gold-pointer` needs: the object key, its checksum, its size, and the
//! snapshot it represents.
//!
//! The address template is optional here and required there (root ADR-0037): an object is a fact,
//! an address is a publication. Writing the artifact must not wait on a serving hostname, but
//! pointing consumers at it must not proceed without one.
//!
//! The artifacts are written to the lakehouse bucket under `gold/`, beside the other Gold
//! artifacts a client fetches (root ADR-0039). The bucket that run wrote to is recorded in the
//! summary, so a run that reached the wrong one says so in its own output.
//!
//! The command does not publish pointers. Producing the object and pointing at it are separate
//! failures, and were separated so a half-run cannot leave a pointer aimed at nothing.

mod profile_document;

use std::{
    collections::BTreeSet,
    env,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use chrono::{SecondsFormat, Utc};
use foundation_shared_kernel::{ObjectKey, ObjectUrlTemplate};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_domain::GOLD_COMPLEX_CATALOG;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use crate::industrial_complex_gold_profile_store::{
    local_root, ProfileObjectStore as ProfileOutput, ProfileStoreConfig as ProfileOutputConfig,
};
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
use profile_document::{GoldSnapshotProvenance, ProfileArtifact, PROFILE_SCHEMA_VERSION};

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_gold_profile_export_summary.v1";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_CONFIRM_EXPORT";
const OUTPUT_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_OUTPUT_STORAGE_DRIVER";
const OUTPUT_ROOT_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_OUTPUT_ROOT";
const PROFILE_URL_TEMPLATE_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_URL_TEMPLATE";
const EXPECTED_ROW_COUNT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_EXPECTED_ROW_COUNT";
const MAX_CONCURRENCY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_MAX_CONCURRENCY";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_SUMMARY_PATH";
const DEFAULT_MAX_CONCURRENCY: usize = 8;
const MAX_CONCURRENCY: usize = 32;

/// Runs the industrial-complex Gold profile export.
pub async fn run() -> anyhow::Result<()> {
    let config = ProfileExportConfig::from_env()?;
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let snapshot = catalog
        .load_current_snapshot_manifest_list(GOLD_COMPLEX_CATALOG.table_name)
        .await
        .context("failed to resolve the Gold catalog snapshot")?
        .with_context(|| {
            format!(
                "{} has no current Iceberg snapshot to export",
                GOLD_COMPLEX_CATALOG.table_name
            )
        })?;

    let lakehouse = LakehouseObjectReader::from_env()?;
    let output = ProfileOutput::open(&config.output)?;
    let summary = export(&config, &lakehouse, &output, &snapshot).await?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, &summary)?;
    }

    if summary.profile_url_template.is_none() {
        tracing::warn!(
            env = PROFILE_URL_TEMPLATE_ENV,
            "profile artifacts were written without an address template;              publish-industrial-complex-gold-pointer requires one before consumers can fetch them"
        );
    }

    tracing::info!(
        output_bucket = summary.output_bucket.as_deref().unwrap_or("(local)"),
        gold_table = %summary.gold_table,
        gold_iceberg_snapshot_id = %summary.gold_iceberg_snapshot_id,
        scanned_row_count = summary.scanned_row_count,
        artifact_count = summary.artifact_count,
        created_object_count = summary.created_object_count,
        reused_object_count = summary.reused_object_count,
        output_storage_driver = summary.output_storage_driver,
        "industrial-complex Gold profile export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProfileExportConfig {
    output: ProfileOutputConfig,
    profile_url_template: Option<ObjectUrlTemplate>,
    expected_row_count: Option<u64>,
    max_concurrency: usize,
    summary_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct ProfileExportSummary {
    schema_version: &'static str,
    profile_schema_version: &'static str,
    generated_at_utc: String,
    export_run_id: Uuid,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    gold_metadata_location: String,
    gold_manifest_list_location: String,
    data_file_count: u64,
    scanned_row_count: u64,
    output_storage_driver: &'static str,
    output_bucket: Option<String>,
    profile_url_template: Option<String>,
    artifact_count: u64,
    created_object_count: u64,
    reused_object_count: u64,
    placeholder_parcel_count_row_count: u64,
    null_calculated_area_row_count: u64,
    artifacts: Vec<ProfileExportEntry>,
}

/// One published profile, named the way `publish-industrial-complex-gold-pointer` reads it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ProfileExportEntry {
    complex_id: String,
    official_complex_code: Option<String>,
    current_version: String,
    profile_object_key: String,
    profile_url: Option<String>,
    profile_size_bytes: u64,
    profile_row_count: u64,
    profile_checksum_sha256: String,
    source_snapshot_id: String,
    iceberg_snapshot_id: String,
    published_at_utc: String,
    created: bool,
}

impl ProfileExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env(CONFIRM_ENV)?.unwrap_or_default();
        ensure!(
            confirm.eq_ignore_ascii_case("true"),
            "{CONFIRM_ENV} must be true"
        );

        let profile_url_template = optional_env(PROFILE_URL_TEMPLATE_ENV)?
            .map(|raw| {
                ObjectUrlTemplate::parse(raw.as_str())
                    .map_err(|error| anyhow::anyhow!("{PROFILE_URL_TEMPLATE_ENV} {error}"))
            })
            .transpose()?;

        Ok(Self {
            output: ProfileOutputConfig::parse(
                optional_env(OUTPUT_STORAGE_DRIVER_ENV)?
                    .unwrap_or_else(|| "local".to_owned())
                    .as_str(),
                local_root(optional_env(OUTPUT_ROOT_ENV)?),
            )
            .with_context(|| format!("{OUTPUT_STORAGE_DRIVER_ENV}/{OUTPUT_ROOT_ENV}"))?,
            profile_url_template,
            expected_row_count: optional_env(EXPECTED_ROW_COUNT_ENV)?
                .map(|value| parse_positive_u64(value.as_str(), EXPECTED_ROW_COUNT_ENV))
                .transpose()?,
            max_concurrency: optional_env(MAX_CONCURRENCY_ENV)?
                .map(|value| parse_max_concurrency(value.as_str()))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
        })
    }
}

/// Writes one built artifact to the profile store.
///
/// The store owns create-only semantics and the canonical-key refusal; this is only the seam
/// between the document the export built and the bytes the store writes.
async fn write_artifact_create_only(
    output: &ProfileOutput,
    artifact: &ProfileArtifact,
) -> anyhow::Result<bool> {
    output
        .write_create_only(
            artifact.object_key.as_str(),
            &artifact.body,
            artifact.checksum_sha256.as_str(),
        )
        .await
}

async fn export(
    config: &ProfileExportConfig,
    lakehouse: &LakehouseObjectReader,
    output: &ProfileOutput,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ProfileExportSummary> {
    let provenance = GoldSnapshotProvenance {
        table: snapshot.table_name.clone(),
        iceberg_snapshot_id: snapshot.snapshot_id.to_string(),
        metadata_location: snapshot.metadata_location.clone(),
        manifest_list_location: snapshot.manifest_list_location.clone(),
    };

    let rows = scan_snapshot_rows(&GOLD_COMPLEX_CATALOG, lakehouse, snapshot).await?;
    let data_file_count = rows.data_file_count;
    let scanned_row_count = u64::try_from(rows.rows.len()).context("scanned row count overflow")?;
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

    let entries = write_artifacts(config, output, &provenance, &rows.rows).await?;
    let mut complex_ids = BTreeSet::new();
    for entry in &entries {
        ensure!(
            complex_ids.insert(entry.complex_id.clone()),
            "Gold snapshot carries more than one row for complex {}",
            entry.complex_id
        );
    }

    let created_object_count = entries.iter().filter(|entry| entry.created).count();
    Ok(ProfileExportSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        profile_schema_version: PROFILE_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        export_run_id: Uuid::now_v7(),
        gold_table: provenance.table.clone(),
        gold_iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        gold_metadata_location: provenance.metadata_location.clone(),
        gold_manifest_list_location: provenance.manifest_list_location.clone(),
        data_file_count,
        scanned_row_count,
        output_storage_driver: output.storage_driver(),
        output_bucket: output.bucket().map(ToOwned::to_owned),
        profile_url_template: config
            .profile_url_template
            .as_ref()
            .map(|template| template.as_str().to_owned()),
        artifact_count: u64::try_from(entries.len()).context("artifact count overflow")?,
        created_object_count: u64::try_from(created_object_count)
            .context("created object count overflow")?,
        reused_object_count: u64::try_from(entries.len() - created_object_count)
            .context("reused object count overflow")?,
        placeholder_parcel_count_row_count: count_rows(&rows.rows, |row| {
            row.get("parcel_count").and_then(JsonValue::as_i64) == Some(0)
        })?,
        null_calculated_area_row_count: count_rows(&rows.rows, |row| {
            row.get("calculated_area_sqm")
                .is_none_or(JsonValue::is_null)
        })?,
        artifacts: entries,
    })
}

async fn write_artifacts(
    config: &ProfileExportConfig,
    output: &ProfileOutput,
    provenance: &GoldSnapshotProvenance,
    rows: &[JsonMap<String, JsonValue>],
) -> anyhow::Result<Vec<ProfileExportEntry>> {
    // Built eagerly rather than through a closure: a closure returning a future that borrows its
    // argument needs a higher-ranked bound the compiler cannot infer here.
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
    config: &ProfileExportConfig,
    output: &ProfileOutput,
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
    index: usize,
) -> anyhow::Result<(usize, ProfileExportEntry)> {
    let artifact = profile_document::build(provenance, row)?;
    let created = write_artifact_create_only(output, &artifact).await?;
    let entry = export_entry(config, provenance, row, &artifact, created)?;
    Ok((index, entry))
}

fn export_entry(
    config: &ProfileExportConfig,
    provenance: &GoldSnapshotProvenance,
    row: &JsonMap<String, JsonValue>,
    artifact: &ProfileArtifact,
    created: bool,
) -> anyhow::Result<ProfileExportEntry> {
    let object_key = ObjectKey::parse(artifact.object_key.as_str())
        .map_err(|error| anyhow::anyhow!("profile object key is not publishable: {error}"))?;
    Ok(ProfileExportEntry {
        complex_id: artifact.complex_id.clone(),
        official_complex_code: row
            .get("official_complex_code")
            .and_then(JsonValue::as_str)
            .map(ToOwned::to_owned),
        current_version: artifact.artifact_id.to_string(),
        profile_object_key: artifact.object_key.clone(),
        profile_url: config
            .profile_url_template
            .as_ref()
            .map(|template| template.materialize(&object_key)),
        profile_size_bytes: u64::try_from(artifact.body.len())
            .context("profile artifact size overflow")?,
        // One document describes one complex, which is what the per-complex pointer counts.
        profile_row_count: 1,
        profile_checksum_sha256: artifact.checksum_sha256.clone(),
        source_snapshot_id: required_row_string(row, "source_snapshot_id")?,
        // The snapshot actually scanned, not the Gold table's `iceberg_snapshot_id` column: that
        // column records the job argument the projection was built from and is carried verbatim
        // inside the artifact.
        iceberg_snapshot_id: provenance.iceberg_snapshot_id.clone(),
        published_at_utc: required_row_string(row, "published_at_utc")?,
        created,
    })
}

fn required_row_string(row: &JsonMap<String, JsonValue>, name: &str) -> anyhow::Result<String> {
    row.get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("Gold catalog row is missing {name}"))
}

fn count_rows(
    rows: &[JsonMap<String, JsonValue>],
    predicate: impl Fn(&JsonMap<String, JsonValue>) -> bool,
) -> anyhow::Result<u64> {
    u64::try_from(rows.iter().filter(|row| predicate(row)).count())
        .context("Gold row tally overflow")
}

fn write_summary(path: &Path, summary: &ProfileExportSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(summary)
        .context("failed to serialize the Gold profile export summary")?;
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
