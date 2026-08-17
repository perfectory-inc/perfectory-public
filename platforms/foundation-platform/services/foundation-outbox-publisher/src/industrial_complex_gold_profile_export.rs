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
//! The artifacts are written to the serving-derivative bucket, never the lakehouse bucket the rows
//! are read from (root ADR-0038). A fetchable artifact must not share a bucket with the canonical
//! collection sources.
//!
//! The command does not publish pointers. Producing the object and pointing at it are separate
//! failures, and were separated so a half-run cannot leave a pointer aimed at nothing.

mod iceberg_scan;
mod profile_document;

use std::{
    collections::BTreeSet,
    env,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use chrono::{SecondsFormat, Utc};
use foundation_outbox::{
    object_storage::R2ObjectStorageConfig,
    object_storage::{ObjectWriteMode, PutObjectRequest},
    EvidenceByteReader, FileObjectStorage, ObjectStorageService, PublishError, R2ObjectStorage,
};
use foundation_shared_kernel::{ObjectKey, ObjectUrlTemplate};
use futures_util::{stream, StreamExt as _, TryStreamExt as _};
use lakehouse_domain::GOLD_COMPLEX_CATALOG;
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotManifestList, LakehouseCatalogConfig,
};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use crate::serving_derivative_object_storage::{
    assert_serving_derivative_key, ServingDerivativeR2Config,
};
use profile_document::{GoldSnapshotProvenance, ProfileArtifact, PROFILE_SCHEMA_VERSION};

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_gold_profile_export_summary.v1";
const PROFILE_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const PROFILE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
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
    let output = ProfileOutput::from_config(&config.output)?;
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProfileOutputConfig {
    Local { root: PathBuf },
    R2,
}

#[derive(Clone)]
enum ProfileOutput {
    Local(FileObjectStorage),
    ServingDerivativeR2(Box<R2ObjectStorage>, String),
}

/// Reads the lakehouse objects the Iceberg snapshot points at.
#[derive(Clone)]
struct LakehouseObjectReader {
    storage: R2ObjectStorage,
    bucket_name: String,
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
            output: ProfileOutputConfig::from_env()?,
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

impl ProfileOutputConfig {
    fn from_env() -> anyhow::Result<Self> {
        let driver = optional_env(OUTPUT_STORAGE_DRIVER_ENV)?
            .unwrap_or_else(|| "local".to_owned())
            .to_ascii_lowercase();
        match driver.as_str() {
            "local" => Ok(Self::Local {
                root: PathBuf::from(required_env(OUTPUT_ROOT_ENV)?),
            }),
            "r2" => Ok(Self::R2),
            other => bail!("{OUTPUT_STORAGE_DRIVER_ENV} must be 'local' or 'r2', got '{other}'"),
        }
    }
}

impl ProfileOutput {
    fn from_config(config: &ProfileOutputConfig) -> anyhow::Result<Self> {
        match config {
            ProfileOutputConfig::Local { root } => {
                Ok(Self::Local(FileObjectStorage::new(root).with_context(
                    || format!("failed to configure local output root {}", root.display()),
                )?))
            }
            ProfileOutputConfig::R2 => {
                let config = ServingDerivativeR2Config::from_env()
                    .context("failed to configure the serving-derivative R2 output")?;
                let bucket_name = config.writer.bucket_name.clone();
                Ok(Self::ServingDerivativeR2(
                    Box::new(R2ObjectStorage::from_config(config.writer)),
                    bucket_name,
                ))
            }
        }
    }

    const fn storage_driver(&self) -> &'static str {
        match self {
            Self::Local(_) => "local",
            Self::ServingDerivativeR2(_, _) => "serving-derivative-r2",
        }
    }

    /// The bucket the artifacts were actually written to.
    ///
    /// Recorded in the summary so a run that reached the wrong bucket says so in its own output
    /// instead of having to be discovered by listing the buckets afterwards.
    fn output_bucket(&self) -> Option<&str> {
        match self {
            Self::Local(_) => None,
            Self::ServingDerivativeR2(_, bucket_name) => Some(bucket_name.as_str()),
        }
    }

    /// Writes one artifact create-only, reusing an existing object only when the bytes match.
    async fn write_create_only(&self, artifact: &ProfileArtifact) -> anyhow::Result<bool> {
        assert_serving_derivative_key(artifact.object_key.as_str())?;
        let request = PutObjectRequest {
            key: artifact.object_key.clone(),
            body: artifact.body.clone(),
            content_type: PROFILE_CONTENT_TYPE.to_owned(),
            cache_control: PROFILE_CACHE_CONTROL.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(artifact.checksum_sha256.clone()),
        };
        let outcome = match self {
            Self::Local(storage) => storage.put_object(request).await,
            Self::ServingDerivativeR2(storage, _) => storage.put_object(request).await,
        };
        match outcome {
            Ok(()) => Ok(true),
            Err(PublishError::ObjectAlreadyExists { key }) if key == artifact.object_key => {
                let stored = self.read_bytes(&artifact.object_key).await?;
                ensure!(
                    stored == artifact.body,
                    "profile object {} already exists with different exact bytes",
                    artifact.object_key
                );
                Ok(false)
            }
            Err(error) => Err(error)
                .with_context(|| format!("failed to write profile object {}", artifact.object_key)),
        }
    }

    async fn read_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Local(storage) => storage.read_evidence_bytes(key).await,
            Self::ServingDerivativeR2(storage, _) => storage.read_evidence_bytes(key).await,
        }
        .with_context(|| format!("failed to read the colliding profile object {key}"))
    }
}

impl LakehouseObjectReader {
    fn from_env() -> anyhow::Result<Self> {
        let config =
            R2ObjectStorageConfig::from_env().context("failed to configure lakehouse R2 reads")?;
        let bucket_name = config.bucket_name.clone();
        Ok(Self {
            storage: R2ObjectStorage::from_config(config),
            bucket_name,
        })
    }

    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        let key = self.object_key(location)?;
        self.storage
            .get_object_bytes(key.as_str())
            .await
            .with_context(|| format!("failed to read lakehouse object {location}"))
    }

    fn object_key(&self, location: &str) -> anyhow::Result<String> {
        lakehouse_object_key(location, self.bucket_name.as_str())
    }
}

/// Maps an Iceberg storage location onto a key in the configured lakehouse bucket.
///
/// A location in another bucket is a configuration error, not something to read past: the scan
/// would silently return the wrong table's rows.
fn lakehouse_object_key(location: &str, bucket_name: &str) -> anyhow::Result<String> {
    let rest = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))
        .with_context(|| format!("lakehouse location {location} is not an S3 URI"))?;
    let (bucket, key) = rest
        .split_once('/')
        .with_context(|| format!("lakehouse location {location} carries no object key"))?;
    ensure!(
        bucket == bucket_name,
        "lakehouse location {location} lives in bucket {bucket}, not the configured {bucket_name}"
    );
    ensure!(
        !key.is_empty(),
        "lakehouse location {location} carries no object key"
    );
    Ok(key.to_owned())
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

    let rows = scan_rows(lakehouse, snapshot).await?;
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
        output_bucket: output.output_bucket().map(ToOwned::to_owned),
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

struct ScannedRows {
    rows: Vec<JsonMap<String, JsonValue>>,
    data_file_count: u64,
    manifest_record_count: u64,
}

async fn scan_rows(
    lakehouse: &LakehouseObjectReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ScannedRows> {
    let manifest_list = lakehouse
        .read(snapshot.manifest_list_location.as_str())
        .await?;
    let manifest_locations = iceberg_scan::manifest_locations(&manifest_list)?;

    let mut data_files = Vec::new();
    for location in manifest_locations {
        let manifest = lakehouse.read(location.as_str()).await?;
        data_files.extend(iceberg_scan::data_files(&manifest)?);
    }

    let mut rows = Vec::new();
    let mut manifest_record_count = 0_u64;
    for data_file in &data_files {
        manifest_record_count = manifest_record_count
            .checked_add(data_file.record_count)
            .context("manifest record count overflow")?;
        let bytes = lakehouse.read(data_file.file_path.as_str()).await?;
        rows.extend(iceberg_scan::decode_rows(&GOLD_COMPLEX_CATALOG, bytes)?);
    }

    Ok(ScannedRows {
        rows,
        data_file_count: u64::try_from(data_files.len()).context("data file count overflow")?,
        manifest_record_count,
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
    let created = output.write_create_only(&artifact).await?;
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

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} is required"))
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
