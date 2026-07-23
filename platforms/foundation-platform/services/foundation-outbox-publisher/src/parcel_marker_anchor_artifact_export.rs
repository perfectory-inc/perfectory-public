//! Parcel marker anchor artifact export from Silver handoff shards.
//!
//! This command intentionally does not persist nationwide anchors into Postgres.
//! `PostGIS` is used as a scratch geometry calculator, and the durable output is
//! a versioned object-storage artifact plus a manifest.

use std::{
    env,
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use foundation_outbox::{
    object_storage::{ObjectWriteMode, PutObjectRequest},
    FileObjectStorage, ObjectStorageService, R2ObjectStorage,
};
use foundation_shared_kernel::events::catalog_v1::{
    CatalogEvent, ParcelMarkerAnchorSnapshotPublishedV1,
};
use futures_util::{stream, StreamExt, TryStreamExt};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};
use uuid::Uuid;

use crate::postgis_parcel_boundary_mirror_national_rebuild::{
    parse_stage_row, push_copy_csv_row, read_execution_evidence, validate_source_snapshot_id,
    ExecutionEvidence, HandoffObject,
};
use crate::r2_layout::{parcel_marker_anchor_artifact_prefix, PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT};

const ENTRY_SCHEMA_VERSION: &str = "foundation-platform.parcel_marker_anchor_artifact_entry.v1";
const REJECTION_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_artifact_rejection.v1";
const MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_artifact_manifest.v1";
const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_artifact_export_summary.v1";
const SOURCE_TABLE: &str = "silver.parcel_boundaries";
const SOURCE_SRID: i32 = 4326;
const WORKING_SRID: i32 = 5179;
const ANCHOR_SRID: i32 = 4326;
const DEFAULT_ALGORITHM_VERSION: &str = "postgis-st_maximuminscribedcircle-v1";
const DEFAULT_COPY_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const MIN_COPY_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_COPY_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 1;
const MAX_CONCURRENCY: usize = 16;
const R2_OBJECT_READ_MAX_ATTEMPTS: usize = 3;
const ANCHOR_JSONL_CONTENT_TYPE: &str = "application/x-ndjson; charset=utf-8";
const MANIFEST_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const ARTIFACT_CACHE_CONTROL: &str = "no-store";
const SNAPSHOT_PUBLISHED_EVENT_TYPE: &str = "catalog.parcel_marker_anchor.snapshot.published.v1";

/// Runs the parcel marker anchor artifact export.
pub async fn run() -> anyhow::Result<()> {
    let config = AnchorArtifactExportConfig::from_env()?;
    let evidence = read_execution_evidence(&config.execution_evidence_path)?;
    let storage =
        R2ObjectStorage::from_env().context("failed to configure R2 for Silver handoff reads")?;
    let export_run_id = Uuid::now_v7();
    let artifact_version = export_run_id.to_string();
    let output =
        AnchorArtifactOutput::from_config(&config.output, artifact_version.as_str()).await?;
    let mut conn = PgConnection::connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL for anchor artifact export")?;

    let report = execute_export(
        &mut conn,
        &storage,
        &output,
        &config,
        &evidence,
        export_run_id,
    )
    .await?;
    insert_anchor_snapshot_published_outbox_event(
        &mut conn,
        &report,
        config.public_base_url.as_str(),
    )
    .await?;

    if let Some(summary_path) = &config.summary_path {
        write_local_summary(summary_path, &report)?;
    }

    tracing::info!(
        export_run_id = %report.export_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        manifest_object_key = %report.manifest_object_key,
        artifact_row_count = report.artifact_row_count,
        artifact_object_count = report.artifact_object_count,
        "parcel marker anchor artifact export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AnchorArtifactExportConfig {
    database_url: String,
    execution_evidence_path: PathBuf,
    source_snapshot_id: String,
    algorithm_version: String,
    expected_row_count: Option<u64>,
    copy_buffer_bytes: usize,
    max_concurrency: usize,
    output: AnchorArtifactOutputConfig,
    public_base_url: String,
    summary_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AnchorArtifactOutputConfig {
    Local { root: PathBuf, prefix: String },
    R2 { prefix: String },
}

#[derive(Clone)]
enum AnchorArtifactOutput {
    Local(FileObjectStorage, String),
    R2(R2ObjectStorage, String),
}

#[derive(Debug, Serialize)]
struct AnchorArtifactManifest {
    schema_version: &'static str,
    artifact_version: String,
    generated_at_utc: String,
    export_run_id: Uuid,
    source_snapshot_id: String,
    source_table: &'static str,
    source_srid: String,
    working_srid: String,
    anchor_srid: String,
    algorithm: &'static str,
    algorithm_version: String,
    source_row_count: u64,
    artifact_object_count: u64,
    artifact_row_count: u64,
    rejected_object_count: u64,
    rejected_row_count: u64,
    checksum_sha256: String,
    objects: Vec<AnchorArtifactObject>,
    rejected_objects: Vec<AnchorArtifactRejectObject>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AnchorArtifactObject {
    shard_id: String,
    source_object_key: String,
    artifact_object_key: String,
    row_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AnchorArtifactRejectObject {
    shard_id: String,
    source_object_key: String,
    rejected_object_key: String,
    row_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Debug, Serialize)]
struct AnchorArtifactExportSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    export_run_id: Uuid,
    source_snapshot_id: String,
    execution_evidence_path: String,
    output_storage_driver: &'static str,
    output_object_prefix: String,
    manifest_object_key: String,
    artifact_object_count: u64,
    rejected_object_count: u64,
    expected_row_count: u64,
    artifact_row_count: u64,
    rejected_row_count: u64,
    checksum_sha256: String,
    object_results: Vec<AnchorArtifactObject>,
    rejected_object_results: Vec<AnchorArtifactRejectObject>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AnchorObjectExportResult {
    anchor_object: AnchorArtifactObject,
    reject_object: Option<AnchorArtifactRejectObject>,
}

struct AnchorSnapshotPublishedOutboxRecord {
    event_id: Uuid,
    event_type: String,
    payload: JsonValue,
}

impl AnchorArtifactExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_CONFIRM_EXPORT")?
                .unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_CONFIRM_EXPORT must be true");
        }

        let source_snapshot_id =
            required_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_SOURCE_SNAPSHOT_ID")?;
        validate_source_snapshot_id(source_snapshot_id.as_str())?;

        let algorithm_version =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_ALGORITHM_VERSION")?
                .unwrap_or_else(|| DEFAULT_ALGORITHM_VERSION.to_owned());
        validate_algorithm_version(algorithm_version.as_str())?;

        let expected_row_count =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_EXPECTED_ROW_COUNT")?
                .map(|value| parse_positive_u64(&value, "expected row count"))
                .transpose()?;

        let copy_buffer_bytes =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_COPY_BUFFER_BYTES")?
                .map(|value| parse_copy_buffer_bytes(&value))
                .transpose()?
                .unwrap_or(DEFAULT_COPY_BUFFER_BYTES);
        let max_concurrency =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_MAX_CONCURRENCY")?
                .map(|value| parse_max_concurrency(&value))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_CONCURRENCY);
        let public_base_url =
            required_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_PUBLIC_BASE_URL")?;
        validate_public_base_url(public_base_url.as_str())?;

        Ok(Self {
            database_url: required_env("DATABASE_URL")?,
            execution_evidence_path: PathBuf::from(required_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_EXECUTION_EVIDENCE_PATH",
            )?),
            source_snapshot_id,
            algorithm_version,
            expected_row_count,
            copy_buffer_bytes,
            max_concurrency,
            output: AnchorArtifactOutputConfig::from_env()?,
            public_base_url,
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
        })
    }
}

impl AnchorArtifactOutputConfig {
    fn from_env() -> anyhow::Result<Self> {
        let driver = optional_env(
            "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_OUTPUT_STORAGE_DRIVER",
        )?
        .unwrap_or_else(|| "local".to_owned())
        .to_ascii_lowercase();
        let prefix =
            required_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_OUTPUT_OBJECT_PREFIX")?;
        if prefix != PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT {
            bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_OUTPUT_OBJECT_PREFIX must be {PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT}"
            );
        }

        match driver.as_str() {
            "local" => Ok(Self::Local {
                root: PathBuf::from(required_env(
                    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_OUTPUT_ROOT",
                )?),
                prefix,
            }),
            "r2" => Ok(Self::R2 { prefix }),
            "" => bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_OUTPUT_STORAGE_DRIVER must not be empty"
            ),
            other => bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ARTIFACT_OUTPUT_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
            ),
        }
    }
}

impl AnchorArtifactOutput {
    async fn from_config(
        config: &AnchorArtifactOutputConfig,
        artifact_version: &str,
    ) -> anyhow::Result<Self> {
        if config.prefix() != PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT {
            bail!("anchor artifact output prefix must be {PARCEL_MARKER_ANCHOR_ARTIFACT_ROOT}");
        }
        let prefix = parcel_marker_anchor_artifact_prefix(artifact_version)?;
        match config {
            AnchorArtifactOutputConfig::Local { root, .. } => Ok(Self::Local(
                FileObjectStorage::new(root).with_context(|| {
                    format!("failed to configure local output root {}", root.display())
                })?,
                prefix,
            )),
            AnchorArtifactOutputConfig::R2 { .. } => Ok(Self::R2(
                R2ObjectStorage::from_env()
                    .context("failed to configure R2 anchor artifact output")?,
                prefix,
            )),
        }
    }

    const fn storage_driver(&self) -> &'static str {
        match self {
            Self::Local(_, _) => "local",
            Self::R2(_, _) => "r2",
        }
    }

    fn prefix(&self) -> &str {
        match self {
            Self::Local(_, prefix) | Self::R2(_, prefix) => prefix,
        }
    }

    fn object_key(&self, suffix: &str) -> anyhow::Result<String> {
        validate_object_key_suffix(suffix)?;
        self.object_key_path(suffix)
    }

    fn object_key_path(&self, suffix: &str) -> anyhow::Result<String> {
        validate_object_key(suffix)?;
        let key = format!("{}/{}", self.prefix(), suffix);
        validate_object_key(key.as_str())?;
        Ok(key)
    }

    async fn put_object(
        &self,
        key: String,
        body: Vec<u8>,
        content_type: &'static str,
    ) -> anyhow::Result<()> {
        let request = PutObjectRequest {
            key,
            sha256: Some(sha256_hex(&body)),
            body,
            content_type: content_type.to_owned(),
            cache_control: ARTIFACT_CACHE_CONTROL.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
        };
        match self {
            Self::Local(storage, _) => storage
                .put_object(request)
                .await
                .context("failed to write local anchor artifact object"),
            Self::R2(storage, _) => storage
                .put_object(request)
                .await
                .context("failed to write R2 anchor artifact object"),
        }
    }
}

impl AnchorArtifactOutputConfig {
    fn prefix(&self) -> &str {
        match self {
            Self::Local { prefix, .. } | Self::R2 { prefix } => prefix,
        }
    }
}

async fn execute_export(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    output: &AnchorArtifactOutput,
    config: &AnchorArtifactExportConfig,
    evidence: &ExecutionEvidence,
    export_run_id: Uuid,
) -> anyhow::Result<AnchorArtifactExportSummary> {
    if let Some(expected_row_count) = config.expected_row_count {
        if expected_row_count != evidence.expected_row_count {
            bail!(
                "configured expected row count {expected_row_count} does not match evidence {}",
                evidence.expected_row_count
            );
        }
    }

    let export_results = export_anchor_objects(conn, storage, output, config, evidence).await?;
    let mut object_results = Vec::with_capacity(export_results.len());
    let mut rejected_object_results = Vec::new();
    let mut artifact_row_count = 0_u64;
    let mut rejected_row_count = 0_u64;
    let mut manifest_digest = Sha256::new();

    for result in export_results {
        manifest_digest.update(result.anchor_object.checksum_sha256.as_bytes());
        artifact_row_count = artifact_row_count
            .checked_add(result.anchor_object.row_count)
            .context("artifact row count overflow")?;
        object_results.push(result.anchor_object);
        if let Some(reject_object) = result.reject_object {
            manifest_digest.update(reject_object.checksum_sha256.as_bytes());
            rejected_row_count = rejected_row_count
                .checked_add(reject_object.row_count)
                .context("rejected row count overflow")?;
            rejected_object_results.push(reject_object);
        }
    }

    let accounted_row_count = artifact_row_count
        .checked_add(rejected_row_count)
        .context("accounted row count overflow")?;
    if accounted_row_count != evidence.expected_row_count {
        bail!(
            "anchor artifact accounted row count mismatch: expected={} artifact={artifact_row_count} rejected={rejected_row_count}",
            evidence.expected_row_count
        );
    }

    let checksum_sha256 = hex_lower(&manifest_digest.finalize());
    let manifest = AnchorArtifactManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        artifact_version: export_run_id.to_string(),
        generated_at_utc: Utc::now().to_rfc3339(),
        export_run_id,
        source_snapshot_id: config.source_snapshot_id.clone(),
        source_table: SOURCE_TABLE,
        source_srid: format!("EPSG:{SOURCE_SRID}"),
        working_srid: format!("EPSG:{WORKING_SRID}"),
        anchor_srid: format!("EPSG:{ANCHOR_SRID}"),
        algorithm: "polylabel",
        algorithm_version: config.algorithm_version.clone(),
        source_row_count: evidence.expected_row_count,
        artifact_object_count: u64::try_from(object_results.len())
            .context("artifact object count overflow")?,
        artifact_row_count,
        rejected_object_count: u64::try_from(rejected_object_results.len())
            .context("rejected object count overflow")?,
        rejected_row_count,
        checksum_sha256: checksum_sha256.clone(),
        objects: object_results.clone(),
        rejected_objects: rejected_object_results.clone(),
    };
    let manifest_object_key = output.object_key("manifest.json")?;
    let manifest_body =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize anchor manifest")?;
    output
        .put_object(
            manifest_object_key.clone(),
            manifest_body,
            MANIFEST_CONTENT_TYPE,
        )
        .await?;

    Ok(AnchorArtifactExportSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        export_run_id,
        source_snapshot_id: config.source_snapshot_id.clone(),
        execution_evidence_path: config.execution_evidence_path.display().to_string(),
        output_storage_driver: output.storage_driver(),
        output_object_prefix: output.prefix().to_owned(),
        manifest_object_key,
        artifact_object_count: u64::try_from(object_results.len())
            .context("artifact object count overflow")?,
        rejected_object_count: u64::try_from(rejected_object_results.len())
            .context("rejected object count overflow")?,
        expected_row_count: evidence.expected_row_count,
        artifact_row_count,
        rejected_row_count,
        checksum_sha256,
        object_results,
        rejected_object_results,
    })
}

async fn export_anchor_objects(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    output: &AnchorArtifactOutput,
    config: &AnchorArtifactExportConfig,
    evidence: &ExecutionEvidence,
) -> anyhow::Result<Vec<AnchorObjectExportResult>> {
    if config.max_concurrency == 1 {
        create_stage_table(conn).await?;
        let mut object_results = Vec::with_capacity(evidence.objects.len());
        for object in &evidence.objects {
            object_results.push(export_anchor_object(conn, storage, output, config, object).await?);
        }
        return Ok(object_results);
    }

    let storage = storage.clone();
    let output = output.clone();
    let config = config.clone();
    let mut indexed_results = stream::iter(evidence.objects.iter().cloned().enumerate())
        .map(|(index, object)| {
            let storage = storage.clone();
            let output = output.clone();
            let config = config.clone();
            async move {
                let mut conn = PgConnection::connect(&config.database_url)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to connect to PostgreSQL for shard {}",
                            object.shard_id
                        )
                    })?;
                create_stage_table(&mut conn).await?;
                let result =
                    export_anchor_object(&mut conn, &storage, &output, &config, &object).await?;
                Ok::<_, anyhow::Error>((index, result))
            }
        })
        .buffer_unordered(config.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    indexed_results.sort_by_key(|(index, _)| *index);
    Ok(indexed_results
        .into_iter()
        .map(|(_, result)| result)
        .collect())
}

async fn create_stage_table(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_marker_anchor_artifact_stage (
             pnu text NOT NULL,
             boundary_id text NOT NULL,
             source_object_key text NOT NULL,
             geometry_wkb_hex text NOT NULL,
             geometry_checksum_sha256 text NOT NULL,
             properties jsonb NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create parcel marker anchor artifact stage")?;
    Ok(())
}

async fn export_anchor_object(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    output: &AnchorArtifactOutput,
    config: &AnchorArtifactExportConfig,
    object: &HandoffObject,
) -> anyhow::Result<AnchorObjectExportResult> {
    conn.execute("TRUNCATE TABLE parcel_marker_anchor_artifact_stage")
        .await
        .context("failed to truncate parcel marker anchor artifact stage")?;

    let object_bytes = read_r2_object_bytes_with_retries(object.object_key.as_str(), || {
        let storage = storage.clone();
        let object_key = object.object_key.clone();
        async move {
            storage
                .get_object_bytes_range_retried(object_key.as_str())
                .await
                .map_err(anyhow::Error::from)
        }
    })
    .await
    .with_context(|| format!("failed to read R2 handoff object {}", object.object_key))?;
    let copied_row_count =
        copy_object_to_stage(conn, object, &object_bytes, config.copy_buffer_bytes).await?;
    if copied_row_count != object.row_count {
        bail!(
            "handoff object {} row count mismatch: expected={} actual={copied_row_count}",
            object.object_key,
            object.row_count
        );
    }

    let artifact_body = copy_anchors_to_jsonl(conn, config).await?;
    let row_count = count_jsonl_rows(&artifact_body)?;
    if row_count > copied_row_count {
        bail!(
            "anchor artifact row count overflow for {}: copied={copied_row_count} exported={row_count}",
            object.object_key
        );
    }
    let rejected_row_count = copied_row_count
        .checked_sub(row_count)
        .context("rejected row count underflow")?;

    let checksum_sha256 = sha256_hex(&artifact_body);
    let size_bytes = u64::try_from(artifact_body.len()).context("artifact byte size overflow")?;
    let artifact_object_key = output.object_key(&format!("{}.jsonl", object.shard_id))?;
    output
        .put_object(
            artifact_object_key.clone(),
            artifact_body,
            ANCHOR_JSONL_CONTENT_TYPE,
        )
        .await?;

    tracing::info!(
        shard_id = %object.shard_id,
        source_object_key = %object.object_key,
        artifact_object_key = %artifact_object_key,
        row_count,
        size_bytes,
        "exported parcel marker anchor artifact object"
    );

    let anchor_object = AnchorArtifactObject {
        shard_id: object.shard_id.clone(),
        source_object_key: object.object_key.clone(),
        artifact_object_key,
        row_count,
        size_bytes,
        checksum_sha256,
    };

    let reject_object = if rejected_row_count == 0 {
        None
    } else {
        let rejected_body = copy_rejected_rows_to_jsonl(conn).await?;
        let rejected_count = count_jsonl_rows(&rejected_body)?;
        if rejected_count != rejected_row_count {
            bail!(
                "anchor rejection row count mismatch for {}: expected={rejected_row_count} actual={rejected_count}",
                object.object_key
            );
        }
        let rejected_object_key =
            output.object_key_path(&format!("rejected/{}.jsonl", object.shard_id))?;
        let rejected_checksum_sha256 = sha256_hex(&rejected_body);
        let rejected_size_bytes =
            u64::try_from(rejected_body.len()).context("rejected artifact byte size overflow")?;
        output
            .put_object(
                rejected_object_key.clone(),
                rejected_body,
                ANCHOR_JSONL_CONTENT_TYPE,
            )
            .await?;
        Some(AnchorArtifactRejectObject {
            shard_id: object.shard_id.clone(),
            source_object_key: object.object_key.clone(),
            rejected_object_key,
            row_count: rejected_count,
            size_bytes: rejected_size_bytes,
            checksum_sha256: rejected_checksum_sha256,
        })
    };

    Ok(AnchorObjectExportResult {
        anchor_object,
        reject_object,
    })
}

async fn copy_object_to_stage(
    conn: &mut PgConnection,
    object: &HandoffObject,
    object_bytes: &[u8],
    copy_buffer_bytes: usize,
) -> anyhow::Result<u64> {
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_marker_anchor_artifact_stage
             (pnu, boundary_id, source_object_key, geometry_wkb_hex,
              geometry_checksum_sha256, properties)
             FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '\"', ESCAPE '\"', NULL '\\N')",
        )
        .await
        .context("failed to start COPY into parcel marker anchor artifact stage")?;
    let mut buffer = Vec::with_capacity(copy_buffer_bytes.min(MAX_COPY_BUFFER_BYTES));
    let mut row_count = 0_u64;

    for (index, raw_line) in object_bytes.split(|byte| *byte == b'\n').enumerate() {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            continue;
        }
        let line_number = u64::try_from(index + 1).context("handoff line number overflow")?;
        let row = parse_stage_row(line, object.object_key.as_str(), line_number)?;
        push_copy_csv_row(&mut buffer, &row, object.object_key.as_str());
        row_count = row_count
            .checked_add(1)
            .context("COPY row count overflow")?;
        if buffer.len() >= copy_buffer_bytes {
            copy.send(buffer.as_slice())
                .await
                .context("COPY send failed")?;
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        copy.send(buffer.as_slice())
            .await
            .context("COPY send failed")?;
    }
    let copied = copy.finish().await.context("COPY finish failed")?;
    if copied != row_count {
        bail!("COPY reported {copied} rows but parser saw {row_count} rows");
    }
    Ok(copied)
}

async fn read_r2_object_bytes_with_retries<Operation, Fut>(
    object_key: &str,
    mut operation: Operation,
) -> anyhow::Result<Vec<u8>>
where
    Operation: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Vec<u8>>>,
{
    for attempt in 1..=R2_OBJECT_READ_MAX_ATTEMPTS {
        match operation().await {
            Ok(body) => return Ok(body),
            Err(error) if attempt < R2_OBJECT_READ_MAX_ATTEMPTS => {
                tracing::warn!(
                    object_key,
                    attempt,
                    max_attempts = R2_OBJECT_READ_MAX_ATTEMPTS,
                    error = %error,
                    "retrying R2 object read after transient failure"
                );
                tokio::time::sleep(r2_object_read_retry_delay(attempt)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("R2 read retry loop always returns on success or final failure")
}

const fn r2_object_read_retry_delay(attempt: usize) -> Duration {
    #[cfg(test)]
    {
        let _ = attempt;
        Duration::from_millis(0)
    }
    #[cfg(not(test))]
    {
        match attempt {
            0 | 1 => Duration::from_secs(1),
            2 => Duration::from_secs(2),
            _ => Duration::from_secs(4),
        }
    }
}

async fn copy_anchors_to_jsonl(
    conn: &mut PgConnection,
    config: &AnchorArtifactExportConfig,
) -> anyhow::Result<Vec<u8>> {
    let statement = anchor_copy_out_statement(
        config.source_snapshot_id.as_str(),
        config.algorithm_version.as_str(),
    );
    let mut body = Vec::new();
    {
        let mut stream = conn
            .copy_out_raw(statement.as_str())
            .await
            .context("failed to start COPY anchor artifact output")?;
        while let Some(chunk) = stream
            .try_next()
            .await
            .context("failed to read COPY anchor artifact output")?
        {
            body.extend_from_slice(&chunk);
        }
    }
    Ok(body)
}

async fn copy_rejected_rows_to_jsonl(conn: &mut PgConnection) -> anyhow::Result<Vec<u8>> {
    let statement = rejected_copy_out_statement();
    let mut body = Vec::new();
    {
        let mut stream = conn
            .copy_out_raw(statement.as_str())
            .await
            .context("failed to start COPY anchor artifact rejection output")?;
        while let Some(chunk) = stream
            .try_next()
            .await
            .context("failed to read COPY anchor artifact rejection output")?
        {
            body.extend_from_slice(&chunk);
        }
    }
    Ok(body)
}

fn anchor_copy_out_statement(source_snapshot_id: &str, algorithm_version: &str) -> String {
    let source_snapshot_id = sql_string_literal(source_snapshot_id);
    let algorithm_version = sql_string_literal(algorithm_version);
    format!(
        "COPY (
             WITH geometries AS (
                 SELECT
                     pnu,
                     boundary_id,
                     source_object_key,
                     geometry_checksum_sha256,
                     ST_Multi(
                         ST_Transform(
                             ST_CollectionExtract(
                                 ST_MakeValid(
                                     ST_SetSRID(ST_GeomFromWKB(decode(geometry_wkb_hex, 'hex')), {SOURCE_SRID})
                                 ),
                                 3
                             ),
                             {WORKING_SRID}
                         )
                     ) AS geom_working
                 FROM parcel_marker_anchor_artifact_stage
             ),
             valid_rows AS (
                 SELECT *
                 FROM geometries
                 WHERE ST_SRID(geom_working) = {WORKING_SRID}
                   AND ST_IsValid(geom_working)
                   AND NOT ST_IsEmpty(geom_working)
                   AND ST_Area(geom_working) > 0
             ),
             anchors AS (
                 SELECT
                     pnu,
                     boundary_id,
                     source_object_key,
                     geometry_checksum_sha256,
                     ST_Transform((ST_MaximumInscribedCircle(geom_working)).center, {ANCHOR_SRID}) AS anchor_point
                 FROM valid_rows
             )
             SELECT json_build_object(
                 'schema_version', '{ENTRY_SCHEMA_VERSION}',
                 'pnu', pnu,
                 'anchor_lng', ST_X(anchor_point),
                 'anchor_lat', ST_Y(anchor_point),
                 'anchor_srid', 'EPSG:{ANCHOR_SRID}',
                 'algorithm', 'polylabel',
                 'algorithm_version', {algorithm_version},
                 'source_snapshot_id', {source_snapshot_id},
                 'source_table', '{SOURCE_TABLE}',
                 'source_row_id', boundary_id,
                 'source_object_key', source_object_key,
                 'source_geometry_checksum_sha256', geometry_checksum_sha256
             )::text
             FROM anchors
             ORDER BY pnu, boundary_id
         ) TO STDOUT"
    )
}

fn rejected_copy_out_statement() -> String {
    format!(
        "COPY (
             WITH geometries AS (
                 SELECT
                     pnu,
                     boundary_id,
                     source_object_key,
                     geometry_checksum_sha256,
                     properties,
                     ST_Multi(
                         ST_Transform(
                             ST_CollectionExtract(
                                 ST_MakeValid(
                                     ST_SetSRID(ST_GeomFromWKB(decode(geometry_wkb_hex, 'hex')), {SOURCE_SRID})
                                 ),
                                 3
                             ),
                             {WORKING_SRID}
                         )
                     ) AS geom_working
                 FROM parcel_marker_anchor_artifact_stage
             ),
             classified AS (
                 SELECT
                     *,
                     CASE
                         WHEN ST_SRID(geom_working) <> {WORKING_SRID} THEN 'invalid_srid'
                         WHEN NOT ST_IsValid(geom_working) THEN 'invalid_geometry'
                         WHEN ST_IsEmpty(geom_working) THEN 'empty_geometry'
                         WHEN ST_Area(geom_working) <= 0 THEN 'nonpositive_area'
                         ELSE NULL
                     END AS rejection_reason
                 FROM geometries
             )
             SELECT json_build_object(
                 'schema_version', '{REJECTION_SCHEMA_VERSION}',
                 'pnu', pnu,
                 'source_row_id', boundary_id,
                 'source_object_key', source_object_key,
                 'source_geometry_checksum_sha256', geometry_checksum_sha256,
                 'rejection_reason', rejection_reason,
                 'source_properties', properties
             )::text
             FROM classified
             WHERE rejection_reason IS NOT NULL
             ORDER BY pnu, boundary_id
         ) TO STDOUT"
    )
}

fn count_jsonl_rows(bytes: &[u8]) -> anyhow::Result<u64> {
    let count = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .count();
    u64::try_from(count).context("JSONL row count overflow")
}

fn write_local_summary(path: &Path, report: &AnchorArtifactExportSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report)
        .context("failed to serialize anchor artifact export summary")?;
    std::fs::write(path, payload).with_context(|| {
        format!(
            "failed to write anchor artifact export summary {}",
            path.display()
        )
    })
}

async fn insert_anchor_snapshot_published_outbox_event(
    conn: &mut PgConnection,
    summary: &AnchorArtifactExportSummary,
    public_base_url: &str,
) -> anyhow::Result<()> {
    let record = build_anchor_snapshot_published_outbox_record(summary, public_base_url)?;
    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at)
         VALUES ($1, $2, $3, now())",
    )
    .bind(record.event_id)
    .bind(record.event_type.as_str())
    .bind(&record.payload)
    .execute(conn)
    .await
    .context("failed to insert parcel marker anchor snapshot published outbox event")?;
    Ok(())
}

fn build_anchor_snapshot_published_outbox_record(
    summary: &AnchorArtifactExportSummary,
    public_base_url: &str,
) -> anyhow::Result<AnchorSnapshotPublishedOutboxRecord> {
    let published_at = DateTime::parse_from_rfc3339(summary.generated_at_utc.as_str())
        .context("anchor artifact summary generated_at_utc must be RFC3339")?
        .with_timezone(&Utc);
    let event =
        CatalogEvent::ParcelMarkerAnchorSnapshotPublished(ParcelMarkerAnchorSnapshotPublishedV1 {
            schema_version: 1,
            anchor_snapshot_id: format!("anchor-snapshot-{}", summary.export_run_id),
            source_geometry_version: summary.source_snapshot_id.clone(),
            artifact_manifest_url: artifact_manifest_url(
                public_base_url,
                summary.manifest_object_key.as_str(),
            )?,
            artifact_checksum_sha256: summary.checksum_sha256.clone(),
            row_count: summary.artifact_row_count,
            published_at,
        });
    let payload = serde_json::to_value(event).context("failed to encode anchor snapshot event")?;
    let event_type = payload_type_tag(&payload)?;
    Ok(AnchorSnapshotPublishedOutboxRecord {
        event_id: summary.export_run_id,
        event_type,
        payload,
    })
}

fn payload_type_tag(payload: &JsonValue) -> anyhow::Result<String> {
    let Some(event_type) = payload.get("type").and_then(JsonValue::as_str) else {
        bail!("anchor snapshot event serialization missing type tag");
    };
    if event_type != SNAPSHOT_PUBLISHED_EVENT_TYPE {
        bail!("anchor snapshot event type mismatch: {event_type}");
    }
    Ok(event_type.to_owned())
}

fn artifact_manifest_url(
    public_base_url: &str,
    manifest_object_key: &str,
) -> anyhow::Result<String> {
    validate_public_base_url(public_base_url)?;
    validate_object_key(manifest_object_key)?;
    Ok(format!(
        "{}/{}",
        public_base_url.trim_end_matches('/'),
        manifest_object_key
    ))
}

fn validate_algorithm_version(value: &str) -> anyhow::Result<()> {
    if value.len() < 2
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b':' | b'-')
        })
        || !value.as_bytes()[0].is_ascii_lowercase()
    {
        bail!("algorithm version must match the parcel marker anchor artifact contract");
    }
    Ok(())
}

fn validate_public_base_url(value: &str) -> anyhow::Result<()> {
    let trimmed = value.trim();
    if trimmed != value || trimmed.is_empty() {
        bail!("public base URL must not be empty or padded");
    }
    if !(trimmed.starts_with("https://") || trimmed.starts_with("http://")) {
        bail!("public base URL must be absolute HTTP(S)");
    }
    if trimmed.contains('?') || trimmed.contains('#') {
        bail!("public base URL must not include query or fragment");
    }
    Ok(())
}

fn validate_object_key_suffix(value: &str) -> anyhow::Result<()> {
    if value.is_empty() || value.contains('/') || value.contains('\\') || value.contains("..") {
        bail!("object key suffix must be a single safe relative segment");
    }
    Ok(())
}

fn validate_object_key(value: &str) -> anyhow::Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("object key must not be empty");
    }
    if trimmed != value {
        bail!("object key must not contain leading or trailing whitespace");
    }
    if trimmed.starts_with('/') || trimmed.contains('\\') || trimmed.contains("..") {
        bail!("object key must be a safe provider-relative path");
    }
    if trimmed == "gold/manifest.json" {
        bail!("object key must not target the runtime gold/manifest.json pointer");
    }
    Ok(())
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn parse_positive_u64(value: &str, label: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{label} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{label} must be greater than zero");
    }
    Ok(parsed)
}

fn parse_copy_buffer_bytes(value: &str) -> anyhow::Result<usize> {
    let parsed = value
        .parse::<usize>()
        .context("copy buffer bytes must be a positive integer")?;
    if !(MIN_COPY_BUFFER_BYTES..=MAX_COPY_BUFFER_BYTES).contains(&parsed) {
        bail!(
            "copy buffer bytes must be between {MIN_COPY_BUFFER_BYTES} and {MAX_COPY_BUFFER_BYTES}"
        );
    }
    Ok(parsed)
}

fn parse_max_concurrency(value: &str) -> anyhow::Result<usize> {
    let parsed = value
        .parse::<usize>()
        .context("max concurrency must be a positive integer")?;
    if !(1..=MAX_CONCURRENCY).contains(&parsed) {
        bail!("max concurrency must be between 1 and {MAX_CONCURRENCY}");
    }
    Ok(parsed)
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.ok_or_else(|| anyhow::anyhow!("{name} is required"))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_lower(&digest)
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}

#[cfg(test)]
mod tests {
    use super::{
        anchor_copy_out_statement, build_anchor_snapshot_published_outbox_record, count_jsonl_rows,
        parse_copy_buffer_bytes, parse_max_concurrency, read_r2_object_bytes_with_retries,
        rejected_copy_out_statement, sql_string_literal, validate_algorithm_version,
        validate_object_key, validate_object_key_suffix, AnchorArtifactExportSummary,
        AnchorArtifactObject, AnchorArtifactOutput, AnchorArtifactOutputConfig,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn object_keys_are_provider_relative_and_do_not_target_runtime_pointer() -> anyhow::Result<()> {
        assert!(crate::r2_layout::parcel_marker_anchor_artifact_prefix(
            "018f0000-0000-7000-8000-000000000001"
        )
        .is_ok());
        assert!(crate::r2_layout::parcel_marker_anchor_artifact_prefix("2026-05-25").is_err());
        validate_object_key_suffix("shard-000001.jsonl")?;
        assert!(validate_object_key("/gold/parcel-marker-anchors/artifacts").is_err());
        assert!(validate_object_key("gold/../manifest.json").is_err());
        assert!(validate_object_key("gold/manifest.json").is_err());
        assert!(validate_object_key_suffix("nested/shard.jsonl").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn output_prefix_is_derived_from_immutable_artifact_version() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-anchor-output-prefix-{}",
            uuid::Uuid::now_v7()
        ));
        let output = AnchorArtifactOutput::from_config(
            &AnchorArtifactOutputConfig::Local {
                root: root.clone(),
                prefix: "gold/parcel-marker-anchors/artifacts".to_owned(),
            },
            "018f0000-0000-7000-8000-000000000001",
        )
        .await?;

        assert_eq!(
            output.prefix(),
            "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn immutable_anchor_artifact_refuses_overwrite() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-anchor-create-only-{}",
            uuid::Uuid::now_v7()
        ));
        let output = AnchorArtifactOutput::from_config(
            &AnchorArtifactOutputConfig::Local {
                root: root.clone(),
                prefix: "gold/parcel-marker-anchors/artifacts".to_owned(),
            },
            "018f0000-0000-7000-8000-000000000001",
        )
        .await?;
        let key = output.object_key("manifest.json")?;

        output
            .put_object(
                key.clone(),
                br#"{"schema_version":1}"#.to_vec(),
                "application/json",
            )
            .await?;
        let duplicate = output
            .put_object(key, br#"{"schema_version":1}"#.to_vec(), "application/json")
            .await;

        std::fs::remove_dir_all(root)?;
        assert!(
            duplicate.is_err(),
            "immutable anchor artifact was overwritten"
        );
        Ok(())
    }

    #[test]
    fn sql_literals_escape_quotes() {
        assert_eq!(
            sql_string_literal("iceberg:snapshot-1"),
            "'iceberg:snapshot-1'"
        );
        assert_eq!(sql_string_literal("a'b"), "'a''b'");
    }

    #[test]
    fn anchor_copy_statement_uses_temp_stage_and_copy_to_stdout() {
        let statement = anchor_copy_out_statement(
            "iceberg:parcel-boundaries-snapshot-001",
            "postgis-st_maximuminscribedcircle-v1",
        );
        assert!(statement.contains("COPY ("));
        assert!(statement.contains("parcel_marker_anchor_artifact_stage"));
        assert!(statement.contains("ST_MaximumInscribedCircle"));
        assert!(statement.contains("TO STDOUT"));
        assert!(!statement.contains("INSERT INTO catalog.parcel_marker_anchor"));
    }

    #[test]
    fn rejected_copy_statement_preserves_unanchorable_rows() {
        let statement = rejected_copy_out_statement();
        assert!(statement.contains("parcel_marker_anchor_artifact_stage"));
        assert!(statement.contains("rejection_reason"));
        assert!(statement.contains("empty_geometry"));
        assert!(statement.contains("nonpositive_area"));
        assert!(statement.contains("TO STDOUT"));
    }

    #[test]
    fn algorithm_version_and_copy_buffer_are_bounded() -> anyhow::Result<()> {
        validate_algorithm_version("postgis-st_maximuminscribedcircle-v1")?;
        assert!(validate_algorithm_version("PostGIS").is_err());
        assert_eq!(parse_copy_buffer_bytes("1048576")?, 1_048_576);
        assert!(parse_copy_buffer_bytes("1024").is_err());
        Ok(())
    }

    #[test]
    fn national_anchor_export_concurrency_is_explicitly_bounded() -> anyhow::Result<()> {
        assert_eq!(parse_max_concurrency("1")?, 1);
        assert_eq!(parse_max_concurrency("16")?, 16);
        assert!(parse_max_concurrency("0").is_err());
        assert!(parse_max_concurrency("17").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn r2_object_read_retries_transient_stream_failures() -> anyhow::Result<()> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = attempts.clone();

        let body = read_r2_object_bytes_with_retries("silver-handoff/shard-0083.jsonl", || {
            let attempts = attempts.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    anyhow::bail!("streaming error");
                }
                Ok(b"{\"pnu\":\"9999900101100010001\"}\n".to_vec())
            }
        })
        .await?;

        assert_eq!(body, b"{\"pnu\":\"9999900101100010001\"}\n");
        assert_eq!(observed_attempts.load(Ordering::SeqCst), 3);
        Ok(())
    }

    #[test]
    fn jsonl_rows_are_counted_by_newline() -> anyhow::Result<()> {
        assert_eq!(count_jsonl_rows(b"{\"a\":1}\n{\"a\":2}\n")?, 2);
        assert_eq!(count_jsonl_rows(b"")?, 0);
        Ok(())
    }

    #[test]
    fn outbox_record_is_derived_from_export_summary() -> anyhow::Result<()> {
        let export_run_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000001")?;
        let summary = AnchorArtifactExportSummary {
            schema_version: "foundation-platform.parcel_marker_anchor_artifact_export_summary.v1",
            generated_at_utc: "2026-05-28T12:00:00Z".to_owned(),
            export_run_id,
            source_snapshot_id: "iceberg:parcel-boundaries-snapshot-001".to_owned(),
            execution_evidence_path: "target/evidence.json".to_owned(),
            output_storage_driver: "r2",
            output_object_prefix:
                "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001".to_owned(),
            manifest_object_key:
                "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json"
                    .to_owned(),
            artifact_object_count: 1,
            rejected_object_count: 0,
            expected_row_count: 2,
            artifact_row_count: 2,
            rejected_row_count: 0,
            checksum_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            object_results: vec![AnchorArtifactObject {
                shard_id: "shard-000001".to_owned(),
                source_object_key: "silver/parcel-boundaries/shard-000001.jsonl".to_owned(),
                artifact_object_key:
                    "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/shard-000001.jsonl"
                        .to_owned(),
                row_count: 2,
                size_bytes: 512,
                checksum_sha256:
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
            }],
            rejected_object_results: vec![],
        };

        let record = build_anchor_snapshot_published_outbox_record(
            &summary,
            "https://foundation-platform.example.com/artifacts",
        )?;

        assert_eq!(record.event_id, export_run_id);
        assert_eq!(
            record.event_type,
            "catalog.parcel_marker_anchor.snapshot.published.v1"
        );
        assert_eq!(
            record.payload["anchor_snapshot_id"],
            "anchor-snapshot-018f0000-0000-7000-8000-000000000001"
        );
        assert_eq!(
            record.payload["source_geometry_version"],
            "iceberg:parcel-boundaries-snapshot-001"
        );
        assert_eq!(
            record.payload["artifact_manifest_url"],
            "https://foundation-platform.example.com/artifacts/gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json"
        );
        assert_eq!(
            record.payload["artifact_checksum_sha256"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(record.payload["row_count"], 2);
        assert_eq!(record.payload["published_at"], "2026-05-28T12:00:00Z");
        Ok(())
    }
}
