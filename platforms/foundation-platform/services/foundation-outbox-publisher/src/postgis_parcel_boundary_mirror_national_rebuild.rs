//! National `PostGIS` rebuild command for `silver.parcel_boundaries` R2 handoff shards.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::Utc;
use foundation_outbox::R2ObjectStorage;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{Connection, Executor, PgConnection};
use uuid::Uuid;

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.postgis_parcel_boundary_mirror_national_rebuild_summary.v1";
const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.silver_gold_national_promotion_execution.v1";
const SOURCE_TABLE: &str = "silver.parcel_boundaries";
const TARGET_SRID: i32 = 5179;
const SOURCE_SRID: i32 = 4326;
const DEFAULT_COPY_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const MIN_COPY_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_COPY_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BOUNDED_OBJECT_COUNT: u64 = 1;
const DEFAULT_MAX_BOUNDED_ROW_COUNT: u64 = 1_000_000;

/// Runs the national `PostGIS` parcel-boundary mirror rebuild.
pub async fn run() -> anyhow::Result<()> {
    let config = RebuildConfig::from_env()?;
    let evidence = read_execution_evidence(&config.execution_evidence_path)?;
    let rebuild_run_id = Uuid::now_v7();
    let mut conn = PgConnection::connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL for PostGIS mirror rebuild")?;
    let storage =
        R2ObjectStorage::from_env().context("failed to configure R2 for PostGIS mirror rebuild")?;

    let report =
        match execute_rebuild(&mut conn, &storage, &config, &evidence, rebuild_run_id).await {
            Ok(report) => report,
            Err(error) => {
                let _ = mark_rebuild_failed(
                    &mut conn,
                    rebuild_run_id,
                    config.source_snapshot_id.as_str(),
                    &error.to_string(),
                )
                .await;
                return Err(error);
            }
        };

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, &report)?;
    }

    tracing::info!(
        rebuild_run_id = %report.rebuild_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        object_count = report.object_count,
        loaded_row_count = report.loaded_row_count,
        "national PostGIS parcel-boundary mirror rebuild succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RebuildConfig {
    database_url: String,
    execution_evidence_path: PathBuf,
    source_snapshot_id: String,
    expected_row_count: Option<u64>,
    max_bounded_object_count: u64,
    max_bounded_row_count: u64,
    copy_buffer_bytes: usize,
    summary_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffObject {
    /// Silver handoff shard id from the execution evidence.
    pub shard_id: String,
    /// R2 object key containing the shard JSONL handoff.
    pub object_key: String,
    /// Expected JSONL row count for this object.
    pub row_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionEvidence {
    /// Number of selected handoff objects.
    pub object_count: u64,
    /// Expected total row count across all handoff objects.
    pub expected_row_count: u64,
    /// Ordered R2 handoff objects to process.
    pub objects: Vec<HandoffObject>,
}

#[derive(Debug, Serialize)]
struct RebuildSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    rebuild_run_id: Uuid,
    source_snapshot_id: String,
    source_table: &'static str,
    source_srid: String,
    target_srid: String,
    storage_driver: &'static str,
    execution_evidence_path: String,
    object_count: u64,
    expected_row_count: u64,
    copied_row_count: u64,
    loaded_row_count: u64,
    rejected_row_count: u64,
    invalid_srid_count: u64,
    invalid_geometry_count: u64,
    empty_geometry_count: u64,
    nonpositive_area_count: u64,
    object_results: Vec<ObjectLoadSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ObjectLoadSummary {
    shard_id: String,
    object_key: String,
    expected_row_count: u64,
    copied_row_count: u64,
    inserted_row_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct RawExecutionEvidence {
    schema_version: String,
    status: String,
    output_storage_driver: String,
    summary: RawExecutionSummary,
    shard_results: Vec<RawShardResult>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawExecutionSummary {
    #[serde(rename = "selected_shard_count")]
    selected_shards: u64,
    #[serde(rename = "succeeded_shard_count")]
    succeeded_shards: u64,
    #[serde(rename = "failed_shard_count")]
    failed_shards: u64,
    #[serde(rename = "output_row_count")]
    output_rows: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct RawShardResult {
    shard_id: String,
    status: String,
    output_storage_driver: String,
    output_object_key: String,
    output_row_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct SilverHandoffRow {
    boundary_id: String,
    pnu: String,
    jibun: Option<String>,
    bonbun: Option<String>,
    bubun: Option<String>,
    geometry_wkb_hex: String,
    geometry_wkb_encoding: String,
    geometry_srid: i32,
    bbox_min_x: f64,
    bbox_min_y: f64,
    bbox_max_x: f64,
    bbox_max_y: f64,
    geometry_checksum_sha256: String,
    source_record_id: String,
    source_snapshot_id: String,
    valid_from_utc: String,
    valid_to_utc: Option<String>,
    ingested_at_utc: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageRow {
    /// Canonical 19-digit parcel number.
    pub pnu: String,
    /// Source row identity for the parcel boundary.
    pub boundary_id: String,
    /// Lowercase hex-encoded WKB geometry in EPSG:4326.
    pub geometry_wkb_hex: String,
    /// Lowercase SHA-256 checksum for the source geometry WKB.
    pub geometry_checksum_sha256: String,
    /// Traceable JSON properties copied into serving projections.
    pub properties_json: String,
}

impl RebuildConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_CONFIRM_REBUILD")?
                .unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_CONFIRM_REBUILD must be true"
            );
        }

        let source_snapshot_id =
            required_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_SNAPSHOT_ID")?;
        validate_source_snapshot_id(source_snapshot_id.as_str())?;

        let expected_row_count =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_EXPECTED_ROW_COUNT")?
                .map(|value| parse_positive_u64(&value, "expected row count"))
                .transpose()?;

        let copy_buffer_bytes =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_COPY_BUFFER_BYTES")?
                .map(|value| parse_copy_buffer_bytes(&value))
                .transpose()?
                .unwrap_or(DEFAULT_COPY_BUFFER_BYTES);

        Ok(Self {
            database_url: required_env("DATABASE_URL")?,
            execution_evidence_path: PathBuf::from(required_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_EXECUTION_EVIDENCE_PATH",
            )?),
            source_snapshot_id,
            expected_row_count,
            max_bounded_object_count: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_MAX_BOUNDED_OBJECT_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "max bounded object count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_BOUNDED_OBJECT_COUNT),
            max_bounded_row_count: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_MAX_BOUNDED_ROW_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "max bounded row count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_BOUNDED_ROW_COUNT),
            copy_buffer_bytes,
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
        })
    }
}

/// Reads and validates the national Silver handoff execution evidence.
pub fn read_execution_evidence(path: &Path) -> anyhow::Result<ExecutionEvidence> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read execution evidence {}", path.display()))?;
    let raw: RawExecutionEvidence = serde_json::from_slice(strip_utf8_bom(&bytes))
        .with_context(|| format!("execution evidence is not valid JSON: {}", path.display()))?;
    execution_evidence_from_raw(raw)
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes)
}

fn execution_evidence_from_raw(raw: RawExecutionEvidence) -> anyhow::Result<ExecutionEvidence> {
    if raw.schema_version != EXECUTION_SCHEMA_VERSION {
        bail!("execution evidence schema_version mismatch");
    }
    if raw.status != "ready" {
        bail!("execution evidence status must be ready");
    }
    if raw.output_storage_driver != "r2" {
        bail!("execution evidence output_storage_driver must be r2");
    }
    if raw.summary.failed_shards != 0 {
        bail!("execution evidence contains failed shards");
    }
    if raw.summary.selected_shards != raw.summary.succeeded_shards {
        bail!("execution evidence selected/succeeded shard count mismatch");
    }

    let mut objects = Vec::with_capacity(raw.shard_results.len());
    let mut row_sum = 0_u64;
    for shard in raw.shard_results {
        if shard.status != "succeeded" {
            bail!("shard {} did not succeed", shard.shard_id);
        }
        if shard.output_storage_driver != "r2" {
            bail!("shard {} output_storage_driver must be r2", shard.shard_id);
        }
        validate_object_key(shard.output_object_key.as_str())?;
        row_sum = row_sum
            .checked_add(shard.output_row_count)
            .context("execution evidence output row count overflow")?;
        objects.push(HandoffObject {
            shard_id: shard.shard_id,
            object_key: shard.output_object_key,
            row_count: shard.output_row_count,
        });
    }

    if u64::try_from(objects.len()).context("object count overflow")? != raw.summary.selected_shards
    {
        bail!("execution evidence shard_results count mismatch");
    }
    if row_sum != raw.summary.output_rows {
        bail!("execution evidence shard row sum mismatch");
    }
    if row_sum == 0 {
        bail!("execution evidence output_row_count must be positive");
    }

    Ok(ExecutionEvidence {
        object_count: raw.summary.selected_shards,
        expected_row_count: row_sum,
        objects,
    })
}

async fn execute_rebuild(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    config: &RebuildConfig,
    evidence: &ExecutionEvidence,
    rebuild_run_id: Uuid,
) -> anyhow::Result<RebuildSummary> {
    assert_bounded_db_projection(evidence, config)?;
    if let Some(expected_row_count) = config.expected_row_count {
        if expected_row_count != evidence.expected_row_count {
            bail!(
                "configured expected row count {expected_row_count} does not match evidence {}",
                evidence.expected_row_count
            );
        }
    }

    insert_rebuild_run(conn, rebuild_run_id, config, evidence).await?;
    prepare_target_tables(conn).await?;

    let mut object_results = Vec::with_capacity(evidence.objects.len());
    let mut copied_row_count = 0_u64;
    for object in &evidence.objects {
        let result = load_handoff_object(
            conn,
            storage,
            object,
            rebuild_run_id,
            config.source_snapshot_id.as_str(),
            config.copy_buffer_bytes,
        )
        .await?;
        copied_row_count = copied_row_count
            .checked_add(result.copied_row_count)
            .context("copied row count overflow")?;
        object_results.push(result);
    }

    if copied_row_count != evidence.expected_row_count {
        bail!(
            "copied row count mismatch: expected={} actual={copied_row_count}",
            evidence.expected_row_count
        );
    }

    let validation = validate_loaded_mirror(conn, config.source_snapshot_id.as_str()).await?;
    if validation.loaded_rows != evidence.expected_row_count {
        bail!(
            "loaded row count mismatch: expected={} actual={}",
            evidence.expected_row_count,
            validation.loaded_rows
        );
    }
    if validation.invalid_srid != 0
        || validation.invalid_geometry != 0
        || validation.empty_geometry != 0
        || validation.nonpositive_area != 0
    {
        bail!("PostGIS mirror validation failed");
    }

    mark_rebuild_succeeded(
        conn,
        rebuild_run_id,
        config.source_snapshot_id.as_str(),
        &validation,
        evidence,
    )
    .await?;

    Ok(RebuildSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        rebuild_run_id,
        source_snapshot_id: config.source_snapshot_id.clone(),
        source_table: SOURCE_TABLE,
        source_srid: format!("EPSG:{SOURCE_SRID}"),
        target_srid: format!("EPSG:{TARGET_SRID}"),
        storage_driver: "r2",
        execution_evidence_path: config.execution_evidence_path.display().to_string(),
        object_count: evidence.object_count,
        expected_row_count: evidence.expected_row_count,
        copied_row_count,
        loaded_row_count: validation.loaded_rows,
        rejected_row_count: 0,
        invalid_srid_count: validation.invalid_srid,
        invalid_geometry_count: validation.invalid_geometry,
        empty_geometry_count: validation.empty_geometry,
        nonpositive_area_count: validation.nonpositive_area,
        object_results,
    })
}

fn assert_bounded_db_projection(
    evidence: &ExecutionEvidence,
    config: &RebuildConfig,
) -> anyhow::Result<()> {
    if evidence.object_count > config.max_bounded_object_count {
        bail!(
            "PostGIS parcel-boundary mirror is bounded QA only: object_count={} max_bounded_object_count={}",
            evidence.object_count,
            config.max_bounded_object_count
        );
    }
    if evidence.expected_row_count > config.max_bounded_row_count {
        bail!(
            "PostGIS parcel-boundary mirror is bounded QA only: expected_row_count={} max_bounded_row_count={}",
            evidence.expected_row_count,
            config.max_bounded_row_count
        );
    }
    Ok(())
}

async fn insert_rebuild_run(
    conn: &mut PgConnection,
    rebuild_run_id: Uuid,
    config: &RebuildConfig,
    evidence: &ExecutionEvidence,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
         (id, source_snapshot_id, source_table, srid, status, loaded_row_count,
          rejected_row_count, quality_report, started_at)
         VALUES ($1, $2, $3, $4, 'running', 0, 0, $5, now())",
    )
    .bind(rebuild_run_id)
    .bind(config.source_snapshot_id.as_str())
    .bind(SOURCE_TABLE)
    .bind(TARGET_SRID)
    .bind(json!({
        "execution_evidence_path": config.execution_evidence_path.display().to_string(),
        "object_count": evidence.object_count,
        "expected_row_count": evidence.expected_row_count,
        "source_srid": format!("EPSG:{SOURCE_SRID}"),
        "target_srid": format!("EPSG:{TARGET_SRID}"),
        "geometry_repair_strategy": "postgis-st_makevalid-collectionextract-polygon-v1",
        "load_strategy": "r2-jsonl-copy-stage-per-object"
    }))
    .execute(&mut *conn)
    .await
    .context("failed to insert PostGIS mirror rebuild run")?;
    Ok(())
}

async fn prepare_target_tables(conn: &mut PgConnection) -> anyhow::Result<()> {
    assert_mirror_table_is_unlogged(conn).await?;
    conn.execute("TRUNCATE TABLE serving_postgis.parcel_boundary_mirror")
        .await
        .context("failed to truncate parcel_boundary_mirror")?;
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_boundary_mirror_load_stage (
             pnu text NOT NULL,
             boundary_id text NOT NULL,
             source_object_key text NOT NULL,
             geometry_wkb_hex text NOT NULL,
             geometry_checksum_sha256 text NOT NULL,
             properties jsonb NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create PostGIS mirror load stage")?;
    Ok(())
}

async fn assert_mirror_table_is_unlogged(conn: &mut PgConnection) -> anyhow::Result<()> {
    let relpersistence = sqlx::query_scalar::<_, String>(
        "SELECT relpersistence::text
         FROM pg_class
         WHERE oid = 'serving_postgis.parcel_boundary_mirror'::regclass",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to inspect parcel_boundary_mirror persistence")?;
    if relpersistence != "u" {
        bail!("serving_postgis.parcel_boundary_mirror must be UNLOGGED before national rebuild");
    }
    Ok(())
}

async fn load_handoff_object(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    object: &HandoffObject,
    rebuild_run_id: Uuid,
    source_snapshot_id: &str,
    copy_buffer_bytes: usize,
) -> anyhow::Result<ObjectLoadSummary> {
    conn.execute("TRUNCATE TABLE parcel_boundary_mirror_load_stage")
        .await
        .context("failed to truncate PostGIS mirror load stage")?;
    let object_bytes = storage
        .get_object_bytes(object.object_key.as_str())
        .await
        .with_context(|| format!("failed to read R2 handoff object {}", object.object_key))?;

    let copied_row_count = copy_object_to_stage(conn, object, &object_bytes, copy_buffer_bytes)
        .await
        .with_context(|| format!("failed to copy handoff object {}", object.object_key))?;
    if copied_row_count != object.row_count {
        bail!(
            "handoff object {} row count mismatch: expected={} actual={copied_row_count}",
            object.object_key,
            object.row_count
        );
    }

    let staged_row_count = count_stage_rows(conn)
        .await
        .with_context(|| format!("failed to count staged rows for {}", object.object_key))?;
    if staged_row_count != copied_row_count {
        bail!(
            "stage row count mismatch for {}: copied={copied_row_count} staged={staged_row_count}",
            object.object_key
        );
    }

    let inserted_row_count = insert_stage_into_mirror(conn, rebuild_run_id, source_snapshot_id)
        .await
        .with_context(|| {
            format!(
                "failed to insert staged rows into PostGIS mirror for {}",
                object.object_key
            )
        })?;
    if inserted_row_count != copied_row_count {
        bail!(
            "inserted row count mismatch for {}: copied={copied_row_count} inserted={inserted_row_count}",
            object.object_key
        );
    }

    tracing::info!(
        shard_id = %object.shard_id,
        object_key = %object.object_key,
        row_count = inserted_row_count,
        "loaded PostGIS mirror handoff object"
    );

    Ok(ObjectLoadSummary {
        shard_id: object.shard_id.clone(),
        object_key: object.object_key.clone(),
        expected_row_count: object.row_count,
        copied_row_count,
        inserted_row_count,
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
            "COPY parcel_boundary_mirror_load_stage
             (pnu, boundary_id, source_object_key, geometry_wkb_hex,
              geometry_checksum_sha256, properties)
             FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '\"', ESCAPE '\"', NULL '\\N')",
        )
        .await
        .context("failed to start COPY into PostGIS mirror load stage")?;
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

/// Parses one Silver handoff JSONL row into the COPY stage representation.
pub fn parse_stage_row(
    line: &[u8],
    source_object_key: &str,
    line_number: u64,
) -> anyhow::Result<StageRow> {
    let row: SilverHandoffRow = serde_json::from_slice(line)
        .with_context(|| format!("handoff line {line_number} is not valid JSON"))?;
    validate_silver_handoff_row(&row, line_number)?;
    let properties = json!({
        "boundary_id": row.boundary_id,
        "handoff_source_snapshot_id": row.source_snapshot_id,
        "handoff_source_record_id": row.source_record_id,
        "source_object_key": source_object_key,
        "jibun": row.jibun,
        "bonbun": row.bonbun,
        "bubun": row.bubun,
        "bbox": {
            "min_x": row.bbox_min_x,
            "min_y": row.bbox_min_y,
            "max_x": row.bbox_max_x,
            "max_y": row.bbox_max_y
        },
        "valid_from_utc": row.valid_from_utc,
        "valid_to_utc": row.valid_to_utc,
        "ingested_at_utc": row.ingested_at_utc
    });

    Ok(StageRow {
        pnu: row.pnu,
        boundary_id: row.boundary_id,
        geometry_wkb_hex: row.geometry_wkb_hex,
        geometry_checksum_sha256: row.geometry_checksum_sha256,
        properties_json: serde_json::to_string(&properties)
            .context("failed to serialize PostGIS mirror row properties")?,
    })
}

fn validate_silver_handoff_row(row: &SilverHandoffRow, line_number: u64) -> anyhow::Result<()> {
    if !is_pnu(&row.pnu) {
        bail!("handoff line {line_number} pnu must be 19 digits");
    }
    if row.boundary_id.trim().is_empty() {
        bail!("handoff line {line_number} boundary_id must not be empty");
    }
    if row.geometry_wkb_encoding != "hex" {
        bail!("handoff line {line_number} geometry_wkb_encoding must be hex");
    }
    if row.geometry_srid != SOURCE_SRID {
        bail!("handoff line {line_number} geometry_srid must be {SOURCE_SRID}");
    }
    if !is_lowercase_even_hex(&row.geometry_wkb_hex) {
        bail!("handoff line {line_number} geometry_wkb_hex must be lowercase even-length hex");
    }
    if !is_lowercase_sha256(&row.geometry_checksum_sha256) {
        bail!("handoff line {line_number} geometry_checksum_sha256 must be lowercase sha256");
    }
    for (name, value) in [
        ("bbox_min_x", row.bbox_min_x),
        ("bbox_min_y", row.bbox_min_y),
        ("bbox_max_x", row.bbox_max_x),
        ("bbox_max_y", row.bbox_max_y),
    ] {
        if !value.is_finite() {
            bail!("handoff line {line_number} {name} must be finite");
        }
    }
    if row.bbox_max_x < row.bbox_min_x || row.bbox_max_y < row.bbox_min_y {
        bail!("handoff line {line_number} bbox must be ordered");
    }
    Ok(())
}

/// Appends one tab-delimited CSV row accepted by `PostgreSQL` `COPY`.
pub fn push_copy_csv_row(buffer: &mut Vec<u8>, row: &StageRow, source_object_key: &str) {
    push_copy_csv_field(buffer, Some(row.pnu.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.boundary_id.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(source_object_key));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.geometry_wkb_hex.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.geometry_checksum_sha256.as_str()));
    buffer.push(b'\t');
    push_copy_csv_field(buffer, Some(row.properties_json.as_str()));
    buffer.push(b'\n');
}

fn push_copy_csv_field(buffer: &mut Vec<u8>, value: Option<&str>) {
    let Some(value) = value else {
        buffer.extend_from_slice(br"\N");
        return;
    };
    buffer.push(b'"');
    for byte in value.bytes() {
        if byte == b'"' {
            buffer.extend_from_slice(b"\"\"");
        } else {
            buffer.push(byte);
        }
    }
    buffer.push(b'"');
}

async fn count_stage_rows(conn: &mut PgConnection) -> anyhow::Result<u64> {
    let count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM parcel_boundary_mirror_load_stage")
            .fetch_one(&mut *conn)
            .await
            .context("failed to count PostGIS mirror load stage rows")?;
    i64_to_u64("stage row count", count)
}

async fn insert_stage_into_mirror(
    conn: &mut PgConnection,
    rebuild_run_id: Uuid,
    source_snapshot_id: &str,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_mirror (
             pnu,
             rebuild_run_id,
             source_snapshot_id,
             source_table,
             source_record_id,
             source_file_asset_id,
             source_object_key,
             source_row_id,
             complex_id,
             parcel_id,
             geometry_checksum_sha256,
             properties,
             geom,
             loaded_at,
             updated_at,
             version
         )
         SELECT
             pnu::char(19),
             $2::uuid,
             $3,
             $1,
             NULL::uuid,
             NULL::uuid,
             source_object_key,
             boundary_id,
             NULL::uuid,
             NULL::uuid,
             geometry_checksum_sha256,
             properties,
             ST_Multi(
                 ST_Transform(
                     ST_CollectionExtract(
                         ST_MakeValid(
                             ST_SetSRID(ST_GeomFromWKB(decode(geometry_wkb_hex, 'hex')), 4326)
                         ),
                         3
                     ),
                     5179
                 )
             ),
             now(),
             now(),
             1
         FROM parcel_boundary_mirror_load_stage",
    )
    .bind(SOURCE_TABLE)
    .bind(rebuild_run_id)
    .bind(source_snapshot_id)
    .execute(&mut *conn)
    .await
    .context("failed to insert PostGIS mirror rows from stage")?;
    Ok(result.rows_affected())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MirrorValidation {
    loaded_rows: u64,
    invalid_srid: u64,
    invalid_geometry: u64,
    empty_geometry: u64,
    nonpositive_area: u64,
}

async fn validate_loaded_mirror(
    conn: &mut PgConnection,
    source_snapshot_id: &str,
) -> anyhow::Result<MirrorValidation> {
    Ok(MirrorValidation {
        loaded_rows: count_mirror_where(conn, source_snapshot_id, "TRUE").await?,
        invalid_srid: count_mirror_where(conn, source_snapshot_id, "ST_SRID(geom) <> 5179").await?,
        invalid_geometry: count_mirror_where(conn, source_snapshot_id, "NOT ST_IsValid(geom)")
            .await?,
        empty_geometry: count_mirror_where(conn, source_snapshot_id, "ST_IsEmpty(geom)").await?,
        nonpositive_area: count_mirror_where(conn, source_snapshot_id, "ST_Area(geom) <= 0")
            .await?,
    })
}

async fn count_mirror_where(
    conn: &mut PgConnection,
    source_snapshot_id: &str,
    predicate: &str,
) -> anyhow::Result<u64> {
    let sql = format!(
        "SELECT count(*)
         FROM serving_postgis.parcel_boundary_mirror
         WHERE source_snapshot_id = $1 AND ({predicate})"
    );
    let count = sqlx::query_scalar::<_, i64>(&sql)
        .bind(source_snapshot_id)
        .fetch_one(&mut *conn)
        .await
        .context("failed to validate PostGIS mirror rows")?;
    i64_to_u64("mirror validation count", count)
}

async fn mark_rebuild_succeeded(
    conn: &mut PgConnection,
    rebuild_run_id: Uuid,
    source_snapshot_id: &str,
    validation: &MirrorValidation,
    evidence: &ExecutionEvidence,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
         SET status = 'succeeded',
             loaded_row_count = $3,
             rejected_row_count = 0,
             quality_report = $4,
             finished_at = now(),
             updated_at = now(),
             version = version + 1
         WHERE id = $1 AND source_snapshot_id = $2",
    )
    .bind(rebuild_run_id)
    .bind(source_snapshot_id)
    .bind(u64_to_i64("loaded_row_count", validation.loaded_rows)?)
    .bind(json!({
        "object_count": evidence.object_count,
        "expected_row_count": evidence.expected_row_count,
        "loaded_row_count": validation.loaded_rows,
        "invalid_srid_count": validation.invalid_srid,
        "invalid_geometry_count": validation.invalid_geometry,
        "empty_geometry_count": validation.empty_geometry,
        "nonpositive_area_count": validation.nonpositive_area,
        "source_srid": format!("EPSG:{SOURCE_SRID}"),
        "target_srid": format!("EPSG:{TARGET_SRID}"),
        "geometry_repair_strategy": "postgis-st_makevalid-collectionextract-polygon-v1"
    }))
    .execute(&mut *conn)
    .await
    .context("failed to mark PostGIS mirror rebuild succeeded")?;
    Ok(())
}

async fn mark_rebuild_failed(
    conn: &mut PgConnection,
    rebuild_run_id: Uuid,
    source_snapshot_id: &str,
    error_message: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
         SET status = 'failed',
             loaded_row_count = (
                 SELECT count(*) FROM serving_postgis.parcel_boundary_mirror
                 WHERE source_snapshot_id = $2
             ),
             error_message = left($3, 4000),
             finished_at = now(),
             updated_at = now(),
             version = version + 1
         WHERE id = $1 AND source_snapshot_id = $2 AND status = 'running'",
    )
    .bind(rebuild_run_id)
    .bind(source_snapshot_id)
    .bind(error_message)
    .execute(&mut *conn)
    .await
    .context("failed to mark PostGIS mirror rebuild failed")?;
    Ok(())
}

fn write_summary(path: &Path, report: &RebuildSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report)
        .context("failed to serialize PostGIS mirror rebuild summary")?;
    fs::write(path, payload)
        .with_context(|| format!("failed to write PostGIS mirror summary {}", path.display()))
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

/// Validates the namespaced source snapshot id lineage shape.
pub fn validate_source_snapshot_id(value: &str) -> anyhow::Result<()> {
    if value.trim() != value || value.len() < 3 || value.len() > 256 {
        bail!("source snapshot id length or padding is invalid");
    }
    let Some((namespace, body)) = value.split_once(':') else {
        bail!("source snapshot id must use <namespace>:<id> format");
    };
    if namespace.len() < 2 || body.len() < 3 {
        bail!("source snapshot id namespace or body length is invalid");
    }
    if value.contains('/') || value.contains('\\') || value.contains("..") {
        bail!("source snapshot id must not contain path separators or traversal markers");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        bail!("source snapshot id contains invalid characters");
    }
    Ok(())
}

fn validate_object_key(value: &str) -> anyhow::Result<()> {
    if value.trim() != value || value.is_empty() {
        bail!("object key must not be empty or padded");
    }
    if value.starts_with('/') || value.contains('\\') || value.contains("//") {
        bail!("object key must be provider-relative and normalized");
    }
    if value
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        bail!("object key must not contain empty, '.', or '..' segments");
    }
    Ok(())
}

fn is_pnu(value: &str) -> bool {
    value.len() == 19 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_lowercase_even_hex(value: &str) -> bool {
    !value.is_empty()
        && value.len().is_multiple_of(2)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn i64_to_u64(label: &str, value: i64) -> anyhow::Result<u64> {
    u64::try_from(value).with_context(|| format!("{label} cannot be negative"))
}

fn u64_to_i64(label: &str, value: u64) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} overflows Postgres BIGINT"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    #[test]
    fn execution_evidence_selects_r2_succeeded_handoff_objects() -> anyhow::Result<()> {
        let evidence = execution_evidence_from_raw(RawExecutionEvidence {
            schema_version: EXECUTION_SCHEMA_VERSION.to_owned(),
            status: "ready".to_owned(),
            output_storage_driver: "r2".to_owned(),
            summary: RawExecutionSummary {
                selected_shards: 2,
                succeeded_shards: 2,
                failed_shards: 0,
                output_rows: 3,
            },
            shard_results: vec![
                RawShardResult {
                    shard_id: "silver-parcel-boundaries-vworld-0001".to_owned(),
                    status: "succeeded".to_owned(),
                    output_storage_driver: "r2".to_owned(),
                    output_object_key: "silver-handoff/a/part-0001.jsonl".to_owned(),
                    output_row_count: 1,
                },
                RawShardResult {
                    shard_id: "silver-parcel-boundaries-vworld-0002".to_owned(),
                    status: "succeeded".to_owned(),
                    output_storage_driver: "r2".to_owned(),
                    output_object_key: "silver-handoff/a/part-0002.jsonl".to_owned(),
                    output_row_count: 2,
                },
            ],
        })?;

        assert_eq!(evidence.object_count, 2);
        assert_eq!(evidence.expected_row_count, 3);
        assert_eq!(
            evidence.objects[1].object_key,
            "silver-handoff/a/part-0002.jsonl"
        );
        Ok(())
    }

    #[test]
    fn execution_evidence_rejects_incomplete_shards() {
        let error = execution_evidence_from_raw(RawExecutionEvidence {
            schema_version: EXECUTION_SCHEMA_VERSION.to_owned(),
            status: "ready".to_owned(),
            output_storage_driver: "r2".to_owned(),
            summary: RawExecutionSummary {
                selected_shards: 1,
                succeeded_shards: 0,
                failed_shards: 1,
                output_rows: 1,
            },
            shard_results: Vec::new(),
        })
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

        assert!(error.contains("failed shards"));
    }

    #[test]
    fn execution_evidence_reader_accepts_utf8_bom() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "foundation-platform-postgis-evidence-bom-{}.json",
            Uuid::now_v7()
        ));
        let payload = format!(
            "\u{feff}{}",
            serde_json::to_string(&json!({
                "schema_version": EXECUTION_SCHEMA_VERSION,
                "status": "ready",
                "output_storage_driver": "r2",
                "summary": {
                    "selected_shard_count": 1,
                    "succeeded_shard_count": 1,
                    "failed_shard_count": 0,
                    "output_row_count": 1
                },
                "shard_results": [{
                    "shard_id": "silver-parcel-boundaries-vworld-0001",
                    "status": "succeeded",
                    "output_storage_driver": "r2",
                    "output_object_key": "silver-handoff/a/part-0001.jsonl",
                    "output_row_count": 1
                }]
            }))?
        );
        fs::write(&path, payload)?;

        let evidence = read_execution_evidence(&path)?;

        assert_eq!(evidence.expected_row_count, 1);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn silver_handoff_line_becomes_stage_row_with_trace_properties() -> anyhow::Result<()> {
        let line = br#"{"boundary_id":"vworld-cadastral:parcel-boundary:pnu:9999900101100010001","pnu":"9999900101100010001","jibun":"1-1","bonbun":"0001","bubun":"0001","geometry_wkb_hex":"010600000000000000","geometry_wkb_encoding":"hex","geometry_srid":4326,"bbox_min_x":127.12347023440,"bbox_min_y":36.123450,"bbox_max_x":127.12347023441,"bbox_max_y":36.123451,"geometry_checksum_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_record_id":"national-promotion:vworld-shard-0001","source_snapshot_id":"national-promotion:vworld-shard-0001","valid_from_utc":"2026-05-24T00:00:00Z","valid_to_utc":null,"ingested_at_utc":"2026-05-24T00:00:01Z"}"#;

        let row = parse_stage_row(line, "silver-handoff/a/part-0001.jsonl", 1)?;
        let properties: JsonValue = serde_json::from_str(&row.properties_json)?;

        assert_eq!(row.pnu, "9999900101100010001");
        assert_eq!(
            properties["handoff_source_record_id"],
            "national-promotion:vworld-shard-0001"
        );
        assert_eq!(
            properties["source_object_key"],
            "silver-handoff/a/part-0001.jsonl"
        );
        assert_eq!(properties["bbox"]["min_x"], 127.123_470_234_40);
        Ok(())
    }

    #[test]
    fn copy_csv_row_escapes_quotes_without_losing_object_key() {
        let row = StageRow {
            pnu: "9999900101100010001".to_owned(),
            boundary_id: "boundary\"id".to_owned(),
            geometry_wkb_hex: "010600000000000000".to_owned(),
            geometry_checksum_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            properties_json: "{\"jibun\":\"1-1\"}".to_owned(),
        };
        let mut buffer = Vec::new();

        push_copy_csv_row(&mut buffer, &row, "silver-handoff/a/part-0001.jsonl");
        let rendered = String::from_utf8(buffer).unwrap_or_default();

        assert!(rendered.contains("\"boundary\"\"id\""));
        assert!(rendered.contains("\"silver-handoff/a/part-0001.jsonl\""));
    }

    #[test]
    fn source_snapshot_id_accepts_pipeline_lineage_not_only_iceberg() -> anyhow::Result<()> {
        validate_source_snapshot_id("iceberg:parcel-boundaries-snapshot-001")?;
        validate_source_snapshot_id("national-promotion:silver-parcel-boundaries-vworld-0002")?;
        assert!(validate_source_snapshot_id("../silver-parcel-boundaries").is_err());
        assert!(validate_source_snapshot_id(" national-promotion:bad").is_err());
        Ok(())
    }

    #[test]
    fn postgis_mirror_refuses_unbounded_national_projection() {
        let evidence = ExecutionEvidence {
            object_count: 85,
            expected_row_count: 39_862_472,
            objects: Vec::new(),
        };
        let config = RebuildConfig {
            database_url: "postgres://example.invalid/foundation_platform".to_owned(),
            execution_evidence_path: PathBuf::from("target/audit/evidence.json"),
            source_snapshot_id: "iceberg:parcel-boundaries-snapshot-001".to_owned(),
            expected_row_count: None,
            max_bounded_object_count: DEFAULT_MAX_BOUNDED_OBJECT_COUNT,
            max_bounded_row_count: DEFAULT_MAX_BOUNDED_ROW_COUNT,
            copy_buffer_bytes: DEFAULT_COPY_BUFFER_BYTES,
            summary_path: None,
        };

        let error = assert_bounded_db_projection(&evidence, &config)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();

        assert!(error.contains("bounded QA only"));
    }
}
