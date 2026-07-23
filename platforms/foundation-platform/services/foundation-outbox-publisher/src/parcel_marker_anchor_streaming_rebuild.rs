//! Streaming parcel marker anchor rebuild from national Silver handoff shards.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::Utc;
use foundation_outbox::R2ObjectStorage;
use serde::Serialize;
use serde_json::json;
use sqlx::{Connection, Executor, PgConnection};
use uuid::Uuid;

use crate::postgis_parcel_boundary_mirror_national_rebuild::{
    parse_stage_row, push_copy_csv_row, read_execution_evidence, validate_source_snapshot_id,
    ExecutionEvidence, HandoffObject,
};

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_streaming_rebuild_summary.v1";
const SOURCE_TABLE: &str = "silver.parcel_boundaries";
const SOURCE_SRID: i32 = 4326;
const ANCHOR_SRID: i32 = 4326;
const DEFAULT_ALGORITHM_VERSION: &str = "postgis-st_maximuminscribedcircle-v1";
const DEFAULT_COPY_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const MIN_COPY_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_COPY_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BOUNDED_OBJECT_COUNT: u64 = 1;
const DEFAULT_MAX_BOUNDED_ROW_COUNT: u64 = 1_000_000;

/// Runs the streaming national parcel marker anchor rebuild.
pub async fn run() -> anyhow::Result<()> {
    let config = StreamingAnchorConfig::from_env()?;
    let evidence = read_execution_evidence(&config.execution_evidence_path)?;
    let generation_run_id = Uuid::now_v7();
    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for streaming anchor rebuild")?;
    let mut conn = PgConnection::connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL for streaming anchor rebuild")?;

    let report = match execute_streaming_anchor_rebuild(
        &mut conn,
        &storage,
        &config,
        &evidence,
        generation_run_id,
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            let _ = mark_generation_failed(
                &mut conn,
                generation_run_id,
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
        generation_run_id = %report.generation_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        loaded_row_count = report.loaded_row_count,
        activated_row_count = report.activated_row_count,
        "streaming parcel marker anchor rebuild succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamingAnchorConfig {
    database_url: String,
    execution_evidence_path: PathBuf,
    source_snapshot_id: String,
    algorithm_version: String,
    expected_row_count: Option<u64>,
    max_bounded_object_count: u64,
    max_bounded_row_count: u64,
    copy_buffer_bytes: usize,
    summary_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct StreamingAnchorSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    generation_run_id: Uuid,
    source_snapshot_id: String,
    source_table: &'static str,
    algorithm: &'static str,
    algorithm_version: String,
    source_srid: String,
    anchor_srid: String,
    execution_evidence_path: String,
    object_count: u64,
    expected_row_count: u64,
    copied_row_count: u64,
    loaded_row_count: u64,
    rejected_row_count: u64,
    superseded_row_count: u64,
    activated_row_count: u64,
    object_results: Vec<AnchorObjectLoadSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AnchorObjectLoadSummary {
    shard_id: String,
    object_key: String,
    expected_row_count: u64,
    copied_row_count: u64,
    loaded_row_count: u64,
}

impl StreamingAnchorConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_CONFIRM_REBUILD")?
                .unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_CONFIRM_REBUILD must be true"
            );
        }

        let source_snapshot_id =
            required_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_SOURCE_SNAPSHOT_ID")?;
        validate_source_snapshot_id(source_snapshot_id.as_str())?;

        let algorithm_version =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_ALGORITHM_VERSION")?
                .unwrap_or_else(|| DEFAULT_ALGORITHM_VERSION.to_owned());
        validate_algorithm_version(algorithm_version.as_str())?;

        let expected_row_count =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_EXPECTED_ROW_COUNT")?
                .map(|value| parse_positive_u64(&value, "expected row count"))
                .transpose()?;
        let copy_buffer_bytes =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_COPY_BUFFER_BYTES")?
                .map(|value| parse_copy_buffer_bytes(&value))
                .transpose()?
                .unwrap_or(DEFAULT_COPY_BUFFER_BYTES);

        Ok(Self {
            database_url: required_env("DATABASE_URL")?,
            execution_evidence_path: PathBuf::from(required_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_EXECUTION_EVIDENCE_PATH",
            )?),
            source_snapshot_id,
            algorithm_version,
            expected_row_count,
            max_bounded_object_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_MAX_BOUNDED_OBJECT_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "max bounded object count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_BOUNDED_OBJECT_COUNT),
            max_bounded_row_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_MAX_BOUNDED_ROW_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "max bounded row count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_BOUNDED_ROW_COUNT),
            copy_buffer_bytes,
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_STREAMING_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
        })
    }
}

async fn execute_streaming_anchor_rebuild(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    config: &StreamingAnchorConfig,
    evidence: &ExecutionEvidence,
    generation_run_id: Uuid,
) -> anyhow::Result<StreamingAnchorSummary> {
    assert_bounded_db_projection(evidence, config)?;
    if let Some(expected_row_count) = config.expected_row_count {
        if expected_row_count != evidence.expected_row_count {
            bail!(
                "configured expected row count {expected_row_count} does not match evidence {}",
                evidence.expected_row_count
            );
        }
    }

    assert_anchor_table_is_unlogged(conn).await?;
    insert_generation_run(conn, generation_run_id, config, evidence).await?;
    create_stage_table(conn).await?;

    let mut object_results = Vec::with_capacity(evidence.objects.len());
    let mut copied_row_count = 0_u64;
    let mut loaded_row_count = 0_u64;
    for object in &evidence.objects {
        let result = load_anchor_object(conn, storage, object, generation_run_id, config).await?;
        copied_row_count = copied_row_count
            .checked_add(result.copied_row_count)
            .context("copied row count overflow")?;
        loaded_row_count = loaded_row_count
            .checked_add(result.loaded_row_count)
            .context("loaded row count overflow")?;
        object_results.push(result);
    }

    if copied_row_count != evidence.expected_row_count
        || loaded_row_count != evidence.expected_row_count
    {
        bail!(
            "anchor row count mismatch: expected={} copied={copied_row_count} loaded={loaded_row_count}",
            evidence.expected_row_count
        );
    }

    let persisted_count = count_generation_anchors(conn, generation_run_id).await?;
    if persisted_count != evidence.expected_row_count {
        bail!(
            "persisted anchor count mismatch: expected={} actual={persisted_count}",
            evidence.expected_row_count
        );
    }

    let superseded_row_count = supersede_previous_and_activate_new(conn, generation_run_id).await?;
    let activated_row_count = count_active_generation_anchors(conn, generation_run_id).await?;
    if activated_row_count != evidence.expected_row_count {
        bail!(
            "activated anchor count mismatch: expected={} actual={activated_row_count}",
            evidence.expected_row_count
        );
    }

    mark_generation_succeeded(
        conn,
        generation_run_id,
        config,
        evidence,
        activated_row_count,
        superseded_row_count,
    )
    .await?;

    Ok(StreamingAnchorSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        generation_run_id,
        source_snapshot_id: config.source_snapshot_id.clone(),
        source_table: SOURCE_TABLE,
        algorithm: "polylabel",
        algorithm_version: config.algorithm_version.clone(),
        source_srid: format!("EPSG:{SOURCE_SRID}"),
        anchor_srid: format!("EPSG:{ANCHOR_SRID}"),
        execution_evidence_path: config.execution_evidence_path.display().to_string(),
        object_count: evidence.object_count,
        expected_row_count: evidence.expected_row_count,
        copied_row_count,
        loaded_row_count,
        rejected_row_count: 0,
        superseded_row_count,
        activated_row_count,
        object_results,
    })
}

fn assert_bounded_db_projection(
    evidence: &ExecutionEvidence,
    config: &StreamingAnchorConfig,
) -> anyhow::Result<()> {
    if evidence.object_count > config.max_bounded_object_count {
        bail!(
            "parcel marker anchor DB projection is bounded only: object_count={} max_bounded_object_count={}",
            evidence.object_count,
            config.max_bounded_object_count
        );
    }
    if evidence.expected_row_count > config.max_bounded_row_count {
        bail!(
            "parcel marker anchor DB projection is bounded only: expected_row_count={} max_bounded_row_count={}",
            evidence.expected_row_count,
            config.max_bounded_row_count
        );
    }
    Ok(())
}

async fn assert_anchor_table_is_unlogged(conn: &mut PgConnection) -> anyhow::Result<()> {
    let relpersistence = sqlx::query_scalar::<_, String>(
        "SELECT relpersistence::text
         FROM pg_class
         WHERE oid = 'catalog.parcel_marker_anchor'::regclass",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to inspect parcel_marker_anchor persistence")?;
    if relpersistence != "u" {
        bail!("catalog.parcel_marker_anchor must be UNLOGGED before national streaming rebuild");
    }
    Ok(())
}

async fn insert_generation_run(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
    config: &StreamingAnchorConfig,
    evidence: &ExecutionEvidence,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.parcel_marker_anchor_generation_run
         (id, source_snapshot_id, source_table, algorithm, algorithm_version, srid,
          status, loaded_row_count, rejected_row_count, quality_report, started_at)
         VALUES ($1, $2, $3, 'polylabel', $4, 4326, 'running', 0, 0, $5, now())",
    )
    .bind(generation_run_id)
    .bind(config.source_snapshot_id.as_str())
    .bind(SOURCE_TABLE)
    .bind(config.algorithm_version.as_str())
    .bind(json!({
        "execution_evidence_path": config.execution_evidence_path.display().to_string(),
        "object_count": evidence.object_count,
        "expected_row_count": evidence.expected_row_count,
        "load_strategy": "r2-jsonl-copy-stage-per-object-anchor-only",
        "source_srid": format!("EPSG:{SOURCE_SRID}"),
        "anchor_srid": format!("EPSG:{ANCHOR_SRID}")
    }))
    .execute(&mut *conn)
    .await
    .context("failed to insert parcel marker anchor generation run")?;
    Ok(())
}

async fn create_stage_table(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_marker_anchor_load_stage (
             pnu text NOT NULL,
             boundary_id text NOT NULL,
             source_object_key text NOT NULL,
             geometry_wkb_hex text NOT NULL,
             geometry_checksum_sha256 text NOT NULL,
             properties jsonb NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create parcel marker anchor load stage")?;
    Ok(())
}

async fn load_anchor_object(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    object: &HandoffObject,
    generation_run_id: Uuid,
    config: &StreamingAnchorConfig,
) -> anyhow::Result<AnchorObjectLoadSummary> {
    conn.execute("TRUNCATE TABLE parcel_marker_anchor_load_stage")
        .await
        .context("failed to truncate parcel marker anchor load stage")?;
    let bytes = storage
        .get_object_bytes(object.object_key.as_str())
        .await
        .with_context(|| format!("failed to read R2 handoff object {}", object.object_key))?;
    let copied_row_count =
        copy_object_to_stage(conn, object, &bytes, config.copy_buffer_bytes).await?;
    if copied_row_count != object.row_count {
        bail!(
            "handoff object {} row count mismatch: expected={} actual={copied_row_count}",
            object.object_key,
            object.row_count
        );
    }

    let loaded_row_count = insert_stage_anchors(
        conn,
        generation_run_id,
        config.source_snapshot_id.as_str(),
        &config.algorithm_version,
    )
    .await?;
    if loaded_row_count != copied_row_count {
        bail!(
            "anchor load count mismatch for {}: copied={copied_row_count} loaded={loaded_row_count}",
            object.object_key
        );
    }

    tracing::info!(
        shard_id = %object.shard_id,
        object_key = %object.object_key,
        row_count = loaded_row_count,
        "loaded streaming parcel marker anchors"
    );

    Ok(AnchorObjectLoadSummary {
        shard_id: object.shard_id.clone(),
        object_key: object.object_key.clone(),
        expected_row_count: object.row_count,
        copied_row_count,
        loaded_row_count,
    })
}

async fn copy_object_to_stage(
    conn: &mut PgConnection,
    object: &HandoffObject,
    bytes: &[u8],
    copy_buffer_bytes: usize,
) -> anyhow::Result<u64> {
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_marker_anchor_load_stage
             (pnu, boundary_id, source_object_key, geometry_wkb_hex,
              geometry_checksum_sha256, properties)
             FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '\"', ESCAPE '\"', NULL '\\N')",
        )
        .await
        .context("failed to start COPY into parcel marker anchor load stage")?;
    let mut buffer = Vec::with_capacity(copy_buffer_bytes.min(MAX_COPY_BUFFER_BYTES));
    let mut row_count = 0_u64;

    for (index, raw_line) in bytes.split(|byte| *byte == b'\n').enumerate() {
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

async fn insert_stage_anchors(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
    source_snapshot_id: &str,
    algorithm_version: &str,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        "WITH geometries AS (
             SELECT
                 pnu,
                 boundary_id,
                 source_object_key,
                 geometry_checksum_sha256,
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
                 ) AS geom_5179
             FROM parcel_marker_anchor_load_stage
         ),
         valid_rows AS (
             SELECT *
             FROM geometries
             WHERE ST_SRID(geom_5179) = 5179
               AND ST_IsValid(geom_5179)
               AND NOT ST_IsEmpty(geom_5179)
               AND ST_Area(geom_5179) > 0
         )
         INSERT INTO catalog.parcel_marker_anchor (
             id, pnu, parcel_id, generation_run_id, source_geometry_version,
             source_table, source_record_id, source_file_asset_id, source_object_key,
             source_row_id, anchor_point, algorithm, algorithm_version,
             source_geometry_checksum_sha256, computed_at_utc, activated_at_utc,
             superseded_at_utc, is_active, created_at, updated_at, version
         )
         SELECT
             gen_random_uuid(),
             pnu::char(19),
             NULL::uuid,
             $1::uuid,
             $2,
             $3,
             NULL::uuid,
             NULL::uuid,
             source_object_key,
             boundary_id,
             ST_Transform((ST_MaximumInscribedCircle(geom_5179)).center, 4326),
             'polylabel',
             $4,
             geometry_checksum_sha256,
             now(),
             NULL,
             NULL,
             false,
             now(),
             now(),
             1
         FROM valid_rows
         ON CONFLICT (pnu, source_geometry_version, algorithm, algorithm_version)
         DO UPDATE
         SET generation_run_id = EXCLUDED.generation_run_id,
             source_table = EXCLUDED.source_table,
             source_record_id = EXCLUDED.source_record_id,
             source_file_asset_id = EXCLUDED.source_file_asset_id,
             source_object_key = EXCLUDED.source_object_key,
             source_row_id = EXCLUDED.source_row_id,
             anchor_point = EXCLUDED.anchor_point,
             source_geometry_checksum_sha256 = EXCLUDED.source_geometry_checksum_sha256,
             computed_at_utc = EXCLUDED.computed_at_utc,
             activated_at_utc = NULL,
             superseded_at_utc = NULL,
             is_active = false,
             updated_at = now(),
             version = catalog.parcel_marker_anchor.version + 1",
    )
    .bind(generation_run_id)
    .bind(source_snapshot_id)
    .bind(SOURCE_TABLE)
    .bind(algorithm_version)
    .execute(&mut *conn)
    .await
    .context("failed to insert streaming parcel marker anchors")?;
    Ok(result.rows_affected())
}

async fn count_generation_anchors(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
) -> anyhow::Result<u64> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM catalog.parcel_marker_anchor WHERE generation_run_id = $1",
    )
    .bind(generation_run_id)
    .fetch_one(&mut *conn)
    .await
    .context("failed to count generation anchors")?;
    i64_to_u64("generation anchor count", count)
}

async fn count_active_generation_anchors(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
) -> anyhow::Result<u64> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM catalog.parcel_marker_anchor
         WHERE generation_run_id = $1 AND is_active",
    )
    .bind(generation_run_id)
    .fetch_one(&mut *conn)
    .await
    .context("failed to count active generation anchors")?;
    i64_to_u64("active generation anchor count", count)
}

async fn supersede_previous_and_activate_new(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
) -> anyhow::Result<u64> {
    conn.execute("BEGIN")
        .await
        .context("failed to begin anchor activation transaction")?;
    let supersede_result = sqlx::query(
        "WITH new_pnus AS (
             SELECT pnu
             FROM catalog.parcel_marker_anchor
             WHERE generation_run_id = $1
         )
         UPDATE catalog.parcel_marker_anchor old
         SET is_active = false,
             superseded_at_utc = now(),
             updated_at = now(),
             version = old.version + 1
         FROM new_pnus
         WHERE old.pnu = new_pnus.pnu
           AND old.is_active = true
           AND old.generation_run_id <> $1",
    )
    .bind(generation_run_id)
    .execute(&mut *conn)
    .await
    .context("failed to supersede previous active anchors")?;
    sqlx::query(
        "UPDATE catalog.parcel_marker_anchor
         SET is_active = true,
             activated_at_utc = now(),
             superseded_at_utc = NULL,
             updated_at = now(),
             version = version + 1
         WHERE generation_run_id = $1",
    )
    .bind(generation_run_id)
    .execute(&mut *conn)
    .await
    .context("failed to activate new anchors")?;
    conn.execute("COMMIT")
        .await
        .context("failed to commit anchor activation transaction")?;
    Ok(supersede_result.rows_affected())
}

async fn mark_generation_succeeded(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
    config: &StreamingAnchorConfig,
    evidence: &ExecutionEvidence,
    activated_row_count: u64,
    superseded_row_count: u64,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE catalog.parcel_marker_anchor_generation_run
         SET status = 'succeeded',
             loaded_row_count = $3,
             rejected_row_count = 0,
             quality_report = $4,
             finished_at = now(),
             updated_at = now(),
             version = version + 1
         WHERE id = $1 AND source_snapshot_id = $2",
    )
    .bind(generation_run_id)
    .bind(config.source_snapshot_id.as_str())
    .bind(u64_to_i64("activated_row_count", activated_row_count)?)
    .bind(json!({
        "object_count": evidence.object_count,
        "expected_row_count": evidence.expected_row_count,
        "activated_row_count": activated_row_count,
        "superseded_row_count": superseded_row_count,
        "load_strategy": "r2-jsonl-copy-stage-per-object-anchor-only",
        "anchor_sql": "ST_Transform((ST_MaximumInscribedCircle(geom_5179)).center, 4326)"
    }))
    .execute(&mut *conn)
    .await
    .context("failed to mark streaming anchor generation succeeded")?;
    Ok(())
}

async fn mark_generation_failed(
    conn: &mut PgConnection,
    generation_run_id: Uuid,
    source_snapshot_id: &str,
    error_message: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE catalog.parcel_marker_anchor_generation_run
         SET status = 'failed',
             loaded_row_count = (
                 SELECT count(*) FROM catalog.parcel_marker_anchor
                 WHERE generation_run_id = $1
             ),
             error_message = left($3, 4000),
             finished_at = now(),
             updated_at = now(),
             version = version + 1
         WHERE id = $1 AND source_snapshot_id = $2 AND status = 'running'",
    )
    .bind(generation_run_id)
    .bind(source_snapshot_id)
    .bind(error_message)
    .execute(&mut *conn)
    .await
    .context("failed to mark streaming anchor generation failed")?;
    Ok(())
}

fn write_summary(path: &Path, report: &StreamingAnchorSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report)
        .context("failed to serialize streaming anchor rebuild summary")?;
    fs::write(path, payload).with_context(|| {
        format!(
            "failed to write streaming anchor summary {}",
            path.display()
        )
    })
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
        bail!("algorithm version must match the parcel_marker_anchor contract");
    }
    Ok(())
}

fn i64_to_u64(label: &str, value: i64) -> anyhow::Result<u64> {
    u64::try_from(value).with_context(|| format!("{label} cannot be negative"))
}

fn u64_to_i64(label: &str, value: u64) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} overflows Postgres BIGINT"))
}

#[cfg(test)]
mod tests {
    use super::{
        assert_bounded_db_projection, parse_copy_buffer_bytes, validate_algorithm_version,
        StreamingAnchorConfig, DEFAULT_COPY_BUFFER_BYTES, DEFAULT_MAX_BOUNDED_OBJECT_COUNT,
        DEFAULT_MAX_BOUNDED_ROW_COUNT,
    };
    use crate::postgis_parcel_boundary_mirror_national_rebuild::ExecutionEvidence;
    use std::path::PathBuf;

    #[test]
    fn copy_buffer_bytes_are_bounded() -> anyhow::Result<()> {
        assert_eq!(parse_copy_buffer_bytes("1048576")?, 1_048_576);
        assert!(parse_copy_buffer_bytes("1024").is_err());
        Ok(())
    }

    #[test]
    fn algorithm_version_uses_contract_shape() -> anyhow::Result<()> {
        validate_algorithm_version("postgis-st_maximuminscribedcircle-v1")?;
        assert!(validate_algorithm_version("PostGIS").is_err());
        Ok(())
    }

    #[test]
    fn streaming_anchor_db_projection_refuses_national_evidence() {
        let evidence = ExecutionEvidence {
            object_count: 85,
            expected_row_count: 39_862_472,
            objects: Vec::new(),
        };
        let config = StreamingAnchorConfig {
            database_url: "postgres://example.invalid/foundation_platform".to_owned(),
            execution_evidence_path: PathBuf::from("target/audit/evidence.json"),
            source_snapshot_id: "iceberg:parcel-boundaries-snapshot-001".to_owned(),
            algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
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

        assert!(error.contains("bounded only"));
    }
}
