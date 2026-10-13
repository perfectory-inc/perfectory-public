//! National `PostGIS` rebuild command for `silver.parcel_boundaries` R2 handoff shards.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::Utc;
use foundation_outbox::R2ObjectStorage;
use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_infrastructure::IcebergRestCatalog;
use serde::Serialize;
use serde_json::json;
use sqlx::{Connection, Executor, PgConnection};
use uuid::Uuid;

use crate::parcel_boundary_handoff::{
    handoff_objects_from_source_contract, parse_stage_row, push_copy_csv_row,
    read_execution_evidence, validate_source_snapshot_id, ExecutionEvidence, HandoffObject,
    SOURCE_SRID,
};
use crate::parcel_publication_contract::{
    ParcelPublicationQuality, GEOMETRY_REPAIR_STRATEGY, PARCEL_LOGICAL_TABLE,
    QUALITY_SCHEMA_VERSION,
};
use crate::r2_command_support::lakehouse_catalog_config_from_env_file;

const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.postgis_parcel_boundary_mirror_national_rebuild_summary.v1";
const TARGET_SRID: i32 = 5179;
const DEFAULT_COPY_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const MIN_COPY_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_COPY_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BOUNDED_OBJECT_COUNT: u64 = 1;
const DEFAULT_MAX_BOUNDED_ROW_COUNT: u64 = 1_000_000;

/// Runs the national `PostGIS` parcel-boundary mirror rebuild.
pub async fn run() -> anyhow::Result<()> {
    let config = RebuildConfig::from_env()?;
    let evidence = read_execution_evidence(&config.execution_evidence_path)?;
    run_with(config, evidence).await
}

/// Runs the contract-driven national rebuild (root ADR-0082): the ADR-0067 source contract
/// derives the complete sigungu handoff set, the operator supplies the measured national row
/// count, and the named snapshot must be the table's current one — a mirror of anything other
/// than silver's present truth refuses to exist.
pub async fn run_from_contract() -> anyhow::Result<()> {
    let (config, contract_path, env_file) = RebuildConfig::from_env_for_contract()?;
    let contract_json = fs::read_to_string(&contract_path).with_context(|| {
        format!(
            "failed to read parcel source contract {}",
            contract_path.display()
        )
    })?;
    let objects = handoff_objects_from_source_contract(&contract_json)?;
    let expected_row_count = config
        .expected_row_count
        .context("contract rebuild requires an expected row count")?;
    let evidence = ExecutionEvidence {
        object_count: u64::try_from(objects.len()).context("object count overflow")?,
        expected_row_count,
        objects,
    };

    let catalog = IcebergRestCatalog::new(lakehouse_catalog_config_from_env_file(&env_file)?)
        .context("failed to initialise Iceberg REST catalog for the contract rebuild")?;
    let current = catalog
        .get_current_snapshot(PARCEL_LOGICAL_TABLE)
        .await
        .context("failed to load the current silver.parcel_boundaries snapshot")?
        .context("silver.parcel_boundaries has no current snapshot")?;
    let current_snapshot = format!("iceberg:{}", current.snapshot_id);
    if config.source_snapshot_id != current_snapshot {
        bail!(
            "the named snapshot {} is not the table's current snapshot {current_snapshot}; a \
             national mirror may only state silver's present truth",
            config.source_snapshot_id
        );
    }

    run_with(config, evidence).await
}

async fn run_with(config: RebuildConfig, evidence: ExecutionEvidence) -> anyhow::Result<()> {
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
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    expected_row_count: Option<u64>,
    max_bounded_object_count: u64,
    max_bounded_row_count: u64,
    copy_buffer_bytes: usize,
    summary_path: Option<PathBuf>,
    scope: RunScopeKind,
}

/// Which publication scope this rebuild's run row states (root ADR-0082).
///
/// Bounded is the QA lane and stays capped; the contract lane may state the national scope
/// because completeness is proven upstream — the derived object set must equal the ADR-0067
/// contract's whole sigungu band and the operator's national row count must match what was
/// actually copied, or the run fails instead of shrinking its claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunScopeKind {
    Bounded,
    NationalFromContract,
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
    /// Absent for contract-lane objects, whose row counts are judged on the national total.
    expected_row_count: Option<u64>,
    copied_row_count: u64,
    inserted_row_count: u64,
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
            source_record_id: parse_uuid_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_RECORD_ID",
            )?,
            source_file_asset_id: parse_uuid_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_FILE_ASSET_ID",
            )?,
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
            scope: RunScopeKind::Bounded,
        })
    }

    /// Environment for the contract-driven national rebuild (root ADR-0082).
    ///
    /// The expected national row count is required here: with no per-object counts in the
    /// contract, the total is the load's only row-count gate, and it must be an operator-measured
    /// value rather than whatever happened to arrive.
    fn from_env_for_contract() -> anyhow::Result<(Self, PathBuf, PathBuf)> {
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
        let expected_row_count = parse_positive_u64(
            &required_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_EXPECTED_ROW_COUNT")?,
            "expected row count",
        )?;
        let contract_path = PathBuf::from(
            optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_CONTRACT_PATH",
            )?
            .unwrap_or_else(|| {
                "infra/lakehouse/contracts/vworld-parcel-source-objects.json".to_owned()
            }),
        );
        let env_file = PathBuf::from(
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_ENV_FILE")?
                .unwrap_or_else(|| ".env.local".to_owned()),
        );
        let copy_buffer_bytes =
            optional_env("FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_COPY_BUFFER_BYTES")?
                .map(|value| parse_copy_buffer_bytes(&value))
                .transpose()?
                .unwrap_or(DEFAULT_COPY_BUFFER_BYTES);
        let config = Self {
            database_url: required_env("DATABASE_URL")?,
            execution_evidence_path: contract_path.clone(),
            source_snapshot_id,
            source_record_id: parse_uuid_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_RECORD_ID",
            )?,
            source_file_asset_id: parse_uuid_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SOURCE_FILE_ASSET_ID",
            )?,
            expected_row_count: Some(expected_row_count),
            max_bounded_object_count: u64::MAX,
            max_bounded_row_count: u64::MAX,
            copy_buffer_bytes,
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_POSTGIS_PARCEL_BOUNDARY_MIRROR_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
            scope: RunScopeKind::NationalFromContract,
        };
        Ok((config, contract_path, env_file))
    }
}
async fn execute_rebuild(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    config: &RebuildConfig,
    evidence: &ExecutionEvidence,
    rebuild_run_id: Uuid,
) -> anyhow::Result<RebuildSummary> {
    // The caps guard the bounded QA lane. The contract lane replaces them with a stronger pair
    // of gates already applied by its entry point: the object set must equal the ADR-0067
    // contract's complete sigungu band, and the operator's national row count must match what is
    // actually copied (root ADR-0082).
    if matches!(config.scope, RunScopeKind::Bounded) {
        assert_bounded_db_projection(evidence, config)?;
    }
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
            config.source_record_id,
            config.source_file_asset_id,
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

    let validation = validate_loaded_mirror(conn, rebuild_run_id).await?;
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
        source_table: PARCEL_LOGICAL_TABLE,
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
          rejected_row_count, quality_report, publication_scope, publication_limits,
          source_record_id, source_file_asset_id, started_at)
         VALUES ($1, $2, $3, $4, 'planned', 0, 0, $5, $6, $7, $8, $9, now())",
    )
    .bind(rebuild_run_id)
    .bind(config.source_snapshot_id.as_str())
    .bind(PARCEL_LOGICAL_TABLE)
    .bind(TARGET_SRID)
    .bind(json!({
        "execution_evidence_path": config.execution_evidence_path.display().to_string(),
        "object_count": evidence.object_count,
        "expected_row_count": evidence.expected_row_count,
        "source_srid": format!("EPSG:{SOURCE_SRID}"),
        "target_srid": format!("EPSG:{TARGET_SRID}"),
        "geometry_repair_strategy": GEOMETRY_REPAIR_STRATEGY,
        "load_strategy": "r2-jsonl-copy-stage-per-object"
    }))
    .bind(match config.scope {
        RunScopeKind::Bounded => json!({"kind": "bounded", "complete": false}),
        // The DB CHECK requires all-null limits with the national scope, and the evidence
        // writer requires exactly this shape — the claim is legal only because the contract
        // lane proved completeness before this row existed (root ADR-0082).
        RunScopeKind::NationalFromContract => json!({"kind": "national", "complete": true}),
    })
    .bind(match config.scope {
        RunScopeKind::Bounded => json!({
            "object_limit": config.max_bounded_object_count,
            "row_limit": config.max_bounded_row_count,
            "shard_limit": evidence.object_count
        }),
        RunScopeKind::NationalFromContract => {
            json!({"object_limit": null, "row_limit": null, "shard_limit": null})
        }
    })
    .bind(config.source_record_id)
    .bind(config.source_file_asset_id)
    .execute(&mut *conn)
    .await
    .context("failed to insert PostGIS mirror rebuild run")?;
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
            SET status = 'running', updated_at = now(), version = version + 1
          WHERE id = $1 AND status = 'planned'",
    )
    .bind(rebuild_run_id)
    .execute(&mut *conn)
    .await
    .context("failed to start PostGIS mirror rebuild run")?;
    Ok(())
}

async fn prepare_target_tables(conn: &mut PgConnection) -> anyhow::Result<()> {
    assert_mirror_table_is_logged(conn).await?;
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

async fn assert_mirror_table_is_logged(conn: &mut PgConnection) -> anyhow::Result<()> {
    let relpersistence = sqlx::query_scalar::<_, String>(
        "SELECT relpersistence::text
         FROM pg_class
         WHERE oid = 'serving_postgis.parcel_boundary_mirror'::regclass",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to inspect parcel_boundary_mirror persistence")?;
    if relpersistence != "p" {
        bail!(
            "serving_postgis.parcel_boundary_mirror must be LOGGED for durable run-scoped evidence"
        );
    }
    Ok(())
}

async fn load_handoff_object(
    conn: &mut PgConnection,
    storage: &R2ObjectStorage,
    object: &HandoffObject,
    rebuild_run_id: Uuid,
    source_snapshot_id: &str,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    copy_buffer_bytes: usize,
) -> anyhow::Result<ObjectLoadSummary> {
    conn.execute("TRUNCATE TABLE parcel_boundary_mirror_load_stage")
        .await
        .context("failed to truncate PostGIS mirror load stage")?;
    let object_bytes = storage
        .get_object_bytes(object.object_key.as_str())
        .await
        .with_context(|| format!("failed to read R2 handoff object {}", object.object_key))?;
    // The ADR-0067 contract names its handoff objects with a `.jsonl.gz` suffix; the API-lane
    // shards are plain. Decoding by suffix keeps one COPY path for both.
    let object_bytes = if object.object_key.ends_with(".gz") {
        let mut decoded = Vec::with_capacity(object_bytes.len().saturating_mul(4));
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(object_bytes.as_slice()),
            &mut decoded,
        )
        .with_context(|| format!("failed to gunzip R2 handoff object {}", object.object_key))?;
        decoded
    } else {
        object_bytes
    };

    let copied_row_count = copy_object_to_stage(conn, object, &object_bytes, copy_buffer_bytes)
        .await
        .with_context(|| format!("failed to copy handoff object {}", object.object_key))?;
    if let Some(expected) = object.row_count {
        if copied_row_count != expected {
            bail!(
                "handoff object {} row count mismatch: expected={expected} actual={copied_row_count}",
                object.object_key
            );
        }
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

    let inserted_row_count = insert_stage_into_mirror(
        conn,
        rebuild_run_id,
        source_snapshot_id,
        source_record_id,
        source_file_asset_id,
    )
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
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
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
             $4::uuid,
             $5::uuid,
             source_object_key,
             boundary_id,
             NULL::uuid,
             NULL::uuid,
             geometry_checksum_sha256,
             properties,
             ST_Multi(
                 ST_CollectionExtract(
                     ST_MakeValid(
                         ST_Transform(
                             ST_CollectionExtract(
                                 ST_MakeValid(
                                     ST_SetSRID(
                                         ST_GeomFromWKB(decode(geometry_wkb_hex, 'hex')),
                                         4326
                                     )
                                 ),
                                 3
                             ),
                             5179
                         )
                     ),
                     3
                 )
             ),
             now(),
             now(),
             1
         FROM parcel_boundary_mirror_load_stage",
    )
    .bind(PARCEL_LOGICAL_TABLE)
    .bind(rebuild_run_id)
    .bind(source_snapshot_id)
    .bind(source_record_id)
    .bind(source_file_asset_id)
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
    rebuild_run_id: Uuid,
) -> anyhow::Result<MirrorValidation> {
    Ok(MirrorValidation {
        loaded_rows: count_mirror_where(conn, rebuild_run_id, "TRUE").await?,
        invalid_srid: count_mirror_where(conn, rebuild_run_id, "ST_SRID(geom) <> 5179").await?,
        invalid_geometry: count_mirror_where(conn, rebuild_run_id, "NOT ST_IsValid(geom)").await?,
        empty_geometry: count_mirror_where(conn, rebuild_run_id, "ST_IsEmpty(geom)").await?,
        nonpositive_area: count_mirror_where(conn, rebuild_run_id, "ST_Area(geom) <= 0").await?,
    })
}

async fn count_mirror_where(
    conn: &mut PgConnection,
    rebuild_run_id: Uuid,
    predicate: &str,
) -> anyhow::Result<u64> {
    let sql = format!(
        "SELECT count(*)
         FROM serving_postgis.parcel_boundary_mirror
         WHERE rebuild_run_id = $1 AND ({predicate})"
    );
    let count = sqlx::query_scalar::<_, i64>(&sql)
        .bind(rebuild_run_id)
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
    let quality = publication_quality(evidence, validation);
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
    .bind(serde_json::to_value(quality).context("failed to serialize parcel publication quality")?)
    .execute(&mut *conn)
    .await
    .context("failed to mark PostGIS mirror rebuild succeeded")?;
    Ok(())
}

fn publication_quality(
    evidence: &ExecutionEvidence,
    validation: &MirrorValidation,
) -> ParcelPublicationQuality {
    ParcelPublicationQuality {
        schema_version: QUALITY_SCHEMA_VERSION.to_owned(),
        object_count: evidence.object_count,
        expected_row_count: evidence.expected_row_count,
        loaded_row_count: validation.loaded_rows,
        invalid_srid_count: validation.invalid_srid,
        invalid_geometry_count: validation.invalid_geometry,
        empty_geometry_count: validation.empty_geometry,
        nonpositive_area_count: validation.nonpositive_area,
        source_srid: format!("EPSG:{SOURCE_SRID}"),
        target_srid: format!("EPSG:{TARGET_SRID}"),
        geometry_repair_strategy: GEOMETRY_REPAIR_STRATEGY.to_owned(),
    }
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
                 WHERE rebuild_run_id = $1
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

fn i64_to_u64(label: &str, value: i64) -> anyhow::Result<u64> {
    u64::try_from(value).with_context(|| format!("{label} cannot be negative"))
}

fn u64_to_i64(label: &str, value: u64) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} overflows Postgres BIGINT"))
}

fn parse_uuid_env(name: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(required_env(name)?.as_str()).with_context(|| format!("{name} must be a UUID"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundation_disposable_database::{run_in_disposable_database, TestResult};

    use crate::test_support::MIGRATOR;

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
            source_record_id: Uuid::nil(),
            source_file_asset_id: Uuid::max(),
            expected_row_count: None,
            max_bounded_object_count: DEFAULT_MAX_BOUNDED_OBJECT_COUNT,
            max_bounded_row_count: DEFAULT_MAX_BOUNDED_ROW_COUNT,
            copy_buffer_bytes: DEFAULT_COPY_BUFFER_BYTES,
            summary_path: None,
            scope: RunScopeKind::Bounded,
        };

        let error = assert_bounded_db_projection(&evidence, &config)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();

        assert!(error.contains("bounded QA only"));
    }

    #[test]
    fn terminal_quality_reuses_the_counts_already_computed_by_rebuild() {
        let evidence = ExecutionEvidence {
            object_count: 3,
            expected_row_count: 7,
            objects: Vec::new(),
        };
        let validation = MirrorValidation {
            loaded_rows: 7,
            invalid_srid: 0,
            invalid_geometry: 0,
            empty_geometry: 0,
            nonpositive_area: 0,
        };

        let quality = publication_quality(&evidence, &validation);

        assert_eq!(quality.schema_version, QUALITY_SCHEMA_VERSION);
        assert_eq!(quality.object_count, evidence.object_count);
        assert_eq!(quality.expected_row_count, evidence.expected_row_count);
        assert_eq!(quality.loaded_row_count, validation.loaded_rows);
        assert_eq!(quality.geometry_repair_strategy, GEOMETRY_REPAIR_STRATEGY);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL 17 with PostGIS and permission to create disposable databases"]
    async fn rebuild_records_one_provenance_pair_on_the_run_and_every_loaded_row() -> TestResult {
        run_in_disposable_database("parcel_rebuild_provenance", |pool| async move {
            MIGRATOR.run(&pool).await?;
            let source_record_id = Uuid::new_v4();
            let unrelated_source_record_id = Uuid::new_v4();
            let source_file_asset_id = Uuid::new_v4();
            let rebuild_run_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO catalog.source_record
                    (id, source, external_id, checksum_sha256, raw_object_key)
                 VALUES ($1, 'parcel-rebuild-test', $2, repeat('a', 64), $3)",
            )
            .bind(source_record_id)
            .bind(format!("parcel-rebuild-{source_record_id}"))
            .bind(format!("silver/parcel-boundaries/{source_record_id}/metadata.json"))
            .execute(&pool)
            .await?;
            sqlx::query(
                "INSERT INTO catalog.file_asset
                    (id, object_key, mime_type, size_bytes, checksum_sha256,
                     source_record_id, visibility)
                 VALUES ($1, $2, 'application/json', 1, repeat('b', 64), $3, 'internal')",
            )
            .bind(source_file_asset_id)
            .bind(format!("silver/parcel-boundaries/{source_file_asset_id}/manifest.json"))
            .bind(source_record_id)
            .execute(&pool)
            .await?;
            sqlx::query(
                "INSERT INTO catalog.source_record
                    (id, source, external_id, checksum_sha256, raw_object_key)
                 VALUES ($1, 'parcel-rebuild-test', $2, repeat('d', 64), $3)",
            )
            .bind(unrelated_source_record_id)
            .bind(format!("parcel-rebuild-{unrelated_source_record_id}"))
            .bind(format!(
                "silver/parcel-boundaries/{unrelated_source_record_id}/metadata.json"
            ))
            .execute(&pool)
            .await?;

            let config = RebuildConfig {
                database_url: "postgres://unused.invalid/foundation_platform".to_owned(),
                execution_evidence_path: PathBuf::from("target/audit/evidence.json"),
                source_snapshot_id: "iceberg:841361364657368626".to_owned(),
                source_record_id,
                source_file_asset_id,
                expected_row_count: None,
                max_bounded_object_count: DEFAULT_MAX_BOUNDED_OBJECT_COUNT,
                max_bounded_row_count: DEFAULT_MAX_BOUNDED_ROW_COUNT,
                copy_buffer_bytes: DEFAULT_COPY_BUFFER_BYTES,
                summary_path: None,
                scope: RunScopeKind::Bounded,
            };
            let evidence = ExecutionEvidence {
                object_count: 1,
                expected_row_count: 1,
                objects: Vec::new(),
            };
            let mut conn = pool.acquire().await?;
            let mismatched_config = RebuildConfig {
                database_url: "postgres://unused.invalid/foundation_platform".to_owned(),
                execution_evidence_path: PathBuf::from("target/audit/evidence.json"),
                source_snapshot_id: "iceberg:841361364657368626".to_owned(),
                source_record_id: unrelated_source_record_id,
                source_file_asset_id,
                expected_row_count: None,
                max_bounded_object_count: DEFAULT_MAX_BOUNDED_OBJECT_COUNT,
                max_bounded_row_count: DEFAULT_MAX_BOUNDED_ROW_COUNT,
                copy_buffer_bytes: DEFAULT_COPY_BUFFER_BYTES,
                summary_path: None,
                scope: RunScopeKind::Bounded,
            };
            let mismatch_error = insert_rebuild_run(
                &mut conn,
                Uuid::new_v4(),
                &mismatched_config,
                &evidence,
            )
            .await
            .expect_err("an asset from another source record must be rejected");
            assert!(
                format!("{mismatch_error:#}").contains(
                    "parcel_boundary_mirror_rebuild_run_source_asset_pair_fkey"
                ),
                "unexpected provenance-pair rejection: {mismatch_error:#}"
            );

            insert_rebuild_run(&mut conn, rebuild_run_id, &config, &evidence).await?;
            prepare_target_tables(&mut conn).await?;
            sqlx::query(
                "INSERT INTO parcel_boundary_mirror_load_stage
                    (pnu, boundary_id, source_object_key, geometry_wkb_hex,
                     geometry_checksum_sha256, properties)
                 VALUES ('9999900101100010001', 'boundary-1', 'silver/part-0001.jsonl',
                         encode(ST_AsBinary(ST_GeomFromText(
                             'POLYGON((127.1231 36.1231,127.1232 36.1231,127.1232 36.1232,127.1231 36.1232,127.1231 36.1231))',
                             4326
                         )), 'hex'), repeat('c', 64), '{}'::jsonb)",
            )
            .execute(&mut *conn)
            .await?;
            assert_eq!(
                insert_stage_into_mirror(
                    &mut conn,
                    rebuild_run_id,
                    config.source_snapshot_id.as_str(),
                    source_record_id,
                    source_file_asset_id,
                )
                .await?,
                1
            );

            let (run_record, run_asset, mismatched_rows): (Option<Uuid>, Option<Uuid>, i64) =
                sqlx::query_as(
                    "SELECT run.source_record_id,
                            run.source_file_asset_id,
                            count(*) FILTER (
                                WHERE mirror.source_record_id IS DISTINCT FROM run.source_record_id
                                   OR mirror.source_file_asset_id IS DISTINCT FROM run.source_file_asset_id
                            )::bigint
                       FROM serving_postgis.parcel_boundary_mirror_rebuild_run AS run
                       JOIN serving_postgis.parcel_boundary_mirror AS mirror
                         ON mirror.rebuild_run_id = run.id
                      WHERE run.id = $1
                      GROUP BY run.source_record_id, run.source_file_asset_id",
                )
                .bind(rebuild_run_id)
                .fetch_one(&mut *conn)
                .await?;
            assert_eq!(run_record, Some(source_record_id));
            assert_eq!(run_asset, Some(source_file_asset_id));
            assert_eq!(mismatched_rows, 0);
            Ok(())
        })
        .await
    }
}
