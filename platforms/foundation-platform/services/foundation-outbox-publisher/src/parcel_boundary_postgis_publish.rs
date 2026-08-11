//! Materialises one sealed parcel source evidence row into the append-only PostGIS projection.
//!
//! R2/Iceberg remains canonical. The operator names only the append-only evidence row that already
//! binds an Iceberg snapshot, Catalog provenance, a durable run-keyed mirror source set, complete
//! quality measurements, and the canonical projection-content digest. This command derives every
//! publication identity from that row and independently rechecks the source and copied target.

use anyhow::{bail, Context};
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::public_data_control_support::{optional_bool_env, required_env_value};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_CONFIRM";
const SOURCE_EVIDENCE_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_SOURCE_EVIDENCE_ID";
const PARCEL_UNIT_KEY: &str = "parcels";
const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_publication_execution_evidence.v1";
const QUALITY_SCHEMA_VERSION: &str = "foundation-platform.parcel_publication_quality.v1";
const GEOMETRY_REPAIR_STRATEGY: &str = "postgis-make-valid-v1";
const CONTENT_DIGEST_PREFIX: &[u8] = b"perfectory.parcel-projection-content.v1\0";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let pool = PgPool::connect(&config.database_url)
        .await
        .context("failed to connect to DATABASE_URL for parcel boundary PostGIS publish")?;
    let opened = open_load(&pool, &config).await?;
    match materialise(&pool, &config, &opened).await {
        Ok(summary) => {
            println!(
                "parcel-boundary-postgis-publish-ok source_evidence_id={} \
                 mirror_rebuild_run_id={} canonical_iceberg_snapshot_id={} \
                 data_revision={} projection_load_id={} source_rows={} loaded_rows={} rejected_rows=0",
                config.source_evidence_id,
                summary.mirror_rebuild_run_id,
                opened.canonical_snapshot_id,
                opened.data_revision,
                opened.projection_load_id,
                summary.source_rows,
                summary.loaded_rows,
            );
            Ok(())
        }
        Err(error) => Err(match close_failed_load(&pool, &opened, &error).await {
            Ok(()) => error.context(format!(
                "projection load {} was closed as failed",
                opened.projection_load_id
            )),
            Err(close_error) => error.context(format!(
                "projection load {} could not be closed as failed: {close_error:#}",
                opened.projection_load_id
            )),
        }),
    }
}

struct Config {
    database_url: String,
    source_evidence_id: Uuid,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if optional_bool_env(CONFIRM_ENV)? != Some(true) {
            bail!("{CONFIRM_ENV}=1 is required before writing parcel boundary PostGIS");
        }
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            source_evidence_id: uuid_env(SOURCE_EVIDENCE_ENV)?,
        })
    }
}

struct OpenedLoad {
    projection_load_id: Uuid,
    publication_unit_id: Uuid,
    data_revision: Uuid,
    source_evidence_id: Uuid,
    canonical_snapshot_id: String,
}

struct MaterialisationSummary {
    mirror_rebuild_run_id: Uuid,
    source_rows: i64,
    loaded_rows: i64,
}

struct SourceEvidence {
    id: Uuid,
    mirror_rebuild_run_id: Uuid,
    canonical_snapshot_id: String,
    mirror_source_snapshot_id: String,
    iceberg_logical_table: String,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    source_row_count: i64,
    projection_content_sha256: String,
    quality: ParcelPublicationQuality,
}

#[derive(Deserialize)]
struct ParcelPublicationQuality {
    schema_version: String,
    object_count: u64,
    expected_row_count: u64,
    loaded_row_count: u64,
    invalid_srid_count: u64,
    invalid_geometry_count: u64,
    empty_geometry_count: u64,
    nonpositive_area_count: u64,
    source_srid: String,
    target_srid: String,
    geometry_repair_strategy: String,
}

struct SourceSetStats {
    row_count: i64,
    object_count: i64,
    invalid_srid_count: i64,
    invalid_geometry_count: i64,
    empty_geometry_count: i64,
    nonpositive_area_count: i64,
    provenance_mismatch_count: i64,
}

async fn open_load(pool: &PgPool, config: &Config) -> anyhow::Result<OpenedLoad> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut *transaction)
        .await?;

    let evidence = read_and_verify_evidence(&mut transaction, config.source_evidence_id).await?;
    let publication_unit_id = ensure_parcel_unit(&mut transaction).await?;
    let data_revision =
        register_or_reuse_revision(&mut transaction, publication_unit_id, &evidence).await?;
    let projection_load_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
             source_evidence_id, status)
         VALUES ($1, $2, $3, $4, $5, 'running')",
    )
    .bind(projection_load_id)
    .bind(publication_unit_id)
    .bind(data_revision)
    .bind(&evidence.canonical_snapshot_id)
    .bind(evidence.id)
    .execute(&mut *transaction)
    .await
    .context("failed to open the parcel PostGIS projection load")?;
    transaction.commit().await?;

    Ok(OpenedLoad {
        projection_load_id,
        publication_unit_id,
        data_revision,
        source_evidence_id: evidence.id,
        canonical_snapshot_id: evidence.canonical_snapshot_id,
    })
}

async fn read_and_verify_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    evidence_id: Uuid,
) -> anyhow::Result<SourceEvidence> {
    let row = sqlx::query(
        "SELECT evidence.id, evidence.mirror_rebuild_run_id,
                evidence.mirror_rebuild_run_status AS evidence_run_status,
                evidence.mirror_rebuild_rejected_row_count AS evidence_rejected_row_count,
                evidence.canonical_iceberg_snapshot_id,
                evidence.mirror_source_snapshot_id,
                evidence.iceberg_logical_table,
                evidence.source_record_id, evidence.source_file_asset_id,
                evidence.execution_evidence_schema_version,
                evidence.source_row_count, evidence.projection_content_sha256::text,
                evidence.quality_schema_version, evidence.sealed_at IS NOT NULL AS is_sealed,
                run.status AS run_status, run.source_snapshot_id AS run_source_snapshot_id,
                run.source_table AS run_source_table, run.source_record_id AS run_source_record_id,
                run.source_file_asset_id AS run_source_file_asset_id, run.srid AS run_srid,
                run.loaded_row_count AS run_loaded_row_count,
                run.rejected_row_count AS run_rejected_row_count,
                run.quality_report, run.finished_at IS NOT NULL AS run_finished
           FROM catalog.parcel_publication_source_evidence AS evidence
           JOIN serving_postgis.parcel_boundary_mirror_rebuild_run AS run
             ON run.id = evidence.mirror_rebuild_run_id
          WHERE evidence.id = $1
          FOR KEY SHARE OF evidence
          FOR SHARE OF run",
    )
    .bind(evidence_id)
    .fetch_optional(&mut **transaction)
    .await?
    .with_context(|| {
        format!(
            "{SOURCE_EVIDENCE_ENV}={evidence_id} names no sealed parcel publication source evidence; \
             bounded-QA mirror runs are not publication evidence"
        )
    })?;

    let execution_schema_version: String = row.try_get("execution_evidence_schema_version")?;
    if execution_schema_version != EXECUTION_SCHEMA_VERSION {
        bail!(
            "parcel source evidence {evidence_id} is not publication-eligible: execution schema \
             {execution_schema_version} cannot prove a full Iceberg commit, production cutover, \
             and national rollout; expected {EXECUTION_SCHEMA_VERSION}"
        );
    }

    let evidence_run_status: String = row.try_get("evidence_run_status")?;
    let run_status: String = row.try_get("run_status")?;
    let evidence_rejected: i64 = row.try_get("evidence_rejected_row_count")?;
    let run_rejected: i64 = row.try_get("run_rejected_row_count")?;
    if evidence_rejected != 0 || run_rejected != 0 {
        bail!(
            "parcel source evidence {evidence_id} has rejected_row_count={run_rejected}; \
             every publication source rejection count must be zero"
        );
    }
    if evidence_run_status != "succeeded" || run_status != "succeeded" {
        bail!(
            "parcel source evidence {evidence_id} names run status evidence={evidence_run_status}, \
             run={run_status}; both must be succeeded"
        );
    }

    let canonical_snapshot_id: String = row.try_get("canonical_iceberg_snapshot_id")?;
    let mirror_source_snapshot_id: String = row.try_get("mirror_source_snapshot_id")?;
    let iceberg_logical_table: String = row.try_get("iceberg_logical_table")?;
    let source_record_id: Uuid = row.try_get("source_record_id")?;
    let source_file_asset_id: Uuid = row.try_get("source_file_asset_id")?;
    let source_row_count: i64 = row.try_get("source_row_count")?;
    let run_source_snapshot_id: String = row.try_get("run_source_snapshot_id")?;
    let run_source_table: String = row.try_get("run_source_table")?;
    let run_source_record_id: Option<Uuid> = row.try_get("run_source_record_id")?;
    let run_source_file_asset_id: Option<Uuid> = row.try_get("run_source_file_asset_id")?;
    let run_loaded_row_count: i64 = row.try_get("run_loaded_row_count")?;
    let run_srid: i32 = row.try_get("run_srid")?;
    let run_finished: bool = row.try_get("run_finished")?;
    let is_sealed: bool = row.try_get("is_sealed")?;

    if !is_sealed
        || source_row_count <= 0
        || run_loaded_row_count != source_row_count
        || run_srid != 5179
        || !run_finished
        || iceberg_logical_table != "silver.parcel_boundaries"
        || mirror_source_snapshot_id != format!("iceberg:{canonical_snapshot_id}")
        || run_source_snapshot_id != mirror_source_snapshot_id
        || run_source_table != iceberg_logical_table
        || run_source_record_id != Some(source_record_id)
        || run_source_file_asset_id != Some(source_file_asset_id)
    {
        bail!(
            "parcel source evidence {evidence_id} no longer matches its complete sealed mirror run tuple"
        );
    }

    let quality_schema_version: String = row.try_get("quality_schema_version")?;
    let quality_report: JsonValue = row.try_get("quality_report")?;
    let quality: ParcelPublicationQuality =
        serde_json::from_value(quality_report).with_context(|| {
            format!(
            "parcel source evidence {evidence_id} requires a complete parcel publication quality \
             report including schema_version and every defect counter"
        )
        })?;
    verify_quality_report(
        evidence_id,
        &quality_schema_version,
        source_row_count,
        &quality,
    )?;

    Ok(SourceEvidence {
        id: row.try_get("id")?,
        mirror_rebuild_run_id: row.try_get("mirror_rebuild_run_id")?,
        canonical_snapshot_id,
        mirror_source_snapshot_id,
        iceberg_logical_table,
        source_record_id,
        source_file_asset_id,
        source_row_count,
        projection_content_sha256: row.try_get("projection_content_sha256")?,
        quality,
    })
}

fn verify_quality_report(
    evidence_id: Uuid,
    evidence_schema_version: &str,
    source_row_count: i64,
    quality: &ParcelPublicationQuality,
) -> anyhow::Result<()> {
    let expected_rows = u64::try_from(source_row_count)
        .context("sealed parcel source row count must be positive")?;
    if evidence_schema_version != QUALITY_SCHEMA_VERSION
        || quality.schema_version != QUALITY_SCHEMA_VERSION
        || quality.object_count == 0
        || quality.expected_row_count != expected_rows
        || quality.loaded_row_count != expected_rows
        || quality.invalid_srid_count != 0
        || quality.invalid_geometry_count != 0
        || quality.empty_geometry_count != 0
        || quality.nonpositive_area_count != 0
        || quality.source_srid != "EPSG:4326"
        || quality.target_srid != "EPSG:5179"
        || quality.geometry_repair_strategy != GEOMETRY_REPAIR_STRATEGY
    {
        bail!(
            "parcel source evidence {evidence_id} does not carry the complete zero-defect \
             {QUALITY_SCHEMA_VERSION} quality report"
        );
    }
    Ok(())
}

async fn ensure_parcel_unit(transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
         VALUES (gen_random_uuid(), $1) ON CONFLICT (unit_key) DO NOTHING",
    )
    .bind(PARCEL_UNIT_KEY)
    .execute(&mut **transaction)
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = $1",
    )
    .bind(PARCEL_UNIT_KEY)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn register_or_reuse_revision(
    transaction: &mut Transaction<'_, Postgres>,
    publication_unit_id: Uuid,
    evidence: &SourceEvidence,
) -> anyhow::Result<Uuid> {
    let proposed_revision_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.publication_revision
            (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (publication_unit_id, canonical_iceberg_snapshot_id) DO NOTHING",
    )
    .bind(proposed_revision_id)
    .bind(publication_unit_id)
    .bind(&evidence.canonical_snapshot_id)
    .bind(evidence.source_record_id)
    .execute(&mut **transaction)
    .await
    .context("failed to register the parcels publication revision")?;

    let stored = sqlx::query(
        "SELECT id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
                derived_from_administrative_revision
           FROM catalog.publication_revision
          WHERE publication_unit_id = $1 AND canonical_iceberg_snapshot_id = $2
          FOR SHARE",
    )
    .bind(publication_unit_id)
    .bind(&evidence.canonical_snapshot_id)
    .fetch_one(&mut **transaction)
    .await?;
    let stored_id: Uuid = stored.try_get("id")?;
    let stored_unit: Uuid = stored.try_get("publication_unit_id")?;
    let stored_snapshot: String = stored.try_get("canonical_iceberg_snapshot_id")?;
    let stored_source_record: Uuid = stored.try_get("source_record_id")?;
    let stored_lineage: Option<Uuid> = stored.try_get("derived_from_administrative_revision")?;

    if let Some(lineage) = stored_lineage {
        bail!(
            "publication revision {stored_id} has administrative lineage {lineage}; \
             sealed parcel evidence requires the whole stored revision to have NULL administrative lineage"
        );
    }
    if stored_unit != publication_unit_id
        || stored_snapshot != evidence.canonical_snapshot_id
        || stored_source_record != evidence.source_record_id
    {
        bail!(
            "publication revision {stored_id} already describes a different sealed publication \
             (stored unit {stored_unit}, snapshot {stored_snapshot}, source record {stored_source_record}); \
             evidence {} requires unit {publication_unit_id}, snapshot {}, source record {}",
            evidence.id,
            evidence.canonical_snapshot_id,
            evidence.source_record_id
        );
    }
    Ok(stored_id)
}

async fn materialise(
    pool: &PgPool,
    config: &Config,
    opened: &OpenedLoad,
) -> anyhow::Result<MaterialisationSummary> {
    let mut transaction = pool.begin().await?;
    let evidence = read_and_verify_evidence(&mut transaction, config.source_evidence_id).await?;
    lock_and_verify_opened_load(&mut transaction, opened, &evidence).await?;

    let source_stats = read_source_set_stats(&mut transaction, &evidence).await?;
    verify_source_set_stats(&evidence, &source_stats)?;
    let (streamed_source_rows, source_digest) = stream_projection_digest(
        &mut transaction,
        ProjectionRows::Source(evidence.mirror_rebuild_run_id),
    )
    .await?;
    if streamed_source_rows != evidence.source_row_count
        || source_digest != evidence.projection_content_sha256
    {
        bail!(
            "sealed source evidence {} records {} row(s) and content digest {}, but the locked \
             source set streamed {streamed_source_rows} row(s) with digest {source_digest}",
            evidence.id,
            evidence.source_row_count,
            evidence.projection_content_sha256
        );
    }

    let inserted_rows = append_projection(&mut transaction, opened, &evidence).await?;
    if inserted_rows != evidence.source_row_count {
        bail!(
            "parcel boundary publish offered {} row(s) and INSERT affected {inserted_rows}",
            evidence.source_row_count
        );
    }

    let target_stats = read_target_set_stats(&mut transaction, opened, &evidence).await?;
    if target_stats.row_count != evidence.source_row_count
        || target_stats.provenance_mismatch_count != 0
        || target_stats.invalid_srid_count != 0
        || target_stats.invalid_geometry_count != 0
        || target_stats.empty_geometry_count != 0
        || target_stats.nonpositive_area_count != 0
    {
        bail!(
            "parcel target postcondition failed: rows={}, lineage_mismatches={}, invalid_srid={}, \
             invalid_geometry={}, empty_geometry={}, nonpositive_area={}",
            target_stats.row_count,
            target_stats.provenance_mismatch_count,
            target_stats.invalid_srid_count,
            target_stats.invalid_geometry_count,
            target_stats.empty_geometry_count,
            target_stats.nonpositive_area_count
        );
    }

    let (streamed_target_rows, target_digest) = stream_projection_digest(
        &mut transaction,
        ProjectionRows::Target(opened.projection_load_id),
    )
    .await?;
    if streamed_target_rows != evidence.source_row_count
        || target_digest != source_digest
        || target_digest != evidence.projection_content_sha256
    {
        bail!(
            "target content digest {target_digest} over {streamed_target_rows} row(s) does not \
             match sealed source digest {} over {} row(s)",
            evidence.projection_content_sha256,
            evidence.source_row_count
        );
    }

    close_succeeded_load(&mut transaction, opened, streamed_target_rows).await?;
    transaction.commit().await?;
    Ok(MaterialisationSummary {
        mirror_rebuild_run_id: evidence.mirror_rebuild_run_id,
        source_rows: streamed_source_rows,
        loaded_rows: streamed_target_rows,
    })
}

async fn lock_and_verify_opened_load(
    transaction: &mut Transaction<'_, Postgres>,
    opened: &OpenedLoad,
    evidence: &SourceEvidence,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT load.status, load.publication_unit_id, load.data_revision,
                load.canonical_iceberg_snapshot_id, load.source_evidence_id,
                unit.unit_key, revision.source_record_id,
                revision.derived_from_administrative_revision
           FROM serving_postgis.spatial_projection_load AS load
           JOIN catalog.vector_tile_publication_unit AS unit
             ON unit.id = load.publication_unit_id
           JOIN catalog.publication_revision AS revision
             ON revision.id = load.data_revision
            AND revision.publication_unit_id = load.publication_unit_id
            AND revision.canonical_iceberg_snapshot_id = load.canonical_iceberg_snapshot_id
          WHERE load.id = $1
          FOR UPDATE OF load
          FOR SHARE OF unit, revision",
    )
    .bind(opened.projection_load_id)
    .fetch_one(&mut **transaction)
    .await?;

    let status: String = row.try_get("status")?;
    let publication_unit_id: Uuid = row.try_get("publication_unit_id")?;
    let data_revision: Uuid = row.try_get("data_revision")?;
    let snapshot: String = row.try_get("canonical_iceberg_snapshot_id")?;
    let source_evidence_id: Option<Uuid> = row.try_get("source_evidence_id")?;
    let unit_key: String = row.try_get("unit_key")?;
    let revision_source_record_id: Uuid = row.try_get("source_record_id")?;
    let revision_lineage: Option<Uuid> = row.try_get("derived_from_administrative_revision")?;
    if status != "running"
        || unit_key != PARCEL_UNIT_KEY
        || data_revision != opened.data_revision
        || snapshot != evidence.canonical_snapshot_id
        || source_evidence_id != Some(evidence.id)
        || source_evidence_id != Some(opened.source_evidence_id)
        || revision_source_record_id != evidence.source_record_id
        || revision_lineage.is_some()
        || publication_unit_id != opened.publication_unit_id
    {
        bail!(
            "projection load {} no longer matches its locked evidence/revision tuple",
            opened.projection_load_id
        );
    }
    Ok(())
}

async fn read_source_set_stats(
    transaction: &mut Transaction<'_, Postgres>,
    evidence: &SourceEvidence,
) -> anyhow::Result<SourceSetStats> {
    let row = sqlx::query(
        "SELECT count(*)::bigint AS row_count,
                count(DISTINCT source_object_key)::bigint AS object_count,
                count(*) FILTER (WHERE public.st_srid(geom) <> 5179)::bigint AS invalid_srid_count,
                count(*) FILTER (WHERE NOT public.st_isvalid(geom))::bigint AS invalid_geometry_count,
                count(*) FILTER (WHERE public.st_isempty(geom))::bigint AS empty_geometry_count,
                count(*) FILTER (WHERE public.st_area(geom) <= 0)::bigint AS nonpositive_area_count,
                count(*) FILTER (
                    WHERE source_snapshot_id IS DISTINCT FROM $2
                       OR source_table IS DISTINCT FROM $3
                       OR source_record_id IS DISTINCT FROM $4
                       OR source_file_asset_id IS DISTINCT FROM $5
                )::bigint AS provenance_mismatch_count
           FROM serving_postgis.parcel_boundary_mirror
          WHERE rebuild_run_id = $1",
    )
    .bind(evidence.mirror_rebuild_run_id)
    .bind(&evidence.mirror_source_snapshot_id)
    .bind(&evidence.iceberg_logical_table)
    .bind(evidence.source_record_id)
    .bind(evidence.source_file_asset_id)
    .fetch_one(&mut **transaction)
    .await?;
    source_set_stats_from_row(&row)
}

fn source_set_stats_from_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<SourceSetStats> {
    Ok(SourceSetStats {
        row_count: row.try_get("row_count")?,
        object_count: row.try_get("object_count")?,
        invalid_srid_count: row.try_get("invalid_srid_count")?,
        invalid_geometry_count: row.try_get("invalid_geometry_count")?,
        empty_geometry_count: row.try_get("empty_geometry_count")?,
        nonpositive_area_count: row.try_get("nonpositive_area_count")?,
        provenance_mismatch_count: row.try_get("provenance_mismatch_count")?,
    })
}

fn verify_source_set_stats(
    evidence: &SourceEvidence,
    stats: &SourceSetStats,
) -> anyhow::Result<()> {
    if stats.row_count != evidence.source_row_count
        || stats.object_count != i64::try_from(evidence.quality.object_count)?
        || stats.invalid_srid_count != 0
        || stats.invalid_geometry_count != 0
        || stats.empty_geometry_count != 0
        || stats.nonpositive_area_count != 0
        || stats.provenance_mismatch_count != 0
    {
        bail!(
            "locked parcel source set does not match evidence/quality: rows={}, objects={}, \
             invalid_srid={}, invalid_geometry={}, empty_geometry={}, nonpositive_area={}, \
             lineage_mismatches={}",
            stats.row_count,
            stats.object_count,
            stats.invalid_srid_count,
            stats.invalid_geometry_count,
            stats.empty_geometry_count,
            stats.nonpositive_area_count,
            stats.provenance_mismatch_count
        );
    }
    Ok(())
}

enum ProjectionRows {
    Source(Uuid),
    Target(Uuid),
}

async fn stream_projection_digest(
    transaction: &mut Transaction<'_, Postgres>,
    rows_to_read: ProjectionRows,
) -> anyhow::Result<(i64, String)> {
    let (sql, id) = match rows_to_read {
        ProjectionRows::Source(run_id) => (
            "SELECT pnu::text AS pnu, public.st_asewkb(geom, 'NDR') AS ewkb
               FROM serving_postgis.parcel_boundary_mirror
              WHERE rebuild_run_id = $1
              ORDER BY pnu COLLATE \"C\"",
            run_id,
        ),
        ProjectionRows::Target(load_id) => (
            "SELECT pnu::text AS pnu, public.st_asewkb(geom, 'NDR') AS ewkb
               FROM serving_postgis.parcel_boundary_publication
              WHERE projection_load_id = $1
              ORDER BY pnu COLLATE \"C\"",
            load_id,
        ),
    };
    let mut rows = sqlx::query(sql).bind(id).fetch(&mut **transaction);
    let mut digest = Sha256::new();
    digest.update(CONTENT_DIGEST_PREFIX);
    let mut row_count = 0_i64;
    while let Some(row) = rows.try_next().await? {
        let pnu: String = row.try_get("pnu")?;
        if pnu.len() != 19 || !pnu.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("projection digest requires an ASCII 19-byte PNU, got {pnu:?}");
        }
        let ewkb: Vec<u8> = row.try_get("ewkb")?;
        digest.update(pnu.as_bytes());
        digest.update([0]);
        digest.update(Sha256::digest(ewkb));
        row_count += 1;
    }
    Ok((row_count, format!("{:x}", digest.finalize())))
}

async fn append_projection(
    transaction: &mut Transaction<'_, Postgres>,
    opened: &OpenedLoad,
    evidence: &SourceEvidence,
) -> anyhow::Result<i64> {
    let affected = sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_publication
            (pnu, data_revision, canonical_iceberg_snapshot_id, source_record_id,
             source_object_key, geometry_checksum_sha256, geom, properties, projection_load_id)
         SELECT mirror.pnu, $2, $3, $4, mirror.source_object_key,
                mirror.geometry_checksum_sha256, mirror.geom, mirror.properties, $5
           FROM serving_postgis.parcel_boundary_mirror AS mirror
          WHERE mirror.rebuild_run_id = $1",
    )
    .bind(evidence.mirror_rebuild_run_id)
    .bind(opened.data_revision)
    .bind(&evidence.canonical_snapshot_id)
    .bind(evidence.source_record_id)
    .bind(opened.projection_load_id)
    .execute(&mut **transaction)
    .await
    .context("failed to append parcel boundary geometry")?
    .rows_affected();
    i64::try_from(affected).context("parcel publication affected-row count does not fit bigint")
}

async fn read_target_set_stats(
    transaction: &mut Transaction<'_, Postgres>,
    opened: &OpenedLoad,
    evidence: &SourceEvidence,
) -> anyhow::Result<SourceSetStats> {
    let row = sqlx::query(
        "SELECT count(*)::bigint AS row_count,
                0::bigint AS object_count,
                count(*) FILTER (WHERE public.st_srid(target.geom) <> 5179)::bigint AS invalid_srid_count,
                count(*) FILTER (WHERE NOT public.st_isvalid(target.geom))::bigint AS invalid_geometry_count,
                count(*) FILTER (WHERE public.st_isempty(target.geom))::bigint AS empty_geometry_count,
                count(*) FILTER (WHERE public.st_area(target.geom) <= 0)::bigint AS nonpositive_area_count,
                count(*) FILTER (
                    WHERE source.pnu IS NULL
                       OR target.data_revision IS DISTINCT FROM $2
                       OR target.canonical_iceberg_snapshot_id IS DISTINCT FROM $3
                       OR target.source_record_id IS DISTINCT FROM $4
                       OR target.projection_load_id IS DISTINCT FROM $5
                       OR target.parcel_id IS NOT NULL
                       OR target.source_object_key IS DISTINCT FROM source.source_object_key
                       OR target.geometry_checksum_sha256 IS DISTINCT FROM source.geometry_checksum_sha256
                       OR target.properties IS DISTINCT FROM source.properties
                )::bigint AS provenance_mismatch_count
           FROM serving_postgis.parcel_boundary_publication AS target
           LEFT JOIN serving_postgis.parcel_boundary_mirror AS source
             ON source.rebuild_run_id = $6 AND source.pnu = target.pnu
          WHERE target.projection_load_id = $1",
    )
    .bind(opened.projection_load_id)
    .bind(opened.data_revision)
    .bind(&evidence.canonical_snapshot_id)
    .bind(evidence.source_record_id)
    .bind(opened.projection_load_id)
    .bind(evidence.mirror_rebuild_run_id)
    .fetch_one(&mut **transaction)
    .await?;
    source_set_stats_from_row(&row)
}

async fn close_succeeded_load(
    transaction: &mut Transaction<'_, Postgres>,
    opened: &OpenedLoad,
    publication_rows: i64,
) -> anyhow::Result<()> {
    let closed = sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET status = 'succeeded', loaded_row_count = $2, rejected_row_count = 0,
                error_message = NULL, finished_at = now()
          WHERE id = $1 AND status = 'running'",
    )
    .bind(opened.projection_load_id)
    .bind(publication_rows)
    .execute(&mut **transaction)
    .await
    .context("failed to close the parcel PostGIS projection load")?
    .rows_affected();
    if closed != 1 {
        bail!(
            "projection load {} was not running when this run tried to close it",
            opened.projection_load_id
        );
    }
    verify_final_load(transaction, opened, "succeeded", publication_rows, false).await
}

async fn close_failed_load(
    pool: &PgPool,
    opened: &OpenedLoad,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    let mut transaction = pool.begin().await?;
    let closed = sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET status = 'failed', loaded_row_count = 0, rejected_row_count = 0,
                error_message = left($2, 4000), finished_at = now()
          WHERE id = $1 AND status = 'running'",
    )
    .bind(opened.projection_load_id)
    .bind(format!("{error:#}"))
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if closed != 1 {
        bail!(
            "projection load {} was not running",
            opened.projection_load_id
        );
    }
    verify_final_load(&mut transaction, opened, "failed", 0, true).await?;
    transaction.commit().await?;
    Ok(())
}

async fn verify_final_load(
    transaction: &mut Transaction<'_, Postgres>,
    opened: &OpenedLoad,
    expected_status: &str,
    expected_loaded_rows: i64,
    expect_error: bool,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT status, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
                source_evidence_id,
                loaded_row_count, rejected_row_count, finished_at IS NOT NULL AS finished,
                error_message
           FROM serving_postgis.spatial_projection_load WHERE id = $1",
    )
    .bind(opened.projection_load_id)
    .fetch_one(&mut **transaction)
    .await?;
    let status: String = row.try_get("status")?;
    let publication_unit_id: Uuid = row.try_get("publication_unit_id")?;
    let data_revision: Uuid = row.try_get("data_revision")?;
    let snapshot: String = row.try_get("canonical_iceberg_snapshot_id")?;
    let source_evidence_id: Option<Uuid> = row.try_get("source_evidence_id")?;
    let loaded_rows: i64 = row.try_get("loaded_row_count")?;
    let rejected_rows: i64 = row.try_get("rejected_row_count")?;
    let finished: bool = row.try_get("finished")?;
    let error_message: Option<String> = row.try_get("error_message")?;
    let error_shape_matches = if expect_error {
        error_message
            .as_deref()
            .is_some_and(|message| !message.trim().is_empty())
    } else {
        error_message.is_none()
    };
    if status != expected_status
        || publication_unit_id != opened.publication_unit_id
        || data_revision != opened.data_revision
        || snapshot != opened.canonical_snapshot_id
        || source_evidence_id != Some(opened.source_evidence_id)
        || loaded_rows != expected_loaded_rows
        || rejected_rows != 0
        || !finished
        || !error_shape_matches
    {
        bail!(
            "projection load {} final tuple does not match its {} postcondition",
            opened.projection_load_id,
            expected_status
        );
    }
    Ok(())
}

fn uuid_env(name: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(&required_env_value(name)?).with_context(|| format!("{name} must be a UUID"))
}
