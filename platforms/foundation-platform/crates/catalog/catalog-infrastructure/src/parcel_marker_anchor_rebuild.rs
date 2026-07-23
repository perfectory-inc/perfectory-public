//! PostGIS-backed parcel marker anchor rebuild implementation.

use async_trait::async_trait;
use catalog_application::ports::{
    ParcelMarkerAnchorRebuildCommand, ParcelMarkerAnchorRebuildPort,
    ParcelMarkerAnchorRebuildReport,
};
use catalog_domain::{CatalogError, MarkerAnchorAlgorithm};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::row_map::map_sqlx;

const SOURCE_MIRROR_TABLE: &str = "serving_postgis.parcel_boundary_mirror";
const TARGET_SOURCE_SRID: &str = "EPSG:5179";
const TARGET_ANCHOR_SRID: &str = "EPSG:4326";

/// `PostgreSQL` implementation for rebuilding parcel marker anchors.
pub struct PgParcelMarkerAnchorRebuilder {
    pool: PgPool,
}

impl PgParcelMarkerAnchorRebuilder {
    /// Creates a rebuilder backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ParcelMarkerAnchorRebuildPort for PgParcelMarkerAnchorRebuilder {
    async fn rebuild_parcel_marker_anchors(
        &self,
        command: ParcelMarkerAnchorRebuildCommand,
    ) -> Result<ParcelMarkerAnchorRebuildReport, CatalogError> {
        if command.algorithm != MarkerAnchorAlgorithm::Polylabel {
            return Err(CatalogError::InvalidParcelMarkerAnchorRebuild(
                "only polylabel parcel marker anchors can be rebuilt from the PostGIS mirror"
                    .to_owned(),
            ));
        }

        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let generation_run_id = Uuid::now_v7();
        let scanned_row_count = count_source_rows_tx(&mut tx, &command.source_snapshot_id).await?;
        let rejected_row_count =
            count_rejected_source_rows_tx(&mut tx, &command.source_snapshot_id).await?;

        if scanned_row_count == 0 {
            insert_failed_generation_run_tx(
                &mut tx,
                generation_run_id,
                &command,
                0,
                0,
                "source snapshot contains no parcel boundary mirror rows",
            )
            .await?;
            tx.commit().await.map_err(map_sqlx)?;
            return Err(CatalogError::InvalidParcelMarkerAnchorRebuild(
                "source snapshot contains no parcel boundary mirror rows".to_owned(),
            ));
        }

        if rejected_row_count > 0 {
            insert_failed_generation_run_tx(
                &mut tx,
                generation_run_id,
                &command,
                0,
                rejected_row_count,
                "source snapshot contains invalid parcel boundary mirror rows",
            )
            .await?;
            tx.commit().await.map_err(map_sqlx)?;
            return Err(CatalogError::InvalidParcelMarkerAnchorRebuild(format!(
                "source snapshot contains {rejected_row_count} invalid parcel boundary mirror rows"
            )));
        }

        insert_running_generation_run_tx(&mut tx, generation_run_id, &command).await?;
        let superseded_row_count =
            supersede_active_anchors_tx(&mut tx, &command.source_snapshot_id).await?;
        let loaded_row_count =
            insert_active_anchors_tx(&mut tx, generation_run_id, &command).await?;
        if loaded_row_count != scanned_row_count {
            return Err(CatalogError::Infrastructure(format!(
                "parcel marker anchor rebuild row count mismatch: scanned={scanned_row_count} loaded={loaded_row_count}"
            )));
        }
        mark_generation_run_succeeded_tx(
            &mut tx,
            generation_run_id,
            &command,
            loaded_row_count,
            superseded_row_count,
        )
        .await?;
        tx.commit().await.map_err(map_sqlx)?;

        Ok(ParcelMarkerAnchorRebuildReport {
            generation_run_id,
            source_snapshot_id: command.source_snapshot_id,
            source_table: command.source_table,
            algorithm: command.algorithm,
            algorithm_version: command.algorithm_version,
            scanned_row_count,
            loaded_row_count,
            rejected_row_count,
            superseded_row_count,
        })
    }
}

async fn count_source_rows_tx(
    tx: &mut Transaction<'_, Postgres>,
    source_snapshot_id: &str,
) -> Result<u64, CatalogError> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM serving_postgis.parcel_boundary_mirror
         WHERE source_snapshot_id = $1",
    )
    .bind(source_snapshot_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    nonnegative_count_to_u64("source row count", count)
}

async fn count_rejected_source_rows_tx(
    tx: &mut Transaction<'_, Postgres>,
    source_snapshot_id: &str,
) -> Result<u64, CatalogError> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM serving_postgis.parcel_boundary_mirror
         WHERE source_snapshot_id = $1
           AND (
             ST_SRID(geom) <> 5179
              -- EPSG:5179 source geometry validity budget.
              OR NOT ST_IsValid(geom)
              OR ST_IsEmpty(geom)
              -- EPSG:5179 source geometry area budget.
              OR ST_Area(geom) <= 0
           )",
    )
    .bind(source_snapshot_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    nonnegative_count_to_u64("rejected row count", count)
}

async fn insert_running_generation_run_tx(
    tx: &mut Transaction<'_, Postgres>,
    generation_run_id: Uuid,
    command: &ParcelMarkerAnchorRebuildCommand,
) -> Result<(), CatalogError> {
    sqlx::query(
        "INSERT INTO catalog.parcel_marker_anchor_generation_run
         (id, source_snapshot_id, source_table, algorithm, algorithm_version, srid,
          status, loaded_row_count, rejected_row_count, quality_report, started_at)
         VALUES ($1, $2, $3, $4, $5, 4326, 'running', 0, 0, $6, now())",
    )
    .bind(generation_run_id)
    .bind(command.source_snapshot_id.as_str())
    .bind(command.source_table.as_str())
    .bind(command.algorithm.wire_name())
    .bind(command.algorithm_version.as_str())
    .bind(rebuild_trace_report(command))
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn insert_failed_generation_run_tx(
    tx: &mut Transaction<'_, Postgres>,
    generation_run_id: Uuid,
    command: &ParcelMarkerAnchorRebuildCommand,
    loaded_row_count: u64,
    rejected_row_count: u64,
    error_message: &str,
) -> Result<(), CatalogError> {
    sqlx::query(
        "INSERT INTO catalog.parcel_marker_anchor_generation_run
         (id, source_snapshot_id, source_table, algorithm, algorithm_version, srid,
          status, loaded_row_count, rejected_row_count, quality_report, started_at,
          finished_at, error_message)
         VALUES ($1, $2, $3, $4, $5, 4326, 'failed', $6, $7, $8, now(), now(), $9)",
    )
    .bind(generation_run_id)
    .bind(command.source_snapshot_id.as_str())
    .bind(command.source_table.as_str())
    .bind(command.algorithm.wire_name())
    .bind(command.algorithm_version.as_str())
    .bind(u64_to_i64("loaded_row_count", loaded_row_count)?)
    .bind(u64_to_i64("rejected_row_count", rejected_row_count)?)
    .bind(json!({
        "source_mirror_table": SOURCE_MIRROR_TABLE,
        "source_srid": TARGET_SOURCE_SRID,
        "target_srid": TARGET_ANCHOR_SRID,
        "requested_by_staff_id": requested_by_staff_id(command),
        "request_id": command.request_id.as_deref(),
    }))
    .bind(error_message)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn supersede_active_anchors_tx(
    tx: &mut Transaction<'_, Postgres>,
    source_snapshot_id: &str,
) -> Result<u64, CatalogError> {
    let result = sqlx::query(
        "WITH source_pnus AS (
             SELECT pnu
             FROM serving_postgis.parcel_boundary_mirror
             WHERE source_snapshot_id = $1
         )
         UPDATE catalog.parcel_marker_anchor pma
         SET is_active = false,
             superseded_at_utc = now(),
             updated_at = now(),
             version = pma.version + 1
         FROM source_pnus
         WHERE pma.pnu = source_pnus.pnu
           AND pma.is_active = true",
    )
    .bind(source_snapshot_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(result.rows_affected())
}

async fn insert_active_anchors_tx(
    tx: &mut Transaction<'_, Postgres>,
    generation_run_id: Uuid,
    command: &ParcelMarkerAnchorRebuildCommand,
) -> Result<u64, CatalogError> {
    let result = sqlx::query(
        "WITH source_rows AS (
             SELECT
                 pnu,
                 parcel_id,
                 source_snapshot_id,
                 source_table,
                 source_record_id,
                 source_file_asset_id,
                 source_object_key,
                 source_row_id,
                 geometry_checksum_sha256,
                 -- EPSG:5179 source polygon anchor; transformed to EPSG:4326 below.
                 (ST_MaximumInscribedCircle(geom)).center AS anchor_5179
             FROM serving_postgis.parcel_boundary_mirror
             WHERE source_snapshot_id = $2
               AND ST_SRID(geom) = 5179
               AND ST_IsValid(geom)
               -- EPSG:5179 source geometry area budget.
               AND NOT ST_IsEmpty(geom)
               AND ST_Area(geom) > 0
         )
         INSERT INTO catalog.parcel_marker_anchor (
             id,
             pnu,
             parcel_id,
             generation_run_id,
             source_geometry_version,
             source_table,
             source_record_id,
             source_file_asset_id,
             source_object_key,
             source_row_id,
             anchor_point,
             algorithm,
             algorithm_version,
             source_geometry_checksum_sha256,
             computed_at_utc,
             activated_at_utc,
             superseded_at_utc,
             is_active,
             created_at,
             updated_at,
             version
         )
         SELECT
             gen_random_uuid(),
             pnu,
             parcel_id,
             $1::uuid,
             source_snapshot_id,
             source_table,
             source_record_id,
             source_file_asset_id,
             source_object_key,
             source_row_id,
             -- Explicit SRID transform: EPSG:5179 mirror polygon label point to EPSG:4326 marker anchor.
             ST_Transform(anchor_5179, 4326),
             $3,
             $4,
             geometry_checksum_sha256,
             now(),
             now(),
             NULL,
             true,
             now(),
             now(),
             1
         FROM source_rows
         ON CONFLICT (pnu, source_geometry_version, algorithm, algorithm_version)
         DO UPDATE
         SET parcel_id = EXCLUDED.parcel_id,
             generation_run_id = EXCLUDED.generation_run_id,
             source_table = EXCLUDED.source_table,
             source_record_id = EXCLUDED.source_record_id,
             source_file_asset_id = EXCLUDED.source_file_asset_id,
             source_object_key = EXCLUDED.source_object_key,
             source_row_id = EXCLUDED.source_row_id,
             anchor_point = EXCLUDED.anchor_point,
             source_geometry_checksum_sha256 = EXCLUDED.source_geometry_checksum_sha256,
             computed_at_utc = EXCLUDED.computed_at_utc,
             activated_at_utc = EXCLUDED.activated_at_utc,
             superseded_at_utc = NULL,
             is_active = true,
             updated_at = now(),
             version = catalog.parcel_marker_anchor.version + 1",
    )
    .bind(generation_run_id)
    .bind(command.source_snapshot_id.as_str())
    .bind(command.algorithm.wire_name())
    .bind(command.algorithm_version.as_str())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(result.rows_affected())
}

async fn mark_generation_run_succeeded_tx(
    tx: &mut Transaction<'_, Postgres>,
    generation_run_id: Uuid,
    command: &ParcelMarkerAnchorRebuildCommand,
    loaded_row_count: u64,
    superseded_row_count: u64,
) -> Result<(), CatalogError> {
    sqlx::query(
        "UPDATE catalog.parcel_marker_anchor_generation_run
         SET status = 'succeeded',
             loaded_row_count = $3,
             rejected_row_count = 0,
             quality_report = $4,
             finished_at = now(),
             updated_at = now(),
             version = version + 1
         WHERE id = $1
           AND source_snapshot_id = $2",
    )
    .bind(generation_run_id)
    .bind(command.source_snapshot_id.as_str())
    .bind(u64_to_i64("loaded_row_count", loaded_row_count)?)
    .bind(json!({
        "source_mirror_table": SOURCE_MIRROR_TABLE,
        "source_srid": TARGET_SOURCE_SRID,
        "target_srid": TARGET_ANCHOR_SRID,
        "anchor_sql": "ST_Transform((ST_MaximumInscribedCircle(geom)).center, 4326)",
        "superseded_row_count": superseded_row_count,
        "requested_by_staff_id": requested_by_staff_id(command),
        "request_id": command.request_id.as_deref(),
    }))
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

fn nonnegative_count_to_u64(field: &str, value: i64) -> Result<u64, CatalogError> {
    u64::try_from(value).map_err(|_| {
        CatalogError::Infrastructure(format!("{field} {value} cannot be converted to u64"))
    })
}

fn u64_to_i64(field: &str, value: u64) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| {
        CatalogError::Infrastructure(format!("{field} {value} overflows Postgres BIGINT"))
    })
}

fn rebuild_trace_report(command: &ParcelMarkerAnchorRebuildCommand) -> serde_json::Value {
    json!({
        "requested_by_staff_id": requested_by_staff_id(command),
        "request_id": command.request_id.as_deref(),
    })
}

fn requested_by_staff_id(command: &ParcelMarkerAnchorRebuildCommand) -> Option<String> {
    command
        .requested_by_staff_id
        .as_ref()
        .map(|staff_id| staff_id.as_uuid().to_string())
}
