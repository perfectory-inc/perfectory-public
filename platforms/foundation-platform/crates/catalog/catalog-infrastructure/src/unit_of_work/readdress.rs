//! Address-only publication start. Reuses the bake ledger, deterministic identities and promotion.

use super::*;

pub(super) async fn start(
    pool: &PgPool,
    command: StartStaticReleaseReaddressCommand,
) -> Result<VectorTileBuildJobId, CatalogError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    set_lock_timeout_tx(&mut tx).await?;
    let fingerprint = command.request_fingerprint();
    if !claim_build_key_tx(
        &mut tx,
        &command.idempotency_key,
        CatalogMutationKind::StartStaticReleaseReaddress,
        &fingerprint,
        command.operator_staff_id,
    )
    .await?
    {
        // Replay precedes the active-input guard: after promotion S1 is history, but the original
        // request must still answer with its one job, whose captured generation replays the CAS.
        let existing: Option<Uuid> = sqlx::query_scalar(
            "SELECT build.id FROM catalog.vector_tile_build_job AS build
             JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = build.publication_unit_id
             WHERE unit.unit_key = $1 AND build.idempotency_key = $2 AND build.kind = 'readdress'",
        )
        .bind(&command.unit_key)
        .bind(&command.idempotency_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let existing = existing.ok_or_else(|| {
            CatalogError::Infrastructure("readdress claim has no build outcome".to_owned())
        })?;
        tx.commit().await.map_err(map_sqlx)?;
        return Ok(VectorTileBuildJobId::new(existing));
    }

    let row = sqlx::query(
        "SELECT unit.id AS publication_unit_id, unit.active_release_id, unit.serving_generation,
                release.data_revision, release.canonical_iceberg_snapshot_id,
                release.source_kind, release.validated_at
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release AS release
           ON release.id = $2 AND release.publication_unit_id = unit.id
         WHERE unit.unit_key = $1
         FOR UPDATE OF unit FOR SHARE OF release",
    )
    .bind(&command.unit_key)
    .bind(command.input_release_id.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| invalid_runtime("readdress input does not belong to the publication unit"))?;
    let active: Option<Uuid> = row.try_get("active_release_id").map_err(map_sqlx)?;
    let source_kind: String = row.try_get("source_kind").map_err(map_sqlx)?;
    let validated: Option<chrono::DateTime<Utc>> = row.try_get("validated_at").map_err(map_sqlx)?;
    if active != Some(command.input_release_id.as_uuid())
        || source_kind != VectorTileBuildKind::Readdress.input_source_kind().as_str()
        || validated.is_none()
    {
        return Err(invalid_runtime(
            "readdress input must be the active validated static_pmtiles release",
        ));
    }
    let build_job_id = VectorTileBuildJobId::new(Uuid::now_v7());
    sqlx::query(
        "INSERT INTO catalog.vector_tile_build_job
         (id, publication_unit_id, input_release_id, input_data_revision,
          frozen_source_snapshot_id, status, idempotency_key, kind,
          input_serving_generation, readdress_tiles_base_url)
         VALUES ($1, $2, $3, $4, $5, 'running', $6, 'readdress', $7, $8)",
    )
    .bind(build_job_id.as_uuid())
    .bind(
        row.try_get::<Uuid, _>("publication_unit_id")
            .map_err(map_sqlx)?,
    )
    .bind(command.input_release_id.as_uuid())
    .bind(row.try_get::<Uuid, _>("data_revision").map_err(map_sqlx)?)
    .bind(
        row.try_get::<String, _>("canonical_iceberg_snapshot_id")
            .map_err(map_sqlx)?,
    )
    .bind(&command.idempotency_key)
    .bind(
        row.try_get::<i64, _>("serving_generation")
            .map_err(map_sqlx)?,
    )
    .bind(&command.public_tiles_base_url)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx)?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(build_job_id)
}
