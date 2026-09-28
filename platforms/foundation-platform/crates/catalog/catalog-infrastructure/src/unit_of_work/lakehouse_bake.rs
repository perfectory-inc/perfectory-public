//! Lakehouse bake start (root ADR-0112). Reuses the bake ledger, deterministic identities and
//! promotion; what is new is the output revision, minted here over the baked snapshot.

use sqlx::postgres::PgRow;
use sqlx::{Postgres, Transaction};

use super::{
    claim_build_key_tx, invalid_runtime, map_sqlx, set_lock_timeout_tx, CatalogError,
    CatalogMutationKind, PgPool, Row, StartLakehouseBakeCommand, Utc, Uuid, VectorTileBuildJobId,
    VectorTileBuildKind,
};

pub(super) async fn start(
    pool: &PgPool,
    command: StartLakehouseBakeCommand,
) -> Result<VectorTileBuildJobId, CatalogError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    set_lock_timeout_tx(&mut tx).await?;
    let fingerprint = command.request_fingerprint();
    if !claim_build_key_tx(
        &mut tx,
        &command.idempotency_key,
        CatalogMutationKind::StartLakehouseBake,
        &fingerprint,
        command.operator_staff_id,
    )
    .await?
    {
        // Replay precedes the active-input guard: after promotion the input is history, but the
        // original request must still answer with its one job.
        let existing = replayed_build(&mut tx, &command).await?;
        tx.commit().await.map_err(map_sqlx)?;
        return Ok(existing);
    }

    let row = sqlx::query(
        "SELECT unit.id AS publication_unit_id, unit.active_release_id, unit.serving_generation,
                release.data_revision, release.canonical_iceberg_snapshot_id,
                release.source_kind, release.validated_at,
                revision.source_record_id, revision.bronze_object_id,
                revision.derived_from_administrative_revision
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release AS release
           ON release.id = $2 AND release.publication_unit_id = unit.id
         JOIN catalog.publication_revision AS revision ON revision.id = release.data_revision
         WHERE unit.unit_key = $1
         FOR UPDATE OF unit FOR SHARE OF release",
    )
    .bind(&command.unit_key)
    .bind(command.input_release_id.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| {
        invalid_runtime("lakehouse bake input does not belong to the publication unit")
    })?;
    let active: Option<Uuid> = row.try_get("active_release_id").map_err(map_sqlx)?;
    let source_kind: String = row.try_get("source_kind").map_err(map_sqlx)?;
    let validated: Option<chrono::DateTime<Utc>> = row.try_get("validated_at").map_err(map_sqlx)?;
    if active != Some(command.input_release_id.as_uuid())
        || source_kind
            != VectorTileBuildKind::LakehouseBake
                .input_source_kind()
                .as_str()
        || validated.is_none()
    {
        return Err(invalid_runtime(
            "lakehouse bake input must be the active validated static_pmtiles release",
        ));
    }
    let input_snapshot: String = row
        .try_get("canonical_iceberg_snapshot_id")
        .map_err(map_sqlx)?;
    if input_snapshot == command.canonical_iceberg_snapshot_id.as_str() {
        return Err(invalid_runtime(
            "lakehouse bake must freeze a snapshot other than the one its input already serves",
        ));
    }
    let publication_unit_id: Uuid = row.try_get("publication_unit_id").map_err(map_sqlx)?;
    let output_data_revision = mint_output_revision(
        &mut tx,
        publication_unit_id,
        command.canonical_iceberg_snapshot_id.as_str(),
        &row,
    )
    .await?;

    let build_job_id = VectorTileBuildJobId::new(Uuid::now_v7());
    sqlx::query(
        "INSERT INTO catalog.vector_tile_build_job
         (id, publication_unit_id, input_release_id, input_data_revision,
          frozen_source_snapshot_id, status, idempotency_key, kind,
          input_serving_generation, output_data_revision)
         VALUES ($1, $2, $3, $4, $5, 'running', $6, 'lakehouse_bake', $7, $8)",
    )
    .bind(build_job_id.as_uuid())
    .bind(publication_unit_id)
    .bind(command.input_release_id.as_uuid())
    .bind(row.try_get::<Uuid, _>("data_revision").map_err(map_sqlx)?)
    .bind(command.canonical_iceberg_snapshot_id.as_str())
    .bind(&command.idempotency_key)
    .bind(
        row.try_get::<i64, _>("serving_generation")
            .map_err(map_sqlx)?,
    )
    .bind(output_data_revision)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx)?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(build_job_id)
}

/// Mints (or, on a retry, reuses) the revision the baked release carries: one per unit per snapshot,
/// anchored to the same collected source as the input's revision. Reuse is accepted only when the
/// existing row names that same source.
async fn mint_output_revision(
    tx: &mut Transaction<'_, Postgres>,
    publication_unit_id: Uuid,
    snapshot: &str,
    input: &PgRow,
) -> Result<Uuid, CatalogError> {
    let source_record_id: Option<Uuid> = input.try_get("source_record_id").map_err(map_sqlx)?;
    let bronze_object_id: Option<Uuid> = input.try_get("bronze_object_id").map_err(map_sqlx)?;
    let derived_from: Option<Uuid> = input
        .try_get("derived_from_administrative_revision")
        .map_err(map_sqlx)?;
    // The revision ledger admits writes only from a publisher transaction.
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    sqlx::query(
        "INSERT INTO catalog.publication_revision
         (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id,
          bronze_object_id, derived_from_administrative_revision)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (publication_unit_id, canonical_iceberg_snapshot_id) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(publication_unit_id)
    .bind(snapshot)
    .bind(source_record_id)
    .bind(bronze_object_id)
    .bind(derived_from)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    let output = sqlx::query(
        "SELECT id, source_record_id, bronze_object_id FROM catalog.publication_revision
         WHERE publication_unit_id = $1 AND canonical_iceberg_snapshot_id = $2",
    )
    .bind(publication_unit_id)
    .bind(snapshot)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    if output
        .try_get::<Option<Uuid>, _>("source_record_id")
        .map_err(map_sqlx)?
        != source_record_id
        || output
            .try_get::<Option<Uuid>, _>("bronze_object_id")
            .map_err(map_sqlx)?
            != bronze_object_id
    {
        return Err(invalid_runtime(
            "the snapshot already has a revision anchored to a different collected source",
        ));
    }
    output.try_get("id").map_err(map_sqlx)
}

async fn replayed_build(
    tx: &mut Transaction<'_, Postgres>,
    command: &StartLakehouseBakeCommand,
) -> Result<VectorTileBuildJobId, CatalogError> {
    let existing: Option<Uuid> = sqlx::query_scalar(
        "SELECT build.id FROM catalog.vector_tile_build_job AS build
         JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = build.publication_unit_id
         WHERE unit.unit_key = $1 AND build.idempotency_key = $2 AND build.kind = 'lakehouse_bake'",
    )
    .bind(&command.unit_key)
    .bind(&command.idempotency_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    existing.map(VectorTileBuildJobId::new).ok_or_else(|| {
        CatalogError::Infrastructure("lakehouse bake claim has no build outcome".to_owned())
    })
}
