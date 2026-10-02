//! Lakehouse bake start (root ADR-0112). Reuses the bake ledger, deterministic identities and
//! promotion; what is new is the output revision, minted here over the baked snapshot.
//!
//! A bake that names the Silver snapshot it read (root ADR-0133 §5) anchors that revision to the
//! source record describing that snapshot, and records the matching verdict that passed it; the
//! build-row guard refuses the row without a passing verdict for the same snapshot.

use sqlx::{Postgres, Transaction};

use super::{
    claim_build_key_tx, invalid_runtime, map_sqlx, set_lock_timeout_tx, CatalogError,
    CatalogMutationKind, PgPool, Row, StartLakehouseBakeCommand, Utc, Uuid, VectorTileBuildJobId,
    VectorTileBuildKind,
};
use catalog_application::ports::LakehouseBakeSilverSource;

/// `catalog.source_record.source` of a record describing one `silver.parcel_boundaries` snapshot.
/// The migration that indexes it unique per snapshot spells the same value.
const SILVER_PARCEL_SOURCE: &str = "lakehouse:silver.parcel_boundaries";

/// The collected source an output revision is anchored to: exactly one of the two.
struct Anchor {
    source_record_id: Option<Uuid>,
    bronze_object_id: Option<Uuid>,
}

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
                revision.source_record_id, revision.bronze_object_id
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
    let silver = command.silver_source.as_ref();
    let (bound_source_record_id, anchor) = output_anchor(&mut tx, silver, &row).await?;
    let output_data_revision = mint_output_revision(
        &mut tx,
        publication_unit_id,
        command.canonical_iceberg_snapshot_id.as_str(),
        &anchor,
    )
    .await?;

    let build_job_id = VectorTileBuildJobId::new(Uuid::now_v7());
    sqlx::query(
        "INSERT INTO catalog.vector_tile_build_job
         (id, publication_unit_id, input_release_id, input_data_revision,
          frozen_source_snapshot_id, status, idempotency_key, kind,
          input_serving_generation, output_data_revision, source_snapshot_id,
          matching_verdict, matching_verdict_sha256, bound_source_record_id)
         VALUES ($1, $2, $3, $4, $5, 'running', $6, 'lakehouse_bake', $7, $8, $9, $10, $11, $12)",
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
    .bind(silver.map(|source| source.source_snapshot_id.as_str()))
    .bind(silver.map(|source| &source.matching_verdict))
    .bind(silver.map(|source| source.matching_verdict_sha256.as_str()))
    .bind(bound_source_record_id)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx)?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(build_job_id)
}

/// What the output revision is anchored to: the record of the Silver snapshot the bake read, which
/// is also returned to be bound on the build row, or else the input revision's collected source.
async fn output_anchor(
    tx: &mut Transaction<'_, Postgres>,
    silver: Option<&LakehouseBakeSilverSource>,
    input: &sqlx::postgres::PgRow,
) -> Result<(Option<Uuid>, Anchor), CatalogError> {
    match silver {
        Some(source) => {
            let record = silver_source_record(tx, source).await?;
            Ok((
                Some(record),
                Anchor {
                    source_record_id: Some(record),
                    bronze_object_id: None,
                },
            ))
        }
        None => Ok((
            None,
            Anchor {
                source_record_id: input.try_get("source_record_id").map_err(map_sqlx)?,
                bronze_object_id: input.try_get("bronze_object_id").map_err(map_sqlx)?,
            },
        )),
    }
}

/// The source record that describes one Silver parcel snapshot, minted the first time a bake
/// names it and reused after; the partial unique index keeps it one per snapshot.
async fn silver_source_record(
    tx: &mut Transaction<'_, Postgres>,
    source: &LakehouseBakeSilverSource,
) -> Result<Uuid, CatalogError> {
    let snapshot = source.source_snapshot_id.trim();
    if snapshot.is_empty() {
        return Err(invalid_runtime(
            "a lakehouse bake that names its Silver source must name the snapshot",
        ));
    }
    sqlx::query(
        "INSERT INTO catalog.source_record (id, source, external_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (external_id) WHERE source = 'lakehouse:silver.parcel_boundaries' DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(SILVER_PARCEL_SOURCE)
    .bind(snapshot)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    sqlx::query_scalar(
        "SELECT id FROM catalog.source_record WHERE source = $1 AND external_id = $2",
    )
    .bind(SILVER_PARCEL_SOURCE)
    .bind(snapshot)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx)
}

/// Mints (or, on a retry, reuses) the revision the baked release carries: one per unit per snapshot,
/// anchored to `anchor`. Reuse is accepted only when the existing row names that same source.
async fn mint_output_revision(
    tx: &mut Transaction<'_, Postgres>,
    publication_unit_id: Uuid,
    snapshot: &str,
    anchor: &Anchor,
) -> Result<Uuid, CatalogError> {
    let source_record_id = anchor.source_record_id;
    let bronze_object_id = anchor.bronze_object_id;
    // Not the input's administrative revision: that link is keyed by the administrative revision's
    // own snapshot, which a Gold snapshot never equals, so carrying it would be refused by
    // `publication_revision_administrative_lineage_fkey` — and it would claim a derivation this
    // revision does not have. A lakehouse bake derives from the Gold snapshot it names.
    let derived_from: Option<Uuid> = None;
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
