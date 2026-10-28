//! Static release promotion: the validated build lock, the release rows and the runtime manifest.
//! Moved out of `unit_of_work.rs` unchanged.

use super::{
    advance_serving_generation, carried_release_ids, insert_manifest_units_tx,
    load_vector_tile_runtime_manifest_by_id, lock_release_rows_tx, map_runtime_manifest_gate_error,
    map_sqlx, observed_serving_generation, serving_state_conflict, static_release_martin_source_id,
    validate_build_promotion, vector_tile_runtime_manifest_object_key, BTreeMap, CarriedSelection,
    CatalogError, LockedPublicationUnit, ManifestUnitSelection, Postgres,
    PromoteTileLayerStaticCommand, Row, Transaction, Uuid, VectorTileBuildJobId,
    VectorTileBuildKind, VectorTileBuildPromotionInput, VectorTileBuildPromotionVerdict,
    VectorTileBuildStatus, VectorTileReleaseId, VectorTileRuntimeManifest,
    VectorTileRuntimeManifestId,
};

/// Whether this transaction owns the idempotency key or is answering a prior use of it.
pub(super) enum MutationClaim {
    /// The key is this transaction's; the mutation proceeds.
    Claimed,
    /// A prior transaction recorded the same request; answer with the manifest it published.
    Replay(VectorTileRuntimeManifestId),
}

pub(super) struct LockedValidatedBuild {
    pub(super) unit_key: String,
    pub(super) kind: VectorTileBuildKind,
    pub(super) input_serving_generation: Option<i64>,
    pub(super) status: VectorTileBuildStatus,
    pub(super) input_release_id: Uuid,
    pub(super) input_data_revision: Uuid,
    /// The revision the new release carries: the input's, except for a lakehouse bake, which
    /// minted its own over the baked snapshot.
    pub(super) release_data_revision: Uuid,
    pub(super) frozen_source_snapshot_id: String,
    pub(super) input_source_record_id: Uuid,
    pub(super) input_source_file_asset_ids: Vec<Uuid>,
    pub(super) release_id: Uuid,
    pub(super) file_asset_id: Uuid,
    pub(super) object_key: String,
    pub(super) tiles_url_template: String,
    pub(super) pmtiles_sha256: String,
    pub(super) pmtiles_bytes: i64,
    pub(super) validation_evidence_sha256: String,
}

pub(super) struct StaticPromotionPlan<'a> {
    pub(super) current_manifest_id: Option<Uuid>,
    pub(super) manifest_id: Uuid,
    pub(super) units: &'a [LockedPublicationUnit],
    pub(super) carried: &'a BTreeMap<Uuid, CarriedSelection>,
    pub(super) target: &'a LockedPublicationUnit,
    pub(super) build: &'a LockedValidatedBuild,
    pub(super) command: &'a PromoteTileLayerStaticCommand,
}

pub(super) async fn mark_superseded_build_tx(
    tx: &mut Transaction<'_, Postgres>,
    target: &LockedPublicationUnit,
    build: &LockedValidatedBuild,
    command: &PromoteTileLayerStaticCommand,
) -> Result<bool, CatalogError> {
    let verdict = validate_build_promotion(VectorTileBuildPromotionInput {
        status: build.status,
        input_release_id: VectorTileReleaseId::new(build.input_release_id),
        active_release_id: VectorTileReleaseId::new(target.active_release_id.ok_or_else(|| {
            invalid_runtime(format!(
                "publication unit {} has no active release",
                command.unit_key
            ))
        })?),
    })
    .map_err(invalid_runtime)?;
    if verdict == VectorTileBuildPromotionVerdict::Promotable {
        return Ok(false);
    }

    sqlx::query(
        "UPDATE catalog.vector_tile_build_job
         SET status = 'superseded', updated_at = now() WHERE id = $1",
    )
    .bind(command.build_job_id.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    // A superseded attempt has no manifest answer. Failed mutations do not retain an idempotency
    // row; the terminal build status is the durable outcome instead.
    sqlx::query("DELETE FROM catalog.catalog_mutation_idempotency WHERE idempotency_key = $1")
        .bind(&command.idempotency_key)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    Ok(true)
}

pub(super) async fn publish_static_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    plan: StaticPromotionPlan<'_>,
) -> Result<VectorTileRuntimeManifest, CatalogError> {
    validate_static_serving_observation(plan.target, plan.command)?;
    let selections = plan_static_activation(plan.units, plan.target, plan.carried, plan.build)?;
    let mut releases_to_lock = carried_release_ids(&selections, plan.build.release_id);
    releases_to_lock.push(plan.build.input_release_id);
    releases_to_lock.sort_unstable();
    releases_to_lock.dedup();
    lock_release_rows_tx(tx, &releases_to_lock).await?;

    insert_static_release_tx(tx, plan.target.id, plan.build).await?;
    copy_release_layers_tx(tx, plan.build.input_release_id, plan.build.release_id).await?;
    insert_static_runtime_manifest_tx(tx, plan.manifest_id, plan.build.input_source_record_id)
        .await?;
    insert_manifest_units_tx(tx, plan.manifest_id, &selections).await?;

    sqlx::query_scalar::<_, i64>("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
        .bind(plan.current_manifest_id)
        .bind(plan.manifest_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(map_runtime_manifest_gate_error)?;

    // The gate has already changed active_release_id, so the old dynamic release can now be stored
    // as a distinct same-revision fallback without violating the column CHECK. Readdress keeps
    // that fallback unchanged: its input is static, and the CAS preserves same-revision fallback.
    if plan.build.kind == VectorTileBuildKind::Bake {
        sqlx::query(
            "UPDATE catalog.vector_tile_publication_unit
         SET fallback_release_id = $2, fallback_data_revision = $3, updated_at = now()
         WHERE id = $1",
        )
        .bind(plan.target.id)
        .bind(plan.build.input_release_id)
        .bind(plan.build.input_data_revision)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }
    if plan.build.kind == VectorTileBuildKind::LakehouseBake {
        // ADR-0112: no fallback. The gate already cleared it because the revision changed; saying so
        // here keeps that true even if the gate's CASE is ever rewritten.
        sqlx::query(
            "UPDATE catalog.vector_tile_publication_unit
             SET fallback_release_id = NULL, fallback_data_revision = NULL, updated_at = now()
             WHERE id = $1",
        )
        .bind(plan.target.id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }
    sqlx::query(
        "UPDATE catalog.vector_tile_build_job
         SET status = 'promoted', updated_at = now() WHERE id = $1",
    )
    .bind(plan.command.build_job_id.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    load_vector_tile_runtime_manifest_by_id(tx, VectorTileRuntimeManifestId::new(plan.manifest_id))
        .await?
        .ok_or_else(|| {
            invalid_runtime("the static promotion gate left no runtime manifest under its id")
        })
}

pub(super) async fn lock_validated_build_tx(
    tx: &mut Transaction<'_, Postgres>,
    build_job_id: VectorTileBuildJobId,
    publication_unit_id: Uuid,
) -> Result<LockedValidatedBuild, CatalogError> {
    let row = sqlx::query(
        "SELECT unit.unit_key, build.kind, build.input_serving_generation, build.status,
                build.input_release_id, build.input_data_revision, build.output_data_revision,
                build.frozen_source_snapshot_id, build.result_release_id,
                build.result_pmtiles_file_asset_id, build.result_pmtiles_object_key,
                build.result_tiles_url_template, build.result_pmtiles_sha256,
                build.result_pmtiles_bytes, build.result_evidence_sha256,
                input.source_record_id, input.source_file_asset_ids, input.source_kind
         FROM catalog.vector_tile_build_job AS build
         JOIN catalog.vector_tile_publication_unit AS unit
           ON unit.id = build.publication_unit_id
         JOIN catalog.vector_tile_release AS input ON input.id = build.input_release_id
         WHERE build.id = $1 AND build.publication_unit_id = $2
         FOR UPDATE OF build FOR SHARE OF input",
    )
    .bind(build_job_id.as_uuid())
    .bind(publication_unit_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or_else(|| {
        invalid_runtime(format!(
            "validated build {build_job_id} does not belong to the requested publication unit"
        ))
    })?;
    let status_raw: String = row.try_get("status").map_err(map_sqlx)?;
    let status = VectorTileBuildStatus::try_from(status_raw.as_str()).map_err(invalid_runtime)?;
    let input_source_kind: String = row.try_get("source_kind").map_err(map_sqlx)?;
    let kind = VectorTileBuildKind::parse(&row.try_get::<String, _>("kind").map_err(map_sqlx)?)
        .map_err(invalid_runtime)?;
    if input_source_kind != kind.input_source_kind().as_str() {
        return Err(invalid_runtime(format!(
            "{} promotion build input must be {}, got {input_source_kind}",
            kind.as_str(),
            kind.input_source_kind().as_str()
        )));
    }
    let required_uuid = |column: &str| -> Result<Uuid, CatalogError> {
        row.try_get::<Option<Uuid>, _>(column)
            .map_err(map_sqlx)?
            .ok_or_else(|| invalid_runtime(format!("validated build is missing {column}")))
    };
    let required_text = |column: &str| -> Result<String, CatalogError> {
        row.try_get::<Option<String>, _>(column)
            .map_err(map_sqlx)?
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_runtime(format!("validated build is missing {column}")))
    };
    Ok(LockedValidatedBuild {
        unit_key: row.try_get("unit_key").map_err(map_sqlx)?,
        kind,
        input_serving_generation: row.try_get("input_serving_generation").map_err(map_sqlx)?,
        status,
        input_release_id: row.try_get("input_release_id").map_err(map_sqlx)?,
        input_data_revision: row.try_get("input_data_revision").map_err(map_sqlx)?,
        release_data_revision: match kind {
            VectorTileBuildKind::LakehouseBake => required_uuid("output_data_revision")?,
            VectorTileBuildKind::Bake | VectorTileBuildKind::Readdress => {
                row.try_get("input_data_revision").map_err(map_sqlx)?
            }
        },
        frozen_source_snapshot_id: row.try_get("frozen_source_snapshot_id").map_err(map_sqlx)?,
        input_source_record_id: row.try_get("source_record_id").map_err(map_sqlx)?,
        input_source_file_asset_ids: row.try_get("source_file_asset_ids").map_err(map_sqlx)?,
        release_id: required_uuid("result_release_id")?,
        file_asset_id: required_uuid("result_pmtiles_file_asset_id")?,
        object_key: required_text("result_pmtiles_object_key")?,
        tiles_url_template: required_text("result_tiles_url_template")?,
        pmtiles_sha256: required_text("result_pmtiles_sha256")?,
        pmtiles_bytes: row
            .try_get::<Option<i64>, _>("result_pmtiles_bytes")
            .map_err(map_sqlx)?
            .ok_or_else(|| invalid_runtime("validated build is missing result_pmtiles_bytes"))?,
        validation_evidence_sha256: required_text("result_evidence_sha256")?,
    })
}

pub(super) fn validate_static_serving_observation(
    unit: &LockedPublicationUnit,
    command: &PromoteTileLayerStaticCommand,
) -> Result<(), CatalogError> {
    let current_release = unit.active_release_id;
    let current_generation = observed_serving_generation(unit)?;
    if current_release != Some(command.expected_active_release_id.as_uuid())
        || current_generation != command.expected_serving_generation.value()
    {
        return Err(serving_state_conflict(
            unit,
            Some(command.expected_active_release_id.as_uuid()),
            Some(command.expected_serving_generation.value()),
        ));
    }
    Ok(())
}

pub(super) fn plan_static_activation(
    units: &[LockedPublicationUnit],
    target: &LockedPublicationUnit,
    carried: &BTreeMap<Uuid, CarriedSelection>,
    build: &LockedValidatedBuild,
) -> Result<Vec<ManifestUnitSelection>, CatalogError> {
    units
        .iter()
        .map(|unit| {
            if unit.id == target.id {
                return Ok(ManifestUnitSelection {
                    publication_unit_id: unit.id,
                    release_id: build.release_id,
                    serving_generation: advance_serving_generation(unit)?,
                    data_revision: build.release_data_revision,
                    canonical_iceberg_snapshot_id: build.frozen_source_snapshot_id.clone(),
                });
            }
            let selection = carried.get(&unit.id).ok_or_else(|| {
                invalid_runtime(format!(
                    "publication unit {} has no carried selection for static promotion",
                    unit.unit_key
                ))
            })?;
            Ok(ManifestUnitSelection {
                publication_unit_id: unit.id,
                release_id: selection.release_id,
                serving_generation: unit.serving_generation,
                data_revision: selection.data_revision,
                canonical_iceberg_snapshot_id: selection.canonical_iceberg_snapshot_id.clone(),
            })
        })
        .collect()
}

pub(super) async fn insert_static_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    publication_unit_id: Uuid,
    build: &LockedValidatedBuild,
) -> Result<(), CatalogError> {
    let source_id = static_release_martin_source_id(
        &build.unit_key,
        VectorTileReleaseId::new(build.release_id),
    );
    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, checksum_sha256,
          source_record_id, visibility, version)
         VALUES ($1, $2, 'application/vnd.pmtiles', $3, $4, $5, 'private', 1)",
    )
    .bind(build.file_asset_id)
    .bind(&build.object_key)
    .bind(build.pmtiles_bytes)
    .bind(&build.pmtiles_sha256)
    .bind(build.input_source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_record_id, source_file_asset_ids, source_kind, martin_source_id,
          tiles_url_template, pmtiles_object_key, pmtiles_file_asset_id,
          pmtiles_sha256, pmtiles_bytes, validated_at, validation_evidence_sha256,
          readdressed_from_release_id)
         VALUES ($1, $2, $3, $4, $5, $6, 'static_pmtiles', $7, $8, $9, $10, $11, $12,
                 now(), $13, $14)",
    )
    .bind(build.release_id)
    .bind(publication_unit_id)
    .bind(build.release_data_revision)
    .bind(&build.frozen_source_snapshot_id)
    .bind(build.input_source_record_id)
    .bind(&build.input_source_file_asset_ids)
    .bind(source_id)
    .bind(&build.tiles_url_template)
    .bind(&build.object_key)
    .bind(build.file_asset_id)
    .bind(&build.pmtiles_sha256)
    .bind(build.pmtiles_bytes)
    .bind(&build.validation_evidence_sha256)
    .bind((build.kind == VectorTileBuildKind::Readdress).then_some(build.input_release_id))
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

pub(super) async fn copy_release_layers_tx(
    tx: &mut Transaction<'_, Postgres>,
    input_release_id: Uuid,
    release_id: Uuid,
) -> Result<(), CatalogError> {
    let inserted = sqlx::query(
        "INSERT INTO catalog.vector_tile_release_layer
         (release_id, layer_id, source_layer, feature_id_property,
          tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
          feature_filter_properties)
         SELECT $2, layer_id, source_layer, feature_id_property,
                tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
                feature_filter_properties
         FROM catalog.vector_tile_release_layer
         WHERE release_id = $1",
    )
    .bind(input_release_id)
    .bind(release_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .rows_affected();
    if inserted == 0 {
        return Err(invalid_runtime(
            "static promotion input release has no layers to copy",
        ));
    }
    Ok(())
}

pub(super) async fn insert_static_runtime_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    source_record_id: Uuid,
) -> Result<(), CatalogError> {
    let manifest_file_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
         VALUES ($1, $2, 'application/json', 0, $3, 'public', 1)",
    )
    .bind(manifest_file_asset_id)
    .bind(vector_tile_runtime_manifest_object_key(
        VectorTileRuntimeManifestId::new(manifest_id),
    ))
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    sqlx::query(
        "INSERT INTO catalog.vector_tile_runtime_manifest
         (id, manifest_generation, manifest_file_asset_id)
         SELECT $1, coalesce(max(manifest_generation), 0) + 1, $2
         FROM catalog.vector_tile_runtime_manifest",
    )
    .bind(manifest_id)
    .bind(manifest_file_asset_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Bounds every lock wait in this transaction so contention is answerable instead of opaque.
///
/// Without it a duplicate blocked on the ledger key or the pointer lock runs into the pool's
/// `statement_timeout` (2500ms, set per connection by the API), surfaces as `57014`, and is served as
/// a redacted 500 — the one case where the client's correct action, retrying the same key, is exactly
/// what it cannot infer. `SET LOCAL` so the setting dies with the transaction and cannot leak back
/// into the pooled connection.
pub(super) async fn set_lock_timeout_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogError> {
    sqlx::query("SET LOCAL lock_timeout = '1200ms'")
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    Ok(())
}

pub(super) fn invalid_runtime(message: impl Into<String>) -> CatalogError {
    CatalogError::InvalidVectorTileRuntimeManifest(message.into())
}
