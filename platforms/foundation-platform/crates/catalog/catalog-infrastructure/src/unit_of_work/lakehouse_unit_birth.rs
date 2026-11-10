//! A publication unit born from its first lakehouse bake (root ADR-0374).
//!
//! The unit row, its first data revision, the validated static release with its layer and a
//! complete runtime manifest are written in one transaction, in the fixed lock order of every other
//! publication: pointer, every unit, then the gate. The gate admits a static first publication only
//! for a unit whose `born_from` is `lakehouse_bake`.

use super::static_release::insert_static_runtime_manifest_tx;
use super::{
    claim_manifest_mutation_key_tx, current_manifest_selections_tx, insert_manifest_units_tx,
    insert_outbox_event, insert_release_layers_tx, invalid_runtime,
    load_vector_tile_runtime_manifest_by_id, lock_publication_units_tx,
    lock_runtime_manifest_pointer_tx, map_runtime_manifest_gate_error, map_sqlx,
    record_mutation_outbox_event_tx, runtime_manifest_published_event, set_lock_timeout_tx,
    static_release_martin_source_id, u64_to_i64, BTreeMap, CatalogError, CatalogMutationKind,
    ManifestUnitSelection, MutationClaim, PgPool, Postgres, Transaction, Uuid,
    VectorTileRuntimeManifest, VectorTileRuntimeManifestId,
};
use catalog_application::ports::{
    RegisterLakehouseTileUnitCommand, RuntimeManifestPublicationCapability,
};

pub(super) async fn register(
    pool: &PgPool,
    publication: RuntimeManifestPublicationCapability,
    command: RegisterLakehouseTileUnitCommand,
) -> Result<VectorTileRuntimeManifest, CatalogError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    set_lock_timeout_tx(&mut tx).await?;
    let manifest_id = Uuid::now_v7();
    if let MutationClaim::Replay(recorded) = claim_manifest_mutation_key_tx(
        &mut tx,
        &command.idempotency_key,
        CatalogMutationKind::RegisterLakehouseTileUnit,
        &command.request_fingerprint(),
        manifest_id,
        command.operator_staff_id,
    )
    .await?
    {
        let manifest = load_vector_tile_runtime_manifest_by_id(&mut tx, recorded)
            .await?
            .ok_or_else(|| {
                CatalogError::Infrastructure(format!(
                    "unit registration key {} records absent manifest {recorded}",
                    command.idempotency_key
                ))
            })?;
        tx.commit().await.map_err(map_sqlx)?;
        return Ok(manifest);
    }

    let current_manifest_id = lock_runtime_manifest_pointer_tx(&mut tx).await?;
    let units = lock_publication_units_tx(&mut tx).await?;
    if units.iter().any(|unit| unit.unit_key == command.unit_key) {
        return Err(invalid_runtime(format!(
            "publication unit {} already exists; its next release is a lakehouse bake",
            command.unit_key
        )));
    }
    let carried = current_manifest_selections_tx(&mut tx, current_manifest_id).await?;
    let mut selections = units
        .iter()
        .map(|unit| {
            let selection = carried.get(&unit.id).ok_or_else(|| {
                invalid_runtime(format!(
                    "publication unit {} has no carried selection for the registration",
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
        .collect::<Result<Vec<_>, CatalogError>>()?;

    let snapshot = command.canonical_iceberg_snapshot_id.as_str();
    let release_id = command.artifact.release_id.as_uuid();
    let (unit_id, source_record_id, revision_id) =
        insert_unit_and_first_release_tx(&mut tx, &command).await?;

    insert_static_runtime_manifest_tx(&mut tx, manifest_id, source_record_id).await?;
    selections.push(ManifestUnitSelection {
        publication_unit_id: unit_id,
        release_id,
        serving_generation: 1,
        data_revision: revision_id,
        canonical_iceberg_snapshot_id: snapshot.to_owned(),
    });
    insert_manifest_units_tx(&mut tx, manifest_id, &selections).await?;
    sqlx::query_scalar::<_, i64>("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
        .bind(current_manifest_id)
        .bind(manifest_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_runtime_manifest_gate_error)?;

    let manifest = load_vector_tile_runtime_manifest_by_id(
        &mut tx,
        VectorTileRuntimeManifestId::new(manifest_id),
    )
    .await?
    .ok_or_else(|| {
        invalid_runtime("the registration gate left no runtime manifest under its id")
    })?;
    if publication.is_enabled() {
        let event_id =
            insert_outbox_event(&mut tx, &runtime_manifest_published_event(&manifest)).await?;
        record_mutation_outbox_event_tx(&mut tx, &command.idempotency_key, event_id).await?;
    }
    tx.commit().await.map_err(map_sqlx)?;
    Ok(manifest)
}

/// The unit row, its source record and first revision, the release's file asset, the release and
/// its layer; returns `(unit, source record, revision)`.
async fn insert_unit_and_first_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &RegisterLakehouseTileUnitCommand,
) -> Result<(Uuid, Uuid, Uuid), CatalogError> {
    let unit_id = Uuid::now_v7();
    let snapshot = command.canonical_iceberg_snapshot_id.as_str();
    sqlx::query(
        "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key, born_from)
         VALUES ($1, $2, 'lakehouse_bake')",
    )
    .bind(unit_id)
    .bind(&command.unit_key)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    let source_record_id = Uuid::now_v7();
    sqlx::query("INSERT INTO catalog.source_record (id, source, external_id) VALUES ($1, $2, $3)")
        .bind(source_record_id)
        .bind(&command.source_record_source)
        .bind(snapshot)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    // The revision ledger admits writes only from a publisher transaction.
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    let revision_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO catalog.publication_revision
         (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(revision_id)
    .bind(unit_id)
    .bind(snapshot)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    let source_file_asset_id = insert_file_assets_tx(tx, command, source_record_id).await?;
    let artifact = &command.artifact;
    let release_id = artifact.release_id.as_uuid();
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_record_id, source_file_asset_ids, source_kind, martin_source_id,
          tiles_url_template, pmtiles_object_key, pmtiles_file_asset_id, pmtiles_sha256,
          pmtiles_bytes, validated_at, validation_evidence_sha256, bake_inputs_sha256)
         VALUES ($1, $2, $3, $4, $5, ARRAY[$13]::uuid[], 'static_pmtiles', $6, $7, $8, $9, $10,
                 $11, now(), $12, $14)",
    )
    .bind(release_id)
    .bind(unit_id)
    .bind(revision_id)
    .bind(snapshot)
    .bind(source_record_id)
    .bind(static_release_martin_source_id(
        &command.unit_key,
        artifact.release_id,
    ))
    .bind(artifact.tiles_url_template.as_str())
    .bind(&artifact.object_key)
    .bind(artifact.file_asset_id.as_uuid())
    .bind(artifact.checksum.as_str())
    .bind(u64_to_i64(artifact.size_bytes)?)
    .bind(command.evidence.as_str())
    .bind(source_file_asset_id)
    .bind(command.bake_inputs_sha256.as_deref())
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    insert_release_layers_tx(
        tx,
        release_id,
        &BTreeMap::from([(command.layer_id.clone(), command.layer.clone())]),
    )
    .await?;
    Ok((unit_id, source_record_id, revision_id))
}

/// The release's two file assets: the input it was baked from (its lineage) and the archive.
/// Returns the input's id.
async fn insert_file_assets_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &RegisterLakehouseTileUnitCommand,
    source_record_id: Uuid,
) -> Result<Uuid, CatalogError> {
    // The input the archive was baked from, the release's lineage (`source_file_asset_ids`).
    let source_file_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
         VALUES ($1, $2, 'application/json', 0, $3, 'private', 1)",
    )
    .bind(source_file_asset_id)
    .bind(&command.source_object_key)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    let artifact = &command.artifact;
    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, checksum_sha256,
          source_record_id, visibility, version)
         VALUES ($1, $2, 'application/vnd.pmtiles', $3, $4, $5, 'private', 1)",
    )
    .bind(artifact.file_asset_id.as_uuid())
    .bind(&artifact.object_key)
    .bind(u64_to_i64(artifact.size_bytes)?)
    .bind(artifact.checksum.as_str())
    .bind(source_record_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(source_file_asset_id)
}
