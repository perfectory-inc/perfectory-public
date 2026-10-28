//! Dynamic activation: publication unit locks, serving generations and the runtime manifest rows.
//! Moved out of `unit_of_work.rs` unchanged.

use super::{
    is_unique_violation_code, map_sqlx, vector_tile_runtime_manifest_object_key, BTreeMap,
    CatalogError, CatalogEvent, FileAssetId, MarkTileLayerDynamicCommand, Postgres, Row,
    RuntimeTileLayer, ServingGeneration, Transaction, Uuid, VectorTileRuntimeManifest,
    VectorTileRuntimeManifestId, VectorTileRuntimeManifestPublishedV2,
    VectorTileRuntimeUnitSelectionV2,
};

/// One publication unit as the activation transaction observed it under `FOR UPDATE`.
pub(super) struct LockedPublicationUnit {
    pub(super) id: Uuid,
    pub(super) unit_key: String,
    pub(super) active_release_id: Option<Uuid>,
    pub(super) serving_generation: i64,
}

/// A selection the currently active manifest already made for one publication unit.
pub(super) struct CarriedSelection {
    pub(super) release_id: Uuid,
    pub(super) data_revision: Uuid,
    pub(super) canonical_iceberg_snapshot_id: String,
}

/// One unit's row in the manifest being assembled.
pub(super) struct ManifestUnitSelection {
    pub(super) publication_unit_id: Uuid,
    pub(super) release_id: Uuid,
    pub(super) serving_generation: i64,
    pub(super) data_revision: Uuid,
    pub(super) canonical_iceberg_snapshot_id: String,
}

/// Maps the promotion gate's own refusals to the typed runtime-manifest error.
///
/// `40001` is the compare-and-swap conflict and the non-monotonic generation; `23514` is every
/// state-machine rule the gate repeats — completeness, first-publication-is-dynamic, static uses the
/// selected revision, no serving-generation gap, release-addressed static identity. All of them say
/// the manifest offered for promotion is not a valid publication, which is what this variant means.
/// Mapping them to an infrastructure error reported a caller's invalid manifest as a server fault.
pub(super) fn map_runtime_manifest_gate_error(error: sqlx::Error) -> CatalogError {
    match error {
        sqlx::Error::Database(database)
            if matches!(database.code().as_deref(), Some("40001" | "23514")) =>
        {
            CatalogError::InvalidVectorTileRuntimeManifest(database.message().to_owned())
        }
        other => map_sqlx(other),
    }
}

/// Takes the first lock of the fixed order and reads the pointer it protects.
///
/// `SHARE ROW EXCLUSIVE` is the mode `catalog.promote_vector_tile_runtime_manifest` takes on this
/// relation. Taking it up front means the gate's own `LOCK TABLE`, later in the same transaction, is
/// a repeat acquisition instead of an upgrade — two transactions each holding the weaker `ROW SHARE`
/// and both trying to upgrade would deadlock. The mode conflicts with itself, so activations
/// serialize, and it does not conflict with `ACCESS SHARE`, so Martin's view keeps reading the
/// current pointer while one is in flight.
pub(super) async fn lock_runtime_manifest_pointer_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<Uuid>, CatalogError> {
    sqlx::query(
        "LOCK TABLE catalog.vector_tile_runtime_manifest_pointer IN SHARE ROW EXCLUSIVE MODE",
    )
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    sqlx::query_scalar(
        "SELECT manifest_id
         FROM catalog.vector_tile_runtime_manifest_pointer
         WHERE singleton = true
         FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)
}

/// Takes the second lock of the fixed order: every publication unit, in one statement.
///
/// Every unit rather than only the one being switched. The gate refuses a manifest that does not
/// select all of them and rewrites all of their pointers, so this transaction reads and changes
/// every unit whether or not the caller named it. `ORDER BY unit_key` fixes the acquisition order in
/// the statement instead of leaving it to the planner.
pub(super) async fn lock_publication_units_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<LockedPublicationUnit>, CatalogError> {
    let rows = sqlx::query(
        "SELECT id, unit_key, active_release_id, serving_generation
         FROM catalog.vector_tile_publication_unit
         ORDER BY unit_key
         FOR UPDATE",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    rows.iter()
        .map(|row| {
            Ok(LockedPublicationUnit {
                id: row.try_get("id").map_err(map_sqlx)?,
                unit_key: row.try_get("unit_key").map_err(map_sqlx)?,
                active_release_id: row.try_get("active_release_id").map_err(map_sqlx)?,
                serving_generation: row.try_get("serving_generation").map_err(map_sqlx)?,
            })
        })
        .collect()
}

/// Takes the last lock of the fixed order over the releases this manifest will name.
///
/// `FOR SHARE`, not `FOR UPDATE`: a release row is immutable once written, so what this transaction
/// needs is for the rows to still be there when the gate resolves the manifest's foreign keys, not
/// the right to change them.
pub(super) async fn lock_release_rows_tx(
    tx: &mut Transaction<'_, Postgres>,
    release_ids: &[Uuid],
) -> Result<(), CatalogError> {
    if release_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "SELECT id
         FROM catalog.vector_tile_release
         WHERE id = ANY($1)
         ORDER BY id
         FOR SHARE",
    )
    .bind(release_ids)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Reads the selections the active manifest already made, keyed by publication unit.
///
/// A one-unit edit still has to publish a complete manifest, so the other units' selections are
/// carried forward from the pointer rather than recomputed — recomputing them from the unit rows
/// would let a half-committed neighbour leak into a manifest this transaction claims is complete.
pub(super) async fn current_manifest_selections_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Option<Uuid>,
) -> Result<BTreeMap<Uuid, CarriedSelection>, CatalogError> {
    let Some(manifest_id) = manifest_id else {
        return Ok(BTreeMap::new());
    };
    let rows = sqlx::query(
        "SELECT publication_unit_id, release_id, data_revision, canonical_iceberg_snapshot_id
         FROM catalog.vector_tile_runtime_manifest_unit
         WHERE manifest_id = $1
         ORDER BY publication_unit_id",
    )
    .bind(manifest_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    let mut selections = BTreeMap::new();
    for row in &rows {
        let publication_unit_id: Uuid = row.try_get("publication_unit_id").map_err(map_sqlx)?;
        selections.insert(
            publication_unit_id,
            CarriedSelection {
                release_id: row.try_get("release_id").map_err(map_sqlx)?,
                data_revision: row.try_get("data_revision").map_err(map_sqlx)?,
                canonical_iceberg_snapshot_id: row
                    .try_get("canonical_iceberg_snapshot_id")
                    .map_err(map_sqlx)?,
            },
        );
    }
    Ok(selections)
}

pub(super) fn find_publication_unit<'units>(
    units: &'units [LockedPublicationUnit],
    unit_key: &str,
) -> Result<&'units LockedPublicationUnit, CatalogError> {
    units
        .iter()
        .find(|unit| unit.unit_key == unit_key)
        .ok_or_else(|| {
            // Units are seeded, not created by an activation. Creating one here would let a typo in
            // `unit_key` publish a second unit nobody configured — and, because the gate demands a
            // complete manifest, break every later publication until someone noticed.
            CatalogError::InvalidVectorTileRuntimeManifest(format!(
                "publication unit {unit_key} does not exist"
            ))
        })
}

/// Builds the complete manifest selection: the activated unit plus every other unit carried over.
///
/// Two rules the promotion gate enforces are answered here so the failure names the unit rather
/// than arriving as a check-constraint violation: a manifest must select every publication unit, and
/// each selected unit's serving generation must be the value that unit's transition implies —
/// `1` for a first publication, one past the current value when the release changes, and the current
/// value when the manifest re-selects the release the unit already serves. A carried unit's source
/// selection did not change, so its generation does not move; see
/// `20260730000003_serving_generation_tracks_one_unit_source_selection.sql`.
pub(super) fn plan_dynamic_activation(
    units: &[LockedPublicationUnit],
    target: &LockedPublicationUnit,
    carried: &BTreeMap<Uuid, CarriedSelection>,
    command: &MarkTileLayerDynamicCommand,
    activated_release_id: Uuid,
) -> Result<Vec<ManifestUnitSelection>, CatalogError> {
    units
        .iter()
        .map(|unit| {
            if unit.id == target.id {
                return Ok(ManifestUnitSelection {
                    publication_unit_id: unit.id,
                    release_id: activated_release_id,
                    serving_generation: next_serving_generation(unit, command)?,
                    data_revision: command.data_revision.as_uuid(),
                    canonical_iceberg_snapshot_id: command
                        .canonical_iceberg_snapshot_id
                        .as_str()
                        .to_owned(),
                });
            }
            let selection = carried.get(&unit.id).ok_or_else(|| {
                CatalogError::InvalidVectorTileRuntimeManifest(format!(
                    "publication unit {} has never published, so no complete manifest can be \
                     assembled; activate it before switching {}",
                    unit.unit_key, command.unit_key
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

/// Resolves the activated unit's next serving generation, refusing a stale observation.
///
/// The stored `serving_generation` is not on its own evidence of what the caller could have seen:
/// the column defaults to 1, so a unit that has never published carries 1 while having no active
/// release at all. Only `active_release_id` distinguishes the two states, which is why a first
/// publication is exactly "both expectations absent" and anything else is compared against both.
pub(super) fn next_serving_generation(
    unit: &LockedPublicationUnit,
    command: &MarkTileLayerDynamicCommand,
) -> Result<i64, CatalogError> {
    let expected_release = command
        .expected_active_release_id
        .map(|release_id| release_id.as_uuid());
    let expected_generation = command
        .expected_serving_generation
        .map(ServingGeneration::value);

    match unit.active_release_id {
        None => {
            if expected_release.is_some() || expected_generation.is_some() {
                return Err(serving_state_conflict(
                    unit,
                    expected_release,
                    expected_generation,
                ));
            }
            Ok(1)
        }
        Some(active_release_id) => {
            let observed = observed_serving_generation(unit)?;
            if expected_release != Some(active_release_id) || expected_generation != Some(observed)
            {
                return Err(serving_state_conflict(
                    unit,
                    expected_release,
                    expected_generation,
                ));
            }
            advance_serving_generation(unit)
        }
    }
}

/// The next generation for a unit whose selected release changes.
///
/// Only for the unit being switched. A carried unit keeps its generation, because the value tracks
/// one unit's source selection and a carry-forward changes nothing about it.
pub(super) fn advance_serving_generation(
    unit: &LockedPublicationUnit,
) -> Result<i64, CatalogError> {
    unit.serving_generation.checked_add(1).ok_or_else(|| {
        CatalogError::InvalidVectorTileRuntimeManifest(format!(
            "publication unit {} has exhausted its serving generation",
            unit.unit_key
        ))
    })
}

pub(super) fn observed_serving_generation(
    unit: &LockedPublicationUnit,
) -> Result<u64, CatalogError> {
    u64::try_from(unit.serving_generation).map_err(|error| {
        CatalogError::Infrastructure(format!(
            "publication unit {} has a negative serving generation: {error}",
            unit.unit_key
        ))
    })
}

pub(super) fn serving_state_conflict(
    unit: &LockedPublicationUnit,
    expected_release: Option<Uuid>,
    expected_generation: Option<u64>,
) -> CatalogError {
    // The stored generation is only reportable alongside an active release: without one it is the
    // column default, not something the caller could have observed.
    let current = unit.active_release_id.map_or_else(
        || describe_serving_state(None, None),
        |active_release_id| {
            describe_serving_state(
                Some(active_release_id),
                u64::try_from(unit.serving_generation).ok(),
            )
        },
    );
    CatalogError::VectorTileServingStateConflict {
        unit_key: unit.unit_key.clone(),
        expected: describe_serving_state(expected_release, expected_generation),
        current,
    }
}

/// Renders an observed-state pair the way the command carries it: both present, or both absent.
pub(super) fn describe_serving_state(release_id: Option<Uuid>, generation: Option<u64>) -> String {
    match (release_id, generation) {
        (None, None) => "unpublished".to_owned(),
        (release_id, generation) => format!(
            "release={} generation={}",
            release_id.map_or_else(|| "none".to_owned(), |id| id.to_string()),
            generation.map_or_else(|| "none".to_owned(), |value| value.to_string()),
        ),
    }
}

/// Every release the assembled manifest names, minus the one this transaction is about to insert.
///
/// The new release cannot be locked before it exists, and does not need to be: no other transaction
/// can reach a row this one has not committed.
pub(super) fn carried_release_ids(
    selections: &[ManifestUnitSelection],
    activated_release_id: Uuid,
) -> Vec<Uuid> {
    selections
        .iter()
        .map(|selection| selection.release_id)
        .filter(|release_id| *release_id != activated_release_id)
        .collect()
}

pub(super) async fn insert_dynamic_release_tx(
    tx: &mut Transaction<'_, Postgres>,
    release_id: Uuid,
    publication_unit_id: Uuid,
    command: &MarkTileLayerDynamicCommand,
) -> Result<(), CatalogError> {
    let source_file_asset_ids = command
        .lineage
        .source_file_asset_ids
        .iter()
        .map(FileAssetId::as_uuid)
        .collect::<Vec<_>>();
    let insert = sqlx::query(
        "INSERT INTO catalog.vector_tile_release
         (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
          source_record_id, source_file_asset_ids, source_kind, martin_source_id,
          tiles_url_template, postgis_projection_revision)
         VALUES ($1, $2, $3, $4, $5, $6, 'dynamic_postgis', $7, $8, $9)",
    )
    .bind(release_id)
    .bind(publication_unit_id)
    .bind(command.data_revision.as_uuid())
    .bind(command.canonical_iceberg_snapshot_id.as_str())
    .bind(command.lineage.source_record_id.as_uuid())
    .bind(&source_file_asset_ids)
    .bind(&command.martin_source_id)
    .bind(command.tiles_url_template.as_str())
    .bind(command.postgis_projection_revision.as_uuid())
    .execute(&mut **tx)
    .await;

    match insert {
        Ok(_) => Ok(()),
        // `vector_tile_release_unit_revision_snapshot_kind_key`. This is what a replayed activation
        // hits: the unit already has a dynamic release for exactly this revision and snapshot, so
        // the retry is refused instead of publishing a second one.
        Err(sqlx::Error::Database(database))
            if is_unique_violation_code(database.code().as_deref()) =>
        {
            Err(CatalogError::InvalidVectorTileRuntimeManifest(format!(
                "publication unit {} already has a dynamic release for data revision {} at snapshot {}",
                command.unit_key,
                command.data_revision,
                command.canonical_iceberg_snapshot_id.as_str()
            )))
        }
        Err(error) => Err(map_sqlx(error)),
    }
}

pub(super) async fn insert_release_layers_tx(
    tx: &mut Transaction<'_, Postgres>,
    release_id: Uuid,
    layers: &BTreeMap<String, RuntimeTileLayer>,
) -> Result<(), CatalogError> {
    for (layer_id, layer) in layers {
        let filter_properties = serde_json::to_value(&layer.feature_filter_properties)
            .map_err(|error| CatalogError::Infrastructure(format!("serde encode: {error}")))?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_release_layer
             (release_id, layer_id, source_layer, feature_id_property,
              tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
              feature_filter_properties)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(release_id)
        .bind(layer_id)
        .bind(&layer.source_layer)
        .bind(layer.feature_id_property.as_str())
        .bind(i16::from(layer.tile_min_zoom))
        .bind(i16::from(layer.tile_max_zoom))
        .bind(i16::from(layer.render_min_zoom))
        .bind(i16::from(layer.render_max_zoom))
        .bind(&filter_properties)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

/// Writes the immutable manifest row and preallocates the `file_asset` identity of its projection.
///
/// The object key is derived, not supplied: `vector_tile_runtime_manifest_object_key` is the one
/// definition of the create-only layout, so the row and the object that will later be written under
/// it cannot disagree. Size zero and no checksum are the honest description of an identity reserved
/// before the bytes exist.
///
/// The generation is derived under the pointer lock rather than read earlier and passed in — the
/// gate refuses a generation that does not increase, and only a value taken while the pointer is
/// held is still true when the gate compares it.
pub(super) async fn insert_runtime_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    command: &MarkTileLayerDynamicCommand,
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
    .bind(command.lineage.source_record_id.as_uuid())
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

pub(super) async fn insert_manifest_units_tx(
    tx: &mut Transaction<'_, Postgres>,
    manifest_id: Uuid,
    selections: &[ManifestUnitSelection],
) -> Result<(), CatalogError> {
    for selection in selections {
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
             (manifest_id, publication_unit_id, release_id, serving_generation,
              data_revision, canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(manifest_id)
        .bind(selection.publication_unit_id)
        .bind(selection.release_id)
        .bind(selection.serving_generation)
        .bind(selection.data_revision)
        .bind(&selection.canonical_iceberg_snapshot_id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

/// Builds the additive v2 event from the manifest the gate actually published.
///
/// From the manifest, not from the command: the event then reports what serving generation and
/// which units the pointer really names, including the neighbours this activation carried forward.
pub(super) fn runtime_manifest_published_event(
    manifest: &VectorTileRuntimeManifest,
) -> CatalogEvent {
    CatalogEvent::VectorTileRuntimeManifestPublished(VectorTileRuntimeManifestPublishedV2 {
        schema_version: 2,
        manifest_id: manifest.current_version,
        manifest_generation: manifest.manifest_generation.value(),
        publication_units: manifest
            .publication_units
            .iter()
            .map(|(unit_key, unit)| {
                (
                    unit_key.clone(),
                    VectorTileRuntimeUnitSelectionV2 {
                        active_release_id: unit.active_release_id,
                        data_revision: unit.data_revision,
                        serving_generation: unit.serving_generation.value(),
                        canonical_iceberg_snapshot_id: unit
                            .canonical_iceberg_snapshot_id
                            .as_str()
                            .to_owned(),
                    },
                )
            })
            .collect(),
        published_at: manifest.published_at,
    })
}
