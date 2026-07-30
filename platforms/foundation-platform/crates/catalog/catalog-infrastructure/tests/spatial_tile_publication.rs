//! `PostgreSQL` tests for the v2 single-source publication transaction.
//!
//! Each test runs against its own migrated, disposable database rather than the shared harness one.
//! That is not a preference: `catalog.promote_vector_tile_runtime_manifest` refuses a manifest whose
//! unit count differs from `count(*)` over `catalog.vector_tile_publication_unit`, so a unit another
//! test left behind would make every activation here fail for a reason that has nothing to do with
//! the code under test.

#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

#[allow(dead_code)]
mod support;

use std::{collections::BTreeMap, sync::Arc};

use catalog_application::{
    ports::{CatalogUnitOfWork, MarkTileLayerDynamicCommand, RuntimeManifestPublicationCapability},
    MarkTileLayerDynamic,
};
use catalog_domain::{
    ActiveTileSource, CanonicalIcebergSnapshotId, CatalogError, FeatureIdProperty,
    RuntimeTileLayer, RuntimeTileLineage, RuntimeTilesUrlTemplate, ServingGeneration,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_shared_kernel::events::catalog_v1::vector_tile_runtime_manifest_object_key;
use foundation_shared_kernel::ids::{
    FileAssetId, PostgisProjectionRevisionId, SourceRecordId, StaffId, VectorTileDataRevisionId,
    VectorTileReleaseId,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

/// The event type tag the additive v2 publication writes to the outbox.
const RUNTIME_MANIFEST_PUBLISHED_V2: &str = "catalog.vector_tile_runtime_manifest.published.v2";

/// The whole vertical slice: application command, one transaction, and the rows it must leave.
///
/// It goes through [`MarkTileLayerDynamic`] rather than calling the unit of work directly, so the
/// path this asserts is the one production takes — a command normalised by the use case, a
/// transaction that assembles the manifest, the database gate that switches the pointer, and the
/// manifest read back from the committed ledger.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_first_activation_publishes_a_complete_dynamic_manifest() -> TestResult {
    run_in_disposable_database("tile_first_activation", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        let unit_id = seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;

        let manifest = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;

        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.manifest_generation.value(), 1);
        assert_eq!(manifest.refresh_after_seconds, 4);
        assert_eq!(manifest.publication_units.len(), 1);

        let unit = published_unit(&manifest, "parcels")?;
        assert_eq!(unit.serving_generation.value(), 1);
        assert_eq!(unit.data_revision.as_uuid(), revision);
        assert_eq!(
            unit.canonical_iceberg_snapshot_id.as_str(),
            PARCELS_SNAPSHOT
        );
        assert!(
            matches!(unit.source, ActiveTileSource::DynamicPostgis(_)),
            "every unit enters the ledger dynamic; got {:?}",
            unit.source
        );
        assert_eq!(unit.layers.len(), 1);
        let release_id = unit.active_release_id.as_uuid();

        assert_eq!(
            active_pointer(&pool).await?,
            manifest.current_version.as_uuid()
        );

        let unit_row = sqlx::query(
            "SELECT active_release_id, active_data_revision, fallback_release_id, serving_generation
             FROM catalog.vector_tile_publication_unit
             WHERE id = $1",
        )
        .bind(unit_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            unit_row.try_get::<Option<Uuid>, _>("active_release_id")?,
            Some(release_id)
        );
        assert_eq!(
            unit_row.try_get::<Option<Uuid>, _>("active_data_revision")?,
            Some(revision)
        );
        assert_eq!(unit_row.try_get::<i64, _>("serving_generation")?, 1);
        // Nothing was replaced, so there is nothing to fall back to.
        assert_eq!(
            unit_row.try_get::<Option<Uuid>, _>("fallback_release_id")?,
            None
        );

        let release_row = sqlx::query(
            "SELECT source_kind, martin_source_id, postgis_projection_revision, pmtiles_object_key
             FROM catalog.vector_tile_release
             WHERE id = $1",
        )
        .bind(release_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            release_row.try_get::<String, _>("source_kind")?,
            "dynamic_postgis"
        );
        assert_eq!(
            release_row.try_get::<String, _>("martin_source_id")?,
            "parcels"
        );
        assert!(release_row
            .try_get::<Option<Uuid>, _>("postgis_projection_revision")?
            .is_some());
        assert_eq!(
            release_row.try_get::<Option<String>, _>("pmtiles_object_key")?,
            None
        );

        let layer_ids: Vec<String> = sqlx::query_scalar(
            "SELECT layer_id FROM catalog.vector_tile_release_layer
             WHERE release_id = $1 ORDER BY layer_id",
        )
        .bind(release_id)
        .fetch_all(&pool)
        .await?;
        assert_eq!(layer_ids, vec!["parcels".to_owned()]);

        // The immutable manifest's `file_asset` identity is preallocated at the derived key, so the
        // create-only projection written later cannot land anywhere else.
        let object_key: Option<String> = sqlx::query_scalar(
            "SELECT asset.object_key
             FROM catalog.vector_tile_runtime_manifest AS manifest
             JOIN catalog.file_asset AS asset ON asset.id = manifest.manifest_file_asset_id
             WHERE manifest.id = $1",
        )
        .bind(manifest.current_version.as_uuid())
        .fetch_optional(&pool)
        .await?;
        assert_eq!(
            object_key,
            Some(vector_tile_runtime_manifest_object_key(
                manifest.current_version
            ))
        );

        assert_eq!(
            outbox_types(&pool).await?,
            vec![RUNTIME_MANIFEST_PUBLISHED_V2]
        );
        Ok(())
    })
    .await
}

/// Both halves of the compare-and-swap, and the claim that a first publication is a claim.
///
/// `expected_active_release_id: None` is not "skip the check" — it asserts the unit has never
/// published. Against a unit that has, it is as stale as a wrong release id, and both must lose
/// without moving anything.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_stale_observation_loses_the_compare_and_swap_and_moves_nothing() -> TestResult {
    run_in_disposable_database("tile_stale_observation", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        let unit_id = seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let published = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let active_release_id = published_unit(&published, "parcels")?.active_release_id;
        let next_revision = seed_data_revision(&pool, NEXT_SNAPSHOT, source_record_id).await?;

        // A repeated first-publication claim against a unit that has published.
        let repeated_first_claim = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                next_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await
            .expect_err("a unit that has published is not unpublished");
        assert!(
            matches!(
                &repeated_first_claim,
                CatalogError::VectorTileServingStateConflict { unit_key, current, .. }
                    if unit_key == "parcels" && current.contains("generation=1")
            ),
            "got: {repeated_first_claim:?}"
        );

        // The right release at the wrong generation. The release id alone is not the state: a
        // same-revision rollback re-activates a preserved release, so one id can be active twice.
        let stale_generation = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                next_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((active_release_id, ServingGeneration::new(7)?)),
            )?)
            .await
            .expect_err("a stale serving generation must lose the compare-and-swap");
        assert!(
            matches!(
                &stale_generation,
                CatalogError::VectorTileServingStateConflict { unit_key, .. } if unit_key == "parcels"
            ),
            "got: {stale_generation:?}"
        );

        assert_eq!(
            active_pointer(&pool).await?,
            published.current_version.as_uuid(),
            "a refused activation must not move the pointer"
        );
        let serving_generation: i64 = sqlx::query_scalar(
            "SELECT serving_generation FROM catalog.vector_tile_publication_unit WHERE id = $1",
        )
        .bind(unit_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(serving_generation, 1);
        assert_eq!(manifest_count(&pool).await?, 1);
        assert_eq!(release_count(&pool).await?, 1);
        assert_eq!(outbox_types(&pool).await?, vec![RUNTIME_MANIFEST_PUBLISHED_V2]);
        Ok(())
    })
    .await
}

/// The completeness rule, and the serving-generation cost it carries.
///
/// One unit changes but the manifest names every unit, because the gate rejects
/// `next_unit_count <> publication_unit_count`. The carried unit's *release* is unchanged while its
/// serving generation advances anyway: the gate compares every selected unit against
/// `unit.serving_generation + 1`, so a manifest that repeated a neighbour's current generation is
/// refused as a serving-generation gap. This is asserted rather than described because it is the
/// one place the installed gate is stricter than "per-unit generations" suggests.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn activating_one_unit_carries_every_other_unit_into_the_new_manifest() -> TestResult {
    run_in_disposable_database("tile_carry_forward", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let parcels_revision =
            seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let first = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                parcels_revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let parcels_release = published_unit(&first, "parcels")?.active_release_id;

        // A second unit is seeded, then activated. That order is forced: its first activation is
        // also the first manifest that can name it, and a manifest naming a unit that has never
        // published has no release to select for it.
        seed_publication_unit(&pool, "complex").await?;
        let complex_revision =
            seed_data_revision(&pool, COMPLEX_SNAPSHOT, source_record_id).await?;
        let second = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "complex",
                complex_revision,
                COMPLEX_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;

        assert_eq!(second.manifest_generation.value(), 2);
        assert_eq!(second.publication_units.len(), 2);
        let carried = published_unit(&second, "parcels")?;
        assert_eq!(
            carried.active_release_id, parcels_release,
            "the untouched unit keeps its release"
        );
        assert_eq!(carried.data_revision.as_uuid(), parcels_revision);
        assert_eq!(
            carried.serving_generation.value(),
            2,
            "the gate advances every selected unit's serving generation, carried or not"
        );
        assert_eq!(
            published_unit(&second, "complex")?
                .serving_generation
                .value(),
            1
        );

        // Switching the first unit again now has to carry the second one, in the other direction.
        let third_revision = seed_data_revision(&pool, NEXT_SNAPSHOT, source_record_id).await?;
        let third = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                third_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((parcels_release, ServingGeneration::new(2)?)),
            )?)
            .await?;

        assert_eq!(third.manifest_generation.value(), 3);
        assert_eq!(third.publication_units.len(), 2);
        let switched = published_unit(&third, "parcels")?;
        assert_ne!(switched.active_release_id, parcels_release);
        assert_eq!(switched.data_revision.as_uuid(), third_revision);
        assert_eq!(switched.serving_generation.value(), 3);
        let complex = published_unit(&third, "complex")?;
        assert_eq!(complex.data_revision.as_uuid(), complex_revision);
        assert_eq!(complex.serving_generation.value(), 2);

        assert_eq!(
            active_pointer(&pool).await?,
            third.current_version.as_uuid()
        );
        assert_eq!(
            outbox_types(&pool).await?,
            vec![
                RUNTIME_MANIFEST_PUBLISHED_V2,
                RUNTIME_MANIFEST_PUBLISHED_V2,
                RUNTIME_MANIFEST_PUBLISHED_V2
            ]
        );
        Ok(())
    })
    .await
}

/// A unit that has never published cannot be silently dropped from the manifest.
///
/// The gate would refuse the assembled manifest as incomplete; the transaction names the unit
/// instead, so the operator learns which one is missing rather than that a count did not match.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_unpublished_neighbour_refuses_the_activation_by_name() -> TestResult {
    run_in_disposable_database("tile_incomplete_manifest", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        seed_publication_unit(&pool, "complex").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await
            .expect_err("a manifest that drops a publication unit is not a complete publication");
        assert!(
            matches!(
                &error,
                CatalogError::InvalidVectorTileRuntimeManifest(message)
                    if message.contains("publication unit complex has never published")
            ),
            "got: {error:?}"
        );

        assert_eq!(manifest_count(&pool).await?, 0);
        assert_eq!(release_count(&pool).await?, 0);
        assert!(outbox_types(&pool).await?.is_empty());
        Ok(())
    })
    .await
}

/// An activation naming a unit nobody configured is refused rather than creating one.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_unknown_publication_unit_is_refused() -> TestResult {
    run_in_disposable_database("tile_unknown_unit", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await
            .expect_err("publication units are seeded, not created by an activation");
        assert!(
            matches!(
                &error,
                CatalogError::InvalidVectorTileRuntimeManifest(message)
                    if message.contains("publication unit parcels does not exist")
            ),
            "got: {error:?}"
        );
        assert_eq!(release_count(&pool).await?, 0);
        Ok(())
    })
    .await
}

/// A replayed activation writes nothing at all — not a partial anything.
///
/// The retry passes the compare-and-swap because it carries the state the first one produced, and
/// then collides with `vector_tile_release_unit_revision_snapshot_kind_key`: the unit already has a
/// dynamic release for exactly this revision and snapshot. Everything written before the collision —
/// the release attempt, the manifest row, its `file_asset`, the manifest unit rows — has to
/// disappear with it, which is the property the whole transaction exists for.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_replayed_activation_leaves_no_partial_state() -> TestResult {
    run_in_disposable_database("tile_replayed_activation", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let published = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let active_release_id = published_unit(&published, "parcels")?.active_release_id;
        let file_assets_before = file_asset_count(&pool).await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                Some((active_release_id, ServingGeneration::new(1)?)),
            )?)
            .await
            .expect_err("one revision publishes once per source kind");
        assert!(
            matches!(
                &error,
                CatalogError::InvalidVectorTileRuntimeManifest(message)
                    if message.contains("already has a dynamic release for data revision")
            ),
            "got: {error:?}"
        );

        assert_eq!(
            active_pointer(&pool).await?,
            published.current_version.as_uuid()
        );
        assert_eq!(manifest_count(&pool).await?, 1);
        assert_eq!(release_count(&pool).await?, 1);
        assert_eq!(file_asset_count(&pool).await?, file_assets_before);
        let manifest_unit_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM catalog.vector_tile_runtime_manifest_unit")
                .fetch_one(&pool)
                .await?;
        assert_eq!(manifest_unit_count, 1);
        assert_eq!(
            outbox_types(&pool).await?,
            vec![RUNTIME_MANIFEST_PUBLISHED_V2]
        );
        Ok(())
    })
    .await
}

/// Publication off still records the activation; it only withholds the public event.
///
/// This is what keeps v1 behaviour byte-identical while v2 rolls out — a deployment that refused the
/// activation outright could not also keep an internal ledger of it.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn publication_capability_off_records_the_activation_without_the_public_event() -> TestResult
{
    run_in_disposable_database("tile_capability_off", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        let unit_id = seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;

        let manifest = use_case(&pool, RuntimeManifestPublicationCapability::disabled())
            .execute(activation_command(
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;

        assert_eq!(
            active_pointer(&pool).await?,
            manifest.current_version.as_uuid()
        );
        let active_release_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT active_release_id FROM catalog.vector_tile_publication_unit WHERE id = $1",
        )
        .bind(unit_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            active_release_id,
            Some(
                published_unit(&manifest, "parcels")?
                    .active_release_id
                    .as_uuid()
            )
        );
        assert!(
            outbox_types(&pool).await?.is_empty(),
            "publication is off, so no public v2 event may be emitted"
        );
        Ok(())
    })
    .await
}

/// The default is fail-closed: a unit of work built without a stated capability publishes nothing.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn the_default_unit_of_work_does_not_publish_the_v2_event() -> TestResult {
    run_in_disposable_database("tile_default_capability", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;

        let uow = PgCatalogUnitOfWork::new(pool.clone());
        uow.mark_tile_layer_dynamic(activation_command(
            "parcels",
            revision,
            PARCELS_SNAPSHOT,
            source_record_id,
            None,
        )?)
        .await?;

        assert!(outbox_types(&pool).await?.is_empty());
        Ok(())
    })
    .await
}

const PARCELS_SNAPSHOT: &str = "841361364657368623";
const COMPLEX_SNAPSHOT: &str = "841361364657368624";
const NEXT_SNAPSHOT: &str = "841361364657368625";

fn use_case(
    pool: &PgPool,
    capability: RuntimeManifestPublicationCapability,
) -> MarkTileLayerDynamic {
    MarkTileLayerDynamic::new(Arc::new(
        PgCatalogUnitOfWork::new(pool.clone()).with_runtime_manifest_publication(capability),
    ))
}

fn published_unit<'manifest>(
    manifest: &'manifest catalog_domain::VectorTileRuntimeManifest,
    unit_key: &str,
) -> TestResult<&'manifest catalog_domain::PublicationUnit> {
    manifest
        .publication_units
        .get(unit_key)
        .ok_or_else(|| format!("{unit_key} is missing from the published manifest").into())
}

fn activation_command(
    unit_key: &str,
    data_revision: Uuid,
    snapshot: &str,
    source_record_id: Uuid,
    expected: Option<(VectorTileReleaseId, ServingGeneration)>,
) -> TestResult<MarkTileLayerDynamicCommand> {
    Ok(MarkTileLayerDynamicCommand {
        unit_key: unit_key.to_owned(),
        expected_active_release_id: expected.map(|(release_id, _)| release_id),
        expected_serving_generation: expected.map(|(_, generation)| generation),
        data_revision: VectorTileDataRevisionId::new(data_revision),
        canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(snapshot.to_owned())?,
        postgis_projection_revision: PostgisProjectionRevisionId::new(Uuid::new_v4()),
        martin_source_id: unit_key.to_owned(),
        tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
            "https://tiles.example.test/{unit_key}/{{z}}/{{x}}/{{y}}"
        ))?,
        layers: BTreeMap::from([(
            unit_key.to_owned(),
            RuntimeTileLayer {
                source_layer: unit_key.to_owned(),
                feature_id_property: FeatureIdProperty::new("pnu".to_owned())?,
                tile_min_zoom: 14,
                tile_max_zoom: 16,
                render_min_zoom: 14,
                render_max_zoom: 22,
                feature_filter_properties: BTreeMap::from([("pnu".to_owned(), "pnu".to_owned())]),
            },
        )]),
        lineage: RuntimeTileLineage {
            source_record_id: SourceRecordId::new(source_record_id),
            source_file_asset_ids: vec![FileAssetId::new(Uuid::new_v4())],
        },
        idempotency_key: format!("activate-{unit_key}-{data_revision}"),
        operator_staff_id: StaffId::new(Uuid::new_v4()),
    })
}

async fn seed_source_record(pool: &PgPool) -> TestResult<Uuid> {
    let source_record_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
         VALUES ($1, 'test', $2, repeat('a', 64))",
    )
    .bind(source_record_id)
    .bind(format!("spatial-tile-publication-{source_record_id}"))
    .execute(pool)
    .await?;
    Ok(source_record_id)
}

/// Seeds the revision `vector_tile_release_data_revision_fkey` binds a release to.
async fn seed_data_revision(
    pool: &PgPool,
    snapshot: &str,
    source_record_id: Uuid,
) -> TestResult<Uuid> {
    let revision_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.administrative_boundary_revision
         (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id,
          status, validated_at)
         VALUES ($1, $2, $3, $4, 'published', now())",
    )
    .bind(revision_id)
    .bind(snapshot)
    .bind(format!("iceberg:spatial-tile-publication-{revision_id}"))
    .bind(source_record_id)
    .execute(pool)
    .await?;
    Ok(revision_id)
}

/// Publication units are provisioned, never created by an activation.
async fn seed_publication_unit(pool: &PgPool, unit_key: &str) -> TestResult<Uuid> {
    let unit_id = Uuid::new_v4();
    sqlx::query("INSERT INTO catalog.vector_tile_publication_unit (id, unit_key) VALUES ($1, $2)")
        .bind(unit_id)
        .bind(unit_key)
        .execute(pool)
        .await?;
    Ok(unit_id)
}

async fn active_pointer(pool: &PgPool) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(
        "SELECT manifest_id FROM catalog.vector_tile_runtime_manifest_pointer WHERE singleton",
    )
    .fetch_one(pool)
    .await?)
}

async fn manifest_count(pool: &PgPool) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM catalog.vector_tile_runtime_manifest")
            .fetch_one(pool)
            .await?,
    )
}

async fn release_count(pool: &PgPool) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM catalog.vector_tile_release")
            .fetch_one(pool)
            .await?,
    )
}

async fn file_asset_count(pool: &PgPool) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM catalog.file_asset")
            .fetch_one(pool)
            .await?,
    )
}

async fn outbox_types(pool: &PgPool) -> TestResult<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT type FROM catalog.outbox_event ORDER BY occurred_at, event_id")
            .fetch_all(pool)
            .await?,
    )
}
