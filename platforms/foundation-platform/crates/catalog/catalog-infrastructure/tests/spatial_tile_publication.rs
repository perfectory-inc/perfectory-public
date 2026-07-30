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
    ports::{
        CatalogUnitOfWork, MarkTileLayerDynamicCommand, PublishedRuntimeManifest,
        RuntimeManifestPublicationCapability,
    },
    MarkTileLayerDynamic,
};
use catalog_domain::{
    ActiveTileSource, CanonicalIcebergSnapshotId, CatalogError, FeatureIdProperty,
    RuntimeTileLayer, RuntimeTileLineage, RuntimeTilesUrlTemplate, ServingGeneration,
    VectorTileRuntimeManifest,
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

        // The mutation, its outbox event and its ledger row are one transaction. The recorded event id
        // is what makes that assertable rather than merely intended.
        let emitted: Vec<Uuid> = sqlx::query_scalar("SELECT event_id FROM catalog.outbox_event")
            .fetch_all(&pool)
            .await?;
        assert_eq!(
            ledger_outbox_event_id(&pool, &format!("activate-parcels-{revision}")).await?,
            emitted.first().copied()
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
            .attempt(activation_command(
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
            .attempt(activation_command(
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

/// The completeness rule, and what it does *not* cost.
///
/// One unit changes but the manifest names every unit, because the gate rejects
/// `next_unit_count <> publication_unit_count`. The carried unit keeps both its release and its
/// serving generation: the value tracks one unit's source selection, and a carry-forward changes
/// nothing about it (ADR-0014). Asserted rather than described because the gate used to demand
/// `unit.serving_generation + 1` from every selected unit, which made per-unit invalidation
/// meaningless — every publication invalidated every unit.
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
            1,
            "a carried unit's source selection did not change, so its generation does not move"
        );
        assert_eq!(
            published_unit(&second, "complex")?
                .serving_generation
                .value(),
            1
        );

        // Switching the first unit again now has to carry the second one, in the other direction.
        // Its expectation is still generation 1 — publishing `complex` did not invalidate it.
        let third_revision = seed_data_revision(&pool, NEXT_SNAPSHOT, source_record_id).await?;
        let third = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                third_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((parcels_release, ServingGeneration::new(1)?)),
            )?)
            .await?;

        assert_eq!(third.manifest_generation.value(), 3);
        assert_eq!(third.publication_units.len(), 2);
        let switched = published_unit(&third, "parcels")?;
        assert_ne!(switched.active_release_id, parcels_release);
        assert_eq!(switched.data_revision.as_uuid(), third_revision);
        assert_eq!(switched.serving_generation.value(), 2);
        let complex = published_unit(&third, "complex")?;
        assert_eq!(complex.data_revision.as_uuid(), complex_revision);
        assert_eq!(
            complex.serving_generation.value(),
            1,
            "the global manifest advanced three times; `complex` was switched once"
        );

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

/// Task 6 Step 1's first interleaving: two edits to the *same* unit from one observed state.
///
/// Exactly one may commit. The loser has to receive a typed conflict rather than a serialization
/// failure, because the two mean different things to a caller: it observed a state that is no longer
/// current, and re-reading is the fix, not retrying the same command.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn two_edits_to_one_unit_from_one_observed_state_leave_exactly_one_winner() -> TestResult {
    run_in_disposable_database("tile_same_unit_race", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let first_revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let published = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                first_revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let observed = published_unit(&published, "parcels")?.active_release_id;
        let observed_generation = ServingGeneration::new(1)?;

        // Two different revisions, one observed state. Distinct revisions keep the release
        // uniqueness key out of it, so the compare-and-swap is the only thing that can decide.
        let left_revision = seed_data_revision(&pool, NEXT_SNAPSHOT, source_record_id).await?;
        let right_revision = seed_data_revision(&pool, FOURTH_SNAPSHOT, source_record_id).await?;
        let capability = RuntimeManifestPublicationCapability::enabled();
        let left_writer = use_case(&pool, capability);
        let right_writer = use_case(&pool, capability);
        let (left, right) = tokio::join!(
            left_writer.attempt(activation_command(
                "parcels",
                left_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((observed, observed_generation)),
            )?),
            right_writer.attempt(activation_command(
                "parcels",
                right_revision,
                FOURTH_SNAPSHOT,
                source_record_id,
                Some((observed, observed_generation)),
            )?),
        );

        let (winner, loser) = match (left, right) {
            (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
            (Ok(_), Ok(_)) => {
                return Err("both activations committed; the compare-and-swap did not hold".into())
            }
            (Err(left), Err(right)) => {
                return Err(format!("neither activation committed: {left}; {right}").into())
            }
        };
        assert!(
            matches!(
                &loser,
                CatalogError::VectorTileServingStateConflict { unit_key, .. } if unit_key == "parcels"
            ),
            "the loser must learn its observation is stale, not that the database serialized: {loser:?}"
        );

        // Both commands carry distinct idempotency keys (derived from their distinct revisions), so
        // the winner published rather than being answered from the ledger.
        let winner = expect_published(winner)?;
        assert_eq!(winner.manifest_generation.value(), 2);
        assert_eq!(
            published_unit(&winner, "parcels")?.serving_generation.value(),
            2
        );
        assert_eq!(active_pointer(&pool).await?, winner.current_version.as_uuid());
        assert_eq!(
            manifest_count(&pool).await?,
            2,
            "the loser's manifest rows must have rolled back with its transaction"
        );
        assert_eq!(release_count(&pool).await?, 2);
        assert_eq!(outbox_types(&pool).await?.len(), 2);
        Ok(())
    })
    .await
}

/// Task 6 Step 1's second interleaving: two edits to *different* units, started together.
///
/// Both observe their own unit before either commits, and both must commit — they are not in
/// conflict. Under the old gap rule the second one lost a compare-and-swap it was not in, because
/// the first one's publication advanced *its* unit's generation too.
///
/// The proof that they serialize on the pointer rather than getting lucky is the later manifest: it
/// holds the earlier writer's new release as well as its own, which is only possible if it read the
/// pointer after the earlier one committed. Step 1 asks for exactly this rather than for a database
/// serialization failure — the lock order is the thing under test, and neither selection is dropped.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn two_edits_to_different_units_both_commit_and_advance_the_global_generation_in_order(
) -> TestResult {
    run_in_disposable_database("tile_two_unit_interleave", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;

        // Both units published, one at a time, because a first activation is also the first manifest
        // that can name its unit.
        seed_publication_unit(&pool, "parcels").await?;
        let parcels_first = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let bootstrap = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "parcels",
                parcels_first,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let parcels_release = published_unit(&bootstrap, "parcels")?.active_release_id;
        seed_publication_unit(&pool, "complex").await?;
        let complex_first = seed_data_revision(&pool, COMPLEX_SNAPSHOT, source_record_id).await?;
        let paired = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command(
                "complex",
                complex_first,
                COMPLEX_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let complex_release = published_unit(&paired, "complex")?.active_release_id;
        let generation_before = paired.manifest_generation.value();

        // Both expectations are read here, before either edit runs. That is the interleaving.
        let parcels_next = seed_data_revision(&pool, NEXT_SNAPSHOT, source_record_id).await?;
        let complex_next = seed_data_revision(&pool, FOURTH_SNAPSHOT, source_record_id).await?;
        let switch_parcels = activation_command(
            "parcels",
            parcels_next,
            NEXT_SNAPSHOT,
            source_record_id,
            Some((parcels_release, ServingGeneration::new(1)?)),
        )?;
        let switch_complex = activation_command(
            "complex",
            complex_next,
            FOURTH_SNAPSHOT,
            source_record_id,
            Some((complex_release, ServingGeneration::new(1)?)),
        )?;
        let capability = RuntimeManifestPublicationCapability::enabled();
        // Bound rather than inlined: `execute` borrows the use case, and a temporary inside
        // `join!` would be dropped while its future still holds the borrow.
        let parcels_writer = use_case(&pool, capability);
        let complex_writer = use_case(&pool, capability);
        let (parcels_result, complex_result) = tokio::join!(
            parcels_writer.attempt(switch_parcels),
            complex_writer.attempt(switch_complex),
        );
        let parcels_published = expect_published(parcels_result?)?;
        let complex_published = expect_published(complex_result?)?;

        // Which one wins the lock is not fixed, so order the two results by what the database
        // decided instead of assuming it.
        let (earlier, later) = if parcels_published.manifest_generation.value()
            < complex_published.manifest_generation.value()
        {
            (&parcels_published, &complex_published)
        } else {
            (&complex_published, &parcels_published)
        };
        assert_eq!(earlier.manifest_generation.value(), generation_before + 1);
        assert_eq!(
            later.manifest_generation.value(),
            generation_before + 2,
            "the global generation advances twice, in order"
        );

        for manifest in [earlier, later] {
            assert_eq!(
                manifest.publication_units.len(),
                2,
                "each manifest selects both units exactly once"
            );
        }
        // The later writer carried the earlier writer's *new* release, not the one it observed.
        assert_ne!(
            published_unit(later, "parcels")?.active_release_id,
            parcels_release
        );
        assert_ne!(
            published_unit(later, "complex")?.active_release_id,
            complex_release
        );
        // The earlier writer switched exactly one unit and carried the other unchanged.
        let earlier_switched = [
            published_unit(earlier, "parcels")?.active_release_id != parcels_release,
            published_unit(earlier, "complex")?.active_release_id != complex_release,
        ];
        assert_eq!(
            earlier_switched.iter().filter(|changed| **changed).count(),
            1,
            "the earlier manifest changes one unit and carries the other"
        );

        assert_eq!(
            active_pointer(&pool).await?,
            later.current_version.as_uuid()
        );
        let units = sqlx::query(
            "SELECT unit_key, active_release_id, serving_generation
             FROM catalog.vector_tile_publication_unit
             ORDER BY unit_key",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(units.len(), 2);
        for row in &units {
            assert_eq!(
                row.try_get::<i64, _>("serving_generation")?,
                2,
                "{} was switched exactly once",
                row.try_get::<String, _>("unit_key")?
            );
        }
        assert_eq!(outbox_types(&pool).await?.len(), 4);
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
            .attempt(activation_command(
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
            .attempt(activation_command(
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

/// Re-publishing one revision after observing it writes nothing at all — not a partial anything.
///
/// Deliberately *not* a replay, and the name used to say otherwise. A client retry resends the
/// identical body, so its `expected_active_release_id` is still `None`, and the compare-and-swap
/// refuses it before any row is written — that path is
/// `a_stale_observation_loses_the_compare_and_swap_and_moves_nothing`. What this covers is the other
/// shape: a caller that re-read the state and asks to publish the *same* revision again. That one
/// passes the compare-and-swap and then collides with
/// `vector_tile_release_unit_revision_snapshot_kind_key`, which is the only case where the
/// transaction has already written a release, a manifest, its `file_asset` and the manifest unit rows
/// before failing. All of it has to disappear together, which is the property the transaction exists
/// for and the reason this test is worth having under an honest name.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn re_publishing_one_revision_after_observing_it_leaves_no_partial_state() -> TestResult {
    run_in_disposable_database("tile_republish_revision", |pool| async move {
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

        // A *new* key deliberately: the same key would be caught by the idempotency ledger before any
        // row is written, which is the right behaviour and the wrong test — nothing would have been
        // written to roll back. A fresh key is a genuinely new request that happens to target a
        // revision already published, which is the only way to reach the uniqueness key with writes
        // already in the transaction.
        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(activation_command_with_key(
                "republish-parcels-under-a-new-key",
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

/// A true retry: the identical body under the identical key is answered from the ledger.
///
/// This is the case Step 5 asks for and the one nothing covered before — the earlier "replay" test
/// sent a different body. Identical means identical, `expected_active_release_id: None` included, even
/// though that observation is now stale: a retry does not get to re-observe, and the ledger answers
/// before the compare-and-swap is ever evaluated.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_identical_retry_is_answered_from_the_ledger_and_writes_nothing() -> TestResult {
    run_in_disposable_database("tile_true_replay", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let command = || {
            activation_command_with_key(
                "retry-me",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )
        };

        let first = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(command()?)
            .await?;
        let manifests_before = manifest_count(&pool).await?;
        let releases_before = release_count(&pool).await?;
        let assets_before = file_asset_count(&pool).await?;

        let retried = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(command()?)
            .await?;

        assert!(
            retried.was_replayed(),
            "an identical retry must be answered from the ledger, not re-executed"
        );
        let replayed = retried.into_manifest();
        assert_eq!(
            replayed.current_version, first.current_version,
            "the retry must return the manifest the first call published"
        );
        assert_eq!(
            replayed.manifest_generation.value(),
            first.manifest_generation.value()
        );
        // Byte-identical, not merely equal-looking: the reply is re-derived from immutable rows, so a
        // drift in how it is rebuilt would show up here rather than in production on someone's retry.
        assert_eq!(
            serde_json::to_string(&replayed)?,
            serde_json::to_string(&first)?
        );

        assert_eq!(manifest_count(&pool).await?, manifests_before);
        assert_eq!(release_count(&pool).await?, releases_before);
        assert_eq!(file_asset_count(&pool).await?, assets_before);
        assert_eq!(
            outbox_types(&pool).await?,
            vec![RUNTIME_MANIFEST_PUBLISHED_V2],
            "a replay must not announce a second time"
        );
        assert_eq!(ledger_row_count(&pool).await?, 1);
        Ok(())
    })
    .await
}

/// The same key carrying a different request is a key reuse, not a retry.
///
/// Answering it with the earlier outcome would report a publication the caller never asked for. The
/// refusal has to be its own error: the caller's fix is to mint a new key, which is a different action
/// from "re-read state and retry" and from "resend the identical body".
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn an_idempotency_key_reused_for_a_different_request_is_refused() -> TestResult {
    run_in_disposable_database("tile_key_reuse", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;
        let next_revision = seed_data_revision(&pool, NEXT_SNAPSHOT, source_record_id).await?;

        let published = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .execute(activation_command_with_key(
                "reused-key",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await?;
        let manifests_before = manifest_count(&pool).await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(activation_command_with_key(
                "reused-key",
                "parcels",
                next_revision,
                NEXT_SNAPSHOT,
                source_record_id,
                Some((
                    published_unit(&published, "parcels")?.active_release_id,
                    ServingGeneration::new(1)?,
                )),
            )?)
            .await
            .expect_err("a key already spent on another request must not answer this one");
        assert!(
            matches!(
                &error,
                CatalogError::MutationIdempotencyKeyReused { idempotency_key, command_kind }
                    if idempotency_key == "reused-key" && command_kind == "mark_tile_layer_dynamic"
            ),
            "got: {error:?}"
        );

        assert_eq!(
            active_pointer(&pool).await?,
            published.current_version.as_uuid()
        );
        assert_eq!(manifest_count(&pool).await?, manifests_before);
        assert_eq!(ledger_row_count(&pool).await?, 1);
        Ok(())
    })
    .await
}

/// A key held by an uncommitted transaction answers "contended", not an opaque server error.
///
/// This exists because the reasoning behind it is not self-evident and was flagged as needing proof
/// rather than argument: that `lock_timeout` bounds the wait an `ON CONFLICT DO NOTHING` insert
/// performs on a conflicting row's transaction. Without that bound the wait runs into the pool's
/// 2500ms `statement_timeout`, arrives as an infrastructure failure, and is served as a 500 with the
/// reason redacted — hiding the one action that resolves it.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_key_held_by_an_uncommitted_transaction_answers_contended() -> TestResult {
    run_in_disposable_database("tile_key_contended", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let source_record_id = seed_source_record(&pool).await?;
        seed_publication_unit(&pool, "parcels").await?;
        let revision = seed_data_revision(&pool, PARCELS_SNAPSHOT, source_record_id).await?;

        // Hold the key from a transaction that never commits. `outcome_manifest_id` may name a
        // manifest that does not exist because the foreign key is deferred to commit time, and this
        // transaction is rolled back — which is also a small proof that the deferral works.
        let mut holder = pool.begin().await?;
        sqlx::query(
            "INSERT INTO catalog.catalog_mutation_idempotency
             (idempotency_key, command_kind, request_fingerprint_sha256,
              request_fingerprint_schema_version, outcome_manifest_id, operator_staff_id)
             VALUES ('contended-key', 'mark_tile_layer_dynamic', repeat('a', 64), 'test.v1', $1, $2)",
        )
        .bind(Uuid::new_v4())
        .bind(OPERATOR_STAFF_ID)
        .execute(&mut *holder)
        .await?;

        let error = use_case(&pool, RuntimeManifestPublicationCapability::enabled())
            .attempt(activation_command_with_key(
                "contended-key",
                "parcels",
                revision,
                PARCELS_SNAPSHOT,
                source_record_id,
                None,
            )?)
            .await
            .expect_err("a key held by an uncommitted transaction cannot be claimed");
        assert!(
            matches!(
                &error,
                CatalogError::MutationContended { idempotency_key } if idempotency_key == "contended-key"
            ),
            "the wait must be bounded and named, not reported as an infrastructure failure: {error:?}"
        );

        holder.rollback().await?;
        assert_eq!(
            ledger_row_count(&pool).await?,
            0,
            "the holder rolled back, so its claim and its dangling manifest reference are gone"
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
        // Recorded as null rather than left unstated: a later retry under the same key replays this
        // outcome and still announces nothing, even if the capability is on by then. Flipping the flag
        // does not retro-announce — the operator has to publish a new manifest.
        assert_eq!(
            ledger_outbox_event_id(&pool, &format!("activate-parcels-{revision}")).await?,
            None
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

/// Fixed operator identity. Inside the request fingerprint, so it must not vary per call.
const OPERATOR_STAFF_ID: Uuid = Uuid::from_u128(0x019d_2b87_3fd1_7e3a_8d88_0b72_c874_3777);

/// A satellite identifier derived from a revision, so rebuilding one command twice yields one request.
///
/// Distinct `slot` values keep the derived ids from colliding with each other or with the revision.
const fn derived_id(revision: Uuid, slot: u128) -> Uuid {
    Uuid::from_u128(revision.as_u128() ^ (slot << 96))
}

const PARCELS_SNAPSHOT: &str = "841361364657368623";
const COMPLEX_SNAPSHOT: &str = "841361364657368624";
const NEXT_SNAPSHOT: &str = "841361364657368625";
const FOURTH_SNAPSHOT: &str = "841361364657368626";

/// The activation use case, with the two ways a test wants to call it.
///
/// `execute` asserts the outcome is a fresh publication and hands back the manifest, so every test
/// that means "publish this" also states that it was not answered from the idempotency ledger. That
/// matters because a replay returns a *valid* manifest: a regression that started replaying would
/// otherwise compare equal in most of these assertions and pass.
///
/// `attempt` returns the outcome untouched, for the tests whose subject is the refusal or the replay.
struct Activator {
    use_case: MarkTileLayerDynamic,
}

impl Activator {
    async fn execute(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> TestResult<VectorTileRuntimeManifest> {
        expect_published(self.use_case.execute(command).await?)
    }

    async fn attempt(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<PublishedRuntimeManifest, CatalogError> {
        self.use_case.execute(command).await
    }
}

/// Asserts an outcome is a fresh publication and returns its manifest.
///
/// A replay returns a *valid* manifest, so most assertions in this file would compare equal against
/// one. Stating the expectation is what makes a regression that started replaying fail loudly.
fn expect_published(outcome: PublishedRuntimeManifest) -> TestResult<VectorTileRuntimeManifest> {
    match outcome {
        PublishedRuntimeManifest::Published(manifest) => Ok(manifest),
        PublishedRuntimeManifest::Replayed(manifest) => Err(format!(
            "expected a fresh publication; the ledger replayed manifest generation {}",
            manifest.manifest_generation.value()
        )
        .into()),
    }
}

fn use_case(pool: &PgPool, capability: RuntimeManifestPublicationCapability) -> Activator {
    Activator {
        use_case: MarkTileLayerDynamic::new(Arc::new(
            PgCatalogUnitOfWork::new(pool.clone()).with_runtime_manifest_publication(capability),
        )),
    }
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

/// A command whose idempotency key is derived from its content.
///
/// Content-derived keys are the pattern a caller with no key store would use, and they make every
/// test in this file that targets a distinct revision carry a distinct key without saying so. The
/// tests whose subject *is* the key use [`activation_command_with_key`].
fn activation_command(
    unit_key: &str,
    data_revision: Uuid,
    snapshot: &str,
    source_record_id: Uuid,
    expected: Option<(VectorTileReleaseId, ServingGeneration)>,
) -> TestResult<MarkTileLayerDynamicCommand> {
    activation_command_with_key(
        &format!("activate-{unit_key}-{data_revision}"),
        unit_key,
        data_revision,
        snapshot,
        source_record_id,
        expected,
    )
}

fn activation_command_with_key(
    idempotency_key: &str,
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
        // Derived from the data revision, not random. Both of these are inside the request
        // fingerprint, so a fresh id per call would make two calls that are meant to be the same
        // request compare as different ones — which is how this line was found: the true-retry test
        // was refused as a key reuse until the fixture stopped inventing values.
        postgis_projection_revision: PostgisProjectionRevisionId::new(derived_id(data_revision, 1)),
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
            source_file_asset_ids: vec![FileAssetId::new(derived_id(data_revision, 2))],
        },
        idempotency_key: idempotency_key.to_owned(),
        // A fixed operator: `operator_staff_id` is inside the request fingerprint, so a random one per
        // call would make every same-key comparison a mismatch and hide the replay path entirely.
        operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
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

async fn ledger_row_count(pool: &PgPool) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM catalog.catalog_mutation_idempotency")
            .fetch_one(pool)
            .await?,
    )
}

/// The outbox event id the ledger row records, if the capability let one be emitted.
async fn ledger_outbox_event_id(pool: &PgPool, key: &str) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar(
        "SELECT outbox_event_id FROM catalog.catalog_mutation_idempotency WHERE idempotency_key = $1",
    )
    .bind(key)
    .fetch_one(pool)
    .await?)
}

async fn outbox_types(pool: &PgPool) -> TestResult<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT type FROM catalog.outbox_event ORDER BY occurred_at, event_id")
            .fetch_all(pool)
            .await?,
    )
}
