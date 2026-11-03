//! A unit born from its first lakehouse bake (root ADR-0374): the unit, its first static release
//! and a complete manifest arrive in one transaction, and its next release is an ordinary bake.

use super::readdress::seed_static_release;
use super::*;
use catalog_application::ports::{RegisterLakehouseTileUnitCommand, StartLakehouseBakeCommand};

/// The Silver snapshot the first zoning archive was baked from (synthetic).
const ZONING_SNAPSHOT: &str = "841361364657368701";

fn birth_command(key: &str, release_id: Uuid) -> TestResult<RegisterLakehouseTileUnitCommand> {
    let release_id = VectorTileReleaseId::new(release_id);
    let source_id = static_release_martin_source_id("zoning", release_id);
    Ok(RegisterLakehouseTileUnitCommand {
        unit_key: "zoning".to_owned(),
        layer_id: "zoning".to_owned(),
        layer: RuntimeTileLayer {
            source_layer: "zoning".to_owned(),
            feature_id_property: FeatureIdProperty::new("zone_id".to_owned())?,
            tile_min_zoom: 10,
            tile_max_zoom: 14,
            render_min_zoom: 10,
            render_max_zoom: 22,
            feature_filter_properties: BTreeMap::from([(
                "layer_code".to_owned(),
                "layer_code".to_owned(),
            )]),
        },
        canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(ZONING_SNAPSHOT.to_owned())?,
        source_record_source: "lakehouse:silver.land_use_zone_area".to_owned(),
        artifact: ValidatedPmtilesArtifact {
            release_id,
            file_asset_id: FileAssetId::new(Uuid::now_v7()),
            object_key: static_release_pmtiles_object_key("zoning", release_id),
            tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
                "https://tiles.example.test/{source_id}/{{z}}/{{x}}/{{y}}"
            ))?,
            checksum: PmtilesChecksum::new("e".repeat(64))?,
            size_bytes: 4096,
        },
        evidence: BuildEvidenceDigest::new("f".repeat(64))?,
        idempotency_key: key.to_owned(),
        operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
    })
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_unit_is_born_static_beside_the_units_it_carries_and_then_bakes_like_any_other(
) -> TestResult {
    run_in_disposable_database("tile_unit_birth", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let (_dynamic, before) = seed_static_release(&pool).await?;
        let complex_before = published_unit(&before, "complex")?.active_release_id;
        let uow = PgCatalogUnitOfWork::new(pool.clone())
            .with_runtime_manifest_publication(RuntimeManifestPublicationCapability::enabled());

        let command = birth_command("zoning-birth-1", Uuid::now_v7())?;
        let born = uow.register_lakehouse_tile_unit(command.clone()).await?;
        assert!(born.manifest_generation.value() > before.manifest_generation.value());
        let zoning = published_unit(&born, "zoning")?;
        assert_eq!(zoning.active_release_id, command.artifact.release_id);
        assert_eq!(zoning.serving_generation.value(), 1);
        assert_eq!(
            published_unit(&born, "complex")?.active_release_id,
            complex_before,
            "the other units are carried unchanged"
        );
        let born_from: Option<String> = sqlx::query_scalar(
            "SELECT born_from FROM catalog.vector_tile_publication_unit WHERE unit_key = 'zoning'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(born_from.as_deref(), Some("lakehouse_bake"));
        assert!(outbox_types(&pool)
            .await?
            .iter()
            .any(|kind| kind == RUNTIME_MANIFEST_PUBLISHED_V2));

        // A retry with the same key answers with the same manifest and writes nothing.
        let replay = uow.register_lakehouse_tile_unit(command.clone()).await?;
        assert_eq!(replay.current_version, born.current_version);
        // A second birth of the same unit is refused: its next release is a bake.
        let again = uow
            .register_lakehouse_tile_unit(birth_command("zoning-birth-2", Uuid::now_v7())?)
            .await;
        assert!(
            matches!(&again, Err(CatalogError::InvalidVectorTileRuntimeManifest(message)) if message.contains("already exists")),
            "{again:?}"
        );

        // The next month's bake starts from the born release like any lakehouse bake.
        let lifecycle = VectorTileBuildLifecycle::new(Arc::new(PgCatalogUnitOfWork::new(pool.clone())));
        lifecycle
            .start_lakehouse_bake(StartLakehouseBakeCommand {
                unit_key: "zoning".to_owned(),
                input_release_id: zoning.active_release_id,
                canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(
                    "841361364657368702".to_owned(),
                )?,
                idempotency_key: "zoning-bake-2".to_owned(),
                operator_staff_id: StaffId::new(OPERATOR_STAFF_ID),
                silver_source: None,
            })
            .await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn born_from_admits_only_a_lakehouse_bake() -> TestResult {
    run_in_disposable_database("tile_unit_birth_gate", |pool| async move {
        MIGRATOR.run(&pool).await?;
        seed_static_release(&pool).await?;
        // Every other unit keeps its dynamic first publication: the column names one other path.
        let refused = sqlx::query(
            "UPDATE catalog.vector_tile_publication_unit SET born_from = 'postgis' WHERE unit_key = 'complex'",
        )
        .execute(&pool)
        .await;
        assert!(refused.is_err(), "born_from admits only lakehouse_bake");
        Ok(())
    })
    .await
}
