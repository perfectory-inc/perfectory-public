//! Dynamic tile source activation use-case tests.
//!
//! These cover the decisions this layer owns: whether publication is permitted, whether the
//! command describes a state that can exist, and whether normalisation actually reaches the port.
//! Generation advancement and the state-machine transition are the transaction's decisions and are
//! covered by `catalog-infrastructure`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catalog_application::ports::{
    CatalogUnitOfWork, MarkTileLayerDynamicCommand, RuntimeManifestPublicationCapability,
    UpsertIndustrialComplexCommand,
};
use catalog_application::MarkTileLayerDynamic;
use catalog_domain::{
    ActiveTileSource, CanonicalIcebergSnapshotId, CatalogError, ComplexMutation,
    DynamicPostgisSource, FeatureIdProperty, IndustrialComplex, ManifestGeneration, Parcel,
    ParcelKind, PublicationUnit, RuntimeTileLayer, RuntimeTileLineage, RuntimeTilesUrlTemplate,
    ServingGeneration, VectorTileManifest, VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, ParcelId, PostgisProjectionRevisionId, SourceRecordId, StaffId,
    VectorTileDataRevisionId, VectorTileReleaseId, VectorTileRuntimeManifestId,
};
use uuid::Uuid;

/// Unit of work that records the activation it was handed and returns a manifest built from it.
///
/// Every method this test does not exercise returns an error rather than a default value, so a test
/// that reached one by mistake would fail instead of quietly passing.
#[derive(Default)]
struct RecordingUnitOfWork {
    activations: Mutex<Vec<MarkTileLayerDynamicCommand>>,
}

impl RecordingUnitOfWork {
    fn recorded(&self) -> Result<Vec<MarkTileLayerDynamicCommand>, CatalogError> {
        Ok(self.activations.lock().map_err(poisoned)?.clone())
    }
}

#[async_trait]
impl CatalogUnitOfWork for RecordingUnitOfWork {
    async fn create_complex(&self, _complex: &IndustrialComplex) -> Result<(), CatalogError> {
        Err(unused("create_complex"))
    }

    async fn upsert_complexes_by_official_code(
        &self,
        _commands: &[UpsertIndustrialComplexCommand],
    ) -> Result<Vec<IndustrialComplex>, CatalogError> {
        Err(unused("upsert_complexes_by_official_code"))
    }

    async fn update_complex(
        &self,
        _id: ComplexId,
        _expected_version: i64,
        _mutate: ComplexMutation,
    ) -> Result<IndustrialComplex, CatalogError> {
        Err(unused("update_complex"))
    }

    async fn archive_complex(
        &self,
        _id: ComplexId,
        _expected_version: i64,
        _operator_staff_id: StaffId,
        _reason: Option<String>,
        _request_id: Option<String>,
    ) -> Result<IndustrialComplex, CatalogError> {
        Err(unused("archive_complex"))
    }

    async fn update_parcel_kind(
        &self,
        _id: ParcelId,
        _expected_version: i64,
        _new_kind: ParcelKind,
    ) -> Result<Parcel, CatalogError> {
        Err(unused("update_parcel_kind"))
    }

    async fn rollback_vector_tile_manifest(
        &self,
        _command: catalog_application::ports::VectorTileManifestRollbackCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        Err(unused("rollback_vector_tile_manifest"))
    }

    async fn promote_vector_tile_manifest(
        &self,
        _command: catalog_application::ports::VectorTileManifestPromotionCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        Err(unused("promote_vector_tile_manifest"))
    }

    async fn mark_tile_layer_dynamic(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.activations
            .lock()
            .map_err(poisoned)?
            .push(command.clone());
        published_manifest(&command)
    }
}

/// Unit of work that implements only the required methods, inheriting the activation default.
struct SilentUnitOfWork;

#[async_trait]
impl CatalogUnitOfWork for SilentUnitOfWork {
    async fn create_complex(&self, _complex: &IndustrialComplex) -> Result<(), CatalogError> {
        Err(unused("create_complex"))
    }

    async fn upsert_complexes_by_official_code(
        &self,
        _commands: &[UpsertIndustrialComplexCommand],
    ) -> Result<Vec<IndustrialComplex>, CatalogError> {
        Err(unused("upsert_complexes_by_official_code"))
    }

    async fn update_complex(
        &self,
        _id: ComplexId,
        _expected_version: i64,
        _mutate: ComplexMutation,
    ) -> Result<IndustrialComplex, CatalogError> {
        Err(unused("update_complex"))
    }

    async fn archive_complex(
        &self,
        _id: ComplexId,
        _expected_version: i64,
        _operator_staff_id: StaffId,
        _reason: Option<String>,
        _request_id: Option<String>,
    ) -> Result<IndustrialComplex, CatalogError> {
        Err(unused("archive_complex"))
    }

    async fn update_parcel_kind(
        &self,
        _id: ParcelId,
        _expected_version: i64,
        _new_kind: ParcelKind,
    ) -> Result<Parcel, CatalogError> {
        Err(unused("update_parcel_kind"))
    }

    async fn rollback_vector_tile_manifest(
        &self,
        _command: catalog_application::ports::VectorTileManifestRollbackCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        Err(unused("rollback_vector_tile_manifest"))
    }

    async fn promote_vector_tile_manifest(
        &self,
        _command: catalog_application::ports::VectorTileManifestPromotionCommand,
    ) -> Result<VectorTileManifest, CatalogError> {
        Err(unused("promote_vector_tile_manifest"))
    }
}

fn unused(method: &str) -> CatalogError {
    CatalogError::Infrastructure(format!("{method} is not exercised by this test"))
}

fn poisoned<T>(_error: T) -> CatalogError {
    CatalogError::Infrastructure("activation mutex poisoned".to_owned())
}

/// Builds the manifest a real transaction would publish for a first activation.
///
/// It goes through the domain validator, so a command the use case let through that could not
/// produce a valid manifest fails here rather than being asserted away.
fn published_manifest(
    command: &MarkTileLayerDynamicCommand,
) -> Result<VectorTileRuntimeManifest, CatalogError> {
    let invalid = CatalogError::InvalidVectorTileRuntimeManifest;
    let unit = PublicationUnit {
        data_revision: command.data_revision,
        serving_generation: ServingGeneration::new(1).map_err(invalid)?,
        active_release_id: VectorTileReleaseId::new(Uuid::now_v7()),
        canonical_iceberg_snapshot_id: command.canonical_iceberg_snapshot_id.clone(),
        source: ActiveTileSource::DynamicPostgis(DynamicPostgisSource {
            martin_source_id: command.martin_source_id.clone(),
            tiles_url_template: command.tiles_url_template.clone(),
            postgis_projection_revision: command.postgis_projection_revision,
            cache_policy: "no_store".to_owned(),
        }),
        layers: command.layers.clone(),
        lineage: command.lineage.clone(),
    };
    let manifest = VectorTileRuntimeManifest {
        schema_version: 2,
        current_version: VectorTileRuntimeManifestId::new(Uuid::now_v7()),
        manifest_generation: ManifestGeneration::new(1).map_err(invalid)?,
        refresh_after_seconds: 4,
        published_at: chrono::Utc::now(),
        publication_units: BTreeMap::from([(command.unit_key.clone(), unit)]),
    };
    manifest.validate().map_err(invalid)?;
    Ok(manifest)
}

fn layer() -> Result<RuntimeTileLayer, CatalogError> {
    Ok(RuntimeTileLayer {
        source_layer: " parcels ".to_owned(),
        feature_id_property: FeatureIdProperty::new("pnu".to_owned())
            .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
        tile_min_zoom: 8,
        tile_max_zoom: 16,
        render_min_zoom: 10,
        render_max_zoom: 22,
        feature_filter_properties: BTreeMap::from([("pnu".to_owned(), "pnu".to_owned())]),
    })
}

/// A first activation with padded text, so trimming is observable at the port.
fn first_activation() -> Result<MarkTileLayerDynamicCommand, CatalogError> {
    Ok(MarkTileLayerDynamicCommand {
        unit_key: " parcels ".to_owned(),
        expected_active_release_id: None,
        expected_serving_generation: None,
        data_revision: VectorTileDataRevisionId::new(Uuid::now_v7()),
        canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(
            "70000000000000001".to_owned(),
        )
        .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
        postgis_projection_revision: PostgisProjectionRevisionId::new(Uuid::now_v7()),
        martin_source_id: " parcels ".to_owned(),
        tiles_url_template: RuntimeTilesUrlTemplate::new(
            "https://tiles.example.com/parcels/{z}/{x}/{y}".to_owned(),
        )
        .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
        layers: BTreeMap::from([(" parcels ".to_owned(), layer()?)]),
        lineage: RuntimeTileLineage {
            source_record_id: SourceRecordId::new(Uuid::now_v7()),
            source_file_asset_ids: vec![FileAssetId::new(Uuid::now_v7())],
        },
        idempotency_key: " activation-1 ".to_owned(),
        operator_staff_id: StaffId::new(Uuid::now_v7()),
    })
}

fn enabled_use_case(uow: Arc<RecordingUnitOfWork>) -> MarkTileLayerDynamic {
    MarkTileLayerDynamic::new(uow, RuntimeManifestPublicationCapability::enabled())
}

async fn refusal_message(command: MarkTileLayerDynamicCommand) -> Result<String, CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let error = enabled_use_case(uow.clone()).execute(command).await.err();
    assert!(
        uow.recorded()?.is_empty(),
        "a refused activation must not reach the unit of work"
    );
    Ok(error.map(|error| error.to_string()).unwrap_or_default())
}

#[tokio::test]
async fn activation_is_normalized_before_it_reaches_the_unit_of_work() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let manifest = enabled_use_case(uow.clone())
        .execute(first_activation()?)
        .await?;

    let recorded = uow.recorded()?;
    assert_eq!(recorded.len(), 1);
    let command = recorded.first().ok_or_else(|| unused("recorded command"))?;
    assert_eq!(command.unit_key, "parcels");
    assert_eq!(command.martin_source_id, "parcels");
    assert_eq!(command.idempotency_key, "activation-1");
    assert_eq!(
        command.layers.keys().collect::<Vec<_>>(),
        vec!["parcels"],
        "layer keys must be trimmed, not carried through padded"
    );
    assert!(manifest.publication_units.contains_key("parcels"));
    Ok(())
}

#[tokio::test]
async fn a_disabled_deployment_refuses_before_touching_the_unit_of_work() -> Result<(), CatalogError>
{
    let uow = Arc::new(RecordingUnitOfWork::default());
    let use_case = MarkTileLayerDynamic::new(
        uow.clone(),
        RuntimeManifestPublicationCapability::disabled(),
    );

    let message = use_case
        .execute(first_activation()?)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

    assert!(
        message.contains("publication is disabled"),
        "got: {message}"
    );
    assert!(uow.recorded()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn half_stated_expectations_are_refused_rather_than_guessed() -> Result<(), CatalogError> {
    let mut command = first_activation()?;
    command.expected_active_release_id = Some(VectorTileReleaseId::new(Uuid::now_v7()));

    let message = refusal_message(command).await?;
    assert!(
        message.contains("must both be present or both be absent"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn a_unit_serving_no_layer_is_not_a_publication() -> Result<(), CatalogError> {
    let mut command = first_activation()?;
    command.layers = BTreeMap::new();

    let message = refusal_message(command).await?;
    assert!(
        message.contains("layers must not be empty"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn a_template_addressing_another_martin_source_is_refused() -> Result<(), CatalogError> {
    let mut command = first_activation()?;
    command.tiles_url_template = RuntimeTilesUrlTemplate::new(
        "https://tiles.example.com/not-parcels/{z}/{x}/{y}".to_owned(),
    )
    .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?;

    let message = refusal_message(command).await?;
    assert!(
        message.contains("must address martin_source_id"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn layer_names_that_collide_only_after_trimming_are_refused() -> Result<(), CatalogError> {
    let mut command = first_activation()?;
    command.layers = BTreeMap::from([
        ("parcels".to_owned(), layer()?),
        (" parcels".to_owned(), layer()?),
    ]);

    let message = refusal_message(command).await?;
    assert!(
        message.contains("layer names must be unique after trimming"),
        "got: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn a_unit_of_work_that_ignores_activation_cannot_report_a_publication(
) -> Result<(), CatalogError> {
    let use_case = MarkTileLayerDynamic::new(
        Arc::new(SilentUnitOfWork),
        RuntimeManifestPublicationCapability::enabled(),
    );

    let message = use_case
        .execute(first_activation()?)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();

    assert!(
        message.contains("not implemented by this Catalog unit of work"),
        "got: {message}"
    );
    Ok(())
}
