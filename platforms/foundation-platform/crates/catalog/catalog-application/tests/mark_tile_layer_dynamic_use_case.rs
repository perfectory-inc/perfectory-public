//! Dynamic tile source activation use-case tests.
//!
//! These cover the decisions this layer owns: whether publication is permitted, whether the
//! command describes a state that can exist, and whether normalisation actually reaches the port.
//! Generation advancement and the state-machine transition are the transaction's decisions and are
//! covered by `catalog-infrastructure`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use catalog_application::ports::MarkTileLayerDynamicCommand;
use catalog_application::MarkTileLayerDynamic;
use catalog_domain::{
    CanonicalIcebergSnapshotId, CatalogError, FeatureIdProperty, RuntimeTileLayer,
    RuntimeTileLineage, RuntimeTilesUrlTemplate,
};
use foundation_shared_kernel::ids::{
    FileAssetId, PostgisProjectionRevisionId, SourceRecordId, StaffId, VectorTileDataRevisionId,
    VectorTileReleaseId,
};
use uuid::Uuid;

use common::{unused, RecordingUnitOfWork, SilentUnitOfWork};

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

fn use_case(uow: Arc<RecordingUnitOfWork>) -> MarkTileLayerDynamic {
    MarkTileLayerDynamic::new(uow)
}

async fn refusal_message(command: MarkTileLayerDynamicCommand) -> Result<String, CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let error = use_case(uow.clone()).execute(command).await.err();
    assert!(
        uow.activations()?.is_empty(),
        "a refused activation must not reach the unit of work"
    );
    Ok(error.map(|error| error.to_string()).unwrap_or_default())
}

#[tokio::test]
async fn activation_is_normalized_before_it_reaches_the_unit_of_work() -> Result<(), CatalogError> {
    let uow = Arc::new(RecordingUnitOfWork::default());
    let manifest = use_case(uow.clone()).execute(first_activation()?).await?;

    let recorded = uow.activations()?;
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
    let use_case = MarkTileLayerDynamic::new(Arc::new(SilentUnitOfWork));

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
