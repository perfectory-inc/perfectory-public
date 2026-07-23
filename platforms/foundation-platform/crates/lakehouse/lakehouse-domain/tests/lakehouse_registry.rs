//! Lakehouse Registry domain contract tests.

use foundation_shared_kernel::ids::{
    LakehouseDataAssetId, LakehouseDatasetVersionId, LakehouseObjectArtifactId,
    LakehouseStorageNamespaceId,
};
use lakehouse_domain::{
    LakehouseAssetKind, LakehouseCatalogProvider, LakehouseDataAsset, LakehouseEnvironment,
    LakehouseError, LakehouseNamespaceStatus, LakehouseObjectArtifact, LakehouseOwnerService,
    LakehouseRegistryLayer, LakehouseStorageNamespace, LakehouseStorageProvider,
};
use uuid::Uuid;

fn namespace(
    owner_service: LakehouseOwnerService,
) -> Result<LakehouseStorageNamespace, LakehouseError> {
    LakehouseStorageNamespace::new(
        LakehouseStorageNamespaceId::new(Uuid::now_v7()),
        LakehouseStorageProvider::R2,
        LakehouseEnvironment::Production,
        owner_service,
        owner_service.production_r2_bucket_name().to_owned(),
        None,
        LakehouseCatalogProvider::R2DataCatalog,
        LakehouseNamespaceStatus::Active,
    )
}

#[test]
fn namespace_rejects_bucket_that_belongs_to_another_service() -> Result<(), &'static str> {
    let Err(error) = LakehouseStorageNamespace::new(
        LakehouseStorageNamespaceId::new(Uuid::now_v7()),
        LakehouseStorageProvider::R2,
        LakehouseEnvironment::Production,
        LakehouseOwnerService::FoundationPlatform,
        "gongzzang-lakehouse-prod".to_owned(),
        None,
        LakehouseCatalogProvider::R2DataCatalog,
        LakehouseNamespaceStatus::Active,
    ) else {
        return Err("foundation-platform must not own the Gongzzang bucket");
    };

    assert!(matches!(
        error,
        LakehouseError::InvalidLakehouseRegistryInput(_)
    ));
    Ok(())
}

#[test]
fn data_asset_qualified_name_must_match_owner_and_layer() -> Result<(), &'static str> {
    let Err(error) = LakehouseDataAsset::new(
        LakehouseDataAssetId::new(Uuid::now_v7()),
        "foundation_platform.gold.listing_marker_tiles".to_owned(),
        LakehouseOwnerService::Gongzzang,
        LakehouseRegistryLayer::Gold,
        LakehouseAssetKind::PbfTileSet,
        Some("docs/contracts/listing-marker-tiles.v1.json".to_owned()),
    ) else {
        return Err("Gongzzang-owned assets must use the gongzzang qualified-name prefix");
    };

    assert!(matches!(
        error,
        LakehouseError::InvalidLakehouseRegistryInput(_)
    ));
    Ok(())
}

#[test]
fn namespace_accepts_only_layer_scoped_object_keys() -> Result<(), LakehouseError> {
    let namespace = namespace(LakehouseOwnerService::FoundationPlatform)?;

    let accepted = namespace.allows_object_key_for_layer(
        LakehouseRegistryLayer::Bronze,
        "bronze/source=vworld/page=1.json",
    )?;
    assert!(accepted);

    let rejected = namespace.allows_object_key_for_layer(
        LakehouseRegistryLayer::Gold,
        "bronze/source=vworld/page=1.json",
    )?;
    assert!(!rejected);

    Ok(())
}

#[test]
fn object_artifact_must_stay_under_the_asset_layer_prefix() -> Result<(), LakehouseError> {
    let namespace = namespace(LakehouseOwnerService::FoundationPlatform)?;
    let dataset_version_id = LakehouseDatasetVersionId::new(Uuid::now_v7());

    let Err(error) = LakehouseObjectArtifact::new(
        LakehouseObjectArtifactId::new(Uuid::now_v7()),
        &namespace,
        LakehouseRegistryLayer::Bronze,
        dataset_version_id,
        "gold/source=vworld/part-000001.json",
        "application/json".to_owned(),
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
        128,
        Some(1),
    ) else {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "Bronze object artifacts must not point at Gold object keys".to_owned(),
        ));
    };

    assert!(matches!(
        error,
        LakehouseError::InvalidLakehouseRegistryInput(_)
    ));

    let artifact = LakehouseObjectArtifact::new(
        LakehouseObjectArtifactId::new(Uuid::now_v7()),
        &namespace,
        LakehouseRegistryLayer::Bronze,
        dataset_version_id,
        "bronze/source=vworld/part-000001.json",
        "application/json".to_owned(),
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
        128,
        Some(1),
    )?;
    assert_eq!(artifact.namespace_id, namespace.id);
    assert_eq!(
        artifact.object_key.as_str(),
        "bronze/source=vworld/part-000001.json"
    );

    Ok(())
}

#[test]
fn media_set_artifact_accepts_media_prefix_for_gold_asset() -> Result<(), LakehouseError> {
    let namespace = namespace(LakehouseOwnerService::Gongzzang)?;
    let asset = LakehouseDataAsset::new(
        LakehouseDataAssetId::new(Uuid::now_v7()),
        "gongzzang.gold.listing_photo_media".to_owned(),
        LakehouseOwnerService::Gongzzang,
        LakehouseRegistryLayer::Gold,
        LakehouseAssetKind::MediaSet,
        Some("gongzzang.listing_photo_media.v1".to_owned()),
    )?;

    let artifact = LakehouseObjectArtifact::new_for_asset(
        LakehouseObjectArtifactId::new(Uuid::now_v7()),
        &namespace,
        &asset,
        LakehouseDatasetVersionId::new(Uuid::now_v7()),
        "media/listing-photo/listings/lst_1/photos/lph_1.webp",
        "image/webp".to_owned(),
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
        2048,
        None,
    )?;

    assert_eq!(
        artifact.object_key.as_str(),
        "media/listing-photo/listings/lst_1/photos/lph_1.webp"
    );
    Ok(())
}

#[test]
fn non_media_gold_asset_rejects_media_prefix() -> Result<(), LakehouseError> {
    let namespace = namespace(LakehouseOwnerService::Gongzzang)?;
    let asset = LakehouseDataAsset::new(
        LakehouseDataAssetId::new(Uuid::now_v7()),
        "gongzzang.gold.listing_marker_tiles".to_owned(),
        LakehouseOwnerService::Gongzzang,
        LakehouseRegistryLayer::Gold,
        LakehouseAssetKind::PbfTileSet,
        Some("gongzzang.listing_marker_tiles.v1".to_owned()),
    )?;

    let Err(error) = LakehouseObjectArtifact::new_for_asset(
        LakehouseObjectArtifactId::new(Uuid::now_v7()),
        &namespace,
        &asset,
        LakehouseDatasetVersionId::new(Uuid::now_v7()),
        "media/listing-photo/listings/lst_1/photos/lph_1.webp",
        "image/webp".to_owned(),
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
        2048,
        None,
    ) else {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "non-media Gold assets must stay under the Gold prefix".to_owned(),
        ));
    };

    assert!(matches!(
        error,
        LakehouseError::InvalidLakehouseRegistryInput(_)
    ));
    Ok(())
}
