//! `PostgreSQL` round-trip tests for Lakehouse Registry metadata.

use foundation_shared_kernel::ids::LakehouseStorageNamespaceId;
use lakehouse_application::ports::{LakehouseRegistryRepository, LakehouseRegistryUnitOfWork};
use lakehouse_application::RegisterLakehouseObjectArtifactCommand;
use lakehouse_domain::{
    LakehouseArtifactFormat, LakehouseAssetKind, LakehouseCatalogProvider,
    LakehouseDatasetVersionState, LakehouseEnvironment, LakehouseError, LakehouseNamespaceStatus,
    LakehouseOwnerService, LakehouseRegistryLayer, LakehouseStorageNamespace,
    LakehouseStorageProvider,
};
use lakehouse_infrastructure::{PgLakehouseRegistryRepository, PgLakehouseRegistryUnitOfWork};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const TEST_ADVISORY_LOCK_KEY: i64 = 0x6c72_6567_6973_7479;

async fn pool() -> Result<Option<PgPool>, sqlx::Error> {
    let Some(url) = std::env::var("DATABASE_URL").ok() else {
        return Ok(None);
    };

    PgPool::connect(&url).await.map(Some)
}

async fn lock_registry_tests(pool: &PgPool) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(TEST_ADVISORY_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn registers_namespace_asset_and_active_version() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    let _lock_tx = lock_registry_tests(&pool).await?;
    let repository = PgLakehouseRegistryRepository::new(pool.clone());
    let unit_of_work = PgLakehouseRegistryUnitOfWork::new(pool.clone());
    let suffix = Uuid::now_v7().simple().to_string();
    let bucket_name = format!("gongzzang-lakehouse-test-{}", &suffix[..12]);
    let qualified_name = format!("gongzzang.gold.registry_smoke_{}", &suffix[..12]);

    let namespace = LakehouseStorageNamespace::new(
        LakehouseStorageNamespaceId::new(Uuid::now_v7()),
        LakehouseStorageProvider::R2,
        LakehouseEnvironment::Staging,
        LakehouseOwnerService::Gongzzang,
        bucket_name.clone(),
        None,
        LakehouseCatalogProvider::None,
        LakehouseNamespaceStatus::Active,
    )?;
    repository.upsert_storage_namespace(&namespace).await?;

    let version_name = format!("v-{}", &suffix[..12]);
    let object_key = format!("gold/registry-smoke-{}/manifest.json", &suffix[..12]);
    let receipt = unit_of_work
        .register_object_artifact(RegisterLakehouseObjectArtifactCommand {
            qualified_name: qualified_name.clone(),
            owner_service: LakehouseOwnerService::Gongzzang,
            environment: LakehouseEnvironment::Staging,
            layer: LakehouseRegistryLayer::Gold,
            asset_kind: LakehouseAssetKind::Manifest,
            schema_contract_ref: "docs/contracts/registry-smoke.v1.json".to_owned(),
            dataset_version: version_name.clone(),
            schema_version: "registry-smoke.v1".to_owned(),
            artifact_format: LakehouseArtifactFormat::Json,
            created_by_ingestion_run_id: None,
            object_key: object_key.clone(),
            content_type: "application/json".to_owned(),
            checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
            size_bytes: 128,
            logical_record_count: Some(1),
        })
        .await?;
    assert_eq!(receipt.object_key, object_key);

    let active = repository
        .find_active_dataset_version(&qualified_name)
        .await?
        .ok_or_else(|| LakehouseError::Persistence("active version not found".to_owned()))?;
    let listed_artifacts = repository.list_object_artifacts(active.id).await?;
    assert_eq!(listed_artifacts.len(), 1);
    assert_eq!(listed_artifacts[0].object_key.as_str(), object_key);
    assert_eq!(listed_artifacts[0].logical_record_count, Some(1));

    let loaded_namespace = repository
        .find_storage_namespace(
            LakehouseOwnerService::Gongzzang,
            LakehouseEnvironment::Staging,
        )
        .await?
        .ok_or_else(|| LakehouseError::Persistence("namespace not found".to_owned()))?;
    assert_eq!(loaded_namespace.bucket_name, bucket_name);

    assert_eq!(active.version, version_name);
    assert_eq!(active.state, LakehouseDatasetVersionState::Active);

    sqlx::query("DELETE FROM catalog.lakehouse_data_asset WHERE qualified_name = $1")
        .bind(&qualified_name)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM catalog.lakehouse_storage_namespace WHERE bucket_name = $1")
        .bind(&bucket_name)
        .execute(&pool)
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn records_gongzzang_media_object_set_artifact() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    let _lock_tx = lock_registry_tests(&pool).await?;
    let repository = PgLakehouseRegistryRepository::new(pool.clone());
    let unit_of_work = PgLakehouseRegistryUnitOfWork::new(pool.clone());
    let suffix = Uuid::now_v7().simple().to_string();
    let bucket_name = format!("gongzzang-lakehouse-media-test-{}", &suffix[..12]);
    let qualified_name = format!("gongzzang.gold.listing_photo_media_{}", &suffix[..12]);

    let namespace = LakehouseStorageNamespace::new(
        LakehouseStorageNamespaceId::new(Uuid::now_v7()),
        LakehouseStorageProvider::R2,
        LakehouseEnvironment::Staging,
        LakehouseOwnerService::Gongzzang,
        bucket_name.clone(),
        None,
        LakehouseCatalogProvider::None,
        LakehouseNamespaceStatus::Active,
    )?;
    repository.upsert_storage_namespace(&namespace).await?;

    let version_name = format!("media-v-{}", &suffix[..12]);
    let object_key = format!(
        "media/listing-photo/listings/lst_{}/photos/lph_{}.webp",
        &suffix[..8],
        &suffix[8..12]
    );
    let receipt = unit_of_work
        .register_object_artifact(RegisterLakehouseObjectArtifactCommand {
            qualified_name: qualified_name.clone(),
            owner_service: LakehouseOwnerService::Gongzzang,
            environment: LakehouseEnvironment::Staging,
            layer: LakehouseRegistryLayer::Gold,
            asset_kind: LakehouseAssetKind::MediaSet,
            schema_contract_ref: "gongzzang.listing_photo_media.v1".to_owned(),
            dataset_version: version_name.clone(),
            schema_version: "gongzzang.listing_photo_media.v1".to_owned(),
            artifact_format: LakehouseArtifactFormat::ObjectSet,
            created_by_ingestion_run_id: None,
            object_key: object_key.clone(),
            content_type: "image/webp".to_owned(),
            checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
            size_bytes: 2048,
            logical_record_count: None,
        })
        .await?;
    assert_eq!(receipt.object_key, object_key);

    let active = repository
        .find_active_dataset_version(&qualified_name)
        .await?
        .ok_or_else(|| LakehouseError::Persistence("active version not found".to_owned()))?;
    assert_eq!(active.version, version_name);
    let listed_artifacts = repository.list_object_artifacts(active.id).await?;
    assert_eq!(listed_artifacts.len(), 1);
    assert_eq!(listed_artifacts[0].object_key.as_str(), object_key);
    assert_eq!(listed_artifacts[0].content_type, "image/webp");
    assert_eq!(listed_artifacts[0].logical_record_count, None);

    sqlx::query("DELETE FROM catalog.lakehouse_data_asset WHERE qualified_name = $1")
        .bind(&qualified_name)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM catalog.lakehouse_storage_namespace WHERE bucket_name = $1")
        .bind(&bucket_name)
        .execute(&pool)
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn artifact_retry_is_idempotent_and_checksum_conflict_fails_loud(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    let _lock_tx = lock_registry_tests(&pool).await?;
    let repository = PgLakehouseRegistryRepository::new(pool.clone());
    let unit_of_work = PgLakehouseRegistryUnitOfWork::new(pool.clone());
    let suffix = Uuid::now_v7().simple().to_string();
    let bucket_name = format!("gongzzang-lakehouse-retry-test-{}", &suffix[..12]);
    let qualified_name = format!("gongzzang.gold.registry_retry_{}", &suffix[..12]);

    let namespace = LakehouseStorageNamespace::new(
        LakehouseStorageNamespaceId::new(Uuid::now_v7()),
        LakehouseStorageProvider::R2,
        LakehouseEnvironment::Staging,
        LakehouseOwnerService::Gongzzang,
        bucket_name.clone(),
        None,
        LakehouseCatalogProvider::None,
        LakehouseNamespaceStatus::Active,
    )?;
    repository.upsert_storage_namespace(&namespace).await?;

    let command = RegisterLakehouseObjectArtifactCommand {
        qualified_name: qualified_name.clone(),
        owner_service: LakehouseOwnerService::Gongzzang,
        environment: LakehouseEnvironment::Staging,
        layer: LakehouseRegistryLayer::Gold,
        asset_kind: LakehouseAssetKind::Manifest,
        schema_contract_ref: "docs/contracts/registry-retry.v1.json".to_owned(),
        dataset_version: format!("retry-v-{}", &suffix[..12]),
        schema_version: "registry-retry.v1".to_owned(),
        artifact_format: LakehouseArtifactFormat::Json,
        created_by_ingestion_run_id: None,
        object_key: format!("gold/registry-retry-{}/manifest.json", &suffix[..12]),
        content_type: "application/json".to_owned(),
        checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_owned(),
        size_bytes: 128,
        logical_record_count: Some(1),
    };

    let first = unit_of_work
        .register_object_artifact(command.clone())
        .await?;
    let retry = unit_of_work
        .register_object_artifact(command.clone())
        .await?;
    assert_eq!(retry.artifact_id, first.artifact_id);

    let mut conflicting = command;
    conflicting.checksum_sha256 =
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned();
    let Err(error) = unit_of_work.register_object_artifact(conflicting).await else {
        return Err(
            LakehouseError::Persistence("conflicting checksum was accepted".to_owned()).into(),
        );
    };
    assert!(matches!(
        error,
        LakehouseError::InvalidLakehouseRegistryInput(_)
    ));

    let active = repository
        .find_active_dataset_version(&qualified_name)
        .await?
        .ok_or_else(|| LakehouseError::Persistence("active version not found".to_owned()))?;
    let artifacts = repository.list_object_artifacts(active.id).await?;
    assert_eq!(artifacts.len(), 1);
    assert_eq!(
        artifacts[0].checksum_sha256,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );

    sqlx::query("DELETE FROM catalog.lakehouse_data_asset WHERE qualified_name = $1")
        .bind(&qualified_name)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM catalog.lakehouse_storage_namespace WHERE bucket_name = $1")
        .bind(&bucket_name)
        .execute(&pool)
        .await?;

    Ok(())
}
