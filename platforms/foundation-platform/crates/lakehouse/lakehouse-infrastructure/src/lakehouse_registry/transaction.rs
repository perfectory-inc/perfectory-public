//! Atomic Lakehouse Registry artifact transaction.

use foundation_shared_kernel::ids::{
    LakehouseDataAssetId, LakehouseDatasetVersionId, LakehouseObjectArtifactId,
};
use lakehouse_application::{
    RegisterLakehouseObjectArtifactCommand, RegisterLakehouseObjectArtifactReceipt,
};
use lakehouse_domain::{
    LakehouseDataAsset, LakehouseDatasetVersion, LakehouseDatasetVersionState,
    LakehouseEnvironment, LakehouseError, LakehouseNamespaceStatus, LakehouseObjectArtifact,
    LakehouseOwnerService, LakehouseStorageNamespace,
};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::row_mapping::{
    row_to_asset, row_to_dataset_version, row_to_namespace, row_to_object_artifact, u64_to_i64,
    DATASET_VERSION_COLUMNS, DATA_ASSET_COLUMNS, NAMESPACE_COLUMNS, OBJECT_ARTIFACT_COLUMNS,
};
use crate::postgres_error::map_sqlx;

pub(super) async fn register_object_artifact(
    pool: &PgPool,
    command: RegisterLakehouseObjectArtifactCommand,
) -> Result<RegisterLakehouseObjectArtifactReceipt, LakehouseError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let namespace = find_storage_namespace_tx(&mut tx, command.owner_service, command.environment)
        .await?
        .ok_or_else(|| {
            LakehouseError::InvalidLakehouseRegistryInput(format!(
                "{} {} lakehouse namespace is not registered",
                command.owner_service.wire_name(),
                command.environment.wire_name()
            ))
        })?;
    if namespace.status != LakehouseNamespaceStatus::Active {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "{} {} lakehouse namespace is not active",
            command.owner_service.wire_name(),
            command.environment.wire_name()
        )));
    }

    let asset = LakehouseDataAsset::new(
        LakehouseDataAssetId::new(Uuid::now_v7()),
        command.qualified_name,
        command.owner_service,
        command.layer,
        command.asset_kind,
        Some(command.schema_contract_ref),
    )?;
    let asset = upsert_data_asset_tx(&mut tx, &asset).await?;
    let version = LakehouseDatasetVersion::new(
        LakehouseDatasetVersionId::new(Uuid::now_v7()),
        asset.id,
        command.dataset_version,
        LakehouseDatasetVersionState::Active,
        command.schema_version,
        command.artifact_format,
        command.created_by_ingestion_run_id,
    )?;
    let version = record_dataset_version_tx(&mut tx, &version).await?;
    let artifact = LakehouseObjectArtifact::new_for_asset(
        LakehouseObjectArtifactId::new(Uuid::now_v7()),
        &namespace,
        &asset,
        version.id,
        &command.object_key,
        command.content_type,
        command.checksum_sha256,
        command.size_bytes,
        command.logical_record_count,
    )?;
    let artifact = record_object_artifact_tx(&mut tx, &artifact).await?;
    tx.commit().await.map_err(map_sqlx)?;

    Ok(RegisterLakehouseObjectArtifactReceipt {
        artifact_id: artifact.id.to_string(),
        qualified_name: asset.qualified_name,
        object_key: artifact.object_key.as_str().to_owned(),
    })
}

async fn find_storage_namespace_tx(
    connection: &mut PgConnection,
    owner_service: LakehouseOwnerService,
    environment: LakehouseEnvironment,
) -> Result<Option<LakehouseStorageNamespace>, LakehouseError> {
    let query = format!(
        "SELECT {NAMESPACE_COLUMNS}
         FROM catalog.lakehouse_storage_namespace
         WHERE owner_service = $1
           AND environment = $2
         FOR SHARE"
    );
    let row = sqlx::query(&query)
        .bind(owner_service.wire_name())
        .bind(environment.wire_name())
        .fetch_optional(connection)
        .await
        .map_err(map_sqlx)?;
    row.as_ref().map(row_to_namespace).transpose()
}

async fn upsert_data_asset_tx(
    connection: &mut PgConnection,
    asset: &LakehouseDataAsset,
) -> Result<LakehouseDataAsset, LakehouseError> {
    let query = format!(
        "INSERT INTO catalog.lakehouse_data_asset
         ({DATA_ASSET_COLUMNS})
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (qualified_name) DO UPDATE
         SET owner_service = EXCLUDED.owner_service,
             layer = EXCLUDED.layer,
             asset_kind = EXCLUDED.asset_kind,
             schema_contract_ref = EXCLUDED.schema_contract_ref,
             status = EXCLUDED.status,
             updated_at = now(),
             version = catalog.lakehouse_data_asset.version + 1
         RETURNING {DATA_ASSET_COLUMNS}"
    );
    let row = sqlx::query(&query)
        .bind(asset.id.as_uuid())
        .bind(asset.qualified_name.as_str())
        .bind(asset.owner_service.wire_name())
        .bind(asset.layer.wire_name())
        .bind(asset.asset_kind.wire_name())
        .bind(asset.schema_contract_ref.as_deref())
        .bind(asset.status.wire_name())
        .bind(asset.created_at)
        .bind(asset.updated_at)
        .bind(asset.version)
        .fetch_one(connection)
        .await
        .map_err(map_sqlx)?;
    row_to_asset(&row)
}

async fn record_dataset_version_tx(
    connection: &mut PgConnection,
    version: &LakehouseDatasetVersion,
) -> Result<LakehouseDatasetVersion, LakehouseError> {
    sqlx::query(
        "UPDATE catalog.lakehouse_dataset_version
         SET state = 'previous'
         WHERE data_asset_id = $1
           AND state = 'active'
           AND id <> $2",
    )
    .bind(version.data_asset_id.as_uuid())
    .bind(version.id.as_uuid())
    .execute(&mut *connection)
    .await
    .map_err(map_sqlx)?;

    let query = format!(
        "INSERT INTO catalog.lakehouse_dataset_version
         ({DATASET_VERSION_COLUMNS})
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (data_asset_id, version) DO UPDATE
         SET state = EXCLUDED.state,
             schema_version = EXCLUDED.schema_version,
             artifact_format = EXCLUDED.artifact_format,
             created_by_ingestion_run_id = EXCLUDED.created_by_ingestion_run_id
         RETURNING {DATASET_VERSION_COLUMNS}"
    );
    let row = sqlx::query(&query)
        .bind(version.id.as_uuid())
        .bind(version.data_asset_id.as_uuid())
        .bind(version.version.as_str())
        .bind(version.state.wire_name())
        .bind(version.schema_version.as_str())
        .bind(version.artifact_format.wire_name())
        .bind(
            version
                .created_by_ingestion_run_id
                .map(|run_id| run_id.as_uuid()),
        )
        .bind(version.created_at)
        .fetch_one(connection)
        .await
        .map_err(map_sqlx)?;
    row_to_dataset_version(&row)
}

async fn record_object_artifact_tx(
    connection: &mut PgConnection,
    artifact: &LakehouseObjectArtifact,
) -> Result<LakehouseObjectArtifact, LakehouseError> {
    let query = format!(
        "INSERT INTO catalog.lakehouse_object_artifact
         ({OBJECT_ARTIFACT_COLUMNS})
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (namespace_id, object_key) DO UPDATE
         SET created_at = catalog.lakehouse_object_artifact.created_at
         WHERE catalog.lakehouse_object_artifact.dataset_version_id = EXCLUDED.dataset_version_id
           AND catalog.lakehouse_object_artifact.content_type = EXCLUDED.content_type
           AND catalog.lakehouse_object_artifact.checksum_sha256 = EXCLUDED.checksum_sha256
           AND catalog.lakehouse_object_artifact.size_bytes = EXCLUDED.size_bytes
           AND catalog.lakehouse_object_artifact.logical_record_count IS NOT DISTINCT FROM EXCLUDED.logical_record_count
         RETURNING {OBJECT_ARTIFACT_COLUMNS}"
    );
    let row = sqlx::query(&query)
        .bind(artifact.id.as_uuid())
        .bind(artifact.dataset_version_id.as_uuid())
        .bind(artifact.namespace_id.as_uuid())
        .bind(artifact.object_key.as_str())
        .bind(artifact.content_type.as_str())
        .bind(artifact.checksum_sha256.as_str())
        .bind(u64_to_i64("size_bytes", artifact.size_bytes)?)
        .bind(
            artifact
                .logical_record_count
                .map(|count| u64_to_i64("logical_record_count", count))
                .transpose()?,
        )
        .bind(artifact.created_at)
        .fetch_optional(connection)
        .await
        .map_err(map_sqlx)?;
    let row = row.ok_or_else(|| {
        LakehouseError::InvalidLakehouseRegistryInput(
            "lakehouse object artifact key already exists with different metadata".to_owned(),
        )
    })?;
    row_to_object_artifact(&row)
}
