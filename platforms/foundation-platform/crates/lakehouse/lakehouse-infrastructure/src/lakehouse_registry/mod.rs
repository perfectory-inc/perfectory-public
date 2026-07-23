//! `PostgreSQL` implementation of Lakehouse Registry reads and atomic writes.

mod row_mapping;
mod transaction;

use async_trait::async_trait;
use foundation_shared_kernel::ids::LakehouseDatasetVersionId;
use lakehouse_application::ports::{LakehouseRegistryRepository, LakehouseRegistryUnitOfWork};
use lakehouse_application::{
    RegisterLakehouseObjectArtifactCommand, RegisterLakehouseObjectArtifactReceipt,
};
use lakehouse_domain::{
    LakehouseDatasetVersion, LakehouseEnvironment, LakehouseError, LakehouseObjectArtifact,
    LakehouseOwnerService, LakehouseStorageNamespace,
};
use sqlx::PgPool;

use crate::postgres_error::map_sqlx;
use row_mapping::{
    row_to_dataset_version, row_to_namespace, row_to_object_artifact, NAMESPACE_COLUMNS,
    OBJECT_ARTIFACT_COLUMNS, QUALIFIED_DATASET_VERSION_COLUMNS,
};

/// `PostgreSQL` implementation of Lakehouse Registry metadata queries and namespace administration.
pub struct PgLakehouseRegistryRepository {
    pool: PgPool,
}

impl PgLakehouseRegistryRepository {
    /// Creates a repository backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `PostgreSQL` transaction boundary for one complete Registry artifact commit.
pub struct PgLakehouseRegistryUnitOfWork {
    pool: PgPool,
}

impl PgLakehouseRegistryUnitOfWork {
    /// Creates a unit of work backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LakehouseRegistryRepository for PgLakehouseRegistryRepository {
    async fn upsert_storage_namespace(
        &self,
        namespace: &LakehouseStorageNamespace,
    ) -> Result<LakehouseStorageNamespace, LakehouseError> {
        let query = format!(
            "INSERT INTO catalog.lakehouse_storage_namespace
             ({NAMESPACE_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (provider, environment, owner_service) DO UPDATE
             SET bucket_name = EXCLUDED.bucket_name,
                 root_prefix = EXCLUDED.root_prefix,
                 catalog_provider = EXCLUDED.catalog_provider,
                 status = EXCLUDED.status,
                 updated_at = now(),
                 version = catalog.lakehouse_storage_namespace.version + 1
             RETURNING {NAMESPACE_COLUMNS}"
        );
        let row = sqlx::query(&query)
            .bind(namespace.id.as_uuid())
            .bind(namespace.provider.wire_name())
            .bind(namespace.environment.wire_name())
            .bind(namespace.owner_service.wire_name())
            .bind(namespace.bucket_name.as_str())
            .bind(
                namespace
                    .root_prefix
                    .as_ref()
                    .map(foundation_shared_kernel::ObjectKeyPrefix::as_str),
            )
            .bind(namespace.catalog_provider.wire_name())
            .bind(namespace.status.wire_name())
            .bind(namespace.created_at)
            .bind(namespace.updated_at)
            .bind(namespace.version)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)?;

        row_to_namespace(&row)
    }

    async fn find_storage_namespace(
        &self,
        owner_service: LakehouseOwnerService,
        environment: LakehouseEnvironment,
    ) -> Result<Option<LakehouseStorageNamespace>, LakehouseError> {
        let query = format!(
            "SELECT {NAMESPACE_COLUMNS}
             FROM catalog.lakehouse_storage_namespace
             WHERE owner_service = $1
               AND environment = $2"
        );
        let row = sqlx::query(&query)
            .bind(owner_service.wire_name())
            .bind(environment.wire_name())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;

        row.as_ref().map(row_to_namespace).transpose()
    }

    async fn list_object_artifacts(
        &self,
        dataset_version_id: LakehouseDatasetVersionId,
    ) -> Result<Vec<LakehouseObjectArtifact>, LakehouseError> {
        let query = format!(
            "SELECT {OBJECT_ARTIFACT_COLUMNS}
             FROM catalog.lakehouse_object_artifact
             WHERE dataset_version_id = $1
             ORDER BY object_key ASC"
        );
        let rows = sqlx::query(&query)
            .bind(dataset_version_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;

        rows.iter().map(row_to_object_artifact).collect()
    }

    async fn find_active_dataset_version(
        &self,
        qualified_name: &str,
    ) -> Result<Option<LakehouseDatasetVersion>, LakehouseError> {
        let query = format!(
            "SELECT {QUALIFIED_DATASET_VERSION_COLUMNS}
             FROM catalog.lakehouse_dataset_version version
             JOIN catalog.lakehouse_data_asset asset
               ON asset.id = version.data_asset_id
             WHERE asset.qualified_name = $1
               AND version.state = 'active'"
        );
        let row = sqlx::query(&query)
            .bind(qualified_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;

        row.as_ref().map(row_to_dataset_version).transpose()
    }
}

#[async_trait]
impl LakehouseRegistryUnitOfWork for PgLakehouseRegistryUnitOfWork {
    async fn register_object_artifact(
        &self,
        command: RegisterLakehouseObjectArtifactCommand,
    ) -> Result<RegisterLakehouseObjectArtifactReceipt, LakehouseError> {
        transaction::register_object_artifact(&self.pool, command).await
    }
}
