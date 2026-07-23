//! Collection application outbound ports.
//!
//! Read-only Bronze queries are separated from mutation operations so infrastructure adapters can
//! preserve transaction boundaries without leaking Catalog application contracts into Collection.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, IngestionRunStatus, SchemaProfile,
    SourceCatalogEntry,
};
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};

/// Command for completing an ingestion run with terminal collection metrics.
#[derive(Clone, Debug)]
pub struct CompleteIngestionRunCommand {
    /// Ingestion run to complete.
    pub id: IngestionRunId,
    /// Terminal status to persist.
    pub status: IngestionRunStatus,
    /// UTC timestamp when the run ended.
    pub finished_at: DateTime<Utc>,
    /// Number of logical source records observed by the collector, when countable.
    pub logical_records_seen: u64,
    /// Number of object-storage objects written to Bronze storage.
    pub objects_written: u64,
    /// Optional failure message for failed or cancelled runs.
    pub error_message: Option<String>,
}

/// Read-only Bronze ingestion queries.
#[async_trait]
pub trait BronzeIngestRepository: Send + Sync {
    /// Finds a source catalog entry by slug.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when repository access fails.
    async fn find_source_catalog_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<SourceCatalogEntry>, CollectionError>;

    /// Finds an ingestion run by id.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when repository access fails.
    async fn find_ingestion_run(
        &self,
        id: IngestionRunId,
    ) -> Result<Option<IngestionRun>, CollectionError>;

    /// Lists Bronze object metadata rows first recorded by one ingestion run.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when repository access fails.
    async fn list_bronze_objects_by_run(
        &self,
        run_id: IngestionRunId,
    ) -> Result<Vec<BronzeObject>, CollectionError>;

    /// Finds a previously recorded Bronze object by immutable source partition.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when repository access fails.
    async fn find_bronze_object_by_source_partition_key(
        &self,
        source_catalog_id: SourceCatalogId,
        source_partition_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError>;

    /// Lists schema profiles produced by one ingestion run.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when repository access fails.
    async fn list_schema_profiles_by_run(
        &self,
        run_id: IngestionRunId,
    ) -> Result<Vec<SchemaProfile>, CollectionError>;
}

/// Mutation boundary for Bronze ingestion metadata.
#[async_trait]
pub trait BronzeIngestUnitOfWork: Send + Sync {
    /// Creates or updates a source catalog entry identified by its slug.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when persistence fails.
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError>;

    /// Creates a new ingestion run.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when persistence fails.
    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError>;

    /// Completes an ingestion run and persists terminal metrics.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when the run is missing or persistence fails.
    async fn complete_ingestion_run(
        &self,
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError>;

    /// Finds a previously recorded Bronze object by its deterministic object key.
    ///
    /// The committer uses this in the recoverable commit protocol: when a `CreateOnly` write
    /// collides, it looks up the row for `(source_catalog_id, object_key)` to decide idempotent
    /// success, recovery, or quarantine.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when repository access fails.
    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError>;

    /// Records Bronze object metadata idempotently by `(source_catalog_id, dedupe_key)`.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when persistence fails.
    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError>;

    /// Creates or updates a schema profile identified by `(ingestion_run_id, field_path)`.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when persistence fails.
    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError>;
}
