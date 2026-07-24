//! Outbound ports owned by the Lakehouse application capability.

use async_trait::async_trait;
use catalog_domain::IndustrialComplex;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, LakehouseDatasetVersionId, StaffId};
use lakehouse_domain::{
    IndustrialComplexGoldPointer, LakehouseDatasetVersion, LakehouseEnvironment, LakehouseError,
    LakehouseObjectArtifact, LakehouseOwnerService, LakehouseStorageNamespace,
    LakehouseTableContract, SparkRunSummary, SparkRunWriteDisposition,
};
use uuid::Uuid;

use crate::{
    PublishIndustrialComplexGoldPointerCommand, RegisterLakehouseObjectArtifactCommand,
    RegisterLakehouseObjectArtifactReceipt,
};

/// Read-only canonical industrial-complex input required by Lakehouse materialization.
#[async_trait]
pub trait IndustrialComplexMaterializationReader: Send + Sync {
    /// Lists active canonical industrial complexes in stable source order.
    ///
    /// # Errors
    /// Returns `LakehouseError` when canonical input access fails.
    async fn list_industrial_complexes(&self) -> Result<Vec<IndustrialComplex>, LakehouseError>;
}

/// Read-only access to published industrial-complex Gold pointers.
#[async_trait]
pub trait IndustrialComplexGoldPointerReader: Send + Sync {
    /// Lists Gold pointers for the requested industrial complexes.
    ///
    /// # Errors
    /// Returns `LakehouseError` when persistence access fails.
    async fn list_industrial_complex_gold_pointers(
        &self,
        complex_ids: &[ComplexId],
    ) -> Result<Vec<IndustrialComplexGoldPointer>, LakehouseError>;

    /// Loads the current Gold pointer for one industrial complex.
    ///
    /// # Errors
    /// Returns `LakehouseError` when persistence access fails.
    async fn find_industrial_complex_gold_pointer(
        &self,
        complex_id: ComplexId,
    ) -> Result<Option<IndustrialComplexGoldPointer>, LakehouseError>;
}

/// Atomic write boundary for industrial-complex Gold publication.
#[async_trait]
pub trait LakehousePublicationUnitOfWork: Send + Sync {
    /// Commits source lineage, file assets, the active pointer, and its outbox event together.
    ///
    /// # Errors
    /// Returns `LakehouseError` and rolls back every mutation when any step fails.
    async fn publish_industrial_complex_gold_pointer(
        &self,
        command: PublishIndustrialComplexGoldPointerCommand,
    ) -> Result<IndustrialComplexGoldPointer, LakehouseError>;
}

/// Current Iceberg snapshot metadata for a Lakehouse table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseTableSnapshot {
    /// Fully qualified table name such as `silver.industrial_complexes`.
    pub table_name: String,
    /// Iceberg snapshot id exposed by the catalog/query engine.
    pub snapshot_id: String,
    /// Provider-neutral metadata location, usually an R2 object URI or HTTPS URL.
    pub metadata_location: String,
}

/// Read model for a validated Lakehouse batch run that may be used by promotion workflows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseBatchRunRecord {
    /// Audit row id.
    pub id: Uuid,
    /// Summary schema version recorded in the indexed audit columns.
    pub schema_version: String,
    /// Stable batch job name.
    pub job_name: String,
    /// Fully qualified Lakehouse contract table name.
    pub contract: String,
    /// UTC timestamp emitted by the batch job.
    pub created_at_utc: DateTime<Utc>,
    /// Write disposition emitted by the batch job.
    pub write_disposition: SparkRunWriteDisposition,
    /// Number of candidate rows produced by the batch job.
    pub row_count: u64,
    /// Number of rows read back from persisted output.
    pub persisted_row_count: Option<u64>,
    /// Distinct Bronze or source snapshot ids represented by the batch.
    pub source_snapshot_ids: Vec<String>,
    /// Original machine-readable batch summary stored for audit and lineage.
    pub summary: SparkRunSummary,
    /// Staff operator that caused Foundation Platform to record this batch audit row.
    pub recorded_by_staff_id: StaffId,
    /// Optional caller-supplied request id used for trace correlation.
    pub request_id: Option<String>,
    /// UTC timestamp when Foundation Platform recorded the audit row.
    pub recorded_at_utc: DateTime<Utc>,
}

/// Command for recording a validated Lakehouse batch run audit row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakehouseBatchRunAuditCommand {
    /// Batch execution summary emitted by the Spark job.
    pub summary: SparkRunSummary,
    /// Staff operator that requested or approved recording the summary.
    pub recorded_by_staff_id: StaffId,
    /// Optional caller-supplied request id used for trace correlation.
    pub request_id: Option<String>,
}

/// Provider-neutral Iceberg catalog port used by Lakehouse application use cases.
#[async_trait]
pub trait LakehouseCatalog: Send + Sync {
    /// Ensures a Lakehouse table exists according to the static Foundation Platform contract.
    ///
    /// # Errors
    /// Returns `LakehouseError` when catalog access or table creation fails.
    async fn ensure_table(
        &self,
        contract: &'static LakehouseTableContract,
    ) -> Result<LakehouseTableSnapshot, LakehouseError>;

    /// Loads the current snapshot for a Lakehouse table.
    ///
    /// # Errors
    /// Returns `LakehouseError` when catalog access fails.
    async fn get_current_snapshot(
        &self,
        table_name: &str,
    ) -> Result<Option<LakehouseTableSnapshot>, LakehouseError>;
}

/// Provider-neutral candidate handle for a spatial tile WAP branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpatialTileWapCandidate {
    /// Immutable candidate branch or snapshot identifier.
    pub candidate_snapshot_id: String,
    /// Release id bound to this candidate.
    pub release_id: Uuid,
    /// Base snapshot from which the candidate was created.
    pub base_snapshot_id: String,
}

/// Validated spatial tile WAP evidence returned by the provider adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpatialTileWapValidation {
    /// Candidate branch that was validated.
    pub candidate_snapshot_id: String,
    /// Canonical data revision represented by the candidate.
    pub data_revision: Uuid,
    /// Feature row count loaded into the complete serving projection.
    pub row_count: u64,
    /// Evidence checksum for the validation receipt.
    pub evidence_sha256: String,
}

/// Provider-neutral Iceberg WAP boundary for spatial tile publication.
#[async_trait]
pub trait SpatialTileWap: Send + Sync {
    /// Creates an isolated candidate from the currently selected base snapshot.
    async fn prepare_candidate(
        &self,
        base_snapshot_id: &str,
        change_set: &str,
        release_id: Uuid,
    ) -> Result<SpatialTileWapCandidate, LakehouseError>;

    /// Validates candidate isolation and complete spatial quality.
    async fn validate_candidate(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<SpatialTileWapValidation, LakehouseError>;

    /// Retains the selected candidate for rollback/audit retention.
    async fn retain_selected(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<(), LakehouseError>;

    /// Expires an unselected candidate after its bounded retention window.
    async fn expire_unselected(
        &self,
        candidate: &SpatialTileWapCandidate,
    ) -> Result<(), LakehouseError>;

    /// Fast-forwards the provider's main pointer only along the selected ancestry.
    async fn fast_forward_main(&self, selected_snapshot_id: &str) -> Result<(), LakehouseError>;
}

/// Audit sink for Lakehouse batch execution summaries.
#[async_trait]
pub trait LakehouseBatchRunAudit: Send + Sync {
    /// Records a validated Spark run summary for audit, lineage, and later promotion decisions.
    ///
    /// # Errors
    /// Returns `LakehouseError` when persistence fails.
    async fn record_spark_run_summary(
        &self,
        command: LakehouseBatchRunAuditCommand,
    ) -> Result<(), LakehouseError>;
}

/// Read-only access to validated Lakehouse batch run audit rows.
#[async_trait]
pub trait LakehouseBatchRunRepository: Send + Sync {
    /// Returns the newest promotion-safe batch run candidate for a contract.
    ///
    /// # Errors
    /// Returns `LakehouseError` when repository access fails.
    async fn latest_promotion_candidate(
        &self,
        contract: &'static LakehouseTableContract,
    ) -> Result<Option<LakehouseBatchRunRecord>, LakehouseError>;
}

/// Atomic write boundary for a governed Registry object artifact.
#[async_trait]
pub trait LakehouseRegistryUnitOfWork: Send + Sync {
    /// Commits namespace validation, asset upsert, version transition, and artifact insertion.
    ///
    /// # Errors
    /// Returns `LakehouseError` and rolls back every database mutation when any step fails.
    async fn register_object_artifact(
        &self,
        command: RegisterLakehouseObjectArtifactCommand,
    ) -> Result<RegisterLakehouseObjectArtifactReceipt, LakehouseError>;
}

/// Non-transactional Registry administration and query boundary.
#[async_trait]
pub trait LakehouseRegistryRepository: Send + Sync {
    /// Creates or updates a storage namespace.
    ///
    /// # Errors
    /// Returns `LakehouseError` when persistence fails.
    async fn upsert_storage_namespace(
        &self,
        namespace: &LakehouseStorageNamespace,
    ) -> Result<LakehouseStorageNamespace, LakehouseError>;

    /// Finds a namespace by service owner and runtime environment.
    ///
    /// # Errors
    /// Returns `LakehouseError` when repository access fails.
    async fn find_storage_namespace(
        &self,
        owner_service: LakehouseOwnerService,
        environment: LakehouseEnvironment,
    ) -> Result<Option<LakehouseStorageNamespace>, LakehouseError>;

    /// Lists object artifacts belonging to a dataset version.
    ///
    /// # Errors
    /// Returns `LakehouseError` when repository access fails.
    async fn list_object_artifacts(
        &self,
        dataset_version_id: LakehouseDatasetVersionId,
    ) -> Result<Vec<LakehouseObjectArtifact>, LakehouseError>;

    /// Finds the active dataset version for a qualified asset name.
    ///
    /// # Errors
    /// Returns `LakehouseError` when repository access fails.
    async fn find_active_dataset_version(
        &self,
        qualified_name: &str,
    ) -> Result<Option<LakehouseDatasetVersion>, LakehouseError>;
}
