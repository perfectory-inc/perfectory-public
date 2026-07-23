//! Collection application use cases and outbound ports.
//!
//! This crate owns Bronze commit/recovery and provider collection planning. It deliberately has no
//! dependency on Catalog packages; infrastructure adapters implement the ports declared here.

#![deny(missing_docs)]

/// Evidence-driven recovery of missing Bronze metadata.
pub mod bronze_catalog_recovery;
/// Single-seam Bronze commit boundary.
pub mod bronze_committer;
/// Planning helpers for catalog-driven `hub.go.kr` bulk collection.
pub mod building_hub_bulk_collection_plan;
/// Planning helpers for building-register Bronze ingestion pages.
pub mod building_register_bronze_plan;
/// Outbound ports implemented by Collection infrastructure.
pub mod ports;
/// Landing-key contract for provider acquisition workers.
pub mod provider_acquisition_landing;
/// Planning helpers for provider acquisition jobs.
pub mod provider_acquisition_plan;
/// Generic planning helpers for JSON public-data Bronze ingestion pages.
pub mod public_data_bronze_plan;
/// Generic planning helpers for immutable public-data bulk files.
pub mod public_data_bulk_plan;
/// Planning helpers for data.go.kr real-transaction Bronze ingestion pages.
pub mod real_transaction_bronze_plan;
/// Planning helpers for `rt.molit.go.kr` real-transaction CSV export Bronze files.
pub mod rt_molit_real_transaction_export_plan;
/// Planning helpers for `VWorld` cadastral Bronze ingestion pages.
pub mod vworld_cadastral_bronze_plan;
/// Planning helpers for `VWorld` provider dataset-file collection.
pub mod vworld_dataset_collection_plan;
/// Planning helpers for `VWorld` land-register Bronze ingestion pages.
pub mod vworld_land_register_bronze_plan;
/// Planning helpers for generic `VWorld` NED attribute Bronze ingestion pages.
pub mod vworld_ned_bronze_plan;

pub use bronze_committer::{
    BronzeCommitError, BronzeCommitOutcome, BronzeCommitter, BronzePayload, BronzeRawObjectWriter,
    BronzeStorageError, BronzeStreamingRawObjectWriter, BronzeStreamingWriteOutcome,
    BronzeStreamingWriteRequest, BronzeWriteMode, BronzeWriteOutcome, BronzeWriteRequest,
    BuildingRegisterCommitInput, BuildingRegisterCommitOutcome, PlannedBronzeObject,
    PlannedStreamingBronzeObject, PublicDataPageCommitInput, PublicDataPageCommitOutcome,
    RealTransactionCommitInput, RealTransactionCommitOutcome, StreamedObjectRehash,
    StreamingBronzeCommitOutcome, StreamingBronzeRecord, VWorldCadastralCommitInput,
    VWorldCadastralCommitOutcome, VWorldLandRegisterCommitInput, VWorldLandRegisterCommitOutcome,
    VWorldNedCommitInput, VWorldNedCommitOutcome,
};
pub use building_hub_bulk_collection_plan::{
    plan_building_hub_bulk_collection, BuildingHubBulkCollectionJob, BuildingHubBulkCollectionPlan,
    BuildingHubBulkCollectionPlanError, BuildingHubBulkEndpoint, BuildingHubBulkInventoryFile,
    BuildingHubBulkInventorySelector,
};
pub use building_register_bronze_plan::{
    build_building_register_bronze_object_key, plan_building_register_bronze_page,
    BuildingRegisterBronzePagePlan, BuildingRegisterBronzePagePlanInput,
    BuildingRegisterBronzePlanError, BuildingRegisterPageRequest,
    BuildingRegisterSchemaObservation,
};
pub use provider_acquisition_landing::{
    provider_landing_key, ProviderLandingError, ProviderLandingObject,
};
pub use provider_acquisition_plan::{
    plan_vworld_raon_acquisition, ProviderAcquisitionPlan, ProviderBlockedVWorldFile,
};
pub use public_data_bronze_plan::{
    build_public_data_bronze_object_key, plan_public_data_bronze_page, PublicDataBronzePagePlan,
    PublicDataBronzePagePlanInput, PublicDataBronzePageRequest, PublicDataBronzePlanError,
    PublicDataFixedQueryParam, PublicDataPageRequest, PublicDataPartitionField,
    PublicDataSchemaObservation,
};
pub use public_data_bulk_plan::{
    plan_public_data_bulk_file, plan_public_data_bulk_file_metadata,
    plan_public_data_bulk_file_storage_location, public_data_bulk_file_dedupe_key,
    public_data_bulk_file_request_params, public_data_bulk_file_source_partition_key,
    PublicDataBulkFileIdentity, PublicDataBulkFileMetadataInput, PublicDataBulkFileMetadataPlan,
    PublicDataBulkFilePlan, PublicDataBulkFilePlanError, PublicDataBulkFilePlanInput,
    PublicDataBulkFileSourcePartitionKeyInput, PublicDataBulkFileStorageLocationInput,
    PublicDataBulkFileStorageLocationPlan,
};
pub use real_transaction_bronze_plan::{
    build_real_transaction_bronze_object_key, plan_real_transaction_bronze_page,
    RealTransactionBronzePagePlan, RealTransactionBronzePagePlanInput,
    RealTransactionBronzePlanError, RealTransactionPageRequest, RealTransactionSchemaObservation,
};
pub use rt_molit_real_transaction_export_plan::{
    plan_rt_molit_real_transaction_export, RtMolitExportScope, RtMolitRealTransactionExportPlan,
    RtMolitRealTransactionExportPlanError, RtMolitRealTransactionExportPlanInput,
    RtMolitRealTransactionExportRequest,
};
pub use vworld_cadastral_bronze_plan::{
    build_vworld_cadastral_bronze_object_key, plan_vworld_cadastral_bronze_page,
    VWorldCadastralBronzePagePlan, VWorldCadastralBronzePagePlanInput,
    VWorldCadastralBronzePlanError, VWorldCadastralPageRequest, VWorldCadastralSchemaObservation,
};
pub use vworld_dataset_collection_plan::{
    plan_vworld_dataset_collection, VWorldDatasetCollectionEndpoint, VWorldDatasetCollectionJob,
    VWorldDatasetCollectionPlan, VWorldDatasetCollectionPlanError, VWorldDatasetInventoryDataset,
    VWorldDatasetInventorySelector,
};
pub use vworld_land_register_bronze_plan::{
    build_vworld_land_register_bronze_object_key, plan_vworld_land_register_bronze_page,
    VWorldLandRegisterBronzePagePlan, VWorldLandRegisterBronzePagePlanInput,
    VWorldLandRegisterBronzePlanError, VWorldLandRegisterPageRequest,
    VWorldLandRegisterSchemaObservation,
};
pub use vworld_ned_bronze_plan::{
    plan_vworld_ned_bronze_page, VWorldNedBronzePagePlan, VWorldNedBronzePagePlanInput,
    VWorldNedBronzePlanError, VWorldNedPageRequest, VWorldNedSchemaObservation,
};
