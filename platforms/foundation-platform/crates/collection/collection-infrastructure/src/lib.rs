//! Collection persistence and provider adapters.

pub mod bronze_repository;
pub mod building_hub_bulk;
pub mod data_go_kr_building_register;
pub mod data_go_kr_odcloud_api;
pub mod data_go_kr_service_api;
mod outbound_http_error;
mod row_map;
pub mod vworld_data_api;
pub mod vworld_dataset_file;
pub mod vworld_ned_attribute;

pub use bronze_repository::{PgBronzeIngestRepository, PgBronzeIngestUnitOfWork};
pub use building_hub_bulk::{
    parse_building_hub_bulk_inventory, BuildingHubBulkClient, BuildingHubBulkConfig,
    BuildingHubBulkDownloadRequest, BuildingHubBulkFile, BuildingHubBulkFileStream,
    BuildingHubBulkInventoryItem,
};
pub use data_go_kr_building_register::{
    DataGoKrBuildingRegisterClient, DataGoKrBuildingRegisterConfig, DataGoKrBuildingRegisterPage,
};
pub use data_go_kr_odcloud_api::{
    DataGoKrOdCloudApiClient, DataGoKrOdCloudApiConfig, DataGoKrOdCloudApiPage,
};
pub use data_go_kr_service_api::{
    DataGoKrRequestPolicy, DataGoKrServiceApiClient, DataGoKrServiceApiConfig,
    DataGoKrServiceApiPage,
};
pub use vworld_data_api::{
    VWorldDataApiClient, VWorldDataApiConfig, VWorldDataApiPage, VWorldDataFeatureRequest,
};
pub use vworld_dataset_file::{
    parse_vworld_dataset_file_inventory_page, VWorldDatasetFile, VWorldDatasetFileClient,
    VWorldDatasetFileConfig, VWorldDatasetFileDownloadRequest, VWorldDatasetFileInventoryItem,
    VWorldDatasetFileInventoryPage, VWorldDatasetFileInventorySelector, VWorldDatasetFileKind,
    VWorldDatasetFileStream, VWorldDatasetLoginClient, VWorldDatasetLoginConfig,
};
pub use vworld_ned_attribute::{
    VWorldNedAttributeClient, VWorldNedAttributeConfig, VWorldNedAttributePage, VWorldRequestPolicy,
};
