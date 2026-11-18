//! Collection persistence and provider adapters.

pub mod bronze_repository;
pub mod building_hub_bulk;
#[cfg(feature = "court-auction-browser")]
pub mod court_auction_browser;
pub mod ilis_industrial_complex_api;
mod outbound_http_error;
mod row_map;
pub mod vworld_dataset_file;

pub use bronze_repository::{PgBronzeIngestRepository, PgBronzeIngestUnitOfWork};
pub use building_hub_bulk::{
    parse_building_hub_bulk_inventory, BuildingHubBulkClient, BuildingHubBulkConfig,
    BuildingHubBulkDownloadRequest, BuildingHubBulkFile, BuildingHubBulkFileStream,
    BuildingHubBulkInventoryItem,
};
#[cfg(feature = "court-auction-browser")]
pub use court_auction_browser::{BrowserSessionConfig, BrowserSessionPool};
pub use ilis_industrial_complex_api::{
    IlisFileDownload, IlisIndustrialComplexApiClient, IlisIndustrialComplexApiConfig,
    IlisIndustrialComplexPage, IlisRequestPolicy,
};
pub use vworld_dataset_file::{
    parse_vworld_dataset_file_inventory_page, VWorldDatasetFile, VWorldDatasetFileClient,
    VWorldDatasetFileConfig, VWorldDatasetFileDownloadRequest, VWorldDatasetFileInventoryItem,
    VWorldDatasetFileInventoryPage, VWorldDatasetFileInventorySelector, VWorldDatasetFileKind,
    VWorldDatasetFileStream, VWorldDatasetLoginClient, VWorldDatasetLoginConfig,
};
