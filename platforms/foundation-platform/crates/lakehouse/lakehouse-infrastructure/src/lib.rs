//! Lakehouse persistence and provider adapters.

#![deny(missing_docs)]

mod gold_publication;
mod postgres_error;

/// Adapter for canonical Catalog inputs consumed by Lakehouse materialization.
pub mod catalog_materialization;

/// Provider-neutral Iceberg REST catalog adapter.
pub mod iceberg_rest_catalog;

/// PostgreSQL Lakehouse batch audit adapters.
pub mod lakehouse_batch_audit;

/// Lakehouse catalog runtime configuration.
pub mod lakehouse_config;

/// Outbound HTTP error mapping for Lakehouse adapters.
pub mod outbound_http_error;

/// PostgreSQL Lakehouse Registry adapters.
pub mod lakehouse_registry;

pub use catalog_materialization::CatalogIndustrialComplexMaterializationReader;
pub use gold_publication::{
    PgIndustrialComplexGoldPointerReader, PgLakehousePublicationUnitOfWork,
};
pub use iceberg_rest_catalog::IcebergRestCatalog;
pub use lakehouse_batch_audit::{PgLakehouseBatchRunAudit, PgLakehouseBatchRunRepository};
pub use lakehouse_config::{
    live_lakehouse_smoke_enabled, validate_lakehouse_smoke_table_name, LakehouseCatalogConfig,
    LakehouseCatalogConfigError, LakehouseCatalogProvider, DEFAULT_LAKEHOUSE_SMOKE_TABLE,
};
pub use lakehouse_registry::{PgLakehouseRegistryRepository, PgLakehouseRegistryUnitOfWork};
