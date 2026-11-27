//! The seam through which `IcebergRestCatalog`'s table loads come from another transport (root
//! ADR-0624 P6).

use std::sync::Arc;

use async_trait::async_trait;
use lakehouse_domain::LakehouseError;

use super::{IcebergRestCatalog, LoadTableResponse};
use crate::lakehouse_config::LakehouseCatalogConfig;

/// Where a table's load-table answer comes from when not from this client's own HTTP call: the
/// Lakekeeper catalog, opened through iceberg-catalog-rest with the lane's OAuth2 client (root
/// ADR-0624 P6: the transport is iceberg-catalog-rest's, the surface stays this client's). The publisher
/// implements it (`foundation_publisher_core::lakehouse_catalog`); this crate stays free of iceberg-rust.
#[async_trait]
pub trait TableMetadataSource: std::fmt::Debug + Send + Sync {
    /// The table's load-table answer as the REST specification writes it (an object with
    /// `metadata-location` and `metadata`); `None` when the catalog holds no such table.
    ///
    /// # Errors
    /// Returns `LakehouseError` when the catalog cannot be reached or refuses the call.
    async fn load_table(
        &self,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, LakehouseError>;
}

impl IcebergRestCatalog {
    /// A client whose table loads come from `source` (the Lakekeeper catalog through
    /// iceberg-catalog-rest) instead of its own HTTP call; every reading method keeps its meaning,
    /// because the answer is the same load-table object.
    ///
    /// # Errors
    ///
    /// Returns `LakehouseError` when the resilience-configured HTTP client cannot be built.
    pub fn with_table_source(
        config: LakehouseCatalogConfig,
        source: Arc<dyn TableMetadataSource>,
    ) -> Result<Self, LakehouseError> {
        let mut catalog = Self::new(config)?;
        catalog.source = Some(source);
        Ok(catalog)
    }
}

/// `table_name`'s load-table answer from `source`, read as this client reads its own.
pub(super) async fn load_from(
    source: &dyn TableMetadataSource,
    table_name: &str,
) -> Result<Option<LoadTableResponse>, LakehouseError> {
    source
        .load_table(table_name)
        .await?
        .map(|answer| {
            serde_json::from_value(answer).map_err(|error| {
                LakehouseError::Upstream(format!(
                    "the catalog's load-table answer for {table_name} is not one: {error}"
                ))
            })
        })
        .transpose()
}
