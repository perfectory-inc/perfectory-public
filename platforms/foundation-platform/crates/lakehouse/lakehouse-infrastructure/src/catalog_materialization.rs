//! Adapter from the canonical Catalog read model to Lakehouse materialization inputs.

use std::sync::Arc;

use async_trait::async_trait;
use catalog_application::ports::CatalogRepository;
use catalog_domain::IndustrialComplex;
use lakehouse_application::ports::IndustrialComplexMaterializationReader;
use lakehouse_domain::LakehouseError;

/// Reads canonical industrial-complex inputs through the Catalog application contract.
pub struct CatalogIndustrialComplexMaterializationReader {
    repository: Arc<dyn CatalogRepository>,
}

impl CatalogIndustrialComplexMaterializationReader {
    /// Creates the Lakehouse-facing adapter around a canonical Catalog reader.
    #[must_use]
    pub fn new(repository: Arc<dyn CatalogRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl IndustrialComplexMaterializationReader for CatalogIndustrialComplexMaterializationReader {
    async fn list_industrial_complexes(&self) -> Result<Vec<IndustrialComplex>, LakehouseError> {
        self.repository
            .list_complexes()
            .await
            .map_err(|error| LakehouseError::Upstream(error.to_string()))
    }
}
