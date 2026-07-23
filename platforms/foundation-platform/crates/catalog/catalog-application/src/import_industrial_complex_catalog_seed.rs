//! Use case for importing source-side industrial-complex seed rows into Catalog.

use std::sync::Arc;

use catalog_domain::{CatalogError, IndustrialComplexKind};
use foundation_shared_kernel::ids::ComplexId;

use crate::industrial_complex_input::{
    validate_clean_required, validate_primary_bjdong_code, validate_source_official_complex_code,
};
use crate::ports::{CatalogUnitOfWork, UpsertIndustrialComplexCommand};

/// Source-side seed row used to establish canonical industrial-complex Catalog identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexCatalogSeedRow {
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Domain-level industrial complex kind.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code shared by parcels that belong to the complex.
    pub primary_bjdong_code: String,
    /// Official complex area in square meters.
    pub area_m2: u64,
}

/// Input for importing source-side industrial-complex seed rows.
pub struct ImportIndustrialComplexCatalogSeedInput {
    /// Source-side seed rows to create or update by `official_complex_code`.
    pub rows: Vec<IndustrialComplexCatalogSeedRow>,
}

/// Import result for source-side industrial-complex seed rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportIndustrialComplexCatalogSeedReport {
    /// Number of Catalog complexes created or matched/updated by the import.
    pub imported_count: usize,
    /// Foundation Platform complex ids returned by Catalog in command order.
    pub complex_ids: Vec<ComplexId>,
}

/// Imports source-side industrial-complex identity into Catalog.
pub struct ImportIndustrialComplexCatalogSeed {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl ImportIndustrialComplexCatalogSeed {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Creates or updates Catalog complexes by source-side `official_complex_code`.
    ///
    /// # Errors
    /// Returns `CatalogError` when input validation or persistence fails.
    pub async fn execute(
        &self,
        input: ImportIndustrialComplexCatalogSeedInput,
    ) -> Result<ImportIndustrialComplexCatalogSeedReport, CatalogError> {
        if input.rows.is_empty() {
            return Err(CatalogError::InvalidIndustrialComplexInput(
                "industrial-complex seed import must contain at least one row".to_owned(),
            ));
        }

        let commands = input
            .rows
            .iter()
            .map(seed_row_to_upsert_command)
            .collect::<Result<Vec<_>, _>>()?;
        let complexes = self
            .uow
            .upsert_complexes_by_official_code(&commands)
            .await?;
        Ok(ImportIndustrialComplexCatalogSeedReport {
            imported_count: complexes.len(),
            complex_ids: complexes.into_iter().map(|complex| complex.id).collect(),
        })
    }
}

fn seed_row_to_upsert_command(
    row: &IndustrialComplexCatalogSeedRow,
) -> Result<UpsertIndustrialComplexCommand, CatalogError> {
    validate_clean_required("official_complex_code", row.official_complex_code.as_str())?;
    validate_source_official_complex_code(row.official_complex_code.as_str())?;
    validate_clean_required("name", row.name.as_str())?;
    validate_primary_bjdong_code(row.primary_bjdong_code.as_str())?;
    Ok(UpsertIndustrialComplexCommand {
        official_complex_code: row.official_complex_code.clone(),
        name: row.name.clone(),
        kind: row.kind,
        primary_bjdong_code: row.primary_bjdong_code.clone(),
        area_m2: row.area_m2,
    })
}
