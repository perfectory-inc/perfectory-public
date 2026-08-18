//! Use case for writing source-side industrial-complex rows into Catalog by official code.
//!
//! One write path serves every producer of `catalog.industrial_complex`: the hand-written seed
//! import and the Gold catalog loader (root ADR-0040). A second use case with the same body would
//! be the place where one of the two forgets a validation rule.

use std::sync::Arc;

use catalog_domain::{CatalogError, IndustrialComplexKind};

use crate::industrial_complex_input::{
    validate_clean_required, validate_optional_primary_bjdong_code,
    validate_source_official_complex_code,
};
use crate::ports::{
    CatalogUnitOfWork, UpsertIndustrialComplexCommand, UpsertIndustrialComplexOutcome,
};

/// Source-side row that establishes canonical industrial-complex identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexCatalogRow {
    /// Source-side official industrial-complex code. The natural key of the upsert.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Domain-level industrial complex kind.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code shared by parcels that belong to the complex, when one is known.
    pub primary_bjdong_code: Option<String>,
    /// Official complex area in square meters.
    pub area_m2: u64,
}

/// Input for writing source-side industrial-complex rows into Catalog.
pub struct UpsertIndustrialComplexCatalogRowsInput {
    /// Source-side rows to create or update by `official_complex_code`.
    pub rows: Vec<IndustrialComplexCatalogRow>,
}

/// Result of writing source-side industrial-complex rows into Catalog.
#[derive(Clone, Debug)]
pub struct UpsertIndustrialComplexCatalogRowsReport {
    /// Per-row outcome in command order.
    pub outcomes: Vec<UpsertIndustrialComplexOutcome>,
}

impl UpsertIndustrialComplexCatalogRowsReport {
    /// Number of rows the write path created, updated, or left alone.
    #[must_use]
    pub const fn written_count(&self) -> usize {
        self.outcomes.len()
    }

    /// Number of rows with the given effect.
    #[must_use]
    pub fn count_with(&self, effect: crate::ports::UpsertIndustrialComplexEffect) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.effect == effect)
            .count()
    }
}

/// Writes source-side industrial-complex identity into Catalog.
pub struct UpsertIndustrialComplexCatalogRows {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl UpsertIndustrialComplexCatalogRows {
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
        input: UpsertIndustrialComplexCatalogRowsInput,
    ) -> Result<UpsertIndustrialComplexCatalogRowsReport, CatalogError> {
        if input.rows.is_empty() {
            return Err(CatalogError::InvalidIndustrialComplexInput(
                "industrial-complex catalog upsert must contain at least one row".to_owned(),
            ));
        }

        let commands = input
            .rows
            .iter()
            .map(catalog_row_to_upsert_command)
            .collect::<Result<Vec<_>, _>>()?;
        let outcomes = self
            .uow
            .upsert_complexes_by_official_code(&commands)
            .await?;
        Ok(UpsertIndustrialComplexCatalogRowsReport { outcomes })
    }
}

fn catalog_row_to_upsert_command(
    row: &IndustrialComplexCatalogRow,
) -> Result<UpsertIndustrialComplexCommand, CatalogError> {
    validate_clean_required("official_complex_code", row.official_complex_code.as_str())?;
    validate_source_official_complex_code(row.official_complex_code.as_str())?;
    validate_clean_required("name", row.name.as_str())?;
    validate_optional_primary_bjdong_code(row.primary_bjdong_code.as_deref())?;
    Ok(UpsertIndustrialComplexCommand {
        official_complex_code: row.official_complex_code.clone(),
        name: row.name.clone(),
        kind: row.kind,
        primary_bjdong_code: row.primary_bjdong_code.clone(),
        area_m2: row.area_m2,
    })
}

#[cfg(test)]
mod tests {
    use super::{catalog_row_to_upsert_command, IndustrialComplexCatalogRow};
    use catalog_domain::{CatalogError, IndustrialComplexKind};

    fn row(primary_bjdong_code: Option<&str>) -> IndustrialComplexCatalogRow {
        IndustrialComplexCatalogRow {
            official_complex_code: "111010".to_owned(),
            name: "한국수출국가산업단지".to_owned(),
            kind: IndustrialComplexKind::National,
            primary_bjdong_code: primary_bjdong_code.map(ToOwned::to_owned),
            area_m2: 3_708_451,
        }
    }

    #[test]
    fn a_row_without_a_legal_dong_code_is_accepted() -> Result<(), CatalogError> {
        let command = catalog_row_to_upsert_command(&row(None))?;
        assert_eq!(command.primary_bjdong_code, None);
        Ok(())
    }

    #[test]
    fn a_row_with_a_malformed_legal_dong_code_is_still_rejected() {
        let error = catalog_row_to_upsert_command(&row(Some("28200")));
        assert!(matches!(
            error,
            Err(CatalogError::InvalidIndustrialComplexInput(_))
        ));
    }

    #[test]
    fn a_row_with_a_well_formed_legal_dong_code_keeps_it() -> Result<(), CatalogError> {
        let command = catalog_row_to_upsert_command(&row(Some("1138010700")))?;
        assert_eq!(command.primary_bjdong_code.as_deref(), Some("1138010700"));
        Ok(())
    }
}
