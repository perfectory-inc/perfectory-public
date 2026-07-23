//! `PostgreSQL` and Catalog collaborator error mapping.

use catalog_domain::CatalogError;
use foundation_normalization_domain::NormalizationError;

#[allow(clippy::needless_pass_by_value)]
pub fn map_sqlx(error: sqlx::Error) -> NormalizationError {
    NormalizationError::Persistence(error.to_string())
}

pub fn map_catalog(error: CatalogError) -> NormalizationError {
    match error {
        CatalogError::ComplexNotFound(id) => NormalizationError::TargetNotFound(id),
        CatalogError::ComplexAlreadyArchived(id) => NormalizationError::TargetArchived(id),
        CatalogError::ComplexVersionConflict { expected, current } => {
            NormalizationError::TargetVersionConflict { expected, current }
        }
        CatalogError::ComplexStateConflict(id) => NormalizationError::TargetStateConflict(id),
        CatalogError::InvalidIndustrialComplexInput(message) => {
            NormalizationError::InvalidInput(message)
        }
        CatalogError::Infrastructure(message) => NormalizationError::Persistence(message),
        other => NormalizationError::Persistence(other.to_string()),
    }
}
