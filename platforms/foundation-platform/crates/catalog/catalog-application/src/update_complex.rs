//! Use case for updating canonical industrial complex metadata.

use std::sync::Arc;

use catalog_domain::{CatalogError, ComplexMutation, IndustrialComplex};
use foundation_shared_kernel::ids::ComplexId;

use crate::industrial_complex_input::validate_clean_required;
use crate::ports::CatalogUnitOfWork;

/// Input required to update a canonical industrial complex with optimistic concurrency.
pub struct UpdateIndustrialComplexInput {
    /// Industrial complex that should be updated.
    pub complex_id: ComplexId,
    /// Version observed by the caller.
    pub expected_version: i64,
    /// Optional replacement name.
    pub name: Option<String>,
    /// Optional replacement official area in square meters.
    pub area_m2: Option<u64>,
}

/// Updates an industrial complex through the Catalog unit of work.
pub struct UpdateIndustrialComplex {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl UpdateIndustrialComplex {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Changes canonical industrial complex metadata and records the matching outbox event.
    ///
    /// # Errors
    /// Returns `CatalogError` when the request is empty, invalid, stale, missing, or persistence
    /// fails.
    pub async fn execute(
        &self,
        input: UpdateIndustrialComplexInput,
    ) -> Result<IndustrialComplex, CatalogError> {
        validate_update_input(&input)?;
        self.uow
            .update_complex(
                input.complex_id,
                input.expected_version,
                ComplexMutation {
                    name: input.name,
                    area_m2: input.area_m2,
                },
            )
            .await
    }
}

fn validate_update_input(input: &UpdateIndustrialComplexInput) -> Result<(), CatalogError> {
    if input.expected_version < 1 {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "expected_version must be positive".to_owned(),
        ));
    }
    if input.name.is_none() && input.area_m2.is_none() {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "at least one industrial complex field must be changed".to_owned(),
        ));
    }
    if let Some(name) = input.name.as_deref() {
        validate_clean_required("name", name)?;
    }
    if input.area_m2 == Some(0) {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "area_m2 must be positive".to_owned(),
        ));
    }
    Ok(())
}
