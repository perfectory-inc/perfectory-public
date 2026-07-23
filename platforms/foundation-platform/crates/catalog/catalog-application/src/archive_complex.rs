//! Use case for archiving an industrial complex without hard deletion.

use std::sync::Arc;

use catalog_domain::{CatalogError, IndustrialComplex};
use foundation_shared_kernel::ids::{ComplexId, StaffId};

use crate::ports::CatalogUnitOfWork;

/// Input required to archive a canonical industrial complex with optimistic concurrency.
pub struct ArchiveIndustrialComplexInput {
    /// Industrial complex that should be archived.
    pub complex_id: ComplexId,
    /// Version observed by the caller.
    pub expected_version: i64,
    /// Staff operator requesting the archive.
    pub operator_staff_id: StaffId,
    /// Optional human-readable archive reason.
    pub reason: Option<String>,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
}

/// Archives an industrial complex through the Catalog unit of work.
pub struct ArchiveIndustrialComplex {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl ArchiveIndustrialComplex {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Archives a complex and records the matching outbox event.
    ///
    /// # Errors
    /// Returns `CatalogError` when the request is invalid, stale, missing, already archived, or
    /// persistence fails.
    pub async fn execute(
        &self,
        input: ArchiveIndustrialComplexInput,
    ) -> Result<IndustrialComplex, CatalogError> {
        validate_archive_input(&input)?;
        self.uow
            .archive_complex(
                input.complex_id,
                input.expected_version,
                input.operator_staff_id,
                input.reason.map(|reason| reason.trim().to_owned()),
                input.request_id,
            )
            .await
    }
}

fn validate_archive_input(input: &ArchiveIndustrialComplexInput) -> Result<(), CatalogError> {
    if input.expected_version < 1 {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "expected_version must be positive".to_owned(),
        ));
    }
    if input
        .reason
        .as_deref()
        .is_some_and(|reason| reason.trim().is_empty())
    {
        return Err(CatalogError::InvalidIndustrialComplexInput(
            "archive reason must not be blank".to_owned(),
        ));
    }
    Ok(())
}
