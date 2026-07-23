//! Use case for changing a parcel kind inside Catalog.

use std::sync::Arc;

use catalog_domain::{CatalogError, Parcel, ParcelKind};
use foundation_shared_kernel::ids::ParcelId;

use crate::ports::CatalogUnitOfWork;

/// Input required to update a parcel kind with optimistic concurrency.
pub struct UpdateParcelKindInput {
    /// Parcel that should be updated.
    pub parcel_id: ParcelId,
    /// Version observed by the caller.
    pub expected_version: i64,
    /// New domain-level parcel kind.
    pub new_kind: ParcelKind,
}

/// Updates a parcel kind through the Catalog unit of work.
pub struct UpdateParcelKind {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl UpdateParcelKind {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Changes a parcel kind and records the matching Catalog outbox event atomically.
    ///
    /// # Errors
    /// Returns `CatalogError` when the parcel is missing, the expected version is stale, or
    /// persistence fails.
    pub async fn execute(&self, input: UpdateParcelKindInput) -> Result<Parcel, CatalogError> {
        self.uow
            .update_parcel_kind(input.parcel_id, input.expected_version, input.new_kind)
            .await
    }
}
