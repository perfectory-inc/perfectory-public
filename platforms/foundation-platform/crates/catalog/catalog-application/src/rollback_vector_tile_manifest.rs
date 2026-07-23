//! Use case for manually rolling back the active static vector tile manifest.

use std::sync::Arc;

use catalog_domain::{CatalogError, VectorTileManifest};
use foundation_shared_kernel::ids::StaffId;

use crate::ports::{CatalogUnitOfWork, VectorTileManifestRollbackCommand};

/// Input required to roll back the active vector tile manifest pointer.
pub struct RollbackVectorTileManifestInput {
    /// Version that should become active after rollback.
    pub to_version: String,
    /// Active version observed by the caller before rollback.
    pub expected_current_version: String,
    /// Human-readable rollback reason persisted for audit.
    pub reason: String,
    /// Staff operator that requested the rollback.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
}

/// Rolls back the active vector tile manifest through the Catalog unit of work.
pub struct RollbackVectorTileManifest {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl RollbackVectorTileManifest {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Switches the active manifest to an existing immutable version.
    ///
    /// # Errors
    /// Returns `CatalogError` when required operator input is empty, the target version does not
    /// exist, the expected version is stale, or persistence/outbox writes fail.
    pub async fn execute(
        &self,
        input: RollbackVectorTileManifestInput,
    ) -> Result<VectorTileManifest, CatalogError> {
        let to_version = input.to_version.trim();
        if to_version.is_empty() {
            return Err(CatalogError::InvalidVectorTileManifestRollback(
                "to_version must not be empty".to_owned(),
            ));
        }

        let expected_current_version = input.expected_current_version.trim();
        if expected_current_version.is_empty() {
            return Err(CatalogError::InvalidVectorTileManifestRollback(
                "expected_current_version must not be empty".to_owned(),
            ));
        }

        let reason = input.reason.trim();
        if reason.is_empty() {
            return Err(CatalogError::InvalidVectorTileManifestRollback(
                "reason must not be empty".to_owned(),
            ));
        }

        self.uow
            .rollback_vector_tile_manifest(VectorTileManifestRollbackCommand {
                to_version: to_version.to_owned(),
                expected_current_version: expected_current_version.to_owned(),
                reason: reason.to_owned(),
                operator_staff_id: input.operator_staff_id,
                request_id: normalize_optional_text(input.request_id),
            })
            .await
    }
}

fn normalize_optional_text(raw: Option<String>) -> Option<String> {
    raw.map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
