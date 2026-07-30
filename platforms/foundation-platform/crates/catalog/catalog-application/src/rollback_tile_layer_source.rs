//! Use case for returning one publication unit to its preserved dynamic release.
//!
//! A rollback undoes a *source switch*, not a publication: the data revision does not change, so the
//! unit keeps serving the same content from `PostGIS` instead of from `PMTiles`. That is why the
//! command names no target release — the unit records exactly one fallback, and the database
//! constrains its revision to equal the active one.
//!
//! This is the counterpart to [`crate::promote_tile_layer_static`], which is what puts the fallback
//! there.

use std::sync::Arc;

use catalog_domain::{CatalogError, VectorTileRuntimeManifest};

use crate::ports::{CatalogUnitOfWork, RollbackTileLayerSourceCommand};

/// Rolls one publication unit back to its fallback through the Catalog unit of work.
pub struct RollbackTileLayerSource {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl RollbackTileLayerSource {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub const fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Returns the unit to its recorded same-revision fallback.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when required text is empty, or when the transaction finds the observed
    /// state stale, finds no recorded fallback, or its writes fail.
    pub async fn execute(
        &self,
        command: RollbackTileLayerSourceCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.uow
            .rollback_tile_layer_source(RollbackTileLayerSourceCommand {
                unit_key: normalize_required(&command.unit_key, "unit_key")?,
                // A rollback is the one operation an operator reads about after the fact, in an
                // incident. A blank reason records that serving changed while withholding why.
                reason: normalize_required(&command.reason, "reason")?,
                idempotency_key: normalize_required(&command.idempotency_key, "idempotency_key")?,
                ..command
            })
            .await
    }
}

fn normalize_required(raw: &str, field: &str) -> Result<String, CatalogError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(CatalogError::InvalidVectorTileRuntimeManifest(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value.to_owned())
}
