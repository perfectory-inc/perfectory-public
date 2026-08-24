//! Use case for promoting a validated static build to the active serving source.
//!
//! This is the second half of the switch that [`crate::mark_tile_layer_dynamic`] opens. A static
//! `PMTiles` release may only replace a dynamic release built from the same data revision, and the
//! previous dynamic release is preserved as the fallback so
//! [`crate::rollback_tile_layer_source`] has somewhere to return to.
//!
//! What this layer can decide is narrow. The transaction owns every artifact fact, so this layer
//! only normalizes the operator-provided unit and idempotency key.

use std::sync::Arc;

use catalog_domain::{CatalogError, VectorTileRuntimeManifest};

use crate::ports::{CatalogUnitOfWork, PromoteTileLayerStaticCommand};

/// Promotes a validated static build through the Catalog unit of work.
pub struct PromoteTileLayerStatic {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl PromoteTileLayerStatic {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub const fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Promotes the static release described by the command.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when required text is empty or when the transaction refuses the
    /// promotion or its writes fail.
    pub async fn execute(
        &self,
        command: PromoteTileLayerStaticCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.uow
            .promote_tile_layer_static(normalize_command(&command)?)
            .await
    }
}

fn normalize_command(
    command: &PromoteTileLayerStaticCommand,
) -> Result<PromoteTileLayerStaticCommand, CatalogError> {
    let unit_key = normalize_required(&command.unit_key, "unit_key")?;

    Ok(PromoteTileLayerStaticCommand {
        unit_key,
        idempotency_key: normalize_required(&command.idempotency_key, "idempotency_key")?,
        build_job_id: command.build_job_id,
        expected_active_release_id: command.expected_active_release_id,
        expected_serving_generation: command.expected_serving_generation,
        operator_staff_id: command.operator_staff_id,
    })
}

fn normalize_required(raw: &str, field: &str) -> Result<String, CatalogError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(invalid(&format!("{field} must not be empty")));
    }
    Ok(value.to_owned())
}

fn invalid(message: &str) -> CatalogError {
    CatalogError::InvalidVectorTileRuntimeManifest(message.to_owned())
}
