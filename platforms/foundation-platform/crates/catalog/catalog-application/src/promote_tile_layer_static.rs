//! Use case for promoting a validated static build to the active serving source.
//!
//! This is the second half of the switch that [`crate::mark_tile_layer_dynamic`] opens. A static
//! `PMTiles` release may only replace a dynamic release built from the same data revision, and the
//! previous dynamic release is preserved as the fallback so
//! [`crate::rollback_tile_layer_source`] has somewhere to return to.
//!
//! What this layer can decide is narrow. Whether the build is still promotable needs the unit's
//! current release, which only the transaction holds under lock; what it *can* decide is that the
//! command's own fields agree with each other — that the published URL actually addresses the
//! release-derived Martin source, and that the artifact is not empty.

use std::sync::Arc;

use catalog_domain::{static_release_martin_source_id, CatalogError, VectorTileRuntimeManifest};

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
    /// Returns `CatalogError` when required text is empty, when the artifact is empty, when the URL
    /// template does not address the release-derived Martin source, or when the transaction refuses
    /// the promotion or its writes fail.
    pub async fn execute(
        &self,
        command: PromoteTileLayerStaticCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        self.uow
            .promote_tile_layer_static(normalize_command(command)?)
            .await
    }
}

fn normalize_command(
    command: PromoteTileLayerStaticCommand,
) -> Result<PromoteTileLayerStaticCommand, CatalogError> {
    let unit_key = normalize_required(&command.unit_key, "unit_key")?;

    // A promotion that replaces the release it observed is not a switch. The transaction would find
    // the generation already advanced past it, but the caller learns nothing from a version conflict
    // about having asked for a no-op.
    if command.release_id == command.expected_active_release_id {
        return Err(invalid(
            "release_id must differ from expected_active_release_id",
        ));
    }
    // The database refuses a zero-byte PMTiles object. Refusing it here names the field instead of
    // surfacing a constraint violation.
    if command.pmtiles_bytes == 0 {
        return Err(invalid("pmtiles_bytes must be greater than zero"));
    }

    // Martin resolves a tile request by the source id in the path, and for a static release that id
    // is derived from the unit and the release. A template addressing anything else would publish a
    // URL that serves a different release than the pointer claims.
    let martin_source_id = static_release_martin_source_id(&unit_key, command.release_id);
    let expected_route = format!("/{martin_source_id}/{{z}}/{{x}}/{{y}}");
    if !command
        .tiles_url_template
        .as_str()
        .ends_with(&expected_route)
    {
        return Err(invalid(&format!(
            "tiles_url_template must address the release-derived Martin source: expected it to end with {expected_route}"
        )));
    }

    Ok(PromoteTileLayerStaticCommand {
        unit_key,
        idempotency_key: normalize_required(&command.idempotency_key, "idempotency_key")?,
        ..command
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
