//! Use case for activating a complete dynamic `PostGIS` source for one publication unit.
//!
//! Every publication unit starts here. A static `PMTiles` release may only ever replace a dynamic
//! source built from the same data revision, so this command is the only way a unit enters the v2
//! publication ledger.
//!
//! The division of labour with the unit of work is deliberate. This layer decides everything the
//! command alone determines — text normalisation and the coherence of the observed-state pair. It
//! decides nothing that depends on state it cannot see: the next serving generation, the new
//! release identity, and the state-machine transition all belong to the transaction that holds the
//! row locks.
//!
//! [`RuntimeManifestPublicationCapability`](crate::ports::RuntimeManifestPublicationCapability) is
//! deliberately *not* consulted here. It gates the v2 outbox event, which the transaction writes: a
//! deployment with publication off still records the activation in the internal ledger and simply
//! emits no public v2 event, so v1 behaviour stays byte-identical while v2 rolls out. Refusing the
//! activation at this layer would make that impossible, and would answer one deployment decision in
//! a second place — the thing an injected capability type exists to prevent.
//!
//! There is no separate input struct. The sibling use cases have one because their transport input
//! is loose text that they parse into a command; this command is already expressed in validated
//! domain types, so a second identical struct would only add a copy.

use std::{collections::BTreeMap, sync::Arc};

use catalog_domain::{CatalogError, RuntimeTileLayer};

use crate::ports::{CatalogUnitOfWork, MarkTileLayerDynamicCommand, PublishedRuntimeManifest};

/// Activates a complete dynamic `PostGIS` source through the Catalog unit of work.
pub struct MarkTileLayerDynamic {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl MarkTileLayerDynamic {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub const fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Activates the complete dynamic source described by the command.
    ///
    /// The outcome says whether this call published the manifest or the idempotency ledger replayed a
    /// prior one, because a replayed manifest can carry an older `manifest_generation` than the
    /// pointer's and callers compare generations to decide what to refetch.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when required text is empty, when the observed release and generation
    /// are not both present or both absent, when the layer set is empty or names one layer twice, when
    /// the idempotency key was already used for a different request, or when the transaction refuses
    /// the transition or its writes fail.
    pub async fn execute(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<PublishedRuntimeManifest, CatalogError> {
        self.uow
            .mark_tile_layer_dynamic(normalize_command(command)?)
            .await
    }
}

/// Trims the command's free text and refuses the shapes that describe no possible state.
///
/// Normalisation happens once, here, so the transaction never has to decide whether a stored
/// `" parcels "` and an incoming `"parcels"` name the same unit.
fn normalize_command(
    command: MarkTileLayerDynamicCommand,
) -> Result<MarkTileLayerDynamicCommand, CatalogError> {
    // Both absent is a first publication; both present is a switch. One of each would have the
    // transaction guess which half to trust, so it is refused before any lock is taken.
    if command.expected_active_release_id.is_some() != command.expected_serving_generation.is_some()
    {
        return Err(invalid(
            "expected_active_release_id and expected_serving_generation must both be present or both be absent",
        ));
    }
    if command.layers.is_empty() {
        return Err(invalid("layers must not be empty"));
    }

    let martin_source_id = normalize_required(&command.martin_source_id, "martin_source_id")?;
    // Martin resolves a tile request by the source id in the path. A template ending in some other
    // route would publish a URL serving a different source than the pointer claims, and no later
    // check on either value alone would notice.
    let expected_route = format!("/{martin_source_id}/{{z}}/{{x}}/{{y}}");
    if !command
        .tiles_url_template
        .as_str()
        .ends_with(&expected_route)
    {
        return Err(invalid(&format!(
            "tiles_url_template must address martin_source_id: expected it to end with {expected_route}"
        )));
    }

    Ok(MarkTileLayerDynamicCommand {
        unit_key: normalize_required(&command.unit_key, "unit_key")?,
        martin_source_id,
        layers: normalize_layers(command.layers)?,
        idempotency_key: normalize_required(&command.idempotency_key, "idempotency_key")?,
        ..command
    })
}

/// Trims layer names, rejecting a set whose names collide only after trimming.
///
/// Collecting into a map would silently keep one of `"parcels"` and `" parcels"` and drop the
/// other, publishing fewer layers than the caller asked for. A caller that names one layer twice is
/// describing two different layer definitions for one name, so the ambiguity is refused instead.
fn normalize_layers(
    layers: BTreeMap<String, RuntimeTileLayer>,
) -> Result<BTreeMap<String, RuntimeTileLayer>, CatalogError> {
    let declared = layers.len();
    let normalized = layers
        .into_iter()
        .map(|(layer_key, layer)| {
            Ok((
                normalize_required(&layer_key, "layer name")?,
                RuntimeTileLayer {
                    source_layer: normalize_required(&layer.source_layer, "source_layer")?,
                    ..layer
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, CatalogError>>()?;
    if normalized.len() != declared {
        return Err(invalid("layer names must be unique after trimming"));
    }
    Ok(normalized)
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
