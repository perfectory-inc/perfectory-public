//! Use case for saving an administrator's polygon edit so customers see it before the next bake
//! (ADR-0112).

use std::sync::Arc;

use async_trait::async_trait;
use catalog_domain::{
    check_unit_takes_edits, CatalogError, MapEditError, MapEditGeometry, MapEditOperation,
};
use foundation_shared_kernel::ids::{StaffId, VectorTileReleaseId};

use crate::ports::{
    CatalogRepository, MapEditAppended, MapEditRecord, MapEditStore, MapEditStoreError,
    ServedReleases,
};

/// An edit as the caller sent it.
pub struct AppendMapEditInput {
    /// The publication unit, e.g. `complex`.
    pub unit: String,
    /// The feature id the unit's tiles carry.
    pub feature_id: String,
    /// `upsert` or `delete`.
    pub operation: String,
    /// `GeoJSON` geometry for an upsert.
    pub geometry: Option<serde_json::Value>,
    /// The unit's public properties for an upsert.
    pub properties: Option<serde_json::Value>,
    /// The authorized staff member.
    pub editor: StaffId,
    /// The caller's retry identity.
    pub idempotency_key: String,
}

/// Why an edit was not saved.
#[derive(Debug, thiserror::Error)]
pub enum AppendMapEditError {
    /// The edit failed the domain checks and was never sent to the store.
    #[error(transparent)]
    Invalid(#[from] MapEditError),
    /// The store refused the edit or could not be reached.
    #[error(transparent)]
    Store(#[from] MapEditStoreError),
}

/// Checks an edit's unit, operation and topology, pins it to the release its unit serves, then
/// appends it to the edit store.
pub struct AppendMapEdit {
    store: Arc<dyn MapEditStore>,
    releases: Arc<dyn ServedReleases>,
}

impl AppendMapEdit {
    /// Creates the use case over an edit store and the releases the units serve.
    #[must_use]
    pub fn new(store: Arc<dyn MapEditStore>, releases: Arc<dyn ServedReleases>) -> Self {
        Self { store, releases }
    }

    /// The same use case over another edit store.
    #[must_use]
    pub fn with_store(self, store: Arc<dyn MapEditStore>) -> Self {
        Self { store, ..self }
    }

    /// The same use case over another account of the served releases.
    #[must_use]
    pub fn with_releases(self, releases: Arc<dyn ServedReleases>) -> Self {
        Self { releases, ..self }
    }

    /// Saves one edit.
    ///
    /// # Errors
    ///
    /// Returns [`AppendMapEditError::Invalid`] when the unit takes no edits (root ADR-0621 §1),
    /// when the operation is unknown, when an upsert has no geometry or a delete has one, or when
    /// the geometry is not a valid polygon; the store is not called. Returns
    /// [`AppendMapEditError::Store`] as a conflict when the unit serves no release (an edit is
    /// pinned to one, root ADR-0621 §3), as unavailable when the served release cannot be read,
    /// and when the store refuses or is unreachable.
    pub async fn execute(
        &self,
        input: AppendMapEditInput,
    ) -> Result<MapEditAppended, AppendMapEditError> {
        check_unit_takes_edits(&input.unit)?;
        let operation = MapEditOperation::parse(&input.operation)?;
        let geometry = match (operation, input.geometry, &input.properties) {
            (MapEditOperation::Upsert, Some(geometry), _) => {
                Some(MapEditGeometry::parse(geometry)?)
            }
            (MapEditOperation::Delete, None, None) => None,
            _ => return Err(MapEditError::GeometryDoesNotMatchOperation.into()),
        };
        // Root ADR-0621 §3: the edit is pinned to the release its unit serves now. The writer
        // stamps it, never the client; the Gold build resolves it to the source state.
        let base_release_id = self
            .releases
            .active_release(&input.unit)
            .await
            .map_err(|error| {
                MapEditStoreError::Unavailable(format!("the served release is unreadable: {error}"))
            })?
            .ok_or_else(|| MapEditStoreError::Conflict("no_served_release".to_owned()))?;
        let record = MapEditRecord {
            unit: input.unit,
            feature_id: input.feature_id,
            operation,
            geometry,
            properties: input.properties,
            editor: input.editor,
            idempotency_key: input.idempotency_key,
            base_release_id,
        };
        Ok(self.store.append(&record).await?)
    }
}

/// The served releases as the active runtime manifest names them: the release each unit's tiles
/// come from, which the staff map draws an edit over.
pub struct RuntimeManifestReleases {
    repository: Arc<dyn CatalogRepository>,
}

impl RuntimeManifestReleases {
    /// Reads the releases from this catalog.
    #[must_use]
    pub fn new(repository: Arc<dyn CatalogRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl ServedReleases for RuntimeManifestReleases {
    async fn active_release(
        &self,
        unit: &str,
    ) -> Result<Option<VectorTileReleaseId>, CatalogError> {
        Ok(self
            .repository
            .get_active_vector_tile_runtime_manifest()
            .await?
            .and_then(|manifest| {
                manifest
                    .publication_units
                    .get(unit)
                    .map(|published| published.active_release_id)
            }))
    }
}
