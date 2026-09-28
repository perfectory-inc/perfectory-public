//! Use case for saving an administrator's polygon edit so customers see it before the next bake
//! (ADR-0112).

use std::sync::Arc;

use catalog_domain::{MapEditError, MapEditGeometry, MapEditOperation};
use foundation_shared_kernel::ids::StaffId;

use crate::ports::{MapEditAppended, MapEditRecord, MapEditStore, MapEditStoreError};

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

/// Checks an edit's operation and topology, then appends it to the edit store.
pub struct AppendMapEdit {
    store: Arc<dyn MapEditStore>,
}

impl AppendMapEdit {
    /// Creates the use case over an edit store.
    #[must_use]
    pub fn new(store: Arc<dyn MapEditStore>) -> Self {
        Self { store }
    }

    /// Saves one edit.
    ///
    /// # Errors
    ///
    /// Returns [`AppendMapEditError::Invalid`] when the operation is unknown, when an upsert has
    /// no geometry or a delete has one, or when the geometry is not a valid polygon; the store is
    /// not called. Returns [`AppendMapEditError::Store`] when the store refuses or is unreachable.
    pub async fn execute(
        &self,
        input: AppendMapEditInput,
    ) -> Result<MapEditAppended, AppendMapEditError> {
        let operation = MapEditOperation::parse(&input.operation)?;
        let geometry = match (operation, input.geometry, &input.properties) {
            (MapEditOperation::Upsert, Some(geometry), _) => {
                Some(MapEditGeometry::parse(geometry)?)
            }
            (MapEditOperation::Delete, None, None) => None,
            _ => return Err(MapEditError::GeometryDoesNotMatchOperation.into()),
        };
        let record = MapEditRecord {
            unit: input.unit,
            feature_id: input.feature_id,
            operation,
            geometry,
            properties: input.properties,
            editor: input.editor,
            idempotency_key: input.idempotency_key,
        };
        Ok(self.store.append(&record).await?)
    }
}
