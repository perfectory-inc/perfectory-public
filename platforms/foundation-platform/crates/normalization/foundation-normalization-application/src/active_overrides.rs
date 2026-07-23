//! Read model for active building-register-unit overrides.

use serde_json::Value as JsonValue;
use uuid::Uuid;

/// Active application id and opaque persisted snapshot consumed by Lakehouse materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveBuildingRegisterUnitOverride {
    /// Durable Normalization application id.
    pub application_id: Uuid,
    /// Opaque application snapshot. Interpretation remains Lakehouse-owned.
    pub snapshot: JsonValue,
}
