//! Spatial layers attached to an industrial complex, parcel, or blueprint.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{
    BlueprintId, ComplexId, ParcelId, SourceRecordId, SpatialLayerId,
};
use foundation_shared_kernel::ObjectKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical spatial layer classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SpatialLayerKind {
    /// Industrial complex boundary.
    ComplexBoundary,
    /// Parcel boundary.
    ParcelBoundary,
    /// Zoning layer.
    Zone,
    /// Road network layer.
    Road,
    /// Utility network layer.
    Utility,
    /// Georeferenced blueprint overlay.
    BlueprintOverlay,
    /// Other or unknown spatial layer kind.
    Other,
}

impl SpatialLayerKind {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::ComplexBoundary => "complex_boundary",
            Self::ParcelBoundary => "parcel_boundary",
            Self::Zone => "zone",
            Self::Road => "road",
            Self::Utility => "utility",
            Self::BlueprintOverlay => "blueprint_overlay",
            Self::Other => "other",
        }
    }

    /// Parses a stable wire value into a domain layer kind.
    ///
    /// # Errors
    /// Returns `ParseSpatialLayerKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseSpatialLayerKindError> {
        match raw {
            "complex_boundary" => Ok(Self::ComplexBoundary),
            "parcel_boundary" => Ok(Self::ParcelBoundary),
            "zone" => Ok(Self::Zone),
            "road" => Ok(Self::Road),
            "utility" => Ok(Self::Utility),
            "blueprint_overlay" => Ok(Self::BlueprintOverlay),
            "other" => Ok(Self::Other),
            other => Err(ParseSpatialLayerKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing a spatial layer kind.
#[derive(Debug, Error)]
pub enum ParseSpatialLayerKindError {
    /// Unsupported wire value.
    #[error("unknown SpatialLayerKind wire value: {0:?}")]
    Unknown(String),
}

/// Geospatial layer metadata owned by Catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpatialLayer {
    /// Stable foundation-platform spatial layer identifier.
    pub id: SpatialLayerId,
    /// Industrial complex that owns this layer.
    pub complex_id: ComplexId,
    /// Optional parcel that narrows the layer scope.
    pub parcel_id: Option<ParcelId>,
    /// Optional blueprint that the layer overlays.
    pub blueprint_id: Option<BlueprintId>,
    /// Canonical spatial layer kind.
    pub layer_kind: SpatialLayerKind,
    /// Optional object key for the geometry artifact.
    pub geometry_object_key: Option<ObjectKey>,
    /// Optional source record that produced this layer.
    pub source_record_id: Option<SourceRecordId>,
    /// UTC timestamp when the layer was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the layer was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
