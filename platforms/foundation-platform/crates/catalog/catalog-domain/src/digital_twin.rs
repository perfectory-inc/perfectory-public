//! Digital twin and 3D visualization assets attached to Catalog resources.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{
    BuildingId, ComplexId, DigitalTwinAssetId, FileAssetId, ParcelId, SourceRecordId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Canonical digital twin asset classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DigitalTwinAssetKind {
    /// Standalone 3D model.
    Model3d,
    /// 3D tileset.
    Tileset3d,
    /// Point cloud artifact.
    PointCloud,
    /// Panorama media.
    Panorama,
    /// Other or unknown digital twin asset kind.
    Other,
}

impl DigitalTwinAssetKind {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Model3d => "model_3d",
            Self::Tileset3d => "tileset_3d",
            Self::PointCloud => "point_cloud",
            Self::Panorama => "panorama",
            Self::Other => "other",
        }
    }

    /// Parses a stable wire value into a domain asset kind.
    ///
    /// # Errors
    /// Returns `ParseDigitalTwinAssetKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseDigitalTwinAssetKindError> {
        match raw {
            "model_3d" => Ok(Self::Model3d),
            "tileset_3d" => Ok(Self::Tileset3d),
            "point_cloud" => Ok(Self::PointCloud),
            "panorama" => Ok(Self::Panorama),
            "other" => Ok(Self::Other),
            other => Err(ParseDigitalTwinAssetKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing a digital twin asset kind.
#[derive(Debug, Error)]
pub enum ParseDigitalTwinAssetKindError {
    /// Unsupported wire value.
    #[error("unknown DigitalTwinAssetKind wire value: {0:?}")]
    Unknown(String),
}

/// Digital twin asset metadata assigned below an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DigitalTwinAsset {
    /// Stable foundation-platform digital twin asset identifier.
    pub id: DigitalTwinAssetId,
    /// Industrial complex that owns this asset.
    pub complex_id: ComplexId,
    /// Optional parcel that narrows the asset scope.
    pub parcel_id: Option<ParcelId>,
    /// Optional building represented by the asset.
    pub building_id: Option<BuildingId>,
    /// File asset that stores the digital twin artifact.
    pub file_asset_id: FileAssetId,
    /// Canonical digital twin asset kind.
    pub asset_kind: DigitalTwinAssetKind,
    /// Optional coordinate transform payload for renderer alignment.
    pub coordinate_transform: Option<Value>,
    /// Optional source record that produced this asset.
    pub source_record_id: Option<SourceRecordId>,
    /// UTC timestamp when the asset was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the asset was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
