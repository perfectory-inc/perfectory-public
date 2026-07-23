//! Industrial-complex blueprint and drawing metadata.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{BlueprintId, ComplexId, FileAssetId, SourceRecordId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical blueprint classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BlueprintKind {
    /// Complex master plan.
    MasterPlan,
    /// Parcel map.
    ParcelMap,
    /// Utility plan.
    UtilityPlan,
    /// Building or floor plan.
    FloorPlan,
    /// Other or unknown blueprint kind.
    Other,
}

impl BlueprintKind {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::MasterPlan => "master_plan",
            Self::ParcelMap => "parcel_map",
            Self::UtilityPlan => "utility_plan",
            Self::FloorPlan => "floor_plan",
            Self::Other => "other",
        }
    }

    /// Parses a stable wire value into a domain blueprint kind.
    ///
    /// # Errors
    /// Returns `ParseBlueprintKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseBlueprintKindError> {
        match raw {
            "master_plan" => Ok(Self::MasterPlan),
            "parcel_map" => Ok(Self::ParcelMap),
            "utility_plan" => Ok(Self::UtilityPlan),
            "floor_plan" => Ok(Self::FloorPlan),
            "other" => Ok(Self::Other),
            other => Err(ParseBlueprintKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing a blueprint kind.
#[derive(Debug, Error)]
pub enum ParseBlueprintKindError {
    /// Unsupported wire value.
    #[error("unknown BlueprintKind wire value: {0:?}")]
    Unknown(String),
}

/// Blueprint or drawing metadata assigned below an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Blueprint {
    /// Stable foundation-platform blueprint identifier.
    pub id: BlueprintId,
    /// Industrial complex that owns this blueprint.
    pub complex_id: ComplexId,
    /// File asset that stores the blueprint object.
    pub file_asset_id: FileAssetId,
    /// Canonical blueprint kind.
    pub blueprint_kind: BlueprintKind,
    /// Coordinate reference system used by the drawing.
    pub coordinate_system: String,
    /// Optional source-provided scale label.
    pub scale: Option<String>,
    /// Optional source record that produced this blueprint.
    pub source_record_id: Option<SourceRecordId>,
    /// UTC timestamp when the blueprint was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the blueprint was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
