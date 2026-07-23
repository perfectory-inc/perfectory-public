//! Parcel aggregate and parcel-kind wire classification.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::events::catalog_v1::ParcelKindChangedV1;
use foundation_shared_kernel::ids::{ComplexId, ParcelId};
use foundation_shared_kernel::pnu::Pnu;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical parcel kind inside an industrial complex.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ParcelKind {
    /// Factory parcel.
    Factory,
    /// Support facility parcel.
    Support,
    /// Public facility parcel.
    Public,
    /// River or water surface parcel.
    River,
    /// Other or unknown parcel kind.
    Other,
}

impl ParcelKind {
    /// Returns the stable wire value used by DB rows, outbox payloads, and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Factory => "factory",
            Self::Support => "support",
            Self::Public => "public",
            Self::River => "river",
            Self::Other => "other",
        }
    }

    /// Parses a stable wire value into a domain parcel kind.
    ///
    /// # Errors
    /// Returns `ParseParcelKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseParcelKindError> {
        match raw {
            "factory" => Ok(Self::Factory),
            "support" => Ok(Self::Support),
            "public" => Ok(Self::Public),
            "river" => Ok(Self::River),
            "other" => Ok(Self::Other),
            other => Err(ParseParcelKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing a parcel kind.
#[derive(Debug, Error)]
pub enum ParseParcelKindError {
    /// Unsupported wire value.
    #[error("unknown ParcelKind wire value: {0:?}")]
    Unknown(String),
}

/// Canonical parcel aggregate root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Parcel {
    /// Stable foundation-platform parcel identifier.
    pub id: ParcelId,
    /// Industrial complex that owns the parcel.
    pub complex_id: ComplexId,
    /// Canonical 19-digit parcel identifier.
    pub pnu: Pnu,
    /// Canonical parcel kind.
    pub kind: ParcelKind,
    /// Official parcel area in square meters.
    pub area_m2: u64,
    /// UTC timestamp when the parcel was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the parcel was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

impl Parcel {
    /// Builds the outbox payload for a parcel-kind change.
    #[must_use]
    pub fn kind_changed_event(&self, new_kind: ParcelKind) -> ParcelKindChangedV1 {
        ParcelKindChangedV1 {
            schema_version: 1,
            parcel_id: self.id,
            pnu: self.pnu.clone(),
            previous_kind: self.kind.wire_name().to_owned(),
            new_kind: new_kind.wire_name().to_owned(),
            changed_at: Utc::now(),
        }
    }
}
