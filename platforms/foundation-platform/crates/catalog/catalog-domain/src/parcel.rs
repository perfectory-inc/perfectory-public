//! Parcel aggregate and parcel-kind wire classification.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::events::catalog_v1::{ParcelKindAssignedV1, ParcelKindChangedV1};
use foundation_shared_kernel::ids::ParcelId;
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
    /// Canonical 19-digit parcel identifier.
    pub pnu: Pnu,
    /// Land use inside an industrial complex, once a person has decided it.
    ///
    /// `None` until then, which is the state every loaded parcel starts in: the vocabulary
    /// describes use *inside* a complex and most parcels belong to none, and the only writer is a
    /// staff edit attributed in the ledger (root ADR-0070). Required here, it kept the table empty
    /// while 39,861,511 parcels sat in canonical Silver.
    pub kind: Option<ParcelKind>,
    /// Official parcel area in square meters, from the cadastral record.
    ///
    /// `None` until a source that carries it is collected. The boundary source carries `PNU` and
    /// `JIBUN` only; the polygon can be measured but a measured area is a different fact from the
    /// official one and does not belong in this field (root ADR-0020, ADR-0070).
    pub area_m2: Option<u64>,
    /// UTC timestamp when the parcel was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the parcel was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

/// What a staff edit to a parcel's kind amounts to.
///
/// A parcel that had no kind is not changing one, and `catalog.parcel.kind_changed.v1` carries
/// `previous_kind` as a required field — a value that does not exist for the first assignment.
/// Rather than widen that field and break every reader of v1, the two facts get their own events.
#[derive(Clone, Debug)]
pub enum ParcelKindEdit {
    /// Nobody had decided a kind before; this edit is the first one.
    Assigned(ParcelKindAssignedV1),
    /// A kind was already decided and this edit replaced it.
    Changed(ParcelKindChangedV1),
}

impl Parcel {
    /// Builds the outbox payload for a staff edit to this parcel's kind.
    #[must_use]
    pub fn kind_edit_event(&self, new_kind: ParcelKind) -> ParcelKindEdit {
        self.kind.map_or_else(
            || {
                ParcelKindEdit::Assigned(ParcelKindAssignedV1 {
                    schema_version: 1,
                    parcel_id: self.id,
                    pnu: self.pnu.clone(),
                    assigned_kind: new_kind.wire_name().to_owned(),
                    assigned_at: Utc::now(),
                })
            },
            |previous| {
                ParcelKindEdit::Changed(ParcelKindChangedV1 {
                    schema_version: 1,
                    parcel_id: self.id,
                    pnu: self.pnu.clone(),
                    previous_kind: previous.wire_name().to_owned(),
                    new_kind: new_kind.wire_name().to_owned(),
                    changed_at: Utc::now(),
                })
            },
        )
    }
}
