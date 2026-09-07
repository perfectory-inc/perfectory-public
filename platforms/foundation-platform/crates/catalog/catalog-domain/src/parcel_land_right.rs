//! Registered unit-level land rights attached to a parcel.

use chrono::{DateTime, Utc};

/// One provider row from the land-right registration ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParcelLandRight {
    /// Parcel Number Unit identifier.
    pub pnu: String,
    /// Provider serial number, preserved as text including leading zeroes.
    pub right_serial_no: String,
    /// Building name, unchanged.
    pub building_name: Option<String>,
    /// Building dong designation; empty when the provider row leaves it blank (root ADR-0093).
    pub dong_name: String,
    /// Floor designation; empty when the provider row leaves it blank.
    pub floor_name: String,
    /// Ho designation; empty when the provider row leaves it blank.
    pub ho_name: String,
    /// Room designation; empty when the provider row leaves it blank.
    pub room_name: String,
    /// Provider land-right ratio, unchanged.
    pub right_ratio: Option<String>,
    /// Provider closure kind name, unchanged.
    pub closure_kind: Option<String>,
    /// Provider closure kind code, unchanged.
    pub closure_kind_code: Option<String>,
    /// Source snapshot identifier carried by the handoff.
    pub source_snapshot_id: String,
    /// UTC timestamp when this row entered the serving catalog.
    pub loaded_at: DateTime<Utc>,
}

/// A bounded first page of one parcel's land rights plus the full match count.
///
/// Apartment parcels can register ten thousand unit rights (root ADR-0093), so
/// reads return a deterministic first page and report how many rows exist.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParcelLandRightPage {
    /// First page in serial → dong → floor → ho → room order.
    pub rights: Vec<ParcelLandRight>,
    /// Total registered rows for the parcel, independent of the page bound.
    pub total: u64,
}
