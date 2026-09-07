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
    /// Building dong name, unchanged.
    pub dong_name: Option<String>,
    /// Floor name, unchanged.
    pub floor_name: Option<String>,
    /// Ho name, unchanged.
    pub ho_name: Option<String>,
    /// Room name, unchanged.
    pub room_name: Option<String>,
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
