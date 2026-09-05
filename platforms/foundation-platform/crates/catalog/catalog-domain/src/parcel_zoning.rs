//! Per-parcel land-use zoning verdicts (root ADR-0083 §5).
//!
//! One value is one (parcel, zone) row the projection loader accepted: the zone code reached
//! an anchor in the LMIS code tree and the designation was 포함 or 저촉. The vocabulary
//! translation (anchor → a product's zoning enum) is deliberately not here — foundation
//! carries the anchor code and the consumer decides what it means.

/// One land-use zoning verdict for a parcel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParcelZoning {
    /// LMIS zone code exactly as the plan ledger stated it (e.g. `UQA320`).
    pub zone_code: String,
    /// Korean zone name the source shipped, untranslated. Absent when the source was blank.
    pub zone_name: Option<String>,
    /// The code-tree anchor the zone resolved to (one of the ADR-0083 §4 anchor set).
    pub anchor_code: String,
    /// `1` 포함 or `2` 저촉 — 접함 never enters the projection.
    pub inclusion_code: String,
    /// The land-use plan vintage this verdict was projected from.
    pub source_snapshot_id: String,
}
