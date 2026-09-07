//! Per-parcel forest-register facts from the `AL_D003` CSV ledger.

/// One forest parcel's newest official ledger row.
#[derive(Clone, Debug, PartialEq)]
pub struct ParcelForestLedger {
    /// Parcel Number Unit identifier.
    pub pnu: String,
    /// Cadastral land category name exactly as the provider wrote it.
    pub land_category: Option<String>,
    /// Official ledger area in square meters.
    pub area_m2: Option<f64>,
    /// Provider ownership category code, not an owner identity.
    pub ownership_kind: Option<String>,
    /// Number of co-owners recorded by the provider.
    pub co_owner_count: Option<i32>,
    /// Source snapshot identifier carried by the handoff.
    pub source_snapshot_id: String,
}
