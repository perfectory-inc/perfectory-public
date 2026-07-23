//! Building metadata assigned to parcels.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{BuildingId, ParcelId};
use serde::{Deserialize, Serialize};

/// Building entity imported from official building sources.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Building {
    /// Stable foundation-platform building identifier.
    pub id: BuildingId,
    /// Parcel that owns this building.
    pub parcel_id: ParcelId,
    /// Source purpose code.
    pub purpose_code: String,
    /// Source structure code.
    pub structure_code: String,
    /// Official floor area in square meters.
    pub floor_area_m2: f64,
    /// Number of above-ground stories when available from source.
    pub stories: i16,
    /// Number of below-ground (basement) floors, aggregated from the cleaned
    /// building-register floor Silver promotion. `0` when none or unknown.
    pub below_ground_floors: i16,
    /// Whether the building has a rooftop (옥탑) structure counted as a floor,
    /// derived from the cleaned building-register floor classification.
    pub has_rooftop: bool,
    /// 옥탑 공용부 allocated area (㎡) reconciled from 전유공용면적, when the
    /// building has a rooftop. `None` when there is no rooftop or it is unknown.
    pub rooftop_area_m2: Option<f64>,
    /// 옥탑 용도 (주용도 · 기타용도) reconciled from 전유공용면적. Empty when the
    /// building has no rooftop.
    pub rooftop_usage: String,
    /// Construction year from official source.
    pub built_year: i32,
    /// UTC timestamp when the building record was last updated.
    pub updated_at: DateTime<Utc>,
}
