//! Public baked building panel shapes (root ADR-0100).
//! The Catalog runtime timestamp is intentionally absent; units reuse the canonical DTO.
use crate::catalog::UnitResponse;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Wire version of one baked building panel.
pub const BUILDING_DOCUMENT_SCHEMA_VERSION: &str = "foundation-platform.building_by_pnu_profile.v2";

/// Canonical building response.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct BuildingPanelBuilding {
    /// Stable foundation-platform identifier for the building.
    pub id: Uuid,
    /// Parcel that owns this building.
    pub parcel_id: Uuid,
    /// Natural key: the title-register record (관리대장 PK) this building was loaded from —
    /// the name a person calls the building by (ADR-0076 §2).
    pub register_pk: String,
    /// Source building purpose code. `null` when the register stated nothing (ADR-0076 §1).
    pub purpose_code: Option<String>,
    /// Source building structure code. `null` when the register stated nothing.
    pub structure_code: Option<String>,
    /// Official floor area (연면적) in square meters. `null` when the register wrote 0 or
    /// nothing.
    pub floor_area_m2: Option<f64>,
    /// Number of above-ground stories. `null` when the register stated nothing.
    pub stories: Option<i16>,
    /// Number of below-ground (basement) floors. `0` when none or unknown.
    pub below_ground_floors: i16,
    /// Whether the building has a rooftop (옥탑) structure counted as a floor.
    pub has_rooftop: bool,
    /// 옥탑 공용부 allocated area (㎡) reconciled from 전유공용면적. `null` when the
    /// building has no rooftop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rooftop_area_m2: Option<f64>,
    /// 옥탑 용도 (주용도 · 기타용도) reconciled from 전유공용면적. Empty when the
    /// building has no rooftop.
    pub rooftop_usage: String,
    /// Construction-approval year from the official source. `null` when the register states
    /// no plausible date — never fabricated.
    pub built_year: Option<i32>,
    /// Current floor rows, including explicitly unresolved floor classifications.
    pub floors: Vec<BuildingPanelFloor>,
    /// Units whose register link resolves to this building.
    pub units: Vec<UnitResponse>,
}

/// A current floor-register row; unknown classifications remain visible.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct BuildingPanelFloor {
    /// Stable Silver row identity.
    pub floor_row_id: String,
    /// Normalized Silver classification, including unknown or special floors.
    pub floor_kind: String,
    /// Number within the classified floor kind, absent when unresolved.
    pub floor_number: Option<u16>,
    /// Signed floor index from normalization, absent when unresolved.
    pub floor_index: Option<i32>,
    /// Normalized source display label, absent when unresolved.
    pub floor_display_ko: Option<String>,
}
