//! Narrow parcel information needed for Gongzzang listing denormalization.
//!
//! This is not the canonical Catalog parcel aggregate. Canonical parcel facts
//! live in Foundation Platform; this struct is the Gongzzang-owned projection shape
//! consumed by listing creation and the parcel summary API.

use serde::{Deserialize, Serialize};
use shared_kernel::admin_division::AdminDivision;
use shared_kernel::land_use_type::LandUseType;
use shared_kernel::money::MoneyKrw;
use shared_kernel::zoning::Zoning;

/// Official land price notice year/month lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GosiYearMonth {
    /// Four-digit notice year.
    pub year: u16,
    /// Notice month, from 1 to 12.
    pub month: u8,
}

/// Raw cadastral characteristics published by Foundation Platform.
///
/// Gongzzang carries these source facts without inventing display labels or storing another
/// canonical copy.
#[derive(Debug, Clone, PartialEq)]
pub struct ParcelCharacteristics {
    /// Cadastral land category name exactly as the source wrote it.
    pub land_category: Option<String>,
    /// Official cadastral area in square meters.
    pub area_m2: f64,
    /// Land-use situation name exactly as the source wrote it.
    pub land_use_situation: Option<String>,
    /// Terrain height name exactly as the source wrote it.
    pub terrain_height: Option<String>,
    /// Terrain shape name exactly as the source wrote it.
    pub terrain_shape: Option<String>,
    /// Road-contact name exactly as the source wrote it.
    pub road_contact: Option<String>,
}

/// Raw forest-register facts published by Foundation Platform.
#[derive(Debug, Clone, PartialEq)]
pub struct ParcelForestLedger {
    /// Cadastral land category name exactly as the provider wrote it.
    pub land_category: Option<String>,
    /// Official ledger area in square meters.
    pub area_m2: Option<f64>,
    /// Provider ownership category code, not an owner identity.
    pub ownership_kind: Option<String>,
    /// Number of co-owners recorded by the provider.
    pub co_owner_count: Option<i32>,
}

/// Raw cadastral transfer event published by Foundation Platform.
#[derive(Debug, Clone, PartialEq)]
pub struct ParcelTransferEvent {
    /// Provider transfer reason, unchanged.
    pub reason: Option<String>,
    /// Provider transfer reason code, unchanged.
    pub reason_code: Option<String>,
    /// Transfer date exactly as the provider wrote it.
    pub moved_at: Option<String>,
    /// Erasure date exactly as the provider wrote it.
    pub erased_at: Option<String>,
    /// Cadastral land category at the time of the event.
    pub land_category: Option<String>,
    /// Official parcel area at the time of the event.
    pub area_m2: Option<f64>,
    /// Provider event sequence within the parcel.
    pub history_seq: i64,
    /// Provider closure sequence, unchanged.
    pub closure_seq: Option<String>,
}

/// Raw registered unit-level land right published by Foundation Platform.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParcelLandRight {
    /// Provider row serial number, preserved verbatim.
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
}

/// Annual official assessment of a dwelling unit, in Korean won.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitOfficialPrice {
    /// Assessment base year supplied by Foundation.
    pub base_year: i16,
    /// Official unit assessment in integer won.
    pub price_won: i64,
}

/// Parcel information subset used by Gongzzang.
#[derive(Debug, Clone, PartialEq)]
pub struct ParcelInfo {
    /// Administrative hierarchy derived from the PNU.
    pub admin: AdminDivision,
    /// Gongzzang-facing land-use classification, when the Foundation source states one.
    ///
    /// `None` since root ADR-0070: the parcel boundary source carries no use, and inventing
    /// one here would be the defect ADR-0078 removed from the building fields.
    pub land_use_type: Option<LandUseType>,
    /// Zoning, when Foundation Platform publishes a zoning source.
    pub zoning: Option<Zoning>,
    /// Official land price in KRW per square meter, when available.
    pub official_land_price_per_m2: Option<MoneyKrw>,
    /// Official land price notice year/month lineage.
    pub gosi_year_month: Option<GosiYearMonth>,
    /// Source cadastral characteristics, when Foundation publishes them.
    pub characteristics: Option<ParcelCharacteristics>,
    /// Source forest-register facts, when Foundation publishes them.
    pub forest_ledger: Option<ParcelForestLedger>,
    /// Complete raw cadastral transfer timeline in Foundation's published order.
    pub transfer_history: Vec<ParcelTransferEvent>,
    /// First page of registered unit-level land rights in Foundation's published order.
    pub land_rights: Vec<ParcelLandRight>,
    /// Total registered land-right rows, independent of the page bound (root ADR-0093).
    pub land_right_total: u64,
}
