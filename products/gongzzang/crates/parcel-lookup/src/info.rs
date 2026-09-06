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
}
