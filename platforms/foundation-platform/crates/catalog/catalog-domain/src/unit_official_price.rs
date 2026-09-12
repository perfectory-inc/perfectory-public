//! Reference-date official prices indexed by a unit's parcel, dong and ho (root ADR-0095).

/// One reference date's official assessment, in the source's integer won.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnitOfficialPrice {
    /// Assessment reference date in YYYYMMDD format.
    pub base_date: String,
    /// Official assessed value in won.
    pub price_won: i64,
}

/// An assessment and the unit identity within the requested PNU.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnitOfficialPriceRow {
    /// Source dong name; an empty name remains empty.
    pub dong_name: String,
    /// Source ho name.
    pub ho_name: String,
    /// Reference-date assessment.
    pub price: UnitOfficialPrice,
}

/// Whether a reference date has the wire contract's eight ASCII digits (YYYYMMDD).
#[must_use]
pub fn valid_base_date(value: &str) -> bool {
    value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit())
}
