//! Annual official prices indexed by a unit's parcel, dong and ho (root ADR-0095).

/// One year's official assessment, in the source's integer won.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnitOfficialPrice {
    /// Four-digit year derived from the assessment base date.
    pub base_year: i16,
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
    /// Annual assessment.
    pub price: UnitOfficialPrice,
}
