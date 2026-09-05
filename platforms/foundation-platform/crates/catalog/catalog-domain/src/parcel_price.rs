//! Per-parcel official land price assessment (root ADR-0085 §2).
//!
//! One value is the newest assessment the projection loader kept for a parcel: the price in
//! won per square meter as the source's integer, the (`base_year`, `base_month`) the ledger
//! stamped, and the announcement date carried verbatim. Formatting money or interpreting the
//! date is deliberately not here — foundation carries the source's facts and the consumer
//! decides how to show them.

/// One parcel's newest official land price assessment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParcelPrice {
    /// Official land price in won per square meter, the source's integer unchanged.
    pub price_per_m2: i64,
    /// Assessment base year (기준연도).
    pub base_year: i16,
    /// Assessment base month (기준월), 1–12.
    pub base_month: i16,
    /// Announcement date (공시일자) exactly as the source wrote it. Absent when blank.
    pub announced_date: Option<String>,
    /// The assessment vintage this value was projected from.
    pub source_snapshot_id: String,
}
