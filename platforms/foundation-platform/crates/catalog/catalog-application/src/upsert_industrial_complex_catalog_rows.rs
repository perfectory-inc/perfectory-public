//! Use case for writing source-side industrial-complex rows into Catalog by official code.
//!
//! One write path serves every producer of `catalog.industrial_complex`: the hand-written seed
//! import and the Gold catalog loader (root ADR-0040). A second use case with the same body would
//! be the place where one of the two forgets a validation rule.

use std::sync::Arc;

use catalog_domain::{
    CatalogError, IndustrialComplexKind, IndustrialComplexLotSalesStatus, IndustrialComplexStatus,
};
use chrono::NaiveDate;
use foundation_shared_kernel::ids::LakehouseComplexId;

use crate::industrial_complex_input::{
    validate_clean_required, validate_optional_business_period_months,
    validate_optional_clean_text, validate_optional_primary_bjdong_code,
    validate_optional_progress_percent, validate_optional_sido_code,
    validate_optional_sigungu_code, validate_source_official_complex_code,
};
use crate::ports::{
    CatalogUnitOfWork, UpsertIndustrialComplexCommand, UpsertIndustrialComplexOutcome,
};

/// Source-side row that establishes canonical industrial-complex identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexCatalogRow {
    /// Lakehouse identifier of the same complex, when the producer read one.
    pub lakehouse_complex_id: Option<LakehouseComplexId>,
    /// Source-side official industrial-complex code. The natural key of the upsert.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Domain-level industrial complex kind.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code shared by parcels that belong to the complex, when one is known.
    pub primary_bjdong_code: Option<String>,
    /// Official complex area in square meters.
    pub area_m2: u64,
    /// Lifecycle status the source stated.
    pub status: Option<IndustrialComplexStatus>,
    /// Province-level administrative code the address resolution produced.
    pub sido_code: Option<String>,
    /// City/county/district administrative code the address resolution produced.
    pub sigungu_code: Option<String>,
    /// Address the source stated, verbatim.
    pub address_text: Option<String>,
    /// Organization that manages the complex.
    pub management_agency_name: Option<String>,
    /// Organization that developed the complex.
    pub developer_name: Option<String>,
    /// Date the complex was officially designated.
    pub designated_date: Option<NaiveDate>,
    /// Date site works started.
    pub construction_start_date: Option<NaiveDate>,
    /// Date the complex's site formation was approved as complete.
    pub completion_date: Option<NaiveDate>,
    /// Site-formation progress percentage as exact decimal text.
    pub development_progress_percent: Option<String>,
    /// Lot-sales status the source stated.
    pub lot_sales_status: Option<IndustrialComplexLotSalesStatus>,
    /// Business period exactly as the source wrote it.
    pub business_period_raw: Option<String>,
    /// First month of the business period as `yyyy-MM`.
    pub business_period_start_month: Option<String>,
    /// Last month of the business period as `yyyy-MM`.
    pub business_period_end_month: Option<String>,
    /// Statute the designation was made under, verbatim.
    pub designation_basis_law_raw: Option<String>,
    /// Development method, verbatim.
    pub development_method_raw: Option<String>,
    /// Stated development purpose, verbatim.
    pub development_purpose_raw: Option<String>,
    /// Industry types the complex set out to attract, verbatim.
    pub invited_industries_raw: Option<String>,
}

/// Input for writing source-side industrial-complex rows into Catalog.
pub struct UpsertIndustrialComplexCatalogRowsInput {
    /// Source-side rows to create or update by `official_complex_code`.
    pub rows: Vec<IndustrialComplexCatalogRow>,
}

/// Result of writing source-side industrial-complex rows into Catalog.
#[derive(Clone, Debug)]
pub struct UpsertIndustrialComplexCatalogRowsReport {
    /// Per-row outcome in command order.
    pub outcomes: Vec<UpsertIndustrialComplexOutcome>,
}

impl UpsertIndustrialComplexCatalogRowsReport {
    /// Number of rows the write path created, updated, or left alone.
    #[must_use]
    pub const fn written_count(&self) -> usize {
        self.outcomes.len()
    }

    /// Number of rows with the given effect.
    #[must_use]
    pub fn count_with(&self, effect: crate::ports::UpsertIndustrialComplexEffect) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.effect == effect)
            .count()
    }
}

/// Writes source-side industrial-complex identity into Catalog.
pub struct UpsertIndustrialComplexCatalogRows {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl UpsertIndustrialComplexCatalogRows {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Creates or updates Catalog complexes by source-side `official_complex_code`.
    ///
    /// # Errors
    /// Returns `CatalogError` when input validation or persistence fails.
    pub async fn execute(
        &self,
        input: UpsertIndustrialComplexCatalogRowsInput,
    ) -> Result<UpsertIndustrialComplexCatalogRowsReport, CatalogError> {
        if input.rows.is_empty() {
            return Err(CatalogError::InvalidIndustrialComplexInput(
                "industrial-complex catalog upsert must contain at least one row".to_owned(),
            ));
        }

        let commands = input
            .rows
            .iter()
            .map(catalog_row_to_upsert_command)
            .collect::<Result<Vec<_>, _>>()?;
        let outcomes = self
            .uow
            .upsert_complexes_by_official_code(&commands)
            .await?;
        Ok(UpsertIndustrialComplexCatalogRowsReport { outcomes })
    }
}

fn catalog_row_to_upsert_command(
    row: &IndustrialComplexCatalogRow,
) -> Result<UpsertIndustrialComplexCommand, CatalogError> {
    validate_clean_required("official_complex_code", row.official_complex_code.as_str())?;
    validate_source_official_complex_code(row.official_complex_code.as_str())?;
    validate_clean_required("name", row.name.as_str())?;
    validate_optional_primary_bjdong_code(row.primary_bjdong_code.as_deref())?;
    validate_optional_sido_code(row.sido_code.as_deref())?;
    validate_optional_sigungu_code(row.sigungu_code.as_deref())?;
    // A present value must be real text. `Some("")` and `Some("  ")` are how "the source said
    // nothing" gets smuggled in as a value, which is the one thing this loader must not do; the
    // caller has to send `None` instead.
    validate_optional_clean_text("address_text", row.address_text.as_deref())?;
    validate_optional_clean_text(
        "management_agency_name",
        row.management_agency_name.as_deref(),
    )?;
    validate_optional_clean_text("developer_name", row.developer_name.as_deref())?;
    for (label, value) in [
        ("business_period_raw", row.business_period_raw.as_deref()),
        (
            "designation_basis_law_raw",
            row.designation_basis_law_raw.as_deref(),
        ),
        (
            "development_method_raw",
            row.development_method_raw.as_deref(),
        ),
        (
            "development_purpose_raw",
            row.development_purpose_raw.as_deref(),
        ),
        (
            "invited_industries_raw",
            row.invited_industries_raw.as_deref(),
        ),
    ] {
        validate_optional_clean_text(label, value)?;
    }
    validate_optional_progress_percent(row.development_progress_percent.as_deref())?;
    validate_optional_business_period_months(
        row.business_period_start_month.as_deref(),
        row.business_period_end_month.as_deref(),
    )?;
    Ok(UpsertIndustrialComplexCommand {
        lakehouse_complex_id: row.lakehouse_complex_id,
        official_complex_code: row.official_complex_code.clone(),
        name: row.name.clone(),
        kind: row.kind,
        primary_bjdong_code: row.primary_bjdong_code.clone(),
        area_m2: row.area_m2,
        status: row.status,
        sido_code: row.sido_code.clone(),
        sigungu_code: row.sigungu_code.clone(),
        address_text: row.address_text.clone(),
        management_agency_name: row.management_agency_name.clone(),
        developer_name: row.developer_name.clone(),
        designated_date: row.designated_date,
        construction_start_date: row.construction_start_date,
        completion_date: row.completion_date,
        development_progress_percent: row.development_progress_percent.clone(),
        lot_sales_status: row.lot_sales_status,
        business_period_raw: row.business_period_raw.clone(),
        business_period_start_month: row.business_period_start_month.clone(),
        business_period_end_month: row.business_period_end_month.clone(),
        designation_basis_law_raw: row.designation_basis_law_raw.clone(),
        development_method_raw: row.development_method_raw.clone(),
        development_purpose_raw: row.development_purpose_raw.clone(),
        invited_industries_raw: row.invited_industries_raw.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{catalog_row_to_upsert_command, IndustrialComplexCatalogRow};
    use catalog_domain::{CatalogError, IndustrialComplexKind};

    fn row(primary_bjdong_code: Option<&str>) -> IndustrialComplexCatalogRow {
        IndustrialComplexCatalogRow {
            official_complex_code: "111010".to_owned(),
            name: "한국수출국가산업단지".to_owned(),
            kind: IndustrialComplexKind::National,
            primary_bjdong_code: primary_bjdong_code.map(ToOwned::to_owned),
            area_m2: 3_708_451,
            lakehouse_complex_id: None,
            status: None,
            sido_code: None,
            sigungu_code: None,
            address_text: None,
            management_agency_name: None,
            developer_name: None,
            designated_date: None,
            construction_start_date: None,
            completion_date: None,
            development_progress_percent: None,
            lot_sales_status: None,
            business_period_raw: None,
            business_period_start_month: None,
            business_period_end_month: None,
            designation_basis_law_raw: None,
            development_method_raw: None,
            development_purpose_raw: None,
            invited_industries_raw: None,
        }
    }

    #[test]
    fn a_row_without_a_legal_dong_code_is_accepted() -> Result<(), CatalogError> {
        let command = catalog_row_to_upsert_command(&row(None))?;
        assert_eq!(command.primary_bjdong_code, None);
        Ok(())
    }

    #[test]
    fn a_row_with_a_malformed_legal_dong_code_is_still_rejected() {
        let error = catalog_row_to_upsert_command(&row(Some("28200")));
        assert!(matches!(
            error,
            Err(CatalogError::InvalidIndustrialComplexInput(_))
        ));
    }

    #[test]
    fn a_row_with_a_well_formed_legal_dong_code_keeps_it() -> Result<(), CatalogError> {
        let command = catalog_row_to_upsert_command(&row(Some("1138010700")))?;
        assert_eq!(command.primary_bjdong_code.as_deref(), Some("1138010700"));
        Ok(())
    }

    #[test]
    fn progress_outside_zero_to_one_hundred_is_refused() {
        for percent in ["-0.01", "100.01", "1000", "", "  ", "구십"] {
            let mut candidate = row(None);
            candidate.development_progress_percent = Some(percent.to_owned());
            assert!(
                matches!(
                    catalog_row_to_upsert_command(&candidate),
                    Err(CatalogError::InvalidIndustrialComplexInput(_))
                ),
                "{percent:?} was accepted"
            );
        }
    }

    #[test]
    fn zero_and_one_hundred_percent_are_both_accepted() -> Result<(), CatalogError> {
        for percent in ["0", "0.00", "59.90", "100", "100.00"] {
            let mut candidate = row(None);
            candidate.development_progress_percent = Some(percent.to_owned());
            let command = catalog_row_to_upsert_command(&candidate)?;
            assert_eq!(
                command.development_progress_percent.as_deref(),
                Some(percent)
            );
        }
        Ok(())
    }

    /// One month without the other would put a boundary on a period the source never bounded.
    #[test]
    fn one_business_period_month_without_the_other_is_refused() {
        for (start, end) in [(Some("1964-04"), None), (None, Some("1974-11"))] {
            let mut candidate = row(None);
            candidate.business_period_start_month = start.map(ToOwned::to_owned);
            candidate.business_period_end_month = end.map(ToOwned::to_owned);
            assert!(
                matches!(
                    catalog_row_to_upsert_command(&candidate),
                    Err(CatalogError::InvalidIndustrialComplexInput(_))
                ),
                "{start:?}/{end:?} was accepted"
            );
        }
    }

    #[test]
    fn a_business_period_month_that_is_not_a_month_is_refused() {
        for (start, end) in [
            ("1964-13", "1974-11"),
            ("1964-00", "1974-11"),
            ("1964-04", "197411"),
            ("1964-4", "1974-11"),
            ("196404", "1974-11"),
        ] {
            let mut candidate = row(None);
            candidate.business_period_start_month = Some(start.to_owned());
            candidate.business_period_end_month = Some(end.to_owned());
            assert!(
                matches!(
                    catalog_row_to_upsert_command(&candidate),
                    Err(CatalogError::InvalidIndustrialComplexInput(_))
                ),
                "{start:?}/{end:?} was accepted"
            );
        }
    }

    /// The `2020-~2024-` complex: raw text present, both months absent. That row has to be
    /// writable, because refusing it would drop a period the source did state.
    #[test]
    fn a_raw_business_period_without_months_is_accepted() -> Result<(), CatalogError> {
        let mut candidate = row(None);
        candidate.business_period_raw = Some("2020-~2024-".to_owned());
        let command = catalog_row_to_upsert_command(&candidate)?;
        assert_eq!(command.business_period_raw.as_deref(), Some("2020-~2024-"));
        assert_eq!(command.business_period_start_month, None);
        assert_eq!(command.business_period_end_month, None);
        Ok(())
    }

    #[test]
    fn blank_free_text_is_refused_rather_than_stored_as_an_empty_value() {
        /// Sets one free-text column, so the loop below can walk all of them.
        type SetFreeText = fn(&mut IndustrialComplexCatalogRow, String);

        let blanks: [(&str, SetFreeText); 5] = [
            ("business_period_raw", |row, value| {
                row.business_period_raw = Some(value);
            }),
            ("designation_basis_law_raw", |row, value| {
                row.designation_basis_law_raw = Some(value);
            }),
            ("development_method_raw", |row, value| {
                row.development_method_raw = Some(value);
            }),
            ("development_purpose_raw", |row, value| {
                row.development_purpose_raw = Some(value);
            }),
            ("invited_industries_raw", |row, value| {
                row.invited_industries_raw = Some(value);
            }),
        ];
        for (label, set) in blanks {
            for blank in ["", "   "] {
                let mut candidate = row(None);
                set(&mut candidate, blank.to_owned());
                assert!(
                    matches!(
                        catalog_row_to_upsert_command(&candidate),
                        Err(CatalogError::InvalidIndustrialComplexInput(_))
                    ),
                    "{label} accepted {blank:?}"
                );
            }
        }
    }
}
