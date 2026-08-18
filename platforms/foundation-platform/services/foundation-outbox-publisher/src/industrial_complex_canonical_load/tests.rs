//! Tests for the Gold-to-canonical industrial-complex load.

use serde_json::{json, Map as JsonMap, Value as JsonValue};

use super::{canonical_area_m2, gold_columns_not_loaded, plan_catalog_rows};
use catalog_domain::{
    IndustrialComplexKind, IndustrialComplexLotSalesStatus, IndustrialComplexStatus,
};
use chrono::NaiveDate;

fn gold_row(code: &str, kind: &str, official_area_sqm: JsonValue) -> JsonMap<String, JsonValue> {
    let JsonValue::Object(row) = json!({
        "complex_id": "001533c1-8504-5651-9d49-d9df4e87bc37",
        "official_complex_code": code,
        "name": "Sourced Industrial Complex",
        "kind": kind,
        "status": "operating",
        "sido_code": "46",
        "sigungu_code": "46830",
        "address_text": "sourced address text",
        "designated_date": "1964-04-15",
        "construction_start_date": "1965-03-12",
        "completion_date": JsonValue::Null,
        "official_area_sqm": official_area_sqm,
        "development_progress_percent": "100.00",
        "lot_sales_status": "completed",
        "business_period_raw": "1964-04~1974-11",
        "business_period_start_month": "1964-04",
        "business_period_end_month": "1974-11",
        "designation_basis_law_raw": "sourced basis law",
        "development_method_raw": "sourced development method",
        "development_purpose_raw": "sourced development purpose",
        "invited_industries_raw": "sourced invited industries",
        "calculated_area_sqm": JsonValue::Null,
        "parcel_count": 0,
        "boundary_object_key": JsonValue::Null,
        "source_snapshot_id": "vworldkr__sandan_profile-202506",
        "iceberg_snapshot_id": "1",
        "published_at_utc": "2026-08-18T00:00:00Z",
    }) else {
        unreachable!("the fixture literal is an object")
    };
    row
}

#[test]
fn a_loaded_row_never_claims_a_legal_dong_code() -> anyhow::Result<()> {
    let plan = plan_catalog_rows(&[gold_row("111010", "national", json!("3708451.00"))])?;

    assert_eq!(plan.rows.len(), 1);
    assert_eq!(plan.rows[0].official_complex_code, "111010");
    assert_eq!(plan.rows[0].kind, IndustrialComplexKind::National);
    assert_eq!(plan.rows[0].area_m2, 3_708_451);
    // gold.complex_catalog has no such column, so the canonical row must not invent one.
    assert_eq!(plan.rows[0].primary_bjdong_code, None);
    assert!(plan.skipped.is_empty());
    Ok(())
}

#[test]
fn a_row_whose_area_cannot_be_derived_is_skipped_with_its_reason() -> anyhow::Result<()> {
    let plan = plan_catalog_rows(&[
        gold_row("111010", "national", json!("3708451.00")),
        gold_row("222020", "general", JsonValue::Null),
        gold_row("333030", "agricultural", json!("1234.50")),
        gold_row("444040", "urban_high_tech", json!("-5.00")),
    ])?;

    assert_eq!(plan.rows.len(), 1);
    let skipped = plan
        .skipped
        .iter()
        .map(|row| (row.official_complex_code.as_str(), row.reason))
        .collect::<Vec<_>>();
    assert_eq!(
        skipped,
        vec![
            ("222020", "official_area_sqm_missing"),
            ("333030", "official_area_sqm_not_whole"),
            ("444040", "official_area_sqm_negative"),
        ]
    );
    Ok(())
}

#[test]
fn an_unknown_kind_fails_the_whole_load_rather_than_skipping_one_row() {
    let error = plan_catalog_rows(&[gold_row("111010", "logistics", json!("3708451.00"))]);
    assert!(error.is_err(), "an off-contract kind must not be skipped");
}

#[test]
fn the_sourced_description_columns_reach_the_canonical_row() -> anyhow::Result<()> {
    let plan = plan_catalog_rows(&[gold_row("111010", "national", json!("3708451.00"))])?;
    let row = &plan.rows[0];

    assert_eq!(row.status, Some(IndustrialComplexStatus::Operating));
    assert_eq!(
        row.lot_sales_status,
        Some(IndustrialComplexLotSalesStatus::Completed)
    );
    assert_eq!(
        row.construction_start_date,
        Some(NaiveDate::from_ymd_opt(1965, 3, 12).expect("a real date"))
    );
    assert_eq!(row.completion_date, None);
    assert_eq!(row.development_progress_percent.as_deref(), Some("100.00"));
    assert_eq!(row.business_period_raw.as_deref(), Some("1964-04~1974-11"));
    assert_eq!(row.business_period_start_month.as_deref(), Some("1964-04"));
    assert_eq!(row.business_period_end_month.as_deref(), Some("1974-11"));
    assert_eq!(
        row.designation_basis_law_raw.as_deref(),
        Some("sourced basis law")
    );
    assert_eq!(
        row.development_method_raw.as_deref(),
        Some("sourced development method")
    );
    assert_eq!(
        row.development_purpose_raw.as_deref(),
        Some("sourced development purpose")
    );
    assert_eq!(
        row.invited_industries_raw.as_deref(),
        Some("sourced invited industries")
    );
    Ok(())
}

/// A Gold row that dropped a description column is not a defect: the column is optional the whole
/// way down. The canonical row records the absence as `NULL` and nothing substitutes for it.
#[test]
fn an_absent_description_column_becomes_null_rather_than_a_substitute() -> anyhow::Result<()> {
    let mut row = gold_row("111010", "national", json!("3708451.00"));
    for column in [
        "lot_sales_status",
        "development_progress_percent",
        "business_period_raw",
        "business_period_start_month",
        "business_period_end_month",
        "designation_basis_law_raw",
        "development_method_raw",
        "development_purpose_raw",
        "invited_industries_raw",
        "construction_start_date",
    ] {
        row.insert(column.to_owned(), JsonValue::Null);
    }

    let plan = plan_catalog_rows(&[row])?;
    let planned = &plan.rows[0];

    assert_eq!(planned.lot_sales_status, None);
    assert_eq!(planned.development_progress_percent, None);
    assert_eq!(planned.business_period_raw, None);
    assert_eq!(planned.business_period_start_month, None);
    assert_eq!(planned.business_period_end_month, None);
    assert_eq!(planned.designation_basis_law_raw, None);
    assert_eq!(planned.development_method_raw, None);
    assert_eq!(planned.development_purpose_raw, None);
    assert_eq!(planned.invited_industries_raw, None);
    assert_eq!(planned.construction_start_date, None);
    Ok(())
}

/// Same rule as an unknown `kind` or `status`: the projection broke its own value domain, so the
/// load fails rather than writing `NULL`, which would report a broken contract as an absent value.
#[test]
fn an_unknown_lot_sales_status_fails_the_whole_load() {
    let mut row = gold_row("111010", "national", json!("3708451.00"));
    row.insert("lot_sales_status".to_owned(), json!("분양중"));

    assert!(
        plan_catalog_rows(&[row]).is_err(),
        "an off-domain lot_sales_status must not be skipped or nulled"
    );
}

/// The command every Gold column this loader does not read shows up in. Derived from the contract
/// rather than listed, so a column added to Gold and not read here surfaces on the next run.
#[test]
fn the_unloaded_gold_columns_are_only_the_ones_with_no_canonical_home() {
    assert_eq!(
        gold_columns_not_loaded(),
        vec![
            "calculated_area_sqm",
            "parcel_count",
            "boundary_object_key",
            // Lineage rather than description: the canonical row's provenance comes from the
            // load itself, not from a column copied out of the projection.
            "source_snapshot_id",
            "iceberg_snapshot_id",
            "published_at_utc",
        ]
    );
}

#[test]
fn a_duplicated_official_code_in_one_snapshot_is_refused() {
    let error = plan_catalog_rows(&[
        gold_row("111010", "national", json!("3708451.00")),
        gold_row("111010", "general", json!("42.00")),
    ]);
    assert!(error.is_err(), "one snapshot must not carry a code twice");
}

#[test]
fn whole_decimal_areas_convert_and_fractional_ones_do_not() {
    assert_eq!(canonical_area_m2(Some(&json!("3708451.00"))), Ok(3_708_451));
    assert_eq!(canonical_area_m2(Some(&json!("11081"))), Ok(11_081));
    assert_eq!(canonical_area_m2(Some(&json!("0.00"))), Ok(0));
    assert_eq!(
        canonical_area_m2(Some(&json!("1.01"))),
        Err("official_area_sqm_not_whole")
    );
    assert_eq!(
        canonical_area_m2(Some(&json!("-1.00"))),
        Err("official_area_sqm_negative")
    );
    assert_eq!(
        canonical_area_m2(Some(&JsonValue::Null)),
        Err("official_area_sqm_missing")
    );
    assert_eq!(canonical_area_m2(None), Err("official_area_sqm_missing"));
    assert_eq!(
        canonical_area_m2(Some(&json!("not a number"))),
        Err("official_area_sqm_unreadable")
    );
}
