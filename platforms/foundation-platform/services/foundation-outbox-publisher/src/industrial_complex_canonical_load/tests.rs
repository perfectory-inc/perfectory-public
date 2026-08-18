//! Tests for the Gold-to-canonical industrial-complex load.

use serde_json::{json, Map as JsonMap, Value as JsonValue};

use super::{canonical_area_m2, plan_catalog_rows};
use catalog_domain::IndustrialComplexKind;

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
        "official_area_sqm": official_area_sqm,
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
