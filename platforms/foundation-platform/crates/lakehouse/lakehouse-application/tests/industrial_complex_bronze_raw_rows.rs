//! Contract tests for `bronze.industrial_complexes_raw_jsonl` normalization.

#![allow(clippy::err_expect, clippy::expect_used)]

use catalog_domain::IndustrialComplexKind;
use chrono::{DateTime, Utc};
use lakehouse_application::{
    industrial_complex_bronze_raw_row_to_jsonl, normalize_industrial_complex_bronze_raw_rows,
    IndustrialComplexAddress, IndustrialComplexAddressBook, IndustrialComplexBronzeRawPlanError,
    IndustrialComplexBronzeRawRowsInput, IndustrialComplexBronzeSourceRecord,
};
use lakehouse_domain::{
    bronze_industrial_complexes_raw_jsonl_columns, INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES,
    INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
};
use serde_json::Value;

// The reserved 20991231DS9999x synthetic range; a captured provider object id is private
// operational evidence and must not sit in a public fixture.
const BRONZE_OBJECT_KEY: &str = "bronze/source=vworldkr__sandan_profile/20991231DS99990-1.zip";
const SOURCE_SLUG: &str = "vworldkr__sandan_profile";

type TestResult = anyhow::Result<()>;

fn ingested_at() -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339("2026-08-16T04:05:06Z")?.to_utc())
}

fn source_record(code: &str) -> IndustrialComplexBronzeSourceRecord {
    IndustrialComplexBronzeSourceRecord {
        official_complex_code: code.to_owned(),
        complex_name: "  구로디지털단지  ".to_owned(),
        complex_kind_label: "국가".to_owned(),
        status_label: "조성완료".to_owned(),
        management_agency_name: Some("한국산업단지공단".to_owned()),
        developer_name: Some("   ".to_owned()),
        designated_date_raw: Some("19640415".to_owned()),
        completion_date_raw: None,
        official_area_sqm_raw: Some("1925368.7".to_owned()),
        snapshot_period: "202506".to_owned(),
        source_row_number: 2,
    }
}

fn address_book(code: &str) -> anyhow::Result<IndustrialComplexAddressBook> {
    let mut book = IndustrialComplexAddressBook::new();
    book.insert(
        code,
        IndustrialComplexAddress::try_new(
            "1153010200",
            "서울특별시 구로구 구로동",
            "ilis__industrial_complex_detail",
            "ilis:111010",
        )?,
    )?;
    Ok(book)
}

#[test]
fn bronze_rows_emit_exactly_the_exported_transport_columns() -> TestResult {
    let records = vec![source_record("111010")];
    let addresses = address_book("111010")?;
    let rows =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })?;

    assert_eq!(rows.len(), 1);
    let line = industrial_complex_bronze_raw_row_to_jsonl(&rows[0])?;
    let value = serde_json::from_str::<Value>(line.as_str())?;
    let record = value.as_object().ok_or_else(|| anyhow::anyhow!("object"))?;

    // serde_json orders object keys; the transport contract fixes the key set, not the order.
    let mut expected_columns = bronze_industrial_complexes_raw_jsonl_columns();
    expected_columns.sort_unstable();
    assert_eq!(
        record.keys().map(String::as_str).collect::<Vec<_>>(),
        expected_columns
    );
    assert_eq!(record["official_complex_code"], "111010");
    assert_eq!(record["complex_name"], "구로디지털단지");
    assert_eq!(record["complex_kind"], "national");
    assert_eq!(record["status"], "operating");
    assert_eq!(record["sido_code"], "11");
    assert_eq!(record["sigungu_code"], "11530");
    assert_eq!(record["primary_bjdong_code"], "1153010200");
    assert_eq!(record["address_text"], "서울특별시 구로구 구로동");
    assert_eq!(record["management_agency_name"], "한국산업단지공단");
    assert_eq!(record["developer_name"], Value::Null);
    assert_eq!(record["designated_date"], "1964-04-15");
    assert_eq!(record["completion_date"], Value::Null);
    assert_eq!(record["official_area_sqm"], "1925368.70");
    assert_eq!(
        record["source_record_id"],
        format!("foundation-platform:bronze:{BRONZE_OBJECT_KEY}#111010")
    );
    assert_eq!(
        record["source_snapshot_id"],
        "vworldkr__sandan_profile-202506"
    );
    assert_eq!(record["valid_from_utc"], "2025-06-01T00:00:00Z");
    assert_eq!(record["ingested_at_utc"], "2026-08-16T04:05:06Z");
    Ok(())
}

#[test]
fn address_rejects_every_shape_that_is_not_a_legal_dong_code() {
    for code in ["", "   ", "미상", "115301020", "11530102000", "11530A0200"] {
        let error = IndustrialComplexAddress::try_new(code, "주소", "dataset", "record")
            .expect_err("address must reject a non legal-dong code");
        assert!(
            matches!(
                error,
                IndustrialComplexBronzeRawPlanError::InvalidAddress(_)
            ),
            "{code:?} produced {error}"
        );
    }
}

#[test]
fn address_rejects_blank_text_and_blank_provenance() {
    for (text, dataset, record_id) in [
        ("", "dataset", "record"),
        ("   ", "dataset", "record"),
        ("주소", "", "record"),
        ("주소", "dataset", "   "),
    ] {
        let error = IndustrialComplexAddress::try_new("1153010200", text, dataset, record_id)
            .expect_err("address must reject blank text or provenance");
        assert!(
            matches!(
                error,
                IndustrialComplexBronzeRawPlanError::InvalidAddress(_)
            ),
            "{text:?}/{dataset:?}/{record_id:?} produced {error}"
        );
    }
}

#[test]
fn administrative_codes_are_derived_from_the_legal_dong_code() -> TestResult {
    let address = IndustrialComplexAddress::try_new(
        " 4111710300 ",
        " 경기도 수원시 장안구 ",
        " dataset ",
        " record ",
    )?;

    assert_eq!(address.sido_code(), "41");
    assert_eq!(address.sigungu_code(), "41117");
    assert_eq!(address.primary_bjdong_code(), "4111710300");
    assert_eq!(address.address_text(), "경기도 수원시 장안구");
    Ok(())
}

#[test]
fn a_row_without_a_sourced_address_stops_the_whole_run() -> TestResult {
    let records = vec![
        source_record("111010"),
        source_record("111020"),
        source_record("111030"),
    ];
    let addresses = address_book("111010")?;

    let error =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })
        .expect_err("unresolved addresses must fail the run");

    match error {
        IndustrialComplexBronzeRawPlanError::MissingAddresses {
            missing_count,
            row_count,
            sample,
        } => {
            assert_eq!(missing_count, 2);
            assert_eq!(row_count, 3);
            assert_eq!(sample, "111020, 111030");
        }
        other => anyhow::bail!("expected MissingAddresses, got {other}"),
    }
    Ok(())
}

#[test]
fn an_empty_address_book_fails_before_anything_is_normalized() -> TestResult {
    let records = vec![source_record("111010")];
    let addresses = IndustrialComplexAddressBook::new();

    let error =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })
        .expect_err("an empty address book must fail the run");

    assert!(
        matches!(
            error,
            IndustrialComplexBronzeRawPlanError::MissingAddresses { .. }
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn the_address_book_refuses_to_resolve_one_code_twice() -> TestResult {
    let mut book = address_book("111010")?;
    let error = book
        .insert(
            "111010",
            IndustrialComplexAddress::try_new("4111710300", "다른 주소", "dataset", "record")?,
        )
        .expect_err("a second resolution must fail");

    assert!(
        matches!(
            error,
            IndustrialComplexBronzeRawPlanError::DuplicateAddress(ref code) if code == "111010"
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn a_repeated_official_complex_code_fails_the_run() -> TestResult {
    let records = vec![source_record("111010"), source_record("111010")];
    let addresses = address_book("111010")?;

    let error =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })
        .expect_err("a duplicated source key must fail");

    assert!(
        matches!(
            error,
            IndustrialComplexBronzeRawPlanError::DuplicateSourceRow(ref code) if code == "111010"
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn mixed_snapshot_months_fail_instead_of_picking_one() -> TestResult {
    let mut second = source_record("111020");
    second.snapshot_period = "202507".to_owned();
    let records = vec![source_record("111010"), second];
    let mut addresses = address_book("111010")?;
    addresses.insert(
        "111020",
        IndustrialComplexAddress::try_new("4111710300", "경기도", "dataset", "record")?,
    )?;

    let error =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })
        .expect_err("mixed snapshot months must fail");

    assert!(
        matches!(
            error,
            IndustrialComplexBronzeRawPlanError::MixedSnapshotPeriods(_)
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn unmapped_source_labels_fail_instead_of_defaulting_to_unknown() -> TestResult {
    for (kind_label, status_label) in [("국가", "미상"), ("복합", "조성완료"), ("국가", "")]
    {
        let mut record = source_record("111010");
        record.complex_kind_label = kind_label.to_owned();
        record.status_label = status_label.to_owned();
        let records = vec![record];
        let addresses = address_book("111010")?;

        let error =
            normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
                records: &records,
                addresses: &addresses,
                bronze_object_key: BRONZE_OBJECT_KEY,
                source_slug: SOURCE_SLUG,
                ingested_at_utc: ingested_at()?,
            })
            .expect_err("an unmapped label must fail");

        assert!(
            matches!(error, IndustrialComplexBronzeRawPlanError::InvalidInput(_)),
            "{kind_label:?}/{status_label:?} produced {error}"
        );
    }
    Ok(())
}

#[test]
fn area_zero_is_absent_and_a_negative_area_fails() -> TestResult {
    let mut zero = source_record("111010");
    zero.official_area_sqm_raw = Some("0".to_owned());
    let records = vec![zero];
    let addresses = address_book("111010")?;
    let rows =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })?;
    assert_eq!(rows[0].official_area_sqm, None);

    let mut negative = source_record("111010");
    negative.official_area_sqm_raw = Some("-1".to_owned());
    let records = vec![negative];
    let error =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })
        .expect_err("a negative area must fail");
    assert!(
        matches!(error, IndustrialComplexBronzeRawPlanError::InvalidInput(_)),
        "{error}"
    );
    Ok(())
}

#[test]
fn impossible_calendar_dates_fail() -> TestResult {
    let mut record = source_record("111010");
    record.designated_date_raw = Some("19640431".to_owned());
    let records = vec![record];
    let addresses = address_book("111010")?;

    let error =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })
        .expect_err("an impossible calendar date must fail");

    assert!(
        matches!(error, IndustrialComplexBronzeRawPlanError::InvalidInput(_)),
        "{error}"
    );
    Ok(())
}

#[test]
fn exported_kind_domain_matches_the_catalog_classification() -> TestResult {
    let kinds = [
        IndustrialComplexKind::National,
        IndustrialComplexKind::General,
        IndustrialComplexKind::Agricultural,
        IndustrialComplexKind::UrbanHighTech,
    ];
    for kind in kinds {
        // Adding a variant to IndustrialComplexKind breaks this match, which is the point:
        // the exported domain below must be extended in the same change.
        match kind {
            IndustrialComplexKind::National
            | IndustrialComplexKind::General
            | IndustrialComplexKind::Agricultural
            | IndustrialComplexKind::UrbanHighTech => {}
        }
    }

    let mut expected = kinds
        .iter()
        .map(|kind| kind.wire_name())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let mut exported = INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES.to_vec();
    exported.sort_unstable();
    assert_eq!(exported, expected);

    for wire in INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES {
        assert_eq!(IndustrialComplexKind::from_wire(wire)?.wire_name(), *wire);
    }
    Ok(())
}

#[test]
fn every_mapped_status_stays_inside_the_exported_domain() -> TestResult {
    for (label, expected) in [
        ("계획", "planned"),
        ("미개발", "planned"),
        ("조성중", "developing"),
        ("조성완료", "operating"),
        ("변경", "changed"),
        ("폐지", "abolished"),
    ] {
        let mut record = source_record("111010");
        record.status_label = label.to_owned();
        let records = vec![record];
        let addresses = address_book("111010")?;
        let rows =
            normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
                records: &records,
                addresses: &addresses,
                bronze_object_key: BRONZE_OBJECT_KEY,
                source_slug: SOURCE_SLUG,
                ingested_at_utc: ingested_at()?,
            })?;
        assert_eq!(rows[0].status, expected);
        assert!(INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES.contains(&expected));
    }
    Ok(())
}
