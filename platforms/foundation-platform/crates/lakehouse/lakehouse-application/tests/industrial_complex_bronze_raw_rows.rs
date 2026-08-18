//! Contract tests for `bronze.industrial_complexes_raw_jsonl` normalization.

#![allow(clippy::err_expect, clippy::expect_used)]

use catalog_domain::{IndustrialComplexKind, IndustrialComplexStatus};
use chrono::{DateTime, Utc};
use lakehouse_application::{
    industrial_complex_bronze_raw_row_to_jsonl, normalize_industrial_complex_bronze_raw_rows,
    IndustrialComplexAddress, IndustrialComplexAddressBook, IndustrialComplexAddressGranularity,
    IndustrialComplexBronzeRawPlanError, IndustrialComplexBronzeRawRowsInput,
    IndustrialComplexBronzeSourceRecord,
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
            IndustrialComplexAddressGranularity::LegalDong,
            "서울특별시 구로구 구로동",
            "industrylandorkr__industrial_complex_list",
            "industrylandorkr:danji_cd=111010",
        )?,
    )?;
    Ok(book)
}

/// A district-granularity address, which is what every real ILIS resolution produces today.
fn sigungu_address(code: &str) -> anyhow::Result<IndustrialComplexAddress> {
    Ok(IndustrialComplexAddress::try_new(
        code,
        IndustrialComplexAddressGranularity::Sigungu,
        "서울특별시 구로구 구로동, 금천구 가산동 일원",
        "industrylandorkr__industrial_complex_list",
        "industrylandorkr:danji_cd=111010",
    )?)
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
fn address_rejects_every_shape_that_is_not_a_ten_digit_administrative_code() {
    for code in ["", "   ", "미상", "115301020", "11530102000", "11530A0200"] {
        let error = IndustrialComplexAddress::try_new(
            code,
            IndustrialComplexAddressGranularity::LegalDong,
            "주소",
            "dataset",
            "record",
        )
        .expect_err("address must reject a non ten-digit administrative code");
        assert!(
            matches!(
                error,
                IndustrialComplexBronzeRawPlanError::InvalidAddress(_)
            ),
            "{code:?} produced {error}"
        );
    }
}

/// The reason the granularity is carried at all: both codes are ten digits, so only the declared
/// granularity distinguishes "this district" from "this dong", and a mismatch is a lie that the
/// shape check alone would pass (root ADR-0034).
#[test]
fn address_rejects_a_code_whose_shape_contradicts_its_declared_granularity() {
    for (code, granularity) in [
        ("1153000000", IndustrialComplexAddressGranularity::LegalDong),
        ("1153010200", IndustrialComplexAddressGranularity::Sigungu),
    ] {
        let error =
            IndustrialComplexAddress::try_new(code, granularity, "주소", "dataset", "record")
                .expect_err("a contradicting granularity must be rejected");
        assert!(
            matches!(
                error,
                IndustrialComplexBronzeRawPlanError::InvalidAddress(ref message)
                    if message.contains("granularity")
            ),
            "{code:?}/{granularity:?} produced {error}"
        );
    }
}

#[test]
fn granularity_wire_values_round_trip_and_reject_anything_else() -> TestResult {
    for granularity in [
        IndustrialComplexAddressGranularity::Sigungu,
        IndustrialComplexAddressGranularity::LegalDong,
    ] {
        assert_eq!(
            IndustrialComplexAddressGranularity::from_wire(granularity.wire_name())?,
            granularity
        );
    }
    for invalid in ["", "  ", "bjdong", "dong", "SIGUNGU", "legal-dong"] {
        assert!(
            IndustrialComplexAddressGranularity::from_wire(invalid).is_err(),
            "{invalid:?} must not parse"
        );
    }
    Ok(())
}

#[test]
fn address_rejects_blank_text_and_blank_provenance() {
    for (text, dataset, record_id) in [
        ("", "dataset", "record"),
        ("   ", "dataset", "record"),
        ("주소", "", "record"),
        ("주소", "dataset", "   "),
    ] {
        let error = IndustrialComplexAddress::try_new(
            "1153010200",
            IndustrialComplexAddressGranularity::LegalDong,
            text,
            dataset,
            record_id,
        )
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
fn administrative_codes_are_derived_from_the_administrative_code() -> TestResult {
    let address = IndustrialComplexAddress::try_new(
        " 4111710300 ",
        IndustrialComplexAddressGranularity::LegalDong,
        " 경기도 수원시 장안구 ",
        " dataset ",
        " record ",
    )?;

    assert_eq!(address.sido_code(), Some("41"));
    assert_eq!(address.sigungu_code(), Some("41117"));
    assert_eq!(address.primary_bjdong_code(), Some("4111710300"));
    assert_eq!(address.address_text(), "경기도 수원시 장안구");
    Ok(())
}

/// `0000000000` is ten ASCII digits and ends in five zeros, so both of the shape checks would pass
/// it and the row would claim province `00`. It is the shape a filler takes once the region columns
/// stop being required, so the constructor has to refuse it by name (root ADR-0035).
#[test]
fn address_rejects_an_all_zero_administrative_code() {
    for (code, granularity) in [
        ("0000000000", IndustrialComplexAddressGranularity::Sigungu),
        ("0000000001", IndustrialComplexAddressGranularity::LegalDong),
    ] {
        let error =
            IndustrialComplexAddress::try_new(code, granularity, "주소", "dataset", "record")
                .expect_err("an all-zero district prefix must be rejected");
        assert!(
            matches!(
                error,
                IndustrialComplexBronzeRawPlanError::InvalidAddress(ref message)
                    if message.contains("all-zero")
            ),
            "{code:?} produced {error}"
        );
    }
}

/// The one complex in 1,442 whose district code no source states. The words are published and the
/// row keeps them; all three region columns are `null`, and no constructor exists that would let a
/// blank or a zero take their place (root ADR-0035).
#[test]
fn an_address_without_an_administrative_code_writes_null_for_every_region_column() -> TestResult {
    let address = IndustrialComplexAddress::try_new_without_administrative_code(
        " 전라남도 신안군 지도읍 ",
        " industrylandorkr__industrial_complex_list ",
        " industrylandorkr:danji_cd=446400 ",
    )?;

    assert_eq!(address.administrative_code(), None);
    assert_eq!(address.granularity(), None);
    assert_eq!(address.sido_code(), None);
    assert_eq!(address.sigungu_code(), None);
    assert_eq!(address.primary_bjdong_code(), None);
    assert_eq!(address.address_text(), "전라남도 신안군 지도읍");

    let records = vec![source_record("446400")];
    let mut addresses = IndustrialComplexAddressBook::new();
    addresses.insert("446400", address)?;
    let rows =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })?;
    let line = industrial_complex_bronze_raw_row_to_jsonl(&rows[0])?;
    let record = serde_json::from_str::<Value>(line.as_str())?;

    assert_eq!(record["sido_code"], Value::Null);
    assert_eq!(record["sigungu_code"], Value::Null);
    assert_eq!(record["primary_bjdong_code"], Value::Null);
    // The words are the location this row does have, and they stay required.
    assert_eq!(record["address_text"], "전라남도 신안군 지도읍");
    Ok(())
}

/// Dropping the code does not drop the provenance: a row with no district still has to say which
/// dataset published the words it carries.
#[test]
fn an_address_without_an_administrative_code_still_rejects_blank_text_and_provenance() {
    for (text, dataset, record_id) in [
        ("", "dataset", "record"),
        ("   ", "dataset", "record"),
        ("주소", "", "record"),
        ("주소", "dataset", "   "),
    ] {
        let error =
            IndustrialComplexAddress::try_new_without_administrative_code(text, dataset, record_id)
                .expect_err("a codeless address must still prove where its words came from");
        assert!(
            matches!(
                error,
                IndustrialComplexBronzeRawPlanError::InvalidAddress(_)
            ),
            "{text:?}/{dataset:?}/{record_id:?} produced {error}"
        );
    }
}

/// A district-granularity source still proves the province and the district — those it named. It
/// has not named a legal dong, and the row must say so rather than let the district code stand in.
#[test]
fn a_district_granularity_address_reports_no_legal_dong_and_the_row_writes_null() -> TestResult {
    let address = sigungu_address("1153000000")?;
    assert_eq!(address.sido_code(), Some("11"));
    assert_eq!(address.sigungu_code(), Some("11530"));
    assert_eq!(address.primary_bjdong_code(), None);
    assert_eq!(address.administrative_code(), Some("1153000000"));

    let records = vec![source_record("111010")];
    let mut addresses = IndustrialComplexAddressBook::new();
    addresses.insert("111010", address)?;
    let rows =
        normalize_industrial_complex_bronze_raw_rows(&IndustrialComplexBronzeRawRowsInput {
            records: &records,
            addresses: &addresses,
            bronze_object_key: BRONZE_OBJECT_KEY,
            source_slug: SOURCE_SLUG,
            ingested_at_utc: ingested_at()?,
        })?;
    let line = industrial_complex_bronze_raw_row_to_jsonl(&rows[0])?;
    let record = serde_json::from_str::<Value>(line.as_str())?;

    assert_eq!(record["sido_code"], "11");
    assert_eq!(record["sigungu_code"], "11530");
    assert_eq!(record["primary_bjdong_code"], Value::Null);
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
            IndustrialComplexAddress::try_new(
                "4111710300",
                IndustrialComplexAddressGranularity::LegalDong,
                "다른 주소",
                "dataset",
                "record",
            )?,
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
        IndustrialComplexAddress::try_new(
            "4111710300",
            IndustrialComplexAddressGranularity::LegalDong,
            "경기도",
            "dataset",
            "record",
        )?,
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
fn exported_status_domain_matches_the_catalog_lifecycle() -> TestResult {
    let statuses = [
        IndustrialComplexStatus::Planned,
        IndustrialComplexStatus::Developing,
        IndustrialComplexStatus::Operating,
        IndustrialComplexStatus::Changed,
        IndustrialComplexStatus::Abolished,
        IndustrialComplexStatus::Unknown,
    ];
    for status in statuses {
        // Adding a variant to IndustrialComplexStatus breaks this match, which is the point:
        // the exported domain below must be extended in the same change.
        match status {
            IndustrialComplexStatus::Planned
            | IndustrialComplexStatus::Developing
            | IndustrialComplexStatus::Operating
            | IndustrialComplexStatus::Changed
            | IndustrialComplexStatus::Abolished
            | IndustrialComplexStatus::Unknown => {}
        }
    }

    let mut expected = statuses
        .iter()
        .map(|status| status.wire_name())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let mut exported = INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES.to_vec();
    exported.sort_unstable();
    assert_eq!(exported, expected);

    for wire in INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES {
        assert_eq!(IndustrialComplexStatus::from_wire(wire)?.wire_name(), *wire);
    }
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
        // The four labels the 202506 Bronze object actually produced, all 1,442 rows.
        ("조성완료", "operating"),
        ("조성중", "developing"),
        ("준비중", "planned"),
        ("보상중", "planned"),
        // Labels no measured snapshot has produced yet.
        ("계획", "planned"),
        ("미개발", "planned"),
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
