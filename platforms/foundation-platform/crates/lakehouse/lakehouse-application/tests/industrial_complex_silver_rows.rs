//! Contract tests for industrial-complex Silver row handoff.

use catalog_domain::{IndustrialComplex, IndustrialComplexKind};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_application::{
    build_industrial_complex_silver_handoff, normalize_industrial_complex_silver_rows,
    IndustrialComplexSilverRowsInput,
};
use lakehouse_domain::SILVER_INDUSTRIAL_COMPLEXES;
use serde_json::Value as JsonValue;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FIXTURE_COMPLEX_ID: &str = "00000000-0000-7000-8000-000000000001";
const FIXTURE_VALID_FROM_UTC: &str = "2099-01-01T00:00:00Z";
const FIXTURE_INGESTED_AT_UTC: &str = "2099-01-01T00:00:01Z";

#[test]
fn normalizes_catalog_complexes_into_silver_rows_without_rekeying() -> TestResult {
    let complex = sample_complex()?;
    let valid_from_utc = parse_utc(FIXTURE_VALID_FROM_UTC)?;
    let ingested_at_utc = parse_utc(FIXTURE_INGESTED_AT_UTC)?;

    let rows = normalize_industrial_complex_silver_rows(&IndustrialComplexSilverRowsInput {
        complexes: std::slice::from_ref(&complex),
        source_snapshot_id: "synthetic-source-snapshot-industrial-complexes-20990101",
        ingested_at_utc,
    })?;

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.complex_id, FIXTURE_COMPLEX_ID);
    assert_eq!(row.official_complex_code, "SYNTHETIC-COMPLEX-001");
    assert_eq!(row.complex_name, "Synthetic Industrial Complex Alpha");
    assert_eq!(
        row.complex_name_normalized,
        "synthetic industrial complex alpha"
    );
    assert_eq!(row.complex_kind, "general");
    assert_eq!(row.status, "unknown");
    assert_eq!(row.sido_code, "99");
    assert_eq!(row.sigungu_code, "99999");
    assert_eq!(row.primary_bjdong_code.as_deref(), Some("9999900101"));
    assert_eq!(row.official_area_sqm, Some(123_456));
    assert_eq!(
        row.source_record_id,
        format!("foundation-platform:catalog.industrial_complex:{FIXTURE_COMPLEX_ID}")
    );
    assert_eq!(
        row.source_snapshot_id,
        "synthetic-source-snapshot-industrial-complexes-20990101"
    );
    assert_eq!(row.valid_from_utc, valid_from_utc);
    assert_eq!(row.ingested_at_utc, ingested_at_utc);
    Ok(())
}

#[test]
fn builds_writer_neutral_jsonl_handoff_for_silver_industrial_complexes() -> TestResult {
    let complex = sample_complex()?;
    let rows = normalize_industrial_complex_silver_rows(&IndustrialComplexSilverRowsInput {
        complexes: std::slice::from_ref(&complex),
        source_snapshot_id: "synthetic-source-snapshot-industrial-complexes-20990101",
        ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
    })?;

    let handoff = build_industrial_complex_silver_handoff(&rows)?;

    assert_eq!(handoff.contract_table_name, "silver.industrial_complexes");
    assert_eq!(
        handoff.table_columns,
        SILVER_INDUSTRIAL_COMPLEXES
            .columns
            .iter()
            .map(|column| column.name.to_owned())
            .collect::<Vec<_>>()
    );
    assert!(handoff.transport_columns.contains(&"complex_id".to_owned()));
    assert_eq!(handoff.quality_metrics["row_count"], 1);
    assert_eq!(handoff.quality_metrics["complex_id__null_count"], 0);
    assert_eq!(handoff.quality_metrics["complex_id__empty_count"], 0);
    assert_eq!(
        handoff.quality_metrics["official_complex_code__empty_count"],
        0
    );
    assert_eq!(handoff.source_snapshot_count, 1);
    assert_eq!(
        handoff.source_snapshot_ids,
        vec!["synthetic-source-snapshot-industrial-complexes-20990101".to_owned()]
    );

    let lines = handoff.jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let record: JsonValue = serde_json::from_str(lines[0])?;
    assert_eq!(record["complex_id"], FIXTURE_COMPLEX_ID);
    assert_eq!(record["official_complex_code"], "SYNTHETIC-COMPLEX-001");
    assert_eq!(record["valid_from_utc"], FIXTURE_VALID_FROM_UTC);
    assert!(record["valid_to_utc"].is_null());
    assert_eq!(record["ingested_at_utc"], FIXTURE_INGESTED_AT_UTC);
    Ok(())
}

#[test]
fn rejects_foundation_platform_placeholder_official_complex_code() -> TestResult {
    let mut complex = sample_complex()?;
    complex.official_complex_code = format!("foundation-platform:{}", complex.id);

    let result = normalize_industrial_complex_silver_rows(&IndustrialComplexSilverRowsInput {
        complexes: std::slice::from_ref(&complex),
        source_snapshot_id: "synthetic-source-snapshot-industrial-complexes-20990101",
        ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
    });

    assert!(result
        .err()
        .ok_or("placeholder official code must be rejected")?
        .to_string()
        .contains("official_complex_code must be source-side"));
    Ok(())
}

fn sample_complex() -> TestResult<IndustrialComplex> {
    Ok(IndustrialComplex {
        id: ComplexId::new(Uuid::parse_str(FIXTURE_COMPLEX_ID)?),
        official_complex_code: "SYNTHETIC-COMPLEX-001".to_owned(),
        name: "Synthetic Industrial Complex Alpha".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: "9999900101".to_owned(),
        area_m2: 123_456,
        created_at: parse_utc(FIXTURE_VALID_FROM_UTC)?,
        updated_at: parse_utc(FIXTURE_VALID_FROM_UTC)?,
        archived_at: None,
        version: 1,
    })
}

fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}
