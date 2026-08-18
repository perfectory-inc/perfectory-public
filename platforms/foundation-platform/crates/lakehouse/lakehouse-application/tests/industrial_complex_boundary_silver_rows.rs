//! Contract tests for industrial-complex Silver boundary row normalization.

use chrono::{DateTime, Utc};
use lakehouse_application::{
    build_industrial_complex_boundary_silver_handoff,
    normalize_industrial_complex_boundary_silver_rows, GeoPoint,
    IndustrialComplexBoundarySilverRowsInput, IndustrialComplexBoundarySource,
    ParsedPolygonalGeometry,
};
use lakehouse_domain::SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES;
use serde_json::Value as JsonValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FIXTURE_VALID_FROM_UTC: &str = "2099-01-01T00:00:00Z";
const FIXTURE_INGESTED_AT_UTC: &str = "2099-01-01T00:00:01Z";

/// A synthetic complex code shaped like the source's six characters without being one.
const FIXTURE_COMPLEX_CODE: &str = "999ZZ0";

#[test]
fn normalizes_a_boundary_into_a_silver_row() -> TestResult {
    let rows = normalize(&[boundary(
        FIXTURE_COMPLEX_CODE,
        ParsedPolygonalGeometry::Polygon(vec![square(200_000.0, 500_000.0, 300.0)]),
    )])?;

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row.boundary_id,
        "vworldkr-sandan-boundary:complex-boundary:official:999ZZ0"
    );
    assert_eq!(row.official_complex_code, FIXTURE_COMPLEX_CODE);
    assert_eq!(row.boundary_kind, "official");
    // The source is Korea 2000 Central Belt in metres and stays that way (root ADR-0042).
    assert_eq!(row.geometry_srid, 5186);
    assert_close(row.bbox.min_x, 200_000.0);
    assert_close(row.bbox.min_y, 500_000.0);
    assert_close(row.bbox.max_x, 200_300.0);
    assert_close(row.bbox.max_y, 500_300.0);
    assert_close(row.centroid.x, 200_150.0);
    assert_close(row.centroid.y, 500_150.0);
    // 300 m on a side, in a CRS whose unit is the metre.
    assert_close(row.area_sqm_calculated, 90_000.0);
    assert!(row.geometry_wkb.starts_with(&[1, 3, 0, 0, 0, 1, 0, 0, 0]));
    assert_eq!(row.geometry_checksum_sha256.len(), 64);
    assert!(row
        .geometry_checksum_sha256
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(row.valid_to_utc, None);
    Ok(())
}

/// A hole is subtracted from the area, which is the whole reason the reader has to know which ring
/// is which before it gets here.
#[test]
fn a_hole_reduces_the_calculated_area() -> TestResult {
    let rows = normalize(&[boundary(
        FIXTURE_COMPLEX_CODE,
        ParsedPolygonalGeometry::Polygon(vec![
            square(0.0, 0.0, 300.0),
            square(100.0, 100.0, 100.0),
        ]),
    )])?;

    assert_close(rows[0].area_sqm_calculated, 80_000.0);
    Ok(())
}

/// The handoff carries every contract column plus the three transport-only fields, and the two
/// columns the writer fills travel as JSON null rather than as a guess.
#[test]
fn the_handoff_leaves_the_joined_columns_null_and_carries_the_join_key() -> TestResult {
    let rows = normalize(&[boundary(
        FIXTURE_COMPLEX_CODE,
        ParsedPolygonalGeometry::Polygon(vec![square(200_000.0, 500_000.0, 300.0)]),
    )])?;

    let handoff = build_industrial_complex_boundary_silver_handoff(&rows)?;

    assert_eq!(
        handoff.contract_table_name,
        SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES.table_name
    );
    let contract_columns = SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect::<Vec<_>>();
    assert_eq!(handoff.table_columns, contract_columns);
    for column in &contract_columns {
        assert!(
            handoff.transport_columns.contains(column),
            "transport is missing contract column {column}"
        );
    }
    for column in [
        "official_complex_code",
        "geometry_wkb_hex",
        "geometry_wkb_encoding",
    ] {
        assert!(
            handoff.transport_columns.iter().any(|name| name == column),
            "transport is missing {column}"
        );
    }

    let record: JsonValue = serde_json::from_str(handoff.jsonl.trim_end())?;
    assert_eq!(record.get("complex_id"), Some(&JsonValue::Null));
    assert_eq!(record.get("sido_code"), Some(&JsonValue::Null));
    assert_eq!(record.get("geometry_wkb"), Some(&JsonValue::Null));
    assert_eq!(
        record
            .get("official_complex_code")
            .and_then(JsonValue::as_str),
        Some(FIXTURE_COMPLEX_CODE)
    );
    assert_eq!(
        record
            .get("geometry_wkb_encoding")
            .and_then(JsonValue::as_str),
        Some("hex")
    );
    assert_eq!(
        record.get("geometry_srid").and_then(JsonValue::as_i64),
        Some(5186)
    );
    let hex = record
        .get("geometry_wkb_hex")
        .and_then(JsonValue::as_str)
        .ok_or("geometry_wkb_hex must be a string")?;
    assert_eq!(hex.len(), rows[0].geometry_wkb.len() * 2);
    Ok(())
}

/// The two columns the writer fills must not be counted as this stage's null defects; every other
/// required column still is.
#[test]
fn quality_metrics_do_not_charge_this_stage_for_the_writers_columns() -> TestResult {
    let rows = normalize(&[boundary(
        FIXTURE_COMPLEX_CODE,
        ParsedPolygonalGeometry::Polygon(vec![square(200_000.0, 500_000.0, 300.0)]),
    )])?;

    let handoff = build_industrial_complex_boundary_silver_handoff(&rows)?;

    assert_eq!(handoff.quality_metrics.get("row_count"), Some(&1));
    assert_eq!(handoff.quality_metrics.get("invalid_bbox_count"), Some(&0));
    assert_eq!(
        handoff.quality_metrics.get("centroid_outside_bbox_count"),
        Some(&0)
    );
    assert_eq!(
        handoff.quality_metrics.get("invalid_checksum_count"),
        Some(&0)
    );
    assert!(!handoff
        .quality_metrics
        .contains_key("complex_id__null_count"));
    assert!(!handoff
        .quality_metrics
        .contains_key("sido_code__null_count"));
    assert!(handoff
        .quality_metrics
        .contains_key("boundary_id__null_count"));
    Ok(())
}

/// A shape with no area cannot state where a complex is, so it fails rather than travelling with a
/// centroid nobody can compute.
#[test]
fn a_boundary_that_encloses_no_area_is_rejected() {
    let collinear = vec![vec![
        GeoPoint { x: 0.0, y: 0.0 },
        GeoPoint { x: 1.0, y: 0.0 },
        GeoPoint { x: 2.0, y: 0.0 },
        GeoPoint { x: 0.0, y: 0.0 },
    ]];

    let error = normalize(&[boundary(
        FIXTURE_COMPLEX_CODE,
        ParsedPolygonalGeometry::Polygon(collinear),
    )])
    .map(|_| ())
    .expect_err("a zero-area boundary must be rejected");

    assert!(format!("{error}").contains("encloses no area"), "{error}");
}

#[test]
fn empty_lineage_is_rejected() {
    let boundaries = [boundary(
        FIXTURE_COMPLEX_CODE,
        ParsedPolygonalGeometry::Polygon(vec![square(0.0, 0.0, 10.0)]),
    )];

    let error = normalize_industrial_complex_boundary_silver_rows(
        &IndustrialComplexBoundarySilverRowsInput {
            boundaries: &boundaries,
            source_record_id: "",
            source_snapshot_id: "synthetic-source-snapshot-0001",
            valid_from_utc: Utc::now(),
            ingested_at_utc: Utc::now(),
        },
    )
    .map(|_| ())
    .expect_err("an empty source_record_id must be rejected");

    assert!(format!("{error}").contains("source_record_id"), "{error}");
}

fn normalize(
    boundaries: &[IndustrialComplexBoundarySource],
) -> TestResult<Vec<lakehouse_application::IndustrialComplexBoundarySilverRow>> {
    Ok(normalize_industrial_complex_boundary_silver_rows(
        &IndustrialComplexBoundarySilverRowsInput {
            boundaries,
            source_record_id: "bronze/source=vworldkr__sandan_boundary/synthetic-0001.zip",
            source_snapshot_id: "synthetic-source-snapshot-0001",
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        },
    )?)
}

fn boundary(
    official_complex_code: &str,
    geometry: ParsedPolygonalGeometry,
) -> IndustrialComplexBoundarySource {
    IndustrialComplexBoundarySource {
        official_complex_code: official_complex_code.to_owned(),
        geometry,
    }
}

/// A closed counter-clockwise square; ring roles here are positional, not winding-based.
fn square(x0: f64, y0: f64, side: f64) -> Vec<GeoPoint> {
    [
        (x0, y0),
        (x0 + side, y0),
        (x0 + side, y0 + side),
        (x0, y0 + side),
        (x0, y0),
    ]
    .into_iter()
    .map(|(x, y)| GeoPoint { x, y })
    .collect()
}

fn parse_utc(value: &str) -> TestResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected {expected}, got {actual}"
    );
}
