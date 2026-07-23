//! Contract tests for `VWorld` cadastral Silver parcel-boundary row normalization.

use chrono::{DateTime, Utc};
use collection_domain::dedupe_vworld_cadastral_features_by_pnu;
use lakehouse_application::{
    build_vworld_cadastral_silver_parcel_boundary_handoff,
    normalize_vworld_cadastral_silver_parcel_boundary_rows,
    VWorldCadastralSilverParcelBoundaryRowsInput,
};
use lakehouse_domain::SILVER_PARCEL_BOUNDARIES;
use serde_json::json;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FIXTURE_VALID_FROM_UTC: &str = "2099-01-01T00:00:00Z";
const FIXTURE_INGESTED_AT_UTC: &str = "2099-01-01T00:00:01Z";

#[test]
fn normalizes_deduped_vworld_features_into_silver_parcel_boundary_rows() -> TestResult {
    let report =
        dedupe_vworld_cadastral_features_by_pnu(&[payload_with_features(&[cadastral_feature(
            "9999900801105800001",
            "580-1",
        )])])?;
    let valid_from_utc = parse_utc(FIXTURE_VALID_FROM_UTC)?;
    let ingested_at_utc = parse_utc(FIXTURE_INGESTED_AT_UTC)?;

    let rows = normalize_vworld_cadastral_silver_parcel_boundary_rows(
        &VWorldCadastralSilverParcelBoundaryRowsInput {
            records: &report.records,
            source_record_id: "synthetic-source-record-0001",
            source_snapshot_id: "synthetic-source-snapshot-0001",
            valid_from_utc,
            ingested_at_utc,
        },
    )?;

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row.boundary_id,
        "vworld-cadastral:parcel-boundary:pnu:9999900801105800001"
    );
    assert_eq!(row.pnu, "9999900801105800001");
    assert_eq!(row.sido_code, "99");
    assert_eq!(row.sigungu_code, "99999");
    assert_eq!(row.bjdong_code, "9999900801");
    assert_eq!(row.jibun.as_deref(), Some("580-1"));
    assert_eq!(row.bonbun.as_deref(), Some("0580"));
    assert_eq!(row.bubun.as_deref(), Some("0001"));
    assert_eq!(row.geometry_srid, 4326);
    assert_close(row.bbox.min_x, 127.123_430);
    assert_close(row.bbox.min_y, 36.123_440_0);
    assert_close(row.bbox.max_x, 127.123_431);
    assert_close(row.bbox.max_y, 36.123_441_0);
    assert!(row
        .geometry_wkb
        .starts_with(&[1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 3, 0, 0, 0]));
    assert_eq!(row.geometry_checksum_sha256.len(), 64);
    assert!(row
        .geometry_checksum_sha256
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(row.source_record_id, "synthetic-source-record-0001");
    assert_eq!(row.source_snapshot_id, "synthetic-source-snapshot-0001");
    assert_eq!(row.valid_from_utc, valid_from_utc);
    assert_eq!(row.ingested_at_utc, ingested_at_utc);
    Ok(())
}

#[test]
fn silver_parcel_boundary_normalization_rejects_non_polygon_geometry() -> TestResult {
    let report = dedupe_vworld_cadastral_features_by_pnu(&[payload_with_features(&[json!({
        "type": "Feature",
        "properties": {
            "pnu": "9999900801105800001"
        },
        "geometry": {
            "type": "Point",
            "coordinates": [127.123_430, 36.123_440]
        }
    })])])?;

    let error = normalize_vworld_cadastral_silver_parcel_boundary_rows(
        &VWorldCadastralSilverParcelBoundaryRowsInput {
            records: &report.records,
            source_record_id: "synthetic-source-record-0001",
            source_snapshot_id: "synthetic-source-snapshot-0001",
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        },
    )
    .err()
    .ok_or("non-polygon cadastral geometry must be rejected")?;

    assert!(
        error
            .to_string()
            .contains("unsupported geometry type Point"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn builds_writer_neutral_jsonl_handoff_for_silver_parcel_boundaries() -> TestResult {
    let rows = sample_silver_rows()?;

    let handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&rows)?;

    assert_eq!(handoff.contract_table_name, "silver.parcel_boundaries");
    assert_eq!(
        handoff.table_columns,
        SILVER_PARCEL_BOUNDARIES
            .columns
            .iter()
            .map(|column| column.name.to_owned())
            .collect::<Vec<_>>()
    );
    assert!(handoff
        .transport_columns
        .contains(&"geometry_wkb_hex".to_owned()));
    assert_eq!(handoff.quality_metrics["row_count"], 1);
    assert_eq!(handoff.quality_metrics["pnu__null_count"], 0);
    assert_eq!(handoff.quality_metrics["pnu__empty_count"], 0);
    assert_eq!(handoff.quality_metrics["geometry_wkb__null_count"], 0);
    assert_eq!(handoff.quality_metrics["invalid_bbox_count"], 0);
    assert_eq!(handoff.quality_metrics["invalid_checksum_count"], 0);
    assert_eq!(handoff.source_snapshot_count, 1);
    assert_eq!(
        handoff.source_snapshot_ids,
        vec!["synthetic-source-snapshot-0001".to_owned()]
    );

    let lines = handoff.jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let record: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(record["pnu"], "9999900801105800001");
    assert_eq!(record["geometry_wkb_encoding"], "hex");
    assert!(record["geometry_wkb"].is_null());
    assert!(record["geometry_wkb_hex"]
        .as_str()
        .ok_or("geometry_wkb_hex string")?
        .starts_with("0106000000"));
    assert_eq!(record["valid_to_utc"], serde_json::Value::Null);
    Ok(())
}

fn payload_with_features(features: &[serde_json::Value]) -> serde_json::Value {
    json!({
        "response": {
            "result": {
                "featureCollection": {
                    "features": features
                }
            }
        }
    })
}

fn cadastral_feature(pnu: &str, jibun: &str) -> serde_json::Value {
    json!({
        "type": "Feature",
        "properties": {
            "pnu": pnu,
            "jibun": jibun,
            "bonbun": "0580",
            "bubun": "0001"
        },
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[
                [127.123_430, 36.123_440_0],
                [127.123_431, 36.123_440_0],
                [127.123_431, 36.123_441_0],
                [127.123_430, 36.123_441_0],
                [127.123_430, 36.123_440_0]
            ]]]
        }
    })
}

fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "expected {actual} to be close to {expected}"
    );
}

fn sample_silver_rows(
) -> TestResult<Vec<lakehouse_application::VWorldCadastralSilverParcelBoundaryRow>> {
    let report =
        dedupe_vworld_cadastral_features_by_pnu(&[payload_with_features(&[cadastral_feature(
            "9999900801105800001",
            "580-1",
        )])])?;

    Ok(normalize_vworld_cadastral_silver_parcel_boundary_rows(
        &VWorldCadastralSilverParcelBoundaryRowsInput {
            records: &report.records,
            source_record_id: "synthetic-source-record-0001",
            source_snapshot_id: "synthetic-source-snapshot-0001",
            valid_from_utc: parse_utc(FIXTURE_VALID_FROM_UTC)?,
            ingested_at_utc: parse_utc(FIXTURE_INGESTED_AT_UTC)?,
        },
    )?)
}
