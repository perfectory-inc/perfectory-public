//! Contract tests for Spark batch run summaries handed back to foundation-platform.

#![recursion_limit = "256"]

use std::error::Error;

use lakehouse_domain::{
    LakehouseTableContract, SparkRunSummary, SparkRunSummaryError, SILVER_BUILDING_REGISTER_FLOORS,
    SILVER_BUILDING_REGISTER_UNITS, SILVER_INDUSTRIAL_COMPLEXES, SILVER_PARCEL_BOUNDARIES,
};
use serde_json::{json, Value};

type TestResult = Result<(), Box<dyn Error>>;

fn valid_summary_value() -> Value {
    json!({
        "schema_version": "foundation-platform.spark_run_summary.v1",
        "job_name": "industrial_complex_bronze_to_silver",
        "contract": "silver.industrial_complexes",
        "created_at_utc": "2026-05-14T05:27:05Z",
        "input": {
            "kind": "bronze_jsonl",
            "path": "/workspace/infra/lakehouse/spark/fixtures/bronze/industrial_complexes.jsonl"
        },
        "target": {
            "kind": "parquet",
            "path": "/workspace/target/lakehouse/smoke/silver/industrial_complexes"
        },
        "write_mode": "parquet",
        "write_disposition": "parquet_overwrite",
        "row_count": 2,
        "persisted_row_count": 2,
        "quality_metrics": {
            "row_count": 2,
            "complex_id__null_count": 0,
            "complex_id__empty_count": 0,
            "official_complex_code__null_count": 0,
            "official_complex_code__empty_count": 0,
            "complex_name__null_count": 0,
            "complex_name__empty_count": 0,
            "complex_name_normalized__null_count": 0,
            "complex_name_normalized__empty_count": 0,
            "complex_kind__null_count": 0,
            "complex_kind__empty_count": 0,
            "status__null_count": 0,
            "status__empty_count": 0,
            "sido_code__null_count": 0,
            "sido_code__empty_count": 0,
            "sigungu_code__null_count": 0,
            "sigungu_code__empty_count": 0,
            "source_record_id__null_count": 0,
            "source_record_id__empty_count": 0,
            "source_snapshot_id__null_count": 0,
            "source_snapshot_id__empty_count": 0,
            "valid_from_utc__null_count": 0,
            "ingested_at_utc__null_count": 0,
            "row_checksum_sha256__null_count": 0,
            "row_checksum_sha256__empty_count": 0,
            "invalid_complex_kind_count": 0,
            "invalid_status_count": 0,
            "invalid_official_area_count": 0,
            "invalid_complex_id_count": 0,
            "invalid_checksum_count": 0
        },
        "column_count": 21,
        "columns": [
            "complex_id",
            "official_complex_code",
            "complex_name",
            "complex_name_normalized",
            "complex_kind",
            "status",
            "sido_code",
            "sigungu_code",
            "primary_bjdong_code",
            "address_text",
            "management_agency_name",
            "developer_name",
            "designated_date",
            "completion_date",
            "official_area_sqm",
            "source_record_id",
            "source_snapshot_id",
            "valid_from_utc",
            "valid_to_utc",
            "ingested_at_utc",
            "row_checksum_sha256"
        ],
        "required_columns": [
            "complex_id",
            "official_complex_code",
            "complex_name",
            "complex_name_normalized",
            "complex_kind",
            "status",
            "sido_code",
            "sigungu_code",
            "source_record_id",
            "source_snapshot_id",
            "valid_from_utc",
            "ingested_at_utc",
            "row_checksum_sha256"
        ],
        "source_snapshot_count": 1,
        "source_snapshot_ids": ["bronze-snapshot-2026-05-14"],
        "source_snapshot_truncated": false
    })
}

fn parse_summary(value: Value) -> Result<SparkRunSummary, SparkRunSummaryError> {
    SparkRunSummary::from_json_value(value)
}

fn valid_parcel_boundary_summary_value() -> Value {
    json!({
        "schema_version": "foundation-platform.spark_run_summary.v1",
        "job_name": "vworld_parcel_boundaries_handoff_to_silver",
        "contract": "silver.parcel_boundaries",
        "created_at_utc": "2026-05-18T05:27:05Z",
        "input": {
            "kind": "silver_handoff_jsonl",
            "path": "/workspace/infra/lakehouse/spark/fixtures/silver_handoff/vworld_parcel_boundaries.jsonl"
        },
        "target": {
            "kind": "parquet",
            "path": "/workspace/target/lakehouse/smoke/silver/parcel_boundaries"
        },
        "write_mode": "parquet",
        "write_disposition": "parquet_overwrite",
        "row_count": 1,
        "persisted_row_count": 1,
        "quality_metrics": {
            "row_count": 1,
            "boundary_id__null_count": 0,
            "boundary_id__empty_count": 0,
            "pnu__null_count": 0,
            "pnu__empty_count": 0,
            "sido_code__null_count": 0,
            "sido_code__empty_count": 0,
            "sigungu_code__null_count": 0,
            "sigungu_code__empty_count": 0,
            "bjdong_code__null_count": 0,
            "bjdong_code__empty_count": 0,
            "geometry_wkb__null_count": 0,
            "geometry_srid__null_count": 0,
            "bbox_min_x__null_count": 0,
            "bbox_min_y__null_count": 0,
            "bbox_max_x__null_count": 0,
            "bbox_max_y__null_count": 0,
            "geometry_checksum_sha256__null_count": 0,
            "geometry_checksum_sha256__empty_count": 0,
            "source_record_id__null_count": 0,
            "source_record_id__empty_count": 0,
            "source_snapshot_id__null_count": 0,
            "source_snapshot_id__empty_count": 0,
            "valid_from_utc__null_count": 0,
            "ingested_at_utc__null_count": 0,
            "invalid_pnu_count": 0,
            "invalid_code_derivation_count": 0,
            "invalid_geometry_srid_count": 0,
            "invalid_geometry_encoding_count": 0,
            "invalid_geometry_wkb_hex_count": 0,
            "invalid_geometry_wkb_count": 0,
            "invalid_bbox_count": 0,
            "invalid_checksum_count": 0,
            "duplicate_active_pnu_count": 0
        },
        "column_count": 20,
        "columns": [
            "boundary_id",
            "pnu",
            "sido_code",
            "sigungu_code",
            "bjdong_code",
            "jibun",
            "bonbun",
            "bubun",
            "geometry_wkb",
            "geometry_srid",
            "bbox_min_x",
            "bbox_min_y",
            "bbox_max_x",
            "bbox_max_y",
            "geometry_checksum_sha256",
            "source_record_id",
            "source_snapshot_id",
            "valid_from_utc",
            "valid_to_utc",
            "ingested_at_utc"
        ],
        "required_columns": [
            "boundary_id",
            "pnu",
            "sido_code",
            "sigungu_code",
            "bjdong_code",
            "geometry_wkb",
            "geometry_srid",
            "bbox_min_x",
            "bbox_min_y",
            "bbox_max_x",
            "bbox_max_y",
            "geometry_checksum_sha256",
            "source_record_id",
            "source_snapshot_id",
            "valid_from_utc",
            "ingested_at_utc"
        ],
        "source_snapshot_count": 1,
        "source_snapshot_ids": ["bronze-vworld-cadastral-run-018f"],
        "source_snapshot_truncated": false
    })
}

fn valid_building_register_floor_summary_value() -> Value {
    let columns = table_columns(&SILVER_BUILDING_REGISTER_FLOORS);
    let required_columns = required_table_columns(&SILVER_BUILDING_REGISTER_FLOORS);
    let mut quality_metrics = required_column_quality_metrics(&SILVER_BUILDING_REGISTER_FLOORS, 2);
    quality_metrics.insert("proposal_required_count".to_owned(), json!(1));
    quality_metrics.insert("invalid_checksum_count".to_owned(), json!(0));

    json!({
        "schema_version": "foundation-platform.spark_run_summary.v1",
        "job_name": "building_register_floor_handoff_to_silver",
        "contract": "silver.building_register_floors",
        "created_at_utc": "2026-07-01T05:27:05Z",
        "input": {
            "kind": "silver_handoff_jsonl",
            "path": "/workspace/target/lakehouse/silver_handoff/building_register_floors.jsonl"
        },
        "target": {
            "kind": "parquet",
            "path": "/workspace/target/lakehouse/smoke/silver/building_register_floors"
        },
        "write_mode": "parquet",
        "write_disposition": "parquet_overwrite",
        "row_count": 2,
        "persisted_row_count": 2,
        "quality_metrics": quality_metrics,
        "column_count": columns.len(),
        "columns": columns,
        "required_columns": required_columns,
        "source_snapshot_count": 1,
        "source_snapshot_ids": ["smoke-datagokr-building-register-floor-overview-11680-10300-page-000001"],
        "source_snapshot_truncated": false
    })
}

fn valid_building_register_unit_summary_value() -> Value {
    let columns = table_columns(&SILVER_BUILDING_REGISTER_UNITS);
    let required_columns = required_table_columns(&SILVER_BUILDING_REGISTER_UNITS);
    let mut quality_metrics = required_column_quality_metrics(&SILVER_BUILDING_REGISTER_UNITS, 2);
    quality_metrics.insert("proposal_required_count".to_owned(), json!(1));
    quality_metrics.insert("invalid_checksum_count".to_owned(), json!(0));

    json!({
        "schema_version": "foundation-platform.spark_run_summary.v1",
        "job_name": "building_register_unit_handoff_to_silver",
        "contract": "silver.building_register_units",
        "created_at_utc": "2026-07-06T05:27:05Z",
        "input": {
            "kind": "silver_handoff_jsonl",
            "path": "/workspace/target/lakehouse/silver_handoff/building_register_units.jsonl"
        },
        "target": {
            "kind": "parquet",
            "path": "/workspace/target/lakehouse/smoke/silver/building_register_units"
        },
        "write_mode": "parquet",
        "write_disposition": "parquet_overwrite",
        "row_count": 2,
        "persisted_row_count": 2,
        "quality_metrics": quality_metrics,
        "column_count": columns.len(),
        "columns": columns,
        "required_columns": required_columns,
        "source_snapshot_count": 1,
        "source_snapshot_ids": ["smoke-hubgokr-building-register-exclusive-unit"],
        "source_snapshot_truncated": false
    })
}

#[test]
fn spark_run_summary_validates_against_silver_contract() -> TestResult {
    let summary = parse_summary(valid_summary_value())?;

    summary.validate_for_contract(&SILVER_INDUSTRIAL_COMPLEXES)?;

    assert_eq!(summary.contract, "silver.industrial_complexes");
    assert_eq!(summary.row_count, 2);
    Ok(())
}

#[test]
fn spark_run_summary_accepts_silver_handoff_input_for_parcel_boundaries() -> TestResult {
    let summary = parse_summary(valid_parcel_boundary_summary_value())?;

    summary.validate_for_contract(&SILVER_PARCEL_BOUNDARIES)?;

    assert_eq!(summary.contract, "silver.parcel_boundaries");
    assert_eq!(summary.input.kind, "silver_handoff_jsonl");
    Ok(())
}

#[test]
fn spark_run_summary_accepts_building_register_floor_contract_with_proposal_metrics() -> TestResult
{
    let summary = parse_summary(valid_building_register_floor_summary_value())?;

    summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS)?;

    assert_eq!(summary.contract, "silver.building_register_floors");
    assert_eq!(summary.input.kind, "silver_handoff_jsonl");
    Ok(())
}

#[test]
fn spark_run_summary_accepts_parquet_silver_handoff_input() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["input"]["kind"] = json!("silver_handoff_parquet");
    value["input"]["path"] =
        json!("/workspace/target/lakehouse/silver_handoff/building_register_floors_hub_parquet");
    let summary = parse_summary(value)?;

    summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS)?;

    assert_eq!(summary.input.kind, "silver_handoff_parquet");
    Ok(())
}

#[test]
fn spark_run_summary_accepts_iceberg_smoke_target_for_overwrite() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["target"] = json!({
        "kind": "iceberg",
        "catalog": "r2",
        "namespace": "silver",
        "table": "building_register_floors_smoke",
        "qualified_table": "r2.silver.building_register_floors_smoke"
    });
    value["write_mode"] = json!("iceberg");
    value["write_disposition"] = json!("iceberg_overwrite");
    value["iceberg_readback_validation"] = json!("deferred");
    let summary = parse_summary(value)?;

    summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS)?;

    assert_eq!(
        summary
            .iceberg_readback_validation
            .map(lakehouse_domain::SparkRunIcebergReadbackValidation::as_str),
        Some("deferred")
    );
    Ok(())
}

#[test]
fn spark_run_summary_rejects_iceberg_smoke_target_for_append() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["target"] = json!({
        "kind": "iceberg",
        "catalog": "r2",
        "namespace": "silver",
        "table": "building_register_floors_smoke",
        "qualified_table": "r2.silver.building_register_floors_smoke"
    });
    value["write_mode"] = json!("iceberg");
    value["write_disposition"] = json!("iceberg_append");
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::ContractMismatch { .. })
    ));
    Ok(())
}

#[test]
fn building_register_floor_summary_requires_proposal_required_metric() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["quality_metrics"]
        .as_object_mut()
        .ok_or("quality_metrics object")?
        .remove("proposal_required_count");
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::MissingQualityMetric(metric))
            if metric == "proposal_required_count"
    ));
    Ok(())
}

#[test]
fn building_register_floor_summary_requires_invalid_checksum_metric() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["quality_metrics"]
        .as_object_mut()
        .ok_or("quality_metrics object")?
        .remove("invalid_checksum_count");
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::MissingQualityMetric(metric))
            if metric == "invalid_checksum_count"
    ));
    Ok(())
}

#[test]
fn building_register_floor_summary_allows_proposal_required_rows() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["quality_metrics"]["proposal_required_count"] = json!(2);
    let summary = parse_summary(value)?;

    summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS)?;

    assert_eq!(summary.row_count, 2);
    Ok(())
}

#[test]
fn building_register_floor_summary_blocks_invalid_checksums() -> TestResult {
    let mut value = valid_building_register_floor_summary_value();
    value["quality_metrics"]["invalid_checksum_count"] = json!(1);
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_FLOORS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::BlockingQualityMetric { metric, value })
            if metric == "invalid_checksum_count" && value == 1
    ));
    Ok(())
}

#[test]
fn spark_run_summary_accepts_building_register_unit_contract_with_proposal_metrics() -> TestResult {
    let summary = parse_summary(valid_building_register_unit_summary_value())?;

    summary.validate_for_contract(&SILVER_BUILDING_REGISTER_UNITS)?;

    assert_eq!(summary.contract, "silver.building_register_units");
    assert_eq!(summary.input.kind, "silver_handoff_jsonl");
    Ok(())
}

#[test]
fn building_register_unit_summary_requires_proposal_required_metric() -> TestResult {
    let mut value = valid_building_register_unit_summary_value();
    value["quality_metrics"]
        .as_object_mut()
        .ok_or("quality_metrics object")?
        .remove("proposal_required_count");
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_UNITS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::MissingQualityMetric(metric))
            if metric == "proposal_required_count"
    ));
    Ok(())
}

#[test]
fn building_register_unit_summary_requires_invalid_checksum_metric() -> TestResult {
    let mut value = valid_building_register_unit_summary_value();
    value["quality_metrics"]
        .as_object_mut()
        .ok_or("quality_metrics object")?
        .remove("invalid_checksum_count");
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_UNITS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::MissingQualityMetric(metric))
            if metric == "invalid_checksum_count"
    ));
    Ok(())
}

#[test]
fn building_register_unit_summary_allows_proposal_required_rows() -> TestResult {
    let mut value = valid_building_register_unit_summary_value();
    value["quality_metrics"]["proposal_required_count"] = json!(2);
    let summary = parse_summary(value)?;

    summary.validate_for_contract(&SILVER_BUILDING_REGISTER_UNITS)?;

    assert_eq!(summary.row_count, 2);
    Ok(())
}

#[test]
fn building_register_unit_summary_blocks_invalid_checksums() -> TestResult {
    let mut value = valid_building_register_unit_summary_value();
    value["quality_metrics"]["invalid_checksum_count"] = json!(1);
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_BUILDING_REGISTER_UNITS);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::BlockingQualityMetric { metric, value })
            if metric == "invalid_checksum_count" && value == 1
    ));
    Ok(())
}

#[test]
fn parcel_boundary_summary_requires_parcel_specific_quality_metrics() -> TestResult {
    let mut value = valid_parcel_boundary_summary_value();
    value["quality_metrics"]
        .as_object_mut()
        .ok_or("quality_metrics object")?
        .remove("invalid_geometry_wkb_count");
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_PARCEL_BOUNDARIES);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::MissingQualityMetric(metric))
            if metric == "invalid_geometry_wkb_count"
    ));
    Ok(())
}

#[test]
fn rejects_summary_when_column_contract_drifted() -> TestResult {
    let mut value = valid_summary_value();
    value["column_count"] = json!(1);
    value["columns"] = json!(["complex_id"]);
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_INDUSTRIAL_COMPLEXES);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::ColumnContractMismatch { .. })
    ));
    Ok(())
}

#[test]
fn rejects_summary_with_blocking_quality_metric() -> TestResult {
    let mut value = valid_summary_value();
    value["quality_metrics"]["invalid_status_count"] = json!(1);
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_INDUSTRIAL_COMPLEXES);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::BlockingQualityMetric { .. })
    ));
    Ok(())
}

#[test]
fn rejects_summary_with_truncated_lineage() -> TestResult {
    let mut value = valid_summary_value();
    value["source_snapshot_count"] = json!(65);
    value["source_snapshot_truncated"] = json!(true);
    let summary = parse_summary(value)?;

    let result = summary.validate_for_contract(&SILVER_INDUSTRIAL_COMPLEXES);

    assert!(matches!(
        result,
        Err(SparkRunSummaryError::TruncatedSourceLineage)
    ));
    Ok(())
}

fn table_columns(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

fn required_table_columns(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .filter(|column| column.required)
        .map(|column| column.name.to_owned())
        .collect()
}

fn required_column_quality_metrics(
    contract: &LakehouseTableContract,
    row_count: u64,
) -> serde_json::Map<String, Value> {
    let mut metrics = serde_json::Map::from_iter([("row_count".to_owned(), json!(row_count))]);
    for column in contract.columns.iter().filter(|column| column.required) {
        metrics.insert(format!("{}__null_count", column.name), json!(0));
        if column.logical_type == "string" {
            metrics.insert(format!("{}__empty_count", column.name), json!(0));
        }
    }
    metrics
}
