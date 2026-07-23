//! Use-case tests for recording Spark lakehouse batch summaries.

use std::sync::Mutex;

use async_trait::async_trait;
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::{
    ports::{LakehouseBatchRunAudit, LakehouseBatchRunAuditCommand},
    RecordLakehouseBatchRun, RecordLakehouseBatchRunInput,
};
use lakehouse_domain::LakehouseError;
use uuid::Uuid;

#[derive(Default)]
struct RecordingAudit {
    contracts: Mutex<Vec<String>>,
    recorded_by_staff_ids: Mutex<Vec<StaffId>>,
    request_ids: Mutex<Vec<Option<String>>>,
}

#[async_trait]
impl LakehouseBatchRunAudit for RecordingAudit {
    async fn record_spark_run_summary(
        &self,
        command: LakehouseBatchRunAuditCommand,
    ) -> Result<(), LakehouseError> {
        self.contracts
            .lock()
            .map_err(|_| LakehouseError::Persistence("audit mutex poisoned".to_owned()))?
            .push(command.summary.contract.clone());
        self.recorded_by_staff_ids
            .lock()
            .map_err(|_| LakehouseError::Persistence("staff id audit mutex poisoned".to_owned()))?
            .push(command.recorded_by_staff_id);
        self.request_ids
            .lock()
            .map_err(|_| LakehouseError::Persistence("request id audit mutex poisoned".to_owned()))?
            .push(command.request_id);
        Ok(())
    }
}

const fn valid_summary_json() -> &'static str {
    r#"{
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
    }"#
}

fn recorded_contracts(audit: &RecordingAudit) -> Result<Vec<String>, LakehouseError> {
    audit
        .contracts
        .lock()
        .map_err(|_| LakehouseError::Persistence("audit mutex poisoned".to_owned()))
        .map(|contracts| contracts.clone())
}

fn recorded_staff_ids(audit: &RecordingAudit) -> Result<Vec<StaffId>, LakehouseError> {
    audit
        .recorded_by_staff_ids
        .lock()
        .map_err(|_| LakehouseError::Persistence("staff id audit mutex poisoned".to_owned()))
        .map(|staff_ids| staff_ids.clone())
}

fn recorded_request_ids(audit: &RecordingAudit) -> Result<Vec<Option<String>>, LakehouseError> {
    audit
        .request_ids
        .lock()
        .map_err(|_| LakehouseError::Persistence("request id audit mutex poisoned".to_owned()))
        .map(|request_ids| request_ids.clone())
}

fn record_input(
    summary_json: impl Into<String>,
    staff_id: StaffId,
    request_id: Option<String>,
) -> RecordLakehouseBatchRunInput {
    RecordLakehouseBatchRunInput {
        summary_json: summary_json.into(),
        recorded_by_staff_id: staff_id,
        request_id,
    }
}

#[tokio::test]
async fn records_valid_summary_after_contract_validation() -> Result<(), LakehouseError> {
    let audit = std::sync::Arc::new(RecordingAudit::default());
    let use_case = RecordLakehouseBatchRun::new(audit.clone());
    let staff_id = StaffId::new(Uuid::now_v7());

    let summary = use_case
        .execute(record_input(
            valid_summary_json(),
            staff_id,
            Some(" request-1 ".to_owned()),
        ))
        .await?;

    assert_eq!(summary.contract, "silver.industrial_complexes");
    assert_eq!(recorded_contracts(&audit)?, ["silver.industrial_complexes"]);
    assert_eq!(recorded_staff_ids(&audit)?, [staff_id]);
    assert_eq!(
        recorded_request_ids(&audit)?,
        [Some("request-1".to_owned())]
    );
    Ok(())
}

#[tokio::test]
async fn rejects_invalid_summary_without_recording_audit() -> Result<(), LakehouseError> {
    let audit = std::sync::Arc::new(RecordingAudit::default());
    let use_case = RecordLakehouseBatchRun::new(audit.clone());
    let invalid_json =
        valid_summary_json().replace("\"invalid_status_count\": 0", "\"invalid_status_count\": 1");

    let result = use_case
        .execute(record_input(
            invalid_json,
            StaffId::new(Uuid::now_v7()),
            Some("request-2".to_owned()),
        ))
        .await;

    assert!(matches!(
        result,
        Err(LakehouseError::InvalidLakehouseBatchRun(_))
    ));
    assert!(recorded_contracts(&audit)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_unknown_contract_without_recording_audit() -> Result<(), LakehouseError> {
    let audit = std::sync::Arc::new(RecordingAudit::default());
    let use_case = RecordLakehouseBatchRun::new(audit.clone());
    let invalid_json = valid_summary_json().replace(
        "\"contract\": \"silver.industrial_complexes\"",
        "\"contract\": \"silver.unknown_table\"",
    );

    let result = use_case
        .execute(record_input(
            invalid_json,
            StaffId::new(Uuid::now_v7()),
            Some("request-3".to_owned()),
        ))
        .await;

    assert!(matches!(
        result,
        Err(LakehouseError::InvalidLakehouseBatchRun(_))
    ));
    assert!(recorded_contracts(&audit)?.is_empty());
    Ok(())
}
