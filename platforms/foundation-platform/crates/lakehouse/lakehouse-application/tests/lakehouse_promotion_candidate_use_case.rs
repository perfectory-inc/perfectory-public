//! Use-case tests for selecting lakehouse batch promotion candidates.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::{
    ports::{LakehouseBatchRunRecord, LakehouseBatchRunRepository},
    GetLakehousePromotionCandidate,
};
use lakehouse_domain::{
    LakehouseError, LakehouseTableContract, SparkRunSummary, SparkRunTarget,
    SILVER_INDUSTRIAL_COMPLEXES,
};
use uuid::Uuid;

#[derive(Default)]
struct RecordingRepository {
    candidate: Mutex<Option<LakehouseBatchRunRecord>>,
    requested_contracts: Mutex<Vec<String>>,
}

#[async_trait]
impl LakehouseBatchRunRepository for RecordingRepository {
    async fn latest_promotion_candidate(
        &self,
        contract: &'static LakehouseTableContract,
    ) -> Result<Option<LakehouseBatchRunRecord>, LakehouseError> {
        self.requested_contracts
            .lock()
            .map_err(|_| {
                LakehouseError::Persistence("requested contracts mutex poisoned".to_owned())
            })?
            .push(contract.table_name.to_owned());
        self.candidate
            .lock()
            .map_err(|_| LakehouseError::Persistence("candidate mutex poisoned".to_owned()))
            .map(|candidate| candidate.clone())
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

fn parsed_utc(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}

fn valid_summary() -> Result<SparkRunSummary, LakehouseError> {
    SparkRunSummary::from_json_str(valid_summary_json())
        .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))
}

fn promotion_record(
    summary: SparkRunSummary,
) -> Result<LakehouseBatchRunRecord, chrono::ParseError> {
    Ok(LakehouseBatchRunRecord {
        id: Uuid::now_v7(),
        schema_version: summary.schema_version.clone(),
        job_name: summary.job_name.clone(),
        contract: summary.contract.clone(),
        created_at_utc: summary.created_at_utc,
        write_disposition: summary.write_disposition,
        row_count: summary.row_count,
        persisted_row_count: summary.persisted_row_count,
        source_snapshot_ids: summary.source_snapshot_ids.clone(),
        summary,
        recorded_by_staff_id: StaffId::new(Uuid::now_v7()),
        request_id: Some("promotion-candidate-test".to_owned()),
        recorded_at_utc: parsed_utc("2026-05-14T06:00:00Z")?,
    })
}

fn target_path(record: &LakehouseBatchRunRecord) -> Option<&str> {
    match &record.summary.target {
        SparkRunTarget::Parquet { path } => Some(path),
        SparkRunTarget::Iceberg { .. } => None,
    }
}

fn requested_contracts(repository: &RecordingRepository) -> Result<Vec<String>, LakehouseError> {
    repository
        .requested_contracts
        .lock()
        .map_err(|_| LakehouseError::Persistence("requested contracts mutex poisoned".to_owned()))
        .map(|contracts| contracts.clone())
}

#[tokio::test]
async fn returns_latest_candidate_after_revalidating_stored_summary(
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = Arc::new(RecordingRepository::default());
    let expected_record = promotion_record(valid_summary()?)?;
    *repository
        .candidate
        .lock()
        .map_err(|_| LakehouseError::Persistence("candidate mutex poisoned".to_owned()))? =
        Some(expected_record.clone());
    let use_case = GetLakehousePromotionCandidate::new(repository.clone());

    let record = use_case
        .execute(SILVER_INDUSTRIAL_COMPLEXES.table_name)
        .await?
        .ok_or_else(|| LakehouseError::Persistence("promotion candidate missing".to_owned()))?;

    assert_eq!(record.contract, SILVER_INDUSTRIAL_COMPLEXES.table_name);
    assert_eq!(record.row_count, 2);
    assert_eq!(
        target_path(&record),
        Some("/workspace/target/lakehouse/smoke/silver/industrial_complexes")
    );
    assert_eq!(
        requested_contracts(&repository)?,
        [SILVER_INDUSTRIAL_COMPLEXES.table_name]
    );
    Ok(())
}

#[tokio::test]
async fn rejects_unknown_contract_without_querying_repository() -> Result<(), LakehouseError> {
    let repository = Arc::new(RecordingRepository::default());
    let use_case = GetLakehousePromotionCandidate::new(repository.clone());

    let result = use_case.execute("silver.unknown_table").await;

    assert!(matches!(
        result,
        Err(LakehouseError::InvalidLakehouseBatchRun(_))
    ));
    assert!(requested_contracts(&repository)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejects_repository_candidate_when_stored_summary_no_longer_matches_contract(
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = Arc::new(RecordingRepository::default());
    let mut summary = valid_summary()?;
    summary.contract = "silver.industrial_complex_boundaries".to_owned();
    let record = promotion_record(summary)?;
    *repository
        .candidate
        .lock()
        .map_err(|_| LakehouseError::Persistence("candidate mutex poisoned".to_owned()))? =
        Some(record);
    let use_case = GetLakehousePromotionCandidate::new(repository);

    let result = use_case
        .execute(SILVER_INDUSTRIAL_COMPLEXES.table_name)
        .await;

    assert!(matches!(
        result,
        Err(LakehouseError::InvalidLakehouseBatchRun(_))
    ));
    Ok(())
}
