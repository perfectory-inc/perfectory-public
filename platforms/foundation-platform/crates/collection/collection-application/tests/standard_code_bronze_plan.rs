//! Contract tests for data.go.kr 행정표준코드 (`getStanReginCdList`) Bronze snapshot planning.

use chrono::NaiveDate;
use collection_application::{
    plan_standard_code_bronze_page, StandardCodeBronzePagePlanInput, StandardCodePageRequest,
};
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn snapshot_payload() -> serde_json::Value {
    // getStanReginCdList body shape: StanReginCd[0]=head, StanReginCd[1]=row.
    json!({
        "StanReginCd": [
            {"head": [{"totalCount": "20560"},
                      {"RESULT": {"resultCode": "INFO-0", "resultMsg": "NORMAL SERVICE."}}]},
            {"row": [
                {"region_cd": "1200000000", "sido_cd": "12", "sgg_cd": "000", "umd_cd": "000",
                 "ri_cd": "00", "locatadd_nm": "전남광주통합특별시", "locathigh_cd": "1200000000",
                 "adpt_de": "20260701"},
                {"region_cd": "1224000000", "sido_cd": "12", "sgg_cd": "240", "umd_cd": "000",
                 "ri_cd": "00", "locatadd_nm": "전남광주통합특별시 서구", "locathigh_cd": "1200000000",
                 "adpt_de": "20260701"}
            ]}
        ]
    })
}

#[test]
fn standard_code_bronze_plan_builds_snapshot_partition_and_collapses_operation() -> TestResult {
    let payload = snapshot_payload();
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-00000000010d")?);

    let plan = plan_standard_code_bronze_page(StandardCodeBronzePagePlanInput {
        source_slug: "datagokr__legal_dong_code",
        ingest_date: NaiveDate::from_ymd_opt(2026, 9, 13).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: StandardCodePageRequest {
            operation: "getStanReginCdList".to_owned(),
            snapshot_id: "20260913".to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload,
        payload,
    })?;

    // The operation 1:1-maps to `legal_dong_code`, so the object key drops `operation=` (like the
    // sibling data.go.kr lanes) and keeps only the snapshot coverage-slice partition + page leaf.
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=datagokr__legal_dong_code/snapshot=20260913/page-000001.json"
    );
    assert!(
        !plan.object_key.as_str().contains("operation="),
        "object key must not carry an operation= segment: {}",
        plan.object_key.as_str()
    );
    // Lineage (source_partition_key) STILL carries the provider operation.
    assert_eq!(
        plan.source_identity_key,
        "snapshot=20260913/page=000001/page_size=1000"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=getStanReginCdList/snapshot=20260913/page=000001"
    );
    // No provider reference date exists, so the snapshot date is the collected-at fallback.
    assert_eq!(plan.snapshot_period, None);
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2026, 9, 13).ok_or("valid snapshot date")?
    );
    assert_eq!(plan.snapshot_granularity.as_str(), "day");
    assert_eq!(plan.snapshot_basis.as_str(), "collected_at_fallback");
    // The logical records are the two rows at /StanReginCd/1/row.
    assert_eq!(plan.logical_record_count, 2);
    assert_eq!(plan.request_params["operation"], "getStanReginCdList");
    assert_eq!(plan.request_params["type"], "json");
    assert_eq!(plan.request_params["pageNo"], 1);
    assert_eq!(plan.request_params["numOfRows"], 1000);
    Ok(())
}

#[test]
fn standard_code_bronze_plan_pins_the_canonical_page_size() -> TestResult {
    let payload = snapshot_payload();
    let error = plan_standard_code_bronze_page(StandardCodeBronzePagePlanInput {
        source_slug: "datagokr__legal_dong_code",
        ingest_date: NaiveDate::from_ymd_opt(2026, 9, 13).ok_or("valid date")?,
        ingestion_run_id: IngestionRunId::new(Uuid::nil()),
        request: StandardCodePageRequest {
            operation: "getStanReginCdList".to_owned(),
            snapshot_id: "20260913".to_owned(),
            page_no: 1,
            num_of_rows: 500,
        },
        raw_payload: serde_json::to_vec(&payload)?,
        payload,
    })
    .err()
    .ok_or("a non-canonical page size must be rejected")?;
    assert!(
        error.to_string().contains("canonical page size 1000"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn standard_code_bronze_plan_rejects_a_malformed_snapshot_id() -> TestResult {
    let error = plan_standard_code_bronze_page(StandardCodeBronzePagePlanInput {
        source_slug: "datagokr__legal_dong_code",
        ingest_date: NaiveDate::from_ymd_opt(2026, 9, 13).ok_or("valid date")?,
        ingestion_run_id: IngestionRunId::new(Uuid::nil()),
        request: StandardCodePageRequest {
            operation: "getStanReginCdList".to_owned(),
            snapshot_id: "2026".to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload: b"{}".to_vec(),
        payload: json!({}),
    })
    .err()
    .ok_or("a malformed snapshot id must be rejected")?;
    assert!(
        error
            .to_string()
            .contains("snapshot_id must be exactly 8 digits"),
        "unexpected error: {error}"
    );
    Ok(())
}
