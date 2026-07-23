//! Contract tests for data.go.kr real-transaction Bronze page planning.

use chrono::NaiveDate;
use collection_application::{
    plan_real_transaction_bronze_page, RealTransactionBronzePagePlanInput,
    RealTransactionPageRequest,
};
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn real_transaction_bronze_plan_builds_canonical_lawd_month_partition() -> TestResult {
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "00",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": {
                "items": {
                    "item": [
                        {
                            "umdNm": "Yeoksam-dong",
                            "jibun": "1-1",
                            "dealAmount": "120000",
                            "dealYear": "2026",
                            "dealMonth": "05",
                            "dealDay": "14"
                        }
                    ]
                },
                "numOfRows": 100,
                "pageNo": 1,
                "totalCount": 1
            }
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000017")?);

    let plan = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
        source_slug: "datagokr__real_transaction_industrial_trade",
        ingest_date: NaiveDate::from_ymd_opt(2026, 6, 2).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: RealTransactionPageRequest {
            operation: "getRTMSDataSvcInduTrade".to_owned(),
            lawd_cd: "11680".to_owned(),
            deal_ymd: "202605".to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload,
        payload,
    })?;

    // ADR 0016 T1.2 / D-D: the object key drops the redundant `operation=getRTMSDataSvcInduTrade`
    // segment (1:1 with the slug's `real_transaction_industrial_trade` dataset); lineage keeps it.
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=datagokr__real_transaction_industrial_trade/period=2026-05/lawd=11680/page-000001.json"
    );
    assert!(
        !plan.object_key.as_str().contains("operation="),
        "object key must not carry an operation= segment: {}",
        plan.object_key.as_str()
    );
    // Lineage (source_partition_key) STILL carries the provider operation.
    assert_eq!(
        plan.source_identity_key,
        "lawd=11680/deal_ymd=202605/page=000001/page_size=1000"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=getRTMSDataSvcInduTrade/lawd=11680/deal_ymd=202605/page=000001"
    );
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-05"));
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2026, 5, 1).ok_or("valid snapshot date")?
    );
    assert_eq!(plan.snapshot_granularity.as_str(), "month");
    assert_eq!(plan.snapshot_basis.as_str(), "request_month");
    assert_eq!(plan.logical_record_count, 1);
    assert_eq!(plan.request_params["operation"], "getRTMSDataSvcInduTrade");
    assert_eq!(plan.request_params["LAWD_CD"], "11680");
    assert_eq!(plan.request_params["DEAL_YMD"], "202605");
    assert_eq!(plan.request_params["_type"], "json");
    Ok(())
}

#[test]
fn real_transaction_bronze_plan_rejects_invalid_scope() -> TestResult {
    let payload = json!({"response": {"body": {"items": {"item": []}}}});
    let error = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
        source_slug: "datagokr__real_transaction_industrial_trade",
        ingest_date: NaiveDate::from_ymd_opt(2026, 6, 2).ok_or("valid date")?,
        ingestion_run_id: IngestionRunId::new(Uuid::nil()),
        request: RealTransactionPageRequest {
            operation: "getRTMSDataSvcInduTrade".to_owned(),
            lawd_cd: "1168".to_owned(),
            deal_ymd: "202605".to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload: b"{}".to_vec(),
        payload,
    })
    .err()
    .ok_or("invalid scope must be rejected")?;

    assert!(
        error
            .to_string()
            .contains("lawdCd must be exactly 5 digits"),
        "unexpected error: {error}"
    );
    Ok(())
}
