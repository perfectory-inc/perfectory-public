//! Contract tests for `rt.molit.go.kr` real-transaction CSV export Bronze planning.

use chrono::NaiveDate;
use collection_application::{
    plan_rt_molit_real_transaction_export, RtMolitExportScope,
    RtMolitRealTransactionExportPlanInput, RtMolitRealTransactionExportRequest,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn rt_molit_export_plan_builds_monthly_nationwide_csv_identity() -> TestResult {
    let raw_payload = b"notice\nheader\nrow-1\n".to_vec();
    let request = RtMolitRealTransactionExportRequest {
        thing_code: "A".to_owned(),
        deal_type_code: "1".to_owned(),
        contract_from: NaiveDate::from_ymd_opt(2026, 6, 1).ok_or("valid date")?,
        contract_to: NaiveDate::from_ymd_opt(2026, 6, 30).ok_or("valid date")?,
        scope: RtMolitExportScope::Nationwide,
        response_format: "csv".to_owned(),
    };

    let plan = plan_rt_molit_real_transaction_export(RtMolitRealTransactionExportPlanInput {
        source_slug: "rtmolitkr__real_transaction_apartment_trade",
        request,
        raw_payload,
    })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=rtmolitkr__real_transaction_apartment_trade/period=2026-06/scope=nationwide/export.csv"
    );
    assert_eq!(
        plan.source_identity_key,
        "contract_from=2026-06-01/contract_to=2026-06-30/scope=nationwide/format=csv"
    );
    assert_eq!(
        plan.source_partition_key,
        "thing=A/deal_type=1/contract_from=2026-06-01/contract_to=2026-06-30/scope=nationwide"
    );
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-06"));
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2026, 6, 1).ok_or("valid date")?
    );
    assert_eq!(plan.snapshot_granularity.as_str(), "month");
    assert_eq!(plan.snapshot_basis.as_str(), "request_month");
    assert_eq!(plan.request_params["srhThingNo"], "A");
    assert_eq!(plan.request_params["srhDelngSecd"], "1");
    assert_eq!(plan.request_params["srhFromDt"], "2026-06-01");
    assert_eq!(plan.request_params["srhToDt"], "2026-06-30");
    assert_eq!(plan.request_params["format"], "csv");
    assert_eq!(plan.checksum_sha256.len(), 64);
    assert_eq!(plan.size_bytes, 20);
    Ok(())
}

#[test]
fn rt_molit_export_plan_builds_monthly_sigungu_csv_identity() -> TestResult {
    let raw_payload = b"notice\nheader\nrow-1\n".to_vec();
    let request = RtMolitRealTransactionExportRequest {
        thing_code: "A".to_owned(),
        deal_type_code: "1".to_owned(),
        contract_from: NaiveDate::from_ymd_opt(2026, 6, 1).ok_or("valid date")?,
        contract_to: NaiveDate::from_ymd_opt(2026, 6, 30).ok_or("valid date")?,
        scope: RtMolitExportScope::Sigungu {
            sido_code: "11000".to_owned(),
            sigungu_code: "11680".to_owned(),
        },
        response_format: "csv".to_owned(),
    };

    let plan = plan_rt_molit_real_transaction_export(RtMolitRealTransactionExportPlanInput {
        source_slug: "rtmolitkr__real_transaction_apartment_trade",
        request,
        raw_payload,
    })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=rtmolitkr__real_transaction_apartment_trade/period=2026-06/sido=11000/sigungu=11680/export.csv"
    );
    assert_eq!(
        plan.source_identity_key,
        "contract_from=2026-06-01/contract_to=2026-06-30/sido=11000/sigungu=11680/format=csv"
    );
    assert_eq!(
        plan.source_partition_key,
        "thing=A/deal_type=1/contract_from=2026-06-01/contract_to=2026-06-30/sido=11000/sigungu=11680"
    );
    assert_eq!(plan.request_params["scope"], "sigungu");
    assert_eq!(plan.request_params["srhSidoCd"], "11000");
    assert_eq!(plan.request_params["srhSggCd"], "11680");
    Ok(())
}

#[test]
fn rt_molit_export_plan_rejects_reversed_contract_range() -> TestResult {
    let request = RtMolitRealTransactionExportRequest {
        thing_code: "A".to_owned(),
        deal_type_code: "1".to_owned(),
        contract_from: NaiveDate::from_ymd_opt(2026, 6, 30).ok_or("valid date")?,
        contract_to: NaiveDate::from_ymd_opt(2026, 6, 1).ok_or("valid date")?,
        scope: RtMolitExportScope::Nationwide,
        response_format: "csv".to_owned(),
    };

    let error = plan_rt_molit_real_transaction_export(RtMolitRealTransactionExportPlanInput {
        source_slug: "rtmolitkr__real_transaction_apartment_trade",
        request,
        raw_payload: b"csv".to_vec(),
    })
    .err()
    .ok_or("reversed range must be rejected")?;

    assert!(
        error.to_string().contains("contract_from must be"),
        "unexpected error: {error}"
    );
    Ok(())
}
