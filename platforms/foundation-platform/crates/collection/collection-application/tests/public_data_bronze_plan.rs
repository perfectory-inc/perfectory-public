//! Contract tests for generic public-data Bronze page planning.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use collection_application::{
    plan_public_data_bronze_page, PublicDataBronzePagePlanInput, PublicDataBronzePageRequest,
    PublicDataFixedQueryParam, PublicDataPartitionField,
};
use collection_domain::SchemaObservedType;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn public_data_bronze_plan_builds_provider_neutral_metadata_for_any_json_api() -> TestResult {
    let payload = json!({
        "response": {
            "body": {
                "items": {
                    "item": [
                        {
                            "tradeId": "11680-202605-1",
                            "area": "400.5"
                        }
                    ]
                }
            }
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000014")?);
    let plan = plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: "datagokr__real_transaction_apartment_trade",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: PublicDataBronzePageRequest {
            operation: "getTradeInfo".to_owned(),
            partition_fields: vec![
                PublicDataPartitionField {
                    name: "lawd".to_owned(),
                    value: "11680".to_owned(),
                },
                PublicDataPartitionField {
                    name: "month".to_owned(),
                    value: "202605".to_owned(),
                },
            ],
            query_params: BTreeMap::from([
                ("LAWD_CD".to_owned(), "11680".to_owned()),
                ("DEAL_YMD".to_owned(), "202605".to_owned()),
            ]),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "_type".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: 3,
            num_of_rows: 100,
        },
        raw_payload,
        payload,
        logical_items_pointer: "/response/body/items/item",
        candidate_key_field_suffixes: vec!["tradeId".to_owned()],
    })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=datagokr__real_transaction_apartment_trade/operation=getTradeInfo/period=2026-05/lawd=11680/page-000003.json"
    );
    assert_eq!(
        plan.source_identity_key,
        "lawd=11680/month=202605/page=000003/page_size=100"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=getTradeInfo/lawd=11680/month=202605/page=000003"
    );
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-05"));
    assert_eq!(
        plan.snapshot_date,
        NaiveDate::from_ymd_opt(2026, 5, 1).ok_or("valid snapshot date")?
    );
    assert_eq!(plan.snapshot_granularity.as_str(), "month");
    assert_eq!(plan.snapshot_basis.as_str(), "request_month");
    assert_eq!(plan.logical_record_count, 1);
    assert_eq!(plan.request_params["operation"], "getTradeInfo");
    assert_eq!(plan.request_params["LAWD_CD"], "11680");
    assert_eq!(plan.request_params["DEAL_YMD"], "202605");
    assert_eq!(plan.request_params["pageNo"], 3);
    assert_eq!(plan.request_params["numOfRows"], 100);
    assert_eq!(plan.checksum_sha256.len(), 64);

    let trade_id = plan
        .schema_observations
        .iter()
        .find(|field| field.field_path == "response.body.items.item[].tradeId")
        .ok_or("expected tradeId schema observation")?;
    assert_eq!(trade_id.observed_type, SchemaObservedType::String);
    assert!(trade_id.candidate_key_score > 0.99);
    Ok(())
}

/// Plans a minimal but valid `getBr*` building-register page at the given `num_of_rows`.
///
/// `getBrTitleInfo` pins to canonical page size 100 (ADR 0016 D-A), so this helper drives the
/// canonical-page-size guard in [`plan_public_data_bronze_page`]. The inner result is returned so a
/// caller can assert either success or the guard error.
fn plan_building_register_page(
    operation: &str,
    num_of_rows: u32,
) -> TestResult<Result<(), collection_application::PublicDataBronzePlanError>> {
    let payload = json!({
        "response": { "body": { "items": { "item": [ { "mgmBldrgstPk": "11680-1" } ] } } }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000015")?);
    let result = plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: "datagokr__building_register_main",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: PublicDataBronzePageRequest {
            operation: operation.to_owned(),
            partition_fields: vec![PublicDataPartitionField {
                name: "sigungu".to_owned(),
                value: "11680".to_owned(),
            }],
            query_params: BTreeMap::new(),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "_type".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: 1,
            num_of_rows,
        },
        raw_payload,
        payload,
        logical_items_pointer: "/response/body/items/item",
        candidate_key_field_suffixes: vec!["mgmBldrgstPk".to_owned()],
    })
    .map(|_plan| ());
    Ok(result)
}

/// A pinned operation whose request page size differs from its canonical SSOT must FAIL at plan
/// compile (never silently write a colliding `page-NNNNNN` object) — ADR 0016 acceptance #7 / D-A.
#[test]
fn canonical_page_size_violation_fails_at_plan_time() -> TestResult {
    let error = plan_building_register_page("getBrTitleInfo", 999)?
        .err()
        .ok_or("non-canonical page size for a pinned operation must fail at plan time")?;
    let message = error.to_string();
    assert!(
        matches!(
            error,
            collection_application::PublicDataBronzePlanError::InvalidRequest(_)
        ),
        "expected InvalidRequest, got: {message}"
    );
    assert!(
        message.contains("100"),
        "error should name the canonical size 100, got: {message}"
    );
    Ok(())
}

/// The same pinned operation at its canonical page size compiles successfully.
#[test]
fn canonical_page_size_match_succeeds_at_plan_time() -> TestResult {
    plan_building_register_page("getBrTitleInfo", 100)?
        .map_err(|error| format!("canonical page size must compile successfully: {error}"))?;
    Ok(())
}

/// A synthetic operation has no pinned canonical, so the guard does not apply at any page size.
#[test]
fn synthetic_operation_is_unenforced_by_canonical_page_size_guard() -> TestResult {
    plan_building_register_page("getTradeInfo", 50)?.map_err(|error| {
        format!(
            "synthetic operation must be unenforced (canonical_page_size returns None): {error}"
        )
    })?;
    Ok(())
}
