//! Contract tests for `VWorld` land-register Bronze page planning.

use chrono::NaiveDate;
use collection_application::{
    plan_vworld_land_register_bronze_page, VWorldLandRegisterBronzePagePlanInput,
    VWorldLandRegisterPageRequest,
};
use collection_domain::SchemaObservedType;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn vworld_land_register_bronze_plan_builds_canonical_pnu_partition() -> TestResult {
    let pnu = "9999900601100010000";
    let payload = json!({
        "ladfrlVOList": {
            "pageNo": "1",
            "ladfrlVOList": [
                {
                    "pnu": pnu,
                    "ldCodeNm": "SYNTHETIC-DISTRICT-BETA",
                    "lndpclAr": "52887.4"
                }
            ],
            "totalCount": "1",
            "error": "",
            "message": "",
            "numOfRows": "10"
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000015")?);

    let plan = plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
        source_slug: "vworldkr__land_register",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: VWorldLandRegisterPageRequest {
            operation: "ladfrlList".to_owned(),
            pnu: pnu.to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload,
        payload,
    })?;

    // ADR 0016 T1.2 / D-C / D-D: the land-register object key drops the redundant `operation=ladfrlList`
    // segment (`ladfrlList` 1:1-maps to the slug's `land_register` dataset via the collection-domain
    // V-World map); lineage keeps the provider operation.
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__land_register/pnu=9999900601100010000/page-000001.json"
    );
    assert!(
        !plan.object_key.as_str().contains("operation="),
        "object key must not carry an operation= segment: {}",
        plan.object_key.as_str()
    );
    // Lineage (source_partition_key) STILL carries the provider operation.
    assert_eq!(
        plan.source_partition_key,
        "operation=ladfrlList/pnu=9999900601100010000/page=000001"
    );
    assert_eq!(plan.logical_record_count, 1);
    assert_eq!(plan.request_params["operation"], "ladfrlList");
    assert_eq!(plan.request_params["pnu"], pnu);
    assert_eq!(plan.request_params["pageNo"], 1);
    assert_eq!(plan.request_params["numOfRows"], 1000);

    let pnu_profile = plan
        .schema_observations
        .iter()
        .find(|field| field.field_path == "ladfrlVOList.ladfrlVOList[].pnu")
        .ok_or("expected pnu schema observation")?;
    assert_eq!(pnu_profile.observed_type, SchemaObservedType::String);
    assert!(pnu_profile.candidate_key_score > 0.99);
    Ok(())
}

#[test]
fn vworld_land_register_bronze_plan_rejects_invalid_pnu() -> TestResult {
    let payload = json!({"ladfrlVOList": {"ladfrlVOList": []}});
    let error = plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
        source_slug: "vworldkr__land_register",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: IngestionRunId::new(Uuid::nil()),
        request: VWorldLandRegisterPageRequest {
            operation: "ladfrlList".to_owned(),
            pnu: "11680".to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload: b"{}".to_vec(),
        payload,
    })
    .err()
    .ok_or("invalid PNU must be rejected")?;

    assert!(
        error
            .to_string()
            .contains("pnu must be either a 10-digit legal-dong prefix or exactly 19 digits"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn vworld_land_register_bronze_plan_accepts_legal_dong_pnu_prefix() -> TestResult {
    let pnu_prefix = "9999900601";
    let payload = json!({
        "ladfrlVOList": {
            "pageNo": "1",
            "ladfrlVOList": [
                {
                    "pnu": "9999900601100010000",
                    "ldCodeNm": "SYNTHETIC-DISTRICT-ALPHA",
                    "lndpclAr": "52887.4"
                }
            ],
            "totalCount": "1",
            "error": "",
            "message": "",
            "numOfRows": "1000"
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000016")?);

    let plan = plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
        source_slug: "vworldkr__land_register",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 27).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: VWorldLandRegisterPageRequest {
            operation: "ladfrlList".to_owned(),
            pnu: pnu_prefix.to_owned(),
            page_no: 1,
            num_of_rows: 1000,
        },
        raw_payload,
        payload,
    })?;

    // Object key drops the redundant `operation=ladfrlList` segment; lineage keeps it (ADR 0016 T1.2).
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__land_register/pnu=9999900601/page-000001.json"
    );
    assert!(!plan.object_key.as_str().contains("operation="));
    assert_eq!(
        plan.source_partition_key,
        "operation=ladfrlList/pnu=9999900601/page=000001"
    );
    assert_eq!(plan.request_params["operation"], "ladfrlList");
    assert_eq!(plan.request_params["pnu"], pnu_prefix);
    assert_eq!(plan.logical_record_count, 1);
    Ok(())
}
