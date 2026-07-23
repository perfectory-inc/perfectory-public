//! Contract tests for building-register Bronze page planning.

use chrono::NaiveDate;
use collection_application::{
    plan_building_register_bronze_page, BuildingRegisterBronzePagePlanInput,
    BuildingRegisterPageRequest,
};
use collection_domain::SchemaObservedType;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn building_register_bronze_plan_builds_canonical_object_metadata() -> TestResult {
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
                            "mgmBldrgstPk": "11680-10300-1",
                            "platPlc": "SYNTHETIC-CITY SYNTHETIC-DISTRICT SYNTHETIC-LOT 12",
                            "totArea": "100.25"
                        },
                        {
                            "mgmBldrgstPk": "11680-10300-2",
                            "platPlc": "SYNTHETIC-CITY SYNTHETIC-DISTRICT SYNTHETIC-LOT 13",
                            "totArea": null
                        }
                    ]
                },
                "numOfRows": 100,
                "pageNo": 1,
                "totalCount": 2
            }
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000002")?);

    let plan = plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
        source_slug: "datagokr__building_register_main",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 14).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        },
        raw_payload,
        payload,
    })?;

    // ADR 0016 T1.2 / D-D: the object key drops the redundant `operation=getBrTitleInfo` segment
    // (1:1 with the slug's `building_register_main` dataset), but lineage keeps the operation.
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json"
    );
    assert!(
        !plan.object_key.as_str().contains("operation="),
        "object key must not carry an operation= segment: {}",
        plan.object_key.as_str()
    );
    // Lineage (source_partition_key) STILL carries the provider operation.
    assert_eq!(
        plan.source_partition_key,
        "operation=getBrTitleInfo/sigungu=11680/bjdong=10300/page=000001"
    );
    assert_eq!(plan.logical_record_count, 2);
    assert_eq!(plan.size_bytes, plan.raw_payload.len() as u64);
    assert_eq!(plan.checksum_sha256.len(), 64);
    assert!(plan.dedupe_key.ends_with(&plan.checksum_sha256));
    assert_eq!(plan.request_params["operation"], "getBrTitleInfo");
    assert_eq!(plan.request_params["sigunguCd"], "11680");
    assert_eq!(plan.request_params["bjdongCd"], "10300");

    let mgm_key = plan
        .schema_observations
        .iter()
        .find(|field| field.field_path == "response.body.items.item[].mgmBldrgstPk")
        .ok_or("expected mgmBldrgstPk schema observation")?;
    assert_eq!(mgm_key.observed_type, SchemaObservedType::String);
    assert_eq!(mgm_key.nonnull_count, 2);
    assert_eq!(mgm_key.null_count, 0);
    assert!(mgm_key.candidate_key_score > 0.9);

    let total_area = plan
        .schema_observations
        .iter()
        .find(|field| field.field_path == "response.body.items.item[].totArea")
        .ok_or("expected totArea schema observation")?;
    assert_eq!(total_area.observed_type, SchemaObservedType::String);
    assert_eq!(total_area.nonnull_count, 1);
    assert_eq!(total_area.null_count, 1);
    Ok(())
}

#[test]
fn building_register_bronze_plan_uses_page_number_as_part_sequence() -> TestResult {
    let payload = json!({
        "response": {
            "body": {
                "items": {
                    "item": [
                        {
                            "mgmBldrgstPk": "11680-10300-3"
                        }
                    ]
                }
            }
        }
    });
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000003")?);

    let plan = plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
        source_slug: "datagokr__building_register_main",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 14).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 2,
            num_of_rows: 100,
        },
        raw_payload,
        payload,
    })?;

    // Object key drops the redundant `operation=` segment (page number remains the leaf); lineage
    // keeps the operation (ADR 0016 T1.2 / D-D).
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000002.json"
    );
    assert!(!plan.object_key.as_str().contains("operation="));
    assert_eq!(
        plan.source_partition_key,
        "operation=getBrTitleInfo/sigungu=11680/bjdong=10300/page=000002"
    );
    assert_eq!(plan.request_params["operation"], "getBrTitleInfo");
    Ok(())
}
