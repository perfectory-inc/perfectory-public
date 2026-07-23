//! Contract tests for generic `VWorld` NED Bronze page planning.

use chrono::NaiveDate;
use collection_application::{
    plan_vworld_ned_bronze_page, VWorldNedBronzePagePlanInput, VWorldNedPageRequest,
};
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::json;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn test_ingest_date() -> Result<NaiveDate, std::io::Error> {
    NaiveDate::from_ymd_opt(2026, 6, 2)
        .ok_or_else(|| std::io::Error::other("invalid test ingest date"))
}

#[test]
fn plans_operation_specific_ned_bronze_key_without_date_partition() -> TestResult {
    let plan = plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
        source_slug: "vworldkr__land_characteristic",
        ingest_date: test_ingest_date()?,
        ingestion_run_id: IngestionRunId::new(Uuid::parse_str(
            "11111111-1111-4111-8111-111111111111",
        )?),
        request: VWorldNedPageRequest {
            operation: "getLandCharacteristic".to_owned(),
            partition_name: "pnu".to_owned(),
            partition_value: "9999900101100010000".to_owned(),
            query_params: [("pnu".to_owned(), "9999900101100010000".to_owned())].into(),
            page_no: 7,
            num_of_rows: 1000,
            logical_items_pointer: "/landCharVOList/landCharVOList".to_owned(),
            candidate_key_field_suffixes: vec!["pnu".to_owned()],
        },
        raw_payload: br#"{"landCharVOList":{"landCharVOList":[{"pnu":"9999900101100010000"}]}}"#
            .to_vec(),
        payload: json!({
            "landCharVOList": {
                "landCharVOList": [
                    {"pnu": "9999900101100010000"}
                ]
            }
        }),
    })?;

    // ADR 0016 T1.2 / D-C / D-D: the V-World NED object key drops the redundant
    // `operation=getLandCharacteristic` segment (1:1 with the slug's `land_characteristic` dataset
    // via the collection-domain V-World map); lineage keeps the provider operation.
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__land_characteristic/pnu=9999900101100010000/page-000007.json"
    );
    assert!(
        !plan.object_key.as_str().contains("operation="),
        "object key must not carry an operation= segment: {}",
        plan.object_key.as_str()
    );
    // Lineage (source_partition_key + request_params) STILL carries the provider operation.
    assert_eq!(
        plan.source_partition_key,
        "operation=getLandCharacteristic/pnu=9999900101100010000/page=000007"
    );
    assert_eq!(plan.request_params["operation"], "getLandCharacteristic");
    assert_eq!(plan.logical_record_count, 1);
    assert_eq!(
        plan.request_params
            .pointer("/format")
            .and_then(|value| value.as_str()),
        Some("json")
    );
    Ok(())
}

#[test]
fn rejects_non_canonical_source_slug() -> TestResult {
    let result = plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
        source_slug: "VWorld-dataset-land-characteristic",
        ingest_date: test_ingest_date()?,
        ingestion_run_id: IngestionRunId::new(Uuid::nil()),
        request: VWorldNedPageRequest {
            operation: "getLandCharacteristic".to_owned(),
            partition_name: "pnu".to_owned(),
            partition_value: "9999900101100010000".to_owned(),
            query_params: [("pnu".to_owned(), "9999900101100010000".to_owned())].into(),
            page_no: 1,
            num_of_rows: 1000,
            logical_items_pointer: "/landCharVOList/landCharVOList".to_owned(),
            candidate_key_field_suffixes: vec!["pnu".to_owned()],
        },
        raw_payload: b"{}".to_vec(),
        payload: json!({}),
    });
    let Err(error) = result else {
        return Err(std::io::Error::other("underscore source slugs must be rejected").into());
    };

    assert!(error.to_string().contains("source_slug"));
    Ok(())
}
