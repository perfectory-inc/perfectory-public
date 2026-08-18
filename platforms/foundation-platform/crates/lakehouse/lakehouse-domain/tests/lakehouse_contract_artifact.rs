//! Contract tests for the Spark-facing lakehouse contract artifact.

use std::{collections::BTreeMap, error::Error, path::Path};

use lakehouse_domain::{
    bronze_industrial_complexes_raw_jsonl_columns, industrial_complex_lakehouse_contracts,
    LakehouseTableContract, BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL,
    INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES, INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES,
    INDUSTRIAL_COMPLEX_SILVER_JOB_DERIVED_COLUMNS, INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
};
use serde_json::Value;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn spark_contract_artifact_matches_rust_lakehouse_contracts() -> TestResult {
    let raw = std::fs::read_to_string(contract_artifact_path()?)?;
    let artifact: Value = serde_json::from_str(&raw)?;
    let contracts = artifact["contracts"]
        .as_object()
        .ok_or("contracts must be a JSON object")?;

    let exported = industrial_complex_lakehouse_contracts()
        .iter()
        .map(|contract| (contract.table_name, contract_json(contract)))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(contracts.len(), exported.len());
    for (table_name, expected) in exported {
        assert_eq!(
            contracts
                .get(table_name)
                .ok_or("missing exported contract")?,
            &expected,
            "contract artifact drifted for {table_name}"
        );
    }
    Ok(())
}

#[test]
fn spark_artifact_exports_the_bronze_jsonl_transport_contract() -> TestResult {
    let raw = std::fs::read_to_string(contract_artifact_path()?)?;
    let artifact: Value = serde_json::from_str(&raw)?;
    let transports = artifact["jsonl_transports"]
        .as_object()
        .ok_or("jsonl_transports must be a JSON object")?;

    assert_eq!(transports.len(), 1);
    assert_eq!(
        transports
            .get(BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL)
            .ok_or("missing exported jsonl transport")?,
        &serde_json::json!({
            "target_contract": "silver.industrial_complexes",
            "target_derived_columns": INDUSTRIAL_COMPLEX_SILVER_JOB_DERIVED_COLUMNS,
            "columns": bronze_industrial_complexes_raw_jsonl_columns(),
        }),
        "jsonl transport artifact drifted for {BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL}"
    );
    Ok(())
}

#[test]
fn spark_artifact_exports_the_industrial_complex_value_domains() -> TestResult {
    let raw = std::fs::read_to_string(contract_artifact_path()?)?;
    let artifact: Value = serde_json::from_str(&raw)?;
    let domains = artifact["value_domains"]
        .as_object()
        .ok_or("value_domains must be a JSON object")?;

    assert_eq!(domains.len(), 1);
    assert_eq!(
        domains
            .get("silver.industrial_complexes")
            .ok_or("missing exported value domains")?,
        &serde_json::json!({
            "complex_kind": INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES,
            "status": INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
            "lot_sales_status": INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES,
        }),
        "value domain artifact drifted for silver.industrial_complexes"
    );
    Ok(())
}

fn contract_artifact_path() -> Result<std::path::PathBuf, &'static str> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .ok_or("catalog-domain manifest must live under crates/catalog/catalog-domain")?;

    Ok(
        workspace_root
            .join("infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"),
    )
}

fn contract_json(contract: &LakehouseTableContract) -> Value {
    serde_json::json!({
        "table_name": contract.table_name,
        "physical_format": format!("{:?}", contract.physical_format),
        "serving_role": format!("{:?}", contract.serving_role),
        "current_row_predicate": contract.current_row_predicate,
        "columns": contract.columns.iter().map(|column| {
            serde_json::json!({
                "name": column.name,
                "logical_type": column.logical_type,
                "required": column.required
            })
        }).collect::<Vec<_>>(),
        "partition_spec": contract.partition_spec,
        "sort_order": contract.sort_order,
        "quality_gates": contract.quality_gates
    })
}
