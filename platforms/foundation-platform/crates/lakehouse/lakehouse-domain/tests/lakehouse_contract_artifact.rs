//! Contract tests for the Spark-facing lakehouse contract artifact.

use std::{collections::BTreeMap, error::Error, path::Path};

use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
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
