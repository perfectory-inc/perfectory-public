use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Type};

use super::*;

/// A real contracted table, so the declaration's table names resolve against the one registry.
const TABLE: &str = "gold.building_panel";
const LANE: &str = "oidc~lane";
const OPERATOR: &str = "oidc~operator";

fn declaration(tables: &[&str], lanes: Value) -> anyhow::Result<Declaration> {
    Ok(serde_json::from_value(json!({
        "warehouse": {
            "bucket": "synthetic-bucket",
            "region": "apac",
            "key_prefix": "tables",
            "layout": {"type": "full-hierarchy", "namespace": "{name}", "tabular": "{name}"},
            "delete_profile": {"type": "soft", "expiration-seconds": 604_800},
            "namespaces": ["silver", "gold"],
            "reason": "kept beside the fields, as the contract has it"
        },
        "tables": tables,
        "lanes": lanes,
        "operator": OPERATOR,
        "drift": "fail"
    }))?)
}

#[test]
fn a_declared_table_resolves_to_its_contract() -> anyhow::Result<()> {
    let declared = declaration(&[TABLE], json!({LANE: {"write": [TABLE]}}))?;
    let contracts = validate(&declared)?;
    assert_eq!(contracts.len(), 1);
    assert_eq!(contracts[0].table_name, TABLE);
    Ok(())
}

#[test]
fn a_declaration_outside_the_contracts_or_its_namespaces_is_refused() -> anyhow::Result<()> {
    assert!(validate(&declaration(&["gold.no_such_table_99999"], json!({}))?).is_err());
    assert!(validate(&declaration(&["bronze.building_panel"], json!({}))?).is_err());
    assert!(validate(&declaration(
        &[TABLE],
        json!({LANE: {"write": ["gold.other"]}})
    )?)
    .is_err());
    assert!(validate(&declaration(&[TABLE], json!({LANE: {"modify": [TABLE]}}))?).is_err());
    Ok(())
}

#[test]
fn missing_grants_are_made_and_undeclared_ones_are_drift_but_the_operators_are_neither(
) -> anyhow::Result<()> {
    let declared = declaration(&[TABLE], json!({LANE: {"select": [TABLE]}}))?;
    let live: BTreeSet<Grant> = [
        ("ownership".to_owned(), OPERATOR.to_owned()),
        ("write".to_owned(), "oidc~other".to_owned()),
    ]
    .into();
    let (missing, extra) = grant_changes(&live, &declared_grants(&declared, TABLE), OPERATOR);
    assert_eq!(missing, vec![("select".to_owned(), LANE.to_owned())]);
    assert_eq!(extra, vec![("write".to_owned(), "oidc~other".to_owned())]);
    assert!(declared_grants(&declared, "gold.other").is_empty());
    Ok(())
}

#[test]
fn a_table_shaped_as_its_contract_has_no_schema_drift_and_another_shape_has() -> anyhow::Result<()>
{
    let contract = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == TABLE)
        .context("the contract")?;
    let (schema, _, _) = contract_layout(contract)?;
    assert_eq!(schema_drift(&schema, &schema), None);
    let other = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()?;
    assert!(schema_drift(&other, &schema).is_some());
    Ok(())
}

#[test]
fn a_warehouse_as_declared_has_no_drift_and_another_key_prefix_has() -> anyhow::Result<()> {
    let declared = declaration(&[], json!({}))?;
    let mut live = json!({
        "storage-profile": {
            "type": "s3", "bucket": "synthetic-bucket", "region": "apac", "key-prefix": "tables",
            "storage-layout": {"type": "full-hierarchy", "namespace": "{name}", "tabular": "{name}"}
        },
        "delete-profile": {"type": "soft", "expiration-seconds": 604_800}
    });
    assert!(warehouse_drift(&live, &declared.warehouse).is_empty());
    live["storage-profile"]["key-prefix"] = json!("elsewhere");
    let drift = warehouse_drift(&live, &declared.warehouse);
    assert_eq!(drift.len(), 1, "{drift:?}");
    assert!(drift[0].starts_with("storage-profile.key-prefix"));
    Ok(())
}

#[test]
fn the_warehouse_request_needs_all_four_parts_of_the_credential() -> anyhow::Result<()> {
    let declared = declaration(&[], json!({}))?;
    assert!(warehouse_request(&declared.warehouse, "account:key:secret").is_err());
    assert!(warehouse_request(&declared.warehouse, "account::secret:token").is_err());
    let request = warehouse_request(&declared.warehouse, "acct99999:key:secret:token")?;
    assert_eq!(
        request["storage-profile"]["endpoint"],
        "https://acct99999.r2.cloudflarestorage.com"
    );
    assert_eq!(request["storage-profile"]["key-prefix"], "tables");
    Ok(())
}
