//! The current rows of a versioned table are defined once (root ADR-0267, ADR-0564 §4): the SQL
//! the contract artifact carries and the rule applied to scanned rows give the same answer.
//!
//! The incident: on 2026-10-16 `silver.industrial_complexes` was read with no current-row rule and
//! every complex came back three times (4,326 rows), which stopped the boundary release. A reader
//! that picks the newest row by hand is the same defect waiting for its table.

use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, CurrentRowsError, LakehouseTableContract,
    CURRENT_ROWS_COLUMNS, CURRENT_ROWS_FILTER, CURRENT_ROWS_SOURCE, GOLD_COMPLEX_DOCUMENT_FACTS,
    GOLD_LAND_USE_ZONE_AREA_BY_TILE, GOLD_PARCEL_PANEL, SILVER_INDUSTRIAL_COMPLEXES,
};
use serde_json::{json, Map, Value};

fn rows(values: &[Value]) -> Vec<Map<String, Value>> {
    values
        .iter()
        .map(|value| value.as_object().cloned().unwrap_or_default())
        .collect()
}

fn picked(contract: &LakehouseTableContract, rows: &[Map<String, Value>]) -> Vec<String> {
    let mut picked: Vec<String> = contract
        .current_rows(rows)
        .unwrap_or_default()
        .into_iter()
        .map(|row| Value::Object(row.clone()).to_string())
        .collect();
    picked.sort();
    picked
}

/// The cases the SQL rendering is held to as well (`test_current_rows_sql.py`).
#[test]
fn the_shared_cases_pick_the_same_rows_as_the_sql_does() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/current_rows_cases.json");
    let fixture: Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let cases = fixture["cases"]
        .as_array()
        .ok_or("cases must be an array")?;
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap_or_default();
        let table = case["table"].as_str().ok_or("a case names its table")?;
        let contract = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(table)
            .ok_or("a case names a contract table")?;
        let versions = rows(case["rows"].as_array().ok_or("rows must be an array")?);
        let mut expected: Vec<String> = case["current"]
            .as_array()
            .ok_or("current must be an array")?
            .iter()
            .map(Value::to_string)
            .collect();
        expected.sort();
        assert_eq!(picked(contract, &versions), expected, "{name}");
    }
    assert_eq!(
        GOLD_LAND_USE_ZONE_AREA_BY_TILE
            .current_rows(&[])
            .map(|rows| rows.len()),
        Ok(0)
    );
    Ok(())
}

#[test]
fn two_rows_of_a_key_tied_on_the_order_are_refused() {
    let fact = |code: &str, run: &str| json!({"official_complex_code": code, "run_id": run});
    let tied = rows(&[fact("99999", "run-2"), fact("99999", "run-2")]);
    assert_eq!(
        GOLD_COMPLEX_DOCUMENT_FACTS.current_rows(&tied),
        Err(CurrentRowsError::Tied {
            table: "gold.complex_document_facts",
            key: vec!["99999".to_owned()],
        })
    );
    let ordered = rows(&[
        fact("99999", "run-1"),
        fact("99999", "run-2"),
        fact("99998", "run-1"),
    ]);
    assert_eq!(
        picked(&GOLD_COMPLEX_DOCUMENT_FACTS, &ordered),
        vec![
            fact("99998", "run-1").to_string(),
            fact("99999", "run-2").to_string()
        ]
    );
}

#[test]
fn a_number_order_compares_as_numbers_and_mixed_values_have_no_order() {
    // A contract of this test's own: the order column is a commit sequence (root ADR-0564 §2),
    // where text order would put 10 before 9.
    let mut contract = GOLD_COMPLEX_DOCUMENT_FACTS;
    contract.row_versions = lakehouse_domain::LakehouseRowVersions::LatestPerKey(
        lakehouse_domain::LakehouseCurrentRowLatest {
            key: &["official_complex_code"],
            newest_first: &["version_seq"],
            removal_column: Some("row_state"),
            keep_ties: false,
        },
    );
    let version = |seq: Value| json!({"official_complex_code": "99999", "version_seq": seq});
    let numbered = rows(&[version(json!(9)), version(json!(10))]);
    assert_eq!(
        picked(&contract, &numbered),
        vec![version(json!(10)).to_string()]
    );
    let mixed = rows(&[version(json!(9)), version(json!("10"))]);
    assert_eq!(
        contract.current_rows(&mixed),
        Err(CurrentRowsError::Unordered {
            table: "gold.complex_document_facts",
            column: "version_seq",
        })
    );
}

#[test]
fn a_table_without_versions_or_with_a_predicate_is_refused_not_guessed() {
    assert_eq!(
        GOLD_PARCEL_PANEL.current_rows(&[]),
        Err(CurrentRowsError::NotVersioned("gold.parcel_panel"))
    );
    assert_eq!(GOLD_PARCEL_PANEL.current_rows_sql_template(), None);
    // `valid_to_utc IS NULL` is SQL: decoded rows cannot apply it, the SQL rendering does.
    assert_eq!(
        SILVER_INDUSTRIAL_COMPLEXES.current_rows(&[]),
        Err(CurrentRowsError::Predicate("silver.industrial_complexes"))
    );
    let sql = SILVER_INDUSTRIAL_COMPLEXES
        .current_rows_sql_template()
        .unwrap_or_default();
    assert!(sql.contains("AND (valid_to_utc IS NULL)"), "{sql}");
}

#[test]
fn every_versioned_table_renders_one_query_with_the_three_slots() {
    let mut rendered = 0;
    for contract in industrial_complex_lakehouse_contracts() {
        let Some(latest) = contract.current_row_latest() else {
            assert_eq!(contract.current_rows_sql_template(), None);
            continue;
        };
        rendered += 1;
        let sql = contract.current_rows_sql_template().unwrap_or_default();
        for slot in [
            CURRENT_ROWS_COLUMNS,
            CURRENT_ROWS_SOURCE,
            CURRENT_ROWS_FILTER,
        ] {
            assert_eq!(
                sql.matches(slot).count(),
                1,
                "{}: {sql}",
                contract.table_name
            );
        }
        assert!(
            sql.contains(&format!("PARTITION BY {}", latest.key.join(", "))),
            "{}: {sql}",
            contract.table_name
        );
        // The removal marker is dropped after the newest row is chosen, never before: a filter
        // inside would make the row a marker removed look current again.
        if let Some(removal) = latest.removal_column {
            let (inner, outer) = sql
                .split_once("WHERE foundation_current_rank = 1")
                .unwrap_or_default();
            assert!(
                !inner.contains("'removed'"),
                "{}: {sql}",
                contract.table_name
            );
            assert!(
                outer.contains(&format!("{removal} <> 'removed'")),
                "{}: {sql}",
                contract.table_name
            );
        }
        // Without a predicate the text is the one the Trino readers already ran (root ADR-0550).
        if contract.current_row_predicate.is_none() {
            assert_eq!(
                latest.current_rows_sql("t", &["a"], "TRUE"),
                sql.replace(CURRENT_ROWS_COLUMNS, "a")
                    .replace(CURRENT_ROWS_SOURCE, "t")
                    .replace(CURRENT_ROWS_FILTER, "TRUE")
            );
        }
    }
    assert!(rendered > 0, "no contract keeps versions per key");
}
