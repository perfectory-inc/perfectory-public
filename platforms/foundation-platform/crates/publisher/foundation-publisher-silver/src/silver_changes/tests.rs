//! The editions compare over a table that already holds editions (root ADR-0540 §3).
//! Reserved 99999 codes only.

use anyhow::Context as _;
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};

use super::changed_editions_of;
use crate::duckdb_parquet_stage::DuckDbParquetStage;

const TABLE: &str = "silver.court_auction_case_related";

fn contract() -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(TABLE).context("the related cases table")
}

/// `name` holding `rows` (the case, the row's place in its list, the related case, the read), every
/// other column of the table one fixed value of its type.
fn table(stage: &DuckDbParquetStage, name: &str, rows: &str) -> anyhow::Result<()> {
    let columns = contract()?
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("case_no" | "related_seq" | "related_case_no" | "source_snapshot_id", _) => {
                format!("v.{}", column.name)
            }
            (other, "int") => format!("1 AS {other}"),
            (other, "timestamp") => format!("TIMESTAMPTZ '2099-01-01 00:00:00+00' AS {other}"),
            (other, _) => format!("'x' AS {other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    stage.connection().execute_batch(&format!(
        "CREATE OR REPLACE TABLE {name} AS SELECT {columns} FROM (VALUES {rows}) \
         v(case_no, related_seq, related_case_no, source_snapshot_id)"
    ))?;
    Ok(())
}

// 2026-11-23 21:35Z: the first production load onto a table that already held editions failed with
// `syntax error at or near "r"`, because the table pinned at its head was followed by an alias.
// Every earlier run and test read a table with no snapshot, which is a parenthesised query.
#[test]
fn a_table_named_with_a_clause_after_it_gives_only_the_groups_whose_list_moved(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    // The table: case A read twice (the newest list is one row), case B read once.
    table(
        &stage,
        "table_rows",
        "('2099타경99999', 1, '2099타경99991', 'e1'), ('2099타경99999', 2, '2099타경99992', 'e1'), \
         ('2099타경99999', 1, '2099타경99991', 'e2'), ('2099타경99998', 1, '2099타경99993', 'e1')",
    )?;
    // The read: A's list is its newest edition's, B's list gained a row, C is new.
    table(
        &stage,
        "staged_rows",
        "('2099타경99999', 1, '2099타경99991', 'e3'), ('2099타경99998', 1, '2099타경99993', 'e3'), \
         ('2099타경99998', 2, '2099타경99994', 'e3'), ('2099타경99997', 1, '2099타경99995', 'e3')",
    )?;
    let changed = changed_editions_of(
        &stage,
        contract()?,
        // As the load names the table: a clause after the name (`AT (VERSION => n)` there), which
        // takes no alias.
        Some("table_rows TABLESAMPLE (100 PERCENT)"),
        "staged_rows",
        "changed_rows",
    )?;
    assert_eq!(changed, 3);
    let cases: Vec<String> = stage
        .connection()
        .prepare("SELECT DISTINCT case_no FROM changed_rows ORDER BY case_no")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(cases, ["2099타경99997", "2099타경99998"]);
    Ok(())
}
