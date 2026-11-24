//! A table read at a snapshot is a relation like any other (`table_at`).

use crate::duckdb_parquet_stage::{table_at, table_at_in, DuckDbParquetStage, LAKEHOUSE_CATALOG};

// 2026-11-21 the first release compares failed on `... AT (VERSION => n) g` and 2026-11-23 21:35Z
// the court auction load on `... AT (VERSION => n) r`: `DuckDB` takes no alias
// after the clause, and each query had only ever read a table with no snapshot. Here `DuckDB`
// itself reads the forms the loads use. The catalog is a local one of that name, which has no
// snapshots to travel to: `DuckDB` may refuse the read, but only after it parsed the query.
#[test]
fn a_table_read_at_a_snapshot_takes_an_alias_a_join_and_a_filter() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&format!(
        "ATTACH ':memory:' AS {LAKEHOUSE_CATALOG}; \
         CREATE SCHEMA {LAKEHOUSE_CATALOG}.silver; \
         CREATE TABLE {LAKEHOUSE_CATALOG}.silver.editions AS SELECT 1 AS id, 'e1' AS edition"
    ))?;
    let pinned = table_at("silver.editions", 42);
    assert_eq!(
        pinned,
        "(SELECT * FROM lakehouse.silver.editions AT (VERSION => 42))"
    );
    assert_eq!(
        table_at_in("other", "silver.editions", 7),
        "(SELECT * FROM other.silver.editions AT (VERSION => 7))"
    );
    for sql in [
        format!("SELECT r.id FROM {pinned} r"),
        format!("SELECT r.id FROM {pinned} AS r JOIN {pinned} s ON r.id = s.id WHERE r.id = 1"),
        format!("SELECT count(*) FROM {pinned} r WHERE EXISTS (SELECT 1 FROM {pinned} s WHERE s.id = r.id)"),
        format!("SELECT edition, max(id) FROM {pinned} r GROUP BY ALL"),
        format!("DESCRIBE SELECT * FROM {pinned}"),
        format!("CREATE OR REPLACE TEMP TABLE pinned_read AS SELECT * FROM {pinned} WHERE id = 1"),
    ] {
        if let Err(error) = stage.connection().prepare(&sql) {
            let said = error.to_string();
            assert!(
                !said.contains("Parser Error") && !said.contains("syntax error"),
                "{sql}: {said}"
            );
        }
    }
    Ok(())
}
