//! A stage reads the lakehouse and never writes it: the catalog is attached read-only, with reader
//! credentials only, and the commit stays the publisher's (root ADR-0560 §2).

use foundation_shared_kernel::env_source::Requirement;
use lakehouse_infrastructure::LakehouseCatalogConfig;

use super::super::{attach_lakehouse, LakehouseRead, LAKEHOUSE_ACCESS, LAKEHOUSE_CATALOG};

#[test]
fn the_lakehouse_is_attached_read_only() {
    assert_eq!(LAKEHOUSE_ACCESS, "READ_ONLY");
    let statement = attach_lakehouse("warehouse", "https://catalog.invalid");
    assert!(
        statement.ends_with(&format!(", {LAKEHOUSE_ACCESS})")),
        "the lakehouse must be attached read-only: {statement}"
    );
}

#[test]
fn duckdb_refuses_every_write_to_a_catalog_attached_that_way() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("catalog.duckdb");
    duckdb::Connection::open(&file)?
        .execute_batch("CREATE SCHEMA silver; CREATE TABLE silver.rows AS SELECT 1 AS a")?;
    let connection = duckdb::Connection::open_in_memory()?;
    connection.execute_batch(&format!(
        "ATTACH '{}' AS {LAKEHOUSE_CATALOG} ({LAKEHOUSE_ACCESS})",
        super::super::sql_path(&file)?
    ))?;
    let rows: i64 = connection.query_row(
        &format!("SELECT count(*) FROM {LAKEHOUSE_CATALOG}.silver.rows"),
        [],
        |row| row.get(0),
    )?;
    assert_eq!(rows, 1, "a read-only catalog is still read");
    for write in [
        "INSERT INTO @.silver.rows VALUES (2)",
        "CREATE TABLE @.silver.more (a INTEGER)",
        "DELETE FROM @.silver.rows",
        "UPDATE @.silver.rows SET a = 3",
        "MERGE INTO @.silver.rows AS t USING (SELECT 4 AS a) AS s ON t.a = s.a \
         WHEN NOT MATCHED THEN INSERT VALUES (s.a)",
        "DROP TABLE @.silver.rows",
    ] {
        let statement = write.replace('@', LAKEHOUSE_CATALOG);
        let Err(error) = connection.execute_batch(&statement) else {
            anyhow::bail!("DuckDB wrote a catalog attached {LAKEHOUSE_ACCESS}: {statement}");
        };
        assert!(
            error.to_string().contains("read-only"),
            "{statement}: {error}"
        );
    }
    Ok(())
}

#[test]
fn a_stage_needs_no_writer_credential() {
    let named = LakehouseRead::requirements()
        .iter()
        .map(Requirement::describe)
        .collect::<Vec<_>>();
    // A task whose secrets are made from this answer holds the read-only token, so the stage never
    // falls back to the unit's own token there.
    assert!(
        named.contains(&format!(
            "required {}",
            LakehouseCatalogConfig::READER_TOKEN_ENV
        )),
        "{named:?}"
    );
    for reader_key in [
        "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
    ] {
        assert!(
            named.contains(&format!("required {reader_key}")),
            "{named:?}"
        );
    }
    assert!(
        named.iter().all(|line| !line.contains("WRITER")),
        "a stage names no writer credential: {named:?}"
    );
}
