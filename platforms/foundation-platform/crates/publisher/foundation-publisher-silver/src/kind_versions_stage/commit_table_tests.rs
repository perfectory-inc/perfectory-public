use super::*;

/// The previous 시도's commit tables pass; a `TEMP` table of their names, which would shadow the
/// next commit's, is refused.
#[test]
fn only_a_temp_table_of_the_commit_s_names_is_refused() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE OR REPLACE TABLE current_rows AS SELECT 1 AS pnu; \
         CREATE OR REPLACE TABLE appended AS SELECT 1 AS pnu;",
    )?;
    ensure_no_commit_tables(&connection)?;
    connection.execute_batch("CREATE TEMP TABLE appended AS SELECT 2 AS pnu;")?;
    let refused = ensure_no_commit_tables(&connection)
        .err()
        .map(|error| error.to_string());
    assert!(refused.is_some_and(|text| text.contains("appended")));
    Ok(())
}
