//! Read-only SQL over the lakehouse for the host's scripts, in place of `docker exec trino` (root
//! ADR-0565: Trino's readers move to the publisher's `DuckDB`, ADR-0559). Each query names its table
//! and reads its rows as `{source}`: the table at the snapshot asked, or at `main`'s head from the
//! catalog's history, so the answer says which snapshot it counted. `{table}` is the table as it is,
//! for what belongs to the table rather than to a snapshot: its current schema, which a column added
//! after the newest snapshot is part of (Trino's `SHOW COLUMNS`; a snapshot's own schema lacks it,
//! measured 2026-11-19 on five Silver tables and `row_state`). One run attaches the catalog once for
//! every query on stdin.
//!
//! stdin: one JSON object per line, `{"table": "<namespace>.<name>", "sql": "... {source} | {table}
//! ...", "snapshot_id": <optional>}`.
//! stdout: one JSON object per query, in the order asked: `{"table", "snapshot_id", "rows": [{...}]}`;
//! `"absent": true` and no rows when the catalog has no such table (the SQL is not run); `snapshot_id`
//! null when the table has no snapshot yet (the SQL reads it as it is). A query `DuckDB` refuses
//! answers `"error"`, and the run goes on to the next.
//!
//! The work directory is `FOUNDATION_PLATFORM_LAKEHOUSE_QUERY_WORK` (the calling job's state):
//! `DuckDB` spills there past its share of what the calling unit may still hold (`engine_config`).

use std::io::BufRead as _;
use std::path::Path;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::lakehouse_reader::{source, LakehouseReader};
use crate::public_data_control_support::required_env_value;

const WORK_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_QUERY_WORK";
const SOURCE: &str = "{source}";
const TABLE: &str = "{table}";

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Query {
    table: String,
    sql: String,
    #[serde(default)]
    snapshot_id: Option<i64>,
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
struct Answer {
    table: String,
    snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    absent: bool,
    rows: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn query(line: &str) -> anyhow::Result<Option<Query>> {
    if line.trim().is_empty() {
        return Ok(None);
    }
    let query: Query =
        serde_json::from_str(line).with_context(|| format!("not a query line: {line:?}"))?;
    let valid = query.table.matches('.').count() == 1
        && query
            .table
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.');
    if !valid {
        bail!(
            "expected a table `<namespace>.<name>`, got {:?}",
            query.table
        );
    }
    if !query.sql.contains(SOURCE) && !query.sql.contains(TABLE) {
        bail!(
            "the query of {} reads neither {SOURCE} nor {TABLE}",
            query.table
        );
    }
    Ok(Some(query))
}

/// The SQL `DuckDB` runs: `{source}` is the table at `snapshot`, `{table}` the table as it is.
fn statement(query: &Query, snapshot: Option<i64>) -> String {
    query
        .sql
        .replace(SOURCE, &source(&query.table, snapshot))
        .replace(TABLE, &source(&query.table, None))
}

pub(crate) async fn run() -> anyhow::Result<()> {
    // Every line first: the lock on stdin is not held across a catalog call.
    let mut queries = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let line = line.context("failed to read the queries from stdin")?;
        queries.extend(query(&line)?);
    }
    let reader = LakehouseReader::open(Path::new(&required_env_value(WORK_ENV)?))?;
    for asked in queries {
        let mut answer = Answer {
            table: asked.table.clone(),
            ..Answer::default()
        };
        match asked.snapshot_id {
            Some(snapshot) => answer.snapshot_id = Some(snapshot),
            None => match reader.main_snapshot(&asked.table).await? {
                None => answer.absent = true,
                Some(snapshot) => answer.snapshot_id = snapshot,
            },
        }
        if !answer.absent {
            match reader.json_rows(&statement(&asked, answer.snapshot_id)) {
                Ok(rows) => answer.rows = rows,
                Err(error) => answer.error = Some(format!("{error:#}")),
            }
        }
        println!("{}", serde_json::to_string(&answer)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{query, statement, Answer, Query};

    #[test]
    fn a_line_is_one_table_and_a_query_that_reads_it() -> anyhow::Result<()> {
        assert_eq!(
            query(r#"{"table": "silver.parcels", "sql": "SELECT count(*) AS n FROM {source}"}"#)?,
            Some(Query {
                table: "silver.parcels".to_owned(),
                sql: "SELECT count(*) AS n FROM {source}".to_owned(),
                snapshot_id: None,
            })
        );
        assert_eq!(query("  ")?, None);
        for bad in [
            r#"{"table": "parcels", "sql": "SELECT 1 FROM {source}"}"#,
            r#"{"table": "silver.Parcels", "sql": "SELECT 1 FROM {source}"}"#,
            r#"{"table": "silver.p; DROP", "sql": "SELECT 1 FROM {source}"}"#,
            r#"{"table": "silver.parcels", "sql": "SELECT 1"}"#,
            r#"{"table": "silver.parcels", "sql": "SELECT 1 FROM tables"}"#,
            r#"{"table": "silver.parcels", "sql": "SELECT 1 FROM {source}", "extra": 1}"#,
            "not json",
        ] {
            assert!(query(bad).is_err(), "{bad}");
        }
        Ok(())
    }

    #[test]
    fn the_source_is_the_table_at_its_snapshot_or_as_it_is() {
        let asked = Query {
            table: "silver.parcels".to_owned(),
            sql: "DESCRIBE SELECT * FROM {source}".to_owned(),
            snapshot_id: None,
        };
        assert_eq!(
            statement(&asked, Some(42)),
            "DESCRIBE SELECT * FROM (SELECT * FROM lakehouse.silver.parcels AT (VERSION => 42))"
        );
        assert_eq!(
            statement(&asked, None),
            "DESCRIBE SELECT * FROM lakehouse.silver.parcels"
        );
        let schema = Query {
            sql: "DESCRIBE {table}".to_owned(),
            ..asked
        };
        assert_eq!(
            statement(&schema, Some(42)),
            "DESCRIBE lakehouse.silver.parcels",
            "the table's current schema, whatever snapshot is answered"
        );
    }

    #[test]
    fn an_absent_table_says_so_and_an_answer_leaves_out_what_it_lacks() -> anyhow::Result<()> {
        let absent = Answer {
            table: "silver.parcels".to_owned(),
            absent: true,
            ..Answer::default()
        };
        assert_eq!(
            serde_json::to_string(&absent)?,
            r#"{"table":"silver.parcels","snapshot_id":null,"absent":true,"rows":[]}"#
        );
        let read = Answer {
            table: "silver.parcels".to_owned(),
            snapshot_id: Some(7),
            rows: vec![serde_json::json!({"n": 3})],
            ..Answer::default()
        };
        assert_eq!(
            serde_json::to_string(&read)?,
            r#"{"table":"silver.parcels","snapshot_id":7,"rows":[{"n":3}]}"#
        );
        Ok(())
    }
}
