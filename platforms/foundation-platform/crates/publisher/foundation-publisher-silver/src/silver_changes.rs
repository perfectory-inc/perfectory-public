//! The changed rows a Silver load that left Spark appends (root ADR-0311, ADR-0559).
//!
//! Of a newest-per-key table: the staged rows whose key is new or whose content differs from the
//! table's current rows at the head it pinned. The Spark loads called it `changed_only`; the
//! `DuckDB` stages share it here.

use anyhow::{ensure, Context};
use lakehouse_domain::{LakehouseRowVersions, LakehouseTableContract};

use crate::duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage};

/// Columns a load writes, not the source: never compared.
pub const LOAD_COLUMNS: [&str; 5] = [
    "source_record_id",
    "source_line_number",
    "source_snapshot_id",
    "ingested_at_utc",
    "row_state",
];

const CHANGED_SQL: &str = include_str!("silver_changes/changed.sql");
const EDITIONS_SQL: &str = include_str!("silver_changes/editions.sql");
const REMOVALS_SQL: &str = include_str!("silver_changes/removals.sql");

/// The table's key: its contract's `current_row_latest` key.
///
/// # Errors
/// Refuses a table that keeps no versions per key.
pub fn table_key(contract: &LakehouseTableContract) -> anyhow::Result<Vec<&'static str>> {
    Ok(contract
        .current_row_latest()
        .with_context(|| format!("{} keeps no versions per key", contract.table_name))?
        .key
        .to_vec())
}

/// Every column but the key, the load's own and `excluded`: what a change is.
///
/// # Errors
/// Refuses a table that keeps no versions per key.
pub fn content_columns(
    contract: &LakehouseTableContract,
    excluded: &[&str],
) -> anyhow::Result<Vec<&'static str>> {
    let key = table_key(contract)?;
    Ok(contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| {
            !key.contains(name) && !LOAD_COLUMNS.contains(name) && !excluded.contains(name)
        })
        .collect())
}

/// `c.x IS NOT DISTINCT FROM s.x AND …`.
#[must_use]
pub fn same(columns: &[&str]) -> String {
    columns
        .iter()
        .map(|column| format!("c.{column} IS NOT DISTINCT FROM s.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The query of `columns` of the table's current rows at `head`; a query of no row (over `staged`,
/// which has the table's columns) when the table has no snapshot yet.
///
/// # Errors
/// Refuses a table that keeps no versions per key; returns a `DuckDB` failure.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn current_rows(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    columns: &[&str],
    staged: &str,
) -> anyhow::Result<String> {
    let Some(head) = head else {
        return Ok(format!(
            "SELECT {} FROM {staged} WHERE false",
            columns.join(", ")
        ));
    };
    let pinned = crate::duckdb_parquet_stage::table_at(contract.table_name, head);
    Ok(contract
        .current_rows_sql_template()
        .with_context(|| format!("{} keeps no versions per key", contract.table_name))?
        .replace("{columns}", &columns.join(", "))
        .replace(
            "{source}",
            &with_contract_columns(stage.connection(), contract, &pinned)?,
        )
        .replace("{filter}", "TRUE"))
}

/// Builds `out` from `staged`: the rows whose key is new or whose `compared` columns differ from every
/// current row of that key at `head`. Returns how many.
///
/// # Errors
/// Refuses a table that keeps no versions per key; returns a `DuckDB` failure.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn changed(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    staged: &str,
    out: &str,
    compared: &[&str],
) -> anyhow::Result<u64> {
    let columns = contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    stage
        .connection()
        .execute_batch(
            &CHANGED_SQL
                .replace(
                    "{current}",
                    &current_rows(stage, contract, head, &columns, staged)?,
                )
                .replace("{staged}", staged)
                .replace("{out}", out)
                .replace("{matches}", &same(compared)),
        )
        .with_context(|| {
            format!(
                "DuckDB failed to compare the rows of {}",
                contract.table_name
            )
        })?;
    let count: i64 =
        stage
            .connection()
            .query_row(&format!("SELECT count(*) FROM {out}"), [], |row| row.get(0))?;
    Ok(u64::try_from(count)?)
}

/// Builds `out` from `staged` of an editions table: every staged row of each group (`per`) whose
/// list is new or differs from the rows of the group's newest edition at `head`. Returns how many.
///
/// # Errors
/// Refuses a table that does not append editions and a group whose newest editions tie; returns a
/// `DuckDB` failure.
pub fn changed_editions(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    staged: &str,
    out: &str,
) -> anyhow::Result<u64> {
    let pinned = head.map(|head| crate::duckdb_parquet_stage::table_at(contract.table_name, head));
    changed_editions_of(stage, contract, pinned.as_deref(), staged, out)
}

/// [`changed_editions`] over `table`: the table as the load names it at its pinned head (`None`: it
/// has no snapshot yet).
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
fn changed_editions_of(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    table: Option<&str>,
    staged: &str,
    out: &str,
) -> anyhow::Result<u64> {
    let LakehouseRowVersions::Editions {
        edition_column,
        newest_first,
        per,
        ..
    } = contract.row_versions
    else {
        anyhow::bail!("{} does not append editions", contract.table_name);
    };
    let columns = contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let content = columns
        .iter()
        .copied()
        .filter(|name| !per.contains(name) && !LOAD_COLUMNS.contains(name))
        .collect::<Vec<_>>();
    let source = match table {
        Some(table) => with_contract_columns(stage.connection(), contract, table)?,
        None => format!("(SELECT {} FROM {staged} WHERE false)", columns.join(", ")),
    };
    let per_match = per
        .iter()
        .map(|column| format!("r.{column} IS NOT DISTINCT FROM s.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let maxima = (0..newest_first.len())
        .map(|index| format!("max({}) AS m{index}", newest_first[index]))
        .collect::<Vec<_>>()
        .join(", ");
    let order = (0..newest_first.len())
        .map(|index| format!("m{index} DESC NULLS LAST"))
        .collect::<Vec<_>>()
        .join(", ");
    stage
        .connection()
        .execute_batch(
            &EDITIONS_SQL
                .replace("{source}", &source)
                .replace("{staged}", staged)
                .replace("{out}", out)
                .replace("{per}", &per.join(", "))
                .replace("{edition}", edition_column)
                .replace("{maxima}", &maxima)
                .replace("{order}", &order)
                .replace("{content}", &content.join(", "))
                .replace("{per_match}", &per_match)
                .replace("{columns}", &columns.join(", ")),
        )
        .with_context(|| {
            format!(
                "DuckDB failed to compare the editions of {}",
                contract.table_name
            )
        })?;
    let tied: i64 = stage.connection().query_row(
        &format!(
            "SELECT count(*) FROM (SELECT {} FROM {out}_editions GROUP BY ALL HAVING count(*) > 1)",
            per.join(", ")
        ),
        [],
        |row| row.get(0),
    )?;
    ensure!(
        tied == 0,
        "{} holds editions tied on {newest_first:?} in {tied} group(s), so none is the newest",
        contract.table_name
    );
    let count: i64 =
        stage
            .connection()
            .query_row(&format!("SELECT count(*) FROM {out}"), [], |row| row.get(0))?;
    Ok(u64::try_from(count)?)
}

/// The result checks of the rows a load appends of one object (root ADR-0559).
///
/// Its unit reports them to Dagster: how many rows there are, how many repeat a key of the table's
/// `(…) unique` quality gate, and how many miss a required column.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResultChecks {
    /// The rows.
    pub rows: u64,
    /// The rows beyond the first of each key the unique gate names.
    pub duplicate_keys: u64,
    /// The rows with a required column null.
    pub missing_required: u64,
}

/// The columns of the table's `(a, b, …) unique` quality gate.
///
/// # Errors
/// Refuses a table that declares none.
pub fn unique_gate(contract: &LakehouseTableContract) -> anyhow::Result<Vec<&'static str>> {
    contract
        .quality_gates
        .iter()
        .find_map(|gate| {
            gate.strip_suffix(" unique")
                .and_then(|columns| columns.strip_prefix('('))
                .and_then(|columns| columns.strip_suffix(')'))
        })
        .map(|columns| columns.split(", ").collect())
        .with_context(|| format!("{} declares no unique gate", contract.table_name))
}

/// The result checks of `rows`, a staging table of rows of `contract`'s table.
///
/// # Errors
/// Refuses a table that declares no unique gate; returns a `DuckDB` failure.
pub fn result_checks(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    rows: &str,
) -> anyhow::Result<ResultChecks> {
    let key = unique_gate(contract)?.join(", ");
    let required = contract
        .columns
        .iter()
        .filter(|column| column.required)
        .map(|column| format!("{} IS NULL", column.name))
        .collect::<Vec<_>>();
    let missing = if required.is_empty() {
        "0".to_owned()
    } else {
        format!("count(*) FILTER (WHERE {})", required.join(" OR "))
    };
    let (rows, duplicate_keys, missing_required): (i64, i64, i64) = stage.connection().query_row(
        &format!("SELECT count(*), count(*) - count(DISTINCT ({key})), {missing} FROM {rows}"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(ResultChecks {
        rows: u64::try_from(rows)?,
        duplicate_keys: u64::try_from(duplicate_keys)?,
        missing_required: u64::try_from(missing_required)?,
    })
}

/// The value of `row_state` that marks a key removed (root ADR-0275).
pub const REMOVED: &str = "removed";

/// Builds `out` from `staged` of a newest-per-key table that keeps removal markers.
///
/// `out` holds the keys new or changed against the current rows at `head`, and, when `complete`, a
/// removal marker per current key the staged rows no longer hold, stamped with this load's object,
/// run and instant. Returns (rows, of them markers).
///
/// # Errors
/// Refuses a table that keeps no versions per key; returns a `DuckDB` failure.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
#[allow(clippy::too_many_arguments)]
pub fn removal_changes(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    staged: &str,
    out: &str,
    complete: bool,
    load: (&str, &str, i64),
) -> anyhow::Result<(u64, u64)> {
    let (object_key, snapshot_id, ingested_at_epoch_seconds) = load;
    ensure!(
        !object_key.contains('\'') && !snapshot_id.contains('\''),
        "a load identity holds a quote"
    );
    let columns = contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let key = table_key(contract)?;
    let markers = columns
        .iter()
        .map(|column| match *column {
            "row_state" => format!("'{REMOVED}' AS row_state"),
            "source_snapshot_id" => format!("'{snapshot_id}' AS source_snapshot_id"),
            "ingested_at_utc" => {
                format!("to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc")
            }
            "source_record_id" => format!("'{object_key}' AS source_record_id"),
            other => format!("c.{other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    stage
        .connection()
        .execute_batch(
            &REMOVALS_SQL
                .replace(
                    "{current}",
                    &current_rows(stage, contract, head, &columns, staged)?,
                )
                .replace("{staged}", staged)
                .replace("{out}", out)
                .replace(
                    "{key_c}",
                    &key.iter()
                        .map(|k| format!("c.{k}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
                .replace("{key}", &key.join(", "))
                .replace("{key_matches}", &same(&key))
                .replace("{content_matches}", &same(&content_columns(contract, &[])?))
                .replace("{complete}", if complete { "true" } else { "false" })
                .replace("{columns}", &columns.join(", "))
                .replace("{marker_columns}", &markers),
        )
        .with_context(|| {
            format!(
                "DuckDB failed to compare the rows of {}",
                contract.table_name
            )
        })?;
    let removed = if columns.contains(&"row_state") {
        format!("count(*) FILTER (WHERE row_state = '{REMOVED}')")
    } else {
        "0".to_owned()
    };
    let (rows, removed): (i64, i64) = stage.connection().query_row(
        &format!("SELECT count(*), {removed} FROM {out}"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((u64::try_from(rows)?, u64::try_from(removed)?))
}

#[cfg(test)]
mod tests;
