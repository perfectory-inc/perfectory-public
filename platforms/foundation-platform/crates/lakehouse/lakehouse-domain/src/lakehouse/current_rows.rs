//! The current rows of a table that keeps versions, defined once (root ADR-0267, ADR-0564 §4).
//!
//! A `LatestPerKey` table holds every version of a key; which row is current is the contract's
//! rule, and a reader that picks by hand reads a second copy of it. Two renderings live here and
//! nowhere else:
//!
//! - [`LakehouseTableContract::current_rows_sql_template`]: one standard SQL text that Spark SQL,
//!   `DuckDB` and Trino run alike. The contract artifact carries it, so the Python readers (the Dagster
//!   checks) substitute into the same characters instead of rendering
//!   their own.
//! - [`LakehouseTableContract::current_rows`]: the same rule over rows a snapshot scan decoded.
//!
//! One fixture (`tests/current_rows.rs`) holds both to the same answer.
//!
//! An `Editions` table read newest first has its SQL twin here too
//! ([`LakehouseTableContract::newest_edition_sql`]) beside the row twin
//! ([`LakehouseTableContract::newest_edition_rows`]); `DuckDB` readers in the publisher take it.

use std::{cmp::Ordering, collections::BTreeMap};

use serde_json::{Map as JsonMap, Value as JsonValue};

use super::{
    LakehouseCurrentRowLatest, LakehouseEditionRead, LakehouseRowVersions, LakehouseTableContract,
    REMOVED_MARKER,
};

/// Where the caller's table reference goes in [`LakehouseTableContract::current_rows_sql_template`].
pub const CURRENT_ROWS_SOURCE: &str = "{source}";
/// Where the caller's projection goes.
pub const CURRENT_ROWS_COLUMNS: &str = "{columns}";
/// Where the caller's filter goes: it restricts which keys are read, never which versions of a
/// key (a filter on a version column would make an older row look current).
pub const CURRENT_ROWS_FILTER: &str = "{filter}";
/// The rank column the query adds inside; a caller's projection never names it.
const RANK: &str = "foundation_current_rank";
/// The edition rank and the per-edition maxima [`LakehouseTableContract::newest_edition_sql`] adds
/// inside; a caller's projection never names them.
const EDITION_RANK: &str = "foundation_edition_rank";
const EDITION_NEWEST: &str = "foundation_edition_newest";

impl LakehouseCurrentRowLatest {
    /// A query of the current rows of `source` (a table reference, pinned by the caller),
    /// projecting `columns`, among the rows `filter` keeps: the SQL twin of
    /// `platform_contracts.current_rows` for a newest-per-key table (root ADR-0267, ADR-0550 §2).
    /// The newest rows per key by `newest_first`, every tied row (a reader that must not see a tie
    /// refuses the extra row; [`LakehouseTableContract::current_rows`] does), and none for a key
    /// whose newest is a removal marker. Standard SQL: Trino, `DuckDB` and Spark SQL run it as is.
    #[must_use]
    pub fn current_rows_sql(&self, source: &str, columns: &[&str], filter: &str) -> String {
        self.render(None)
            .replace(CURRENT_ROWS_COLUMNS, &columns.join(", "))
            .replace(CURRENT_ROWS_SOURCE, source)
            .replace(CURRENT_ROWS_FILTER, filter)
    }

    fn render(&self, predicate: Option<&str>) -> String {
        let order = self
            .newest_first
            .iter()
            .map(|column| format!("{column} DESC NULLS LAST"))
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = predicate.map_or_else(String::new, |text| format!(" AND ({text})"));
        let removal = self.removal_column.map_or_else(String::new, |column| {
            format!(" AND ({column} IS NULL OR {column} <> '{REMOVED_MARKER}')")
        });
        format!(
            "SELECT {CURRENT_ROWS_COLUMNS} FROM (SELECT *, rank() OVER (PARTITION BY {key} ORDER BY {order}) \
             AS {RANK} FROM {CURRENT_ROWS_SOURCE} WHERE {CURRENT_ROWS_FILTER}{predicate}) \
             WHERE {RANK} = 1{removal}",
            key = self.key.join(", "),
        )
    }
}

impl LakehouseTableContract {
    /// The current-rows query of a `LatestPerKey` table with [`CURRENT_ROWS_COLUMNS`],
    /// [`CURRENT_ROWS_SOURCE`] and [`CURRENT_ROWS_FILTER`] left to the reader (`TRUE` reads every
    /// key); `None` for a table that keeps no versions per key. The contract's
    /// `current_row_predicate` is applied before the newest row is chosen, as
    /// `platform_contracts.current_rows` applies it. This text is what the contract artifact
    /// carries as `current_rows_sql` (root ADR-0564 §4).
    #[must_use]
    pub fn current_rows_sql_template(&self) -> Option<String> {
        self.current_row_latest()
            .map(|latest| latest.render(self.current_row_predicate))
    }

    /// A query of the rows of the newest edition of `source` (a table reference, pinned by the
    /// caller) per `per` group, projecting `columns`, among the rows `filter` keeps, then the
    /// contract's `current_row_predicate`: the SQL twin of [`Self::newest_edition_rows`] and
    /// `platform_contracts.newest_edition` (root ADR-0286 §3, ADR-0564 §4). `None` for a table that
    /// does not append editions read newest first.
    ///
    /// Each edition's maxima of `newest_first`, compared in turn with a missing value oldest, rank
    /// the editions of a group; every row of the top edition is read. The edition is chosen over the
    /// whole of `source` and `filter` only restricts which of its rows are read: a filter before the
    /// choice would make an older edition look newest where the newest holds no row. One scan of
    /// `source` (window aggregates, no self-join): a pinned R2 table read twice cost a production run
    /// 46 minutes on 2026-11-19 (`parcel_nearby_stage::current_rows`). SQL cannot refuse two
    /// editions tied on every column; a reader that must not read both counts the editions per group
    /// it read, as [`Self::newest_edition_rows`] refuses them.
    #[must_use]
    pub fn newest_edition_sql(
        &self,
        source: &str,
        columns: &[&str],
        filter: &str,
    ) -> Option<String> {
        let LakehouseRowVersions::Editions {
            edition_column,
            newest_first,
            per,
            read: LakehouseEditionRead::Newest,
        } = self.row_versions
        else {
            return None;
        };
        let partition = |extra: Option<&str>| -> String {
            let columns: Vec<&str> = per.iter().copied().chain(extra).collect();
            if columns.is_empty() {
                String::new()
            } else {
                format!("PARTITION BY {}", columns.join(", "))
            }
        };
        let maxima = newest_first
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "max({column}) OVER ({}) AS {EDITION_NEWEST}_{index}",
                    partition(Some(edition_column))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let order = (0..newest_first.len())
            .map(|index| format!("{EDITION_NEWEST}_{index} DESC NULLS LAST"))
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = self
            .current_row_predicate
            .map_or_else(String::new, |text| format!(" AND ({text})"));
        Some(format!(
            "SELECT {columns} FROM (SELECT *, dense_rank() OVER ({partition} ORDER BY {order}) AS {EDITION_RANK} \
             FROM (SELECT *, {maxima} FROM {source})) WHERE {EDITION_RANK} = 1 AND ({filter}){predicate}",
            columns = columns.join(", "),
            partition = partition(None),
        ))
    }

    /// The current rows among `rows` (every version a scan decoded, of whole keys): the newest
    /// row per key by `newest_first`, compared in turn with a missing value oldest, and no row for
    /// a key whose newest is a removal marker. The row twin of
    /// [`Self::current_rows_sql_template`].
    ///
    /// # Errors
    ///
    /// The table keeps no versions per key; it selects by a predicate, which is SQL and cannot be
    /// applied to decoded rows; two rows of a key tie on every order column and the contract does
    /// not keep ties; or an order column holds values that have no order (two types, or neither
    /// text nor a number).
    pub fn current_rows<'a>(
        &self,
        rows: &'a [JsonMap<String, JsonValue>],
    ) -> Result<Vec<&'a JsonMap<String, JsonValue>>, CurrentRowsError> {
        let table = self.table_name;
        let latest = self
            .current_row_latest()
            .ok_or(CurrentRowsError::NotVersioned(table))?;
        if self.current_row_predicate.is_some() {
            return Err(CurrentRowsError::Predicate(table));
        }
        let key_of = |row: &JsonMap<String, JsonValue>| -> Vec<String> {
            latest
                .key
                .iter()
                .map(|column| match row.get(*column) {
                    None | Some(JsonValue::Null) => String::new(),
                    Some(JsonValue::String(text)) => text.clone(),
                    Some(other) => other.to_string(),
                })
                .collect()
        };
        // Per key: the rows that are its newest so far (several only while they tie).
        let mut newest: BTreeMap<Vec<String>, Vec<&'a JsonMap<String, JsonValue>>> =
            BTreeMap::new();
        for row in rows {
            let held = newest.entry(key_of(row)).or_default();
            let Some(first) = held.first() else {
                held.push(row);
                continue;
            };
            match newer(&latest, row, first)
                .map_err(|column| CurrentRowsError::Unordered { table, column })?
            {
                Ordering::Greater => *held = vec![row],
                Ordering::Equal => held.push(row),
                Ordering::Less => {}
            }
        }
        let mut current = Vec::with_capacity(newest.len());
        for (key, held) in newest {
            if held.len() > 1 && !latest.keep_ties {
                return Err(CurrentRowsError::Tied { table, key });
            }
            current.extend(held.into_iter().filter(|row| {
                latest.removal_column.is_none_or(|column| {
                    row.get(column).and_then(JsonValue::as_str) != Some(REMOVED_MARKER)
                })
            }));
        }
        Ok(current)
    }
}

/// How `row` orders against `held` by `newest_first`: `Greater` when `row` is the newer. `Err`
/// names the column whose two values have no order.
fn newer(
    latest: &LakehouseCurrentRowLatest,
    row: &JsonMap<String, JsonValue>,
    held: &JsonMap<String, JsonValue>,
) -> Result<Ordering, &'static str> {
    for column in latest.newest_first {
        let order = match (
            row.get(*column).unwrap_or(&JsonValue::Null),
            held.get(*column).unwrap_or(&JsonValue::Null),
        ) {
            // DESC NULLS LAST: a row without a value is the oldest.
            (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
            (JsonValue::Null, _) => Ordering::Less,
            (_, JsonValue::Null) => Ordering::Greater,
            (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
            (JsonValue::Number(left), JsonValue::Number(right)) => {
                match (left.as_i64(), right.as_i64()) {
                    (Some(left), Some(right)) => left.cmp(&right),
                    _ => left
                        .as_f64()
                        .zip(right.as_f64())
                        .and_then(|(left, right)| left.partial_cmp(&right))
                        .ok_or(*column)?,
                }
            }
            _ => return Err(column),
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}

/// Why [`LakehouseTableContract::current_rows`] names no current rows.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum CurrentRowsError {
    /// The table's rows do not pile up as versions per key.
    #[error("{0} keeps no versions per key")]
    NotVersioned(&'static str),
    /// The contract selects current rows by a SQL predicate first.
    #[error("{0} selects its current rows by a predicate: read them through current_rows_sql")]
    Predicate(&'static str),
    /// Two rows of one key tie on every `newest_first` column.
    #[error("{table} holds rows of key {key:?} tied on its version order, so none is current")]
    Tied {
        /// The table.
        table: &'static str,
        /// The key's values, in the contract's key order.
        key: Vec<String>,
    },
    /// An order column holds two values that cannot be compared.
    #[error("{table}: the values of {column} have no order")]
    Unordered {
        /// The table.
        table: &'static str,
        /// The `newest_first` column.
        column: &'static str,
    },
}
