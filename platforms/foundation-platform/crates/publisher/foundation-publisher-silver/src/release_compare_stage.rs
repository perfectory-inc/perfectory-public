//! What a release appends to `main`, compared on `DuckDB` (root ADR-0275, ADR-0318, ADR-0599).
//!
//! A Silver refresh lane holds its release as a local copy (the table's columns) and compares it
//! with the current rows of `main` at one pinned snapshot, bucket by bucket: every row of a key
//! that is new or whose rows changed, and one removal marker per key that has current rows and
//! none in the release. It was the Spark job `silver_release_compare.py`; the rule is that job's,
//! in `release_compare_stage/compare.sql`.
//!
//! Which rows are current is the table contract's query (root ADR-0564 §4). This module only
//! hands it a narrow projection of `main` (the key, the order columns and one fingerprint per
//! row), so the rank window never carries a content column.

use std::path::Path;

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseTableContract;

use crate::{
    data_go_kr_file_data_stage::{column_names, epoch, literal},
    duckdb_parquet_stage::{sql_path, DuckDbParquetStage},
};

const COMPARE_SQL: &str = include_str!("release_compare_stage/compare.sql");
/// Written by the load, never by the source: comparing them would make every row look changed.
const LOAD_COLUMNS: [&str; 3] = ["source_record_id", "source_snapshot_id", "ingested_at_utc"];
const INSTANT_COLUMN: &str = "ingested_at_utc";
const REMOVED: &str = "removed";

/// How one table's releases are compared: its row key, its order of versions and its content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompareRule {
    /// The row key.
    pub key: Vec<&'static str>,
    /// What orders a key's versions, newest first: `[edition, ingested_at_utc]` or the instant
    /// alone (root ADR-0328).
    pub newest_first: Vec<&'static str>,
    /// The column a removal marker sets.
    pub removal_column: &'static str,
    /// The column a release's edition is in; `None` for a table ordered by its instant alone.
    pub edition_column: Option<&'static str>,
    /// The columns a row's content is (the lane contract's `compare.content_columns`).
    pub content: Vec<String>,
}

impl CompareRule {
    /// The rule of `contract` for a lane that compares `content`.
    ///
    /// # Errors
    /// The table keeps no newest row per key with a removal column, selects its current rows by
    /// a predicate, orders its versions another way than a compare can stamp, or `content` names
    /// a column that is not its own source column.
    pub fn of(contract: &LakehouseTableContract, content: &[String]) -> anyhow::Result<Self> {
        let table = contract.table_name;
        let latest = contract
            .current_row_latest()
            .with_context(|| format!("{table} keeps no newest row per key to compare with"))?;
        let removal_column = latest.removal_column.with_context(|| {
            format!("{table} declares no removal column to append changes under")
        })?;
        ensure!(
            contract.current_row_predicate.is_none(),
            "{table} selects its current rows by a predicate: the narrow compare does not carry its columns"
        );
        let (last, editions) = latest
            .newest_first
            .split_last()
            .with_context(|| format!("{table} orders its versions by nothing"))?;
        ensure!(
            *last == INSTANT_COLUMN && editions.len() <= 1 && !editions.contains(&INSTANT_COLUMN),
            "a compared table orders its versions by [edition, {INSTANT_COLUMN}] or [{INSTANT_COLUMN}], not {:?}",
            latest.newest_first
        );
        let names = column_names(contract);
        let unknown: Vec<&String> = content
            .iter()
            .filter(|column| !names.contains(&column.as_str()))
            .collect();
        let loaded: Vec<&String> = content
            .iter()
            .filter(|column| {
                let column = column.as_str();
                latest.key.contains(&column)
                    || LOAD_COLUMNS.contains(&column)
                    || latest.newest_first.contains(&column)
                    || column == removal_column
            })
            .collect();
        ensure!(
            !content.is_empty() && unknown.is_empty() && loaded.is_empty(),
            "{table} content columns must be its own source columns, not its key or load columns: \
             unknown {unknown:?}, not content {loaded:?}"
        );
        Ok(Self {
            key: latest.key.to_vec(),
            newest_first: latest.newest_first.to_vec(),
            removal_column,
            edition_column: editions.first().copied(),
            content: content.to_vec(),
        })
    }
}

/// What every row a compare appends is stamped with, changed row and marker alike (ADR-0328).
#[derive(Clone, Copy, Debug)]
pub struct Stamp<'a> {
    /// The release's `source_snapshot_id`: what a marker carries.
    pub source_snapshot_id: &'a str,
    /// The release's edition, for a table ordered by one.
    pub edition: Option<&'a str>,
    /// One instant for the run: a marker outranks the row it removes.
    pub run_at: DateTime<Utc>,
}

/// One bucket of keys: the leading characters of the key's first column (root ADR-0318).
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Bucket {
    /// The rows whose key column is null.
    Null,
    /// The rows whose key column starts with this; shorter than the bucket length, it is the
    /// whole value, so no value is in two buckets.
    Prefix(String),
    /// The compare is not split: every row.
    All,
}

impl Bucket {
    /// The rows of the bucket, as a predicate over `column` for buckets of `length` characters.
    #[must_use]
    pub fn predicate(&self, column: &str, length: u32) -> String {
        match self {
            Self::All => "TRUE".to_owned(),
            Self::Null => format!("{column} IS NULL"),
            Self::Prefix(prefix)
                if u32::try_from(prefix.chars().count()).is_ok_and(|n| n < length) =>
            {
                format!("{column} = {}", literal(prefix))
            }
            Self::Prefix(prefix) => format!("starts_with({column}, {})", literal(prefix)),
        }
    }

    /// How the bucket is named in a summary: JSON, so the null bucket and `""` stay apart.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::All => "\"*\"".to_owned(),
            Self::Null => "null".to_owned(),
            Self::Prefix(prefix) => serde_json::Value::String(prefix.clone()).to_string(),
        }
    }
}

/// The buckets `sides` hold, null first, then in order: the same order on every run.
///
/// `sides` are relations with the key column (the release, and `main` unless the release states
/// only its own buckets, root ADR-0396); only that column is read.
///
/// # Errors
/// `DuckDB` fails a read.
pub fn buckets(
    stage: &DuckDbParquetStage,
    rule: &CompareRule,
    sides: &[&str],
    length: u32,
) -> anyhow::Result<Vec<Bucket>> {
    if length == 0 {
        return Ok(vec![Bucket::All]);
    }
    let column = rule.key[0];
    let mut found = std::collections::BTreeSet::new();
    for side in sides {
        let mut statement = stage.connection().prepare(&format!(
            "SELECT DISTINCT substr({column}, 1, {length}) FROM {side}"
        ))?;
        let prefixes = statement
            .query_map([], |row| row.get::<_, Option<String>>(0))?
            .collect::<Result<Vec<_>, _>>()
            .context("DuckDB failed to read the buckets")?;
        found.extend(
            prefixes
                .into_iter()
                .map(|prefix| prefix.map_or(Bucket::Null, Bucket::Prefix)),
        );
    }
    Ok(found.into_iter().collect())
}

/// What one bucket's compare found.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BucketChanges {
    /// Keys that are new or whose rows changed: every release row of theirs is appended.
    pub changed_keys: u64,
    /// Keys with current rows and none in the release: one removal marker each.
    pub removed_keys: u64,
}

/// `compare.sql` for one bucket: `release` and `main` are relations of the table's columns
/// (`main` pinned by the caller, with every contract column), `in_bucket` the bucket's predicate.
///
/// # Errors
/// The table's edition is not text, or the release names none where the table is ordered by one.
// `{name}` is a placeholder of the SQL text, not a format argument. One template, its parameters
// rendered in one place.
#[allow(clippy::literal_string_with_formatting_args, clippy::too_many_lines)]
pub fn render(
    contract: &LakehouseTableContract,
    rule: &CompareRule,
    release: &str,
    main: &str,
    in_bucket: &str,
    stamp: Stamp<'_>,
) -> anyhow::Result<String> {
    let latest = contract
        .current_row_latest()
        .context("a compared table keeps a newest row per key")?;
    let columns = column_names(contract);
    let edition = match rule.edition_column {
        None => None,
        Some(column) => {
            let kind = contract
                .columns
                .iter()
                .find(|declared| declared.name == column)
                .map(|declared| declared.logical_type);
            ensure!(
                kind == Some("string"),
                "{}: the edition column {column} is {kind:?}; this compare stamps a text edition",
                contract.table_name
            );
            let Some(edition) = stamp.edition.filter(|edition| !edition.is_empty()) else {
                bail!(
                    "a release of a table ordered by {column} names its edition ({})",
                    contract.table_name
                );
            };
            Some((column, literal(edition)))
        }
    };
    let same = |left: &str, right: &str| {
        rule.key
            .iter()
            .map(|key| format!("{left}.{key} IS NOT DISTINCT FROM {right}.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let qualified = |alias: &str| {
        rule.key
            .iter()
            .map(|key| format!("{alias}.{key}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Null-ness hashed apart from the value, so a null and an empty string differ.
    let fingerprint = format!(
        "hash({}, {})",
        rule.content
            .iter()
            .map(|column| format!("{column} IS NULL"))
            .collect::<Vec<_>>()
            .join(", "),
        rule.content.join(", ")
    );
    let mut narrow: Vec<&str> = Vec::new();
    for column in rule
        .key
        .iter()
        .chain(&rule.newest_first)
        .chain(std::iter::once(&rule.removal_column))
    {
        if !narrow.contains(column) {
            narrow.push(column);
        }
    }
    let narrow_source = format!(
        "(SELECT {}, {fingerprint} AS rc_fp FROM {main} WHERE {in_bucket}) rc_narrow",
        narrow.join(", ")
    );
    let full_source = format!(
        "(SELECT * FROM {main} g WHERE {in_bucket} AND EXISTS (SELECT 1 FROM rc_removed k WHERE {})) rc_gone",
        same("k", "g")
    );
    let key_and_fingerprint: Vec<&str> = rule
        .key
        .iter()
        .copied()
        .chain(std::iter::once("rc_fp"))
        .collect();
    let mut sql = COMPARE_SQL.to_owned();
    for (name, value) in [
        ("{release}", release.to_owned()),
        (
            "{current_narrow}",
            latest.current_rows_sql(&narrow_source, &key_and_fingerprint, "TRUE"),
        ),
        (
            "{current_full}",
            latest.current_rows_sql(&full_source, &columns, "TRUE"),
        ),
        ("{key}", rule.key.join(", ")),
        ("{s_key}", qualified("s")),
        ("{c_key}", qualified("c")),
        ("{m_key}", qualified("m")),
        ("{same_s_c}", same("s", "c")),
        ("{same_k_r}", same("k", "r")),
        ("{fingerprint}", fingerprint),
        (
            "{stamped}",
            stamped(&columns, rule, edition.as_ref(), stamp, "r", false),
        ),
        (
            "{marker}",
            stamped(&columns, rule, edition.as_ref(), stamp, "m", true),
        ),
        ("{marker_order}", marker_order(&columns, rule)),
    ] {
        sql = sql.replace(name, &value);
    }
    Ok(sql)
}

/// A row's columns as the compare appends them: the run's instant and the release's edition on
/// every row; on a marker also the removal value and the release's `source_snapshot_id`.
fn stamped(
    columns: &[&str],
    rule: &CompareRule,
    edition: Option<&(&str, String)>,
    stamp: Stamp<'_>,
    alias: &str,
    marker: bool,
) -> String {
    columns
        .iter()
        .map(|column| {
            if *column == INSTANT_COLUMN {
                return format!("{} AS {column}", epoch(stamp.run_at));
            }
            if let Some((edition_column, value)) = edition {
                if column == edition_column {
                    return format!("{value} AS {column}");
                }
            }
            if marker && *column == rule.removal_column {
                return format!("'{REMOVED}' AS {column}");
            }
            if marker && *column == "source_snapshot_id" {
                return format!("{} AS {column}", literal(stamp.source_snapshot_id));
            }
            format!("{alias}.{column}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The content columns, then every other column: one order, so no two rows of a key tie.
fn marker_order(columns: &[&str], rule: &CompareRule) -> String {
    rule.content
        .iter()
        .map(String::as_str)
        .chain(
            columns
                .iter()
                .copied()
                .filter(|column| !rule.content.iter().any(|content| content == column)),
        )
        .map(|column| format!("m.{column}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Compares one bucket and leaves its rows as the view `rc_changes` (the table's columns).
///
/// The rows are not materialised: the caller reads the view once, when it writes the bucket's
/// file, and takes the row counts from that file.
///
/// # Errors
/// What [`render`] refuses, or a `DuckDB` failure.
pub fn compare_bucket(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    rule: &CompareRule,
    release: &str,
    main: &str,
    in_bucket: &str,
    stamp: Stamp<'_>,
) -> anyhow::Result<BucketChanges> {
    let sql = render(contract, rule, release, main, in_bucket, stamp)?;
    stage.connection().execute_batch(&sql).with_context(|| {
        format!(
            "DuckDB failed to compare a bucket of {}",
            contract.table_name
        )
    })?;
    let (changed, removed): (i64, i64) = stage.connection().query_row(
        "SELECT (SELECT count(*) FROM rc_changed), (SELECT count(*) FROM rc_removed)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(BucketChanges {
        changed_keys: u64::try_from(changed)?,
        removed_keys: u64::try_from(removed)?,
    })
}

/// A handoff column read as text and cast to the table's type: what the Spark load's
/// `cast_handoff_frame` did (an instant is `yyyy-MM-ddTHH:mm:ssZ`, a binary column lowercase hex).
fn typed(name: &str, logical_type: &str) -> anyhow::Result<String> {
    Ok(match logical_type {
        "string" => name.to_owned(),
        // No time zone database is loaded in the stage: the instant is read as UTC text.
        "timestamp" => {
            format!("to_timestamp(epoch(strptime({name}, '%Y-%m-%dT%H:%M:%SZ'))) AS {name}")
        }
        "date" => format!("CAST({name} AS DATE) AS {name}"),
        "int" => format!("CAST({name} AS INTEGER) AS {name}"),
        "long" => format!("CAST({name} AS BIGINT) AS {name}"),
        "double" => format!("CAST({name} AS DOUBLE) AS {name}"),
        "boolean" => format!("CAST({name} AS BOOLEAN) AS {name}"),
        "binary" => format!("from_hex({name}) AS {name}"),
        decimal if decimal.starts_with("decimal(") => {
            format!("CAST({name} AS {decimal}) AS {name}")
        }
        other => bail!("a handoff column of type {other} has no reader"),
    })
}

/// Copies a release's handoffs into one local Parquet file; returns its row count.
///
/// `urls` are gzip JSON Lines objects the stage's keys may read; the copy has the table's columns
/// and types (root ADR-0599 §1). Every bucket of the compare reads this copy, never the handoffs
/// again. It is written under a temporary name and renamed, so a file at `copy` is a whole copy.
///
/// # Errors
/// A handoff column has no reader, a value does not cast, or `DuckDB` fails the read or write.
pub fn copy_release(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    urls: &[String],
    copy: &Path,
) -> anyhow::Result<u64> {
    ensure!(!urls.is_empty(), "a release has at least one handoff");
    let read = contract
        .columns
        .iter()
        .map(|column| format!("{}: 'VARCHAR'", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let columns = contract
        .columns
        .iter()
        .map(|column| typed(column.name, column.logical_type))
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    let handoffs = urls
        .iter()
        .map(|url| literal(url))
        .collect::<Vec<_>>()
        .join(", ");
    let partial = copy.with_extension("partial");
    stage
        .connection()
        .execute_batch(&format!(
            "COPY (SELECT {columns} FROM read_json([{handoffs}], format = 'newline_delimited', \
             compression = 'gzip', columns = {{{read}}})) TO '{}' (FORMAT parquet, COMPRESSION zstd)",
            sql_path(&partial)?
        ))
        .with_context(|| format!("DuckDB failed to copy the release of {}", contract.table_name))?;
    std::fs::rename(&partial, copy).with_context(|| format!("cannot finish {}", copy.display()))?;
    let rows: i64 = stage.connection().query_row(
        &format!("SELECT count(*) FROM read_parquet('{}')", sql_path(copy)?),
        [],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(rows)?)
}

/// The release's local copy as a relation.
///
/// # Errors
/// The path is not UTF-8 or holds a quote.
pub fn release_relation(copy: &Path) -> anyhow::Result<String> {
    Ok(format!("read_parquet('{}')", sql_path(copy)?))
}

/// The row checks of a release before anything of it is committed (root ADR-0599 §2): what the
/// Spark load refused before it staged a batch. Returns the release's row count.
///
/// # Errors
/// The release holds no row, a required column holds a null, a required text column an empty
/// value, a checksum is not lowercase SHA-256 hex, a normalization status is outside its domain,
/// the rows are not all of `source_snapshot_id`, or their count is not `expected_rows`.
pub fn check_release(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    release: &str,
    source_snapshot_id: &str,
    expected_rows: Option<u64>,
) -> anyhow::Result<u64> {
    let table = contract.table_name;
    let mut checks: Vec<(String, String)> = Vec::new();
    for column in contract.columns.iter().filter(|column| column.required) {
        checks.push((
            format!("{} must not be null", column.name),
            format!("{} IS NULL", column.name),
        ));
        if column.logical_type == "string" {
            checks.push((
                format!("{} must not be empty", column.name),
                format!("length({}) = 0", column.name),
            ));
        }
    }
    let names = column_names(contract);
    if names.contains(&"row_checksum_sha256") {
        checks.push((
            "row_checksum_sha256 must be lowercase sha256 hex".to_owned(),
            "NOT regexp_full_match(row_checksum_sha256, '[0-9a-f]{64}')".to_owned(),
        ));
    }
    if names.contains(&"normalization_status") {
        checks.push((
            "normalization_status is outside the allowed domain".to_owned(),
            "normalization_status NOT IN ('accepted', 'proposal_required')".to_owned(),
        ));
    }
    if names.contains(&"source_snapshot_id") {
        checks.push((
            format!("every row is of the release {source_snapshot_id}"),
            format!(
                "source_snapshot_id IS DISTINCT FROM {}",
                literal(source_snapshot_id)
            ),
        ));
    }
    let counts = checks
        .iter()
        .map(|(_, violated)| format!("count(*) FILTER ({violated})"))
        .collect::<Vec<_>>()
        .join(", ");
    let separator = if counts.is_empty() { "" } else { ", " };
    let mut statement = stage
        .connection()
        .prepare(&format!(
            "SELECT count(*){separator}{counts} FROM {release}"
        ))
        .with_context(|| format!("DuckDB could not check the release of {table}"))?;
    let found: Vec<i64> = statement
        .query_row([], |row| {
            (0..=checks.len())
                .map(|index| row.get::<_, i64>(index))
                .collect()
        })
        .with_context(|| format!("DuckDB failed to check the release of {table}"))?;
    let rows = u64::try_from(found[0])?;
    ensure!(
        rows > 0,
        "the release of {table} holds no row: every key would read as removed"
    );
    for ((rule, _), violations) in checks.iter().zip(&found[1..]) {
        ensure!(*violations == 0, "{table}: {rule}. count={violations}");
    }
    if let Some(expected) = expected_rows {
        ensure!(
            rows == expected,
            "expected {expected} rows of {table}, found {rows}"
        );
    }
    Ok(rows)
}

#[cfg(test)]
mod tests;
