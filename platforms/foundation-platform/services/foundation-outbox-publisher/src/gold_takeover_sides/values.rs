//! A side of the verdict read as values (`foundation_publisher_core::json_values`, the reading the
//! switch's content check holds documents to), a 시군구 at a time.
//!
//! `DuckDB` hands the publisher a query's result whole, so a side is read in [`Parts`]: the ranges of
//! its 시도 column's first [`CHUNK_PREFIX`] characters (a 시군구's parcels, a few hundred thousand rows;
//! both tables are ordered by that column, so a range skips the other files), or, for a key whose
//! 시도 is a region label, a sixteenth of a region's ids. Each row's `*_json` cells are
//! read as the values they say; the row's hash (its key with them) is summed over the whole side, and
//! the rows of the verdict's 시도 are written, as values, to the table its Parquet is copied from.

use std::hash::{DefaultHasher, Hash as _, Hasher as _};

use anyhow::{ensure, Context};
use foundation_publisher_core::json_values::canonical_text;
use foundation_publisher_silver::duckdb::{appender_params_from_iter, Connection};

/// The characters of the 시도 column one read holds: a 시군구.
pub(super) const CHUNK_PREFIX: usize = 5;
/// The suffix of a column that holds a JSON document as text, read as the value it says.
pub(super) const JSON_SUFFIX: &str = "_json";

/// The predicate that keeps exactly the rows whose `column` begins with `prefix` (or is `prefix`,
/// when shorter): a range, so `DuckDB` skips the files outside it, and the prefix itself, so a
/// shorter code (a 시도's own row) is read once. A prefix that is not a code (the steward queue's
/// place reference items, `silver.<source>|…`) is bounded by its last character's successor.
pub(super) fn prefix_filter(column: &str, prefix: &str) -> anyhow::Result<String> {
    ensure!(
        !prefix.is_empty()
            && prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'_' | b'|' | b'-')),
        "the prefix {prefix:?} is neither a code nor a name"
    );
    let upper = if prefix.bytes().all(|byte| byte.is_ascii_digit()) {
        let number: u64 = prefix.parse()?;
        let next = format!("{:0width$}", number + 1, width = prefix.len());
        if next.len() > prefix.len() {
            String::new()
        } else {
            format!(" AND {column} < '{next}'")
        }
    } else {
        let (last, head) = prefix.as_bytes().split_last().context("an empty prefix")?;
        let next = format!("{}{}", String::from_utf8_lossy(head), char::from(last + 1));
        format!(" AND {column} < '{next}'")
    };
    Ok(format!(
        "({column} >= '{prefix}'{upper} AND substr({column}, 1, {CHUNK_PREFIX}) = '{prefix}')"
    ))
}

/// How a side is read a part at a time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Parts {
    /// By the first [`CHUNK_PREFIX`] characters of a 시도 code column (a PNU, a region code).
    Code(&'static str),
    /// By a region label column (`scope`: a 시도 code, or `national`) and the first character of an
    /// id column (a UUID): a part is `<label>/<character>`, a sixteenth of a region. The table is
    /// partitioned by the label and ordered by the id, so a part skips the other files.
    Labelled {
        /// The region label column.
        label: &'static str,
        /// The id column the region is cut by.
        id: &'static str,
    },
}

impl Parts {
    /// The SQL of a relation's parts, one row each, as `p`.
    fn distinct_sql(self, relation: &str) -> String {
        match self {
            Self::Code(column) => {
                format!("SELECT DISTINCT substr({column}, 1, {CHUNK_PREFIX}) AS p FROM {relation}")
            }
            Self::Labelled { label, id } => {
                format!("SELECT DISTINCT {label} || '/' || substr({id}, 1, 1) AS p FROM {relation}")
            }
        }
    }

    /// The predicate that keeps exactly the rows of `part`.
    ///
    /// # Errors
    /// Refuses a part that is not one [`Parts::distinct_sql`] lists.
    pub(super) fn filter(self, part: &str) -> anyhow::Result<String> {
        match self {
            Self::Code(column) => prefix_filter(column, part),
            Self::Labelled { label, id } => {
                let (region, first) = part
                    .split_once('/')
                    .with_context(|| format!("the part {part:?} is not <label>/<character>"))?;
                ensure!(
                    !region.is_empty()
                        && region
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                        && first.len() == 1
                        && first.bytes().all(|byte| byte.is_ascii_alphanumeric()),
                    "the part {part:?} is not <label>/<character>"
                );
                let next = char::from(first.as_bytes()[0] + 1);
                Ok(format!(
                    "({label} = '{region}' AND {id} >= '{first}' AND {id} < '{next}')"
                ))
            }
        }
    }

    /// Whether `part` is of the 시도 `sido`.
    pub(super) fn in_sido(self, part: &str, sido: &str) -> bool {
        match self {
            Self::Code(_) => part.starts_with(sido),
            Self::Labelled { .. } => part
                .split_once('/')
                .is_some_and(|(region, _)| region == sido),
        }
    }
}

/// The parts the rows of `relations` have, in order.
pub(super) fn prefixes(
    connection: &Connection,
    relations: &[&str],
    parts: Parts,
) -> anyhow::Result<Vec<String>> {
    let union = relations
        .iter()
        .map(|relation| parts.distinct_sql(relation))
        .collect::<Vec<_>>()
        .join(" UNION ");
    connection
        .prepare(&format!(
            "SELECT p FROM ({union}) WHERE p IS NOT NULL ORDER BY p"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()
        .context("DuckDB failed to list a side's prefixes")
}

/// `columns` as text, for a read whose rows are compared cell by cell.
pub(super) fn as_text(columns: &[&str]) -> String {
    columns
        .iter()
        .map(|column| format!("CAST({column} AS VARCHAR) AS {column}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A cell as the verdict compares it: a `*_json` cell as the values it says (text that is not JSON
/// as it is), any other as its text.
pub(super) fn compared_cell(column: &str, cell: Option<String>) -> Option<String> {
    match cell {
        Some(text) if column.ends_with(JSON_SUFFIX) => Some(canonical_text(&text).unwrap_or(text)),
        other => other,
    }
}

/// One row's hash: every cell, the key's with the content's, so content that moved to another key
/// changes the sum.
pub(super) fn row_hash(cells: &[Option<String>]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for cell in cells {
        cell.as_deref().hash(&mut hasher);
    }
    hasher.finish()
}

/// What one pass over a side found: the sum of its rows' hashes and the rows of the 시도 it wrote.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Pass {
    pub hash_sum: u128,
    pub sido_rows: u64,
}

/// Reads `side(filter)` (`columns`, each as text) a part at a time, sums its rows' hashes and
/// appends the rows of the parts of `sido` to `table` (its columns `columns`, all text), each cell
/// as [`compared_cell`] reads it.
///
/// # Errors
/// Returns an error when `DuckDB` cannot run a read or append a row.
pub(super) fn pass(
    connection: &Connection,
    side: &dyn Fn(&str) -> String,
    parts: Parts,
    prefixes: &[String],
    columns: &[&str],
    sido: &str,
    table: &str,
) -> anyhow::Result<Pass> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {table} ({});",
        columns
            .iter()
            .map(|column| format!("{column} VARCHAR"))
            .collect::<Vec<_>>()
            .join(", ")
    ))?;
    let mut found = Pass::default();
    for prefix in prefixes {
        let rows: Vec<Vec<Option<String>>> = connection
            .prepare(&format!(
                "SELECT {} FROM ({})",
                as_text(columns),
                side(&parts.filter(prefix)?)
            ))?
            .query_map([], |row| {
                (0..columns.len())
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<_, _>>()
            .with_context(|| format!("DuckDB failed to read the prefix {prefix} of {table}"))?;
        let in_sido = parts.in_sido(prefix, sido);
        let mut appender = in_sido.then(|| connection.appender(table)).transpose()?;
        for row in rows {
            let cells: Vec<Option<String>> = row
                .into_iter()
                .zip(columns)
                .map(|(cell, column)| compared_cell(column, cell))
                .collect();
            found.hash_sum = found.hash_sum.wrapping_add(u128::from(row_hash(&cells)));
            if let Some(appender) = appender.as_mut() {
                appender.append_row(appender_params_from_iter(cells.iter()))?;
                found.sido_rows += 1;
            }
        }
        if let Some(mut appender) = appender {
            appender.flush()?;
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefix_reads_its_rows_once() -> anyhow::Result<()> {
        assert_eq!(
            prefix_filter("pnu", "41110")?,
            "(pnu >= '41110' AND pnu < '41111' AND substr(pnu, 1, 5) = '41110')"
        );
        assert_eq!(
            prefix_filter("region_code", "41")?,
            "(region_code >= '41' AND region_code < '42' AND substr(region_code, 1, 5) = '41')"
        );
        assert_eq!(
            prefix_filter("pnu", "99999")?,
            "(pnu >= '99999' AND substr(pnu, 1, 5) = '99999')"
        );
        assert_eq!(
            prefix_filter("subject_code", "silve")?,
            "(subject_code >= 'silve' AND subject_code < 'silvf' AND substr(subject_code, 1, 5) = 'silve')"
        );
        for bad in ["", "4'", "a b", "가"] {
            assert!(prefix_filter("pnu", bad).is_err(), "{bad:?} was taken");
        }
        Ok(())
    }

    /// Two writings of one value hash alike; the same content under another key does not.
    #[test]
    fn a_row_hashes_its_key_with_its_values() {
        let spark = [
            compared_cell("pnu", Some("9999910100100010000".to_owned())),
            compared_cell(
                "a_json",
                Some(r#"{"area_m2":1.0E7,"note":null}"#.to_owned()),
            ),
        ];
        let duckdb = [
            compared_cell("pnu", Some("9999910100100010000".to_owned())),
            compared_cell("a_json", Some(r#"{"area_m2":10000000.0}"#.to_owned())),
        ];
        assert_eq!(row_hash(&spark), row_hash(&duckdb));
        let moved = [
            compared_cell("pnu", Some("9999910100100020000".to_owned())),
            compared_cell("a_json", Some(r#"{"area_m2":10000000.0}"#.to_owned())),
        ];
        assert_ne!(row_hash(&spark), row_hash(&moved));
        // A column that is not a document is its text; a document cell that is not JSON stays text.
        assert_eq!(
            compared_cell("v", Some("1.0E7".to_owned())).as_deref(),
            Some("1.0E7")
        );
        assert_eq!(
            compared_cell("v_json", Some(String::new())).as_deref(),
            Some("")
        );
        assert_eq!(compared_cell("v_json", None), None);
    }

    /// A pass sums every row of the side and writes only the 시도's, each cell as a value.
    #[test]
    fn a_pass_sums_the_side_and_writes_the_sido() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            r#"CREATE TABLE t (pnu VARCHAR, n BIGINT, d_json VARCHAR);
               INSERT INTO t VALUES ('9999910100100010000', 1, '{"x":1.0E7}'),
                 ('9999920100100010000', 2, '[1]'), ('9999930100100010000', 3, NULL);"#,
        )?;
        let side = |filter: &str| format!("SELECT pnu, n, d_json FROM t WHERE {filter}");
        let columns = ["pnu", "n", "d_json"];
        let found = prefixes(&connection, &["t"], Parts::Code("pnu"))?;
        assert_eq!(found, ["99999"]);
        let written = pass(
            &connection,
            &side,
            Parts::Code("pnu"),
            &found,
            &columns,
            "99",
            "side_reference",
        )?;
        assert_eq!(written.sido_rows, 3);
        let rows: Vec<(String, Option<String>)> = connection
            .prepare("SELECT n, d_json FROM side_reference ORDER BY pnu")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        assert_eq!(
            rows,
            [
                ("1".to_owned(), Some(r#"{"x":10000000}"#.to_owned())),
                ("2".to_owned(), Some("[1]".to_owned())),
                ("3".to_owned(), None)
            ]
        );
        // Every row of the side is summed, whether its 시도 is written or not.
        let cells = |pnu: &str, n: &str, document: Option<&str>| {
            [
                Some(pnu.to_owned()),
                Some(n.to_owned()),
                compared_cell("d_json", document.map(str::to_owned)),
            ]
        };
        let expected = [
            cells("9999910100100010000", "1", Some(r#"{"x":10000000.0}"#)),
            cells("9999920100100010000", "2", Some("[1]")),
            cells("9999930100100010000", "3", None),
        ]
        .iter()
        .fold(0_u128, |sum, row| {
            sum.wrapping_add(u128::from(row_hash(row)))
        });
        assert_eq!(written.hash_sum, expected);
        let elsewhere = pass(
            &connection,
            &side,
            Parts::Code("pnu"),
            &found,
            &columns,
            "98",
            "side_candidate",
        )?;
        assert_eq!((elsewhere.sido_rows, elsewhere.hash_sum), (0, expected));
        Ok(())
    }

    #[test]
    fn a_labelled_region_is_read_a_character_of_its_ids_at_a_time() -> anyhow::Result<()> {
        let parts = Parts::Labelled {
            label: "scope",
            id: "place_id",
        };
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE t (scope VARCHAR, place_id VARCHAR, status VARCHAR);
             INSERT INTO t VALUES ('99', 'a1', 'current'), ('99', 'a2', 'current'),
               ('99', 'b1', 'historic'), ('national', 'c1', 'current');",
        )?;
        let found = prefixes(&connection, &["t"], parts)?;
        assert_eq!(found, ["99/a", "99/b", "national/c"]);
        assert_eq!(
            parts.filter("99/a")?,
            "(scope = '99' AND place_id >= 'a' AND place_id < 'b')"
        );
        assert!(parts.in_sido("99/b", "99"));
        assert!(!parts.in_sido("national/c", "99"));
        assert!(!parts.in_sido("991/a", "99"));
        for bad in ["99", "99/ab", "9'/a", "/a", "99/'"] {
            assert!(parts.filter(bad).is_err(), "{bad} was taken");
        }
        let side = |filter: &str| format!("SELECT scope, place_id, status FROM t WHERE {filter}");
        let written = pass(
            &connection,
            &side,
            parts,
            &found,
            &["scope", "place_id", "status"],
            "99",
            "side_reference",
        )?;
        assert_eq!(written.sido_rows, 3);
        Ok(())
    }
}
