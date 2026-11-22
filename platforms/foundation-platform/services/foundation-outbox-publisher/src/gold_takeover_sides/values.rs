//! A side of the verdict read as values (`foundation_publisher_core::json_values`, the reading the
//! switch's content check holds documents to), a 시군구 at a time.
//!
//! `DuckDB` hands the publisher a query's result whole, so a side is read in the ranges of its 시도
//! column's first [`CHUNK_PREFIX`] characters (a 시군구's parcels, a few hundred thousand rows; both
//! tables are ordered by that column, so a range skips the other files). Each row's `*_json` cells are
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
/// shorter code (a 시도's own row) is read once.
pub(super) fn prefix_filter(column: &str, prefix: &str) -> anyhow::Result<String> {
    ensure!(
        !prefix.is_empty() && prefix.bytes().all(|byte| byte.is_ascii_digit()),
        "the prefix {prefix:?} is not digits"
    );
    let number: u64 = prefix.parse()?;
    let next = format!("{:0width$}", number + 1, width = prefix.len());
    let upper = if next.len() > prefix.len() {
        String::new()
    } else {
        format!(" AND {column} < '{next}'")
    };
    Ok(format!(
        "({column} >= '{prefix}'{upper} AND substr({column}, 1, {CHUNK_PREFIX}) = '{prefix}')"
    ))
}

/// The prefixes the rows of `relations` have in `column`, in order.
pub(super) fn prefixes(
    connection: &Connection,
    relations: &[&str],
    column: &str,
) -> anyhow::Result<Vec<String>> {
    let union = relations
        .iter()
        .map(|relation| {
            format!("SELECT DISTINCT substr({column}, 1, {CHUNK_PREFIX}) AS p FROM {relation}")
        })
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

/// Reads `side(filter)` (`columns`, each as text) a prefix at a time, sums its rows' hashes and
/// appends the rows of the prefixes that begin with `sido` to `table` (its columns `columns`, all
/// text), each cell as [`compared_cell`] reads it.
///
/// # Errors
/// Returns an error when `DuckDB` cannot run a read or append a row.
pub(super) fn pass(
    connection: &Connection,
    side: &dyn Fn(&str) -> String,
    column: &str,
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
                side(&prefix_filter(column, prefix)?)
            ))?
            .query_map([], |row| {
                (0..columns.len())
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<_, _>>()
            .with_context(|| format!("DuckDB failed to read the prefix {prefix} of {table}"))?;
        let in_sido = prefix.starts_with(sido);
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
        for bad in ["", "4'", "ab"] {
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
                 ('9999920100100010000', 2, '[1]'), ('4199910100100010000', 3, NULL);"#,
        )?;
        let side = |filter: &str| format!("SELECT pnu, n, d_json FROM t WHERE {filter}");
        let columns = ["pnu", "n", "d_json"];
        let found = prefixes(&connection, &["t"], "pnu")?;
        assert_eq!(found, ["41999", "99999"]);
        let pass = pass(
            &connection,
            &side,
            "pnu",
            &found,
            &columns,
            "99",
            "side_reference",
        )?;
        assert_eq!(pass.sido_rows, 2);
        let written: Vec<(String, String)> = connection
            .prepare("SELECT n, d_json FROM side_reference ORDER BY pnu")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        assert_eq!(
            written,
            [
                ("1".to_owned(), r#"{"x":10000000}"#.to_owned()),
                ("2".to_owned(), "[1]".to_owned())
            ]
        );
        // Every row of the side is summed, the 시도's and the others'.
        let cells = |pnu: &str, n: &str, document: Option<&str>| {
            [
                Some(pnu.to_owned()),
                Some(n.to_owned()),
                compared_cell("d_json", document.map(str::to_owned)),
            ]
        };
        let expected = [
            cells("4199910100100010000", "3", None),
            cells("9999910100100010000", "1", Some(r#"{"x":10000000.0}"#)),
            cells("9999920100100010000", "2", Some("[1]")),
        ]
        .iter()
        .fold(0_u128, |sum, row| {
            sum.wrapping_add(u128::from(row_hash(row)))
        });
        assert_eq!(pass.hash_sum, expected);
        Ok(())
    }
}
