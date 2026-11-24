//! The judged 시도 of a side, read as values (`foundation_publisher_core::json_values`, the reading
//! the switch's content check holds documents to), a 시군구 at a time.
//!
//! `DuckDB` hands the publisher a query's result whole, so the 시도 is read in the ranges of its key
//! column's first [`CHUNK_PREFIX`] characters (a 시군구's parcels, a few hundred thousand rows; both
//! tables are ordered by that column, so a range skips the other files). Each row's `*_json` cells
//! are read as the values they say, and the 시군구's rows are written as one Parquet file. Only the
//! judged 시도 comes through here: the whole of a side is aggregated inside `DuckDB` and no row of it
//! is brought out (root ADR-0564 Revision 2026-11-24).

use std::path::Path;

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

/// `columns` each as its text, as a list of values: what a row's hash is taken of.
pub(super) fn as_text_values(columns: &[&str]) -> String {
    columns
        .iter()
        .map(|column| format!("CAST({column} AS VARCHAR)"))
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

/// Writes `side(filter)` (`columns`, each as text, each cell as [`compared_cell`] reads it) as
/// `directory/<prefix>.parquet`, one file and one progress line for each of `prefixes`, and returns
/// the rows written. A prefix the side has no row in still gets its (empty) file, so both sides
/// hold a file for every chunk the comparison names.
///
/// # Errors
/// Returns an error when `DuckDB` cannot run a read, append a row or write a file.
pub(super) fn write_chunks(
    connection: &Connection,
    side: &dyn Fn(&str) -> String,
    column: &str,
    prefixes: &[String],
    columns: &[&str],
    name: &str,
    directory: &Path,
) -> anyhow::Result<u64> {
    std::fs::create_dir_all(directory)
        .with_context(|| format!("cannot create {}", directory.display()))?;
    let table = format!("side_{name}");
    let declared = columns
        .iter()
        .map(|column| format!("{column} VARCHAR"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut written = 0_u64;
    for prefix in prefixes {
        let started = std::time::Instant::now();
        connection.execute_batch(&format!("CREATE OR REPLACE TABLE {table} ({declared});"))?;
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
            .with_context(|| format!("DuckDB failed to read the chunk {prefix} of {name}"))?;
        let count = rows.len();
        {
            let mut appender = connection.appender(&table)?;
            for row in rows {
                let cells: Vec<Option<String>> = row
                    .into_iter()
                    .zip(columns)
                    .map(|(cell, column)| compared_cell(column, cell))
                    .collect();
                appender.append_row(appender_params_from_iter(cells.iter()))?;
            }
            appender.flush()?;
        }
        let target = super::sql_path(&directory.join(format!("{prefix}.parquet")))?;
        connection
            .execute_batch(&format!(
                "COPY {table} TO '{target}' (FORMAT parquet, COMPRESSION zstd);"
            ))
            .with_context(|| format!("DuckDB failed to write the chunk {prefix} of {name}"))?;
        written += u64::try_from(count)?;
        println!(
            "gold-takeover-sides chunk {name} {prefix}: {count} rows in {:.1}s",
            started.elapsed().as_secs_f64()
        );
    }
    connection.execute_batch(&format!("DROP TABLE IF EXISTS {table};"))?;
    Ok(written)
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

    /// A document cell is the values it says; any other cell, and a document that is not JSON, is
    /// its text.
    #[test]
    fn a_document_cell_is_read_as_its_values() {
        assert_eq!(
            compared_cell(
                "a_json",
                Some(r#"{"area_m2":1.0E7,"note":null}"#.to_owned())
            ),
            compared_cell("a_json", Some(r#"{"area_m2":10000000.0}"#.to_owned()))
        );
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

    /// A side's 시도 is written a chunk a file, each cell as a value; a chunk the side has no row
    /// in is an empty file, and rows of no named chunk are not written.
    #[test]
    fn a_side_is_written_a_chunk_a_file() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            r#"CREATE TABLE t (pnu VARCHAR, n BIGINT, d_json VARCHAR);
               INSERT INTO t VALUES ('9999910100100010000', 1, '{"x":1.0E7}'),
                 ('9999920100100010000', 2, '[1]'), ('9999930100100010000', 3, NULL),
                 ('9999810100100010000', 4, '{}'), ('9900010100100010000', 5, '{}');"#,
        )?;
        let side = |filter: &str| format!("SELECT pnu, n, d_json FROM t WHERE {filter}");
        let columns = ["pnu", "n", "d_json"];
        assert_eq!(
            prefixes(
                &connection,
                &["(SELECT pnu FROM t WHERE pnu >= '9999')"],
                "pnu"
            )?,
            ["99998", "99999"]
        );
        let work = tempfile::tempdir()?;
        let chunks = ["99997".to_owned(), "99998".to_owned(), "99999".to_owned()];
        let written = write_chunks(
            &connection,
            &side,
            "pnu",
            &chunks,
            &columns,
            "reference",
            work.path(),
        )?;
        assert_eq!(written, 4);
        let file =
            |prefix: &str| super::super::sql_path(&work.path().join(format!("{prefix}.parquet")));
        let rows = |prefix: &str| -> anyhow::Result<Vec<(String, Option<String>)>> {
            Ok(connection
                .prepare(&format!(
                    "SELECT n, d_json FROM read_parquet('{}') ORDER BY pnu",
                    file(prefix)?
                ))?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?)
        };
        assert!(rows("99997")?.is_empty());
        assert_eq!(rows("99998")?, [("4".to_owned(), Some("{}".to_owned()))]);
        assert_eq!(
            rows("99999")?,
            [
                ("1".to_owned(), Some(r#"{"x":10000000}"#.to_owned())),
                ("2".to_owned(), Some("[1]".to_owned())),
                ("3".to_owned(), None)
            ]
        );
        Ok(())
    }
}
