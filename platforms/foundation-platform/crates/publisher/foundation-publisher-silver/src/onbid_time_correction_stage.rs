// one-off: until no current row of the four Onbid tables with minute times holds a time the Spark
// load misread (the load's summary reports `keys_selected` 0 for each); deleted in the PR after.
//
//! Re-versions the Onbid rows whose times the Spark load misread (2026-11-20).
//!
//! The provider writes these times as twelve digits (`yyyyMMddHHmm`). The Spark load read them with
//! `strptime` and the seconds format first, so 09:30 was stored as 09:03:00: every such value is
//! `HH:0M:0S` in the table, and wrong unless its minute was 00. The fixed reader (#1538) re-versions
//! a row the provider still answers; a row it no longer answers stays wrong. This selects, per
//! table, the current rows that hold the pattern, reads the Bronze object each came from again, and
//! makes one correction version per row whose times differ:
//!
//! - the current row, with only its time columns replaced by the values read again (a time the
//!   row holds as null stays null);
//! - `source_snapshot_id` is the corrected version's with [`SUFFIX`] appended. The tables rank a
//!   key's versions by `source_snapshot_id` alone, so the correction sorts right after the version
//!   it corrects and before every later pass: the wrong row stays as history, a later pass still
//!   wins, and an object not loaded yet is never outranked by a correction.
//!
//! A row already corrected holds no pattern, so a second run selects nothing.

use anyhow::Context;
use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseTableContract;

use crate::duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG};

/// What a correction version's `source_snapshot_id` ends with.
pub const SUFFIX: &str = "+c1";

fn key(contract: &LakehouseTableContract) -> anyhow::Result<&'static [&'static str]> {
    Ok(contract
        .current_row_latest()
        .with_context(|| format!("{} keeps no versions per key", contract.table_name))?
        .key)
}

fn same_key(contract: &LakehouseTableContract, a: &str, b: &str) -> anyhow::Result<String> {
    Ok(key(contract)?
        .iter()
        .map(|column| format!("{a}.{column} IS NOT DISTINCT FROM {b}.{column}"))
        .collect::<Vec<_>>()
        .join(" AND "))
}

/// Whether `time` is stored `HH:0M:0S` with minute and second not both zero.
///
/// From the instant itself (seconds into the hour): the stage has no time zone extension, and
/// Korean time is whole hours from UTC.
fn misread(time: &str) -> String {
    let within = format!("(epoch_us({time}) // 1000000 % 3600)");
    format!("({within} // 60 < 10 AND {within} % 60 < 10 AND {within} <> 0)")
}

fn count(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<u64> {
    let value: i64 =
        stage
            .connection()
            .query_row(&format!("SELECT count(*) FROM {query}"), [], |row| {
                row.get(0)
            })?;
    Ok(u64::try_from(value)?)
}

/// Selects the current rows of the table at `head` that hold the misread pattern.
///
/// `fix_current` holds every current row, `fix_candidates` those with one of `times` (the columns
/// the provider writes as twelve digits) stored `HH:0M:0S`, minute and second not both zero, and `fix_reread` (empty) the times read again. Returns the
/// Bronze objects the candidates came from, in order.
///
/// # Errors
/// The table keeps no versions per key, or `DuckDB` fails the read.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn candidates(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    times: &[&'static str],
    head: i64,
) -> anyhow::Result<Vec<String>> {
    let connection = stage.connection();
    let columns = contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>()
        .join(", ");
    let pinned = format!(
        "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
        contract.table_name
    );
    let current = contract
        .current_rows_sql_template()
        .with_context(|| format!("{} keeps no versions per key", contract.table_name))?
        .replace("{columns}", &columns)
        .replace(
            "{source}",
            &with_contract_columns(connection, contract, &pinned)?,
        )
        .replace("{filter}", "TRUE");
    let pattern = times
        .iter()
        .map(|time| misread(time))
        .collect::<Vec<_>>()
        .join(" OR ");
    let reread = key(contract)?
        .iter()
        .chain(times)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE fix_current AS {current};
             CREATE OR REPLACE TEMP TABLE fix_candidates AS SELECT * FROM fix_current WHERE {pattern};
             CREATE OR REPLACE TEMP TABLE fix_reread AS SELECT {reread}, source_record_id FROM fix_current WHERE false;"
        ))
        .with_context(|| format!("DuckDB failed to read the current rows of {}", contract.table_name))?;
    let mut objects =
        connection.prepare("SELECT DISTINCT source_record_id FROM fix_candidates ORDER BY 1")?;
    let found = objects
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(found)
}

/// How many candidates came from `object_key`.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn candidates_of(stage: &DuckDbParquetStage, object_key: &str) -> anyhow::Result<u64> {
    count(
        stage,
        &format!(
            "fix_candidates WHERE source_record_id = '{}'",
            object_key.replace('\'', "''")
        ),
    )
}

/// Keeps, of the object just read again (`onbid_rows`), the times of the candidates it holds.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn keep_reread(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    times: &[&'static str],
    object_key: &str,
) -> anyhow::Result<()> {
    let object = object_key.replace('\'', "''");
    let columns = key(contract)?
        .iter()
        .chain(times)
        .map(|column| format!("r.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    stage
        .connection()
        .execute_batch(&format!(
            "INSERT INTO fix_reread SELECT {columns}, '{object}' FROM onbid_rows r \
             WHERE EXISTS (SELECT 1 FROM fix_candidates k WHERE k.source_record_id = '{object}' AND {})",
            same_key(contract, "r", "k")?
        ))
        .with_context(|| format!("DuckDB failed to keep the times of {object_key}"))
}

/// A time both the table and the object hold. A time the table holds as null is left null: the
/// rows loaded before a column was mapped (the 개찰 times, until 2026-10-29) never held it, and
/// filling it is not this correction.
fn read_both(time: &str) -> String {
    format!("c.{time} IS NOT NULL AND r.{time} IS NOT NULL")
}

/// What the correction of one table found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Corrections {
    /// Candidates found again in the object they came from.
    pub found_again: u64,
    /// Of them, rows whose times differ: the correction versions in `fix_rows`.
    pub would_change: u64,
}

/// `fix_rows`: one correction version per candidate whose times, read again, differ.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn corrections(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    times: &[&'static str],
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<Corrections> {
    let replaced = times
        .iter()
        .map(|time| {
            format!(
                "CASE WHEN {} THEN r.{time} ELSE c.{time} END AS {time}",
                read_both(time)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let differs = times
        .iter()
        .map(|time| format!("({} AND c.{time} <> r.{time})", read_both(time)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let joined = format!(
        "fix_candidates c JOIN fix_reread r ON r.source_record_id = c.source_record_id AND {}",
        same_key(contract, "c", "r")?
    );
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE fix_rows AS \
             SELECT c.* REPLACE ({replaced}, c.source_snapshot_id || '{SUFFIX}' AS source_snapshot_id, \
                                 to_timestamp({}) AS ingested_at_utc) \
             FROM {joined} WHERE {differs}",
            ingested_at.timestamp()
        ))
        .with_context(|| format!("DuckDB failed to correct {}", contract.table_name))?;
    Ok(Corrections {
        found_again: count(stage, &format!("({joined})"))?,
        would_change: count(stage, "fix_rows")?,
    })
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;
    use chrono::TimeZone as _;

    use super::{corrections, keep_reread, misread, Corrections};
    use crate::duckdb_parquet_stage::DuckDbParquetStage;

    #[test]
    fn a_misread_minute_gets_a_correction_version_right_after_the_one_it_corrects(
    ) -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let stage = DuckDbParquetStage::open(directory.path())?;
        let contract = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
            "silver.onbid_announcement_bid_result",
        )
        .context("the announcement bid results have a contract")?;
        let times = ["cltr_opbd_dt"];
        // Two candidates of one object: 09:30 stored as 09:03:00, and a time that was read right
        // (a minute of 05 is 00:05, the same either way only when the reader agrees).
        stage.connection().execute_batch(
            "CREATE TABLE fix_candidates AS SELECT * FROM (VALUES
               ('A1', 1, TIMESTAMPTZ '2026-11-05 00:03:00+00', 'bronze/o1.gz', '20261105T000000Z', TIMESTAMPTZ '2026-11-05 01:00:00+00'),
               ('A2', 1, TIMESTAMPTZ '2026-11-05 00:05:00+00', 'bronze/o1.gz', '20261105T000000Z', TIMESTAMPTZ '2026-11-05 01:00:00+00'))
               t(onbid_pbanc_no, pbct_no, cltr_opbd_dt, source_record_id, source_snapshot_id, ingested_at_utc);
             CREATE TABLE fix_reread AS SELECT onbid_pbanc_no, pbct_no, cltr_opbd_dt, source_record_id FROM fix_candidates WHERE false;
             CREATE TABLE onbid_rows AS SELECT * FROM (VALUES
               ('A1', 1, TIMESTAMPTZ '2026-11-05 00:30:00+00'),
               ('A2', 1, TIMESTAMPTZ '2026-11-05 00:05:00+00'),
               ('A3', 1, TIMESTAMPTZ '2026-11-05 00:45:00+00')) t(onbid_pbanc_no, pbct_no, cltr_opbd_dt);",
        )?;
        // 00:03:00 and 00:05:09 could be misread; 00:30:00 and 00:00:00 could not.
        let held: Vec<bool> = stage
            .connection()
            .prepare(&format!(
                "SELECT {} FROM (VALUES (to_timestamp(180)), (to_timestamp(309)), (to_timestamp(1800)), (to_timestamp(7200))) t(seen)",
                misread("seen")
            ))?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        assert_eq!(held, [true, true, false, false]);
        keep_reread(&stage, contract, &times, "bronze/o1.gz")?;
        let at = chrono::Utc
            .with_ymd_and_hms(2026, 11, 20, 3, 0, 0)
            .single()
            .context("a valid instant")?;
        assert_eq!(
            corrections(&stage, contract, &times, at)?,
            Corrections {
                found_again: 2,
                would_change: 1
            }
        );
        let row: (String, String, String, String) = stage.connection().query_row(
            "SELECT onbid_pbanc_no, source_snapshot_id, source_record_id, \
                    CAST(epoch_us(cltr_opbd_dt) // 1000000 % 3600 AS VARCHAR) FROM fix_rows",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(
            row,
            (
                "A1".to_owned(),
                "20261105T000000Z+c1".to_owned(),
                "bronze/o1.gz".to_owned(),
                // Seconds into the hour: 00:30:00.
                "1800".to_owned()
            )
        );
        // After the version it corrects, before any later pass.
        assert!("20261105T000000Z" < "20261105T000000Z+c1");
        assert!("20261105T000000Z+c1" < "20261105T010000Z");
        Ok(())
    }
}
