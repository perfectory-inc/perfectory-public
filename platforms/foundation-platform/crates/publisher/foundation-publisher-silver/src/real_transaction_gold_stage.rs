//! The 실거래 Gold's transforms in `DuckDB` inside the publisher (root ADR-0559, ADR-0564 §9).
//!
//! They were the Spark job `real_transaction_silver_to_gold.py`. Each part moves on an
//! engine-equality verdict against the rows the job wrote (ADR-0559 §3); the SQL here is that
//! job's rules, written so the rows and their `row_digest` are byte for byte the job's:
//!
//! - `region_stats.sql`: the region statistics (`region_rows`, `region_gold_rows`). Judged
//!   nationally on 2026-11-14 against the served `gold.real_transaction_region_stats` at the pins
//!   its commit records: 3,204,170 rows, none only on one side, every column and every
//!   `row_digest` equal; 36.7 s and 4.70 GiB in `DuckDB` (4 threads) from reading the Silver to the
//!   comparison.
//!
//! The caller loads `deals` (the Silver's current rows at the pin, with their current 법정동) and
//! appends what the stage writes; nothing here reads the lakehouse or commits.

use anyhow::ensure;
use chrono::{Datelike, NaiveDate};

/// The months the trailing period covers (`REGION_MONTHS` of the Spark job).
pub const REGION_MONTHS: u32 = 12;

const REGION_STATS_SQL: &str = include_str!("real_transaction_gold_stage/region_stats.sql");

/// Spark's `percentile(column, p)` (`Percentile.getPercentile`: linear between the two ranks
/// around `(n - 1) * p`) over an ascending list, then Spark's `round(_, 1)` of a double (HALF_UP
/// of its decimal text). NULL for no values.
const SPARK_PERCENTILE: &str = "CREATE OR REPLACE MACRO spark_percentile(l, p) AS \
    CASE WHEN l IS NULL OR len(l) = 0 THEN NULL ELSE \
    CAST(round(CAST(CAST((CASE \
        WHEN ceil((len(l) - 1) * p) = floor((len(l) - 1) * p) \
          OR l[CAST(ceil((len(l) - 1) * p) AS BIGINT) + 1] = l[CAST(floor((len(l) - 1) * p) AS BIGINT) + 1] \
        THEN l[CAST(floor((len(l) - 1) * p) AS BIGINT) + 1] \
        ELSE (ceil((len(l) - 1) * p) - (len(l) - 1) * p) * l[CAST(floor((len(l) - 1) * p) AS BIGINT) + 1] \
           + ((len(l) - 1) * p - floor((len(l) - 1) * p)) * l[CAST(ceil((len(l) - 1) * p) AS BIGINT) + 1] \
        END) AS VARCHAR) AS DECIMAL(38, 18)), 1) AS DOUBLE) END";

/// How Spark's `to_json` writes a non-negative double of one decimal (Java's `Double.toString`):
/// plain below 10⁷, else one digit, the rest without trailing zeros, and `E` with the exponent.
const SPARK_DOUBLE_TEXT: &str = "CREATE OR REPLACE MACRO spark_double_text(v) AS \
    CASE WHEN v IS NULL THEN NULL WHEN v < 1e7 THEN CAST(v AS VARCHAR) ELSE \
    substr(CAST(CAST(round(v * 10) AS HUGEINT) AS VARCHAR), 1, 1) || '.' \
    || coalesce(nullif(rtrim(substr(CAST(CAST(round(v * 10) AS HUGEINT) AS VARCHAR), 2), '0'), ''), '0') \
    || 'E' || CAST(length(CAST(CAST(round(v * 10) AS HUGEINT) AS VARCHAR)) - 2 AS VARCHAR) END";

/// The build month's first day `months` months after `month` (negative: before).
fn add_months(month: NaiveDate, months: i32) -> anyhow::Result<NaiveDate> {
    let index = month.year() * 12 + i32::try_from(month.month0())? + months;
    NaiveDate::from_ymd_opt(
        index.div_euclid(12),
        u32::try_from(index.rem_euclid(12))? + 1,
        1,
    )
    .ok_or_else(|| anyhow::anyhow!("no month {index} months from year 0"))
}

/// Defines the macros the stage's SQL calls on `connection`.
///
/// # Errors
/// Returns `DuckDB`'s error.
pub fn define_macros(connection: &duckdb::Connection) -> anyhow::Result<()> {
    connection.execute_batch(&format!("{SPARK_PERCENTILE}; {SPARK_DOUBLE_TEXT};"))?;
    Ok(())
}

/// The query of the region statistics of the `deals` table, for the build month of `as_of`: the
/// rows of `gold.real_transaction_region_stats` but its two lineage columns, which the caller adds.
///
/// # Errors
/// Refuses a date whose month cannot be stepped back (year 0).
pub fn region_stats_query(as_of: NaiveDate) -> anyhow::Result<String> {
    let as_of = as_of
        .with_day(1)
        .ok_or_else(|| anyhow::anyhow!("no first day of {as_of}"))?;
    let trailing_from = add_months(as_of, -i32::try_from(REGION_MONTHS)?)?;
    let query = REGION_STATS_SQL
        .replace("{as_of}", &as_of.format("%Y-%m-%d").to_string())
        .replace(
            "{trailing_from}",
            &trailing_from.format("%Y-%m-%d").to_string(),
        )
        .replace("{as_of_month}", &as_of.format("%Y-%m").to_string());
    ensure!(
        !query.contains("{as_of") && !query.contains("{trailing_from}"),
        "region_stats.sql names a value the stage does not fill"
    );
    Ok(query)
}

#[cfg(test)]
mod tests;
