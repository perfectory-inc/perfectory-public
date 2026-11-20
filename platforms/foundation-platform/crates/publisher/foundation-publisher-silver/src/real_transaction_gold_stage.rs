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
//! - `document.sql`: the by-PNU kind's rows (`gold_rows`, `with_place_ids`, `history_coverage`),
//!   from the deals `real_transaction_placement_stage::place` placed.
//!
//! The caller loads `deals` (the Silver's current rows at the pin, with their current 법정동) and
//! appends what the stage writes; nothing here reads the lakehouse or commits.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use chrono::{Datelike, NaiveDate};

/// The months the trailing period covers (`REGION_MONTHS` of the Spark job).
pub const REGION_MONTHS: u32 = 12;
/// The months before the build month a parcel's comparables reach back (`COMPARABLE_MONTHS`).
pub const COMPARABLE_MONTHS: u32 = 36;
/// The comparables a document lists (`COMPARABLE_COUNT`).
pub const COMPARABLE_COUNT: u32 = 20;
/// A document's bound (`DOCUMENT_BYTE_LIMIT`) less what its envelope is kept
/// (`DOCUMENT_ENVELOPE_BYTES`), measured as Spark's `length` does, in characters.
const DOCUMENT_BUDGET: u32 = 256 * 1024 - 1024;
/// A deal as the document carries it (`TRANSACTION_FIELDS`): the provider's columns as the Silver
/// names them, and how it was placed.
const TRANSACTION_FIELDS: [&str; 24] = [
    "dataset_slug",
    "deal_date",
    "deal_amount_10k_krw",
    "deposit_10k_krw",
    "monthly_rent_10k_krw",
    "rent_kind",
    "building_type",
    "building_use",
    "building_name",
    "building_area_m2",
    "building_area_kind",
    "land_area_m2",
    "land_area_kind",
    "floor",
    "build_year",
    "land_use",
    "jimok",
    "jibun_raw",
    "share_dealing_type",
    "dealing_gbn",
    "cancelled",
    "cancel_date",
    "place_identity_grade",
    "attached_via",
];
/// A comparable deal (`COMPARABLE_FIELDS`).
const COMPARABLE_FIELDS: [&str; 12] = [
    "deal_date",
    "deal_amount_10k_krw",
    "land_area_m2",
    "building_area_m2",
    "land_use",
    "building_use",
    "build_year",
    "jibun_raw",
    "pnu",
    "place_identity_grade",
    "main_lot_group_key",
    "main_lot_group_size",
];

const REGION_STATS_SQL: &str = include_str!("real_transaction_gold_stage/region_stats.sql");
const DOCUMENT_SQL: &str = include_str!("real_transaction_gold_stage/document.sql");
/// The values `region_stats.sql` takes from its caller.
const AS_OF: &str = "{as_of}";
const TRAILING_FROM: &str = "{trailing_from}";
const AS_OF_MONTH: &str = "{as_of_month}";

/// Spark's `percentile(column, p)` then `round(_, 1)`, over an ascending list.
///
/// `Percentile.getPercentile` is linear between the two ranks around `(n - 1) * p`; the rounding
/// is `HALF_UP` of the double's decimal text. NULL for no values.
const SPARK_PERCENTILE: &str = "CREATE OR REPLACE MACRO spark_percentile(l, p) AS \
    CASE WHEN l IS NULL OR len(l) = 0 THEN NULL ELSE \
    CAST(round(CAST(CAST((CASE \
        WHEN ceil((len(l) - 1) * p) = floor((len(l) - 1) * p) \
          OR l[CAST(ceil((len(l) - 1) * p) AS BIGINT) + 1] = l[CAST(floor((len(l) - 1) * p) AS BIGINT) + 1] \
        THEN l[CAST(floor((len(l) - 1) * p) AS BIGINT) + 1] \
        ELSE (ceil((len(l) - 1) * p) - (len(l) - 1) * p) * l[CAST(floor((len(l) - 1) * p) AS BIGINT) + 1] \
           + ((len(l) - 1) * p - floor((len(l) - 1) * p)) * l[CAST(ceil((len(l) - 1) * p) AS BIGINT) + 1] \
        END) AS VARCHAR) AS DECIMAL(38, 18)), 1) AS DOUBLE) END";

/// How Spark's `to_json` writes a double (Java's `Double.toString`), from `DuckDB`'s shortest text:
/// plain from 10⁻³ to below 10⁷, else one digit, the rest without trailing zeros, `E` and the
/// exponent; `NaN` and the infinities as Jackson quotes them. `DuckDB`'s `to_json` writes a JSON
/// value's numbers again its own way, so the documents are text built field by field.
const SPARK_DOUBLE: &str = "CREATE OR REPLACE MACRO spark_double_digits(s) AS \
    CASE WHEN contains(s, 'e') THEN \
      CASE WHEN contains(split_part(s, 'e', 1), '.') THEN split_part(s, 'e', 1) \
           ELSE split_part(s, 'e', 1) || '.0' END \
      || 'E' || CAST(CAST(split_part(s, 'e', 2) AS INTEGER) AS VARCHAR) \
    WHEN CAST(s AS DOUBLE) >= 1e7 THEN \
      substr(split_part(s, '.', 1), 1, 1) || '.' \
      || coalesce(nullif(rtrim(substr(split_part(s, '.', 1), 2) || split_part(s, '.', 2), '0'), ''), '0') \
      || 'E' || CAST(length(split_part(s, '.', 1)) - 1 AS VARCHAR) \
    WHEN CAST(s AS DOUBLE) > 0 AND CAST(s AS DOUBLE) < 1e-3 THEN \
      substr(ltrim(split_part(s, '.', 2), '0'), 1, 1) || '.' \
      || coalesce(nullif(substr(ltrim(split_part(s, '.', 2), '0'), 2), ''), '0') \
      || 'E-' || CAST(length(split_part(s, '.', 2)) - length(ltrim(split_part(s, '.', 2), '0')) + 1 AS VARCHAR) \
    ELSE s END; \
  CREATE OR REPLACE MACRO spark_double_text(v) AS \
    CASE WHEN v IS NULL THEN NULL \
      WHEN isnan(v) THEN '\"NaN\"' WHEN isinf(v) AND v > 0 THEN '\"Infinity\"' \
      WHEN isinf(v) THEN '\"-Infinity\"' \
      ELSE (CASE WHEN v < 0 OR (v = 0 AND signbit(v)) THEN '-' ELSE '' END) \
           || spark_double_digits(CAST(abs(v) AS VARCHAR)) END";

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
    connection.execute_batch(&format!("{SPARK_PERCENTILE}; {SPARK_DOUBLE};"))?;
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
        .with_context(|| format!("no first day of {as_of}"))?;
    let trailing_from = add_months(as_of, -i32::try_from(REGION_MONTHS)?)?;
    let query = REGION_STATS_SQL
        .replace(AS_OF_MONTH, &as_of.format("%Y-%m").to_string())
        .replace(AS_OF, &as_of.format("%Y-%m-%d").to_string())
        .replace(TRAILING_FROM, &trailing_from.format("%Y-%m-%d").to_string());
    ensure!(
        [AS_OF, TRAILING_FROM, AS_OF_MONTH]
            .iter()
            .all(|value| !query.contains(value)),
        "region_stats.sql names a value the stage does not fill"
    );
    Ok(query)
}

/// One dataset whose history the build's Silver does not hold whole, and the month the screen says
/// it holds deals from (`YYYY-MM`; None when it holds no month).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coverage {
    /// The dataset (`dataset_slug`).
    pub dataset_slug: String,
    /// The first month the Silver holds deals of it from, `YYYY-MM`.
    pub from_month: Option<String>,
}

/// A month's index (`year * 12 + month0`) from its year and 1-based month texts.
fn month_index(year: &str, month: &str) -> anyhow::Result<i32> {
    let year: i32 = year
        .parse()
        .with_context(|| format!("no year in {year:?}"))?;
    let month: i32 = month
        .parse()
        .with_context(|| format!("no month in {month:?}"))?;
    ensure!((1..=12).contains(&month), "no month {month}");
    Ok(year * 12 + month - 1)
}

/// `history_coverage` of the Spark job (root ADR-0479), from the (dataset, 계약 달 `YYYYMM`) pairs of
/// the whole pinned Silver, never of the 법정동s a partial run restricts its inputs to.
///
/// A dataset is whole when the gapless run of months ending at its newest reaches its contract's
/// `first_month` (`YYYY-MM`, `first_months`); it is then left out. Otherwise the claim is the start
/// of the recent history the collector fills first (`recent_months` before the build month) once the
/// run reaches it, else the run's own start.
///
/// # Errors
/// Refuses a month that is not `YYYYMM` or a `first_month` that is not `YYYY-MM`.
pub fn history_coverage<'a>(
    months: impl IntoIterator<Item = (&'a str, &'a str)>,
    first_months: &BTreeMap<String, String>,
    as_of: NaiveDate,
    recent_months: u32,
) -> anyhow::Result<Vec<Coverage>> {
    let mut held: BTreeMap<&str, BTreeSet<i32>> = BTreeMap::new();
    for (dataset, month) in months {
        let (year, rest) = month.split_at_checked(4).context("no YYYYMM")?;
        held.entry(dataset)
            .or_default()
            .insert(month_index(year, rest.get(..2).context("no YYYYMM")?)?);
    }
    let first_day = as_of.with_day(1).context("no first day")?;
    let recent = add_months(first_day, -i32::try_from(recent_months)?)?;
    let recent_index = recent.year() * 12 + i32::try_from(recent.month0())?;
    let mut coverage = Vec::new();
    for (dataset, first) in first_months {
        let (year, month) = first
            .split_once('-')
            .with_context(|| format!("{dataset}: no YYYY-MM in {first}"))?;
        let Some(mut oldest) = held.get(dataset.as_str()).and_then(|m| m.last().copied()) else {
            coverage.push(Coverage {
                dataset_slug: dataset.clone(),
                from_month: None,
            });
            continue;
        };
        let months = &held[dataset.as_str()];
        while months.contains(&(oldest - 1)) {
            oldest -= 1;
        }
        if oldest <= month_index(year, month)? {
            continue;
        }
        let claim = oldest.max(recent_index);
        coverage.push(Coverage {
            dataset_slug: dataset.clone(),
            from_month: Some(format!(
                "{:04}-{:02}",
                claim.div_euclid(12),
                claim.rem_euclid(12) + 1
            )),
        });
    }
    Ok(coverage)
}

/// The coverage as Spark's `to_json` of `array<struct<dataset_slug,from_month>>` writes it.
fn coverage_json(coverage: &[Coverage]) -> anyhow::Result<String> {
    let entries = coverage
        .iter()
        .map(|c| {
            Ok(format!(
                "{{\"dataset_slug\":{},\"from_month\":{}}}",
                serde_json::to_string(&c.dataset_slug)?,
                serde_json::to_string(&c.from_month)?
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("[{}]", entries.join(",")))
}

/// The JSON object Spark's `to_json(struct(fields), ignoreNullFields=false)` writes, as a `DuckDB`
/// text expression over `placed`'s columns (`types`: name → `DuckDB` type), in order: every key, a
/// missing value null, a date as its ISO text, a double as Java writes it, and a comparable's `pnu`
/// none for a 본번 group deal (root ADR-0405).
fn entry_sql(types: &BTreeMap<String, String>, fields: &[&str]) -> anyhow::Result<String> {
    let parts = fields
        .iter()
        .map(|field| {
            let kind = types
                .get(*field)
                .with_context(|| format!("placed has no column {field}"))?;
            let value = match kind.as_str() {
                "VARCHAR" if *field == "pnu" => {
                    "to_json(CASE WHEN main_lot_group_key IS NULL THEN pnu END)".to_owned()
                }
                "VARCHAR" => format!("to_json({field})"),
                "DATE" => format!("'\"' || CAST({field} AS VARCHAR) || '\"'"),
                "DOUBLE" => format!("spark_double_text({field})"),
                "BIGINT" | "INTEGER" | "SMALLINT" | "TINYINT" | "HUGEINT" | "BOOLEAN" => {
                    format!("CAST({field} AS VARCHAR)")
                }
                other => anyhow::bail!("{field} is a {other}, which the document does not write"),
            };
            Ok(format!(
                "'\"{field}\":' || coalesce(CAST({value} AS VARCHAR), 'null')"
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("('{{' || {} || '}}')", parts.join(" || ',' || ")))
}

/// Builds `gold_rows` for the build month of `as_of` and returns its row count.
///
/// The rows are `pnu`, `real_transaction_json`, `transaction_total`, `attached_via`, `place_id`,
/// `main_lot_group_place_ids` and `row_digest`, from the tables
/// `real_transaction_placement_stage::place` leaves (`placed`, `deals`, `parcels`, `land`) and
/// `place_ids(pnu, place_id)`; the caller adds the two lineage columns.
///
/// # Errors
/// Returns `DuckDB`'s error, or refuses a `placed` without a field the document carries.
pub fn gold_rows(
    connection: &duckdb::Connection,
    as_of: NaiveDate,
    coverage: &[Coverage],
) -> anyhow::Result<u64> {
    let as_of = as_of
        .with_day(1)
        .with_context(|| format!("no first day of {as_of}"))?;
    define_macros(connection)?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE region AS {}",
        region_stats_query(as_of)?
    ))?;
    let types = connection
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = 'placed'",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<BTreeMap<String, String>, _>>()?;
    let date = |months: u32, forward: bool| -> anyhow::Result<String> {
        let months = i32::try_from(months)?;
        let month = add_months(as_of, if forward { months } else { -months })?;
        Ok(month.format("%Y-%m-%d").to_string())
    };
    let values = [
        ("{transaction}", entry_sql(&types, &TRANSACTION_FIELDS)?),
        ("{comparable}", entry_sql(&types, &COMPARABLE_FIELDS)?),
        ("{build_month}", date(0, true)?),
        ("{comparables_from}", date(COMPARABLE_MONTHS, false)?),
        ("{next_month}", date(1, true)?),
        ("{region_from}", date(REGION_MONTHS, false)?),
        ("{comparable_count}", COMPARABLE_COUNT.to_string()),
        ("{budget}", DOCUMENT_BUDGET.to_string()),
        (
            "{coverage_json}",
            coverage_json(coverage)?.replace('\'', "''"),
        ),
    ];
    let mut sql = DOCUMENT_SQL.to_owned();
    for (name, value) in &values {
        ensure!(sql.contains(name), "document.sql takes no {name}");
        sql = sql.replace(name, value);
    }
    connection
        .execute_batch(&sql)
        .context("DuckDB failed to build the 실거래 Gold rows")?;
    let rows: i64 = connection.query_row("SELECT count(*) FROM gold_rows", [], |row| row.get(0))?;
    Ok(u64::try_from(rows)?)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod document_tests;
