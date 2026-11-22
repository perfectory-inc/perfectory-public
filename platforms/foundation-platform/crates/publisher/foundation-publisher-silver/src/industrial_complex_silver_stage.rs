//! Stages one export of the industrial-complex profile as `silver.industrial_complexes` rows (root
//! ADR-0033, ADR-0559).
//!
//! In `DuckDB` inside the publisher: it was the Spark job `industrial_complex_bronze_to_silver.py`.
//! `export-industrial-complex-bronze-raw-jsonl` writes one JSON line per complex of a profile
//! workbook, its address resolved from the ILIS sources; `lines.sql` and `rows.sql` read each line the way
//! the Spark frame did, the gates refuse what the Spark job refused (before anything is staged), and
//! `changed.sql` keeps the complexes whose content is new or differs from the table's current row
//! (root ADR-0311: only the changed keys are appended). The caller appends that file, or none, in
//! one commit that records every Bronze object the export was built from.
//!
//! `row_checksum_sha256` is the rule every Silver producer that writes the column follows: SHA-256 of
//! the row's other columns as one compact JSON object, keys in name order, a null as `null`, every
//! value as its text (a date `yyyy-MM-dd`, an instant `yyyy-MM-ddTHH:MM:SSZ`, a decimal at its scale).
//! The Spark job hashed Spark's `to_json` of the struct instead; a row it wrote keeps its checksum,
//! and the checksum is not compared (it is the load's, not the source's).

use std::path::Path;

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_domain::{
    industrial_complex_jsonl_transport::{
        bronze_industrial_complexes_raw_jsonl_columns,
        INDUSTRIAL_COMPLEX_DEVELOPMENT_STAGE_WIRE_VALUES, INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES,
        INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES, INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
    },
    LakehouseTableContract,
};

use crate::duckdb_parquet_stage::{
    with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG, PYTHON_VALUES_SQL,
};

/// The table this stage writes.
pub const TABLE: &str = "silver.industrial_complexes";

const LINES_SQL: &str = include_str!("industrial_complex_silver_stage/lines.sql");
const ROWS_SQL: &str = include_str!("industrial_complex_silver_stage/rows.sql");
const CHANGED_SQL: &str = include_str!("industrial_complex_silver_stage/changed.sql");

/// Columns the load writes rather than the source states: never compared.
const LOAD_COLUMNS: [&str; 6] = [
    "source_record_id",
    "source_snapshot_id",
    "valid_from_utc",
    "valid_to_utc",
    "ingested_at_utc",
    "row_checksum_sha256",
];

/// One export, staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedComplexes {
    /// Rows the export holds, one per complex.
    pub rows: u64,
    /// Of those, the complexes that are new or whose content changed: what is appended.
    pub changed: u64,
    /// The changed rows as one Parquet file; `None` when there are none.
    pub file: Option<StagedFile>,
}

fn sql_list(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", value.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The checksum expression over every contract column but the checksum, in name order.
///
/// # Errors
/// A column whose logical type has no text rule here, or a column name that is not plain.
pub fn checksum_sql(contract: &LakehouseTableContract) -> anyhow::Result<String> {
    let mut columns = contract
        .columns
        .iter()
        .filter(|column| column.name != "row_checksum_sha256")
        .collect::<Vec<_>>();
    columns.sort_by_key(|column| column.name);
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        ensure!(
            column
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "{} is not a plain column name",
            column.name
        );
        let name = column.name;
        let text = match column.logical_type {
            "string" => format!("\"{name}\""),
            "date" => format!("strftime(\"{name}\", '%Y-%m-%d')"),
            // An instant as UTC text: the cast to a plain timestamp is UTC without ICU.
            "timestamp" => format!("strftime(CAST(\"{name}\" AS TIMESTAMP), '%Y-%m-%dT%H:%M:%SZ')"),
            decimal if decimal.starts_with("decimal(") => format!("CAST(\"{name}\" AS VARCHAR)"),
            other => bail!("{name}: a {other} column has no checksum text"),
        };
        fields.push(format!("\"{name}\" := CAST({text} AS VARCHAR)"));
    }
    Ok(format!(
        "sha256(CAST(to_json(struct_pack({})) AS VARCHAR))",
        fields.join(", ")
    ))
}

/// The Spark job's refusals, each a predicate over `complex_rows` and its message.
fn gates(contract: &LakehouseTableContract) -> Vec<(String, String)> {
    let mut gates = Vec::new();
    for column in contract.columns.iter().filter(|column| column.required) {
        gates.push((
            format!("\"{}\" IS NULL", column.name),
            format!("{} must not be null", column.name),
        ));
    }
    // Every string column, not only the required ones: null is the honest "unknown" and `""` is a
    // producer that filled the column with nothing (root ADR-0035).
    for column in contract
        .columns
        .iter()
        .filter(|column| column.logical_type == "string")
    {
        gates.push((
            format!("length(\"{}\") = 0", column.name),
            format!("{} must not be empty", column.name),
        ));
    }
    let domain = |column: &str, values: &[&str], required: bool| {
        let off = format!("{column} NOT IN ({})", sql_list(values));
        if required {
            off
        } else {
            format!("({column} IS NOT NULL AND {off})")
        }
    };
    gates.extend([
        (
            domain("complex_kind", INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES, true),
            "complex_kind is outside the allowed domain".to_owned(),
        ),
        (
            domain("status", INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES, true),
            "status is outside the allowed domain".to_owned(),
        ),
        (
            "official_area_sqm IS NOT NULL AND official_area_sqm <= 0".to_owned(),
            "official_area_sqm must be positive when present".to_owned(),
        ),
        (
            "NOT regexp_full_match(complex_id, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}')"
                .to_owned(),
            "complex_id must be a lowercase UUID string".to_owned(),
        ),
        (
            "NOT regexp_full_match(row_checksum_sha256, '[0-9a-f]{64}')".to_owned(),
            "row_checksum_sha256 must be lowercase sha256 hex".to_owned(),
        ),
        (
            ["sido_code:2", "sigungu_code:5", "primary_bjdong_code:10"]
                .iter()
                .map(|entry| {
                    let (column, width) = entry.split_once(':').unwrap_or_default();
                    format!(
                        "({column} IS NOT NULL AND (NOT regexp_full_match({column}, '[0-9]{{{width}}}') OR regexp_full_match({column}, '0+')))"
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR "),
            "a region code that is present must be all digits of its own width and must not be all zeros; an unknown region is null".to_owned(),
        ),
        (
            domain(
                "lot_sales_status",
                INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES,
                false,
            ),
            "lot_sales_status is outside the allowed domain".to_owned(),
        ),
        (
            domain(
                "development_stage",
                INDUSTRIAL_COMPLEX_DEVELOPMENT_STAGE_WIRE_VALUES,
                false,
            ),
            "development_stage is outside the allowed domain".to_owned(),
        ),
        (
            "development_progress_percent IS NOT NULL AND (development_progress_percent < 0 OR development_progress_percent > 100)".to_owned(),
            "development_progress_percent must be between 0 and 100 when present".to_owned(),
        ),
        (
            "((business_period_start_month IS NULL) <> (business_period_end_month IS NULL)) \
             OR (business_period_start_month IS NOT NULL AND NOT regexp_full_match(business_period_start_month, '[0-9]{4}-(0[1-9]|1[0-2])')) \
             OR (business_period_end_month IS NOT NULL AND NOT regexp_full_match(business_period_end_month, '[0-9]{4}-(0[1-9]|1[0-2])'))"
                .to_owned(),
            "business_period_start_month and business_period_end_month must be present together and must both be yyyy-MM; a period with one boundary states one the source did not".to_owned(),
        ),
    ]);
    gates
}

/// Refuses the export when any gate holds for a row: the count and up to five of the rows.
fn refuse_invalid(
    connection: &Connection,
    contract: &LakehouseTableContract,
) -> anyhow::Result<()> {
    // The producer's transport columns (`jsonl_transports`): Spark's reader refused an input that
    // lacked one.
    for column in bronze_industrial_complexes_raw_jsonl_columns() {
        let missing: i64 = connection.query_row(
            &format!(
                "SELECT count(*) FROM complex_lines_json WHERE json_extract(j, '$.\"{column}\"') IS NULL"
            ),
            [],
            |row| row.get(0),
        )?;
        ensure!(
            missing == 0,
            "the export is missing the column {column} on {missing} line(s)"
        );
    }
    for (predicate, message) in gates(contract) {
        let (count, samples): (i64, Option<String>) = connection
            .query_row(
                &format!(
                    "SELECT count(*), CAST(to_json(list(to_json(r)) FILTER (WHERE r.n <= 5)) AS VARCHAR) \
                     FROM (SELECT *, row_number() OVER () AS n FROM complex_rows WHERE {predicate}) r"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| format!("DuckDB could not check: {message}"))?;
        ensure!(
            count == 0,
            "{message}. count={count} samples={}",
            samples.unwrap_or_default()
        );
    }
    let (repeated, samples): (i64, Option<String>) = connection.query_row(
        "SELECT count(*), CAST(to_json(list(official_complex_code || '@' || source_snapshot_id)) AS VARCHAR) \
         FROM (SELECT official_complex_code, source_snapshot_id FROM complex_rows \
               GROUP BY ALL HAVING count(*) > 1 LIMIT 5)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(
        repeated == 0,
        "(official_complex_code, source_snapshot_id) must be unique. samples={}",
        samples.unwrap_or_default()
    );
    Ok(())
}

/// Fills `complex_ids` with the id of every code the export names.
fn derive_ids(connection: &Connection) -> anyhow::Result<()> {
    let codes = connection
        .prepare(
            "SELECT DISTINCT trim(complex_ic_field(j, 'official_complex_code')) FROM complex_lines_json \
             WHERE complex_ic_field(j, 'official_complex_code') IS NOT NULL",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE complex_ids (official_complex_code VARCHAR, complex_id VARCHAR)",
    )?;
    let mut appender = connection.appender("complex_ids")?;
    for code in &codes {
        appender.append_row(duckdb::params![
            code,
            ComplexId::for_official_complex_code(code).to_string()
        ])?;
    }
    appender.flush()?;
    Ok(())
}

/// The current rows of the table at `head` (none when it has no snapshot) for the codes the export
/// names, projecting `columns`.
fn current_rows(
    connection: &Connection,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    columns: &[&str],
) -> anyhow::Result<String> {
    let Some(head) = head else {
        return Ok(format!(
            "SELECT {} FROM complex_rows WHERE false",
            columns.join(", ")
        ));
    };
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{TABLE} keeps no versions per key"))?;
    let pinned = format!("{LAKEHOUSE_CATALOG}.{TABLE} AT (VERSION => {head})");
    Ok(latest.current_rows_sql(
        &with_contract_columns(connection, contract, &pinned)?,
        columns,
        "official_complex_code IN (SELECT official_complex_code FROM complex_rows)",
    ))
}

/// Builds the rows of the export at `jsonl`, refuses it when a gate holds, and stages the
/// complexes that are new or changed against the table's current rows at `head` (the snapshot
/// the append will be pinned on). `name` names the staged file.
///
/// # Errors
/// A line that is not a JSON object, a missing column, a gate, and a `DuckDB` failure.
pub fn stage_export(
    stage: &DuckDbParquetStage,
    shape: &TableShape,
    contract: &LakehouseTableContract,
    jsonl: &Path,
    head: Option<i64>,
    name: &str,
) -> anyhow::Result<StagedComplexes> {
    ensure!(
        contract.table_name == TABLE,
        "{} is not {TABLE}",
        contract.table_name
    );
    stage.load_lines(jsonl, "complex_lines")?;
    let connection = stage.connection();
    connection
        .execute_batch(PYTHON_VALUES_SQL)
        .context("DuckDB refused the Python value readers")?;
    connection
        .execute_batch(LINES_SQL)
        .context("DuckDB could not read the export's lines")?;
    derive_ids(connection)?;
    connection
        .execute_batch(&ROWS_SQL.replace("{checksum}", &checksum_sql(contract)?))
        .context("DuckDB could not build the rows of the export")?;
    refuse_invalid(connection, contract)?;

    let compared = contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !LOAD_COLUMNS.contains(name))
        .collect::<Vec<_>>();
    let same_row = compared
        .iter()
        .map(|column| format!("c.{column} IS NOT DISTINCT FROM r.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    connection
        .execute_batch(
            &CHANGED_SQL
                .replace(
                    "{current}",
                    &current_rows(connection, contract, head, &compared)?,
                )
                .replace("{matches}", &same_row),
        )
        .context("DuckDB could not compare the export with the table's current rows")?;

    let count = |table: &str| -> anyhow::Result<u64> {
        let value: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        Ok(u64::try_from(value)?)
    };
    let (rows, changed) = (count("complex_rows")?, count("complex_rows_changed")?);
    ensure!(rows > 0, "the export holds no complex");
    let file = if changed == 0 {
        None
    } else {
        Some(stage.write(shape, "SELECT * FROM complex_rows_changed", name)?)
    };
    Ok(StagedComplexes {
        rows,
        changed,
        file,
    })
}

#[cfg(test)]
mod tests;
