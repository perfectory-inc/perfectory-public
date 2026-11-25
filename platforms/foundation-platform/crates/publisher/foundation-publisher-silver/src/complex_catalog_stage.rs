//! The industrial-complex catalog in `DuckDB`.
//!
//! It was the Spark job `industrial_complex_silver_to_gold.py` (root ADR-0426, ADR-0564 §6): each
//! complex's current row of `silver.industrial_complexes`, its 시도·시군구 carried to the current codes
//! through the legal-dong pairs in force (`place_identity_stage`'s crosswalk), and the current 시도's
//! name in the newest 법정동 snapshot (`build.sql`). The codes are re-derived on every build, so a new
//! pair reaches the catalog by the next build, never by a merge of its own.

use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use duckdb::Connection;
use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseTableContract, CURRENT_ROWS_COLUMNS,
    CURRENT_ROWS_FILTER, CURRENT_ROWS_SOURCE,
};
use sha2::{Digest, Sha256};

use crate::duckdb_parquet_stage::{table_at, with_contract_columns, PYTHON_VALUES_SQL};
use crate::place_identity_stage::{
    reference_from_loaded, CODE_CHANGE_TABLE, DONG_NAME_TABLE, REFERENCE_SQL,
};
use crate::whole_table_stage::{WholeBuilt, WholeTableStage};

/// The table the catalog is built from.
pub const SILVER: &str = "silver.industrial_complexes";
/// The table the catalog replaces while its readers move (root ADR-0564 §6).
pub const REPLACED: &str = "gold.complex_catalog";
/// What a build reads, each at its pin.
pub const INPUTS: [&str; 3] = [SILVER, CODE_CHANGE_TABLE, DONG_NAME_TABLE];
const BUILD_SQL: &str = include_str!("complex_catalog_stage/build.sql");
/// The Silver columns the build reads.
const SILVER_COLUMNS: &[&str] = &[
    "complex_id",
    "official_complex_code",
    "complex_name",
    "complex_kind",
    "status",
    "development_stage",
    "complex_kind_raw",
    "status_raw",
    "sido_code",
    "sigungu_code",
    "primary_bjdong_code",
    "address_text",
    "management_agency_name",
    "developer_name",
    "designated_date",
    "construction_start_date",
    "completion_date",
    "official_area_sqm",
    "development_progress_percent",
    "lot_sales_status",
    "lot_sales_status_raw",
    "business_period_raw",
    "business_period_start_month",
    "business_period_end_month",
    "designation_basis_law_raw",
    "development_method_raw",
    "development_purpose_raw",
    "invited_industries_raw",
    "source_snapshot_id",
];

/// The snapshot of each input a build reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComplexPins {
    /// `silver.industrial_complexes`.
    pub silver: i64,
    /// `reference.legal_dong_code_change`.
    pub code_changes: i64,
    /// `reference.legal_dong_code_snapshot`.
    pub dong_names: i64,
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

fn pinned(table: &str, snapshot: i64) -> String {
    table_at(table, snapshot)
}

/// Reads the inputs at `pins` into the tables [`build`] reads.
///
/// `complex_current` holds the Silver's current rows by its contract's rule, `place_code_changes`
/// the code changes and `place_dong_names` the newest 법정동 snapshot's rows.
///
/// # Errors
/// Returns an error when an input cannot be read.
pub fn load(connection: &Connection, pins: &ComplexPins) -> anyhow::Result<()> {
    let silver = contract(SILVER)?;
    // The contract's one rendering of its current rows (root ADR-0564 §4): the newest per key, then
    // its predicate.
    let current = silver
        .current_rows_sql_template()
        .with_context(|| format!("{SILVER} keeps no versions per key"))?
        .replace(CURRENT_ROWS_COLUMNS, &SILVER_COLUMNS.join(", "))
        .replace(
            CURRENT_ROWS_SOURCE,
            &with_contract_columns(connection, silver, &pinned(SILVER, pins.silver))?,
        )
        .replace(CURRENT_ROWS_FILTER, "TRUE");
    let (changes, names) = (
        pinned(CODE_CHANGE_TABLE, pins.code_changes),
        pinned(DONG_NAME_TABLE, pins.dong_names),
    );
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE complex_current AS {current};\n\
             CREATE OR REPLACE TEMP TABLE place_code_changes AS \
             SELECT kind, old_code, new_code, level, effective_date, source FROM {changes};\n\
             CREATE OR REPLACE TEMP TABLE place_dong_names AS \
             SELECT region_cd, full_name, status FROM {names} \
             WHERE snapshot_date = (SELECT max(snapshot_date) FROM {names});"
        ))
        .context("DuckDB failed to read the industrial-complex catalog's inputs")
}

/// Builds `built` from what [`load`] left and returns its rows.
///
/// The rows carry `iceberg_snapshot_id` (the Silver snapshot read) and `published_at_utc`; a row the
/// Spark job's gates refused (a required column null, a string column empty, a negative parcel
/// count) is refused.
///
/// # Errors
/// Returns an error when `DuckDB` refuses a statement or a row breaks a gate.
pub fn build(
    connection: &Connection,
    iceberg_snapshot_id: i64,
    published_at_utc: &str,
) -> anyhow::Result<u64> {
    ensure!(
        published_at_utc
            .bytes()
            .all(|byte| byte.is_ascii_digit() || b"-:TZ".contains(&byte)),
        "{published_at_utc:?} is not an instant"
    );
    reference_from_loaded(connection)?;
    connection
        .execute_batch(
            &BUILD_SQL
                .replace("@iceberg_snapshot_id@", &iceberg_snapshot_id.to_string())
                .replace("@published_at_utc@", published_at_utc),
        )
        .context("DuckDB failed to build the industrial-complex catalog")?;
    // The Spark job's gates (`assert_quality_metrics`), read off the replaced table's contract.
    let replaced = contract(REPLACED)?;
    let mut gates: Vec<(String, String)> = Vec::new();
    for column in replaced.columns {
        if column.required {
            gates.push((
                format!("{} is null", column.name),
                format!("{} IS NULL", column.name),
            ));
        }
        if column.logical_type == "string" {
            gates.push((
                format!("{} is empty", column.name),
                format!("{} = ''", column.name),
            ));
        }
    }
    gates.push((
        "parcel_count is negative".to_owned(),
        "parcel_count < 0".to_owned(),
    ));
    let counts = gates
        .iter()
        // `count_if` is NULL when every input is; a filtered count is never.
        .map(|(_, predicate)| format!("count(*) FILTER (WHERE {predicate})"))
        .collect::<Vec<_>>()
        .join(", ");
    let broken: Vec<String> =
        connection.query_row(&format!("SELECT {counts} FROM built"), [], |row| {
            let mut broken = Vec::new();
            for (index, (gate, _)) in gates.iter().enumerate() {
                let rows: i64 = row.get(index)?;
                if rows > 0 {
                    broken.push(format!("{gate} on {rows} rows"));
                }
            }
            Ok(broken)
        })?;
    ensure!(
        broken.is_empty(),
        "the industrial-complex catalog breaks its gates: {}",
        broken.join("; ")
    );
    let rows: i64 = connection.query_row("SELECT count(*) FROM built", [], |row| row.get(0))?;
    Ok(u64::try_from(rows)?)
}

/// The build's code version (`foundation.build-source-sha256`).
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [BUILD_SQL, REFERENCE_SQL, PYTHON_VALUES_SQL] {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// The catalog as a whole-table lane of the shared versions build (root ADR-0615, revision
/// 2026-11-26).
pub struct CatalogStage;

/// The industrial-complex catalog's stage.
pub const CATALOG: CatalogStage = CatalogStage;

impl WholeTableStage for CatalogStage {
    fn inputs(&self) -> Vec<&'static str> {
        INPUTS.to_vec()
    }

    fn code_version(&self) -> String {
        code_version()
    }

    fn build(
        &self,
        connection: &Connection,
        pins: &BTreeMap<String, i64>,
        published_at_utc: &str,
    ) -> anyhow::Result<WholeBuilt> {
        let pin = |table: &str| {
            pins.get(table)
                .copied()
                .with_context(|| format!("{table} is not pinned: the build reads it"))
        };
        let pins = ComplexPins {
            silver: pin(SILVER)?,
            code_changes: pin(CODE_CHANGE_TABLE)?,
            dong_names: pin(DONG_NAME_TABLE)?,
        };
        load(connection, &pins)?;
        Ok(WholeBuilt {
            rows: build(connection, pins.silver, published_at_utc)?,
            source_snapshot_id: pins.silver.to_string(),
        })
    }
}

#[cfg(test)]
mod tests;
