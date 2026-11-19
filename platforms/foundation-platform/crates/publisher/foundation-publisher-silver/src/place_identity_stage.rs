//! The one place-identity resolver every parcel source calls at ingestion (root ADR-0288 §1), in
//! `DuckDB` inside the publisher (root ADR-0559).
//!
//! It was `place_identity_resolver.py` and `address_pnu.py`, read by the Spark loads. A source names
//! a place by a legal-dong code or by an address; this turns either into the current PNU of a served
//! parcel, never keeping a superseded code verbatim:
//!
//! - `reference.sql`, once a run: the code crosswalk of the pairs in force (old → current, the codes
//!   that split), the earlier forms of each code a pair touches, and the address readings of the
//!   newest 법정동 snapshot (시도 words, 법정동 names, abolished names through their pairs);
//! - `offers.sql`, over the rows to place (`place_rows`): what each address offers and every PNU the
//!   run asks the served parcels about;
//! - the served parcels among them and the register titles on the roads offered, read once
//!   ([`read_served`]);
//! - `place.sql`: the code path first, then the address, as `place_result`.

use anyhow::Context;

use crate::duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG};

const REFERENCE_SQL: &str = include_str!("place_identity_stage/reference.sql");
const OFFERS_SQL: &str = include_str!("place_identity_stage/offers.sql");
const PLACE_SQL: &str = include_str!("place_identity_stage/place.sql");

/// The legal-dong code change record.
pub const CODE_CHANGE_TABLE: &str = "reference.legal_dong_code_change";
/// The 법정동 code list, one snapshot per release.
pub const DONG_NAME_TABLE: &str = "reference.legal_dong_code_snapshot";
/// The parcels, every edition.
pub const PARCEL_TABLE: &str = "silver.parcel_boundaries";
/// The building register's titles (road addresses).
pub const TITLE_TABLE: &str = "silver.building_register_titles";

/// The snapshots a run reads its place-identity inputs at (each its head when the run starts), and
/// the parcel edition the map serves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacePins {
    /// `reference.legal_dong_code_change`.
    pub code_changes: i64,
    /// `reference.legal_dong_code_snapshot`.
    pub dong_names: i64,
    /// `silver.parcel_boundaries`.
    pub parcels: i64,
    /// `silver.building_register_titles`.
    pub titles: i64,
    /// `silver.parcel_boundaries.source_snapshot_id` of the served edition (root ADR-0291 §5).
    pub served_snapshot_id: String,
}

fn source(table: &str, snapshot: i64) -> String {
    format!("{LAKEHOUSE_CATALOG}.{table} AT (VERSION => {snapshot})")
}

/// Reads the code changes and the newest 법정동 snapshot at `pins`, and builds the crosswalk and the
/// address readings (`reference.sql`). Needs `python_values.sql` on the stage.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn load_reference(stage: &DuckDbParquetStage, pins: &PlacePins) -> anyhow::Result<()> {
    let changes = source(CODE_CHANGE_TABLE, pins.code_changes);
    let names = source(DONG_NAME_TABLE, pins.dong_names);
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE place_code_changes AS \
             SELECT kind, old_code, new_code, level, effective_date, source FROM {changes};\n\
             CREATE OR REPLACE TEMP TABLE place_dong_names AS \
             SELECT region_cd, full_name, status FROM {names} \
             WHERE snapshot_date = (SELECT max(snapshot_date) FROM {names});\n{REFERENCE_SQL}"
        ))
        .context("DuckDB failed to read the place-identity reference tables")
}

/// What the rows in `place_rows` offer and ask (`offers.sql`).
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn offers(stage: &DuckDbParquetStage) -> anyhow::Result<()> {
    stage
        .connection()
        .execute_batch(OFFERS_SQL)
        .context("DuckDB failed to read the addresses")
}

/// How many PNUs the run asked about, and how many the served edition holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParcelsRead {
    /// PNUs asked.
    pub asked: u64,
    /// Of them, served parcels.
    pub held: u64,
}

/// Reads the served parcels among `place_asked` and the register titles of every building that ever
/// stood on a road `place_roads` names, current rows only (root ADR-0390: the whole register is not
/// carried past the scan). `title_rows_sql` is the titles' contract-rendered current-row query.
///
/// # Errors
/// Returns a `DuckDB` failure, or the snapshot id holds a quote.
pub fn read_served(
    stage: &DuckDbParquetStage,
    pins: &PlacePins,
    title_rows_sql: &str,
) -> anyhow::Result<ParcelsRead> {
    anyhow::ensure!(
        !pins.served_snapshot_id.contains('\''),
        "the served edition's snapshot id holds a quote"
    );
    let parcels = source(PARCEL_TABLE, pins.parcels);
    let titles = source(TITLE_TABLE, pins.titles);
    let keys = format!(
        "mgm_bldrgst_pk IN (SELECT mgm_bldrgst_pk FROM {titles} \
         WHERE regexp_extract(road_address_raw, place_register_road(), 2) IN (SELECT road_name FROM place_roads))"
    );
    let title_rows = title_rows_sql
        .replace("{columns}", "pnu, road_address_raw")
        .replace("{source}", &titles)
        .replace("{filter}", &keys);
    let connection = stage.connection();
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE place_parcels AS SELECT DISTINCT pnu FROM {parcels} \
             WHERE source_snapshot_id = '{}' AND valid_to_utc IS NULL AND pnu IN (SELECT pnu FROM place_asked);\n\
             CREATE OR REPLACE TEMP TABLE place_titles AS {title_rows};",
            pins.served_snapshot_id
        ))
        .context("DuckDB failed to read the served parcels and the register titles")?;
    let count = |table: &str| -> anyhow::Result<u64> {
        let value: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        Ok(u64::try_from(value)?)
    };
    Ok(ParcelsRead {
        asked: count("place_asked")?,
        held: count("place_parcels")?,
    })
}

/// Places the rows in `place_rows` (`offers.sql` then `place.sql`) as `place_result`, and returns
/// how many each grade placed (`pnu_basis`, or `unresolved:<reason>`).
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn place(
    stage: &DuckDbParquetStage,
) -> anyhow::Result<std::collections::BTreeMap<String, u64>> {
    let connection = stage.connection();
    connection
        .execute_batch(&format!("{OFFERS_SQL}\n{PLACE_SQL}"))
        .context("DuckDB failed to place the rows")?;
    let mut grades = connection.prepare(
        "SELECT coalesce(pnu_basis, 'unresolved:' || coalesce(pnu_unresolved_reason, 'None')), count(*) \
         FROM place_result GROUP BY 1 ORDER BY 1",
    )?;
    let found = grades
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .map(|row| {
            let (grade, n) = row?;
            Ok((grade, u64::try_from(n)?))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(found)
}

#[cfg(test)]
mod tests;
