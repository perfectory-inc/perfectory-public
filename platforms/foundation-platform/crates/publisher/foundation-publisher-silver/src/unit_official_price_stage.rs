//! One vintage's unit prices in `DuckDB` (root ADR-0095, ADR-0182, ADR-0317 §6).
//!
//! It was the Spark join `unit_official_price.py`, which `gold-panel-rebuild.sh` runs before the
//! building panel: the vintage's prices of `silver.building_register_apartment_price`, each
//! (management key, reference date)'s confirmed notice, joined to the exclusive units as they stood
//! at the vintage (`silver.building_register_exclusive_unit`, its versions at or before the vintage
//! read by the contract's current-row rule: `platform_contracts.rows_as_of`) on a 19-digit PNU
//! (`compose.sql`). A management key that names two units is refused, over the whole dictionary,
//! before any row is joined. A batch names its inputs (`price:<n>|exclusive:<n>|vintage:<yyyymm>`,
//! [`BatchPins`]), so a composition of a batch reads exactly what that batch's join read.

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
    CURRENT_ROWS_COLUMNS, CURRENT_ROWS_FILTER, CURRENT_ROWS_SOURCE,
};

use crate::duckdb_parquet_stage::{table_at, with_contract_columns};
use crate::unit_price_stage::batch::batch_vintage;

/// The table a batch's join appends to while the readers read it (root ADR-0182).
pub const REPLACED: &str = "silver.unit_official_price";
/// The prices, one full copy of the national file per vintage.
pub const PRICE_TABLE: &str = "silver.building_register_apartment_price";
/// The exclusive units, appended as each vintage's changed units (root ADR-0318).
pub const EXCLUSIVE_TABLE: &str = "silver.building_register_exclusive_unit";
const COMPOSE_SQL: &str = include_str!("unit_official_price_stage/compose.sql");
/// The exclusive units' columns the dictionary is made of.
const DICTIONARY_COLUMNS: &[&str] = &["mgmt_key", "pnu", "dong_name", "ho_name"];

/// The inputs one batch joined: the two hub snapshots and the vintage it names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchPins {
    /// The snapshot of [`PRICE_TABLE`].
    pub price: i64,
    /// The snapshot of [`EXCLUSIVE_TABLE`].
    pub exclusive: i64,
    /// `yyyymm`.
    pub vintage: String,
}

impl BatchPins {
    /// The pins a batch's `source_snapshot_id` names (`batch_source_snapshot_id` of the Spark job).
    ///
    /// # Errors
    /// The id is not one the join writes.
    pub fn parse(batch: &str) -> anyhow::Result<Self> {
        let vintage = batch_vintage(batch)
            .with_context(|| format!("{batch:?} is not a batch of {REPLACED}"))?;
        // `batch_vintage` held the shape: digits between the three labels.
        let (price, rest) = batch["price:".len()..]
            .split_once("|exclusive:")
            .context("a batch names its unit snapshot")?;
        let exclusive = rest.split_once("|vintage:").map_or(rest, |(id, _)| id);
        let snapshot = |digits: &str| -> anyhow::Result<i64> {
            digits
                .parse()
                .with_context(|| format!("{batch:?}: {digits} is no snapshot id"))
        };
        Ok(Self {
            price: snapshot(price)?,
            exclusive: snapshot(exclusive)?,
            vintage: vintage.to_owned(),
        })
    }

    /// The batch's `source_snapshot_id`.
    #[must_use]
    pub fn batch(&self) -> String {
        format!(
            "price:{}|exclusive:{}|vintage:{}",
            self.price, self.exclusive, self.vintage
        )
    }
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The exclusive units of `source` (a relation with the table's columns) as they stood at
/// `vintage`: the contract's current rows among the versions at or before it.
///
/// # Errors
/// The table keeps no versions per key.
pub fn units_as_of_sql(source: &str, vintage: &str) -> anyhow::Result<String> {
    ensure!(
        vintage.len() == 6 && vintage.bytes().all(|byte| byte.is_ascii_digit()),
        "{vintage:?} is not a vintage"
    );
    let template = contract(EXCLUSIVE_TABLE)?
        .current_rows_sql_template()
        .with_context(|| format!("{EXCLUSIVE_TABLE} keeps no versions per key"))?;
    // The versions read are cut before the rank, so a later version never hides an older one.
    Ok(template
        .replace(CURRENT_ROWS_COLUMNS, &DICTIONARY_COLUMNS.join(", "))
        .replace(
            CURRENT_ROWS_SOURCE,
            &format!("(SELECT * FROM {source} WHERE vintage <= '{vintage}')"),
        )
        .replace(CURRENT_ROWS_FILTER, "TRUE"))
}

/// Reads the batch's inputs at its pins: `exclusive_source`, the units as of the vintage, and
/// `price_source`, a view of the vintage's price rows (read once, by [`compose`]).
///
/// # Errors
/// Returns an error when an input cannot be read at its pin (an expired snapshot is one).
pub fn load(connection: &Connection, pins: &BatchPins) -> anyhow::Result<()> {
    let pinned = |table: &str, snapshot: i64| -> anyhow::Result<String> {
        with_contract_columns(connection, contract(table)?, &table_at(table, snapshot))
    };
    let units = units_as_of_sql(&pinned(EXCLUSIVE_TABLE, pins.exclusive)?, &pins.vintage)?;
    let prices = pinned(PRICE_TABLE, pins.price)?;
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE exclusive_source AS {units};\n\
             CREATE OR REPLACE TEMP VIEW price_source AS \
             SELECT mgmt_key, base_date, notice_date, price_won FROM {prices} \
             WHERE vintage = '{}';",
            pins.vintage
        ))
        .with_context(|| format!("DuckDB failed to read the inputs of {}", pins.batch()))
}

/// What a composition joined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Composed {
    /// The rows of `unit_prices`.
    pub rows: u64,
    /// The units of the dictionary, over the whole of it.
    pub units: u64,
}

/// The predicate that keeps one 시도's units by their PNU, as a range so the scan prunes by it.
///
/// # Errors
/// The 시도 is not a two-digit code below 99.
pub fn sido_scope(sido: &str) -> anyhow::Result<String> {
    let number: u8 = sido
        .parse()
        .ok()
        .filter(|_| sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()))
        .with_context(|| format!("the 시도 {sido:?} is not a two-digit code"))?;
    ensure!(number < 99, "the 시도 {sido} has no next code to bound it");
    Ok(format!("(pnu >= '{sido}' AND pnu < '{:02}')", number + 1))
}

/// Composes `unit_prices` from what [`load`] (or a test) left: the nation's when `scope` is `None`,
/// else the rows of the units `scope` keeps ([`sido_scope`]).
///
/// # Errors
/// A management key names more than one unit (`unit_official_price.refuse_conflicting`), no row is
/// joined (the Spark job refused an empty batch), or `DuckDB` refuses a statement.
pub fn compose(
    connection: &Connection,
    pins: &BatchPins,
    scope: Option<&str>,
) -> anyhow::Result<Composed> {
    let sql = COMPOSE_SQL
        .replace("@batch@", &pins.batch())
        .replace("@vintage@", &pins.vintage)
        .replace("@scope@", scope.unwrap_or("TRUE"));
    connection
        .execute_batch(&sql)
        .with_context(|| format!("DuckDB failed to join {}", pins.batch()))?;
    // Refused before anything is written, as the Spark job refused it before its join.
    let conflicting: i64 = connection.query_row(
        "SELECT count(*) FROM (SELECT mgmt_key FROM unit_dictionary GROUP BY mgmt_key \
         HAVING count(*) > 1)",
        [],
        |row| row.get(0),
    )?;
    if conflicting > 0 {
        bail!("management keys name multiple units: conflicting={conflicting}");
    }
    let count = |table: &str| -> anyhow::Result<u64> {
        let rows: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        Ok(u64::try_from(rows)?)
    };
    let composed = Composed {
        rows: count("unit_prices")?,
        units: count("unit_dictionary")?,
    };
    ensure!(
        composed.rows > 0,
        "{} produced no price rows; refusing an empty batch",
        pins.batch()
    );
    Ok(composed)
}

#[cfg(test)]
mod tests;
