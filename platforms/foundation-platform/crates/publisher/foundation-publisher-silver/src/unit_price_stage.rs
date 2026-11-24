//! The twin of `unit_price_panel_silver_to_gold.py` (root ADR-0204).
//!
//! One row per (PNU, building) holding the official price history of every priced unit placed there:
//! a unit is placed under the PNU whose building document carries it (`building_unit_placement`, the
//! building panel's rule), its building the title its evidenced link names (the nil UUID when none).
//! A unit's history is every (reference date, price) of the price table's newest vintage batch, newest
//! date first; a row's units are ordered by their catalog id. The register rows are their contracts'
//! current rows; a register row without a PNU is dropped and counted, one whose PNU breaks the
//! cadastral grammar or whose key repeats is refused, and a price whose (unit, date) repeats or whose
//! value is invalid is refused, priced unit or not, as the Spark job refuses them.
//!
//! A register row is read under the served code: its PNU is re-keyed through the shared renumbering
//! (`kind_versions_stage::spool` leaves `renumbering(src, dst)` from `reference.legal_dong_code_change`
//! and the served parcel edition), as every kind's rows are, so the day the served edition moves to
//! new 법정동 codes a unit is still placed under the PNU its parcel is served by.
//!
//! Regional, one pass: the registers are read once and placed nationally (narrow: a key and two
//! PNUs per unit), the chosen price batch is read once, joined to the placement and spooled by the
//! 시도 of the PNU each unit is served under, and each 시도 is then loaded and folded alone. Every run
//! builds every row: the reading is the cost, and a run that named the changed PNUs would read both
//! its sides; the commit appends only the keys whose rows changed. The documents are written as
//! `DuckDB` writes JSON; the takeover verdict compares them as values.

use std::{collections::BTreeSet, path::Path};

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use sha2::{Digest, Sha256};

use crate::building_unit_placement::{self, catalog_digest_sql, catalog_id_sql};
use crate::duckdb_parquet_stage::with_contract_columns;
use crate::kind_versions_stage::{
    current_rows_query, fill, is_plain, renumbering_join, served_pnu, KindVersionsStage, RowSet,
    CODE_CHANGE_SOURCE, PARCEL_SOURCE,
};
use crate::parcel_nearby_stage::Source;

pub mod batch;

/// The register titles a building is.
pub const TITLE_SOURCE: &str = "silver.building_register_titles";
/// The register units.
pub const UNIT_SOURCE: &str = "silver.building_register_units";
/// The units' reference-date prices, one batch per vintage.
pub const PRICE_SOURCE: &str = "silver.unit_official_price";
/// The building of a unit no evidenced link places in one.
pub const UNLINKED_BUILDING_ID: &str = "00000000-0000-0000-0000-000000000000";
/// The spool part of the prices no placed unit has (a 시도 is two digits, so it is never one).
const UNPLACED: &str = "unplaced";
/// What the rules are; part of the build's code version.
const RULES_VERSION: &str = "unit-price-duckdb-v1";

/// A register's current rows, the PNU trimmed and put under the served code (`@served@` over
/// `@join@`, the shared renumbering), as `<name>`: read once, the rows without a PNU counted from it
/// and then dropped.
const REGISTER_SQL: &str = "\
CREATE OR REPLACE TABLE @name@ AS SELECT t.* REPLACE (@served@ AS pnu) \
FROM (SELECT * REPLACE (trim(pnu, ' ') AS pnu) FROM (@rows@)) t @join@;";

/// `built`: the content columns of `gold.unit_price` and its digest.
const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, building_id VARCHAR, \
    units_json VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// The titles as the placement reads them: key, PNU and catalog id.
const TITLES_SQL: &str = "\
CREATE OR REPLACE TABLE titles AS \
SELECT mgm_bldrgst_pk, pnu, @id@ AS id FROM (SELECT *, @digest@ AS d FROM title_rows);";

/// The price batch joined to the placement, spooled by the served PNU's 시도; a price of no placed
/// unit lies in the [`UNPLACED`] part.
const PRICE_SPOOL_SQL: &str = "\
COPY (SELECT p.mgm_bldrgst_pk, p.base_date, p.price_won, pl.serving_pnu AS pnu, \
        coalesce(pl.building_id, '@unlinked@') AS building_id, pl.unit_id, \
        coalesce(substr(pl.serving_pnu, 1, 2), '@unplaced@') AS sido \
      FROM (SELECT mgm_bldrgst_pk, base_date, price_won FROM @prices@ \
            WHERE source_snapshot_id = '@batch@') p \
      LEFT JOIN placed pl USING (mgm_bldrgst_pk)) \
TO '@dir@/prices' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (sido), OVERWRITE_OR_IGNORE);";

/// What a price part refuses (`building_panel.checked_prices`): a (register, reference date) twice,
/// a register key absent, a date not of eight digits, a price absent or negative.
const PRICE_REFUSALS_SQL: &str = "\
SELECT (SELECT count(*) - count(DISTINCT (mgm_bldrgst_pk, base_date)) FROM @part@), \
  (SELECT count(*) FROM @part@ WHERE base_date IS NULL OR NOT regexp_full_match(base_date, '[0-9]{8}') \
     OR price_won IS NULL OR price_won < 0), \
  (SELECT count(*) FROM @part@ WHERE mgm_bldrgst_pk IS NULL)";

/// The loaded 시도's rows, one per (PNU, building), inserted into `built`: the content columns of
/// `gold.unit_price` and its `row_digest` of them alone (root ADR-0099).
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH histories AS ( \
  SELECT pnu, building_id, unit_id, list({'base_date': base_date, 'price_won': price_won} \
    ORDER BY base_date DESC, price_won DESC) AS history \
  FROM prices GROUP BY pnu, building_id, unit_id), \
rows AS ( \
  SELECT pnu, building_id, to_json(list({'id': unit_id, 'official_price_history': history} \
    ORDER BY unit_id))::VARCHAR AS units_json \
  FROM histories GROUP BY pnu, building_id) \
SELECT pnu, building_id, units_json, \
  sha256(to_json({'pnu': pnu, 'building_id': building_id, 'units_json': units_json})::VARCHAR) AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM rows";

/// What a read of the registers found.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Registers {
    /// Titles without a PNU, dropped.
    pub titles_without_pnu: u64,
    /// Units without a PNU, dropped.
    pub units_without_pnu: u64,
    /// Units placed.
    pub placed_units: u64,
}

/// The cadastral grammar a building document's PNU follows (`building_by_pnu_gateway.object_key`,
/// the pattern the Spark job reads).
///
/// # Errors
/// Returns an error when the gateway contract cannot be read.
pub fn pnu_pattern() -> anyhow::Result<&'static str> {
    Ok(
        &foundation_publisher_core::by_pnu_gateway_contract::ByPnuLane::Building
            .policy()?
            .object_key
            .pnu_pattern,
    )
}

/// The build's code version: a change of it rebuilds every row. It folds the SQL as it runs, the
/// generated parts filled in (the placement, the evidence rule, the catalog ids, the PNU grammar).
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    let generated = [
        pnu_pattern().map_or_else(|error| error.to_string(), str::to_owned),
        catalog_digest_sql("building", "mgm_bldrgst_pk"),
        catalog_id_sql("d"),
    ];
    let texts = [
        RULES_VERSION,
        REGISTER_SQL,
        TITLES_SQL,
        PRICE_SPOOL_SQL,
        PRICE_REFUSALS_SQL,
        DOCUMENT_SQL,
    ]
    .map(str::to_owned)
    .into_iter()
    .chain(generated)
    .chain(building_unit_placement::code_texts());
    for text in texts {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

fn count(connection: &Connection, sql: &str) -> anyhow::Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value)?)
}

/// Leaves `name` of `table`'s current rows (`columns`) with a present PNU, refuses a PNU off the
/// grammar and a repeated or absent key (`building_panel.valid_pnus`, `assert_unique`), and returns
/// how many rows had no PNU.
fn register(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
    name: &str,
) -> anyhow::Result<u64> {
    let rows = current_rows_query(connection, source, table, columns, "TRUE")?;
    connection.execute_batch(&fill(
        REGISTER_SQL,
        &[
            ("name", name),
            ("rows", &rows),
            ("served", &served_pnu("t.pnu", "r")),
            ("join", &renumbering_join("t.pnu", "r", "")),
        ],
    ))?;
    let without = count(
        connection,
        &format!("SELECT count(*) FROM {name} WHERE pnu IS NULL OR pnu = ''"),
    )?;
    connection.execute_batch(&format!(
        "DELETE FROM {name} WHERE pnu IS NULL OR pnu = '';"
    ))?;
    let pattern = pnu_pattern()?;
    ensure!(
        !pattern.contains('\''),
        "the PNU grammar {pattern:?} holds a quote"
    );
    let off_grammar = count(
        connection,
        &format!("SELECT count(*) FROM {name} WHERE NOT regexp_full_match(pnu, '{pattern}')"),
    )?;
    ensure!(
        off_grammar == 0,
        "{table} has {off_grammar} rows with an invalid cadastral PNU"
    );
    let (rows_held, keys): (i64, i64) = connection.query_row(
        &format!(
            "SELECT count(*), count(DISTINCT mgm_bldrgst_pk) FILTER (WHERE trim(mgm_bldrgst_pk, ' ') <> '') \
             FROM {name}"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(
        rows_held == keys,
        "{table} has a duplicate or absent identity [\"mgm_bldrgst_pk\"] ({rows_held} rows on {keys} keys)"
    );
    Ok(without)
}

/// Reads the titles and the units at `source` and leaves `titles` and `placed` (`mgm_bldrgst_pk`,
/// `serving_pnu`, `building_id`, `unit_id`).
///
/// # Errors
/// Returns an error when an input cannot be read or a register row is refused.
pub fn read_registers(connection: &Connection, source: &Source<'_>) -> anyhow::Result<Registers> {
    let titles_without_pnu = register(
        connection,
        source,
        TITLE_SOURCE,
        &["mgm_bldrgst_pk", "pnu"],
        "title_rows",
    )?;
    connection.execute_batch(&fill(
        TITLES_SQL,
        &[
            ("digest", &catalog_digest_sql("building", "mgm_bldrgst_pk")),
            ("id", &catalog_id_sql("d")),
        ],
    ))?;
    let policy = building_unit_placement::policy()?;
    let mut unit_columns = vec!["mgm_bldrgst_pk", "pnu", "building_mgm_bldrgst_pk"];
    unit_columns.extend(policy.columns.iter().map(String::as_str));
    let units_without_pnu = register(connection, source, UNIT_SOURCE, &unit_columns, "unit_rows")?;
    building_unit_placement::place(connection, "unit_rows", "titles", "placed")?;
    let placed_units = count(connection, "SELECT count(*) FROM placed")?;
    Ok(Registers {
        titles_without_pnu,
        units_without_pnu,
        placed_units,
    })
}

/// A spool directory as `DuckDB` SQL text.
fn sql_dir(dir: &Path) -> anyhow::Result<String> {
    let text = dir
        .to_str()
        .context("the spool directory must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !text.contains('\''),
        "the spool directory must not contain a quote"
    );
    Ok(text)
}

/// The price table at `source`, with every contract column.
fn prices_relation(connection: &Connection, source: &Source<'_>) -> anyhow::Result<String> {
    let contract = lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == PRICE_SOURCE)
        .with_context(|| format!("no lakehouse contract for {PRICE_SOURCE}"))?;
    with_contract_columns(connection, contract, &source.of(PRICE_SOURCE)?)
}

/// The `source_snapshot_id`s the price table holds at `source`, the batches [`batch::chosen_batch`]
/// chooses among.
///
/// # Errors
/// Returns an error when the table cannot be read.
pub fn batch_ids(connection: &Connection, source: &Source<'_>) -> anyhow::Result<BTreeSet<String>> {
    let prices = prices_relation(connection, source)?;
    let ids: BTreeSet<Option<String>> = connection
        .prepare(&format!("SELECT DISTINCT source_snapshot_id FROM {prices}"))?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    // A null id is no batch: as the empty one, it leaves the mix to be refused.
    Ok(ids.into_iter().map(Option::unwrap_or_default).collect())
}

/// What a price part refuses, as the Spark job refuses the batch.
fn refuse_prices(connection: &Connection, part: &str) -> anyhow::Result<()> {
    let (repeated, invalid, keyless): (i64, i64, i64) =
        connection.query_row(&fill(PRICE_REFUSALS_SQL, &[("part", part)]), [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    ensure!(
        repeated == 0 && keyless == 0,
        "{PRICE_SOURCE} has duplicate identity [\"mgm_bldrgst_pk\", \"base_date\"] ({repeated} repeated, \
         {keyless} without a key)"
    );
    ensure!(
        invalid == 0,
        "unit official price has an invalid base_date or negative price ({invalid} rows)"
    );
    Ok(())
}

/// A part of the price spool, as `DuckDB` reads it.
fn part(dir_text: &str, name: &str) -> String {
    format!("read_parquet('{dir_text}/prices/sido={name}/*.parquet', hive_partitioning = false)")
}

/// What a spool holds: the 시도 with placed prices, and how many prices no unit is placed for.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Spooled {
    /// The 시도 of the PNUs the priced units are served under.
    pub sidos: BTreeSet<String>,
    /// The batch's prices of registers no unit is placed for (`unplaced_price_unit_count` counts
    /// their registers).
    pub unplaced_prices: u64,
    /// The registers of those prices.
    pub unplaced_price_units: u64,
}

/// Spools the price batch `batch` at `source`, joined to `placed`, into `dir` by 시도.
///
/// The 시도 is the served PNU's. The invalid prices no unit is placed for are refused here, each
/// 시도's when it is loaded.
///
/// # Errors
/// Returns an error when the batch is not a plain id, the table cannot be read, or a price is
/// refused.
pub fn spool_prices(
    connection: &Connection,
    source: &Source<'_>,
    batch: &str,
    dir: &Path,
) -> anyhow::Result<Spooled> {
    // Any id the table holds is read, as the Spark job reads it; only a quote could leave the SQL.
    ensure!(
        !batch.contains('\''),
        "the price batch {batch:?} holds a quote"
    );
    let prices = prices_relation(connection, source)?;
    let dir_text = sql_dir(dir)?;
    connection.execute_batch(&fill(
        PRICE_SPOOL_SQL,
        &[
            ("prices", &prices),
            ("batch", batch),
            ("unlinked", UNLINKED_BUILDING_ID),
            ("unplaced", UNPLACED),
            ("dir", &dir_text),
        ],
    ))?;
    let mut spooled = Spooled::default();
    let root = dir.join("prices");
    if !root.is_dir() {
        return Ok(spooled);
    }
    for entry in
        std::fs::read_dir(&root).with_context(|| format!("cannot list {}", root.display()))?
    {
        let name = entry?.file_name().to_string_lossy().into_owned();
        match name.strip_prefix("sido=") {
            Some(UNPLACED) => {
                let unplaced = part(&dir_text, UNPLACED);
                refuse_prices(connection, &unplaced)?;
                spooled.unplaced_prices =
                    count(connection, &format!("SELECT count(*) FROM {unplaced}"))?;
                spooled.unplaced_price_units = count(
                    connection,
                    &format!("SELECT count(DISTINCT mgm_bldrgst_pk) FROM {unplaced}"),
                )?;
            }
            Some(sido) if sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()) => {
                spooled.sidos.insert(sido.to_owned());
            }
            _ => bail!("the price spool holds {name}, neither a 시도 nor the unplaced part"),
        }
    }
    Ok(spooled)
}

/// Loads `sido` of the spool as `prices`, refuses its invalid prices and leaves an empty `built`.
/// Returns its price rows.
///
/// # Errors
/// Returns an error when `sido` is not two digits, the spool cannot be read or a price is refused.
pub fn load_region(connection: &Connection, dir: &Path, sido: &str) -> anyhow::Result<u64> {
    ensure!(
        sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()),
        "the 시도 {sido:?} is not two digits"
    );
    let dir_text = sql_dir(dir)?;
    let body = if dir.join("prices").join(format!("sido={sido}")).is_dir() {
        let held = part(&dir_text, sido);
        refuse_prices(connection, &held)?;
        format!("SELECT * FROM {held}")
    } else {
        "SELECT CAST(NULL AS VARCHAR) AS mgm_bldrgst_pk, CAST(NULL AS VARCHAR) AS base_date, \
         CAST(NULL AS BIGINT) AS price_won, CAST(NULL AS VARCHAR) AS pnu, \
         CAST(NULL AS VARCHAR) AS building_id, CAST(NULL AS VARCHAR) AS unit_id WHERE FALSE"
            .to_owned()
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE prices AS {body}; {BUILT_TABLE}"
    ))?;
    count(connection, "SELECT count(*) FROM prices")
}

/// Builds the rows of every PNU of the loaded 시도 into `built`.
///
/// # Errors
/// Returns an error when the stamps are not plain ids or `DuckDB` refuses the statement.
pub fn build(
    connection: &Connection,
    source_snapshot_id: &str,
    published_at_utc: &str,
) -> anyhow::Result<()> {
    ensure!(
        is_plain(source_snapshot_id) && is_plain(published_at_utc),
        "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
    );
    connection.execute_batch(&fill(
        DOCUMENT_SQL,
        &[
            ("source_snapshot_id", source_snapshot_id),
            ("published_at_utc", published_at_utc),
        ],
    ))?;
    Ok(())
}

/// The unit price lane as the shared build sees it (root ADR-0615 §7): what the shared spool reads
/// for it (the served parcels and the code change table, for the renumbering) and its build, which
/// the shared [`crate::kind_versions_parts::build_in_parts`] runs one 시군구 of `prices` at a time.
///
/// The lane still has its own driver (`unit_price_versions_build`): the price batch's choice, the
/// host's register pins and the national check of shrunk histories are not the shared driver's yet.
#[derive(Clone, Copy, Debug)]
pub struct UnitPrice;

/// The one unit price lane.
pub const UNIT_PRICE: UnitPrice = UnitPrice;

impl KindVersionsStage for UnitPrice {
    fn section_column(&self) -> &'static str {
        "units_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![
            PARCEL_SOURCE,
            TITLE_SOURCE,
            UNIT_SOURCE,
            PRICE_SOURCE,
            CODE_CHANGE_SOURCE,
        ]
    }

    // Every run builds every row (the module's first lines): any input moves every document.
    fn whole_table_inputs(&self) -> Vec<&'static str> {
        self.inputs()
    }

    fn code_version(&self) -> String {
        code_version()
    }

    // The lane's own driver reads its registers and prices; the shared spool reads only the served
    // parcels and the code changes for it.
    fn row_sets(&self) -> Vec<RowSet> {
        Vec::new()
    }

    fn pnu_spools(&self) -> Vec<&'static str> {
        vec!["prices"]
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    fn build(
        &self,
        connection: &Connection,
        _all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        build(connection, source_snapshot_id, published_at_utc)
    }
}

/// The unit histories of the rows `rows` (a relation with `units_json`): `(id, records)` per unit,
/// what the shrink check compares (`unit_price_panel.history_lengths`).
#[must_use]
pub fn history_lengths_sql(rows: &str) -> String {
    format!(
        "SELECT unit ->> 'id' AS id, \
           CAST(json_array_length(unit -> 'official_price_history') AS BIGINT) AS records \
         FROM (SELECT unnest(from_json(units_json, '[\"JSON\"]')) AS unit FROM ({rows}))"
    )
}

/// Why a build whose `shrunk` of the current `units` units lost price records is refused, or `None`.
///
/// `unit_price_panel.shrink_verdict`: a vintage keeps every reference date it ever published (root
/// ADR-0185), so a unit's history only grows, and a vintage loaded in part shrinks many at once.
#[must_use]
pub fn shrink_verdict(shrunk: u64, units: u64, max_fraction: f64) -> Option<String> {
    #[allow(clippy::cast_precision_loss)]
    let (shrunk_f, units_f) = (shrunk as f64, units as f64);
    if shrunk == 0 || shrunk_f <= units_f * max_fraction {
        return None;
    }
    Some(format!(
        "{shrunk} of the current Gold's {units} units would keep fewer price records than it holds \
         ({:.4}%; at most {:.4}% may, gold-panel-rebuild.contract.json \
         max_price_history_shrink_fraction): a price vintage that lost records must not replace one \
         that has them",
        100.0 * shrunk_f / units_f.max(1.0),
        100.0 * max_fraction
    ))
}

#[cfg(test)]
mod tests;
