//! The twin of `unit_price_panel_silver_to_gold.py` (root ADR-0204), on the shared Gold versions
//! driver (`kind_versions_build`, root ADR-0615 §7).
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
//! What the lane gives the shared driver: the registers read and placed once, nationally
//! ([`KindVersionsStage::prepare`]: narrow, a key and two PNUs per unit); the price table read by
//! one batch ([`KindVersionsStage::batch_input`]: the newest vintage's, chosen by the table's ingest
//! log); that batch joined to the placement and spooled by the 시도 of the PNU each unit is served
//! under, by the shared split ([`KindVersionsStage::spool_more`]); a 시도 of it loaded
//! ([`KindVersionsStage::load_more`]) and built a 시군구 at a time by the shared build. The registers
//! are pinned to the building Gold's build by the rebuild contract's `pins_from`, which the driver
//! reads. Every run builds every row: any input moves every document, the reading is the cost, and a
//! run that named the changed PNUs would read both its sides; the commit appends only the keys whose
//! rows changed. A build in which more of the current Gold's units would keep fewer price records
//! than the contract allows is refused before anything is appended
//! ([`KindVersionsStage::national_checks`]). The documents are written as `DuckDB` writes JSON; the
//! takeover verdict compares them as values.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{ensure, Context};
use duckdb::Connection;
use lakehouse_infrastructure::IngestCommitLog;
use sha2::{Digest, Sha256};

use crate::building_unit_placement::{self, catalog_digest_sql, catalog_id_sql};
use crate::duckdb_parquet_stage::with_contract_columns;
use crate::kind_versions_stage::{
    current_rows_query, elapsed, fill, input_batch, is_plain, renumbering_join, served_pnu,
    spool_by_sido, BatchInput, KindVersionsStage, NationalCheck, ResultCheck, RowSet,
    CODE_CHANGE_SOURCE, PARCEL_SOURCE, SHARED_SQL,
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
/// The commit property that names the price batch a commit read (root ADR-0564 §9 차례 4).
pub const PRICE_BATCH_PROPERTY: &str = "foundation.unit-price-batch";
/// The spool, and a 시도's table, of the batch's prices joined to the placement.
const PRICES: &str = "prices";
/// What the read dropped, `(name, breaches, judged)`, for the checks that report it.
const READ_COUNTS: &str = "unit_price_read";
/// The check that reports the register rows without a PNU.
const WITHOUT_PNU: &str = "register_rows_without_pnu";
/// The check that reports the priced units no unit row is placed for.
const UNPLACED: &str = "priced_units_not_placed";
/// What the rules are; part of the build's code version.
const RULES_VERSION: &str = "unit-price-duckdb-v1";
/// Why the lane reads no base side.
const WHOLE_RUNS: &str =
    "the unit price lane builds every row on every run (any input moves every document): it reads no base side";

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

/// The price batch joined to the placement, the rows of the [`PRICES`] spool: a price of no placed
/// unit has no PNU, and the shared split writes it under no 시도.
const PRICE_QUERY: &str = "\
SELECT p.mgm_bldrgst_pk, p.base_date, p.price_won, pl.serving_pnu AS pnu, \
  coalesce(pl.building_id, '@unlinked@') AS building_id, pl.unit_id \
FROM (SELECT mgm_bldrgst_pk, base_date, price_won FROM @prices@ \
      WHERE source_snapshot_id = '@batch@') p \
LEFT JOIN placed pl USING (mgm_bldrgst_pk)";

/// A 시도 of the spool that holds no price.
const NO_PRICES: &str = "SELECT CAST(NULL AS VARCHAR) AS mgm_bldrgst_pk, \
    CAST(NULL AS VARCHAR) AS base_date, CAST(NULL AS BIGINT) AS price_won, \
    CAST(NULL AS VARCHAR) AS pnu, CAST(NULL AS VARCHAR) AS building_id, \
    CAST(NULL AS VARCHAR) AS unit_id WHERE FALSE";

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

/// A built row that names no building or holds no priced unit.
const UNPRICED_ROW: &str = "building_id IS NULL OR units_json = '[]'";

/// The tables the check of kept price histories gathers into: the unit histories of the rows each
/// 시도's commit would replace and of those it would write, and the units its current rows hold.
const SHRINK_SETUP_SQL: &str = "\
CREATE OR REPLACE TABLE shrink_before (id VARCHAR, records BIGINT); \
CREATE OR REPLACE TABLE shrink_after (id VARCHAR, records BIGINT); \
CREATE OR REPLACE TABLE shrink_units (units BIGINT);";

/// One 시도's share of them: `@before@` and `@after@` are [`history_lengths_sql`] of the rows the
/// commit replaces and writes (a unit that moved 시도 meets its new row in the national count).
const SHRINK_GATHER_SQL: &str = "\
INSERT INTO shrink_before @before@; INSERT INTO shrink_after @after@; \
INSERT INTO shrink_units \
  SELECT CAST(coalesce(sum(json_array_length(units_json)), 0) AS BIGINT) FROM current_rows;";

/// The units whose history would keep fewer records than it holds, and the current units.
const SHRINK_COUNT_SQL: &str = "\
SELECT (SELECT count(*) FROM shrink_before b LEFT JOIN shrink_after a USING (id) \
          WHERE coalesce(a.records, 0) < b.records), \
       (SELECT CAST(coalesce(sum(units), 0) AS BIGINT) FROM shrink_units)";

/// What a read of the registers found.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Registers {
    /// Titles without a PNU, dropped.
    pub titles_without_pnu: u64,
    /// Units without a PNU, dropped.
    pub units_without_pnu: u64,
    /// Register rows read, titles and units, with a PNU or without.
    pub rows_read: u64,
    /// Units placed.
    pub placed_units: u64,
    /// Milliseconds each register took to read, by table name.
    pub read_millis: BTreeMap<String, u64>,
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
/// generated parts filled in (the placement, the evidence rule, the catalog ids, the PNU grammar),
/// and the shared renumbering and split.
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
        PRICE_QUERY,
        PRICE_REFUSALS_SQL,
        DOCUMENT_SQL,
    ]
    .into_iter()
    .chain(SHARED_SQL)
    .map(str::to_owned)
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
/// how many rows had no PNU and how many were read.
fn register(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
    name: &str,
) -> anyhow::Result<(u64, u64)> {
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
    let read = count(connection, &format!("SELECT count(*) FROM {name}"))?;
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
    Ok((without, read))
}

/// Reads the titles and the units at `source` and leaves `titles` and `placed` (`mgm_bldrgst_pk`,
/// `serving_pnu`, `building_id`, `unit_id`).
///
/// # Errors
/// Returns an error when an input cannot be read or a register row is refused.
pub fn read_registers(connection: &Connection, source: &Source<'_>) -> anyhow::Result<Registers> {
    let mut read_millis = BTreeMap::new();
    let at = Instant::now();
    let (titles_without_pnu, titles_read) = register(
        connection,
        source,
        TITLE_SOURCE,
        &["mgm_bldrgst_pk", "pnu"],
        "title_rows",
    )?;
    read_millis.insert(TITLE_SOURCE.to_owned(), elapsed(at));
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
    let at = Instant::now();
    let (units_without_pnu, units_read) =
        register(connection, source, UNIT_SOURCE, &unit_columns, "unit_rows")?;
    read_millis.insert(UNIT_SOURCE.to_owned(), elapsed(at));
    building_unit_placement::place(connection, "unit_rows", "titles", "placed")?;
    let placed_units = count(connection, "SELECT count(*) FROM placed")?;
    Ok(Registers {
        titles_without_pnu,
        units_without_pnu,
        rows_read: titles_read + units_read,
        placed_units,
        read_millis,
    })
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

/// The batch the Gold reads ([`BatchInput::choose`]): of the ids the price table holds at `source`,
/// the newest vintage's, its ties broken by the order the table's ingest `log` appended them.
///
/// # Errors
/// Returns an error when the table cannot be read or its batches cannot be ordered.
pub fn choose_batch(
    connection: &Connection,
    source: &Source<'_>,
    log: &IngestCommitLog,
) -> anyhow::Result<String> {
    let order = batch::order_batches(&log.ingested, log.head, &log.snapshots);
    batch::chosen_batch(&batch_ids(connection, source)?, &order)
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

/// Builds the rows of every PNU of the loaded 시도 (or 시군구) into `built`.
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

/// The unit price lane as the shared driver builds it (root ADR-0615 §7, `kind_versions_build`
/// `UNIT_PRICE_BUILD`).
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

    // The registers are prepared and the prices spooled by the lane's own read; no Silver is a row
    // set of it.
    fn row_sets(&self) -> Vec<RowSet> {
        Vec::new()
    }

    // The registers and their placement, once, nationally (8,081,287 title and 19,902,649 unit rows,
    // root ADR-0617's measurement of 2026-11-24), narrow: a key, two PNUs and two ids per unit. A
    // unit is placed under its building's PNU, which its own row does not name, so no 시도 of the
    // units is placed without every title.
    fn prepare(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        ensure!(suffix.is_empty(), "{WHOLE_RUNS}");
        let registers = read_registers(connection, source)?;
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {READ_COUNTS} (name VARCHAR, breaches BIGINT, judged BIGINT); \
             INSERT INTO {READ_COUNTS} VALUES ('{WITHOUT_PNU}', {}, {});",
            registers.titles_without_pnu + registers.units_without_pnu,
            registers.rows_read
        ))?;
        Ok(registers.read_millis)
    }

    // The batch the driver chose, once, joined to the placement and split by the shared spool; its
    // prices of no placed unit are refused as the placed ones are, and counted.
    fn spool_more(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
        dir_text: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        ensure!(suffix.is_empty(), "{WHOLE_RUNS}");
        let at = Instant::now();
        let batch = input_batch(connection)?;
        // Any id the table holds is read, as the Spark job reads it; only a quote could leave the SQL.
        ensure!(
            !batch.contains('\''),
            "the price batch {batch:?} holds a quote"
        );
        let prices = prices_relation(connection, source)?;
        spool_by_sido(
            connection,
            &fill(
                PRICE_QUERY,
                &[
                    ("prices", &prices),
                    ("batch", &batch),
                    ("unlinked", UNLINKED_BUILDING_ID),
                ],
            ),
            dir_text,
            PRICES,
        )?;
        let read_millis = BTreeMap::from([(PRICE_SOURCE.to_owned(), elapsed(at))]);
        let (unplaced, priced) = if Path::new(dir_text).join(PRICES).is_dir() {
            let every = format!(
                "read_parquet('{dir_text}/{PRICES}/*/*.parquet', hive_partitioning = false)"
            );
            let held = format!("(SELECT * FROM {every} WHERE pnu IS NULL)");
            refuse_prices(connection, &held)?;
            (
                count(
                    connection,
                    &format!("SELECT count(DISTINCT mgm_bldrgst_pk) FROM {held}"),
                )?,
                count(
                    connection,
                    &format!("SELECT count(DISTINCT mgm_bldrgst_pk) FROM {every}"),
                )?,
            )
        } else {
            (0, 0)
        };
        connection.execute_batch(&format!(
            "INSERT INTO {READ_COUNTS} VALUES ('{UNPLACED}', {unplaced}, {priced});"
        ))?;
        Ok(read_millis)
    }

    fn load_more(
        &self,
        connection: &Connection,
        read: &dyn Fn(&str) -> Option<String>,
        suffix: &str,
    ) -> anyhow::Result<u64> {
        ensure!(suffix.is_empty(), "{WHOLE_RUNS}");
        let body = match read(PRICES) {
            Some(held) => {
                refuse_prices(connection, &held)?;
                format!("SELECT * FROM {held}")
            }
            None => NO_PRICES.to_owned(),
        };
        connection.execute_batch(&format!("CREATE OR REPLACE TABLE {PRICES} AS {body};"))?;
        count(connection, &format!("SELECT count(*) FROM {PRICES}"))
    }

    fn pnu_spools(&self) -> Vec<&'static str> {
        vec![PRICES]
    }

    fn result_checks(&self) -> Vec<ResultCheck> {
        vec![ResultCheck {
            name: "every_row_has_a_building_and_a_priced_unit",
            sql: format!("SELECT count(*) FROM built WHERE {UNPRICED_ROW}"),
            blocking: true,
            samples_sql: Some(format!("SELECT pnu FROM built WHERE {UNPRICED_ROW}")),
        }]
    }

    fn national_checks(&self) -> Vec<NationalCheck> {
        vec![
            // `unit_price_panel.assert_price_histories_kept`.
            NationalCheck {
                name: "price_histories_kept",
                blocking: true,
                setup_sql: SHRINK_SETUP_SQL.to_owned(),
                gather_sql: fill(
                    SHRINK_GATHER_SQL,
                    &[
                        (
                            "before",
                            &history_lengths_sql(
                                "SELECT c.units_json FROM current_rows c \
                                 SEMI JOIN appended a USING (pnu, building_id)",
                            ),
                        ),
                        (
                            "after",
                            &history_lengths_sql(
                                "SELECT units_json FROM appended WHERE change <> 'removed'",
                            ),
                        ),
                    ],
                ),
                count_sql: SHRINK_COUNT_SQL.to_owned(),
                bound: Some("max_price_history_shrink_fraction"),
                why:
                    "a price vintage keeps every reference date it ever published (root ADR-0185), \
                      so a vintage that lost records must not replace one that has them",
            },
            NationalCheck {
                name: WITHOUT_PNU,
                blocking: false,
                setup_sql: String::new(),
                gather_sql: String::new(),
                count_sql: format!(
                    "SELECT breaches, judged FROM {READ_COUNTS} WHERE name = '{WITHOUT_PNU}'"
                ),
                bound: None,
                why: "a register row without a PNU places no unit",
            },
            NationalCheck {
                name: UNPLACED,
                blocking: false,
                setup_sql: String::new(),
                gather_sql: String::new(),
                count_sql: format!(
                    "SELECT breaches, judged FROM {READ_COUNTS} WHERE name = '{UNPLACED}'"
                ),
                bound: None,
                why: "a price of a unit no register row places is served under no PNU",
            },
        ]
    }

    fn batch_input(&self) -> Option<BatchInput> {
        Some(BatchInput {
            input: PRICE_SOURCE,
            property: PRICE_BATCH_PROPERTY,
            choose: choose_batch,
        })
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
/// what the check of kept histories compares (`unit_price_panel.history_lengths`).
#[must_use]
pub fn history_lengths_sql(rows: &str) -> String {
    format!(
        "SELECT unit ->> 'id' AS id, \
           CAST(json_array_length(unit -> 'official_price_history') AS BIGINT) AS records \
         FROM (SELECT unnest(from_json(units_json, '[\"JSON\"]')) AS unit FROM ({rows}))"
    )
}

#[cfg(test)]
mod tests;
