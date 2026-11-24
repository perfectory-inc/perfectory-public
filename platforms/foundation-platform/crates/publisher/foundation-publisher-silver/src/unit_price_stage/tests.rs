//! The unit price rules on synthetic registers (PNUs under `99999`), read as the shared driver reads
//! them (`kind_versions_stage::read_for_test`): placement under the building's PNU with evidence,
//! the nil building without, one batch read, newest reference date first, units by id, an unplaced
//! price counted, the refusals, the digest and the verdict of kept histories.

use anyhow::Context;
use duckdb::Connection;
use serde_json::{json, Value};

use super::*;
use crate::kind_versions_stage::{leave_input_batch, national_verdict, read_for_test};

const BUILDING_PNU: &str = "9999910100100010000";
const UNIT_PNU: &str = "9999910100100020000";
const EDITION: &str = "synthetic__parcel-209906";
const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BATCH: &str = "price:1|exclusive:2|vintage:209910";

fn load(connection: &Connection, prices: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        r"
CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB,
  source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP);
INSERT INTO silver_parcel_boundaries VALUES
  ('{BUILDING_PNU}', NULL, '{EDITION}', NULL), ('{UNIT_PNU}', NULL, '{EDITION}', NULL);
CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR,
  new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR);
CREATE OR REPLACE TABLE silver_building_register_titles (mgm_bldrgst_pk VARCHAR, pnu VARCHAR,
  valid_from_utc TIMESTAMP, ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_building_register_titles VALUES
  ('b1', ' {BUILDING_PNU} ', TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL),
  ('b9', NULL, TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_building_register_units (mgm_bldrgst_pk VARCHAR, pnu VARCHAR,
  building_mgm_bldrgst_pk VARCHAR, unit_row_id VARCHAR, building_link_method VARCHAR,
  building_link_source_record_id VARCHAR, building_link_input_sha256 VARCHAR, building_link_reason VARCHAR,
  normalization_application_id VARCHAR, valid_from_utc TIMESTAMP, ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_building_register_units VALUES
  ('u1', '{UNIT_PNU}', 'b1', 'r1', 'parent_key', 'rec', '{SHA}', NULL, NULL, TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL),
  ('u2', '{UNIT_PNU}', 'b1', 'r2', 'parent_key', 'rec', 'short', NULL, NULL, TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL),
  ('u3', '{UNIT_PNU}', NULL, 'r3', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_unit_official_price (mgm_bldrgst_pk VARCHAR, pnu VARCHAR, base_date VARCHAR,
  price_won BIGINT, source_snapshot_id VARCHAR, vintage VARCHAR);
INSERT INTO silver_unit_official_price VALUES {prices};"
    ))?;
    // The batch the driver chose from the ingest log.
    leave_input_batch(connection, BATCH)
}

fn prices() -> String {
    [
        format!("('u1', '{UNIT_PNU}', '20980101', 100, '{BATCH}', '209910')"),
        format!("('u1', '{UNIT_PNU}', '20990101', 200, '{BATCH}', '209910')"),
        format!("('u2', '{UNIT_PNU}', '20990101', 300, '{BATCH}', '209910')"),
        format!("('u3', '{UNIT_PNU}', '20990101', 400, '{BATCH}', '209910')"),
        // An unplaced unit's price, and another batch's row, which is not read.
        format!("('u9', '{UNIT_PNU}', '20990101', 500, '{BATCH}', '209910')"),
        format!(
            "('u1', '{UNIT_PNU}', '20990101', 999, 'price:1|exclusive:2|vintage:209909', '209909')"
        ),
    ]
    .join(", ")
}

/// The read's counts of what it dropped: `(breaches, judged)` of a reporting check.
fn dropped(connection: &Connection, name: &str) -> anyhow::Result<(i64, i64)> {
    Ok(connection.query_row(
        &format!("SELECT breaches, judged FROM {READ_COUNTS} WHERE name = '{name}'"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, String, Value)>> {
    let rows: Vec<(String, String, String)> = connection
        .prepare("SELECT pnu, building_id, units_json FROM built ORDER BY pnu, building_id")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    rows.into_iter()
        .map(|(pnu, building, units)| Ok((pnu, building, serde_json::from_str(&units)?)))
        .collect()
}

fn unit_id(connection: &Connection, key: &str) -> anyhow::Result<String> {
    Ok(connection.query_row(
        &format!(
            "SELECT {} FROM (SELECT {} AS d)",
            catalog_id_sql("d"),
            catalog_digest_sql("building_unit", &format!("'{key}'"))
        ),
        [],
        |row| row.get(0),
    )?)
}

#[test]
fn units_are_served_under_their_building_with_newest_prices_first() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &prices())?;
    let read = read_for_test(&UNIT_PRICE, &connection, EDITION, "")?;
    assert_eq!((read.served_parcels, read.rows), (2, 4));
    // b9 has no PNU (of five register rows); u9's price places no unit (of four priced units).
    assert_eq!(dropped(&connection, WITHOUT_PNU)?, (1, 5));
    assert_eq!(dropped(&connection, UNPLACED)?, (1, 4));
    let placed: i64 = connection.query_row("SELECT count(*) FROM placed", [], |row| row.get(0))?;
    assert_eq!(placed, 3);
    build(&connection, "999990000000000001", "2099-03-01T00:00:00Z")?;
    let rows = built(&connection)?;
    let building: String = connection.query_row(
        "SELECT id FROM titles WHERE mgm_bldrgst_pk = 'b1'",
        [],
        |row| row.get(0),
    )?;
    // u1 hangs off b1 (evidence); u2's evidence is incomplete and u3 has no building: their own PNU.
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].0.as_str(), rows[0].1.as_str()),
        (BUILDING_PNU, building.as_str())
    );
    assert_eq!(
        rows[0].2,
        json!([{"id": unit_id(&connection, "u1")?, "official_price_history": [
            {"base_date": "20990101", "price_won": 200}, {"base_date": "20980101", "price_won": 100}]}])
    );
    assert_eq!(
        (rows[1].0.as_str(), rows[1].1.as_str()),
        (UNIT_PNU, UNLINKED_BUILDING_ID)
    );
    let mut ids = [unit_id(&connection, "u2")?, unit_id(&connection, "u3")?];
    ids.sort();
    let listed: Vec<&str> = rows[1]
        .2
        .as_array()
        .context("units")?
        .iter()
        .filter_map(|unit| unit["id"].as_str())
        .collect();
    assert_eq!(listed, ids);
    let lengths: Vec<(String, i64)> = connection
        .prepare(&format!(
            "{} ORDER BY records",
            history_lengths_sql("SELECT * FROM built")
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        lengths
            .iter()
            .map(|(_, records)| *records)
            .collect::<Vec<_>>(),
        [1, 1, 2]
    );
    // Every row names a building and holds a priced unit.
    for check in UNIT_PRICE.result_checks() {
        let breaches: i64 = connection.query_row(&check.sql, [], |row| row.get(0))?;
        assert_eq!(breaches, 0, "{}", check.name);
    }
    Ok(())
}

#[test]
fn a_register_row_under_a_renumbered_code_is_served_under_the_served_one() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &prices())?;
    // The registers still carry the 동's old code; the served edition holds the new one, and the
    // code change table pairs them.
    connection.execute_batch(
        "UPDATE silver_building_register_titles SET pnu = '9999910200' || substr(trim(pnu), 11) \
           WHERE pnu IS NOT NULL; \
         UPDATE silver_building_register_units SET pnu = '9999910200' || substr(pnu, 11); \
         INSERT INTO reference_legal_dong_code_change VALUES \
           ('pair', '9999910200', '9999910100', 'eupmyeondong', '2099-01-01', 'official:x');",
    )?;
    read_for_test(&UNIT_PRICE, &connection, EDITION, "")?;
    // The shared build, a 시군구 at a time, as the driver runs it.
    crate::kind_versions_parts::build_in_parts(
        &UNIT_PRICE,
        &connection,
        true,
        "999990000000000001",
        "2099-03-01T00:00:00Z",
    )?;
    let pnus: Vec<String> = built(&connection)?.into_iter().map(|row| row.0).collect();
    assert_eq!(pnus, [BUILDING_PNU, UNIT_PNU], "the rows kept the old code");
    Ok(())
}

#[test]
fn a_repeated_or_invalid_price_is_refused_placed_or_not() -> anyhow::Result<()> {
    for extra in [
        format!("('u1', '{UNIT_PNU}', '20990101', 7, '{BATCH}', '209910')"),
        format!("('u9', '{UNIT_PNU}', '2099-01', 7, '{BATCH}', '209910')"),
        format!("('u3', '{UNIT_PNU}', '20970101', -1, '{BATCH}', '209910')"),
        format!("(NULL, '{UNIT_PNU}', '20970101', 1, '{BATCH}', '209910')"),
    ] {
        let connection = Connection::open_in_memory()?;
        load(&connection, &format!("{}, {extra}", prices()))?;
        let refused = read_for_test(&UNIT_PRICE, &connection, EDITION, "");
        assert!(refused.is_err(), "{extra} was taken");
    }
    Ok(())
}

#[test]
fn a_repeated_register_key_and_a_pnu_off_the_grammar_are_refused() -> anyhow::Result<()> {
    for extra in [
        "INSERT INTO silver_building_register_titles VALUES ('b2', '9999910100100010000', \
         TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL), ('b2', '9999910100100030000', \
         TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL);",
        "INSERT INTO silver_building_register_titles VALUES ('b3', '99999101001', \
         TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL);",
        "INSERT INTO silver_building_register_titles VALUES ('', '9999910100100010000', \
         TIMESTAMP '2099-01-01', TIMESTAMP '2099-01-01', NULL);",
    ] {
        let connection = Connection::open_in_memory()?;
        load(&connection, &prices())?;
        connection.execute_batch(extra)?;
        // The registers read under the shared renumbering, which the spool leaves; none here.
        connection.execute_batch(
            "CREATE OR REPLACE TEMP TABLE renumbering (src VARCHAR, dst VARCHAR);",
        )?;
        assert!(
            read_registers(&connection, &Source::Local).is_err(),
            "{extra}"
        );
    }
    Ok(())
}

/// The batch is the newest vintage's of those the table holds, and only a quote is refused from it.
#[test]
fn the_driver_s_batch_is_the_one_read() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &prices())?;
    let log = IngestCommitLog::default();
    let chosen = (UNIT_PRICE.batch_input().context("a batch input")?.choose)(
        &connection,
        &Source::Local,
        &log,
    )?;
    assert_eq!(chosen, BATCH);
    leave_input_batch(&connection, "it's")?;
    assert!(read_for_test(&UNIT_PRICE, &connection, EDITION, "").is_err());
    Ok(())
}

/// ADR-0564's second revision of 2026-11-22: the verdict does not compare `row_digest`, so each
/// port shows here that every content column of the replaced table moves it.
#[test]
fn every_content_column_moves_the_row_digest() -> anyhow::Result<()> {
    let contract = lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == "gold.unit_price")
        .context("gold.unit_price has a contract")?;
    let content: Vec<&str> = contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !["row_digest", "source_snapshot_id", "published_at_utc"].contains(name))
        .collect();
    assert_eq!(content, ["pnu", "building_id", "units_json"]);
    let connection = Connection::open_in_memory()?;
    let digest = |pnu: &str, building: &str, units: &str| -> anyhow::Result<String> {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE prices AS SELECT 'u' AS mgm_bldrgst_pk, '20990101' AS base_date, \
               CAST({units} AS BIGINT) AS price_won, '{pnu}' AS pnu, '{building}' AS building_id, \
               'unit' AS unit_id; \
             CREATE OR REPLACE TABLE built (pnu VARCHAR, building_id VARCHAR, units_json VARCHAR, \
               row_digest VARCHAR, source_snapshot_id VARCHAR, published_at_utc VARCHAR);"
        ))?;
        build(&connection, "999990000000000001", "2099-03-01T00:00:00Z")?;
        Ok(connection.query_row("SELECT row_digest FROM built", [], |row| row.get(0))?)
    };
    let base = digest(BUILDING_PNU, "b", "1")?;
    for moved in [
        digest(UNIT_PNU, "b", "1")?,
        digest(BUILDING_PNU, "c", "1")?,
        digest(BUILDING_PNU, "b", "2")?,
    ] {
        assert_ne!(moved, base);
    }
    Ok(())
}

/// `test_unit_price_panel`'s shrink cases: none shrunk, within the bound, over it; and the check's
/// gathering over a 시도's diff tables, as the driver runs it.
#[test]
fn a_build_that_shrinks_many_histories_is_refused() -> anyhow::Result<()> {
    let checks = UNIT_PRICE.national_checks();
    let kept = checks
        .iter()
        .find(|check| check.name == "price_histories_kept")
        .context("the check of kept histories")?;
    assert!(kept.blocking);
    assert_eq!(national_verdict(kept, 0, 1_000, Some(0.001)), None);
    assert_eq!(national_verdict(kept, 1, 1_000, Some(0.001)), None);
    assert!(national_verdict(kept, 2, 1_000, Some(0.001))
        .is_some_and(|why| why.contains("2 of 1000 break price_histories_kept")));

    // A unit that kept one of its two records, in a 시도 whose head held it.
    let connection = Connection::open_in_memory()?;
    let units =
        |records: &str| format!(r#"[{{"id": "x", "official_price_history": [{records}]}}]"#);
    let two = units(r#"{"base_date": "20990101"}, {"base_date": "20980101"}"#);
    let one = units(r#"{"base_date": "20990101"}"#);
    connection.execute_batch(&format!(
        "CREATE TABLE current_rows AS SELECT '{BUILDING_PNU}' AS pnu, 'b' AS building_id, \
           '{two}' AS units_json; \
         CREATE TABLE appended AS SELECT '{BUILDING_PNU}' AS pnu, 'b' AS building_id, \
           '{one}' AS units_json, 'changed' AS change; {} {}",
        kept.setup_sql, kept.gather_sql
    ))?;
    let (breaches, of): (i64, i64) =
        connection.query_row(&kept.count_sql, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
    assert_eq!((breaches, of), (1, 1));
    Ok(())
}

#[test]
fn the_code_version_is_stable() {
    assert_eq!(code_version(), code_version());
    assert_eq!(code_version().len(), 64);
}
