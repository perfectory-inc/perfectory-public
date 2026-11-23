//! The unit price rules on synthetic registers (PNUs under `99999`): placement under the building's
//! PNU with evidence, the nil building without, one batch read, newest reference date first, units by
//! id, an unplaced price counted, the refusals, the digest and the shrink verdict.

use anyhow::Context;
use duckdb::Connection;
use serde_json::{json, Value};

use super::*;

const BUILDING_PNU: &str = "9999910100100010000";
const UNIT_PNU: &str = "9999910100100020000";
const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BATCH: &str = "price:1|exclusive:2|vintage:209910";

fn load(connection: &Connection, prices: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        r"
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
    Ok(())
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

fn read(connection: &Connection, dir: &Path) -> anyhow::Result<Spooled> {
    let registers = read_registers(connection, &Source::Local)?;
    assert_eq!(registers.titles_without_pnu, 1);
    assert_eq!(registers.placed_units, 3);
    spool_prices(connection, &Source::Local, BATCH, dir)
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
    let dir = tempfile::tempdir()?;
    let spooled = read(&connection, dir.path())?;
    assert_eq!(
        spooled,
        Spooled {
            sidos: ["99".to_owned()].into(),
            unplaced_prices: 1,
            unplaced_price_units: 1,
        }
    );
    assert_eq!(load_region(&connection, dir.path(), "99")?, 4);
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
        let dir = tempfile::tempdir()?;
        let refused =
            read(&connection, dir.path()).and_then(|_| load_region(&connection, dir.path(), "99"));
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
        assert!(
            read_registers(&connection, &Source::Local).is_err(),
            "{extra}"
        );
    }
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

/// `test_unit_price_panel`'s shrink cases: none shrunk, within the bound, over it.
#[test]
fn a_build_that_shrinks_many_histories_is_refused() {
    assert_eq!(shrink_verdict(0, 1_000, 0.001), None);
    assert_eq!(shrink_verdict(1, 1_000, 0.001), None);
    assert!(shrink_verdict(2, 1_000, 0.001)
        .is_some_and(|why| why.contains("2 of the current Gold's 1000 units")));
}

#[test]
fn the_code_version_is_stable() {
    assert_eq!(code_version(), code_version());
    assert_eq!(code_version().len(), 64);
}
