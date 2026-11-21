//! The energy rules on synthetic inputs (PNUs under `99999`): a month of one energy only, the
//! `decimal(38,3)` text Spark writes, the twelve-month bound, unreadable rows, the digest, and the
//! partial diff. The national judgement against the Spark job is its takeover verdict.

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::*;

const EDITION: &str = "synthetic__parcel-209906";
const PNU: &str = "9999910100100010000";

fn usage(key: &str, month: &str, amount: &str) -> String {
    let amount = if amount == "NULL" {
        "NULL".to_owned()
    } else {
        format!("'{amount}'")
    };
    format!("('{key}', '{PNU}', '{month}', {amount}, '202610', TIMESTAMP '2099-01-02', NULL)")
}

fn load(connection: &Connection, electricity: &[String], gas: &[String]) -> anyhow::Result<()> {
    let table = |name: &str, rows: &[String]| {
        let insert = if rows.is_empty() {
            String::new()
        } else {
            format!("INSERT INTO {name} VALUES {};", rows.join(", "))
        };
        format!(
            "CREATE OR REPLACE TABLE {name} (mgmt_key VARCHAR, pnu VARCHAR, use_ym VARCHAR, \
               usage_amount VARCHAR, vintage VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); {insert}"
        )
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, \
           source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
         INSERT INTO silver_parcel_boundaries VALUES ('{PNU}', NULL, '{EDITION}', NULL); \
         CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         {} {}",
        table("silver_building_energy_monthly_electricity", electricity),
        table("silver_building_energy_monthly_gas", gas)
    ))?;
    Ok(())
}

fn document(connection: &Connection) -> anyhow::Result<(String, String)> {
    Ok(
        connection.query_row("SELECT energy_json, row_digest FROM built", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?,
    )
}

#[test]
fn a_document_is_spark_s_bytes_with_the_decimal_scale_written() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(
        &connection,
        &[
            usage("b1", "202501", " 12.5 "),
            usage("b2", "202501", "7"),
            // Not a number: counted as a row and as unreadable, not summed.
            usage("b3", "202501", "n/a"),
            usage("b1", "202412", "1.2345"),
        ],
        &[usage("b1", "202412", "3")],
    )?;
    ENERGY.load_inputs(&connection, &Source::Local, EDITION, "")?;
    ENERGY.build(
        &connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    let (json, digest) = document(&connection)?;
    // 2025-01 first; its 가스 is absent (null, 0 rows); 1.2345 is held at three places as Spark's
    // decimal(38,3) cast holds it.
    let expected = "{\"months\":[\
        {\"use_ym\":\"202501\",\"electricity_usage\":19.500,\"electricity_rows\":3,\"gas_usage\":null,\"gas_rows\":0},\
        {\"use_ym\":\"202412\",\"electricity_usage\":1.235,\"electricity_rows\":1,\"gas_usage\":3.000,\"gas_rows\":1}],\
        \"electricity_unit\":\"kWh\",\"latest_use_ym\":\"202501\",\"unparsed_usage_rows\":1}";
    assert_eq!(json, expected);
    let content = format!(
        "{{\"pnu\":\"{PNU}\",\"energy_json\":{}}}",
        serde_json::to_string(expected)?
    );
    assert_eq!(digest, format!("{:x}", Sha256::digest(content.as_bytes())));
    Ok(())
}

#[test]
fn a_document_keeps_the_newest_twelve_months() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let months: Vec<String> = (1..=14)
        .map(|n| usage("b1", &format!("2024{n:02}"), "1"))
        .collect();
    // Months 13 and 14 are not months, but they sort as text like the provider's do.
    load(&connection, &months, &[])?;
    ENERGY.load_inputs(&connection, &Source::Local, EDITION, "")?;
    ENERGY.build(
        &connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    let (json, _) = document(&connection)?;
    let value: serde_json::Value = serde_json::from_str(&json)?;
    let kept = value["months"].as_array().cloned().unwrap_or_default();
    assert_eq!(kept.len(), MONTH_BOUND as usize);
    assert_eq!(kept[0]["use_ym"], "202414");
    assert_eq!(kept[11]["use_ym"], "202403");
    Ok(())
}

#[test]
fn a_partial_run_names_the_pnus_whose_rows_changed() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let rows = [usage("b1", "202501", "1")];
    load(&connection, &rows, &[])?;
    ENERGY.load_inputs(&connection, &Source::Local, EDITION, "_old")?;
    ENERGY.load_inputs(&connection, &Source::Local, EDITION, "")?;
    assert_eq!(ENERGY.changed_pnus(&connection)?, 0);
    // The same row twice is another month total: a change though the set of rows is the same.
    load(
        &connection,
        &[usage("b1", "202501", "1"), usage("b2", "202501", "1")],
        &[],
    )?;
    ENERGY.load_inputs(&connection, &Source::Local, EDITION, "")?;
    assert_eq!(ENERGY.changed_pnus(&connection)?, 1);
    Ok(())
}
