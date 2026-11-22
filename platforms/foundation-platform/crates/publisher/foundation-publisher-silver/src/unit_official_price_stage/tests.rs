//! The join's rules on synthetic registers (PNUs under `99999`): a key's confirmed notice, the
//! prices the Spark job's validity test drops, the units as of the vintage, the conflict refusal,
//! the PNU test, the 시도 scope and the batch's stamps.

use duckdb::Connection;

use super::*;

const PNU_A: &str = "9999910100100010000";
const PNU_B: &str = "9999910100100020000";

fn pins() -> BatchPins {
    BatchPins {
        price: 11,
        exclusive: 22,
        vintage: "209911".to_owned(),
    }
}

/// The exclusive units' versions (the columns the as-of read needs) and the vintage's prices.
fn inputs(connection: &Connection, units: &str, prices: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        r"
CREATE OR REPLACE TABLE units (mgmt_key VARCHAR, pnu VARCHAR, dong_name VARCHAR, ho_name VARCHAR,
  vintage VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO units VALUES {units};
CREATE OR REPLACE TABLE price_source (mgmt_key VARCHAR, base_date VARCHAR, notice_date VARCHAR,
  price_won VARCHAR);
INSERT INTO price_source VALUES {prices};"
    ))?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE exclusive_source AS {}",
        units_as_of_sql("units", &pins().vintage)?
    ))?;
    Ok(())
}

fn rows(connection: &Connection) -> anyhow::Result<Vec<(String, String, String, String, i64)>> {
    let mut statement = connection.prepare(
        "SELECT mgm_bldrgst_pk, pnu, ho_name, base_date, price_won FROM unit_prices \
         ORDER BY mgm_bldrgst_pk, base_date",
    )?;
    let found = statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(found)
}

#[test]
fn a_batch_id_names_its_pins_and_nothing_else_is_one() -> anyhow::Result<()> {
    let batch = "price:11|exclusive:22|vintage:209911";
    assert_eq!(BatchPins::parse(batch)?, pins());
    assert_eq!(pins().batch(), batch);
    for bad in [
        "price:11|exclusive:22",
        "price:x|exclusive:22|vintage:209911",
        "price:11|exclusive:22|vintage:20991",
        "hubgokr:209911",
        "price:99999999999999999999|exclusive:22|vintage:209911",
    ] {
        assert!(BatchPins::parse(bad).is_err(), "{bad} was taken");
    }
    Ok(())
}

/// A key's correction notices resolve to the newest notice, then the higher price; a price the
/// validity test refuses is not read, and a key no unit holds joins to nothing.
#[test]
fn a_key_takes_its_newest_notice_and_invalid_prices_are_dropped() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let unit = |key: &str, ho: &str| {
        format!("('{key}', '{PNU_A}', '101', '{ho}', '209911', TIMESTAMP '2099-11-01', NULL)")
    };
    inputs(
        &connection,
        &[unit("k1", "1001"), unit("k2", "1002")].join(", "),
        &[
            // k1 on one date: three notices; the newest wins, a tie by the higher price.
            "('k1', '20990101', '20990110', '100')",
            "('k1', '20990101', '20990120', '200')",
            "('k1', '20990101', '20990120', '150')",
            // Trimmed of spaces as Spark trims them.
            "('k1', ' 20980101 ', NULL, ' 90 ')",
            // Refused: a date that is not eight digits, a year below 1000, a price that is not
            // digits, a blank key.
            "('k2', '2099011', '20990110', '100')",
            "('k2', '09990101', '20990110', '100')",
            "('k2', '20990101', '20990110', '1e3')",
            "(' ', '20990101', '20990110', '100')",
            "('k2', '20990301', NULL, '300')",
            // No unit holds k9.
            "('k9', '20990101', '20990110', '100')",
        ]
        .join(", "),
    )?;
    let composed = compose(&connection, &pins(), None)?;
    assert_eq!(composed, Composed { rows: 3, units: 2 });
    let pnu = PNU_A.to_owned();
    assert_eq!(
        rows(&connection)?,
        vec![
            (
                "k1".into(),
                pnu.clone(),
                "1001".into(),
                "20980101".into(),
                90
            ),
            (
                "k1".into(),
                pnu.clone(),
                "1001".into(),
                "20990101".into(),
                200
            ),
            ("k2".into(), pnu, "1002".into(), "20990301".into(), 300),
        ]
    );
    Ok(())
}

/// The units as they stood at the vintage: a later vintage's version is not read, a newer version
/// at or before it wins, a removal marker drops the unit, and a PNU that is not 19 digits joins to
/// nothing.
#[test]
fn units_are_read_as_of_the_vintage() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    inputs(
        &connection,
        &[
            format!("('k1', '{PNU_A}', '101', '1001', '209910', TIMESTAMP '2099-10-01', NULL)"),
            format!("('k1', '{PNU_B}', '101', '1001', '209911', TIMESTAMP '2099-11-01', NULL)"),
            format!("('k1', '{PNU_A}', '101', '9999', '209912', TIMESTAMP '2099-12-01', NULL)"),
            format!("('k2', '{PNU_A}', '101', '1002', '209910', TIMESTAMP '2099-10-01', NULL)"),
            format!(
                "('k2', '{PNU_A}', '101', '1002', '209911', TIMESTAMP '2099-11-01', 'removed')"
            ),
            "('k3', '99999', NULL, NULL, '209911', TIMESTAMP '2099-11-01', NULL)".to_owned(),
        ]
        .join(", "),
        "('k1', '20990101', '20990110', '100'), ('k2', '20990101', '20990110', '100'), \
         ('k3', '20990101', '20990110', '100')",
    )?;
    let composed = compose(&connection, &pins(), None)?;
    // k1 and k3 are units of the vintage (k3's PNU is no parcel); k2 was removed in it.
    assert_eq!(composed, Composed { rows: 1, units: 2 });
    assert_eq!(
        rows(&connection)?,
        vec![(
            "k1".into(),
            PNU_B.into(),
            "1001".into(),
            "20990101".into(),
            100
        )]
    );
    Ok(())
}

#[test]
fn a_key_that_names_two_units_is_refused() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    // Kept ties: two rows of one version that disagree on the unit.
    inputs(
        &connection,
        &format!(
            "('k1', '{PNU_A}', '101', '1001', '209911', TIMESTAMP '2099-11-01', NULL), \
             ('k1', '{PNU_A}', '101', '1002', '209911', TIMESTAMP '2099-11-01', NULL)"
        ),
        "('k1', '20990101', '20990110', '100')",
    )?;
    let refused = compose(&connection, &pins(), None).expect_err("a conflicting key was joined");
    assert!(refused.to_string().contains("conflicting=1"), "{refused:#}");
    Ok(())
}

/// The rows carry the batch, the vintage and one load identity per 시도; a scope keeps one 시도's
/// units, and a batch that joins no row is refused.
#[test]
fn rows_carry_the_batch_and_a_scope_keeps_one_sido() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    inputs(
        &connection,
        &format!("('k1', '{PNU_A}', NULL, NULL, '209911', TIMESTAMP '2099-11-01', NULL)"),
        "('k1', '20990101', '20990110', '100')",
    )?;
    assert!(sido_scope("99").is_err(), "99 has no next code");
    assert!(sido_scope("4").is_err());
    let gyeonggi = sido_scope("41")?;
    assert_eq!(gyeonggi, "(pnu >= '41' AND pnu < '42')");
    // Every synthetic PNU is 시도 99, which `sido_scope` cannot bound: its range is written here.
    let all_of_99 = "(pnu >= '99' AND pnu < '9:')";
    assert_eq!(compose(&connection, &pins(), Some(all_of_99))?.rows, 1);
    let stamps: (String, String, String, String, String) = connection.query_row(
        "SELECT dong_name, sido, source_snapshot_id, vintage, source_record_id FROM unit_prices",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    let batch = pins().batch();
    assert_eq!(
        stamps,
        (
            String::new(),
            "99".to_owned(),
            batch.clone(),
            "209911".to_owned(),
            format!("unit-official-price/base-date-v2/99/{batch}")
        )
    );
    let none = compose(&connection, &pins(), Some(gyeonggi.as_str()))
        .expect_err("an empty batch was taken");
    assert!(none.to_string().contains("no price rows"), "{none:#}");
    Ok(())
}
