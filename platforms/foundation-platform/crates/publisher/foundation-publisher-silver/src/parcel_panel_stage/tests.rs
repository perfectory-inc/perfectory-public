//! The parcel panel's rules on synthetic inputs (PNUs under `99999`): each section as the Spark job
//! folds it, the current rows chosen per 시도 (an older release and a removal marker), a renumbered
//! code on the served parcel, an unserved PNU left out, the refusals, the partial diff, and a digest
//! every content column moves. The national judgement against the Spark job is its takeover verdict.

use duckdb::Connection;
use serde_json::{json, Value};

use super::*;
use crate::kind_versions_stage::{ensure_no_commit_tables, read_for_test, KindVersionsStage};

const EDITION: &str = "synthetic__parcel-209906";
const P1: &str = "9999910100100010000";
const P2: &str = "9999910100100020000";
/// The served parcel the code change table renumbers `9999910200…` into.
const P3: &str = "9999910300100010000";
const OLD_CODE_PNU: &str = "9999910200100010000";
const P4: &str = "9999910100100040000";
const UNSERVED: &str = "9999910100100030000";

/// Every input of the panel, as local tables (`Source::Local`), with `extra` statements after.
fn load(connection: &Connection, extra: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        r"
CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, source_snapshot_id VARCHAR,
  valid_to_utc TIMESTAMP);
INSERT INTO silver_parcel_boundaries VALUES ('{P1}', NULL, '{EDITION}', NULL), ('{P2}', NULL, '{EDITION}', NULL),
  ('{P3}', NULL, '{EDITION}', NULL), ('{P4}', NULL, '{EDITION}', NULL),
  ('{UNSERVED}', NULL, 'synthetic__parcel-209903', NULL);
CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, new_code VARCHAR,
  level VARCHAR, effective_date VARCHAR, source VARCHAR);
INSERT INTO reference_legal_dong_code_change VALUES
  ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x');
CREATE OR REPLACE TABLE silver_land_use_zone_code (ucode VARCHAR, parent_ucode VARCHAR,
  ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_land_use_zone_code VALUES ('UQA100', '000000', TIMESTAMP '2099-01-01', NULL),
  ('UQA110', ' UQA100 ', TIMESTAMP '2099-01-01', NULL), ('UQB001', NULL, TIMESTAMP '2099-01-01', NULL),
  ('UQA001', NULL, TIMESTAMP '2099-01-01', NULL), ('UQZ999', '000000', TIMESTAMP '2099-01-01', NULL),
  ('UQC110', 'UQC001', TIMESTAMP '2099-01-01', NULL), ('UQC001', NULL, TIMESTAMP '2099-01-01', NULL),
  ('UQD001', NULL, TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_land_use_plan (pnu VARCHAR, inclusion_code VARCHAR, zone_code VARCHAR,
  zone_name VARCHAR, source_snapshot_id VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_land_use_plan VALUES
  ('{P1}', '1', 'UQA110', 'superseded', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', '2', 'UQA110', ' 제2종 ', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{P1}', '1', 'UQA110', ' 제2종 ', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{P1}', '1', 'UQB001', 'B', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{P1}', '1', 'UQZ999', 'dies', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{P1}', '3', 'UQA001', 'adjacent', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{P2}', '1', 'UQA001', 'removed later', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P2}', '1', 'UQA001', 'removed later', 's2', TIMESTAMP '2099-02-01', 'removed'),
  ('{OLD_CODE_PNU}', '1', 'UQA100', ' ', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{P4}', '1', 'UQC110', 'C', 's2', TIMESTAMP '2099-02-01', NULL),
  ('{UNSERVED}', '1', 'UQA100', 'x', 's2', TIMESTAMP '2099-02-01', NULL);
CREATE OR REPLACE TABLE silver_land_individual_price (pnu VARCHAR, base_year VARCHAR, base_month VARCHAR,
  price_per_m2 VARCHAR, announced_date VARCHAR, source_snapshot_id VARCHAR, ingested_at_utc TIMESTAMP,
  row_state VARCHAR);
INSERT INTO silver_land_individual_price VALUES
  ('{P1}', '2098', '1', '1000', '2098-01-01', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', ' 2099', '01', '900', ' ', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', '2099', '13', '5000', 'x', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', '2099', '12', '1.5', 'x', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P2}', '2099', '1', '5', ' ', 's1', TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_land_characteristic (pnu VARCHAR, land_category VARCHAR, area_m2 DOUBLE,
  land_use_situation VARCHAR, terrain_height VARCHAR, terrain_shape VARCHAR, road_contact VARCHAR,
  source_snapshot_id VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_land_characteristic VALUES
  ('{P1}', '대', 10000000.0, '공업용', NULL, '정방형', '광대', 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P2}', '전', 0.0, NULL, NULL, NULL, NULL, 's1', TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_land_forest_ledger (pnu VARCHAR, land_category VARCHAR, area_m2 DOUBLE,
  ownership_kind_code VARCHAR, ownership_kind_name VARCHAR, co_owner_count INTEGER,
  source_snapshot_id VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_land_forest_ledger VALUES
  ('{P1}', '임', 5.5, '01', '개인', 2, 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P2}', '임', 3.0, '01', '개인', -1, 's1', TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_land_transfer_history (pnu VARCHAR, transfer_history_seq BIGINT,
  parcel_history_seq VARCHAR, reason_code VARCHAR, reason VARCHAR, moved_at VARCHAR, erased_at VARCHAR,
  land_category VARCHAR, area_m2 DOUBLE, closure_seq VARCHAR, source_snapshot_id VARCHAR,
  ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_land_transfer_history VALUES
  ('{P1}', 1, '1', '10', '분할', '2098-01-01', NULL, '대', 12.0, NULL, 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', 2, '1', '20', 'b', '2099-01-01', NULL, '대', 12.0, NULL, 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', 2, ' 1 ', '20', 'a', '2099-01-01', NULL, '대', 12.0, NULL, 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', 3, '1', '30', '합병', NULL, NULL, '대', 12.0, NULL, 's1', TIMESTAMP '2099-01-01', NULL);
CREATE OR REPLACE TABLE silver_land_right_registration (pnu VARCHAR, right_serial_no VARCHAR,
  building_name VARCHAR, dong_name VARCHAR, floor_name VARCHAR, ho_name VARCHAR, room_name VARCHAR,
  right_ratio VARCHAR, closure_kind_name VARCHAR, closure_kind_code VARCHAR, source_snapshot_id VARCHAR,
  ingested_at_utc TIMESTAMP, row_state VARCHAR);
INSERT INTO silver_land_right_registration VALUES
  ('{P1}', 'B', 'x', '101', NULL, NULL, NULL, '1/2', NULL, NULL, 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', 'A', 'x', NULL, '', NULL, NULL, '1/2', NULL, NULL, 's1', TIMESTAMP '2099-01-01', NULL),
  ('{P1}', 'A', NULL, '', NULL, NULL, NULL, '1/2', NULL, NULL, 's1', TIMESTAMP '2099-01-01', NULL);
INSERT INTO silver_land_right_registration
  SELECT '{P2}', lpad(CAST(i AS VARCHAR), 4, '0'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 's1',
    TIMESTAMP '2099-01-01', NULL FROM range(201) t(i);
CREATE OR REPLACE TABLE reference_legal_dong_code_snapshot (region_cd VARCHAR, full_name VARCHAR,
  status VARCHAR, snapshot_date DATE, lowest_name VARCHAR);
INSERT INTO reference_legal_dong_code_snapshot VALUES
  ('9999910100', '옛 이름', '존재', DATE '2099-01-01', '옛'),
  ('9999910100', ' 합성시 합성동 ', '존재', DATE '2099-02-01', '합성동'),
  ('9999910300', '합성시 새동', '존재', DATE '2099-02-01', NULL),
  ('9999910200', '합성시 옛동', '폐지', DATE '2099-02-01', '옛동');
{extra}"
    ))?;
    Ok(())
}

/// The built rows as (pnu, every content column parsed when it is a document).
fn built(connection: &Connection) -> anyhow::Result<BTreeMap<String, Value>> {
    let rows: Vec<(String, String)> = connection
        .prepare("SELECT pnu, to_json(b)::VARCHAR FROM built b ORDER BY pnu")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    rows.into_iter()
        .map(|(pnu, text)| {
            let mut row: Value = serde_json::from_str(&text)?;
            if let Some(fields) = row.as_object_mut() {
                for (name, value) in fields.iter_mut() {
                    if let (true, Some(text)) = (name.ends_with("_json"), value.as_str()) {
                        *value = serde_json::from_str(text)?;
                    }
                }
            }
            Ok((pnu, row))
        })
        .collect()
}

fn build_all(connection: &Connection) -> anyhow::Result<BTreeMap<String, Value>> {
    let read = read_for_test(&PANEL, connection, EDITION, "")?;
    assert_eq!(read.served_parcels, 4);
    PANEL.build(
        connection,
        true,
        "999990000000000001",
        "2099-03-01T00:00:00Z",
    )?;
    ensure_no_commit_tables(connection)?;
    built(connection)
}

#[test]
fn every_section_is_folded_as_the_spark_job_folds_it() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, "")?;
    let rows = build_all(&connection)?;
    // The unserved PNU has no row; the renumbered code's row lands on the served P3.
    assert_eq!(
        rows.keys().map(String::as_str).collect::<Vec<_>>(),
        [P1, P2, P4, P3]
    );
    let p1 = &rows[P1];
    // s2 replaced s1; UQA110 collapses to its lowest inclusion and trimmed name; the walk that dies
    // and the adjacent verdict are dropped; inclusion, then code, orders them.
    assert_eq!(
        p1["zonings_json"],
        json!([
            {"zone_code": "UQA110", "zone_name": "제2종", "anchor_code": "UQA100", "inclusion_code": "1"},
            {"zone_code": "UQB001", "zone_name": "B", "anchor_code": "UQB001", "inclusion_code": "1"}
        ])
    );
    // The newest valid price: month 13 is skipped; the price `1.5` is 1, as Spark's cast reads it.
    assert_eq!(
        p1["price_json"],
        json!({"price_per_m2": 1, "base_year": 2099, "base_month": 12, "announced_date": "x"})
    );
    assert_eq!(p1["characteristics_json"]["area_m2"], json!(10_000_000.0));
    assert_eq!(p1["characteristics_json"]["terrain_height"], Value::Null);
    assert_eq!(
        p1["forest_ledger_json"],
        json!({"land_category": "임", "area_m2": 5.5, "ownership_kind": "01",
               "ownership_kind_name": "개인", "co_owner_count": 2})
    );
    // One entry per (seq, trimmed parcel seq), the smallest reason kept; newest move first, a missing
    // one last.
    let transfers = p1["transfer_history_json"]
        .as_array()
        .context("a list")?
        .iter()
        .map(|entry| (entry["history_seq"].clone(), entry["reason"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        transfers,
        [
            (json!(2), json!("a")),
            (json!(1), json!("분할")),
            (json!(3), json!("합병"))
        ]
    );
    // A blank designation is ''; of A's two rows the one without a building name is kept; A before B.
    let rights = p1["land_rights_json"].as_array().context("a list")?;
    assert_eq!(rights.len(), 2);
    assert_eq!(rights[0]["right_serial_no"], "A");
    assert_eq!(rights[0]["building_name"], Value::Null);
    assert_eq!(rights[0]["dong_name"], "");
    assert_eq!(rights[1]["dong_name"], "101");
    assert_eq!(p1["land_right_total"], 2);
    assert_eq!(
        p1["legal_dong_json"],
        json!({"name": "합성시 합성동", "lowest_name": "합성동"})
    );
    for absent in ["kind", "area_m2", "lineage_json", "attached_via_json"] {
        assert_eq!(p1[absent], Value::Null, "{absent}");
    }

    let p2 = &rows[P2];
    // A blank announcement date is null.
    assert_eq!(
        p2["price_json"],
        json!({"price_per_m2": 5, "base_year": 2099, "base_month": 1, "announced_date": null})
    );
    // Its plan's newest version is a removal marker; its area 0 and negative co-owners are invalid.
    assert_eq!(p2["zonings_json"], json!([]));
    assert_eq!(p2["characteristics_json"], Value::Null);
    assert_eq!(p2["forest_ledger_json"], Value::Null);
    assert_eq!(p2["transfer_history_json"], json!([]));
    // The first 200 of 201 rights, all counted.
    let rights = p2["land_rights_json"].as_array().context("a list")?;
    assert_eq!(rights.len(), 200);
    assert_eq!(rights[0]["right_serial_no"], "0000");
    assert_eq!(p2["land_right_total"], 201);

    let p3 = &rows[P3];
    assert_eq!(
        p3["zonings_json"],
        json!([{"zone_code": "UQA100", "zone_name": null, "anchor_code": "UQA100", "inclusion_code": "1"}])
    );
    assert_eq!(
        p3["legal_dong_json"],
        json!({"name": "합성시 새동", "lowest_name": null})
    );
    Ok(())
}

#[test]
fn two_valid_rows_of_one_parcel_and_a_missing_parcel_sequence_are_refused() -> anyhow::Result<()> {
    for (extra, refusal) in [
        (
            format!(
                "INSERT INTO silver_land_characteristic VALUES ('{P1}', '대', 3.0, NULL, NULL, NULL, \
                 NULL, 's1', TIMESTAMP '2099-01-01', NULL);"
            ),
            "duplicate PNU rows",
        ),
        (
            format!(
                "INSERT INTO silver_land_transfer_history VALUES ('{P1}', 9, ' ', '1', 'x', NULL, NULL, \
                 '대', 1.0, NULL, 's1', TIMESTAMP '2099-01-01', NULL);"
            ),
            "without parcel_history_seq",
        ),
    ] {
        let connection = Connection::open_in_memory()?;
        load(&connection, &extra)?;
        let Err(refused) = build_all(&connection) else {
            anyhow::bail!("{refusal}: taken");
        };
        assert!(format!("{refused:#}").contains(refusal), "{refused:#}");
    }
    Ok(())
}

/// Between two reads, a changed row, a zone code whose anchor moved and a 법정동 whose name moved
/// name their PNUs; the rest is not rebuilt.
#[test]
fn a_partial_run_names_the_parcels_whose_inputs_moved() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, "")?;
    read_for_test(&PANEL, &connection, EDITION, "_old")?;
    load(
        &connection,
        &format!(
            "INSERT INTO silver_land_individual_price VALUES ('{P1}', '2100', '1', '1', NULL, 's3', \
               TIMESTAMP '2099-03-01', NULL); \
             UPDATE silver_land_use_zone_code SET parent_ucode = 'UQD001' WHERE ucode = 'UQC110'; \
             UPDATE reference_legal_dong_code_snapshot SET full_name = '합성시 다른동' \
               WHERE region_cd = '9999910300' AND snapshot_date = DATE '2099-02-01';"
        ),
    )?;
    read_for_test(&PANEL, &connection, EDITION, "")?;
    assert_eq!(PANEL.changed_pnus(&connection)?, 3);
    let named: Vec<String> = connection
        .prepare("SELECT pnu FROM targets ORDER BY pnu")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(named, [P1, P4, P3]);
    PANEL.build(
        &connection,
        false,
        "999990000000000002",
        "2099-03-02T00:00:00Z",
    )?;
    let rows = built(&connection)?;
    assert_eq!(
        rows.keys().map(String::as_str).collect::<Vec<_>>(),
        [P1, P3, P4]
    );
    assert_eq!(rows[P1]["price_json"]["base_year"], 2100);
    assert_eq!(rows[P4]["zonings_json"][0]["anchor_code"], "UQD001");
    assert_eq!(rows[P3]["legal_dong_json"]["name"], "합성시 다른동");
    Ok(())
}

/// The digest is of every content column: changing any one of them changes it, and a stamp does not.
#[test]
fn every_content_column_moves_the_row_digest() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let columns = content_columns();
    let digest = |values: &[String]| -> anyhow::Result<String> {
        let select = columns
            .iter()
            .zip(values)
            .map(|(column, value)| format!("{value} AS {column}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(connection.query_row(
            &format!("SELECT {} FROM (SELECT {select})", row_digest_sql()),
            [],
            |row| row.get(0),
        )?)
    };
    let base: Vec<String> = columns
        .iter()
        .map(|column| match *column {
            "area_m2" | "land_right_total" => "CAST(1 AS BIGINT)".to_owned(),
            other => format!("'{other}'"),
        })
        .collect();
    let first = digest(&base)?;
    assert_eq!(first.len(), 64);
    for (index, column) in columns.iter().enumerate() {
        for changed in ["NULL".to_owned(), "CAST('other' AS VARCHAR)".to_owned()] {
            let mut values = base.clone();
            values[index] =
                if matches!(*column, "area_m2" | "land_right_total") && changed != "NULL" {
                    "CAST(2 AS BIGINT)".to_owned()
                } else {
                    changed.clone()
                };
            assert_ne!(digest(&values)?, first, "{column} = {changed}");
        }
    }
    assert!(!columns.contains(&"source_snapshot_id") && !columns.contains(&"published_at_utc"));
    Ok(())
}

#[test]
fn a_zone_code_walks_up_to_its_anchor_or_dies() {
    let parents: BTreeMap<String, Option<String>> = [
        ("UQA110", Some("UQA100")),
        ("UQA100", None),
        ("UQX1", Some("UQX2")),
        ("UQX2", None),
        ("LOOP1", Some("LOOP2")),
        ("LOOP2", Some("LOOP1")),
    ]
    .into_iter()
    .map(|(code, parent)| (code.to_owned(), parent.map(str::to_owned)))
    .collect();
    assert_eq!(anchor_of("UQA110", &parents).as_deref(), Some("UQA100"));
    assert_eq!(anchor_of("UQA001", &parents).as_deref(), Some("UQA001"));
    assert_eq!(anchor_of("UQX1", &parents), None);
    assert_eq!(anchor_of("LOOP1", &parents), None);
    assert_eq!(anchor_of("UNKNOWN", &parents), None);
}

#[test]
fn the_code_version_moves_with_the_rules() {
    let other = ParcelPanel {
        rules_version: "parcel-panel-duckdb-v0",
    };
    assert_ne!(PANEL.code_version(), other.code_version());
    assert_eq!(PANEL.code_version(), PANEL.code_version());
}
