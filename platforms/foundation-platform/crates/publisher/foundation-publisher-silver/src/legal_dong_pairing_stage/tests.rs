//! The pairing's files are what the rules read and wrote (root ADR-0559). Reserved 99999 codes and
//! 2099 dates only.

use anyhow::Context as _;
use chrono::NaiveDate;
use lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name;
use serde_json::{json, Value};

use super::{
    export_changes, export_official_links, export_parcels, export_snapshot, pairing_floor,
    stage_append, stage_unit, AppendUnit, APPEND_FILE, CHANGE_TABLE, LINKS_FILE, PARCELS_FILE,
    RECORDED_FILE, REQUESTS_FILE, SNAPSHOT_FILE,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

fn contract(table: &str) -> anyhow::Result<&'static lakehouse_domain::LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no contract {table}"))
}

fn read(path: &std::path::Path) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

#[test]
fn the_floor_is_the_source_contracts() -> anyhow::Result<()> {
    assert!(pairing_floor()? >= NaiveDate::from_ymd_opt(2023, 1, 1).context("day")?);
    Ok(())
}

#[test]
fn the_snapshot_and_the_recorded_changes_are_written_as_the_rules_read_them() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(
        "CREATE TABLE snapshots AS SELECT * FROM (VALUES
           ('9999910100', 'ㄱ동', '존재', '9999900000', '20990101', NULL, DATE '2099-01-02', 'bronze/a'),
           ('9999910200', 'ㄴ동', '폐지', '9999900000', NULL, '20990101', DATE '2099-01-02', 'bronze/a'),
           ('9999910300', 'ㄷ동', '존재', '9999900000', NULL, NULL, DATE '2099-01-01', 'bronze/b')
         ) t(region_cd, full_name, status, parent_cd, created_date, abolished_date, snapshot_date, source_record_id);
         CREATE TABLE changes AS SELECT 'pair|rule|9999910200|9999910100' AS change_key, 'pair' AS kind,
           '9999910200' AS old_code, '9999910100' AS new_code, 'dong' AS level, '20990101' AS effective_date,
           'rule' AS source, NULL::VARCHAR AS rule_verdict, NULL::VARCHAR AS detail, 'run-1' AS derivation_run_id,
           to_timestamp(4070908800) AS recorded_at_utc;",
    )?;
    let day = NaiveDate::from_ymd_opt(2099, 1, 2).context("day")?;
    assert_eq!(
        export_snapshot(&stage, "snapshots", day, "bronze/a", directory.path())?,
        2
    );
    let snapshot = read(&directory.path().join(SNAPSHOT_FILE))?;
    assert_eq!(snapshot[1]["region_cd"], "9999910200");
    assert_eq!(snapshot[0]["abolished_date"], Value::Null);
    assert_eq!(snapshot[0].as_object().map(serde_json::Map::len), Some(6));

    let changes = contract(CHANGE_TABLE)?;
    assert_eq!(
        export_changes(
            &stage,
            changes,
            Some("changes"),
            directory.path(),
            RECORDED_FILE
        )?,
        1
    );
    let recorded = read(&directory.path().join(RECORDED_FILE))?;
    assert_eq!(recorded[0]["recorded_at_utc"], "2099-01-01T00:00:00Z");
    assert_eq!(recorded[0]["detail"], Value::Null);
    // A table nothing was written to holds no change.
    assert_eq!(
        export_changes(&stage, changes, None, directory.path(), RECORDED_FILE)?,
        0
    );
    assert_eq!(read(&directory.path().join(RECORDED_FILE))?, json!([]));
    Ok(())
}

/// Root ADR-0318 §8: the history is events. A load appends only the events the table does not hold,
/// so a link an earlier file appended is read like one the newest file did; a quarantined or earlier
/// one never, and a repeated one once.
#[test]
fn the_official_links_are_every_files_dated_in_the_window() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(
        "CREATE TABLE history AS SELECT * FROM (VALUES
           ('9999910100100010000', '9999910200100010000', DATE '2099-02-01', NULL, '99', 'f-1@2099-02-01'),
           ('9999910100100010000', '9999910200100010000', DATE '2099-02-01', NULL, '99', 'f-1@2099-02-01'),
           ('9999910100100020000', '9999910200100020000', DATE '2098-12-31', NULL, '99', 'f-1@2099-02-01'),
           ('9999910100100030000', '9999910200100030000', NULL, 'unparseable_land_mov_ymd', '99', 'f-1@2099-02-01'),
           ('9999910100100040000', '9999910200100040000', DATE '2099-02-02', NULL, '99', 'f-1@2099-03-01')
         ) t(old_pnu, new_pnu, changed_on, quarantine_reason, source_region_code, source_snapshot_id);",
    )?;
    let floor = NaiveDate::from_ymd_opt(2099, 1, 1).context("day")?;
    assert_eq!(
        export_official_links(&stage, "history", floor, directory.path())?,
        2
    );
    // The link the first file appended and the one the next month's file added: both.
    assert_eq!(
        std::fs::read_to_string(directory.path().join(LINKS_FILE))?,
        "9999910100100010000 9999910200100010000 20990201
9999910100100040000 9999910200100040000 20990202
"
    );
    Ok(())
}

#[test]
fn each_parcel_request_is_answered_and_an_edition_not_loaded_is_named() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(
        "CREATE TABLE parcels AS SELECT * FROM (VALUES
           ('9999910100100010000', '1대', '\\x01'::BLOB, 'edition-1'),
           ('9999910100100020000', '2전', '\\x02'::BLOB, 'edition-1'),
           ('9999910100100030000', '3전', NULL, 'edition-1'),
           ('9999910900100010000', '1대', '\\x03'::BLOB, 'edition-1'),
           ('9999910200100010000', '1대', '\\x04'::BLOB, 'edition-2'),
           ('9999910200100090000', '9대', '\\x05'::BLOB, 'edition-2'),
           ('9999910300100020000', '2전', '\\x06'::BLOB, 'edition-2')
         ) t(pnu, jibun, geometry_wkb, source_snapshot_id);",
    )?;
    std::fs::write(
        directory.path().join(REQUESTS_FILE),
        json!([
            {"edition": "209901", "snapshot_id": "edition-1", "codes": ["9999910100"], "key": [], "lots_of": null},
            {"edition": "209902", "snapshot_id": "edition-2", "codes": ["9999910200", "9999910300"], "key": [], "lots_of": 0},
            {"edition": "209903", "snapshot_id": "edition-3", "codes": ["9999910200"], "key": [], "lots_of": null},
            {"edition": "209902", "snapshot_id": "edition-2", "codes": [], "key": [], "lots_of": null},
            {"edition": "209902", "snapshot_id": "edition-2", "codes": ["9999910200"], "key": [], "lots_of": 2}
        ])
        .to_string(),
    )?;
    let answers = export_parcels(&stage, "parcels", directory.path())?;
    let shape = answers
        .iter()
        .map(|answer| (answer.present, answer.file.is_some(), answer.rows))
        .collect::<Vec<_>>();
    assert_eq!(
        shape,
        [
            // The two parcels with a boundary under the code.
            (true, true, 2),
            // Only the 지번 the earlier edition held: lot 9 is left out.
            (true, true, 2),
            // Not loaded: named, not "no parcels".
            (false, false, 0),
            (true, false, 0),
            // Bound by an edition that is not loaded: no 지번 is asked for.
            (true, false, 0),
        ]
    );
    let first = std::fs::read_to_string(
        directory
            .path()
            .join(answers[0].file.as_deref().context("file")?),
    )?;
    let row: Value = serde_json::from_str(first.lines().next().context("line")?)?;
    assert_eq!(
        row,
        json!({"pnu": "9999910100100010000", "jibun": "1대", "wkb": "AQ=="})
    );
    assert_eq!(
        read(&directory.path().join(PARCELS_FILE))?[2],
        json!({"present": false, "file": null, "rows": 0})
    );
    Ok(())
}

#[test]
fn the_rows_to_append_are_staged_by_their_load_unit_in_order() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let changes = contract(CHANGE_TABLE)?;
    let line = |seq: u32, unit: &str, what: &str, old: &str| {
        json!({"change_key": format!("pair|x|{old}|9999910100"), "kind": "pair", "old_code": old, "new_code": "9999910100",
               "level": "dong", "effective_date": "20990101", "source": "rule", "rule_verdict": null, "detail": null,
               "derivation_run_id": unit, "seq": seq, "recorded_at_us": 4_070_908_800_000_000_i64, "what": what})
        .to_string()
    };
    std::fs::write(directory.path().join(APPEND_FILE), "")?;
    assert!(stage_append(&stage, changes, directory.path())?.is_empty());
    std::fs::write(
        directory.path().join(APPEND_FILE),
        [
            line(0, "steward:b", "steward", "9999910200"),
            line(1, "run-2", "change", "9999910300"),
            line(2, "run-2", "change", "9999910400"),
        ]
        .join("\n"),
    )?;
    let units = stage_append(&stage, changes, directory.path())?;
    let unit = |unit: &str, steward, rows| AppendUnit {
        unit: unit.to_owned(),
        steward,
        rows,
    };
    assert_eq!(units, [unit("steward:b", true, 1), unit("run-2", false, 2)]);
    stage_unit(&stage, changes, &units[1])?;
    let staged: Vec<(String, i64)> = stage
        .connection()
        .prepare("SELECT old_code, epoch_us(recorded_at_utc) FROM pair_unit")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        staged,
        [
            ("9999910300".to_owned(), 4_070_908_800_000_000),
            ("9999910400".to_owned(), 4_070_908_800_000_000)
        ]
    );
    // One unit holding both kinds is no load the rules wrote.
    std::fs::write(
        directory.path().join(APPEND_FILE),
        [
            line(0, "run-3", "steward", "9999910200"),
            line(1, "run-3", "change", "9999910300"),
        ]
        .join("\n"),
    )?;
    assert!(stage_append(&stage, changes, directory.path()).is_err());
    Ok(())
}
