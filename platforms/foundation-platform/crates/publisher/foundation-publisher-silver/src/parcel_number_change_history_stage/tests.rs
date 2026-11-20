//! The 필지고유번호변동연혁 file is read as the Spark job's Python read it (root ADR-0144 §4).
//!
//! `fixtures/history.txt` is a synthetic file (a BOM, a row repeated three times, blank
//! lines, a date that is no date, a 3-digit 부번, a date after the collection);
//! `history.rows.json` is what `vworld_parcel_number_change_history.parse_rows` made of it. The
//! Python parser stays for the collection's handoff; these rows hold the two to one answer.
//! Reserved 99999 codes and 2099 dates only.

use std::io::Write as _;

use anyhow::Context as _;
use chrono::NaiveDate;
use serde_json::Value;

use super::{py_splitlines, unheld_in, HistoryRow, HistorySource};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

fn fixture(name: &str) -> anyhow::Result<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/parcel_number_change_history_stage/fixtures")
        .join(name);
    std::fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))
}

fn day(text: &str) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").with_context(|| format!("{text} is no day"))
}

fn quarantine(reason: Option<&str>) -> anyhow::Result<Option<&'static str>> {
    Ok(match reason {
        None => None,
        Some("implausible_land_mov_ymd") => Some("implausible_land_mov_ymd"),
        Some("malformed_pnu") => Some("malformed_pnu"),
        Some("unparseable_land_mov_ymd") => Some("unparseable_land_mov_ymd"),
        Some(other) => anyhow::bail!("the fixture names a reason this reader has not: {other}"),
    })
}

#[test]
fn the_file_is_the_rows_python_read() -> anyhow::Result<()> {
    let answers: Value = serde_json::from_str(&fixture("history.rows.json")?)?;
    let text = |value: &Value| value.as_str().map(str::to_owned);
    let number = |value: &Value| value.as_i64().and_then(|n| i32::try_from(n).ok());
    let expected = answers["rows"]
        .as_array()
        .context("rows")?
        .iter()
        .map(|row| {
            Ok(HistoryRow {
                old_pnu: text(&row[0]).context("old")?,
                new_pnu: text(&row[1]).context("new")?,
                is_dong_level: row[2].as_bool().context("dong level")?,
                reason_code: text(&row[3]),
                changed_on: row[4].as_str().map(day).transpose()?,
                changed_on_raw: text(&row[5]),
                quarantine_reason: quarantine(row[6].as_str())?,
                source_sigungu_code: text(&row[7]),
                source_line_number: number(&row[8]).context("line")?,
                duplicate_count: number(&row[9]).context("repeats")?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let source = HistorySource::compiled()?;
    let collected = day(answers["collected"].as_str().context("collected")?)?;
    let rows = source.parse_rows(&fixture("history.txt")?, collected)?;
    assert_eq!(rows, expected);
    source.check_rows(&rows, "ABPD_UNQ_NO_CHG_HIST_99_209907.txt")?;
    for region in answers["regions"].as_array().context("regions")? {
        assert_eq!(
            Some(source.region_code(region[0].as_str().context("member")?)?),
            region[1].as_str()
        );
    }
    Ok(())
}

#[test]
fn a_moved_header_or_a_short_row_refuses_the_file_whole() -> anyhow::Result<()> {
    let source = HistorySource::compiled()?;
    let text = fixture("history.txt")?;
    let collected = day("2099-07-15")?;
    let moved = text.replacen("OLD_BOBN|OLD_BUBN", "OLD_BUBN|OLD_BOBN", 1);
    assert!(source.parse_rows(&moved, collected).is_err());
    let short = text.replacen("|50|20990701|99999", "|50|20990701", 1);
    let Err(error) = source.parse_rows(&short, collected) else {
        anyhow::bail!("a short row was read");
    };
    assert!(
        format!("{error:#}").contains("line 2 has 12 fields"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn a_file_of_dateless_rows_is_refused_and_a_few_are_kept() -> anyhow::Result<()> {
    let source = HistorySource::compiled()?;
    let row = |dated: bool| HistoryRow {
        old_pnu: "9999910100100010000".to_owned(),
        new_pnu: "9999910200100010000".to_owned(),
        is_dong_level: false,
        reason_code: None,
        changed_on: None,
        changed_on_raw: None,
        quarantine_reason: (!dated).then_some("unparseable_land_mov_ymd"),
        source_sigungu_code: None,
        source_line_number: 2,
        duplicate_count: 1,
    };
    // Six dateless rows of ten: over the row allowance and over the share.
    let mut rows = vec![row(false); 6];
    rows.extend(vec![row(true); 4]);
    assert!(source.check_rows(&rows, "m").is_err());
    // Five of ten: inside the row allowance, whatever the share (a small 시도's file).
    rows[0] = row(true);
    source.check_rows(&rows, "m")?;
    assert!(source.check_rows(&[], "m").is_err());
    Ok(())
}

#[test]
fn the_zip_must_hold_one_named_utf8_member() -> anyhow::Result<()> {
    let source = HistorySource::compiled()?;
    let zipped = |members: &[(&str, &[u8])]| -> anyhow::Result<Vec<u8>> {
        let mut archive = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for (name, bytes) in members {
            archive.start_file(*name, zip::write::SimpleFileOptions::default())?;
            archive.write_all(bytes)?;
        }
        Ok(archive.finish()?.into_inner())
    };
    let name = "ABPD_UNQ_NO_CHG_HIST_99_209907.txt";
    let (member, text) = source.read_member(&zipped(&[(name, b"a|b\n")])?)?;
    assert_eq!((member.as_str(), text.as_str()), (name, "a|b\n"));
    assert!(source.read_member(b"not a zip").is_err());
    assert!(source
        .read_member(&zipped(&[("other.txt", b"x")])?)
        .is_err());
    assert!(source
        .read_member(&zipped(&[
            (name, b"x"),
            ("ABPD_UNQ_NO_CHG_HIST_98_209907.txt", b"x")
        ])?)
        .is_err());
    assert!(source
        .read_member(&zipped(&[(name, &[0xff, 0xfe])])?)
        .is_err());
    Ok(())
}

#[test]
fn a_key_names_its_file_and_its_bytes() -> anyhow::Result<()> {
    let source = HistorySource::compiled()?;
    let checksum = "a".repeat(64);
    let (file, other) = ("30527-5", "30527-6");
    let folder = "bronze/source=vworldkr__parcel_number_change_history";
    let key = format!("{folder}/{file}--sha256-{checksum}.zip");
    assert_eq!(source.content_key_checksum(&key, file)?, checksum);
    // Another file's key, and the plain keys of 2026-10-05, are never loaded.
    assert!(source.content_key_checksum(&key, other).is_err());
    assert!(source
        .content_key_checksum(&format!("{folder}/{file}.zip"), file)
        .is_err());
    Ok(())
}

#[test]
fn lines_split_where_python_splits_them() {
    assert_eq!(py_splitlines("a\r\nb\rc\n\nd"), ["a", "b", "c", "", "d"]);
    assert_eq!(py_splitlines("a\u{2028}b\u{0c}c\n"), ["a", "b", "c"]);
    assert!(py_splitlines("").is_empty());
}

/// Root ADR-0318: an upload that repeats the rows the table holds appends only its new rows; a
/// quarantined row has no date and an ordinary one no reason, and a plain join never matches a null.
#[test]
fn only_the_rows_the_table_does_not_hold_are_appended() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let columns = "old_pnu, new_pnu, is_dong_level, reason_code, changed_on, changed_on_raw, quarantine_reason, \
                   source_sigungu_code, source_region_code, source_line_number";
    stage.connection().execute_batch(&format!(
        "CREATE TABLE held AS SELECT * FROM (VALUES
           ('9999910100100010000', '9999910100100020000', false, '10', DATE '2099-01-01', '20990101', NULL, '99999', '99', 1),
           ('9999910100100030000', '9999910100100020000', false, NULL, NULL, 'x', 'unparseable_land_mov_ymd', '99999', '99', 2)
         ) t({columns});
         CREATE TABLE pnch_rows AS SELECT * FROM (VALUES
           ('9999910100100010000', '9999910100100020000', false, '10', DATE '2099-01-01', '20990101', NULL, '99999', '99', 7),
           ('9999910100100030000', '9999910100100020000', false, NULL, NULL, 'x', 'unparseable_land_mov_ymd', '99999', '99', 8),
           ('9999910100100040000', '9999910100100020000', false, '10', DATE '2099-02-01', '20990201', NULL, '99999', '99', 9),
           ('9999910100100010000', '9999910100100020000', false, '10', DATE '2099-01-01', '20990101', NULL, '99999', '98', 10)
         ) t({columns});"
    ))?;
    assert_eq!(unheld_in(&stage, "held")?, 2);
    let fresh: Vec<i32> = stage
        .connection()
        .prepare("SELECT source_line_number FROM pnch_fresh ORDER BY 1")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    // The new link, and the held link under another file's region.
    assert_eq!(fresh, [9, 10]);
    Ok(())
}
