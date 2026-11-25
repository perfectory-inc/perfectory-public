//! The file data stage reads, keys, links and compares as the Spark job did (root ADR-0401 §3–4).
//!
//! The cases are the ones the Spark job's own tests held (`test_data_go_kr_file_data.py`, removed
//! with the job), on local tables. Synthetic codes under 99999 only.

use std::io::Write as _;

use anyhow::Context as _;
use chrono::TimeZone as _;
use serde_json::json;

use super::{
    affected_filter, changes, decode, follow_record, keyed, last_followed, link, read_file,
    Changes, FileEntry, PlaceInputs, PlaceLink, PlannedFile, PyCsvRecords,
};
use crate::{
    duckdb_parquet_stage::{DuckDbParquetStage, FetchedFile, PYTHON_VALUES_SQL},
    place_identity_stage::PlacePins,
};

fn stage() -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(PYTHON_VALUES_SQL)?;
    Ok((directory, stage))
}

fn entry(slug: &str, entry: serde_json::Value) -> FileEntry {
    FileEntry {
        slug: slug.to_owned(),
        entry,
    }
}

fn pins() -> PlacePins {
    PlacePins {
        code_changes: 0,
        dong_names: 0,
        parcels: 0,
        titles: 0,
        served_snapshot_id: "e1".to_owned(),
    }
}

fn records(text: &str) -> Vec<Vec<String>> {
    PyCsvRecords { rest: text }.collect()
}

#[test]
fn a_csv_text_is_read_as_python_reads_it() {
    assert_eq!(
        records("a,\"b,\"\"c\"\"\"\r\n\n\"x\ny\",z\"q\rlast"),
        [
            vec!["a".to_owned(), "b,\"c\"".to_owned()],
            vec![],
            vec!["x\ny".to_owned(), "z\"q".to_owned()],
            vec!["last".to_owned()],
        ]
    );
    // A character after a closing quote is kept, and a text that ends inside a quote is a record.
    assert_eq!(records("\"a\"b,c\n\"open"), [vec!["ab", "c"], vec!["open"]]);
    assert_eq!(records("a,\n"), [vec!["a", ""]]);
}

#[test]
fn a_file_decodes_strictly_or_is_refused() -> anyhow::Result<()> {
    let (bytes, _, _) = encoding_rs::EUC_KR.encode("역이름");
    assert_eq!(decode(&bytes, "cp949")?, "역이름");
    assert!(decode(b"\xff\xff", "cp949").is_err());
    assert_eq!(decode(b"\xef\xbb\xbfa", "utf-8-sig")?, "a");
    assert!(decode(b"\xff", "utf-8").is_err());
    assert!(decode(b"a", "latin-9").is_err());
    Ok(())
}

fn station() -> FileEntry {
    entry(
        "railway_station",
        json!({
            "format": "csv", "encoding": "cp949", "header": ["역이름", "주소", "비고"],
            "bounds": {"min_rows": 2, "max_shrink_share": 0.15},
            "row_key": {"kind": "columns", "columns": ["역이름"]},
            "place_link": {"kind": "address_pnu", "address_column": "주소"},
        }),
    )
}

fn write_cp949(directory: &std::path::Path, text: &str) -> anyhow::Result<std::path::PathBuf> {
    let path = directory.join("edition.csv");
    let (bytes, _, unmappable) = encoding_rs::EUC_KR.encode(text);
    anyhow::ensure!(!unmappable, "the fixture is not cp949 text");
    std::fs::File::create(&path)?.write_all(&bytes)?;
    Ok(path)
}

#[test]
fn a_csv_edition_is_keyed_by_its_provider_key_and_keeps_every_cell() -> anyhow::Result<()> {
    let (directory, stage) = stage()?;
    let file = station();
    let path = write_cp949(
        directory.path(),
        "역이름,주소,비고\r\n가상역,합성도 가상시 가나동 8,\"줄\n바꿈 \"\"인용\"\"\"\r\n,,\r\n나상역,,\r\n",
    )?;
    assert_eq!(read_file(&stage, &file, &FetchedFile::at(path))?, 2);
    keyed(&stage, &file)?;
    let rows: Vec<(String, String, i32)> = stage
        .connection()
        .prepare("SELECT row_key, provider_fields_json, source_line_number FROM file_keyed ORDER BY _row")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        [
            (
                "가상역".to_owned(),
                r#"{"역이름":"가상역","주소":"합성도 가상시 가나동 8","비고":"줄\n바꿈 \"인용\""}"#
                    .to_owned(),
                2
            ),
            // The blank row is left out and still counted: the next row is line 4.
            (
                "나상역".to_owned(),
                r#"{"역이름":"나상역","주소":"","비고":""}"#.to_owned(),
                4
            ),
        ]
    );

    let wrong = write_cp949(directory.path(), "역이름,주소\r\n가상역,어디\r\n")?;
    let Err(refused) = read_file(&stage, &file, &FetchedFile::at(wrong)) else {
        anyhow::bail!("another header was read");
    };
    assert!(refused.to_string().contains("header"), "{refused}");
    let repeated = write_cp949(
        directory.path(),
        "역이름,주소,비고\r\n가상역,,\r\n가상역,,\r\n",
    )?;
    read_file(&stage, &file, &FetchedFile::at(repeated))?;
    let Err(refused) = keyed(&stage, &file) else {
        anyhow::bail!("a repeated key was kept");
    };
    assert!(refused.to_string().contains("repeats"), "{refused}");
    Ok(())
}

#[test]
fn a_row_without_a_provider_key_is_keyed_by_its_sha256() -> anyhow::Result<()> {
    let (directory, stage) = stage()?;
    let file = entry(
        "highway_access_point",
        json!({
            "format": "csv", "encoding": "cp949", "header": ["도", "시군구"],
            "row_key": {"kind": "sha256_of_row"},
            "place_link": {"kind": "legal_dong_by_names", "names": ["도", "시군구"]},
        }),
    );
    let path = write_cp949(directory.path(), "도,시군구\n합성도,가상시\n")?;
    read_file(&stage, &file, &FetchedFile::at(path))?;
    keyed(&stage, &file)?;
    let key: String =
        stage
            .connection()
            .query_row("SELECT row_key FROM file_keyed", [], |row| row.get(0))?;
    // hashlib.sha256("합성도\x1f가상시".encode()).hexdigest()
    let expected: String = stage.connection().query_row(
        "SELECT sha256('합성도' || chr(31) || '가상시')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(key, expected);
    assert_eq!(key.len(), 64);
    Ok(())
}

#[test]
fn a_standard_pass_is_read_in_header_order_and_counted() -> anyhow::Result<()> {
    let (directory, stage) = stage()?;
    let file = entry(
        "school_location",
        json!({
            "format": "standard_pages", "header": ["SCHOOL_ID", "NM", "N"],
            "row_key": {"kind": "columns", "columns": ["SCHOOL_ID"]},
            "place_link": {"kind": "address_pnu", "address_columns": ["NM"]},
        }),
    );
    let pass = |total: u64| -> anyhow::Result<std::path::PathBuf> {
        let lines = [
            json!({"kind": "columns", "body": json!({"totalCount": total}).to_string()}),
            json!({"kind": "rows", "body": json!([{"NM": "가", "SCHOOL_ID": "S1", "N": 3}, {"SCHOOL_ID": "S2", "NM": null}]).to_string()}),
        ];
        let path = directory.path().join("pass.gz");
        let mut out = flate2::write::GzEncoder::new(
            std::fs::File::create(&path)?,
            flate2::Compression::default(),
        );
        for line in lines {
            writeln!(out, "{line}")?;
        }
        out.finish()?;
        Ok(path)
    };
    assert_eq!(read_file(&stage, &file, &FetchedFile::at(pass(2)?))?, 2);
    let rows: Vec<(i32, String)> = stage
        .connection()
        .prepare("SELECT source_line_number, cells FROM file_rows ORDER BY _row")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        [
            (2, r#"["S1","가","3"]"#.to_owned()),
            (3, r#"["S2","",""]"#.to_owned())
        ]
    );
    let Err(refused) = read_file(&stage, &file, &FetchedFile::at(pass(3)?)) else {
        anyhow::bail!("a short pass was read");
    };
    assert!(format!("{refused:#}").contains("reports"), "{refused:#}");
    Ok(())
}

type LinkRow = (i64, Option<String>, Option<String>, Option<String>);

/// The reference tables of a tiny synthetic 법정동 list.
fn reference(stage: &DuckDbParquetStage) -> anyhow::Result<()> {
    stage.connection().execute_batch(&format!(
        "CREATE TABLE place_code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
         effective_date VARCHAR, source VARCHAR);
         CREATE TABLE place_dong_names AS SELECT * FROM (VALUES
           ('9900000000', '합성도', '존재'), ('9999900000', '합성도 가상시', '존재'),
           ('9999910100', '합성도 가상시 가나동', '존재')) t(region_cd, full_name, status);
         {}",
        crate::place_identity_stage::REFERENCE_SQL
    ))?;
    Ok(())
}

#[test]
fn names_link_to_the_legal_dong_they_name_or_say_why_not() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    reference(&stage)?;
    let file = entry(
        "highway_access_point",
        json!({"format": "csv", "header": ["도", "시군구", "읍면동"], "row_key": {"kind": "sha256_of_row"},
               "place_link": {"kind": "legal_dong_by_names", "names": ["도", "시군구", "읍면동"]}}),
    );
    stage.connection().execute_batch(
        r#"CREATE TABLE rows_in AS SELECT * FROM (VALUES
             (1, '{"도":"합성도","시군구":" 가상시 ","읍면동":"가나동"}'),
             (2, '{"도":"합성도","시군구":"","읍면동":""}'),
             (3, '{"도":"없는도","시군구":"가상시","읍면동":""}'),
             (4, '{"도":"합성도","시군구":"다른시","읍면동":""}')) t(_row, provider_fields_json)"#,
    )?;
    let title = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
        crate::place_identity_stage::TITLE_TABLE,
    )
    .context("the titles have a contract")?;
    link(
        &stage,
        &file,
        "rows_in",
        PlaceInputs {
            pins: &pins(),
            title_contract: title,
        },
    )?;
    let rows: Vec<LinkRow> = stage
        .connection()
        .prepare("SELECT _row, legal_dong_code, administrative_dong_code, legal_dong_unresolved_reason FROM file_link ORDER BY _row")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        [
            (1, Some("9999910100".to_owned()), None, None),
            (2, None, None, Some("no_address".to_owned())),
            (3, None, None, Some("unparsed".to_owned())),
            (4, None, None, Some("no_legal_dong".to_owned())),
        ]
    );
    Ok(())
}

#[test]
fn an_edition_appends_its_new_and_changed_keys_and_a_marker_per_vanished_key() -> anyhow::Result<()>
{
    let (_directory, stage) = stage()?;
    let file = station();
    let contract = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
        "silver.railway_station",
    )
    .context("the stations have a contract")?;
    let row = |key: &str, fields: &str, pnu: &str, snapshot: &str| {
        format!(
            "('{key}', '{fields}', {pnu}, NULL, NULL, NULL, 'bronze/old', 2, '{snapshot}', TIMESTAMPTZ '2026-01-01 00:00:00+00', NULL)"
        )
    };
    let columns = "row_key, provider_fields_json, pnu, pnu_basis, pnu_unresolved_reason, legal_dong_code, \
                   source_record_id, source_line_number, source_snapshot_id, ingested_at_utc, row_state";
    stage.connection().execute_batch(&format!(
        "CREATE TABLE file_current AS SELECT * FROM (VALUES {}, {}, {}) t({columns});
         CREATE TABLE file_staged AS SELECT * FROM (VALUES {}, {}, {}) t({columns});",
        row("same", "{}", "NULL", "old"),
        row("moved", "{}", "NULL", "old"),
        row("gone", "{}", "NULL", "old"),
        row("same", "{}", "NULL", "new"),
        row("moved", "{}", "'9999910100100070000'", "new"),
        row("fresh", "{}", "NULL", "new"),
    ))?;
    let at = chrono::Utc
        .with_ymd_and_hms(2026, 2, 1, 0, 0, 0)
        .single()
        .context("a valid instant")?;
    assert_eq!(
        changes(&stage, &file, contract, "new", at)?,
        Changes {
            appended: 3,
            removed: 1
        }
    );
    let rows: Vec<(String, Option<String>, String, String)> = stage
        .connection()
        .prepare("SELECT row_key, row_state, source_snapshot_id, source_record_id FROM file_changes ORDER BY row_key")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        [
            (
                "fresh".to_owned(),
                None,
                "new".to_owned(),
                "bronze/old".to_owned()
            ),
            // The marker is the current row with this edition's id; it keeps the file it came from.
            (
                "gone".to_owned(),
                Some("removed".to_owned()),
                "new".to_owned(),
                "bronze/old".to_owned()
            ),
            (
                "moved".to_owned(),
                None,
                "new".to_owned(),
                "bronze/old".to_owned()
            ),
        ]
    );
    Ok(())
}

#[test]
fn a_follow_record_names_the_snapshot_and_the_newest_one_is_the_last_followed() -> anyhow::Result<()>
{
    let at = chrono::Utc
        .with_ymd_and_hms(2026, 11, 20, 1, 2, 3)
        .single()
        .context("a valid instant")?;
    let record = follow_record(42, at);
    assert_eq!(
        record,
        "follow:reference.legal_dong_code_change@42@20261120T010203Z"
    );
    let recorded = [
        "bronze/source=datagokr__port/x.csv",
        "follow:reference.legal_dong_code_change@7@20261119T000000Z",
        record.as_str(),
        "follow:reference.legal_dong_code_change@9@not-a-time",
    ];
    assert_eq!(last_followed(recorded), Some(42));
    assert_eq!(last_followed(["bronze/x"]), None);
    Ok(())
}

#[test]
fn a_follow_reads_the_rows_under_a_changed_code_and_the_unlinked() {
    assert_eq!(affected_filter(PlaceLink::AdmDongViaLinkage, false), "TRUE");
    let filter = affected_filter(PlaceLink::AddressPnu, true);
    assert!(filter.contains("c.legal_dong_code IS NULL"), "{filter}");
    assert!(filter.contains("c.pnu"), "{filter}");
    assert!(!affected_filter(PlaceLink::LegalDongByNames, true).contains("c.pnu"));
}

#[test]
fn an_edition_id_is_the_bronze_leaf_without_its_extension() {
    let planned = PlannedFile {
        object_key: "bronze/source=datagokr__port/port-12345.csv".to_owned(),
        slug: "port".to_owned(),
        path: FetchedFile::at(std::path::PathBuf::new()),
    };
    assert_eq!(planned.snapshot_id(), "port-12345");
}
