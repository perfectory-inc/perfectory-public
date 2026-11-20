//! The registered factory stage reads an edition and sums its links as the Spark job did (root
//! ADR-0280 §4).
//!
//! The cases are the ones the Spark job's own tests held (`test_data_go_kr_registered_factory.py`,
//! removed with the job), on local tables. Synthetic names and codes under 99999 only.

use std::io::Write as _;

use super::{link_summary, read_edition, text_or_none, whole, Edition, FactorySource};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

type Row = (
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    i32,
);

/// A zip of `members` (name, text), each written as CP949 with a UTF-8 name.
fn zip_of(
    directory: &std::path::Path,
    members: &[(&str, &str)],
) -> anyhow::Result<std::path::PathBuf> {
    let path = directory.join("edition.zip");
    let mut archive = zip::ZipWriter::new(std::fs::File::create(&path)?);
    for (name, text) in members {
        archive.start_file(*name, zip::write::SimpleFileOptions::default())?;
        let (bytes, _, lossy) = encoding_rs::EUC_KR.encode(text);
        anyhow::ensure!(!lossy, "{name} is not CP949 text");
        archive.write_all(&bytes)?;
    }
    archive.finish()?;
    Ok(path)
}

const HEADER: &str = "순번,회사명,단지명,생산품,공장주소";

#[test]
fn an_edition_is_its_one_data_member_a_row_per_line_that_is_not_blank() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&directory.path().join("stage"))?;
    let source = FactorySource::compiled()?;
    let text = format!(
        "{HEADER}\r\n1, 합성산업 ,가상단지,\"부품, 금형\", 합성도 가상시 가나동 99-1 \r\n,,,,\r\n둘,가상정밀,,,\r\n3,짧은줄\r\n"
    );
    let path = zip_of(
        directory.path(),
        &[
            ("안내/항목설명.csv", "항목,설명\r\n"),
            ("전국등록공장현황_20990131.csv", &text),
        ],
    )?;
    let edition = read_edition(&stage, &source, &path)?;
    assert_eq!(
        edition,
        Edition {
            snapshot_id: "20990131".to_owned(),
            rows: 3
        }
    );
    let rows: Vec<Row> = stage
        .connection()
        .prepare("SELECT * FROM factory_rows ORDER BY _row")?
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })?
        .collect::<Result<_, _>>()?;
    let text = |value: &str| Some(value.to_owned());
    assert_eq!(
        rows,
        [
            // The blank line is left out and not numbered: the line after it is line 3.
            (
                1,
                Some(1),
                text("합성산업"),
                text("가상단지"),
                text("부품, 금형"),
                text("합성도 가상시 가나동 99-1"),
                " 합성도 가상시 가나동 99-1 ".to_owned(),
                2
            ),
            (
                2,
                None,
                text("가상정밀"),
                None,
                None,
                None,
                String::new(),
                3
            ),
            // A short line reads as empty cells.
            (
                3,
                Some(3),
                text("짧은줄"),
                None,
                None,
                None,
                String::new(),
                4
            ),
        ]
    );
    Ok(())
}

#[test]
fn a_zip_without_its_one_data_member_or_with_another_header_is_refused() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&directory.path().join("stage"))?;
    let source = FactorySource::compiled()?;
    let line = format!("{HEADER}\r\n1,합성산업,,,\r\n");
    for (members, message) in [
        (vec![("항목설명.txt", "설명")], "expected one data member"),
        (
            vec![
                ("현황_20990131.csv", line.as_str()),
                ("현황_20990228.csv", line.as_str()),
            ],
            "expected one data member",
        ),
        (
            vec![("현황_20990131.csv", "순번,회사명\r\n1,합성산업\r\n")],
            "the header is not the contract's",
        ),
    ] {
        let path = zip_of(directory.path(), &members)?;
        let Err(refused) = read_edition(&stage, &source, &path) else {
            anyhow::bail!("{message}: the zip was read");
        };
        assert!(format!("{refused:#}").contains(message), "{refused:#}");
    }
    Ok(())
}

#[test]
fn an_edition_under_the_floor_or_shrunk_past_its_bound_is_refused() -> anyhow::Result<()> {
    let source = FactorySource::compiled()?;
    let floor = source.min_rows;
    source.check_bounds(floor, 0)?;
    assert!(source.check_bounds(floor - 1, 0).is_err());
    // Against the last loaded edition: the bound is a share of it.
    let previous = floor * 2;
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let least = (previous as f64 * (1.0 - source.max_shrink_share)).ceil() as u64;
    source.check_bounds(least, previous)?;
    assert!(source.check_bounds(least - 1, previous).is_err());
    Ok(())
}

#[test]
fn a_cell_is_a_whole_number_or_a_text_or_nothing() -> anyhow::Result<()> {
    assert_eq!(whole(" 12 ")?, Some(12));
    assert_eq!(whole("1.5")?, None);
    assert_eq!(whole("")?, None);
    assert_eq!(text_or_none("  가  "), Some("가"));
    assert_eq!(text_or_none(" \u{3000} "), None);
    Ok(())
}

#[test]
fn the_link_summary_counts_each_step_and_each_reason() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(
        "CREATE TABLE factory_staged AS SELECT * FROM (VALUES
           ('9999910100100990001', 'jibun_address', NULL),
           ('9999910100100990002', 'jibun_address', NULL),
           ('9999910100100990003', 'road_address', NULL),
           (NULL, NULL, 'road_not_found'),
           (NULL, NULL, 'no_address'),
           (NULL, NULL, 'no_address')) t(pnu, pnu_basis, pnu_unresolved_reason)",
    )?;
    let summary = link_summary(&stage)?;
    let counts = |pairs: &[(&str, u64)]| {
        pairs
            .iter()
            .map(|(name, rows)| ((*name).to_owned(), *rows))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(summary.rows, 6);
    assert_eq!(
        summary.by_basis,
        counts(&[("jibun_address", 2), ("road_address", 1), ("unresolved", 3)])
    );
    assert_eq!(
        summary.unresolved_reasons,
        counts(&[("no_address", 2), ("road_not_found", 1)])
    );
    assert_eq!(summary.match_rate, Some(0.5));
    Ok(())
}
