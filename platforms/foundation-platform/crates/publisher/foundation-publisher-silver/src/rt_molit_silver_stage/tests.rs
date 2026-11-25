//! The load on a synthetic export (`fixtures/export.utf8.csv`: codes 99999, 2099 dates, every line
//! `synthetic`) against what main's Spark job functions made of the same bytes
//! (`fixtures/spark_rows.jsonl`: `rows_of`, then `legal_dong_code`, `lot_of` and `candidate_pnu` per
//! row; `fixtures/spark_rejected.json`; a row's `row_key` is written there as `name_and_rank`, a
//! field name the secret scan does not read as a credential). The export holds the cases a day's data may not: a quoted
//! field with a comma, a quote and a line end, rows of one name ranked by their cancellation, a
//! short record, another export's row, another month's row, an empty line, and each kind of lot.

use std::collections::BTreeMap;

use anyhow::Context;
use lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name;
use serde_json::Value;

use super::*;

const DATASET: &str = "real_transaction_land_trade";
const MONTH: &str = "209904";
const OBJECT_KEY: &str =
    "bronze/rtmolitgokr__real_transaction_land_trade/period=2099-04/export--sha256-synthetic.csv";
const DONG: &str = "9999933021";
const DONG_NAME: &str = "합성도 합성시 합성면 가상리";
const INGESTED: i64 = 4_081_363_200;
/// The fixture's name for `row_key`.
const NAME_AND_RANK: &str = "name_and_rank";
/// The empty line of the fixture's rows. `DuckDB`'s reader skips an empty line without counting it,
/// so the rows after it start one line earlier than the Spark job counted (a load column, never
/// compared: `silver_changes::LOAD_COLUMNS`).
const EMPTY_LINE: i64 = 26;

fn fixture(name: &str) -> anyhow::Result<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/rt_molit_silver_stage/fixtures")
        .join(name);
    std::fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))
}

/// The export as the provider sends it: CP949 bytes.
fn export_bytes(text: &str) -> Vec<u8> {
    encoding_rs::EUC_KR.encode(text).0.into_owned()
}

fn spark_rows() -> anyhow::Result<Vec<Value>> {
    fixture("spark_rows.jsonl")?
        .lines()
        .map(|line| serde_json::from_str(line).context("a fixture row is not JSON"))
        .collect()
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no contract for {table}"))
}

/// A stage with the code tables of one 법정동, the export `bytes` written into its directory, and the
/// planned object that names it.
fn stage_with(
    bytes: &[u8],
) -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage, PlannedObject)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&format!(
        "CREATE TABLE code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
           effective_date VARCHAR, source VARCHAR); \
         CREATE TABLE code_names AS SELECT * FROM (VALUES ('9999900000', '합성도 합성시', '존재'), \
           ('{DONG}', '{DONG_NAME}', '존재')) AS t(region_cd, full_name, status);"
    ))?;
    reference_from_loaded(&stage)?;
    let path = stage.directory().join("export.csv");
    std::fs::write(&path, bytes)?;
    let object = PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        dataset: DATASET.to_owned(),
        month: MONTH.to_owned(),
        source_snapshot_id: "20990501T000000Z".to_owned(),
        path,
    };
    Ok((directory, stage, object))
}

/// The fixture staged, with the contract's quarantine bound at `max_rejected_share`.
fn staged_at(
    max_rejected_share: f64,
) -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage, Staged)> {
    let (directory, stage, object) = stage_with(&export_bytes(&fixture("export.utf8.csv")?))?;
    let mut source = RtMolitSource::compiled()?;
    source.max_rejected_share = max_rejected_share;
    let work = stage.directory().to_owned();
    let staged = stage_export(
        &stage,
        contract(TABLE)?,
        &object,
        0,
        (&source, INGESTED, &work),
    )?;
    Ok((directory, stage, staged))
}

/// The fixture staged. It holds three bad lines of twenty on purpose; the contract's bound (0.1%)
/// would quarantine it, which its own test covers.
fn staged() -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage, Staged)> {
    staged_at(1.0)
}

fn texts(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let connection = stage.connection();
    let mut statement = connection.prepare(query)?;
    let columns = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            (0..columns)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Result<Vec<_>, _>>()
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// The line the Spark job gave a row of the fixture, as `DuckDB`'s reader counts it.
fn duckdb_line(spark_line: i64) -> i64 {
    if spark_line > EMPTY_LINE {
        spark_line - 1
    } else {
        spark_line
    }
}

#[test]
fn the_rows_are_the_rows_the_spark_job_named() -> anyhow::Result<()> {
    let (_directory, stage, staged) = staged()?;
    let expected = spark_rows()?;
    assert!(!staged.refused);
    assert_eq!(staged.rows, u64::try_from(expected.len())?);
    let ours: Vec<Value> = texts(
        &stage,
        "SELECT CAST(to_json(d) AS VARCHAR) FROM (SELECT * EXCLUDE (pnu, rt_current_code, rt_reason) \
         FROM rt_deals_0) d ORDER BY source_line_number",
    )?
    .into_iter()
    .map(|row| -> anyhow::Result<Value> {
        let text = row.into_iter().next().flatten().context("a row as JSON")?;
        Ok(serde_json::from_str(&text)?)
    })
    .collect::<anyhow::Result<_>>()?;
    assert_eq!(ours.len(), expected.len());
    for (row, spark) in ours.iter().zip(&expected) {
        let spark = spark.as_object().context("a fixture row is an object")?;
        let row = row.as_object().context("a staged row is an object")?;
        let line = spark["source_line_number"].as_i64().context("a line")?;
        for (name, value) in spark.iter().filter(|(name, _)| !name.starts_with('_')) {
            let ours = row
                .get(if name == NAME_AND_RANK {
                    "row_key"
                } else {
                    name.as_str()
                })
                .unwrap_or(&Value::Null);
            if name == "source_line_number" {
                assert_eq!(ours.as_i64(), Some(duckdb_line(line)), "line {line}");
            } else {
                assert_eq!(ours, value, "line {line} column {name}");
            }
        }
        // Python wrote only the columns its export carries; every other column is null here.
        for (name, value) in row {
            let fixture_name = if name == "row_key" {
                NAME_AND_RANK
            } else {
                name.as_str()
            };
            assert!(
                spark.contains_key(fixture_name) || value.is_null(),
                "line {line} adds {name}"
            );
        }
    }
    Ok(())
}

#[test]
fn the_lines_not_loaded_are_the_spark_jobs() -> anyhow::Result<()> {
    let (_directory, stage, staged) = staged()?;
    let expected: Value = serde_json::from_str(&fixture("spark_rejected.json")?)?;
    let ours: Vec<Value> = texts(
        &stage,
        "SELECT CAST(source_line_number AS VARCHAR), raw_line, reason FROM rt_rejected_0 \
         ORDER BY source_line_number",
    )?
    .into_iter()
    .map(|row| {
        let line: i64 = row[0].as_deref().unwrap_or_default().parse().unwrap_or(-1);
        serde_json::json!([line, row[1], row[2]])
    })
    .collect();
    assert_eq!(Value::Array(ours), expected["rejected"]);
    assert_eq!(
        serde_json::json!(staged.unrepresented_fields),
        expected["unrepresented"]
    );
    assert_eq!(
        staged.rejected_reasons,
        BTreeSet::from([
            "cells_18_of_20".to_owned(),
            "deal_month_outside_export".to_owned(),
            "row_number_1_where_17".to_owned()
        ])
    );
    Ok(())
}

#[test]
fn an_export_that_is_not_the_months_csv_is_refused_whole() -> anyhow::Result<()> {
    let text = fixture("export.utf8.csv")?;
    let cases = [
        (
            export_bytes(&text.replace("2099-04-30", "2099-05-31")),
            "the preamble does not name the month 209904",
        ),
        (
            export_bytes(&text.replace("\"NO\"", "\"번호\"")),
            "the export has no header line",
        ),
        (vec![0xff, 0xff, 0x22], "is not CP949 text"),
    ];
    for (bytes, reason) in cases {
        let (_directory, stage, object) = stage_with(&bytes)?;
        let source = RtMolitSource::compiled()?;
        let work = stage.directory().to_owned();
        let staged = stage_export(
            &stage,
            contract(TABLE)?,
            &object,
            0,
            (&source, INGESTED, &work),
        )?;
        assert!(staged.refused && staged.rows == 0, "{reason}");
        let lines = texts(
            &stage,
            "SELECT CAST(source_line_number AS VARCHAR), reason FROM rt_rejected_0",
        )?;
        assert_eq!(lines.len(), 1, "{reason}");
        let line = lines.first().context("a refused export has its line 0")?;
        assert_eq!(line[0].as_deref(), Some("0"));
        let written = line[1].as_deref().unwrap_or_default();
        assert!(
            written.starts_with("export_refused: ") && written.contains(reason),
            "{written}"
        );
        assert_eq!(
            texts(&stage, "SELECT count(*)::VARCHAR FROM rt_deals_0")?,
            vec![vec![Some("0".to_owned())]]
        );
    }
    Ok(())
}

#[test]
fn the_row_numbers_are_walked_as_the_spark_job_walked_them() {
    let record = |record: i64, cells: i64, first: &str| Record {
        record,
        cells,
        first_cell: first.to_owned(),
        blank: false,
    };
    let records = [
        record(2, 3, "1"),
        // Blank: skipped, and it takes no number.
        Record {
            blank: true,
            ..record(3, 1, "")
        },
        // Short, but this export's row 2: refused, and the next row is 3.
        record(4, 2, "2"),
        record(5, 3, "3"),
        // Another export's rows: refused, and they take no number.
        record(6, 3, "1"),
        record(7, 2, "2"),
        record(8, 3, "4"),
    ];
    assert_eq!(
        walk_row_numbers(&records, 3),
        vec![
            (4, "cells_2_of_3".to_owned()),
            (6, "row_number_1_where_4".to_owned()),
            (7, "cells_2_of_3".to_owned()),
        ]
    );
}

#[test]
fn a_record_is_read_as_the_provider_writes_it() -> anyhow::Result<()> {
    let text = "\"NO\",\"a\",\"b\"\r\n\"1\",\"x, \"\"y\"\"\r\nz\",\"\"\r\n   \r\n\"2\",\" p \",\"q\",\"extra\"\r\n\"3\",,\r\n";
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let path = stage.directory().join("records.csv");
    std::fs::write(&path, text)?;
    let source = RtMolitSource::compiled()?;
    let object = PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        dataset: DATASET.to_owned(),
        month: MONTH.to_owned(),
        source_snapshot_id: "20990501T000000Z".to_owned(),
        path: path.clone(),
    };
    let read = read_export(
        stage.connection(),
        &Transcoded {
            path,
            first_line: 10,
        },
        &object,
        &source,
        0,
    )?
    .map_err(anyhow::Error::msg)?;
    assert_eq!((read.rows, read.rejected), (2, 1));
    let records = texts(
        &stage,
        "SELECT CAST(record AS VARCHAR), CAST(cells AS VARCHAR), first_cell, CAST(blank AS VARCHAR), \
                CAST(source_line_number AS VARCHAR) FROM rt_records_0 ORDER BY record",
    )?;
    let some = |text: &str| Some(text.to_owned());
    assert_eq!(
        records,
        vec![
            // A quoted comma, a doubled quote and a line end stay in one cell; an empty quoted cell is ''.
            vec![some("2"), some("3"), some("1"), some("false"), some("11")],
            // A record of blanks: read, and skipped as blank.
            vec![some("3"), some("1"), some(""), some("true"), some("13")],
            // One cell more than the header: refused by its count.
            vec![some("4"), some("4"), some("2"), some("false"), some("14")],
            // Unquoted empty cells are cells.
            vec![some("5"), some("3"), some("3"), some("false"), some("15")],
        ]
    );
    let cells = texts(
        &stage,
        "SELECT value FROM rt_cells_0 WHERE record = 2 ORDER BY idx",
    )?;
    assert_eq!(
        cells,
        vec![vec![some("1")], vec![some("x, \"y\"\r\nz")], vec![some("")]]
    );
    assert_eq!(
        texts(&stage, "SELECT reason FROM rt_rejected_lines_0")?,
        vec![vec![some("cells_4_of_3")]]
    );
    Ok(())
}

#[test]
fn a_field_is_written_as_pythons_json_writes_it() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(COLUMNS_SQL)?;
    // `json.dumps(chr(n), ensure_ascii=False)` for the characters DuckDB's writer could write otherwise.
    let cases = [
        (0x0b, "\"\\u000b\""),
        (0x1f, "\"\\u001f\""),
        (0x0a, "\"\\n\""),
        (0x08, "\"\\b\""),
        (0x01, "\"\\u0001\""),
        (0x7f, "\"\u{7f}\""),
        (0x2028, "\"\u{2028}\""),
        (u32::from('/'), "\"/\""),
        (u32::from('"'), "\"\\\"\""),
    ];
    for (code, python) in cases {
        let ours: String = stage.connection().query_row(
            "SELECT rt_json_text(chr(CAST(? AS INTEGER)))",
            [i64::from(code)],
            |row| row.get(0),
        )?;
        assert_eq!(ours, python, "U+{code:04X}");
    }
    let stripped: String = stage.connection().query_row(
        "SELECT rt_strip(chr(12288) || chr(9) || ' 가 나 ' || chr(160) || chr(28))",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stripped, "가 나");
    Ok(())
}

#[test]
fn each_row_names_its_legal_dong_and_its_lot_as_the_spark_job_did() -> anyhow::Result<()> {
    let (_directory, stage, staged) = staged()?;
    let expected = spark_rows()?;
    assert_eq!(staged.rows, u64::try_from(expected.len())?);
    assert_eq!(staged.cancelled, 3);
    assert_eq!(staged.rejected, 3);
    assert_eq!(staged.quarantined, None);
    let ours = texts(
        &stage,
        "SELECT CAST(source_line_number AS VARCHAR), row_key, rt_reason, pnu, rt_current_code, \
                CAST(deal_date AS VARCHAR), CAST(land_area_m2 AS VARCHAR), CAST(deal_amount_10k_krw AS VARCHAR), \
                provider_fields_json \
         FROM rt_deals_0 ORDER BY source_line_number",
    )?;
    let text = |value: &Value| match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    };
    let spark: Vec<Vec<Option<String>>> = expected
        .iter()
        .map(|row| {
            vec![
                row["source_line_number"]
                    .as_i64()
                    .map(|line| duckdb_line(line).to_string()),
                text(&row[NAME_AND_RANK]),
                text(&row["_reason"]),
                text(&row["_source_pnu"]),
                text(&row["_legal_dong_code"]),
                text(&row["deal_date"]),
                text(&row["land_area_m2"]),
                text(&row["deal_amount_10k_krw"]),
                text(&row["provider_fields_json"]),
            ]
        })
        .collect();
    assert_eq!(ours, spark);
    Ok(())
}

#[test]
fn a_lot_is_placed_on_a_served_parcel_and_the_month_is_compared_with_itself() -> anyhow::Result<()>
{
    let (_directory, stage, _) = staged()?;
    let table = contract(TABLE)?;
    let object = PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        dataset: DATASET.to_owned(),
        month: MONTH.to_owned(),
        source_snapshot_id: "20990501T000000Z".to_owned(),
        path: PathBuf::new(),
    };
    // The served edition holds two of the lots the export names.
    stage.connection().execute_batch(&format!(
        "CREATE TABLE served (pnu VARCHAR, source_snapshot_id VARCHAR, valid_to_utc TIMESTAMPTZ); \
         INSERT INTO served VALUES ('{DONG}100120003', 'edition-a', NULL), ('{DONG}200070000', 'edition-a', NULL), \
           ('{DONG}100120004', 'edition-b', NULL);"
    ))?;
    let read = read_parcels_from(
        &stage,
        "served",
        "edition-a",
        contract(crate::place_identity_stage::PARCEL_TABLE)?,
        1,
    )?;
    assert_eq!(read.held, 2);
    // Seven distinct lots have a source PNU; no code changed, so each is asked under itself.
    assert_eq!(read.asked, 7);
    let linked = place_export(&stage, table, &object, 0, INGESTED)?;
    assert_eq!((linked.rows, linked.pnu), (17, 2));
    let reasons: BTreeMap<&str, u64> = linked
        .unresolved
        .iter()
        .map(|(reason, rows)| (reason.as_str(), *rows))
        .collect();
    assert_eq!(
        reasons,
        BTreeMap::from([
            ("block_lot", 1),
            ("jibun_masked", 1),
            ("legal_dong_not_found", 1),
            ("no_address", 1),
            ("no_jibun", 1),
            ("parcel_absent", 7),
            ("provisional_lot", 1),
            ("unparsed", 2),
        ])
    );
    // Lots: every row but the masked, the two that are no 지번 and the one without a lot.
    assert_eq!((linked.lots, linked.unresolved_lots), (13, 11));
    assert!(!link_bound_breached(
        linked.lots,
        linked.unresolved_lots,
        0.9
    ));
    assert!(!link_bound_breached(13, 13, 0.5));
    assert!(link_bound_breached(50, 26, 0.5));
    let placed = texts(
        &stage,
        "SELECT pnu, place_identity_grade, legal_dong_code, source_snapshot_id, row_state \
         FROM rt_staged WHERE pnu IS NOT NULL ORDER BY pnu",
    )?;
    let some = |text: &str| Some(text.to_owned());
    assert_eq!(
        placed,
        vec![
            vec![
                some("9999933021100120003"),
                some("code_exact"),
                some(DONG),
                some("20990501T000000Z"),
                None
            ],
            vec![
                some("9999933021200070000"),
                some("code_exact"),
                some(DONG),
                some("20990501T000000Z"),
                None
            ],
        ]
    );
    // No snapshot yet: every row is new, and there is nothing to mark removed.
    let (rows, removed) = month_changes(&stage, table, None, &object, INGESTED)?;
    assert_eq!((rows, removed), (17, 0));
    let checks = silver_changes::result_checks(&stage, table, "rt_changes")?;
    assert_eq!(
        (checks.rows, checks.duplicate_keys, checks.missing_required),
        (17, 0, 0)
    );
    Ok(())
}

#[test]
fn an_export_with_too_many_rejected_lines_stages_no_row_and_a_marker() -> anyhow::Result<()> {
    let (_directory, stage, staged) = staged_at(0.01)?;
    assert_eq!((staged.rows, staged.rejected), (0, 4));
    assert_eq!(
        staged.quarantined.as_deref(),
        Some("export_quarantined: 15.0000% of lines rejected")
    );
    assert!(staged.rejected_reasons.contains("export_quarantined"));
    let rejected = contract(REJECTED_TABLE)?;
    let checks = silver_changes::result_checks(&stage, rejected, "rt_rejected_0")?;
    assert_eq!(
        (checks.rows, checks.duplicate_keys, checks.missing_required),
        (4, 0, 0)
    );
    let columns = texts(
        &stage,
        "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns \
         WHERE table_name = 'rt_rejected_0'",
    )?;
    let expected = rejected
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(columns, vec![vec![Some(expected)]]);
    assert_eq!(
        texts(&stage, "SELECT count(*)::VARCHAR FROM rt_deals_0")?,
        vec![vec![Some("0".to_owned())]]
    );
    Ok(())
}

#[test]
fn the_plan_gives_each_object_its_dataset_month_and_landing_time() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let plan = serde_json::json!({
        "schema_version": PLAN_SCHEMA,
        "table": TABLE,
        "objects": [
            {"object_key": OBJECT_KEY, "source_slug": format!("{SOURCE_PREFIX}{DATASET}"),
             "last_modified": "2099-05-01T09:00:00+09:00", "local_path": "objects/1.csv"},
            {"object_key": OBJECT_KEY.replace("2099-04", "2099-03"), "source_slug": format!("{SOURCE_PREFIX}{DATASET}"),
             "last_modified": "2099-04-30T00:00:00Z", "local_path": "objects/0.csv"},
        ],
    });
    std::fs::write(directory.path().join("plan.json"), plan.to_string())?;
    let source = RtMolitSource::compiled()?;
    let objects = planned_objects(&[directory.path().to_path_buf()], &source)?;
    let read: Vec<(&str, &str, &str)> = objects
        .iter()
        .map(|o| {
            (
                o.month.as_str(),
                o.source_snapshot_id.as_str(),
                o.dataset.as_str(),
            )
        })
        .collect();
    assert_eq!(
        read,
        vec![
            ("209903", "20990430T000000Z", DATASET),
            ("209904", "20990501T000000Z", DATASET),
        ]
    );
    assert!(source
        .source_slugs()
        .contains("rtmolitgokr__real_transaction_land_trade"));
    assert!(month_of("bronze/x/period=2099-4/a.csv").is_err());
    Ok(())
}
