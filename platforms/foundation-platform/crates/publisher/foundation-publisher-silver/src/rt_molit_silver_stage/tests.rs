//! The load on a synthetic export (`fixtures/export.utf8.csv`: codes 99999, 2099 dates, every line
//! `synthetic`) against what main's Spark job functions made of the same bytes
//! (`fixtures/spark_rows.jsonl`: `rows_of`, then `legal_dong_code`, `lot_of` and `candidate_pnu` per
//! row; `fixtures/spark_rejected.json`; a row's `row_key` is written there as `name_and_rank`, a
//! field name the secret scan does not read as a credential). The export holds the cases a day's data may not: a quoted
//! field with a comma, a quote and a line end, rows of one name ranked by their cancellation, a
//! short record, another export's row, another month's row, and each kind of lot.

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

fn fixture(name: &str) -> anyhow::Result<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/rt_molit_silver_stage/fixtures")
        .join(name);
    std::fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))
}

fn object() -> PlannedObject {
    PlannedObject {
        object_key: OBJECT_KEY.to_owned(),
        dataset: DATASET.to_owned(),
        month: MONTH.to_owned(),
        source_snapshot_id: "20990501T000000Z".to_owned(),
        path: PathBuf::new(),
    }
}

/// The export as the provider sends it: CP949 bytes.
fn export_bytes(text: &str) -> Vec<u8> {
    encoding_rs::EUC_KR.encode(text).0.into_owned()
}

fn parsed() -> anyhow::Result<(RtMolitSource, Parsed)> {
    let source = RtMolitSource::compiled()?;
    let parsed = parse_export(
        &export_bytes(&fixture("export.utf8.csv")?),
        &object(),
        &source,
    );
    Ok((source, parsed))
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

/// A stage with the code tables of one 법정동 and the staged export.
fn staged() -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage, Staged)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(&format!(
        "CREATE TABLE code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
           effective_date VARCHAR, source VARCHAR); \
         CREATE TABLE code_names AS SELECT * FROM (VALUES ('9999900000', '합성도 합성시', '존재'), \
           ('{DONG}', '{DONG_NAME}', '존재')) AS t(region_cd, full_name, status);"
    ))?;
    reference_from_loaded(&stage)?;
    let (source, parsed) = parsed()?;
    let staged = stage_export(
        &stage,
        contract(TABLE)?,
        &object(),
        &parsed,
        0,
        (&source, INGESTED),
    )?;
    Ok((directory, stage, staged))
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

#[test]
fn the_rows_are_the_rows_the_spark_job_named() -> anyhow::Result<()> {
    let (_, parsed) = parsed()?;
    let expected = spark_rows()?;
    assert!(!parsed.refused);
    assert_eq!(parsed.rows.len(), expected.len());
    for (row, spark) in parsed.rows.iter().zip(&expected) {
        let spark = spark.as_object().context("a fixture row is an object")?;
        for (name, value) in spark.iter().filter(|(name, _)| !name.starts_with('_')) {
            let name = if name == NAME_AND_RANK {
                "row_key"
            } else {
                name.as_str()
            };
            // Python wrote every column it read; a column this row does not carry is null there.
            let ours = row.values.get(name).unwrap_or(&Value::Null);
            assert_eq!(ours, value, "line {} column {name}", row.line);
        }
        for name in row.values.keys() {
            let name = if name == "row_key" {
                NAME_AND_RANK
            } else {
                name.as_str()
            };
            assert!(spark.contains_key(name), "line {} adds {name}", row.line);
        }
    }
    Ok(())
}

#[test]
fn the_lines_not_loaded_are_the_spark_jobs() -> anyhow::Result<()> {
    let (_, parsed) = parsed()?;
    let expected: Value = serde_json::from_str(&fixture("spark_rejected.json")?)?;
    let ours: Vec<Value> = parsed
        .rejected
        .iter()
        .map(|line| serde_json::json!([line.line, line.raw, line.reason]))
        .collect();
    assert_eq!(Value::Array(ours), expected["rejected"]);
    assert_eq!(
        serde_json::json!(parsed.unrepresented),
        expected["unrepresented"]
    );
    Ok(())
}

#[test]
fn an_export_that_is_not_the_months_csv_is_refused_whole() -> anyhow::Result<()> {
    let source = RtMolitSource::compiled()?;
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
        (vec![0xff, 0xff, 0x22], "is not EUC-KR text"),
    ];
    for (bytes, reason) in cases {
        let parsed = parse_export(&bytes, &object(), &source);
        assert!(parsed.refused && parsed.rows.is_empty(), "{reason}");
        let line = parsed
            .rejected
            .first()
            .context("a refused export has its line 0")?;
        assert_eq!(line.line, 0);
        assert!(
            line.reason.starts_with("export_refused: ") && line.reason.contains(reason),
            "{}",
            line.reason
        );
    }
    Ok(())
}

#[test]
fn a_record_is_read_as_pythons_csv_reader_reads_it() {
    let read = export::records_for_test("a,\"b,\"\"c\"\"\r\nd\",e\"f\r\n\r\n,\n\"x\"y,z");
    let cells: Vec<Vec<&str>> = read
        .iter()
        .map(|(cells, _)| cells.iter().map(String::as_str).collect())
        .collect();
    assert_eq!(
        cells,
        vec![
            vec!["a", "b,\"c\"\r\nd", "e\"f"],
            vec![],
            vec!["", ""],
            vec!["xy", "z"],
        ]
    );
    assert_eq!(
        read.first().map(|(_, raw)| raw.as_str()),
        Some("a,\"b,\"\"c\"\"\r\nd\",e\"f")
    );
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
            let code = text(&row["_legal_dong_code"]);
            vec![
                text(&row["source_line_number"]),
                text(&row[NAME_AND_RANK]),
                text(&row["_reason"]),
                text(&row["_source_pnu"]),
                code,
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
    let linked = place_export(&stage, table, &object(), 0, INGESTED)?;
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
    let (rows, removed) = month_changes(&stage, table, None, &object(), INGESTED)?;
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
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(
        "CREATE TABLE code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
           effective_date VARCHAR, source VARCHAR); \
         CREATE TABLE code_names (region_cd VARCHAR, full_name VARCHAR, status VARCHAR);",
    )?;
    reference_from_loaded(&stage)?;
    let (mut source, parsed) = parsed()?;
    source.max_rejected_share = 0.01;
    let staged = stage_export(
        &stage,
        contract(TABLE)?,
        &object(),
        &parsed,
        0,
        (&source, INGESTED),
    )?;
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
