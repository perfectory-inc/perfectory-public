//! The plans and the run rows of the intelligence document jobs. Reserved 99999 codes and 2099
//! dates only.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};
use serde_json::{json, Value};

use super::{
    export_fold_inputs, media_key, plan_facts, plan_text, relation, stage_run_rows,
    ATTACHMENT_TABLE, COMPLEX_FACTS, DETAIL_TABLE, FACT, FACT_OUTCOME, FOLD_CODES_FILE,
    FOLD_ROWS_FILE, INDEX_FILE, MEDIA_FILE, TEXT_OUTCOME, TEXT_PAGE,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no contract {table}"))
}

/// The intelligence contract the image carries, as the repository holds it.
fn extraction() -> anyhow::Result<Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../intelligence-platform/config/document-extraction.contract.json");
    Ok(serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?,
    )?)
}

/// The stage and a directory inside it: the only place the stage's `DuckDB` may read a file.
fn stage_and_work(root: &Path) -> anyhow::Result<(DuckDbParquetStage, PathBuf)> {
    let stage = DuckDbParquetStage::open(root)?;
    let work = root.join(
        std::fs::read_dir(root)?
            .next()
            .context("the stage directory")??
            .file_name(),
    );
    Ok((stage, work))
}

fn hash(digit: char) -> String {
    digit.to_string().repeat(64)
}

/// Rows of the attachment index: `(notice, hash digit, format, bytes)`.
fn attachments(rows: &[(i64, char, &str, i64)]) -> anyhow::Result<String> {
    let columns = contract(ATTACHMENT_TABLE)?
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("notice_sequence", _) => "v.notice AS notice_sequence".to_owned(),
            ("sha256", _) => "v.sha AS sha256".to_owned(),
            ("media_format", _) => "v.format AS media_format".to_owned(),
            ("size_bytes", _) => "v.bytes AS size_bytes".to_owned(),
            ("source_snapshot_id", _) => "'s1' AS source_snapshot_id".to_owned(),
            (name, "int") => format!("CAST(1 AS INTEGER) AS {name}"),
            (name, "long") => format!("CAST(1 AS BIGINT) AS {name}"),
            (name, "timestamp") => format!("to_timestamp(4070908800) AS {name}"),
            (name, _) => format!("'x' AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let values = rows
        .iter()
        .map(|(notice, digit, format, bytes)| {
            format!("({notice}, '{}', '{format}', {bytes})", hash(*digit))
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {columns} FROM (VALUES {values}) v(notice, sha, format, bytes))"
    ))
}

/// Rows of the text outcomes: `(hash digit, outcome, error, run)`, of the contract's version.
fn text_outcomes(
    version: &str,
    rows: &[(char, &str, Option<&str>, &str)],
) -> anyhow::Result<String> {
    let columns = contract(TEXT_OUTCOME)?
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("sha256", _) => "v.sha AS sha256".to_owned(),
            ("extractor_version", _) => format!("'{version}' AS extractor_version"),
            ("outcome", _) => "v.outcome AS outcome".to_owned(),
            ("error", _) => "v.error AS error".to_owned(),
            ("run_id", _) => "v.run AS run_id".to_owned(),
            ("media_format", _) => "'pdf' AS media_format".to_owned(),
            (name, "int") => format!("CAST(1 AS INTEGER) AS {name}"),
            (name, "long") => format!("CAST(1 AS BIGINT) AS {name}"),
            (name, "timestamp") => format!("to_timestamp(4070908800) AS {name}"),
            (name, _) => format!("'x' AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let values = rows
        .iter()
        .map(|(digit, outcome, error, run)| {
            format!(
                "('{}', '{outcome}', {}, '{run}')",
                hash(*digit),
                error.map_or_else(
                    || "CAST(NULL AS VARCHAR)".to_owned(),
                    |text| format!("'{text}'")
                )
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {columns} FROM (VALUES {values}) v(sha, outcome, error, run))"
    ))
}

fn lines(path: &Path) -> anyhow::Result<Vec<String>> {
    Ok(std::fs::read_to_string(path)
        .with_context(|| format!("cannot read {}", path.display()))?
        .lines()
        .map(str::to_owned)
        .collect())
}

#[test]
fn the_text_plan_is_the_documents_with_no_outcome_and_the_memory_kills_with_attempts_left(
) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let (stage, work) = stage_and_work(root.path())?;
    let extraction = extraction()?;
    let version = extraction["document_text"]["extractor_version"]
        .as_str()
        .context("the text version")?;
    let prefix = extraction["document_text"]["memory_kill"]["error_prefixes"][0]
        .as_str()
        .context("a memory kill prefix")?;
    let attempts = extraction["document_text"]["memory_kill"]["attempts"]
        .as_u64()
        .context("the attempts")?;
    let kill = format!("{prefix} at page 9");
    // a: never read. b: read. c: one memory kill, read again. d: killed as often as allowed.
    // e: failed for another reason, done. f: a format the job does not read, still planned.
    let index = attachments(&[
        (99_001, 'a', "pdf", 100),
        (99_002, 'a', "pdf", 100),
        (99_003, 'b', "pdf", 100),
        (99_004, 'c', "pdf", 40),
        (99_005, 'd', "pdf", 100),
        (99_006, 'e', "pdf", 100),
        (99_007, 'f', "zip", 7),
    ])?;
    let mut outcomes = vec![
        ('b', "text", None, "r1"),
        ('c', "failed", Some(kill.as_str()), "r1"),
        ('e', "failed", Some("the file is not a PDF"), "r1"),
    ];
    let runs: Vec<String> = (0..attempts).map(|run| format!("r{run}")).collect();
    for run in &runs {
        outcomes.push(('d', "failed", Some(kill.as_str()), run.as_str()));
    }
    let summary = plan_text(
        &stage,
        contract(ATTACHMENT_TABLE)?,
        contract(TEXT_OUTCOME)?,
        &index,
        &text_outcomes(version, &outcomes)?,
        &extraction,
        "media/store/",
        &work.join("plan"),
    )?;
    assert_eq!(
        (
            &summary["owed"],
            &summary["planned"],
            &summary["memory_kill_exhausted"]
        ),
        (&json!(3), &json!(3), &json!(1))
    );
    // Only the bytes of the formats the job reads are fetched: a (100) and c (40).
    assert_eq!(summary["bytes_to_fetch"], json!(140));
    let planned: Vec<Value> = lines(&work.join("plan").join(INDEX_FILE))?
        .iter()
        .map(|line| serde_json::from_str(line))
        .collect::<Result<_, _>>()?;
    // Newest notice first; a document two notices attach is one, under its newest notice.
    assert_eq!(
        planned
            .iter()
            .map(|row| (
                row["notice_sequence"].as_i64(),
                row["sha256"].as_str().map(|sha| &sha[..1])
            ))
            .collect::<Vec<_>>(),
        [
            (Some(99_007), Some("f")),
            (Some(99_004), Some("c")),
            (Some(99_002), Some("a"))
        ]
    );
    assert_eq!(
        lines(&work.join("plan").join(MEDIA_FILE))?,
        [
            format!("media/store/sha256/{0}.pdf\t{0}.pdf", hash('c')),
            format!("media/store/sha256/{0}.pdf\t{0}.pdf", hash('a')),
        ]
    );
    // Planted: a hash that is not a digest, a format that is not a word.
    assert!(media_key("p", "zz", "pdf").is_err());
    assert!(media_key("p", &hash('a'), "p/df").is_err());
    Ok(())
}

#[test]
fn facts_are_owed_where_the_text_is_of_a_later_run_than_the_facts() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let (stage, work) = stage_and_work(root.path())?;
    let extraction = extraction()?;
    let text_version = extraction["document_fact"]["text_extractor_version"]
        .as_str()
        .context("the text version")?;
    let fact_version = extraction["document_fact"]["extractor_version"]
        .as_str()
        .context("the fact version")?;
    // a: text, no facts. b: facts of the same run as its text. c: read again after its facts.
    let text = text_outcomes(
        text_version,
        &[
            ('a', "text", None, "r1"),
            ('b', "text", None, "r1"),
            ('c', "text", None, "r1"),
            ('c', "ocr", None, "r3"),
        ],
    )?;
    let fact_columns = contract(FACT_OUTCOME)?
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("sha256", _) => "v.sha AS sha256".to_owned(),
            ("extractor_version", _) => format!("'{fact_version}' AS extractor_version"),
            ("run_id", _) => "v.run AS run_id".to_owned(),
            (name, "int") => format!("CAST(1 AS INTEGER) AS {name}"),
            (name, "long") => format!("CAST(1 AS BIGINT) AS {name}"),
            (name, "timestamp") => format!("to_timestamp(4070908800) AS {name}"),
            (name, _) => format!("'x' AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let facts = format!(
        "(SELECT {fact_columns} FROM (VALUES ('{}', 'r1'), ('{}', 'r2')) v(sha, run))",
        hash('b'),
        hash('c')
    );
    let page_columns = contract(TEXT_PAGE)?
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("sha256", _) => "v.sha AS sha256".to_owned(),
            ("extractor_version", _) => format!("'{text_version}' AS extractor_version"),
            ("page_number", _) => "v.page AS page_number".to_owned(),
            ("method", _) => "v.method AS method".to_owned(),
            (name, "int") => format!("CAST(1 AS INTEGER) AS {name}"),
            (name, "long") => format!("CAST(1 AS BIGINT) AS {name}"),
            (name, _) => format!("'x' AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let pages = format!(
        "(SELECT {page_columns} FROM (VALUES ('{a}', 1, 'text_layer'), ('{c}', 1, 'ocr'), ('{c}', 2, 'text_layer'), \
         ('{b}', 1, 'ocr')) v(sha, page, method))",
        a = hash('a'),
        b = hash('b'),
        c = hash('c')
    );
    let summary = plan_facts(
        &stage,
        contract(TEXT_OUTCOME)?,
        contract(FACT_OUTCOME)?,
        &text,
        &facts,
        &pages,
        &extraction,
        "media/store",
        &work.join("plan"),
    )?;
    assert_eq!(
        (
            &summary["owed"],
            &summary["planned"],
            &summary["pages"],
            &summary["documents_to_fetch"]
        ),
        (&json!(2), &json!(2), &json!(3), &json!(1))
    );
    let planned: Vec<Value> = lines(&work.join("plan").join("document_text_outcome.jsonl"))?
        .iter()
        .map(|line| serde_json::from_str(line))
        .collect::<Result<_, _>>()?;
    assert_eq!(planned.len(), 2);
    // The current text outcome of c is its later run, and the instant is written as the image reads it.
    let again = planned
        .iter()
        .find(|row| row["sha256"] == json!(hash('c')))
        .context("c is owed")?;
    assert_eq!(again["outcome"], json!("ocr"));
    assert_eq!(again["extracted_at_utc"], json!("2099-01-01T00:00:00Z"));
    // Only the document with an OCR'd page is fetched for the vision read.
    assert_eq!(
        lines(&work.join("plan").join(MEDIA_FILE))?,
        [format!("media/store/sha256/{0}.pdf\t{0}.pdf", hash('c'))]
    );
    Ok(())
}

#[test]
fn a_runs_rows_are_the_contracts_columns_stamped_with_the_run_or_refused() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let (stage, work) = stage_and_work(root.path())?;
    let table = contract(TEXT_OUTCOME)?;
    let mut columns: Vec<String> = extraction()?["outputs"]["document_text_outcome"]["columns"]
        .as_array()
        .context("the output's columns")?
        .iter()
        .filter_map(|column| column.as_str().map(str::to_owned))
        .collect();
    columns.push("run_id".to_owned());
    let row = |extra: &str| {
        format!(
            "{{\"sha256\": \"{}\", \"extractor_version\": \"v\", \"media_format\": \"pdf\", \"outcome\": \"text\", \
             \"page_count\": 3, \"ocr_page_count\": 0, \"char_count\": 99, \"error\": null, \"elapsed_ms\": 12, \
             \"extracted_at_utc\": \"2099-01-01T00:00:00Z\"{extra}}}\n",
            hash('a')
        )
    };
    let path = work.join("document_text_outcome.jsonl");
    // A run that wrote no file has no rows.
    assert_eq!(stage_run_rows(&stage, table, &columns, &path, "r1")?, 0);
    std::fs::write(&path, format!("{}\n{}", row(""), row("")))?;
    assert_eq!(stage_run_rows(&stage, table, &columns, &path, "r1")?, 2);
    let (run, error, pages, instant): (String, Option<String>, i32, i64) = stage.connection().query_row(
        "SELECT run_id, error, page_count, epoch_us(extracted_at_utc) // 1000000 FROM id_run_rows LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!(
        (run.as_str(), error, pages, instant),
        ("r1", None, 3, 4_070_908_800)
    );
    // Planted: a column the contract does not name; an image that writes other columns.
    std::fs::write(&path, row(", \"surprise\": 1"))?;
    assert!(stage_run_rows(&stage, table, &columns, &path, "r1").is_err());
    let mut other = columns.clone();
    other.insert(0, "surprise".to_owned());
    assert!(stage_run_rows(&stage, table, &other, &path, "r1").is_err());
    Ok(())
}

/// Two files, two writers: the intelligence contract names the columns its image writes, the
/// lakehouse contract the columns the tables hold. A drift is refused at append; this holds them
/// equal before that.
#[test]
fn every_output_table_is_the_intelligence_columns_then_the_run() -> anyhow::Result<()> {
    let extraction = extraction()?;
    for (table, output) in [
        (TEXT_OUTCOME, "document_text_outcome"),
        (TEXT_PAGE, "document_text_page"),
        (FACT, "document_fact"),
        (FACT_OUTCOME, "document_fact_outcome"),
    ] {
        let declared = &extraction["outputs"][output];
        assert_eq!(declared["table"], json!(table));
        let mut columns: Vec<&str> = declared["columns"]
            .as_array()
            .context("columns")?
            .iter()
            .filter_map(Value::as_str)
            .collect();
        columns.push("run_id");
        let held = contract(table)?;
        assert_eq!(
            held.columns
                .iter()
                .map(|column| column.name)
                .collect::<Vec<_>>(),
            columns,
            "{table}"
        );
        let latest = held.current_row_latest().context("a newest row per key")?;
        assert_eq!(
            json!(latest.key),
            declared["key"],
            "{table}: the key is the intelligence key"
        );
        assert_eq!(latest.newest_first, ["run_id"], "{table}");
    }
    // The fold's table ends with the run the publisher stamps.
    assert_eq!(
        contract(COMPLEX_FACTS)?
            .columns
            .last()
            .map(|column| column.name),
        Some("run_id")
    );
    Ok(())
}

/// Rows of `table`: the `named` columns from `values`, every other column one fixed value of its type.
fn rows_of(table: &str, named: &[&str], values: &str) -> anyhow::Result<String> {
    let columns = contract(table)?
        .columns
        .iter()
        .map(|column| {
            let name = column.name;
            if named.contains(&name) {
                return format!("v.\"{name}\" AS \"{name}\"");
            }
            match column.logical_type {
                "int" => format!("CAST(1 AS INTEGER) AS \"{name}\""),
                "long" => format!("CAST(1 AS BIGINT) AS \"{name}\""),
                "date" => format!("DATE '2099-01-01' AS \"{name}\""),
                "timestamp" => format!("to_timestamp(4070908800) AS \"{name}\""),
                _ => format!("'x' AS \"{name}\""),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let aliases = named
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {columns} FROM (VALUES {values}) v({aliases}))"
    ))
}

/// A fold covers what changed and leaves nothing behind: a complex whose facts are of a later run
/// than the run that folded it, or that no run folded, is folded by the next run, whatever that
/// run read (2026-11-21: 32 complexes had facts of runs whose fold was skipped, and no row).
#[test]
fn a_fold_makes_up_the_complexes_an_earlier_run_left_behind() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let (stage, work) = stage_and_work(root.path())?;
    let extraction = extraction()?;
    let version = extraction["document_fact"]["extractor_version"]
        .as_str()
        .context("the fact version")?;
    let (a, b, c, d) = (hash('a'), hash('b'), hash('c'), hash('d'));
    let attachments = rows_of(
        ATTACHMENT_TABLE,
        &["notice_sequence", "sha256", "official_complex_code"],
        &format!(
            "(CAST(99001 AS BIGINT), '{a}', '999991'), (99002, '{b}', '999992'), \
             (99003, '{c}', '999993'), (99004, '{d}', '999994')"
        ),
    )?;
    // 999991: facts of r2, folded by r1 (the fold of r2 was skipped). 999992: facts, never folded.
    // 999993: folded after its facts. 999994: folded by the run of its facts.
    let facts = rows_of(
        FACT,
        &["sha256", "extractor_version", "fact_kind", "run_id"],
        &format!(
            "('{a}', '{version}', 'land_use_area', 'r2'), ('{b}', '{version}', 'land_use_area', 'r1'), \
             ('{c}', '{version}', 'land_use_area', 'r1'), ('{d}', '{version}', 'land_use_area', 'r5')"
        ),
    )?;
    let held = rows_of(
        COMPLEX_FACTS,
        &[
            "official_complex_code",
            "fact_extractor_version",
            "accuracy_gate_json",
            "run_id",
        ],
        &format!(
            "('999991', '{version}', '[]', 'r1'), ('999993', '{version}', '[]', 'r3'), \
             ('999994', '{version}', '[]', 'r5')"
        ),
    )?;
    let details = relation(&stage, contract(DETAIL_TABLE)?, None)?;
    let pages = relation(&stage, contract(TEXT_PAGE)?, None)?;
    // Run r9 read nothing: its own scope is empty.
    let inputs = export_fold_inputs(
        &stage,
        [
            contract(COMPLEX_FACTS)?,
            contract(ATTACHMENT_TABLE)?,
            contract(FACT)?,
            contract(DETAIL_TABLE)?,
            contract(TEXT_PAGE)?,
        ],
        [&held, &attachments, &facts, &details, &pages],
        &extraction,
        "[]",
        "r9",
        false,
        &work,
    )?;
    assert!(!inputs.whole);
    assert_eq!(
        lines(&work.join(FOLD_CODES_FILE))?,
        ["999991", "999992"],
        "the two left behind, and neither of the two that are folded"
    );
    assert_eq!((inputs.complexes, inputs.rows), (2, 2));
    let folded: Vec<Value> = lines(&work.join(FOLD_ROWS_FILE))?
        .iter()
        .map(|line| serde_json::from_str(line))
        .collect::<Result<_, _>>()?;
    assert_eq!(
        folded
            .iter()
            .map(|row| row["official_complex_code"].as_str())
            .collect::<Vec<_>>(),
        [Some("999991"), Some("999992")]
    );
    // A gate the held rows were not folded under is the declared full refold: every complex.
    let inputs = export_fold_inputs(
        &stage,
        [
            contract(COMPLEX_FACTS)?,
            contract(ATTACHMENT_TABLE)?,
            contract(FACT)?,
            contract(DETAIL_TABLE)?,
            contract(TEXT_PAGE)?,
        ],
        [&held, &attachments, &facts, &details, &pages],
        &extraction,
        "[[\"land_use_area\", \"rule\", \"text_layer\"]]",
        "r9",
        false,
        &work,
    )?;
    assert!(inputs.whole);
    assert_eq!(inputs.complexes, 4);
    Ok(())
}
