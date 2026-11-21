//! The lakehouse half of the intelligence document jobs (root ADR-0477), computed by `DuckDB`
//! inside the publisher (root ADR-0559): what is owed, the rows a run wrote, and the rows the
//! panel fold reads.
//!
//! The extraction itself runs in the intelligence image on local files and never touches the
//! lakehouse. Owed is computed from the tables, never from a stored queue (root ADR-0352): a hash
//! of the attachment index with no text outcome of this extractor version, or whose current one
//! is a memory kill with attempts left (root ADR-0593), newest notice first, up to the contract's
//! per-run limit; for facts, a hash whose current text outcome is of a later run than its current
//! fact outcome, or has none. It was `intelligence_documents.py` on Spark; the files it writes
//! are the same files.

use std::{
    fs::File,
    io::{BufWriter, Write as _},
    path::Path,
};

use anyhow::{bail, ensure, Context};
use lakehouse_domain::LakehouseTableContract;
use serde_json::{json, Value};

use crate::{
    data_go_kr_file_data_stage::literal,
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG},
};

/// The attachment index the text job plans from.
pub const ATTACHMENT_TABLE: &str = "silver.industrial_complex_notice_attachment";
/// One row per document a text run finished.
pub const TEXT_OUTCOME: &str = "intelligence.document_text_outcome";
/// The pages of a document a text run read.
pub const TEXT_PAGE: &str = "intelligence.document_text_page";
/// The facts a fact run read.
pub const FACT: &str = "intelligence.document_fact";
/// One row per document a fact run finished.
pub const FACT_OUTCOME: &str = "intelligence.document_fact_outcome";
/// The notices the attachments belong to.
pub const DETAIL_TABLE: &str = "silver.industrial_complex_notice_detail";
/// The facts a complex's panel may state, one row per complex and fold.
pub const COMPLEX_FACTS: &str = "gold.complex_document_facts";
/// The kinds a complex's panel states (`intelligence_documents.PANEL_KINDS`).
pub const PANEL_KINDS: [&str; 3] = ["land_use_area", "invited_industry", "sale_price"];

/// The plan files a run reads.
pub const INDEX_FILE: &str = "index.jsonl";
/// `<object key>\t<file name>` per line: the bytes a run fetches.
pub const MEDIA_FILE: &str = "media.tsv";

/// `contract`'s table at `head` with every contract column, or a relation of no row for a table
/// with no snapshot.
///
/// # Errors
/// `DuckDB` cannot describe the table at that snapshot.
pub fn relation(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
) -> anyhow::Result<String> {
    match head {
        Some(head) => with_contract_columns(
            stage.connection(),
            contract,
            &format!(
                "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
                contract.table_name
            ),
        ),
        None => Ok(format!(
            "(SELECT {} WHERE false)",
            contract
                .columns
                .iter()
                .map(|column| Ok(format!(
                    "CAST(NULL AS {}) AS {}",
                    duckdb_type(column.logical_type)?,
                    column.name
                )))
                .collect::<anyhow::Result<Vec<_>>>()?
                .join(", ")
        )),
    }
}

fn duckdb_type(logical_type: &str) -> anyhow::Result<&'static str> {
    Ok(match logical_type {
        "string" => "VARCHAR",
        "int" => "INTEGER",
        "long" => "BIGINT",
        "double" => "DOUBLE",
        "boolean" => "BOOLEAN",
        "date" => "DATE",
        "timestamp" => "TIMESTAMPTZ",
        other => bail!("no DuckDB type here for the contract type {other}"),
    })
}

fn current(
    contract: &LakehouseTableContract,
    source: &str,
    columns: &[&str],
) -> anyhow::Result<String> {
    Ok(contract
        .current_row_latest()
        .with_context(|| format!("{} keeps no newest row per key", contract.table_name))?
        .current_rows_sql(source, columns, "TRUE"))
}

fn text(value: &Value, path: &[&str]) -> anyhow::Result<String> {
    let mut found = value;
    for part in path {
        found = &found[*part];
    }
    found
        .as_str()
        .map(str::to_owned)
        .with_context(|| format!("the extraction contract has no text at {}", path.join(".")))
}

fn number(value: &Value, path: &[&str]) -> anyhow::Result<u64> {
    let mut found = value;
    for part in path {
        found = &found[*part];
    }
    found.as_u64().with_context(|| {
        format!(
            "the extraction contract has no number at {}",
            path.join(".")
        )
    })
}

fn texts(value: &Value, path: &[&str]) -> anyhow::Result<Vec<String>> {
    let mut found = value;
    for part in path {
        found = &found[*part];
    }
    found
        .as_array()
        .with_context(|| format!("the extraction contract has no list at {}", path.join(".")))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .with_context(|| format!("{} holds something that is not text", path.join(".")))
        })
        .collect()
}

/// Where the shared content-addressed store keeps a file (root ADR-0298).
///
/// # Errors
/// `sha256` is not a SHA-256 hex digest or `media_format` not a plain word.
pub fn media_key(prefix: &str, sha256: &str, media_format: &str) -> anyhow::Result<String> {
    ensure!(
        sha256.len() == 64
            && sha256
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')),
        "not a SHA-256 hex digest: {sha256:?}"
    );
    ensure!(
        !media_format.is_empty() && media_format.chars().all(char::is_alphanumeric),
        "not a media format: {media_format:?}"
    );
    Ok(format!(
        "{}/sha256/{sha256}.{media_format}",
        prefix.trim_matches('/')
    ))
}

fn create(path: &Path) -> anyhow::Result<BufWriter<File>> {
    Ok(BufWriter::new(File::create(path).with_context(|| {
        format!("cannot write {}", path.display())
    })?))
}

/// The rows of `query` as one JSON object per line of `path`; returns how many.
fn write_lines(stage: &DuckDbParquetStage, query: &str, path: &Path) -> anyhow::Result<u64> {
    let mut file = create(path)?;
    let mut rows = 0_u64;
    stage.each_json_row(query, |row| {
        file.write_all(row.as_bytes())?;
        file.write_all(b"\n")?;
        rows += 1;
        Ok(())
    })?;
    file.flush()
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(rows)
}

/// The media list of the rows of `query` (`sha256`, `media_format`); returns how many.
fn write_media(
    stage: &DuckDbParquetStage,
    query: &str,
    prefix: &str,
    path: &Path,
) -> anyhow::Result<u64> {
    let mut file = create(path)?;
    let mut rows = 0_u64;
    let mut statement = stage.connection().prepare(query)?;
    let mut found = statement.query([])?;
    while let Some(row) = found.next()? {
        let (sha256, media_format): (String, String) = (row.get(0)?, row.get(1)?);
        writeln!(
            file,
            "{}\t{sha256}.{media_format}",
            media_key(prefix, &sha256, &media_format)?
        )?;
        rows += 1;
    }
    file.flush()
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(rows)
}

fn count(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<u64> {
    let found: i64 = stage.connection().query_row(query, [], |row| row.get(0))?;
    Ok(u64::try_from(found)?)
}

fn in_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| literal(value))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `plan-text`: the documents owed a text read, newest notice first, as `index.jsonl` and
/// `media.tsv` in `out`. Returns the step's summary.
///
/// `attachments` and `outcomes` are the two tables pinned by the caller ([`relation`]).
///
/// # Errors
/// The extraction contract lacks a setting, a planned hash is not a digest, or `DuckDB` fails.
#[allow(clippy::too_many_arguments)]
pub fn plan_text(
    stage: &DuckDbParquetStage,
    attachment_contract: &LakehouseTableContract,
    outcome_contract: &LakehouseTableContract,
    attachments: &str,
    outcomes: &str,
    extraction: &Value,
    media_prefix: &str,
    out: &Path,
) -> anyhow::Result<Value> {
    let version = text(extraction, &["document_text", "extractor_version"])?;
    let limit = number(extraction, &["document_text", "per_run_limit"])?;
    let attempts = number(extraction, &["document_text", "memory_kill", "attempts"])?;
    let prefixes = texts(
        extraction,
        &["document_text", "memory_kill", "error_prefixes"],
    )?;
    ensure!(!prefixes.is_empty(), "memory_kill.error_prefixes is empty");
    let mut readable = texts(extraction, &["document_text", "pdf_formats"])?;
    readable.extend(texts(extraction, &["document_text", "converted_formats"])?);
    let index_columns = texts(extraction, &["inputs", "attachment_index", "columns"])?;
    let killed = prefixes
        .iter()
        .map(|prefix| format!("starts_with(error, {})", literal(prefix)))
        .collect::<Vec<_>>()
        .join(" OR ");
    stage.connection().execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE id_outcomes AS SELECT * FROM {outcomes} WHERE extractor_version = {version};
         -- One document per hash, however many notices attach it; the newest notice orders it.
         CREATE OR REPLACE TEMP TABLE id_documents AS
         SELECT sha256, media_format, max(notice_sequence) AS notice_sequence, max(size_bytes) AS size_bytes
         FROM ({attachments_now}) GROUP BY sha256, media_format;
         CREATE OR REPLACE TEMP TABLE id_current AS {current_outcomes};
         -- A memory kill says nothing about the document, only about the cap (root ADR-0593).
         CREATE OR REPLACE TEMP TABLE id_memory_kills AS
         SELECT o.sha256, a.attempts FROM id_current o
         JOIN (SELECT sha256, count(*) AS attempts FROM id_outcomes GROUP BY sha256) a USING (sha256)
         WHERE o.outcome = 'failed' AND o.error IS NOT NULL AND ({killed});
         CREATE OR REPLACE TEMP TABLE id_owed AS
         SELECT d.* FROM id_documents d WHERE NOT EXISTS (
           SELECT 1 FROM id_current o WHERE o.sha256 = d.sha256
             AND o.sha256 NOT IN (SELECT sha256 FROM id_memory_kills WHERE attempts < {attempts}));
         CREATE OR REPLACE TEMP TABLE id_planned AS
         SELECT * FROM id_owed ORDER BY notice_sequence DESC, sha256 LIMIT {limit};",
        version = literal(&version),
        attachments_now = current(
            attachment_contract,
            attachments,
            &["sha256", "media_format", "notice_sequence", "size_bytes"]
        )?,
        current_outcomes = current(outcome_contract, "id_outcomes", &["sha256", "outcome", "error"])?,
    ))?;
    std::fs::create_dir_all(out).with_context(|| format!("cannot create {}", out.display()))?;
    let planned = write_lines(
        stage,
        &format!(
            "SELECT {} FROM id_planned ORDER BY notice_sequence DESC, sha256",
            index_columns.join(", ")
        ),
        &out.join(INDEX_FILE),
    )?;
    let readable = in_list(&readable);
    write_media(
        stage,
        &format!(
            "SELECT sha256, media_format FROM id_planned WHERE media_format IN ({readable}) \
             ORDER BY notice_sequence DESC, sha256"
        ),
        media_prefix,
        &out.join(MEDIA_FILE),
    )?;
    Ok(json!({
        "step": "plan-text", "extractor_version": version,
        "owed": count(stage, "SELECT count(*) FROM id_owed")?,
        "planned": planned,
        // Memory kills no attempt is left for: said in the summary, not dropped in silence.
        "memory_kill_exhausted": count(stage, &format!("SELECT count(*) FROM id_memory_kills WHERE attempts >= {attempts}"))?,
        "bytes_to_fetch": count(stage, &format!(
            "SELECT CAST(coalesce(sum(size_bytes), 0) AS BIGINT) FROM id_planned WHERE media_format IN ({readable})"
        ))?,
    }))
}

/// `plan-facts`: the text outcomes owed facts, newest text first, with their pages, as
/// `document_text_outcome.jsonl`, `document_text_page.jsonl` and `media.tsv` in `out`.
///
/// Facts follow the newest text, so a document read again is owed when its current text outcome
/// is of a later run than its current fact outcome. The vision re-read draws the OCR'd pages of a
/// PDF: only those documents' bytes are listed.
///
/// # Errors
/// The extraction contract lacks a setting, or `DuckDB` fails.
#[allow(clippy::too_many_arguments)]
pub fn plan_facts(
    stage: &DuckDbParquetStage,
    text_contract: &LakehouseTableContract,
    fact_contract: &LakehouseTableContract,
    text_outcomes: &str,
    fact_outcomes: &str,
    pages: &str,
    extraction: &Value,
    media_prefix: &str,
    out: &Path,
) -> anyhow::Result<Value> {
    let version = text(extraction, &["document_fact", "extractor_version"])?;
    let text_version = text(extraction, &["document_fact", "text_extractor_version"])?;
    let limit = number(extraction, &["document_fact", "per_run_limit"])?;
    let vision = in_list(&texts(extraction, &["document_fact", "vision_formats"])?);
    let text_columns = texts(extraction, &["outputs", "document_text_outcome", "columns"])?;
    let page_columns = texts(extraction, &["outputs", "document_text_page", "columns"])?;
    let all_text: Vec<&str> = text_contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    stage.connection().execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE id_text AS {text_now};
         CREATE OR REPLACE TEMP TABLE id_facts AS {facts_now};
         CREATE OR REPLACE TEMP TABLE id_owed AS
         SELECT t.* FROM id_text t LEFT JOIN id_facts f USING (sha256)
         WHERE f.fact_run_id IS NULL OR f.fact_run_id < t.run_id;
         CREATE OR REPLACE TEMP TABLE id_planned AS
         SELECT * FROM id_owed ORDER BY extracted_at_utc DESC, sha256 LIMIT {limit};
         CREATE OR REPLACE TEMP TABLE id_pages AS
         SELECT * FROM {pages} WHERE extractor_version = {text_version}
           AND sha256 IN (SELECT sha256 FROM id_planned);",
        text_now = current(
            text_contract,
            &format!(
                "(SELECT * FROM {text_outcomes} WHERE extractor_version = {})",
                literal(&text_version)
            ),
            &all_text
        )?,
        facts_now = current(
            fact_contract,
            &format!(
                "(SELECT sha256, extractor_version, run_id FROM {fact_outcomes} WHERE extractor_version = {})",
                literal(&version)
            ),
            &["sha256", "run_id AS fact_run_id"]
        )?,
        text_version = literal(&text_version),
    ))?;
    std::fs::create_dir_all(out).with_context(|| format!("cannot create {}", out.display()))?;
    let planned = write_lines(
        stage,
        &format!(
            "SELECT {} FROM id_planned ORDER BY extracted_at_utc DESC, sha256",
            text_columns
                .iter()
                .map(|column| if column == "extracted_at_utc" {
                    "strftime(make_timestamp(epoch_us(extracted_at_utc)), '%Y-%m-%dT%H:%M:%SZ') AS extracted_at_utc".to_owned()
                } else {
                    column.clone()
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        &out.join("document_text_outcome.jsonl"),
    )?;
    let page_count = write_lines(
        stage,
        &format!(
            "SELECT {} FROM id_pages ORDER BY sha256, page_number",
            page_columns.join(", ")
        ),
        &out.join("document_text_page.jsonl"),
    )?;
    let fetched = write_media(
        stage,
        &format!(
            "SELECT p.sha256, p.media_format FROM id_planned p \
             WHERE p.media_format IN ({vision}) \
               AND p.sha256 IN (SELECT sha256 FROM id_pages WHERE method = 'ocr') ORDER BY p.sha256"
        ),
        media_prefix,
        &out.join(MEDIA_FILE),
    )?;
    Ok(json!({
        "step": "plan-facts", "extractor_version": version,
        "owed": count(stage, "SELECT count(*) FROM id_owed")?,
        "planned": planned, "pages": page_count, "documents_to_fetch": fetched,
    }))
}

/// Loads a run's file of `contract`'s table as the temp table `id_run_rows`, stamped with
/// `run_id`, in the table's columns and types; returns how many rows (0 for a file not there).
///
/// `columns` are the intelligence contract's columns of the output, then `run_id`: every line's
/// keys must be exactly the contract's, in order, so a newer image cannot write what the table
/// does not hold.
///
/// # Errors
/// The image writes other columns than the table holds, a line has other keys, an instant is not
/// `yyyy-MM-ddTHH:mm:ssZ`, or `DuckDB` fails.
pub fn stage_run_rows(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    columns: &[String],
    path: &Path,
    run_id: &str,
) -> anyhow::Result<u64> {
    let table = contract.table_name;
    let held: Vec<&str> = contract.columns.iter().map(|column| column.name).collect();
    ensure!(
        columns.iter().map(String::as_str).eq(held.iter().copied()),
        "{table}: the image writes {columns:?}, the lakehouse contract holds {held:?}"
    );
    stage
        .connection()
        .execute_batch("DROP TABLE IF EXISTS id_run_rows; DROP TABLE IF EXISTS id_run_lines")?;
    if !path.exists() {
        return Ok(0);
    }
    stage.load_lines(path, "id_run_lines")?;
    let declared = in_list(&columns[..columns.len() - 1]);
    let unlike: i64 = stage.connection().query_row(
        &format!(
            "SELECT count(*) FROM id_run_lines WHERE length(trim(line)) > 0 \
             AND json_keys(line) IS DISTINCT FROM [{declared}]"
        ),
        [],
        |row| row.get(0),
    )?;
    ensure!(
        unlike == 0,
        "{}: {unlike} lines have other columns than the contract's {:?}",
        path.display(),
        &columns[..columns.len() - 1]
    );
    let selected = contract
        .columns
        .iter()
        .map(|column| {
            let name = column.name;
            if name == "run_id" {
                return Ok(format!("{} AS run_id", literal(run_id)));
            }
            let raw = format!("json_extract_string(line, '$.{name}')");
            Ok(match column.logical_type {
                "string" => format!("{raw} AS {name}"),
                "int" => format!("CAST({raw} AS INTEGER) AS {name}"),
                "long" => format!("CAST({raw} AS BIGINT) AS {name}"),
                // No time zone database is loaded: the instant is UTC text, counted in microseconds.
                "timestamp" => format!(
                    "CASE WHEN {raw} IS NULL THEN NULL \
                     WHEN regexp_full_match({raw}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z') \
                     THEN to_timestamp(epoch_us(CAST(replace(replace({raw}, 'T', ' '), 'Z', '') AS TIMESTAMP)) // 1000000) \
                     ELSE error('{name} is not an instant like 2099-01-01T00:00:00Z: ' || {raw}) END AS {name}"
                ),
                other => bail!("{table}.{name}: a run file column of type {other} has no reader"),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    stage.connection().execute_batch(&format!(
        "CREATE TEMP TABLE id_run_rows AS SELECT {selected} FROM id_run_lines \
         WHERE length(trim(line)) > 0 ORDER BY line_number"
    ))?;
    count(stage, "SELECT count(*) FROM id_run_rows")
}

/// What the fold reads, as files in `out`: which complexes are in scope and the joined fact rows.
pub struct FoldInputs {
    /// Whether every complex is refolded (`--all`, or rows folded under another gate or version).
    pub whole: bool,
    /// The complexes in scope.
    pub complexes: u64,
    /// The joined fact rows written.
    pub rows: u64,
}

/// The file of the complexes in scope, one code per line.
pub const FOLD_CODES_FILE: &str = "fold-codes.txt";
/// The file of the joined fact rows the fold rule reads, one JSON object per line.
pub const FOLD_ROWS_FILE: &str = "fold-rows.jsonl";

/// `fold-complex-facts`, the read: the complexes in scope and their joined fact rows.
///
/// In scope are the complexes whose attachments got facts in run `run_id`; every complex with
/// `all`, or when the table holds rows folded under another gate or fact version (the declared
/// full refold, root ADR-0477 §3). Each fact is joined to its attachment, its notice and the page
/// it was read on.
///
/// `gate_json` is the admitted `(fact_kind, method, page_method)` list as the fold writes it.
///
/// # Errors
/// The extraction contract lacks a setting, or `DuckDB` fails.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn export_fold_inputs(
    stage: &DuckDbParquetStage,
    contracts: [&LakehouseTableContract; 5],
    relations: [&str; 5],
    extraction: &Value,
    gate_json: &str,
    run_id: &str,
    all: bool,
    out: &Path,
) -> anyhow::Result<FoldInputs> {
    let [held_contract, attachment_contract, fact_contract, detail_contract, page_contract] =
        contracts;
    let [held, attachments, facts, details, pages] = relations;
    let version = text(extraction, &["document_fact", "extractor_version"])?;
    let kinds = in_list(&PANEL_KINDS.map(str::to_owned));
    stage.connection().execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE id_held AS {held_now};
         CREATE OR REPLACE TEMP TABLE id_attachments AS {attachments_now};
         CREATE OR REPLACE TEMP TABLE id_fact_rows AS
         SELECT * FROM ({facts_now}) WHERE extractor_version = {version} AND fact_kind IN ({kinds});",
        held_now = current(
            held_contract,
            held,
            &["official_complex_code", "accuracy_gate_json", "fact_extractor_version"]
        )?,
        attachments_now = current(
            attachment_contract,
            attachments,
            &["official_complex_code", "notice_sequence", "sha256", "file_sequence", "file_title", "media_format"]
        )?,
        facts_now = current(
            fact_contract,
            facts,
            &fact_contract.columns.iter().map(|column| column.name).collect::<Vec<_>>()
        )?,
        version = literal(&version),
    ))?;
    let outdated = count(
        stage,
        &format!(
            "SELECT count(*) FROM (SELECT 1 FROM id_held WHERE accuracy_gate_json <> {} \
             OR fact_extractor_version <> {} LIMIT 1)",
            literal(gate_json),
            literal(&version)
        ),
    )?;
    let whole = all || outdated > 0;
    let scope = if whole {
        "SELECT official_complex_code FROM id_attachments WHERE sha256 IN (SELECT sha256 FROM id_fact_rows) \
         UNION SELECT official_complex_code FROM id_held"
            .to_owned()
    } else {
        format!(
            "SELECT official_complex_code FROM id_attachments WHERE sha256 IN \
             (SELECT sha256 FROM {facts} WHERE run_id = {})",
            literal(run_id)
        )
    };
    stage.connection().execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE id_codes AS
         SELECT DISTINCT official_complex_code FROM ({scope})
         WHERE official_complex_code IS NOT NULL AND official_complex_code <> '';
         CREATE OR REPLACE TEMP TABLE id_notices AS {notices_now};
         CREATE OR REPLACE TEMP TABLE id_page_methods AS {pages_now};",
        notices_now = current(
            detail_contract,
            details,
            &[
                "notice_sequence",
                "notice_number",
                "notice_title",
                "notice_date"
            ]
        )?,
        pages_now = current(
            page_contract,
            pages,
            &[
                "sha256",
                "extractor_version AS text_extractor_version",
                "page_number",
                "method AS page_method"
            ]
        )?,
    ))?;
    std::fs::create_dir_all(out).with_context(|| format!("cannot create {}", out.display()))?;
    let mut codes = create(&out.join(FOLD_CODES_FILE))?;
    let mut complexes = 0_u64;
    {
        let mut statement = stage
            .connection()
            .prepare("SELECT official_complex_code FROM id_codes ORDER BY 1")?;
        let mut found = statement.query([])?;
        while let Some(row) = found.next()? {
            let code: String = row.get(0)?;
            writeln!(codes, "{code}")?;
            complexes += 1;
        }
    }
    codes.flush()?;
    let rows = write_lines(
        stage,
        "SELECT a.official_complex_code, a.notice_sequence, n.notice_number, n.notice_title, \
                CAST(n.notice_date AS VARCHAR) AS notice_date, f.sha256, a.file_sequence, a.file_title, \
                a.media_format, f.fact_kind, f.fact_index, f.value_json, f.page_number, f.quote, f.method, \
                p.page_method, f.\"check\" \
         FROM id_fact_rows f \
         JOIN id_attachments a ON a.sha256 = f.sha256 \
              AND a.official_complex_code IN (SELECT official_complex_code FROM id_codes) \
         LEFT JOIN id_notices n ON n.notice_sequence = a.notice_sequence \
         LEFT JOIN id_page_methods p ON p.sha256 = f.sha256 \
              AND p.text_extractor_version = f.text_extractor_version AND p.page_number = f.page_number \
         ORDER BY a.official_complex_code, f.fact_kind, a.notice_sequence, f.sha256, f.fact_index",
        &out.join(FOLD_ROWS_FILE),
    )?;
    Ok(FoldInputs {
        whole,
        complexes,
        rows,
    })
}

#[cfg(test)]
mod tests;
