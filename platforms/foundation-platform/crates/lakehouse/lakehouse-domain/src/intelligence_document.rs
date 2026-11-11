//! The tables the intelligence platform writes back from the documents foundation hands it (root
//! ADR-0477): a text outcome per document and extractor version, the text of each page with its
//! place in the document, and the cited facts with their own outcome.
//!
//! Their columns are the intelligence platform's contract
//! (`platforms/intelligence-platform/config/document-extraction.contract.json` `outputs`) plus the
//! run that appended them; a test in `infra/lakehouse/spark/tests` holds the two equal. Only the
//! intelligence jobs write these tables; foundation reads them, and its read for the complex panel
//! is `gold.complex_document_facts`.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const DOCUMENT_KEY: &[&str] = &["sha256", "extractor_version"];
const PAGE_KEY: &[&str] = &["sha256", "extractor_version", "page_number"];
const FACT_KEY: &[&str] = &["sha256", "extractor_version", "fact_kind", "fact_index"];
const COMPLEX_KEY: &[&str] = &["official_complex_code"];
// A key is read once per extractor version, so a second row exists only if a run's append was
// repeated; the later run (its id is its UTC start, `YYYYMMDDTHHMMSSZ`) is the one kept.
const NEWEST: &[&str] = &["run_id"];

const fn latest(key: &'static [&'static str]) -> LakehouseRowVersions {
    LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key,
        newest_first: NEWEST,
        removal_column: None,
        keep_ties: false,
    })
}

/// Its rows are the "done" record of the text job: a hash with an outcome for this extractor
/// version is never read again by that version, failures included (root ADR-0477 §3).
pub const INTELLIGENCE_DOCUMENT_TEXT_OUTCOME: LakehouseTableContract = LakehouseTableContract {
    table_name: "intelligence.document_text_outcome",
    layer: LakehouseLayer::Intelligence,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: latest(DOCUMENT_KEY),
    columns: &[
        column("sha256", "string", true),
        column("extractor_version", "string", true),
        column("media_format", "string", true),
        column("outcome", "string", true),
        column("page_count", "int", true),
        column("ocr_page_count", "int", true),
        column("char_count", "long", true),
        column("error", "string", false),
        column("elapsed_ms", "long", true),
        column("extracted_at_utc", "timestamp", true),
        column("run_id", "string", true),
    ],
    partition_spec: &[],
    sort_order: DOCUMENT_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(sha256, extractor_version) unique",
        "outcome is text, partial_ocr, ocr, empty, unsupported_format or failed",
        "appended after the run's pages, so an outcome never names pages that are missing",
    ],
    load: LakehouseLoadUnit::Run { column: "run_id" },
};

/// One row per page; `char_start`..`char_end` place the page in the document's text (pages joined
/// by form feed), and a citation holds only when that span equals its quote (root ADR-0477 §4).
pub const INTELLIGENCE_DOCUMENT_TEXT_PAGE: LakehouseTableContract = LakehouseTableContract {
    table_name: "intelligence.document_text_page",
    layer: LakehouseLayer::Intelligence,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: latest(PAGE_KEY),
    columns: &[
        column("sha256", "string", true),
        column("extractor_version", "string", true),
        column("page_number", "int", true),
        column("char_start", "long", true),
        column("char_end", "long", true),
        column("method", "string", true),
        column("text", "string", true),
        column("run_id", "string", true),
    ],
    partition_spec: &[],
    sort_order: PAGE_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(sha256, extractor_version, page_number) unique",
        "method is text_layer or ocr",
    ],
    load: LakehouseLoadUnit::Run { column: "run_id" },
};

/// One value and its one citation; written only when its check passed (rule totals, citation,
/// and for OCR pages the table total or the vision re-read, root ADR-0477 §5).
pub const INTELLIGENCE_DOCUMENT_FACT: LakehouseTableContract = LakehouseTableContract {
    table_name: "intelligence.document_fact",
    layer: LakehouseLayer::Intelligence,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: latest(FACT_KEY),
    columns: &[
        column("sha256", "string", true),
        column("extractor_version", "string", true),
        column("text_extractor_version", "string", true),
        column("fact_kind", "string", true),
        column("fact_index", "int", true),
        column("value_json", "string", true),
        column("page_number", "int", true),
        column("char_start", "long", true),
        column("char_end", "long", true),
        column("quote", "string", true),
        column("method", "string", true),
        column("model", "string", false),
        column("check", "string", true),
        column("extracted_at_utc", "timestamp", true),
        column("run_id", "string", true),
    ],
    partition_spec: &[],
    sort_order: FACT_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(sha256, extractor_version, fact_kind, fact_index) unique",
        "method is rule or local_llm; model is set exactly when method is local_llm",
        "every row's (page_number, char_start, char_end) span equals its quote",
    ],
    load: LakehouseLoadUnit::Run { column: "run_id" },
};

/// The fact job's "done" record: a document read by this fact version, with what was found,
/// what the rules could not close and what failed its check.
pub const INTELLIGENCE_DOCUMENT_FACT_OUTCOME: LakehouseTableContract = LakehouseTableContract {
    table_name: "intelligence.document_fact_outcome",
    layer: LakehouseLayer::Intelligence,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: latest(DOCUMENT_KEY),
    columns: &[
        column("sha256", "string", true),
        column("extractor_version", "string", true),
        column("text_extractor_version", "string", true),
        column("fact_counts_json", "string", true),
        column("unsolved_pages_json", "string", true),
        column("dropped_json", "string", true),
        column("model_calls", "int", true),
        column("elapsed_ms", "long", true),
        column("extracted_at_utc", "timestamp", true),
        column("run_id", "string", true),
    ],
    partition_spec: &[],
    sort_order: DOCUMENT_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(sha256, extractor_version) unique",
        "appended after the run's facts, so an outcome never names facts that are missing",
    ],
    load: LakehouseLoadUnit::Run { column: "run_id" },
};

/// Foundation's own read of the facts (root ADR-0477 §1): per industrial complex, the facts its
/// panel may state, from the kinds the hand-checked gate admits (§5), each from the newest notice
/// that states it with the page it was read on. Written only by the fact job's fold step, which
/// appends a row for each complex whose attachments got facts in the run; the newest row per
/// complex is the one served.
pub const GOLD_COMPLEX_DOCUMENT_FACTS: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.complex_document_facts",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    row_versions: latest(COMPLEX_KEY),
    columns: &[
        column("official_complex_code", "string", true),
        column("document_facts_json", "string", true),
        column("fact_count", "int", true),
        column("fact_extractor_version", "string", true),
        column("accuracy_gate_json", "string", true),
        column("folded_at_utc", "timestamp", true),
        column("run_id", "string", true),
    ],
    partition_spec: &[],
    sort_order: COMPLEX_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(official_complex_code, run_id) unique",
        "every fact's (fact_kind, method, page_method) is in the row's accuracy_gate_json",
        "fact_count is the number of facts in document_facts_json",
    ],
    load: LakehouseLoadUnit::Run { column: "run_id" },
};
