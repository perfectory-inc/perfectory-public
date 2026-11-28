//! 산업용지 분양·처분·공급 공고의 읽은 기록과 첨부 색인 (root ADR-0603).
//!
//! Three sources (the ILIS sale-notice registry, the KICOX sale board, LH land notices) are collected
//! the same way: the collector reads each owed notice's detail, stores every attachment once in the
//! shared content-addressed media store, and lands the detail in Bronze with each attachment
//! annotated by its hash. The Silver load appends one read row per notice read — the record the
//! collector's next frontier is computed from — and one attachment row per stored file: the index
//! the media reconciler reads the store through (root ADR-0387). A notice is named by its source
//! and the source's own id; nothing here interprets the notice (areas, prices, periods are later).

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

macro_rules! with_load_columns {
    ($($column:expr),* $(,)?) => {
        &[
            $($column,)*
            column("source_record_id", "string", true),
            column("source_line_number", "int", true),
            column("source_snapshot_id", "string", true),
            column("ingested_at_utc", "timestamp", true),
        ]
    };
}

const READ_KEY: &[&str] = &["notice_source", "notice_id"];
const ATTACHMENT_KEY: &[&str] = &["notice_source", "notice_id", "sha256"];
const LATEST: &[&str] = &["source_snapshot_id", "source_line_number"];

/// One row per notice detail read, failed reads included: the record the collector's next run
/// computes what is still owed from (root ADR-0603 §3).
///
/// `list_fingerprint` is the hash of the list fields the source contract names: a listed notice
/// whose fingerprint differs from its last read is read again.
pub const SILVER_INDUSTRIAL_LAND_SALE_NOTICE_FETCH_ATTEMPT: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.industrial_land_sale_notice_fetch_attempt",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: READ_KEY,
            newest_first: LATEST,
            removal_column: None,
            keep_ties: false,
        }),
        columns: with_load_columns!(
            column("notice_source", "string", true),
            column("notice_id", "string", true),
            column("list_fingerprint", "string", true),
            column("requested_at_utc", "timestamp", true),
            column("http_status", "int", false),
            column("read_outcome", "string", true),
            column("file_count", "int", false),
            column("files_stored", "int", false),
        ),
        partition_spec: &[],
        sort_order: READ_KEY,
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "one row per notice detail read, failed reads included (root ADR-0603 §3)",
            "(notice_source, notice_id, source_snapshot_id, source_line_number) unique",
        ],
        load: LakehouseLoadUnit::Object {
            column: "source_record_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };

/// A notice's attachments, one row per content hash; the bytes live once in the shared
/// content-addressed media store (root ADR-0298, ADR-0603 §2).
pub const SILVER_INDUSTRIAL_LAND_SALE_NOTICE_ATTACHMENT: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.industrial_land_sale_notice_attachment",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: ATTACHMENT_KEY,
            newest_first: LATEST,
            removal_column: None,
            keep_ties: false,
        }),
        columns: with_load_columns!(
            column("notice_source", "string", true),
            column("notice_id", "string", true),
            column("sha256", "string", true),
            column("file_sequence", "int", true),
            column("file_title", "string", false),
            column("provider_file_id", "string", false),
            column("provider_file_size", "long", false),
            column("source_url", "string", true),
            column("media_format", "string", true),
            column("size_bytes", "long", true),
            column("fetched_at_utc", "timestamp", true),
        ),
        partition_spec: &[],
        sort_order: &["notice_source", "notice_id", "file_sequence"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "(notice_source, notice_id, sha256, source_snapshot_id) unique",
        ],
        load: LakehouseLoadUnit::Object {
            column: "source_record_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };
