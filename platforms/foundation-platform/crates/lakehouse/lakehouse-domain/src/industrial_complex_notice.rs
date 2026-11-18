//! 산업입지정보시스템(ILIS) 산업단지 고시의 읽은 기록과 첨부 색인 (root ADR-0484).
//!
//! The collector reads each notice's detail once, stores every attachment once in the shared
//! content-addressed media store, and lands the detail (each attachment annotated with its hash) in
//! Bronze. The Silver load appends one read row per notice read — the record the collector's next
//! frontier is computed from — and one attachment row per stored file: the index the media
//! reconciler and, later, the intelligence platform's extraction read the store through.

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

const DETAIL_KEY: &[&str] = &["notice_sequence"];
const ATTACHMENT_KEY: &[&str] = &["notice_sequence", "sha256"];
const LATEST: &[&str] = &["source_snapshot_id", "source_line_number"];

/// One row per notice detail read, failed reads included: the record the collector's next run
/// computes what is still owed from (root ADR-0484 §3).
pub const SILVER_INDUSTRIAL_COMPLEX_NOTICE_DETAIL: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.industrial_complex_notice_detail",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: DETAIL_KEY,
            newest_first: LATEST,
            removal_column: None,
            keep_ties: false,
        }),
        columns: with_load_columns!(
            column("notice_sequence", "long", true),
            column("requested_at_utc", "timestamp", true),
            column("http_status", "int", false),
            column("read_outcome", "string", true),
            column("official_complex_code", "string", false),
            column("notice_title", "string", false),
            column("notice_number", "string", false),
            column("notice_date", "date", false),
            column("notice_type_code", "string", false),
            column("notice_type_raw", "string", false),
            column("complex_kind_code", "string", false),
            column("complex_kind_raw", "string", false),
            column("file_count", "int", false),
            column("files_stored", "int", false),
        ),
        partition_spec: &[],
        sort_order: DETAIL_KEY,
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "one row per notice detail read, failed reads included (root ADR-0484 §3)",
            "(notice_sequence, source_snapshot_id, source_line_number) unique",
        ],
        load: LakehouseLoadUnit::Object {
            column: "source_record_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };

/// A notice's attachments, one row per content hash; the bytes live once in the shared
/// content-addressed media store (root ADR-0298, ADR-0484 §2).
pub const SILVER_INDUSTRIAL_COMPLEX_NOTICE_ATTACHMENT: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.industrial_complex_notice_attachment",
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
            column("notice_sequence", "long", true),
            column("sha256", "string", true),
            column("file_sequence", "int", true),
            column("official_complex_code", "string", false),
            column("file_title", "string", false),
            column("file_extension_raw", "string", false),
            column("provider_file_size", "long", false),
            column("provider_file_path", "string", false),
            column("provider_file_id", "string", false),
            column("source_url", "string", true),
            column("media_format", "string", true),
            column("size_bytes", "long", true),
            column("fetched_at_utc", "timestamp", true),
        ),
        partition_spec: &[],
        sort_order: &["notice_sequence", "file_sequence"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "(notice_sequence, sha256, source_snapshot_id) unique",
        ],
        load: LakehouseLoadUnit::Object {
            column: "source_record_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };
