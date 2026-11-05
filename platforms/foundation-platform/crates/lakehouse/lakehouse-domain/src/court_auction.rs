//! 법원경매정보 (courtauction.go.kr) list rows, keyed to a PNU found from their address (root ADR-0280,
//! ADR-0281).
//!
//! One row per 물건 (court office, case, property and item sequence) as the site's two list searches
//! give it: the open-item search and the sale-result search. A load appends the rows of keys whose
//! content changed; the item table also marks a key that left a complete pass (root ADR-0275). Values
//! only: codes stay codes.

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

/// The columns both searches give, in one order.
macro_rules! court_auction_columns {
    ($($extra:expr),* $(,)?) => {
        &[
            // Key: boCd, saNo, maemulSer, mokmulSer.
            column("court_office_code", "string", true),
            column("case_no", "string", true),
            column("property_seq", "int", true),
            column("item_seq", "int", true),
            column("user_case_no", "string", false),
            column("court_name", "string", false),
            column("department_name", "string", false),
            column("appraisal_amount_krw", "long", false),
            column("minimum_bid_price_krw", "long", false),
            column("sale_date", "date", false),
            column("sale_time_raw", "string", false),
            column("sale_decision_date", "date", false),
            column("sale_place", "string", false),
            column("failed_bid_count", "int", false),
            // 매각가; null while unsold (the site writes 0).
            column("sale_amount_krw", "long", false),
            column("case_status_code", "string", false),
            column("item_status_code", "string", false),
            column("item_in_progress_yn", "string", false),
            column("usage_lcls_code", "string", false),
            column("usage_mcls_code", "string", false),
            column("usage_scls_code", "string", false),
            column("address_sido_name", "string", false),
            column("address_sigungu_name", "string", false),
            column("address_dong_name", "string", false),
            column("address_ri_name", "string", false),
            column("representative_lot_raw", "string", false),
            column("road_name", "string", false),
            column("building_no_raw", "string", false),
            column("building_name", "string", false),
            column("print_address_raw", "string", false),
            column("pnu", "string", false),
            column("pnu_basis", "string", false),
            column("pnu_unresolved_reason", "string", false),
            column("pnu_candidate_count", "int", false),
            column("legal_dong_code", "string", false),
            column("wgs84_x", "double", false),
            column("wgs84_y", "double", false),
            column("source_record_id", "string", true),
            column("source_line_number", "int", true),
            column("source_snapshot_id", "string", true),
            column("ingested_at_utc", "timestamp", true),
            $($extra,)*
        ]
    };
}

const KEY: &[&str] = &["court_office_code", "case_no", "property_seq", "item_seq"];

const SILVER_COURT_AUCTION_ITEM_COLUMNS: &[LakehouseColumn] = court_auction_columns!(
    // `removed` on a removal marker (root ADR-0275); null on every row the source holds.
    column("row_state", "string", false),
    // The PNU the row's own legal-dong code and lot name, before the resolver (root ADR-0382).
    column("source_code_pnu_raw", "string", false),
);

const SILVER_COURT_AUCTION_SALE_RESULT_COLUMNS: &[LakehouseColumn] = court_auction_columns!();

/// The open items of the site's item search, as their content changes.
pub const SILVER_COURT_AUCTION_ITEM: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: KEY,
        newest_first: &["source_snapshot_id"],
        removal_column: Some("row_state"),
        keep_ties: false,
    }),
    columns: SILVER_COURT_AUCTION_ITEM_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "court_office_code", "case_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "pnu is set exactly when pnu_basis is set and pnu_unresolved_reason is not",
        "(court_office_code, case_no, property_seq, item_seq, source_snapshot_id) unique",
        "removal markers only from a complete pass",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// The rows of the site's sale-result search, as their content changes.
pub const SILVER_COURT_AUCTION_SALE_RESULT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_sale_result",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: KEY,
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_COURT_AUCTION_SALE_RESULT_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "court_office_code", "case_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "pnu is set exactly when pnu_basis is set and pnu_unresolved_reason is not",
        "(court_office_code, case_no, property_seq, item_seq, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// The load columns every table of this source carries, in one order.
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

const DETAIL_KEY: &[&str] = &["court_office_code", "case_no", "property_seq"];
const PHOTO_KEY: &[&str] = &["court_office_code", "case_no", "sha256"];
const LATEST: &[&str] = &["source_snapshot_id", "source_line_number"];

/// One row per 물건상세 read, failed reads included: the record the next run's detail frontier is
/// computed from (root ADR-0352, ADR-0384).
pub const SILVER_COURT_AUCTION_ITEM_DETAIL: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item_detail",
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
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("property_seq", "int", true),
        column("requested_at_utc", "timestamp", true),
        column("http_status", "int", false),
        column("read_outcome", "string", true),
        column("list_fingerprint", "string", false),
        column("photo_count", "int", false),
    ),
    partition_spec: &[],
    sort_order: DETAIL_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "one row per 물건상세 read, failed reads included (root ADR-0352 §2)",
        "(court_office_code, case_no, property_seq, source_snapshot_id, source_line_number) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// A case's photos, one row per content hash, as the 물건상세 lists them; the bytes live once in the
/// shared content-addressed media store (root ADR-0290 §7, ADR-0298).
pub const SILVER_COURT_AUCTION_ITEM_PHOTO: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item_photo",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: PHOTO_KEY,
        newest_first: LATEST,
        removal_column: None,
        keep_ties: false,
    }),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("sha256", "string", true),
        column("photo_kind_code", "string", false),
        column("sequence", "int", false),
        column("page_sequence", "int", false),
        column("title", "string", false),
        column("media_format", "string", true),
        column("size_bytes", "long", true),
    ),
    partition_spec: &[],
    sort_order: &[
        "court_office_code",
        "case_no",
        "photo_kind_code",
        "sequence",
    ],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, sha256, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
