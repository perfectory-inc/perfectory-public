//! 법원경매정보 (courtauction.go.kr) list rows, keyed to a PNU found from their address (root ADR-0280,
//! ADR-0281).
//!
//! One row per 물건 (court office, case, property and item sequence) as the site's two list searches
//! give it: the open-item search and the sale-result search. A load appends the rows of keys whose
//! content changed; the item table also marks a key that left a complete pass (root ADR-0275). Values
//! only: codes stay codes.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseEditionRead, LakehouseLayer,
    LakehouseLoadUnit, LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole,
    LakehouseTableContract, LakehouseWriteDistribution,
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

const CASE_KEY: &[&str] = &["court_office_code", "case_no"];
const PER_PROPERTY: &[&str] = &["court_office_code", "case_no", "property_seq"];
const NEWEST_READ: &[&str] = &["source_snapshot_id"];

/// One edition per property (or per case, `per`): the list the newest read gave is current, and a
/// list that changed is appended whole (root ADR-0540 §4).
const fn read_editions(per: &'static [&'static str]) -> LakehouseRowVersions {
    LakehouseRowVersions::Editions {
        edition_column: "source_snapshot_id",
        newest_first: NEWEST_READ,
        per,
        read: LakehouseEditionRead::Newest,
    }
}

const OBJECT_LOAD: LakehouseLoadUnit = LakehouseLoadUnit::Object {
    column: "source_record_id",
    object_prefix: None,
    object_suffix_separator: None,
};

/// A case as the 물건상세 states it (`csBaseInfo`, `dstrtDemnInfo`): its name, dates, claim and
/// department (root ADR-0540 §4). No person's name: the 사법보좌관 is left in Bronze.
pub const SILVER_COURT_AUCTION_CASE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_case",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: CASE_KEY,
        newest_first: LATEST,
        removal_column: None,
        keep_ties: false,
    }),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("court_name", "string", false),
        column("court_branch_name", "string", false),
        column("user_case_no", "string", false),
        column("case_name", "string", false),
        column("case_received_date", "date", false),
        column("commencement_date", "date", false),
        column("claim_amount_krw", "long", false),
        column("department_code", "string", false),
        column("department_name", "string", false),
        column("department_phone", "string", false),
        column("execution_officer_phone", "string", false),
        column("case_progress_status_code", "string", false),
        column("suspension_status_code", "string", false),
        column("closure_kind_code", "string", false),
        column("closed_date", "date", false),
        column("duplicate_merge_kind_code", "string", false),
        column("appeal_yn", "string", false),
        column("lawsuit_kind_code", "string", false),
        // 배당요구종기: the latest of the dates the case lists.
        column("dividend_demand_deadline", "date", false),
    ),
    partition_spec: &[],
    sort_order: CASE_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, source_snapshot_id, source_line_number) unique",
        "no person's name (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

/// A property's 매각물건명세서 summary and sale-date facts as the 물건상세 states them
/// (`dspslGdsDxdyInfo`). Free text has the names it holds masked (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_ITEM_SPEC: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item_spec",
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
        column("item_usage_code", "string", false),
        column("item_status_code", "string", false),
        column("schedule_item_status_code", "string", false),
        column("real_item_kind", "string", false),
        column("appraisal_amount_krw", "long", false),
        column("minimum_bid_price_krw", "long", false),
        column("failed_bid_count", "int", false),
        column("bid_kind_code", "string", false),
        column("sale_date", "date", false),
        column("sale_time_raw", "string", false),
        column("sale_place", "string", false),
        column("bid_start_date", "date", false),
        column("bid_end_date", "date", false),
        column("sale_decision_date", "date", false),
        column("sale_decision_time_raw", "string", false),
        column("sale_decision_place", "string", false),
        column("deposit_rate_percent", "int", false),
        // 매각물건명세서: 작성일, 인수되는 권리, 지상권 개요, 비고, 최선순위 설정.
        column("spec_written_date", "date", false),
        column("assumed_rights", "string", false),
        column("superficies", "string", false),
        column("spec_remarks", "string", false),
        column("first_rank_setting", "string", false),
        column("item_remarks", "string", false),
        column("spec_document_id", "string", false),
    ),
    partition_spec: &[],
    sort_order: DETAIL_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, property_seq, source_snapshot_id, source_line_number) unique",
        "free text masked by courtauction-go-kr.contract.json personal_names (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

/// A property's 기일내역 (`gdsDspslDxdyLst`), in the site's order: one edition per property read
/// whose list changed (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_ITEM_SCHEDULE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item_schedule",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(PER_PROPERTY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("property_seq", "int", true),
        // The row's place in the site's list, from 1.
        column("schedule_seq", "int", true),
        column("schedule_date", "date", false),
        column("schedule_time_raw", "string", false),
        column("schedule_kind_code", "string", false),
        column("schedule_place", "string", false),
        column("result_code", "string", false),
        column("schedule_item_status_code", "string", false),
        column("minimum_bid_price_krw", "long", false),
        // 매각가; null while unsold (the site writes 0).
        column("sale_amount_krw", "long", false),
        column("bid_start_date", "date", false),
        column("bid_end_date", "date", false),
    ),
    partition_spec: &[],
    sort_order: &[
        "court_office_code",
        "case_no",
        "property_seq",
        "schedule_seq",
    ],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, property_seq, schedule_seq, source_snapshot_id) unique",
        "a property's rows of one read are appended together, only when its list changed",
    ],
    load: OBJECT_LOAD,
};

/// A property's 목록내역 (`gdsDspslObjctLst`): each listed parcel or building.
///
/// Its kind, structure, area, 지목 and appraisal. The share is kept as numbers only; the share text
/// names its holders and stays in Bronze (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_ITEM_OBJECT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item_object",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(PER_PROPERTY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("property_seq", "int", true),
        column("object_seq", "int", true),
        column("list_kind_code", "string", false),
        column("realty_kind", "string", false),
        column("building_structure", "string", false),
        column("area_raw", "string", false),
        column("land_category", "string", false),
        column("appraisal_amount_krw", "long", false),
        column("usage_lcls_code", "string", false),
        column("usage_mcls_code", "string", false),
        column("usage_scls_code", "string", false),
        // The listed parcel: its legal-dong code (four fields) and lot as the site writes them, and
        // the site's own PNU text (`pnuNoCtt`, sometimes short or an old code).
        column("legal_dong_code", "string", false),
        column("lot_raw", "string", false),
        column("source_pnu_raw", "string", false),
        column("building_name", "string", false),
        column("building_unit_raw", "string", false),
        column("print_address_raw", "string", false),
        column("x_raw", "double", false),
        column("y_raw", "double", false),
        column("share_numerator", "double", false),
        column("share_denominator", "double", false),
        column("register_read_yn", "string", false),
    ),
    partition_spec: &[],
    sort_order: &["court_office_code", "case_no", "property_seq", "object_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, property_seq, object_seq, source_snapshot_id) unique",
        "a property's rows of one read are appended together, only when its list changed",
        "no share text (it names its holders, root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

/// A property's 제시외건물 (`gdsNotSugtBldLsstAll`), per listed object, in the site's order.
pub const SILVER_COURT_AUCTION_ITEM_ANNEX: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_item_annex",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(PER_PROPERTY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("property_seq", "int", true),
        column("object_seq", "int", true),
        column("annex_seq", "int", true),
        column("usage_raw", "string", false),
        column("structure_raw", "string", false),
        column("area_raw", "string", false),
        column("appraisal_amount_krw", "long", false),
        column("remarks", "string", false),
    ),
    partition_spec: &[],
    sort_order: &[
        "court_office_code",
        "case_no",
        "property_seq",
        "object_seq",
        "annex_seq",
    ],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, property_seq, object_seq, annex_seq, source_snapshot_id) unique",
        "a property's rows of one read are appended together, only when its list changed",
    ],
    load: OBJECT_LOAD,
};

/// A case's 감정요항표 (`aeeWevlMnpntLst`), in the site's order: one edition per case read whose
/// list changed (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_APPRAISAL_POINT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_appraisal_point",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(CASE_KEY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        // The row's place in the site's list, from 1.
        column("point_seq", "int", true),
        column("table_code", "string", false),
        column("item_code", "string", false),
        column("detail_seq", "int", false),
        column("content", "string", false),
    ),
    partition_spec: &[],
    sort_order: &["court_office_code", "case_no", "point_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, point_seq, source_snapshot_id) unique",
        "a case's rows of one read are appended together, only when its list changed",
        "free text masked by courtauction-go-kr.contract.json personal_names (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

const CODE_KEY: &[&str] = &["system_code", "owner_code", "group_code", "code"];

/// The site's 공통코드: the name its screens give each code the documents carry (root ADR-0540 §5).
///
/// As the run's one request answered it. A name that changed is appended; the newest per code is
/// current, and a code never vanishes. No name is kept in the repository (root ADR-0403).
pub const SILVER_COURT_AUCTION_CODE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_code",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: CODE_KEY,
        newest_first: LATEST,
        removal_column: None,
        keep_ties: false,
    }),
    columns: with_load_columns!(
        // systmCd, intgCdOnrIdntId, intgGrpCd, intgCd.
        column("system_code", "string", true),
        column("owner_code", "string", true),
        column("group_code", "string", true),
        column("code", "string", true),
        column("name", "string", false),
        column("owner_kind_code", "string", false),
        column("listed_codes_raw", "string", false),
        column("first_extra_raw", "string", false),
        column("second_extra_raw", "string", false),
    ),
    partition_spec: &[],
    sort_order: CODE_KEY,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(system_code, owner_code, group_code, code, source_snapshot_id) unique",
    ],
    load: OBJECT_LOAD,
};

const CASE_DOCUMENT_READ_KEY: &[&str] =
    &["court_office_code", "case_no", "document", "asked_round"];
const PER_CASE_ROUND: &[&str] = &["court_office_code", "case_no", "round_seq"];

/// One row per 사건 문서 request (사건상세, 현황조사, 감정평가), failed reads included: the record the
/// next run's case frontier is computed from (root ADR-0352, ADR-0540 §3).
pub const SILVER_COURT_AUCTION_CASE_DOCUMENT_READ: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.court_auction_case_document_read",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: CASE_DOCUMENT_READ_KEY,
            newest_first: LATEST,
            removal_column: None,
            keep_ties: false,
        }),
        columns: with_load_columns!(
            column("court_office_code", "string", true),
            column("case_no", "string", true),
            // The contract's role: case_detail, case_survey or case_appraisal.
            column("document", "string", true),
            // The round asked; 0 asks the newest.
            column("asked_round", "int", true),
            column("answered_round", "int", false),
            column("requested_at_utc", "timestamp", true),
            column("http_status", "int", false),
            column("read_outcome", "string", true),
            column("list_fingerprint", "string", false),
        ),
        partition_spec: &[],
        sort_order: CASE_DOCUMENT_READ_KEY,
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "(court_office_code, case_no, document, asked_round, source_snapshot_id) unique",
        ],
        load: OBJECT_LOAD,
    };

/// A case's related and merged cases as the 사건상세 lists them (`dlt_rletReltCsLst`,
/// `dlt_dpcnMrgTrnscsCsRlet`): one edition per case read whose list changed (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_CASE_RELATED: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_case_related",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(CASE_KEY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        // The row's place in the case's lists, from 1 (related cases, then merged ones).
        column("related_seq", "int", true),
        // related (관련사건) or merged (중복·병합).
        column("list", "string", true),
        column("related_court_office_code", "string", false),
        column("related_case_no", "string", false),
        column("related_user_case_no", "string", false),
        column("relation_kind_code", "string", false),
        column("relation_kind_name", "string", false),
        column("related_court_name", "string", false),
    ),
    partition_spec: &[],
    sort_order: &["court_office_code", "case_no", "related_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, related_seq, source_snapshot_id) unique",
        "a case's rows of one read are appended together, only when its list changed",
    ],
    load: OBJECT_LOAD,
};

/// A case's 목록 and each one's 종국결과 as the 사건상세 lists them (`dlt_rletCsDspslObjctLst`): one
/// edition per case read whose list changed (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_CASE_OBJECT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_case_object",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(CASE_KEY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("object_seq", "int", true),
        column("list_kind_code", "string", false),
        column("list_kind_name", "string", false),
        column("closure_kind_code", "string", false),
        column("closure_kind_name", "string", false),
        column("legal_dong_code", "string", false),
        column("lot_raw", "string", false),
        column("building_name", "string", false),
        column("building_unit_raw", "string", false),
        column("print_address_raw", "string", false),
        column("register_record_yn", "string", false),
    ),
    partition_spec: &[],
    sort_order: &["court_office_code", "case_no", "object_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, object_seq, source_snapshot_id) unique",
        "a case's rows of one read are appended together, only when its list changed",
    ],
    load: OBJECT_LOAD,
};

/// A case's 당사자 by kind (`dlt_rletCsIntrpsLst`): the kind and its place in the list, never the
/// person's name, which stays in Bronze (root ADR-0540 §4). One edition per case read whose list
/// changed.
pub const SILVER_COURT_AUCTION_CASE_PARTY: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_case_party",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(CASE_KEY),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        // The row's place in the site's list, from 1.
        column("party_seq", "int", true),
        column("party_kind_code", "string", false),
        column("party_kind_name", "string", false),
    ),
    partition_spec: &[],
    sort_order: &["court_office_code", "case_no", "party_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, party_seq, source_snapshot_id) unique",
        "no person's name (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

/// A case's 현황조사 of one round (`dma_ordTsCurstExmnMngInf`): its dates, status and the
/// possession and lease narrative, names masked (root ADR-0540 §4). Newest per (case, round).
pub const SILVER_COURT_AUCTION_SURVEY: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_survey",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: PER_CASE_ROUND,
        newest_first: LATEST,
        removal_column: None,
        keep_ties: false,
    }),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("round_seq", "int", true),
        column("transmit_status_code", "string", false),
        column("case_name_code", "string", false),
        // 조사일시: the site's caret-joined text, as received.
        column("surveyed_raw", "string", false),
        column("sent_date", "date", false),
        column("received_date", "date", false),
        column("cancelled_date", "date", false),
        column("possession_summary", "string", false),
        column("first_possession", "string", false),
        column("second_possession", "string", false),
        column("lease_text", "string", false),
        column("lawsuit_kind_code", "string", false),
    ),
    partition_spec: &[],
    sort_order: PER_CASE_ROUND,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, round_seq, source_snapshot_id, source_line_number) unique",
        "free text masked by courtauction-go-kr.contract.json personal_names (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

/// A 현황조사 round's 목록 (`dlt_ordTsCurstExmnRletLst`): each one's possession code, its 임차 count
/// and the possession text, names masked. One edition per (case, round) read whose list changed
/// (root ADR-0540 §4).
pub const SILVER_COURT_AUCTION_SURVEY_OBJECT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_survey_object",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: read_editions(PER_CASE_ROUND),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("round_seq", "int", true),
        column("object_seq", "int", true),
        column("sale_object_seq", "int", false),
        column("list_kind_code", "string", false),
        column("possession_code", "string", false),
        column("possession_text", "string", false),
        column("remarks", "string", false),
        column("lease_count", "int", false),
        column("land_category", "string", false),
        column("area_raw", "string", false),
        column("lot_raw", "string", false),
        column("building_name", "string", false),
        column("building_unit_raw", "string", false),
        column("print_address_raw", "string", false),
    ),
    partition_spec: &[],
    sort_order: &["court_office_code", "case_no", "round_seq", "object_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, round_seq, object_seq, source_snapshot_id) unique",
        "a round's rows of one read are appended together, only when its list changed",
        "free text masked by courtauction-go-kr.contract.json personal_names (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};

/// A case's 감정평가 of one round (`dma_ordTsIndvdAeeWevlInf`): its number and dates (the key of its
/// 감정평가서) and the opinion, names masked; never the appraiser's name (root ADR-0540 §4). Newest
/// per (case, round).
pub const SILVER_COURT_AUCTION_APPRAISAL: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.court_auction_appraisal",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: PER_CASE_ROUND,
        newest_first: LATEST,
        removal_column: None,
        keep_ties: false,
    }),
    columns: with_load_columns!(
        column("court_office_code", "string", true),
        column("case_no", "string", true),
        column("round_seq", "int", true),
        column("appraisal_no", "string", false),
        column("price_basis_date", "date", false),
        column("surveyed_date", "date", false),
        column("written_date", "date", false),
        column("list_kind_code", "string", false),
        column("opinion", "string", false),
        column("court_name", "string", false),
        column("user_case_no", "string", false),
        column("lawsuit_kind_code", "string", false),
    ),
    partition_spec: &[],
    sort_order: PER_CASE_ROUND,
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(court_office_code, case_no, round_seq, source_snapshot_id, source_line_number) unique",
        "no person's name (root ADR-0540 §4)",
    ],
    load: OBJECT_LOAD,
};
