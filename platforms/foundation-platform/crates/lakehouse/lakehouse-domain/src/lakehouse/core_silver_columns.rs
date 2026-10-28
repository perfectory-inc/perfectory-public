//! Columns of the industrial complex, parcel boundary and land Silver tables. Moved out of
//! `lakehouse.rs` unchanged.

use super::LakehouseColumn;

pub(super) const SILVER_INDUSTRIAL_COMPLEXES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "complex_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "official_complex_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_name_normalized",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    // One code per site-formation word the source states, named after it (root ADR-0121).
    // Optional because rows loaded before it existed carry none; every new load fills it.
    LakehouseColumn {
        name: "development_stage",
        logical_type: "string",
        required: false,
    },
    // The source's own words for the codes above, trimmed and otherwise verbatim (root ADR-0117
    // §5). The codes are for filtering; these are what a person is shown. Several words can share
    // one code (`준비중` and `보상중` are both `planned`), so the code alone cannot say which.
    LakehouseColumn {
        name: "complex_kind_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "status_raw",
        logical_type: "string",
        required: false,
    },
    // Region is optional for the whole administrative triple: the owner deferred per-region
    // industrial-complex work, and requiring a code no source states would only be satisfiable by
    // inventing one. Absent stays `null`, never `""` or a zero code (root ADR-0035).
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "primary_bjdong_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "address_text",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "management_agency_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "developer_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "designated_date",
        logical_type: "date",
        required: false,
    },
    // Designation, ground-breaking, completion — the three dates in the order they happen.
    LakehouseColumn {
        name: "construction_start_date",
        logical_type: "date",
        required: false,
    },
    LakehouseColumn {
        name: "completion_date",
        logical_type: "date",
        required: false,
    },
    LakehouseColumn {
        name: "official_area_sqm",
        logical_type: "decimal(18,2)",
        required: false,
    },
    // What the profile source says about the complex beyond its identity and its dates. Every one
    // of these is optional for the same reason the columns above are: the provider leaves a cell
    // blank rather than stating a value, and one column of its twenty (`rent_hsmp_se_code`) is
    // blank in all 1,442 rows — a provider that empties a whole column empties single cells too.
    // Requiring any of them would mean one blank cell rejects the entire snapshot.
    //
    // `frst_regist_de` is deliberately absent. See root ADR-0044 and the header block in
    // `services/foundation-outbox-publisher/src/industrial_complex_bronze_raw_jsonl_export/\
    // profile_workbook_decoder.rs`.
    LakehouseColumn {
        name: "development_progress_percent",
        logical_type: "decimal(5,2)",
        required: false,
    },
    LakehouseColumn {
        name: "lot_sales_status",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "lot_sales_status_raw",
        logical_type: "string",
        required: false,
    },
    // The business period as the source wrote it, plus the two months a parse could recover. The
    // raw text is the contract: 1,440 of 1,441 values are `YYYY-MM~YYYY-MM` and one is `2020-~2024-`
    // with no months at all, so a shape that only held the parse would drop that row's fact
    // entirely. The two derived columns are null together whenever the parse does not apply.
    LakehouseColumn {
        name: "business_period_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "business_period_start_month",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "business_period_end_month",
        logical_type: "string",
        required: false,
    },
    // Four free-text columns, carried verbatim. `devlop_mth` has 232 distinct values and
    // `appn_basis_law` 48, and the distinctness is spelling rather than meaning — `공영개발`,
    // `공영개발방식`, and `공영개발 방식` are three of them. Mapping those onto an enumeration would
    // invent a classification nobody published, so the `_raw` suffix is the contract: this column
    // holds what the source wrote and normalization is a separate, evidenced decision.
    LakehouseColumn {
        name: "designation_basis_law_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "development_method_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "development_purpose_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "invited_industries_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "row_checksum_sha256",
        logical_type: "string",
        required: true,
    },
];

pub(super) const SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "boundary_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "boundary_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "centroid_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "centroid_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "area_sqm_calculated",
        logical_type: "decimal(18,2)",
        required: false,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

pub(super) const SILVER_PARCEL_BOUNDARIES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "boundary_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "bjdong_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "jibun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bonbun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bubun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

// D155 필지별 토지이용계획 속성 CSV 의 열 순서 그대로다 (root ADR-0083). *_name 열은
// 원천이 준 한국어 표기를 손대지 않고 나른다 — 어휘 번역은 소비자(공짱)의 결정이다.
pub(super) const SILVER_LAND_USE_PLAN_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "legal_dong_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "legal_dong_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ledger_kind_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ledger_kind_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "jibun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "drawing_number",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "inclusion_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "inclusion_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "zone_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "zone_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "registered_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "data_reference_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_sigungu_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "remark",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

pub use crate::building_panel::GOLD_BUILDING_PANEL;
pub use crate::court_auction::{SILVER_COURT_AUCTION_ITEM, SILVER_COURT_AUCTION_SALE_RESULT};
pub use crate::land_characteristic::SILVER_LAND_CHARACTERISTIC;
pub use crate::land_forest_ledger::SILVER_LAND_FOREST_LEDGER;
pub use crate::land_right_registration::SILVER_LAND_RIGHT_REGISTRATION;
pub use crate::land_transfer_history::SILVER_LAND_TRANSFER_HISTORY;
pub use crate::onbid_public_auction::{
    SILVER_ONBID_ITEM_BID_RESULT, SILVER_ONBID_REAL_ESTATE_ITEM,
    SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO,
};
pub use crate::parcel_number_change_history::SILVER_PARCEL_NUMBER_CHANGE_HISTORY;
pub use crate::parcel_panel::GOLD_PARCEL_PANEL;
pub use crate::registered_factory::SILVER_REGISTERED_FACTORY;

// D151 필지별 개별공시지가 CSV 의 열 순서 그대로 (root ADR-0085). 값은 원천 표기
// 그대로 문자열로 나른다 — 형 변환은 소비 투영의 몫이고, 원천이 준 것을 바꾸지 않는다.
pub(super) const SILVER_LAND_INDIVIDUAL_PRICE_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "legal_dong_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "legal_dong_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "special_land_kind_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "special_land_kind_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "jibun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "base_year",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "base_month",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "price_per_m2",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "announced_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "standard_parcel_flag",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "data_reference_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_sigungu_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

// LMIS 용도지역지구 코드표 (LART_LMISZONE.csv) 열 순서 그대로 (root ADR-0083).
// `parent_ucode` 는 판정 투영이 걸어 올라가는 트리의 간선이라 여기 있어야 하고,
// 원천에 빈 부모(UQA500 등)가 실재하므로 필수로 만들 수 없다.
pub(super) const SILVER_LAND_USE_ZONE_CODES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "ucode",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "uname",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "division_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "law_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_kind",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "law_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "annex_flag",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "enforcement_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "article_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "article_sub_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "record_seqno",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "parent_ucode",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "deleted_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "deleted_text",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "terms_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "first_registered_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "last_updated_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];
