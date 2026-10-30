//! 국토교통부 실거래가 (rt.molit.go.kr bulk exports), one table for the twelve datasets (root ADR-0348, ADR-0349).
//!
//! A load appends, per (dataset, 계약 달) bulk export, the rows whose name is new or whose content
//! changed, and a removal marker for a name the month's export no longer holds. A cancellation is a
//! changed row, not a removal. Values only: the provider's words stay its words.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const SILVER_REAL_TRANSACTION_COLUMNS: &[LakehouseColumn] = &[
    // Key: the bucket (dataset, 계약 달 YYYYMM, nationwide: one bulk export) and the row's name (root ADR-0349 §2).
    column("dataset_slug", "string", true),
    column("deal_ym", "string", true),
    column("row_key", "string", true),
    // 계약일 (계약년월·계약일).
    column("deal_date", "date", false),
    // Amounts in 만원 as the provider writes them: 거래금액, 보증금·월세금 and the previous contract's.
    column("deal_amount_10k_krw", "long", false),
    column("deposit_10k_krw", "long", false),
    column("monthly_rent_10k_krw", "long", false),
    column("pre_deposit_10k_krw", "long", false),
    column("pre_monthly_rent_10k_krw", "long", false),
    column("rent_kind", "string", false),
    column("contract_term", "string", false),
    column("contract_type", "string", false),
    column("use_rr_right", "string", false),
    // Where: the provider's 시군구 (the full 법정동 address), the lot as written (a masked lot such as `3**` kept as is), 본번·부번, the road address.
    column("address_raw", "string", false),
    column("jibun_raw", "string", false),
    column("jibun_masked", "boolean", false),
    column("bonbun", "string", false),
    column("bubun", "string", false),
    column("road_address", "string", false),
    // What: the building (단지명·건물명, 동), its type and use, the areas in m² with which provider area each is.
    column("building_name", "string", false),
    column("apt_dong", "string", false),
    column("building_type", "string", false),
    column("house_type", "string", false),
    column("building_use", "string", false),
    column("building_area_m2", "double", false),
    column("building_area_kind", "string", false),
    column("land_area_m2", "double", false),
    column("land_area_kind", "string", false),
    column("floor", "string", false),
    column("build_year", "int", false),
    column("land_use", "string", false),
    column("jimok", "string", false),
    column("road_condition", "string", false),
    // How: 거래유형, 매수·매도, 중개사소재지, 지분구분, 분/입주권.
    column("dealing_gbn", "string", false),
    column("buyer_gbn", "string", false),
    column("seller_gbn", "string", false),
    column("estate_agent_sgg_nm", "string", false),
    column("share_dealing_type", "string", false),
    column("ownership_gbn", "string", false),
    // Filled in after the contract (root ADR-0349 §4): 등기일자 and 해제사유발생일; a cancelled row is a new version, never a removal.
    column("registration_date", "date", false),
    column("cancel_date", "date", false),
    column("cancelled", "boolean", false),
    // Every provider column but the row number, as received, sorted JSON.
    column("provider_fields_json", "string", false),
    // Place (root ADR-0349 §5, the one resolver of root ADR-0288): the current 법정동 code, the current PNU when the lot is unmasked and a current parcel, the resolver's grade, or why no PNU.
    column("legal_dong_code", "string", false),
    column("pnu", "string", false),
    column("place_identity_grade", "string", false),
    column("pnu_unresolved_reason", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
    // `removed` on a removal marker (root ADR-0275): the name left its month's export.
    column("row_state", "string", false),
];

/// Every 실거래 row of the twelve datasets, by bucket and content name (root ADR-0349).
pub const SILVER_REAL_TRANSACTION: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.real_transaction",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["dataset_slug", "deal_ym", "row_key"],
        newest_first: &["source_snapshot_id", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: false,
    }),
    columns: SILVER_REAL_TRANSACTION_COLUMNS,
    // About 60 million rows over 12 datasets and 21 years (root ADR-0348): a partition is one
    // dataset-year, about 240 thousand rows; a load reads its month's partition only.
    partition_spec: &["dataset_slug", "truncate(4, deal_ym)"],
    sort_order: &["deal_ym", "row_key"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "pnu is null or a current parcel at load",
        "(dataset_slug, deal_ym, row_key, source_snapshot_id) unique",
    ],
    // One Bronze object (one dataset's nationwide month) is one load.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
