//! 주소DB (주소기반산업지원서비스), one row per 도로명주소 관리번호 with its representative 지번's PNU
//! (root ADR-0495).
//!
//! The base is the monthly full loaded once; each day's change file then appends only the keys it
//! changes and a removal marker for an address it abolishes (변경사유 63, root ADR-0318). The edition
//! of a row is the day it holds: the full's 기준일 (its month's last day) or the change file's day, so
//! the newest day per key is the address as it is now.

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

const SILVER_JUSO_ROAD_ADDRESS_COLUMNS: &[LakehouseColumn] = &[
    column("road_address_management_no", "string", true),
    // 도로명코드: the 시군구 code (5 digits) and the road's own number (7).
    column("road_code", "string", false),
    column("sigungu_code", "string", false),
    column("emd_seq", "string", false),
    column("road_name", "string", false),
    column("sido_name", "string", false),
    column("sigungu_name", "string", false),
    // 0 above ground, 1 underground, 2 in the air, as the provider writes it.
    column("underground_code", "string", false),
    column("building_main_no", "int", false),
    column("building_sub_no", "int", false),
    // The representative 지번 (대표여부 1).
    column("legal_dong_code", "string", false),
    column("mountain_yn", "string", false),
    column("lot_main_no", "int", false),
    column("lot_sub_no", "int", false),
    column("pnu", "string", false),
    column("source_record_id", "string", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
    column("row_state", "string", false),
];

/// Every 도로명주소 of the 주소DB with the PNU of its representative 지번, as each day's change moves it.
pub const SILVER_JUSO_ROAD_ADDRESS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.juso_road_address",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: &["road_address_management_no"],
        newest_first: &["source_snapshot_id", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: false,
    }),
    columns: SILVER_JUSO_ROAD_ADDRESS_COLUMNS,
    partition_spec: &[],
    sort_order: &["sigungu_code", "road_name", "building_main_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(road_address_management_no, source_snapshot_id) unique",
        "pnu is the representative jibun's PNU of legal_dong_code, mountain_yn, lot_main_no and lot_sub_no",
        "removal markers only for an address a change file abolishes",
    ],
    // One provider file (one Bronze zip) is one load.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
