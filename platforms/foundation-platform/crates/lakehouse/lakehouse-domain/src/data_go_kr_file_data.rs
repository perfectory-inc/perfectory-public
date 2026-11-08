//! The other public bulk files of the data.go.kr file data, one Silver table per file (root ADR-0401
//! §3–4). `infra/lakehouse/contracts/data-go-kr-file-data.contract.json` names each file's header,
//! row key and place link; the load is `infra/lakehouse/spark/jobs/data_go_kr_file_data.py`.
//!
//! Every table has one shape. A row keeps every raw column under its header name in
//! `provider_fields_json` (the header is the contract's, so Iceberg names stay identifiers) and its
//! key in `row_key`: the provider key's values when the file has one, else the SHA-256 of the row.
//! An edition appends only its new and changed keys and one removal marker per key it no longer
//! holds (root ADR-0318). A parcel file adds the PNU its place resolved to and how; a 법정동 or
//! 행정동 file adds the code it links by.

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

const ROW: [LakehouseColumn; 2] = [
    column("row_key", "string", true),
    // Every raw column of the row, keyed by its header name, in header order.
    column("provider_fields_json", "string", true),
];

const LOAD: [LakehouseColumn; 5] = [
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
    // `removed` on a removal marker (root ADR-0275, ADR-0318); null on every row the source holds.
    column("row_state", "string", false),
];

const PARCEL_COLUMNS: &[LakehouseColumn] = &[
    ROW[0],
    ROW[1],
    column("pnu", "string", false),
    // The step that found the PNU: `jibun_address`/`road_address` (address_pnu) or
    // `code_exact`/`code_superseded` (the place-identity resolver).
    column("pnu_basis", "string", false),
    column("pnu_unresolved_reason", "string", false),
    column("legal_dong_code", "string", false),
    LOAD[0],
    LOAD[1],
    LOAD[2],
    LOAD[3],
    LOAD[4],
];

const AREA_COLUMNS: &[LakehouseColumn] = &[
    ROW[0],
    ROW[1],
    column("legal_dong_code", "string", false),
    column("administrative_dong_code", "string", false),
    column("legal_dong_unresolved_reason", "string", false),
    LOAD[0],
    LOAD[1],
    LOAD[2],
    LOAD[3],
    LOAD[4],
];

const fn table(
    table_name: &'static str,
    columns: &'static [LakehouseColumn],
    keep_ties: bool,
) -> LakehouseTableContract {
    LakehouseTableContract {
        table_name,
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // A key's newest row is current; a code-change follow-up appends after the edition it
        // corrects, so the commit instant orders them.
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: &["row_key"],
            newest_first: &["ingested_at_utc"],
            removal_column: Some("row_state"),
            // A row keyed by its own SHA-256 may repeat in a file; repeats stay one key's set.
            keep_ties,
        }),
        columns,
        partition_spec: &[],
        sort_order: &["row_key"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "only new or changed keys and removal markers are appended",
            "provider_fields_json holds every header column",
        ],
        // One Bronze file is one load.
        load: LakehouseLoadUnit::Object {
            column: "source_record_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    }
}

/// 한국도로공사 출입시설: the 법정동 its 도·시군구·읍면동 names name (no lot, so no PNU).
pub const SILVER_HIGHWAY_ACCESS_POINT: LakehouseTableContract =
    table("silver.highway_access_point", AREA_COLUMNS, true);
/// 행정안전부 행정동 세대원수별 세대수, one row per (행정기관코드, 기준연월).
pub const SILVER_HOUSEHOLD_COUNT: LakehouseTableContract =
    table("silver.household_count", AREA_COLUMNS, false);
/// 행정안전부 행정동 성별 연령별 인구수, one row per (행정기관코드, 기준연월).
pub const SILVER_DONG_POPULATION: LakehouseTableContract =
    table("silver.dong_population", AREA_COLUMNS, false);
/// 국가철도공단 철도역, with the PNU its address resolved to; 관련노선 is the station-line endpoint.
pub const SILVER_RAILWAY_STATION: LakehouseTableContract =
    table("silver.railway_station", PARCEL_COLUMNS, false);
/// 해양수산부 항만정보, with the PNU its 항만위치 resolved to when it names one.
pub const SILVER_PORT: LakehouseTableContract = table("silver.port", PARCEL_COLUMNS, false);
/// 한국공항공사 공항 위치정보, with the PNU its 행정구역 address resolved to.
pub const SILVER_AIRPORT: LakehouseTableContract = table("silver.airport", PARCEL_COLUMNS, false);
/// 소상공인시장진흥공단 상가(상권)정보, with the current PNU the place-identity resolver made of 지번코드.
pub const SILVER_COMMERCIAL_DISTRICT: LakehouseTableContract =
    table("silver.commercial_district", PARCEL_COLUMNS, false);
/// 국가데이터처 법정동 연계정보: the 행정동 ↔ 법정동 bridge, one row per (행정동, 법정동, 개정일자).
pub const SILVER_ADM_DONG_CODE_LINKAGE: LakehouseTableContract =
    table("silver.adm_dong_code_linkage", AREA_COLUMNS, false);

/// 한국교육시설안전원 전국초중등학교위치 (표준데이터 내려받기, root ADR-0453), one row per 학교ID, with the PNU its
/// 지번 then 도로명 address resolved to.
pub const SILVER_SCHOOL_LOCATION: LakehouseTableContract =
    table("silver.school_location", PARCEL_COLUMNS, false);
/// 전국대학및전문대학정보 (표준데이터 내려받기, root ADR-0453), one row per (학교명, 본분교구분명), with the PNU its
/// address resolved to.
pub const SILVER_UNIVERSITY: LakehouseTableContract =
    table("silver.university", PARCEL_COLUMNS, false);
/// 전국도시철도역사정보 (표준데이터 내려받기, root ADR-0453), keyed by the row's SHA-256 (no provider key), with the
/// PNU its road address resolved to.
pub const SILVER_SUBWAY_STATION: LakehouseTableContract =
    table("silver.subway_station", PARCEL_COLUMNS, true);

/// The eleven tables, in the contract's file order.
pub const DATA_GO_KR_FILE_DATA_TABLES: [LakehouseTableContract; 11] = [
    SILVER_HIGHWAY_ACCESS_POINT,
    SILVER_HOUSEHOLD_COUNT,
    SILVER_DONG_POPULATION,
    SILVER_RAILWAY_STATION,
    SILVER_PORT,
    SILVER_AIRPORT,
    SILVER_COMMERCIAL_DISTRICT,
    SILVER_ADM_DONG_CODE_LINKAGE,
    SILVER_SCHOOL_LOCATION,
    SILVER_UNIVERSITY,
    SILVER_SUBWAY_STATION,
];
