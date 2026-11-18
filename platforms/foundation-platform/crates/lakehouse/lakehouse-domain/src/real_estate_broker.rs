//! `VWorld` 부동산중개업 files (root ADR-0511 §4): the `AL_D171` register of brokerage offices, one
//! row per office a 시군구 registered, and the `AL_D172` roster of the people working in each. The
//! provider names its 5-digit 시군구 code 법정동코드; it is the registering authority, and the
//! registration number is unique only within it.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn text(name: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type: "string",
        required,
    }
}

const SILVER_REAL_ESTATE_BROKER_OFFICE_COLUMNS: &[LakehouseColumn] = &[
    text("sigungu_code", true),
    text("sigungu_name", false),
    text("registration_no", true),
    text("office_name", false),
    text("representative_name", false),
    text("status_code", false),
    text("status_name", false),
    text("registered_on", false),
    // Blank on every row of the 2026-11-09 file; kept so a value the provider starts sending lands.
    text("phone_number", false),
    text("guarantee_start_on", false),
    text("guarantee_end_on", false),
    text("data_reference_date", false),
    text("jibun_address", false),
    text("road_address", false),
    text("road_address_code", false),
    text("source_record_id", true),
    text("source_snapshot_id", true),
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    // `removed` on a removal marker (root ADR-0275, ADR-0311); null on every row the source holds.
    text("row_state", false),
];

/// Every registered brokerage office, appended only where an office changed (root ADR-0511 §4).
pub const SILVER_REAL_ESTATE_BROKER_OFFICE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.real_estate_broker_office",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // An office is its 시군구 and registration number: 194 numbers of the 2026-11-09 file repeat
    // across 시군구 (old-format numbers), and 10 pairs repeat within one, so a key is a set of rows.
    // A daily release appends only the changed offices and a removal marker per closed office.
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: &["sigungu_code", "registration_no"],
        // The release's file date, then the commit instant (ADR-0328).
        newest_first: &["source_snapshot_id", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: true,
    }),
    columns: SILVER_REAL_ESTATE_BROKER_OFFICE_COLUMNS,
    partition_spec: &[],
    sort_order: &["sigungu_code", "registration_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &["sigungu_code_not_null", "registration_no_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_REAL_ESTATE_BROKER_MEMBER_COLUMNS: &[LakehouseColumn] = &[
    text("sigungu_code", true),
    text("sigungu_name", false),
    text("registration_no", true),
    text("office_name", false),
    text("member_name", false),
    // 1 중개인, 2 공인중개사, 3 법인, 4 중개보조원 (5 has no name in the 2026-11-09 file: 11 rows).
    text("member_kind_code", false),
    text("member_kind_name", false),
    // Blank for 중개보조원, who hold none, and for 168 공인중개사 rows of the 2026-11-09 file.
    text("license_no", false),
    text("license_acquired_on", false),
    // 01 대표, 02 감사, 03 이사, 04 일반.
    text("position_code", false),
    text("position_name", false),
    text("data_reference_date", false),
    text("source_record_id", true),
    text("source_snapshot_id", true),
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    // `removed` on a removal marker (root ADR-0275, ADR-0311); null on every row the source holds.
    text("row_state", false),
];

/// Every person registered to a brokerage office, as the office's roster (root ADR-0511 §4).
pub const SILVER_REAL_ESTATE_BROKER_MEMBER: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.real_estate_broker_member",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // An office's roster is one version, keyed as the office is: a person has no provider key
    // (a 중개보조원 has no license number, and 41 rows of the 2026-11-09 file repeat whole), so a
    // daily release appends every row of an office whose roster changed and a removal marker per
    // office that left, as the land-ownership lane keys a parcel's owners by the parcel.
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: &["sigungu_code", "registration_no"],
        newest_first: &["source_snapshot_id", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: true,
    }),
    columns: SILVER_REAL_ESTATE_BROKER_MEMBER_COLUMNS,
    partition_spec: &[],
    sort_order: &[
        "sigungu_code",
        "registration_no",
        "position_code",
        "license_no",
    ],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &["sigungu_code_not_null", "registration_no_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
