//! VWorld 부동산중개업 files (root ADR-0511 §4): the `AL_D171` register of brokerage offices, one
//! row per office a 시군구 registered. The provider names its 5-digit 시군구 code 법정동코드; it is
//! the registering authority, and the registration number is unique only within it.

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
