//! Named `AL_D161` CSV attributes: one row per co-owner of a parcel, and per unit of a collective
//! building on it (root ADR-0360). The file names no person; land category, area and price stay
//! with the lanes that own them (토지특성, 개별공시지가).

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn text(name: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type: "string",
        required,
    }
}

const SILVER_LAND_OWNERSHIP_COLUMNS: &[LakehouseColumn] = &[
    text("pnu", true),
    text("legal_dong_code", false),
    text("legal_dong_name", false),
    text("ledger_kind_code", false),
    text("ledger_kind_name", false),
    text("jibun", false),
    text("collective_building_serial_no", false),
    text("dong_name", false),
    text("floor_name", false),
    text("ho_name", false),
    text("room_name", false),
    text("co_owner_serial_no", true),
    text("base_year_month", false),
    text("ownership_kind_code", false),
    text("ownership_kind_name", false),
    text("residence_kind_code", false),
    text("residence_kind_name", false),
    text("state_agency_kind_code", false),
    text("state_agency_kind_name", false),
    text("ownership_change_reason_code", false),
    text("ownership_change_reason", false),
    text("ownership_changed_on", false),
    LakehouseColumn {
        name: "co_owner_count",
        logical_type: "int",
        required: false,
    },
    text("data_reference_date", false),
    text("source_sigungu_code", false),
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

/// Ownership facts per parcel without a second parcel geometry (root ADR-0360).
pub const SILVER_LAND_OWNERSHIP: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_ownership",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // A parcel's owners are one version: a release appends every row of a parcel whose owners
    // changed and a removal marker per vanished parcel (root ADR-0275, ADR-0311), as the land-use
    // plan lane keys its many designations by the parcel.
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["pnu"],
        // Then the commit instant: a vintage loaded again outranks its earlier load (ADR-0328).
        newest_first: &["source_snapshot_id", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: true,
    }),
    columns: SILVER_LAND_OWNERSHIP_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "collective_building_serial_no", "co_owner_serial_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &["pnu_not_null", "co_owner_serial_no_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
