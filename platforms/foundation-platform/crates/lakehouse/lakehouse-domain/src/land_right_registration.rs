//! Named `AL_D006` CSV attributes; every registered unit remains in the ledger (root ADR-0090).

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const SILVER_LAND_RIGHT_REGISTRATION_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "right_serial_no",
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
        name: "jibun",
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
        name: "building_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "dong_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ho_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "room_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "right_ratio",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "closure_kind_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "closure_kind_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "related_parcel_code",
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

/// Canonical source attributes without a second parcel geometry (root ADR-0090).
pub const SILVER_LAND_RIGHT_REGISTRATION: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_right_registration",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    columns: SILVER_LAND_RIGHT_REGISTRATION_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "right_serial_no"],
    quality_gates: &["pnu_not_null", "right_serial_no_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
