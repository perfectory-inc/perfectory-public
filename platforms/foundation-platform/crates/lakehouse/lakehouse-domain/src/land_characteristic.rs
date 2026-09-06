//! Named `AL_D195` CSV attributes; price remains owned by the price lane (root ADR-0087).

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const SILVER_LAND_CHARACTERISTIC_COLUMNS: &[LakehouseColumn] = &[
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
        name: "land_serial_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "base_year",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "base_month",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "land_category_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "land_category",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_m2",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "zone_code_1",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "zone_name_1",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "zone_code_2",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "zone_name_2",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "land_use_situation_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "land_use_situation",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "terrain_height_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "terrain_height",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "terrain_shape_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "terrain_shape",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "road_contact_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "road_contact",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "data_reference_date",
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

/// Canonical source attributes without a second parcel geometry (root ADR-0087).
pub const SILVER_LAND_CHARACTERISTIC: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_characteristic",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    columns: SILVER_LAND_CHARACTERISTIC_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "data_reference_date"],
    quality_gates: &["pnu_not_null", "area_m2_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
