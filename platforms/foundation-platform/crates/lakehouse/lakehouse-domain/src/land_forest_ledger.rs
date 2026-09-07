//! Named `AL_D003` CSV attributes; forest-ledger fields remain independent of land characteristics (root ADR-0088).

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const SILVER_LAND_FOREST_LEDGER_COLUMNS: &[LakehouseColumn] = &[
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
        name: "ownership_kind_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ownership_kind_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "co_owner_count",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "scale_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "scale_name",
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

/// Canonical source attributes without a second parcel geometry (root ADR-0088).
pub const SILVER_LAND_FOREST_LEDGER: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_forest_ledger",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    columns: SILVER_LAND_FOREST_LEDGER_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "data_reference_date"],
    quality_gates: &["pnu_not_null", "area_m2_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
