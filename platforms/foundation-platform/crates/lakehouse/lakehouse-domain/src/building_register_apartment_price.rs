//! Headerless HUB apartment-price observations (root ADR-0092).

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "mgmt_key",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "sigungu_cd",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bjdong_cd",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "san_gubun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bonbeon",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bubeon",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "base_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "price_won",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "notice_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "raw_columns",
        logical_type: "array<string>",
        required: true,
    },
    LakehouseColumn {
        name: "vintage",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_part_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_line_number",
        logical_type: "long",
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

/// Observations are appended by vintage; no unmeasured management-key uniqueness is assumed.
pub const SILVER_BUILDING_REGISTER_APARTMENT_PRICE: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_register_apartment_price",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        columns: COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["pnu", "mgmt_key", "base_date"],
        quality_gates: &[
            "append_only",
            "raw_columns_not_null",
            "vintage_not_null",
            "source_part_id_not_null",
        ],
        // A Bronze ZIP spans many independent handoff objects. Comparing its key would skip
        // every later part after the first append; the actual handoff object is the load unit.
        load: LakehouseLoadUnit::Object {
            column: "source_part_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };
