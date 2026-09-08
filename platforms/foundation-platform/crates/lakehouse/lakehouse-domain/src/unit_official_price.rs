//! Unit-indexed annual price observations derived from the two HUB registers (ADR-0095).

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

/// Append-only province batches; the serving projection resolves repeated annual keys.
pub const SILVER_UNIT_OFFICIAL_PRICE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.unit_official_price",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    columns: &[
        LakehouseColumn {
            name: "pnu",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "dong_name",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "ho_name",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "base_year",
            logical_type: "int",
            required: true,
        },
        LakehouseColumn {
            name: "price_won",
            logical_type: "long",
            required: true,
        },
        LakehouseColumn {
            name: "sido",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "source_snapshot_id",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "source_record_id",
            logical_type: "string",
            required: true,
        },
    ],
    partition_spec: &["sido"],
    sort_order: &["pnu", "dong_name", "ho_name", "base_year"],
    quality_gates: &[
        "append_only",
        "pnu_not_null",
        "source_snapshot_id_not_null",
        "source_record_id_not_null",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
