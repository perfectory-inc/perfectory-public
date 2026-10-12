//! One row per unit and reference date: the unit price history, apart from the building panel
//! (root ADR-0189).
use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

const COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "unit_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "register_pk",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "base_date",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "price_won",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "row_digest",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "string",
        required: true,
    },
];
/// Gold unit price history, placed under the PNU whose building document carries the unit.
pub const GOLD_UNIT_PRICE_HISTORY: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.unit_price_history",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    columns: COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "register_pk", "base_date"],
    write_distribution: LakehouseWriteDistribution::Range,
    quality_gates: &[
        "one row per register_pk and base_date",
        "pnu matches the cadastral grammar",
        "base_date is eight digits and price_won is non-negative",
        "row_digest contains content only",
        "published_at_utc is present",
    ],
    load: LakehouseLoadUnit::Derived,
};
