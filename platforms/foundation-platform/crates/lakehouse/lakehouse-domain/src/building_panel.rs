//! One row per PNU for independently published buildings, floors and units (ADR-0100).
use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "buildings_json",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "unlinked_units_json",
        logical_type: "string",
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
/// Gold building-panel projection, including explicitly unlinked units.
pub const GOLD_BUILDING_PANEL: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.building_panel",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    columns: COLUMNS,
    partition_spec: &["source_snapshot_id"],
    sort_order: &["pnu"],
    quality_gates: &[
        "one row per pnu",
        "pnu matches the cadastral grammar",
        "nested identities are unique",
        "unlinked units remain visible",
        "row_digest contains content only",
        "published_at_utc is present",
    ],
    load: LakehouseLoadUnit::Derived,
};
