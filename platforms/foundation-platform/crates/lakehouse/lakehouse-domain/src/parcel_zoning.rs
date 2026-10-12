//! One row per PNU with its zoning verdict, apart from the parcel panel (root ADR-0193).
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
        name: "zonings_json",
        logical_type: "string",
        required: true,
    },
    // The lineage path when the verdict is a predecessor's (root ADR-0113 §6); null when own.
    LakehouseColumn {
        name: "attached_via",
        logical_type: "string",
        required: false,
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
/// Gold parcel zoning: the zoning verdict of each served parcel that has one.
pub const GOLD_PARCEL_ZONING: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.parcel_zoning",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    columns: COLUMNS,
    partition_spec: &["source_snapshot_id"],
    sort_order: &["pnu"],
    write_distribution: LakehouseWriteDistribution::Range,
    quality_gates: &[
        "one row per pnu",
        "pnu matches the cadastral grammar",
        "every row has a zoning",
        "row_digest contains content only",
        "published_at_utc is present",
    ],
    load: LakehouseLoadUnit::Derived,
};
