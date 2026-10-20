//! One row per building of a PNU with its units' official price histories, apart from the
//! building panel (root ADR-0190, ADR-0204).
use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat, LakehouseRowKey,
    LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

const COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "building_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "units_json",
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
/// Gold unit price panel: each priced unit under the PNU whose building document carries it, one
/// row per building there (the nil UUID for units linked to no building).
pub const GOLD_UNIT_PRICE_PANEL: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.unit_price",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::Replaced {
        key: LakehouseRowKey::Columns(&["pnu", "building_id"]),
    },
    columns: COLUMNS,
    partition_spec: &["source_snapshot_id"],
    sort_order: &["pnu", "building_id"],
    write_distribution: LakehouseWriteDistribution::Range,
    quality_gates: &[
        "one row per (pnu, building_id)",
        "pnu matches the cadastral grammar",
        "every row has a priced unit",
        "row_digest contains content only",
        "published_at_utc is present",
    ],
    load: LakehouseLoadUnit::Derived,
};
