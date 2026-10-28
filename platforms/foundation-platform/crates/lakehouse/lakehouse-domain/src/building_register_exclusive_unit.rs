//! HUB exclusive-unit observations bridging management keys and unit labels (ADR-0094).

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
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
        name: "dong_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ho_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_kind",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_no",
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
    // `removed` on a removal marker (root ADR-0275, ADR-0318); null on every row the source holds.
    LakehouseColumn {
        name: "row_state",
        logical_type: "string",
        required: false,
    },
];

/// Append-only observations; management-key and unit-label uniqueness are not assumed.
pub const SILVER_BUILDING_REGISTER_EXCLUSIVE_UNIT: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_register_exclusive_unit",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // A monthly release appends only its changed units and a removal marker per vanished unit
        // (root ADR-0275, ADR-0318); a unit's version is its newest vintage's row.
        current_row_latest: Some(LakehouseCurrentRowLatest {
            key: &["mgmt_key"],
            newest_first: &["vintage"],
            removal_column: Some("row_state"),
            keep_ties: true,
        }),
        columns: COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["pnu", "mgmt_key", "dong_name", "ho_name"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "raw_columns_not_null",
            "vintage_not_null",
            "source_part_id_not_null",
        ],
        load: LakehouseLoadUnit::Object {
            column: "source_part_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };
