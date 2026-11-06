//! HUB 건축물 정기점검이력: one row per building a regular inspection covered (root ADR-0402, ADR-0404).

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn text(name: &'static str) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type: "string",
        required: false,
    }
}

const COLUMNS: &[LakehouseColumn] = &[
    // The SHA-256 of the row: 점검실시PK repeats across the buildings one inspection covers
    // (58,343 values over 64,328 rows in the 2026-10 edition; root ADR-0404).
    text("mgmt_key"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("bonbeon"),
    text("bubeon"),
    text("dong_name"),
    text("inspection_key"),
    text("building_register_key"),
    text("inspection_agency_name"),
    text("inspection_start_date"),
    text("processed_date"),
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
    text("row_state"),
];

/// Append-only inspection observations, keyed by their own content. No `pnu`: a row reaches its
/// parcel through `building_register_key` and the titles' current rows at read (root ADR-0404).
pub const SILVER_BUILDING_MAINTENANCE_REGULAR_INSPECTION_HISTORY: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_maintenance_regular_inspection_history",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // A row is its own key: a monthly release appends only rows it has not held and a removal
        // marker per row it no longer holds (root ADR-0318, ADR-0397, ADR-0404).
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: &["mgmt_key"],
            newest_first: &["vintage", "ingested_at_utc"],
            removal_column: Some("row_state"),
            keep_ties: true,
        }),
        columns: COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["building_register_key", "mgmt_key"],
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
