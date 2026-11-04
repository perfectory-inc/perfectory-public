//! HUB 건축인허가 동별개요: one row per building (동) of a permit, its purpose, structure and areas (root ADR-0402).

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
    // 관리동별개요PK: filled and distinct on every row of the 2026-10 edition (root ADR-0402).
    text("mgmt_key"),
    text("permit_mgmt_key"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("main_annex_code"),
    text("main_annex_name"),
    text("main_purpose_code"),
    text("main_purpose_name"),
    text("unit_count"),
    text("family_count"),
    text("household_count"),
    text("structure_code"),
    text("structure_name"),
    text("roof_code"),
    text("roof_name"),
    text("building_area_m2"),
    text("total_floor_area_m2"),
    text("far_floor_area_m2"),
    text("passenger_elevator_count"),
    text("emergency_elevator_count"),
    text("pnu"),
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

/// Append-only building (동) observations of permits. Values stay the provider's text.
pub const SILVER_BUILDING_PERMIT_BUILDING_OUTLINE: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_permit_building_outline",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // A monthly release appends only its changed buildings and a removal marker per vanished building
        // (root ADR-0318, ADR-0402); a building's version is its newest vintage's rows (ADR-0328).
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key: &["mgmt_key"],
            newest_first: &["vintage", "ingested_at_utc"],
            removal_column: Some("row_state"),
            keep_ties: true,
        }),
        columns: COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["pnu", "mgmt_key"],
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
