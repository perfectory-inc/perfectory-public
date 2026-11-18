//! HUB 주택인허가 기본개요: one row per housing project approval, with the dates of its life (root ADR-0511).

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
    text("mgmt_key"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("purpose_code"),
    text("purpose_name"),
    text("structure_code"),
    text("structure_name"),
    text("main_building_count"),
    text("total_floor_area_m2"),
    text("total_household_count"),
    text("approval_date"),
    text("start_scheduled_date"),
    text("start_actual_date"),
    text("use_inspection_scheduled_date"),
    text("use_inspection_date"),
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

/// Append-only housing approval observations. Values stay the provider's text: dates are `YYYYMMDD` or empty.
pub const SILVER_HOUSING_PERMIT_BASIS_OUTLINE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.housing_permit_basis_outline",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // A monthly release appends only its changed approvals and a removal marker per vanished approval
    // (root ADR-0318); an approval's version is its newest vintage's rows (ADR-0328).
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
