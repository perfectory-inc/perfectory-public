//! HUB 철거멸실관리대장: one row per demolition or loss report, the end of a building (root ADR-0397).

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
    // The SHA-256 of the row: the provider's 관리번호 column is empty on every row (root ADR-0397).
    text("mgmt_key"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("demolition_kind_code"),
    text("demolition_kind_name"),
    text("demolition_start_date"),
    text("demolition_end_date"),
    text("demolition_loss_date"),
    text("total_floor_area_m2"),
    text("building_count"),
    text("main_purpose_code"),
    text("main_purpose_name"),
    text("structure_code"),
    text("structure_name"),
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

/// Append-only demolition observations, keyed by their own content. Values stay the provider's text.
pub const SILVER_BUILDING_PERMIT_DEMOLITION_LOSS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_permit_demolition_loss",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // A row is its own key: a monthly release appends only rows it has not held and a removal
    // marker per row it no longer holds (root ADR-0318, ADR-0397). Repeated rows stay one key's set.
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
