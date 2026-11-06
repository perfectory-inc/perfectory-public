//! HUB 건축인허가 대수선: one row per repaired structural part of a permit (root ADR-0402).

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
    // The SHA-256 of the row: the provider's 관리허가대장PK repeats across a permit's rows (root ADR-0402).
    text("mgmt_key"),
    text("permit_mgmt_key"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("major_repair_kind_code"),
    text("major_repair_kind_name"),
    text("major_repair_change_code"),
    text("major_repair_change_name"),
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

/// Append-only major-repair observations, keyed by their own content. Values stay the provider's text.
pub const SILVER_BUILDING_PERMIT_MAJOR_REPAIR: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_permit_major_repair",
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
