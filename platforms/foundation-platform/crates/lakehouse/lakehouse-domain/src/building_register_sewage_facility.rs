//! HUB 건축물대장 오수정화시설: one row per sewage treatment facility a building register lists (root ADR-0490).

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
    // The SHA-256 of the row: a building's 관리건축물대장PK repeats across its facilities (root ADR-0490).
    text("mgmt_key"),
    text("building_mgmt_key"),
    text("register_kind_code"),
    text("register_kind_name"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("treatment_method_code"),
    text("treatment_method_name"),
    text("other_method"),
    text("capacity_unit_name"),
    text("capacity_persons"),
    text("capacity_cubic_m"),
    text("created_date"),
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

/// Append-only sewage facility observations, keyed by their own content. Values stay the provider's text.
pub const SILVER_BUILDING_REGISTER_SEWAGE_FACILITY: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_register_sewage_facility",
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
