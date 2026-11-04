//! HUB 부속지번: every other parcel a register's building stands on (root ADR-0395).

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
    text("register_kind_code"),
    text("register_kind_name"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("sub_sigungu_cd"),
    text("sub_bjdong_cd"),
    text("sub_san_gubun"),
    text("sub_bonbeon"),
    text("sub_bubeon"),
    text("pnu"),
    // The parcel the building also stands on: the second 지번 of the row (root ADR-0395).
    text("sub_pnu"),
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

/// Append-only sub-parcel observations, many rows per register. Values stay the provider's text.
pub const SILVER_BUILDING_REGISTER_SUB_PARCEL: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_sub_parcel",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // A register's version is the set of its sub-parcel rows of its newest vintage (keep_ties,
    // ADR-0328); a monthly release appends only the registers whose set changed and a removal
    // marker per vanished register (root ADR-0318, ADR-0395).
    row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
        key: &["mgmt_key"],
        newest_first: &["vintage", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: true,
    }),
    columns: COLUMNS,
    partition_spec: &["vintage"],
    sort_order: &["sub_pnu", "mgmt_key"],
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
