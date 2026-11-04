//! HUB 월별 건물 에너지(전기·가스): each 지번's usage rows of one month (root ADR-0396).

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
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
    // `use_ym|시군구|법정동|대지구분|번|지`: the provider names no key; a 지번's rows of a month are
    // one version (its 순번 is not stable between publications, root ADR-0396).
    text("mgmt_key"),
    text("use_ym"),
    text("sigungu_cd"),
    text("bjdong_cd"),
    text("san_gubun"),
    text("bonbeon"),
    text("bubeon"),
    text("usage_amount"),
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

/// Append-only monthly 전기 usage rows by 지번. Values stay the provider's text.
pub const SILVER_BUILDING_ENERGY_MONTHLY_ELECTRICITY: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_energy_monthly_electricity",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // One release is one usage month: it appends only the 지번 of that month whose rows changed and
        // a removal marker per 지번 of that month it no longer holds; other months are not compared
        // (`bucket_scope` release, root ADR-0396). A later publication of a month outranks the earlier
        // by its vintage (ADR-0328).
        current_row_latest: Some(LakehouseCurrentRowLatest {
            key: &["mgmt_key"],
            newest_first: &["vintage", "ingested_at_utc"],
            removal_column: Some("row_state"),
            keep_ties: true,
        }),
        columns: COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["pnu", "use_ym"],
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

/// Append-only monthly 가스 usage rows by 지번. Values stay the provider's text.
pub const SILVER_BUILDING_ENERGY_MONTHLY_GAS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_energy_monthly_gas",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // One release is one usage month: it appends only the 지번 of that month whose rows changed and
    // a removal marker per 지번 of that month it no longer holds; other months are not compared
    // (`bucket_scope` release, root ADR-0396). A later publication of a month outranks the earlier
    // by its vintage (ADR-0328).
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["mgmt_key"],
        newest_first: &["vintage", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: true,
    }),
    columns: COLUMNS,
    partition_spec: &["vintage"],
    sort_order: &["pnu", "use_ym"],
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
