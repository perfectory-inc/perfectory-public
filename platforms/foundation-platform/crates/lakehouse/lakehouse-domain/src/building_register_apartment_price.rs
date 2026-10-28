//! Headerless HUB apartment-price observations (root ADR-0092).

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
        name: "base_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "price_won",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "notice_date",
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
];

/// Observations are appended by vintage; no unmeasured management-key uniqueness is assumed.
pub const SILVER_BUILDING_REGISTER_APARTMENT_PRICE: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_register_apartment_price",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        current_row_latest: None,
        columns: COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["pnu", "mgmt_key", "base_date"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "append_only",
            "raw_columns_not_null",
            "vintage_not_null",
            "source_part_id_not_null",
        ],
        // A Bronze ZIP spans many independent handoff objects. Comparing its key would skip
        // every later part after the first append; the actual handoff object is the load unit.
        load: LakehouseLoadUnit::Object {
            column: "source_part_id",
            object_prefix: None,
            object_suffix_separator: None,
        },
    };

/// The apartment-price vintages stored as versions (root ADR-0317).
///
/// The first vintage is stored whole, then per vintage only the rows of the
/// `(mgmt_key, base_date)` keys whose rows changed and a removal marker per vanished key. Any
/// vintage is the newest version of every key at or before it
/// (`platform_contracts.rows_as_of`), equal to that vintage's full copy in
/// `silver.building_register_apartment_price` on every source column.
pub const SILVER_BUILDING_REGISTER_APARTMENT_PRICE_VERSIONS: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "silver.building_register_apartment_price_versions",
        layer: LakehouseLayer::Silver,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Canonical,
        current_row_predicate: None,
        // A correction notice repeats a key within a vintage, so a key's version is all of its
        // rows of one commit: ordered by the vintage, then the commit's instant (a vintage loaded
        // again after a partial load, root ADR-0185), every tie current.
        current_row_latest: Some(LakehouseCurrentRowLatest {
            key: &["mgmt_key", "base_date"],
            newest_first: &["vintage", "ingested_at_utc"],
            removal_column: Some("row_state"),
            keep_ties: true,
        }),
        columns: VERSION_COLUMNS,
        partition_spec: &["vintage"],
        sort_order: &["pnu", "mgmt_key", "base_date"],
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

/// The observation's columns and the removal marker's: `removed` on a marker (root ADR-0275),
/// null on every row the source holds.
const VERSION_COLUMNS: &[LakehouseColumn] = &{
    let mut columns = [LakehouseColumn {
        name: "row_state",
        logical_type: "string",
        required: false,
    }; COLUMNS.len() + 1];
    let mut index = 0;
    while index < COLUMNS.len() {
        columns[index] = COLUMNS[index];
        index += 1;
    }
    columns
};
