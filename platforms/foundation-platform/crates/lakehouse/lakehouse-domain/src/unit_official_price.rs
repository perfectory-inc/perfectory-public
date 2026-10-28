//! Unit-indexed reference-date prices derived from the two HUB registers.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

/// Append-only province batches retaining every assessment reference date.
pub const SILVER_UNIT_OFFICIAL_PRICE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.unit_official_price",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: &[
        LakehouseColumn {
            name: "mgm_bldrgst_pk",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "pnu",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "dong_name",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "ho_name",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "base_date",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "price_won",
            logical_type: "long",
            required: true,
        },
        LakehouseColumn {
            name: "sido",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "source_snapshot_id",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "source_record_id",
            logical_type: "string",
            required: true,
        },
    ],
    partition_spec: &["sido"],
    sort_order: &["pnu", "dong_name", "ho_name", "base_date"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "pnu_not_null",
        "source_snapshot_id_not_null",
        "source_record_id_not_null",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// The unit prices stored as versions (root ADR-0317): a vintage's join appends only the rows of
/// the `(mgm_bldrgst_pk, base_date)` keys whose row changed and a removal marker per vanished key.
/// Any vintage is the newest version of every key at or before it (`platform_contracts.rows_as_of`),
/// equal on every column but the load's to the batch `silver.unit_official_price` holds for it.
pub const SILVER_UNIT_OFFICIAL_PRICE_VERSIONS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.unit_official_price_versions",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // One row per key in a vintage (correction notices resolve before the join); ordered by the
    // vintage, then the commit's instant (a vintage joined again from reloaded sources).
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["mgm_bldrgst_pk", "base_date"],
        newest_first: &["vintage", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: false,
    }),
    columns: &[
        LakehouseColumn {
            name: "mgm_bldrgst_pk",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "pnu",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "dong_name",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "ho_name",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "base_date",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "price_won",
            logical_type: "long",
            required: true,
        },
        LakehouseColumn {
            name: "sido",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "vintage",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "source_snapshot_id",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "source_record_id",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "ingested_at_utc",
            logical_type: "timestamp",
            required: true,
        },
        // `removed` on a removal marker (root ADR-0275); null on every joined row.
        LakehouseColumn {
            name: "row_state",
            logical_type: "string",
            required: false,
        },
    ],
    partition_spec: &["vintage"],
    sort_order: &["pnu", "dong_name", "ho_name", "base_date"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "pnu_not_null",
        "vintage_not_null",
        "source_snapshot_id_not_null",
        "source_record_id_not_null",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
