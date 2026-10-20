//! Unit-indexed reference-date prices derived from the two HUB registers.

use crate::lakehouse::{
    LakehouseColumn, LakehouseEditionRead, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

/// Append-only province batches retaining every assessment reference date.
pub const SILVER_UNIT_OFFICIAL_PRICE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.unit_official_price",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::Editions {
        edition_column: "source_snapshot_id",
        newest_first: &["vintage"],
        per: &[],
        read: LakehouseEditionRead::Newest,
    },
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
        // The vintage the batch joined: what orders the batches, so a reader takes the newest
        // without parsing the batch id (root ADR-0286 stage 1c).
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
