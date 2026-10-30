//! The lines a 실거래가 export carried that the Silver load did not load (root ADR-0364): a dead
//! letter per line, referencing its Bronze object and line, so one bad provider row never stops a
//! lane and none is lost without a trace.

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const SILVER_REAL_TRANSACTION_REJECTED_COLUMNS: &[LakehouseColumn] = &[
    column("dataset_slug", "string", true),
    column("deal_ym", "string", true),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("raw_line", "string", false),
    column("reason", "string", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Every line of a 실거래가 export the Silver load rejected, with why and where it is in Bronze.
pub const SILVER_REAL_TRANSACTION_REJECTED: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.real_transaction_rejected",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_REAL_TRANSACTION_REJECTED_COLUMNS,
    partition_spec: &[],
    sort_order: &["source_record_id", "source_line_number"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(source_record_id, source_line_number) unique",
        "line 0 is an export refused whole",
    ],
    // One Bronze object's rejected lines are one load of this table.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
