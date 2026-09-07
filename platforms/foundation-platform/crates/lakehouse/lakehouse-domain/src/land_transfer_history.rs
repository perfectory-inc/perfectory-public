//! Named `AL_D157` CSV attributes; every parcel event remains in the timeline (root ADR-0089).

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const SILVER_LAND_TRANSFER_HISTORY_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "legal_dong_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "legal_dong_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ledger_kind_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "ledger_kind_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "jibun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "transfer_history_seq",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "closure_seq",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "land_category_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "land_category",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_m2",
        logical_type: "double",
        required: false,
    },
    LakehouseColumn {
        name: "reason_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "reason",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "moved_at",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "erased_at",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "parcel_history_seq",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "data_reference_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_sigungu_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
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

/// Canonical source attributes without a second parcel geometry (root ADR-0089).
pub const SILVER_LAND_TRANSFER_HISTORY: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_transfer_history",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    columns: SILVER_LAND_TRANSFER_HISTORY_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "transfer_history_seq"],
    quality_gates: &["pnu_not_null", "transfer_history_seq_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
