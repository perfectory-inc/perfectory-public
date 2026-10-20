//! 한국산업단지공단 전국등록공장현황, one row per factory of each edition, keyed to a PNU found from
//! its address (root ADR-0280 §4).
//!
//! The provider gives no PNU and no stable factory key (순번 is a per-file surrogate), so an edition is
//! appended whole and a reader takes the rows of the newest `source_snapshot_id` (the file's 기준일).
//! Each row says which step found its PNU, or why none did.

use crate::lakehouse::{
    LakehouseColumn, LakehouseEditionRead, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const SILVER_REGISTERED_FACTORY_COLUMNS: &[LakehouseColumn] = &[
    column("serial_no", "long", false),
    column("company_name", "string", false),
    column("complex_name", "string", false),
    column("products", "string", false),
    column("factory_address_raw", "string", false),
    column("pnu", "string", false),
    // `jibun_address` or `road_address`: the step that found the PNU.
    column("pnu_basis", "string", false),
    column("pnu_unresolved_reason", "string", false),
    // How many PNUs the address matched; 1 when resolved, more when ambiguous.
    column("pnu_candidate_count", "int", false),
    column("legal_dong_code", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Every row of every edition of the registered-factory file, with the PNU its address resolved to.
pub const SILVER_REGISTERED_FACTORY: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.registered_factory",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::Editions {
        edition_column: "source_snapshot_id",
        per: &[],
        read: LakehouseEditionRead::Newest,
    },
    columns: SILVER_REGISTERED_FACTORY_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "source_line_number"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "a reader takes the rows of the newest source_snapshot_id",
        "pnu is set exactly when pnu_basis is set and pnu_unresolved_reason is not",
        "(source_record_id, source_line_number) unique",
    ],
    // One provider file (one Bronze zip) is one load.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
