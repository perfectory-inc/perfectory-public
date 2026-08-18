//! JSONL transport contract for the industrial-complex Bronze-to-Silver Spark job.
//!
//! `LakehousePhysicalFormat` deliberately has no JSONL member: a JSONL handoff is writer input,
//! not lakehouse table storage. The Spark job still needs a fixed column set and fixed value
//! domains for its input, and both a Rust producer and the Python job must agree on them. This
//! module is that single definition; the Spark-facing artifact exports it and
//! `tests/lakehouse_contract_artifact.rs` fails when the export drifts.

use crate::lakehouse::SILVER_INDUSTRIAL_COMPLEXES;

/// Dataset name of the JSONL input read by `industrial_complex_bronze_to_silver`.
pub const BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL: &str = "bronze.industrial_complexes_raw_jsonl";

/// `silver.industrial_complexes` columns the Bronze-to-Silver job computes for itself.
///
/// A JSONL producer must not supply these: `complex_id` and `complex_name_normalized` are
/// derived, `valid_to_utc` is a literal null in the job, and `row_checksum_sha256` covers the
/// columns the job produced.
pub const INDUSTRIAL_COMPLEX_SILVER_JOB_DERIVED_COLUMNS: &[&str] = &[
    "complex_id",
    "complex_name_normalized",
    "valid_to_utc",
    "row_checksum_sha256",
];

/// Wire values allowed in the `silver.industrial_complexes` `complex_kind` column.
///
/// `crates/lakehouse/lakehouse-application/tests/industrial_complex_bronze_raw_rows.rs` pins this
/// list to `catalog_domain::IndustrialComplexKind`, which owns the classification.
pub const INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES: &[&str] =
    &["national", "general", "agricultural", "urban_high_tech"];

/// Wire values allowed in the `silver.industrial_complexes` `status` column.
///
/// `crates/lakehouse/lakehouse-application/tests/industrial_complex_bronze_raw_rows.rs` pins this
/// list to `catalog_domain::IndustrialComplexStatus`, which owns the lifecycle domain — the same
/// arrangement as the classification above. Until the canonical table carried a status this list
/// had no owner to be pinned to.
pub const INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES: &[&str] = &[
    "planned",
    "developing",
    "operating",
    "changed",
    "abolished",
    "unknown",
];

/// Wire values allowed in the `silver.industrial_complexes` `lot_sales_status` column.
///
/// `lttot_sttus_nm` (분양상태) is the one label column of the eight this contract took from the
/// profile source that is an enumeration at all: three distinct values across the whole 1,442-row
/// table. `appn_basis_law` (48 distinct) and `devlop_mth` (232) are free text and stay free text —
/// mapping spelling variants onto codes would invent a classification the source never published.
///
/// There is no `unknown` member. An absent cell is `null`, and a label outside this domain fails
/// the export loudly with its row number rather than being folded into a bucket, which is the same
/// treatment `complex_kind` gets. `crates/lakehouse/lakehouse-application/tests/\
/// industrial_complex_bronze_raw_rows.rs` pins this list to
/// `catalog_domain::IndustrialComplexLotSalesStatus`, which owns the domain.
pub const INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES: &[&str] =
    &["planned", "in_progress", "completed"];

/// Columns a `bronze.industrial_complexes_raw_jsonl` record must carry, in Silver contract order.
#[must_use]
pub fn bronze_industrial_complexes_raw_jsonl_columns() -> Vec<&'static str> {
    SILVER_INDUSTRIAL_COMPLEXES
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !INDUSTRIAL_COMPLEX_SILVER_JOB_DERIVED_COLUMNS.contains(name))
        .collect()
}
