//! The 실거래 region statistics (root ADR-0350 §4, ADR-0371 §4): per (dataset, 법정동 or 시군구,
//! contract month or the trailing 12 months) the deal and cancellation counts and the m² price
//! quartiles in 만원, written beside the 실거래 by-PNU kind by its producer from the same deals.

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat, LakehouseRowKey,
    LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const GOLD_REAL_TRANSACTION_REGION_STATS_COLUMNS: &[LakehouseColumn] = &[
    column("dataset_slug", "string", true),
    column("region_level", "string", true),
    column("region_code", "string", true),
    column("period_kind", "string", true),
    column("period", "string", true),
    column("deal_count", "long", true),
    column("cancelled_count", "long", true),
    column("land_price_per_m2_p25", "double", false),
    column("land_price_per_m2_median", "double", false),
    column("land_price_per_m2_p75", "double", false),
    column("building_price_per_m2_p25", "double", false),
    column("building_price_per_m2_median", "double", false),
    column("building_price_per_m2_p75", "double", false),
    column("row_digest", "string", true),
    column("source_snapshot_id", "string", true),
    column("published_at_utc", "string", true),
];

/// Region statistics of 실거래, keyed by region code and period, not by PNU: hash-distributed.
pub const GOLD_REAL_TRANSACTION_REGION_STATS: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.real_transaction_region_stats",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    // Root ADR-0286: a run merges its 시군구's rows by the region key, replacing each in place.
    row_versions: LakehouseRowVersions::Replaced {
        key: LakehouseRowKey::Columns(&[
            "dataset_slug",
            "region_level",
            "region_code",
            "period_kind",
            "period",
        ]),
    },
    columns: GOLD_REAL_TRANSACTION_REGION_STATS_COLUMNS,
    partition_spec: &["dataset_slug"],
    sort_order: &["region_code", "period"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "one row per (dataset_slug, region_level, region_code, period_kind, period)",
        "region_code is a 10-digit 법정동 or 시군구 code",
        "row_digest contains content only",
        "published_at_utc is present",
    ],
    // Rebuilt from silver.real_transaction by the 실거래 kind's producer, whole or by 시군구.
    load: LakehouseLoadUnit::Derived,
};
