//! The map edit ledger and the served boundary tables (industrial complex, administrative, parcel).
//! Moved out of `lakehouse.rs` unchanged.

use super::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

pub(super) const SILVER_MAP_EDIT_LEDGER_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "unit",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "change_seq",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "feature_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "op",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_geojson",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: false,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "properties_json",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "editor",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "edited_at",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "export_batch_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Append-only lakehouse ledger of admin polygon edits (root ADR-0112 §7).
pub const SILVER_MAP_EDIT_LEDGER: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.map_edit_ledger",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_MAP_EDIT_LEDGER_COLUMNS,
    partition_spec: &[],
    sort_order: &["unit", "change_seq"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "geometry_srid is the Silver CRS of the unit",
        "(unit, change_seq) unique",
        "op is upsert or delete",
        "an upsert carries geometry_wkb and geometry_geojson; a delete carries neither",
        "append_only",
    ],
    // 한 번의 내보내기가 편집 묶음 하나를 덧붙인다. 같은 change_seq 는 두 번 들어가지 않는다.
    load: LakehouseLoadUnit::Run {
        column: "export_batch_id",
    },
};

pub(super) const GOLD_INDUSTRIAL_COMPLEX_BOUNDARY_SERVED_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "complex_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "official_complex_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "origin",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "silver_iceberg_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "edits_through_change_seq",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Gold projection of industrial complex boundaries as they are tiled: current official Silver boundaries with every ledgered admin edit applied (root ADR-0112 §7·§9).
pub const GOLD_INDUSTRIAL_COMPLEX_BOUNDARY_SERVED: LakehouseTableContract =
    LakehouseTableContract {
        table_name: "gold.industrial_complex_boundary_served",
        layer: LakehouseLayer::Gold,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Projection,
        current_row_predicate: None,
        current_row_latest: None,
        columns: GOLD_INDUSTRIAL_COMPLEX_BOUNDARY_SERVED_COLUMNS,
        partition_spec: &[],
        sort_order: &["complex_id"],
        write_distribution: LakehouseWriteDistribution::Hash,
        quality_gates: &[
            "geometry_srid = 5186",
            "one row per complex_id",
            "origin is source or edit",
            "geometry_checksum_sha256 is 64 lowercase hex",
        ],
        // silver.industrial_complex_boundaries 와 silver.map_edit_ledger 에서 파생. 생산자가 overwrite 로 돌아 덮어쓴다.
        load: LakehouseLoadUnit::Derived,
    };

pub(super) const SILVER_ADMINISTRATIVE_BOUNDARIES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "administrative_unit_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "scope_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "canonical_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "display_name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "parent_canonical_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "parent_display_name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
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

/// Canonical Silver `GeoParquet` table for legal-dong administrative boundaries, one
/// append-only snapshot per collected source (root ADR-0112).
pub const SILVER_ADMINISTRATIVE_BOUNDARIES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.administrative_boundaries",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::GeoParquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_ADMINISTRATIVE_BOUNDARIES_COLUMNS,
    partition_spec: &[],
    sort_order: &["source_snapshot_id", "canonical_code"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "geometry_srid = 4326",
        "(canonical_code, source_snapshot_id) unique",
        "administrative_unit_id is kept across renumbering; a new place gets the UUIDv5 of scope:legal-dong:<its first code>",
        "canonical_code is 10 digits",
        "geometry_checksum_sha256 is 64 lowercase hex",
        "append_only",
    ],
    // 한 번의 적재가 수집 원천 스냅숏 하나를 통째로 덧붙인다. 같은 source_snapshot_id 는 두 번 들어가지 않는다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

pub(super) const GOLD_ADMINISTRATIVE_BOUNDARY_SERVED_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "administrative_unit_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "scope_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "canonical_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "display_name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "origin",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "silver_iceberg_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "edits_through_change_seq",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// The administrative boundaries exactly as they are tiled: the newest Silver snapshot with every
/// ledgered `admin` edit applied (root ADR-0112 §7·§9).
pub const GOLD_ADMINISTRATIVE_BOUNDARY_SERVED: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.administrative_boundary_served",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_ADMINISTRATIVE_BOUNDARY_SERVED_COLUMNS,
    partition_spec: &[],
    sort_order: &["administrative_unit_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "geometry_srid = 4326",
        "one row per administrative_unit_id",
        "(scope_kind, canonical_code) unique",
        "origin is source or edit",
        "geometry_checksum_sha256 is 64 lowercase hex",
    ],
    // silver.administrative_boundaries 와 silver.map_edit_ledger 에서 파생. 생산자가 overwrite 로 돌아 덮어쓴다.
    load: LakehouseLoadUnit::Derived,
};

pub(super) const GOLD_PARCEL_BOUNDARY_SERVED_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_wkb",
        logical_type: "binary",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_srid",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "origin",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "silver_iceberg_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "edits_through_change_seq",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// The parcel boundaries exactly as they are tiled: one Silver `source_snapshot_id` with every
/// ledgered `parcels` edit applied, built on the Spark executors (root ADR-0133 §2).
pub const GOLD_PARCEL_BOUNDARY_SERVED: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.parcel_boundary_served",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_PARCEL_BOUNDARY_SERVED_COLUMNS,
    // No partitions, for the reason silver.parcel_boundaries has none: every reader takes the
    // whole snapshot (root ADR-0066).
    partition_spec: &[],
    sort_order: &["pnu"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "geometry_srid = 4326",
        "one row per pnu",
        "origin is source or edit",
        "geometry_checksum_sha256 is 64 lowercase hex",
    ],
    // silver.parcel_boundaries 의 한 판과 silver.map_edit_ledger 에서 파생. 생산자가 overwrite 로 돌아 덮어쓴다.
    load: LakehouseLoadUnit::Derived,
};
