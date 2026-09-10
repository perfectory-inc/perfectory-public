//! Gold projection carrying one parcel's complete public panel payload (root ADR-0096).
//!
//! One row per PNU, with each panel section pre-aggregated into a JSON string column shaped
//! exactly like the `foundation-contracts` response DTO the Catalog API serves from Postgres.
//! The by-PNU serving export re-parses those columns through the same DTOs before writing the
//! baked object, so a section the API could not serve cannot be baked either.

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract,
};

const GOLD_PARCEL_PANEL_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    // Postgres-curated parcel kind (root ADR-0070). The lakehouse has no producer for it, so
    // the projection carries null until a merge path is decided; the column stays so that
    // decision changes data, not schema.
    LakehouseColumn {
        name: "kind",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_m2",
        logical_type: "long",
        required: false,
    },
    LakehouseColumn {
        name: "zonings_json",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "price_json",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "characteristics_json",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "forest_ledger_json",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "transfer_history_json",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "land_rights_json",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "land_right_total",
        logical_type: "long",
        required: true,
    },
    // Deterministic SHA-256 of the content columns only, lineage excluded (root ADR-0099).
    // The daily delta job compares this fingerprint across snapshots to name the parcels
    // whose serving documents must be re-baked.
    LakehouseColumn {
        name: "row_digest",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "string",
        required: true,
    },
];

/// Gold projection for the parcel by-PNU serving bake (root ADR-0096).
pub const GOLD_PARCEL_PANEL: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.parcel_panel",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    columns: GOLD_PARCEL_PANEL_COLUMNS,
    // Follows the canonical parcel table it projects: the snapshot the rows were built from.
    partition_spec: &["source_snapshot_id"],
    sort_order: &["pnu"],
    quality_gates: &[
        "one row per pnu",
        "pnu matches the cadastral grammar",
        "land_right_total is non-negative",
        "published_at_utc is present",
        "row_digest is the sha256 of the content columns only (ADR-0099)",
    ],
    // 여러 silver 표를 조인해 파생. 생산자가 overwrite 로 돌아 덮어쓴다.
    load: LakehouseLoadUnit::Derived,
};
