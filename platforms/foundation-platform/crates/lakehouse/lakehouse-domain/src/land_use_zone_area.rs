//! The 용도지역지구 areas (VWorld NA/24 `AL_D<layer>_00_<YYYYMMDD>` shapefiles, root ADR-0362): one
//! row per designated area of a served layer, its geometry in EPSG:4326 WKB. Per-parcel designations
//! stay with `silver.land_use_plan`; this table is the regulation areas themselves, for the map.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const SILVER_LAND_USE_ZONE_AREA_COLUMNS: &[LakehouseColumn] = &[
    // `D032`: the layer the member name says (AL_D032_00_20260609.shp).
    column("layer_code", "string", true),
    // DBF `A1`: the 33-character area number the provider writes (issuing office, 시군구, notice,
    // zone code and serial); unique within a layer in every release measured (root ADR-0362).
    column("zone_feature_id", "string", true),
    column("layer_name", "string", false),
    column("sigungu_code", "string", false),
    column("zone_code", "string", false),
    column("zone_name", "string", false),
    // DBF `A7`–`A9`: the provider ships them without names (an 8-digit day, then two texts), so
    // they keep their positions as names rather than meanings nobody wrote down.
    column("field_a7", "string", false),
    column("field_a8", "string", false),
    column("field_a9", "string", false),
    column("geometry_wkb", "binary", true),
    column("geometry_srid", "int", true),
    column("bbox_min_x", "double", true),
    column("bbox_min_y", "double", true),
    column("bbox_max_x", "double", true),
    column("bbox_max_y", "double", true),
    column("geometry_checksum_sha256", "string", true),
    column("source_record_id", "string", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
    // `removed` on a removal marker (root ADR-0275, ADR-0311); null on every row the source holds.
    column("row_state", "string", false),
];

/// Regulation areas of the served 용도지역지구 layers (root ADR-0362).
pub const SILVER_LAND_USE_ZONE_AREA: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_use_zone_area",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::GeoParquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // A monthly release appends only the areas whose content or geometry changed and a removal
    // marker per vanished area (root ADR-0275, ADR-0311, ADR-0318), compared one layer at a time.
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["layer_code", "zone_feature_id"],
        // Then the commit instant: a vintage loaded again outranks its earlier load (ADR-0328).
        newest_first: &["source_snapshot_id", "ingested_at_utc"],
        removal_column: Some("row_state"),
        keep_ties: true,
    }),
    columns: SILVER_LAND_USE_ZONE_AREA_COLUMNS,
    partition_spec: &[],
    sort_order: &["layer_code", "zone_feature_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "layer_code_not_null",
        "zone_feature_id_not_null",
        "geometry_wkb_not_null",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
