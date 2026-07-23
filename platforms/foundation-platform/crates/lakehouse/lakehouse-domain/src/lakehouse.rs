//! Lakehouse table contracts for Catalog-owned data products.
//!
//! These contracts intentionally do not depend on a concrete Iceberg SDK. They define the table
//! names, columns, partitioning, sorting, and quality gates that writer/query adapters must honor.

/// Medallion layer that owns a lakehouse table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseLayer {
    /// Cleaned, typed, source-aligned canonical table.
    Silver,
    /// Serving-oriented projection or artifact input table.
    Gold,
}

/// Physical file format used by the lakehouse table.
///
/// This is the canonical Silver/Gold storage contract. App-layer JSONL handoff payloads are
/// transient writer/model inputs and must not be treated as lakehouse table storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehousePhysicalFormat {
    /// Apache Parquet table without geometry metadata.
    Parquet,
    /// `GeoParquet` table with geometry metadata.
    GeoParquet,
}

/// Serving role of a lakehouse table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseServingRole {
    /// Canonical source table for the domain fact.
    Canonical,
    /// Consumer/API-oriented projection derived from canonical tables.
    Projection,
    /// Spatial pruning locator derived from canonical geometry.
    SpatialLocator,
}

/// One column in a lakehouse table contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LakehouseColumn {
    /// Stable column name.
    pub name: &'static str,
    /// Iceberg-facing logical type name used by docs and adapters.
    pub logical_type: &'static str,
    /// Whether the column must be present and non-null for valid rows.
    pub required: bool,
}

/// Static contract for a Catalog-owned lakehouse table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LakehouseTableContract {
    /// Fully qualified logical table name.
    pub table_name: &'static str,
    /// Medallion layer for this table.
    pub layer: LakehouseLayer,
    /// Physical file format.
    pub physical_format: LakehousePhysicalFormat,
    /// Serving role.
    pub serving_role: LakehouseServingRole,
    /// Stable column contract.
    pub columns: &'static [LakehouseColumn],
    /// Iceberg partition spec expressed as stable contract text.
    pub partition_spec: &'static [&'static str],
    /// Sort order expressed as stable contract text.
    pub sort_order: &'static [&'static str],
    /// Quality gates that must pass before publish/promote.
    pub quality_gates: &'static [&'static str],
}

const SILVER_INDUSTRIAL_COMPLEXES_COLUMNS: &[LakehouseColumn] = &[
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
        name: "complex_name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_name_normalized",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "primary_bjdong_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "address_text",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "management_agency_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "developer_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "designated_date",
        logical_type: "date",
        required: false,
    },
    LakehouseColumn {
        name: "completion_date",
        logical_type: "date",
        required: false,
    },
    LakehouseColumn {
        name: "official_area_sqm",
        logical_type: "decimal(18,2)",
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
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "row_checksum_sha256",
        logical_type: "string",
        required: true,
    },
];

const SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "boundary_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "boundary_kind",
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
        name: "bbox_min_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "centroid_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "centroid_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "area_sqm_calculated",
        logical_type: "decimal(18,2)",
        required: false,
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
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

const SILVER_PARCEL_BOUNDARIES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "boundary_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "bjdong_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "jibun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bonbun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "bubun",
        logical_type: "string",
        required: false,
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
        name: "bbox_min_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_y",
        logical_type: "double",
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
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

const SILVER_BUILDING_REGISTER_FLOORS_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "floor_row_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "mgm_bldrgst_pk",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "floor_type_code_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_type_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_number_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_label_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "floor_number",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "floor_index",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "floor_display_ko",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "normalization_status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "normalization_reason",
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
        name: "bronze_object_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_line_number",
        logical_type: "long",
        required: false,
    },
    LakehouseColumn {
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "row_checksum_sha256",
        logical_type: "string",
        required: true,
    },
];

const SILVER_BUILDING_REGISTER_UNITS_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "unit_row_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "mgm_bldrgst_pk",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "register_parcel_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "dong_join_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "dong_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "unit_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "unit_number",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "unit_label_ko",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "unit_designation",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "floor_index",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "floor_number",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "building_mgm_bldrgst_pk",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "building_link_method",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "building_main_or_annex",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "building_title_unit_count",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "normalization_status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "normalization_reason",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "normalization_application_id",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "bronze_object_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_line_number",
        logical_type: "long",
        required: false,
    },
    LakehouseColumn {
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "row_checksum_sha256",
        logical_type: "string",
        required: true,
    },
];

const SILVER_BUILDING_REGISTER_UNIT_AREAS_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "area_row_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "mgm_bldrgst_pk",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "register_kind_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "register_type_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "register_parcel_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "dong_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "unit_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "unit_designation",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "floor_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "floor_index",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "floor_number",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "floor_label_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "area_kind_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "main_or_annex_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "structure_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "usage_code_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "usage_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "usage_detail_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_m2",
        logical_type: "double",
        required: false,
    },
    LakehouseColumn {
        name: "area_m2_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "created_date_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "normalization_status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "normalization_reason",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "bronze_object_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_line_number",
        logical_type: "long",
        required: false,
    },
    LakehouseColumn {
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "row_checksum_sha256",
        logical_type: "string",
        required: true,
    },
];

const SILVER_COMPLEX_PARCEL_MEMBERSHIPS_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "membership_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "parcel_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "bjdong_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "membership_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_method",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "area_overlap_sqm",
        logical_type: "decimal(18,2)",
        required: false,
    },
    LakehouseColumn {
        name: "overlap_ratio",
        logical_type: "decimal(9,6)",
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
        name: "valid_from_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to_utc",
        logical_type: "timestamp",
        required: false,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    LakehouseColumn {
        name: "row_checksum_sha256",
        logical_type: "string",
        required: true,
    },
];

const GOLD_COMPLEX_CATALOG_COLUMNS: &[LakehouseColumn] = &[
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
        name: "name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "address_text",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "official_area_sqm",
        logical_type: "decimal(18,2)",
        required: false,
    },
    LakehouseColumn {
        name: "calculated_area_sqm",
        logical_type: "decimal(18,2)",
        required: false,
    },
    LakehouseColumn {
        name: "parcel_count",
        logical_type: "long",
        required: true,
    },
    LakehouseColumn {
        name: "boundary_object_key",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "iceberg_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

const GOLD_COMPLEX_SPATIAL_LOCATOR_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "spatial_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "complex_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "boundary_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_min_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_x",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "bbox_max_y",
        logical_type: "double",
        required: true,
    },
    LakehouseColumn {
        name: "geometry_checksum_sha256",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "object_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "iceberg_snapshot_id",
        logical_type: "string",
        required: true,
    },
];

/// Canonical Silver table for industrial complex facts.
pub const SILVER_INDUSTRIAL_COMPLEXES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.industrial_complexes",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_INDUSTRIAL_COMPLEXES_COLUMNS,
    partition_spec: &["sido_code", "bucket(32, complex_id)"],
    sort_order: &[
        "sigungu_code",
        "complex_name_normalized",
        "official_complex_code",
    ],
    quality_gates: &[
        "(official_complex_code, source_snapshot_id) unique",
        "complex_name non-empty",
        "complex_kind is a supported domain wire value",
        "official_area_sqm > 0 when present",
        "active rows for the same complex_id do not overlap",
    ],
};

/// Canonical Silver `GeoParquet` table for industrial complex boundaries.
pub const SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.industrial_complex_boundaries",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::GeoParquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES_COLUMNS,
    partition_spec: &["sido_code", "bucket(32, complex_id)"],
    sort_order: &["complex_id", "boundary_kind", "valid_from_utc"],
    quality_gates: &[
        "geometry_srid = 4326",
        "bbox min/max ordering is valid",
        "centroid is inside bbox",
        "geometry_wkb is valid polygon or multipolygon",
        "active official boundary is at most one per complex_id",
        "geometry_checksum_sha256 is 64 lowercase hex",
    ],
};

/// Canonical Silver `GeoParquet` table for cadastral parcel boundaries.
pub const SILVER_PARCEL_BOUNDARIES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.parcel_boundaries",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::GeoParquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_PARCEL_BOUNDARIES_COLUMNS,
    partition_spec: &["sigungu_code", "bucket(256, pnu)"],
    sort_order: &["pnu", "valid_from_utc"],
    quality_gates: &[
        "pnu passes shared PNU validation",
        "geometry_srid = 4326",
        "bbox min/max ordering is valid",
        "geometry_wkb is valid polygon or multipolygon",
        "one active parcel boundary per pnu",
        "geometry_checksum_sha256 is 64 lowercase hex",
    ],
};

/// Canonical Silver table for official building-register floor rows.
pub const SILVER_BUILDING_REGISTER_FLOORS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_floors",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_BUILDING_REGISTER_FLOORS_COLUMNS,
    partition_spec: &["bucket(16, mgm_bldrgst_pk)"],
    sort_order: &["mgm_bldrgst_pk", "floor_index", "floor_row_id"],
    quality_gates: &[
        "floor_row_id_not_null",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "row_checksum_sha256_valid",
    ],
};

/// Canonical Silver table for official building-register unit rows.
pub const SILVER_BUILDING_REGISTER_UNITS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_units",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_BUILDING_REGISTER_UNITS_COLUMNS,
    partition_spec: &["bucket(256, pnu)"],
    sort_order: &[
        "pnu",
        "building_mgm_bldrgst_pk",
        "floor_index",
        "unit_number",
        "unit_row_id",
    ],
    quality_gates: &[
        "unit_row_id_not_null",
        "register_parcel_key_not_null",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "building_link_method_in_allowed_values",
        "row_checksum_sha256_valid",
    ],
};

/// Canonical Silver table for official building-register unit-area (전유공용면적) rows.
///
/// Area rows join `silver.building_register_units` directly on the provider's
/// shared `mgm_bldrgst_pk`.
pub const SILVER_BUILDING_REGISTER_UNIT_AREAS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_unit_areas",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_BUILDING_REGISTER_UNIT_AREAS_COLUMNS,
    partition_spec: &["bucket(32, mgm_bldrgst_pk)"],
    sort_order: &["mgm_bldrgst_pk", "area_kind", "floor_index", "area_row_id"],
    quality_gates: &[
        "area_row_id_not_null",
        "register_parcel_key_not_null",
        "area_kind_in_allowed_values",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "row_checksum_sha256_valid",
    ],
};

/// Canonical Silver table for industrial complex to parcel membership.
pub const SILVER_COMPLEX_PARCEL_MEMBERSHIPS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.complex_parcel_memberships",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    columns: SILVER_COMPLEX_PARCEL_MEMBERSHIPS_COLUMNS,
    partition_spec: &["sigungu_code", "bucket(256, pnu)"],
    sort_order: &["complex_id", "pnu", "membership_kind"],
    quality_gates: &[
        "pnu passes shared PNU validation",
        "one active inside or intersects membership per complex_id and pnu",
        "overlap_ratio is between 0 and 1 when present",
        "excluded rows include source method and lineage",
    ],
};

/// Gold projection for API list/detail and consumer read models.
pub const GOLD_COMPLEX_CATALOG: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.complex_catalog",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    columns: GOLD_COMPLEX_CATALOG_COLUMNS,
    partition_spec: &["sido_code"],
    sort_order: &["sigungu_code", "name", "complex_id"],
    quality_gates: &[
        "one active row per complex_id",
        "parcel_count is non-negative",
        "iceberg_snapshot_id is present",
        "published_at_utc is present",
    ],
};

/// Gold spatial locator for bbox, tile, or H3 based pruning.
pub const GOLD_COMPLEX_SPATIAL_LOCATOR: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.complex_spatial_locator",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::SpatialLocator,
    columns: GOLD_COMPLEX_SPATIAL_LOCATOR_COLUMNS,
    partition_spec: &["spatial_key_prefix"],
    sort_order: &["spatial_key", "complex_id"],
    quality_gates: &[
        "spatial_key is stable",
        "bbox min/max ordering is valid",
        "object_key points to a source GeoParquet artifact",
        "iceberg_snapshot_id is present",
    ],
};

const INDUSTRIAL_COMPLEX_LAKEHOUSE_CONTRACTS: &[LakehouseTableContract] = &[
    SILVER_INDUSTRIAL_COMPLEXES,
    SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES,
    SILVER_PARCEL_BOUNDARIES,
    SILVER_BUILDING_REGISTER_FLOORS,
    SILVER_BUILDING_REGISTER_UNITS,
    SILVER_BUILDING_REGISTER_UNIT_AREAS,
    SILVER_COMPLEX_PARCEL_MEMBERSHIPS,
    GOLD_COMPLEX_CATALOG,
    GOLD_COMPLEX_SPATIAL_LOCATOR,
];

/// Returns the industrial complex lakehouse `PoC` table contracts in publish order.
#[must_use]
pub const fn industrial_complex_lakehouse_contracts() -> &'static [LakehouseTableContract] {
    INDUSTRIAL_COMPLEX_LAKEHOUSE_CONTRACTS
}

/// Finds an industrial complex lakehouse contract by fully qualified table name.
#[must_use]
pub fn industrial_complex_lakehouse_contract_by_table_name(
    table_name: &str,
) -> Option<&'static LakehouseTableContract> {
    INDUSTRIAL_COMPLEX_LAKEHOUSE_CONTRACTS
        .iter()
        .find(|contract| contract.table_name == table_name)
}
