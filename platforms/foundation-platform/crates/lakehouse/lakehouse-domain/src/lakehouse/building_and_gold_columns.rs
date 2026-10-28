//! Columns of the building register and complex Silver tables and the complex Gold tables. Moved
//! out of `lakehouse.rs` unchanged.

use super::LakehouseColumn;

pub(super) const SILVER_BUILDING_REGISTER_FLOORS_COLUMNS: &[LakehouseColumn] = &[
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

/// Columns of the 표제부 Silver table, measured off the July 2026 national snapshot
/// (77 pipe-delimited fields; the mapping and the area-column disambiguation are in root
/// ADR-0073).
pub(super) const SILVER_BUILDING_REGISTER_TITLES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "title_row_id",
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
        name: "dong_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "main_or_annex_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "main_or_annex_name_raw",
        logical_type: "string",
        required: false,
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
        name: "jibun_address_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "road_address_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "structure_code_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "structure_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "purpose_code_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "purpose_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "purpose_detail_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "roof_code_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "roof_name_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "building_area_m2",
        logical_type: "double",
        required: false,
    },
    LakehouseColumn {
        name: "floor_area_m2",
        logical_type: "double",
        required: false,
    },
    LakehouseColumn {
        name: "ground_floor_count",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "basement_floor_count",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "title_unit_count",
        logical_type: "int",
        required: false,
    },
    LakehouseColumn {
        name: "approval_date_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "approval_year",
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
        name: "source_record_id",
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

pub(super) const SILVER_BUILDING_REGISTER_UNITS_COLUMNS: &[LakehouseColumn] = &[
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
        name: "unit_designation_normalized",
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
        name: "building_link_source_record_id",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "building_link_input_sha256",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "building_link_reason",
        logical_type: "string",
        required: false,
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
        name: "source_record_id",
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

pub(super) const SILVER_BUILDING_REGISTER_UNIT_AREAS_COLUMNS: &[LakehouseColumn] = &[
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
        name: "source_record_id",
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

pub(super) const SILVER_COMPLEX_PARCEL_MEMBERSHIPS_COLUMNS: &[LakehouseColumn] = &[
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

pub(super) const GOLD_COMPLEX_CATALOG_COLUMNS: &[LakehouseColumn] = &[
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
    // One code per site-formation word the source states, named after it (root ADR-0121).
    // Optional because rows loaded before it existed carry none; every new load fills it.
    LakehouseColumn {
        name: "development_stage",
        logical_type: "string",
        required: false,
    },
    // The source's own words for the codes above, trimmed and otherwise verbatim (root ADR-0117
    // §5). The codes are for filtering; these are what a person is shown. Several words can share
    // one code (`준비중` and `보상중` are both `planned`), so the code alone cannot say which.
    LakehouseColumn {
        name: "kind_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "status_raw",
        logical_type: "string",
        required: false,
    },
    // The projection cannot require what the canonical table no longer carries for every row
    // (root ADR-0035).
    LakehouseColumn {
        name: "sido_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "sigungu_code",
        logical_type: "string",
        required: false,
    },
    // The columns from here to `invited_industries_raw` describe the complex itself and reach Gold
    // unchanged from Silver. They are optional here for the same reason they are optional there:
    // the profile source leaves a cell blank rather than stating a value, and a projection may not
    // fill one in.
    //
    // `primary_bjdong_code` is deliberately not among them. Silver declares the column, but zero of
    // its 1,442 rows carry a value — the address resolution reaches sigungu granularity at best
    // (root ADR-0034). Projecting it would add a column no producer fills, which is the shape root
    // ADR-0040 exists to refuse. It reaches Gold when something fills it in Silver, not before.
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
        name: "construction_start_date",
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
    // The remaining ten Silver columns the profile source fills, projected unchanged. Same rule as
    // the block above: the projection carries what Silver carries and fills nothing in.
    LakehouseColumn {
        name: "development_progress_percent",
        logical_type: "decimal(5,2)",
        required: false,
    },
    LakehouseColumn {
        name: "lot_sales_status",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "lot_sales_status_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "business_period_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "business_period_start_month",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "business_period_end_month",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "designation_basis_law_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "development_method_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "development_purpose_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "invited_industries_raw",
        logical_type: "string",
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

pub(super) const GOLD_COMPLEX_SPATIAL_LOCATOR_COLUMNS: &[LakehouseColumn] = &[
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
