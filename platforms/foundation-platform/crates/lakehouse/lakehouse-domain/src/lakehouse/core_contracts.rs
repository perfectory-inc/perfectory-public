//! The industrial complex, parcel, land and building register table contracts. Moved out of
//! `lakehouse.rs` unchanged.

use super::*;

/// Canonical Silver table for industrial complex facts.
pub const SILVER_INDUSTRIAL_COMPLEXES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.industrial_complexes",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: Some("valid_to_utc IS NULL"),
    // Every load appends a whole version and leaves the earlier ones (append-only); a re-load of
    // the same source snapshot is a newer version of the same row, not a second complex
    // (root ADR-0267).
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["official_complex_code"],
        newest_first: &["valid_from_utc", "ingested_at_utc"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_INDUSTRIAL_COMPLEXES_COLUMNS,
    // The snapshot, not the province. `sido_code` cannot partition a table that admits a row
    // without one, and this table is loaded, re-read, and superseded one monthly snapshot at a
    // time — `source_snapshot_id` is required, single-valued per load, and already the predicate
    // the Iceberg read-back filters on. The `bucket(32, complex_id)` fan-out went with it: 1,442
    // rows per snapshot is 45 rows a bucket, which is file overhead rather than pruning
    // (root ADR-0035).
    partition_spec: &["source_snapshot_id"],
    sort_order: &["complex_name_normalized", "official_complex_code"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "(official_complex_code, source_snapshot_id, ingested_at_utc) unique",
        "complex_name non-empty",
        "complex_kind is a supported domain wire value",
        "official_area_sqm > 0 when present",
        "lot_sales_status is a supported domain wire value when present",
        "development_progress_percent is between 0 and 100 when present",
        "business_period_start_month and business_period_end_month are present together",
        "business_period months are yyyy-MM",
        "current_row_latest leaves one current row per official_complex_code",
    ],
    // 2026-09-01 실측: 1,442개 값이 한 객체에서 나왔다. 값은 접두사 + 객체키 + `#` + 단지코드다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: Some("foundation-platform:bronze:"),
        object_suffix_separator: Some("#"),
    },
};

/// Canonical Silver `GeoParquet` table for industrial complex boundaries.
pub const SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.industrial_complex_boundaries",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::GeoParquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES_COLUMNS,
    // No partitions. 1,343 rows in 8 MB were split across 371 partitions, one file each, twenty
    // kilobytes apiece — a layout compaction cannot repair, because it may only merge within a
    // partition and every partition already held one file. Databricks puts the threshold for
    // partitioning at a terabyte and calls a partition under a gigabyte over-partitioned; this
    // table is four orders of magnitude below the first number. The sort order below carries what
    // pruning there is to carry (root ADR-0066).
    partition_spec: &[],
    sort_order: &["complex_id", "boundary_kind", "valid_from_utc"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "geometry_srid = 5186",
        "bbox min/max ordering is valid",
        "centroid is inside bbox",
        "geometry_wkb is valid polygon or multipolygon",
        "active official boundary is at most one per complex_id",
        "geometry_checksum_sha256 is 64 lowercase hex",
    ],
    // 2026-09-01 실측: 값 1개, 이미 객체키 그대로다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// Canonical Silver `GeoParquet` table for cadastral parcel boundaries.
pub const SILVER_PARCEL_BOUNDARIES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.parcel_boundaries",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::GeoParquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: Some("valid_to_utc IS NULL"),
    current_row_latest: None,
    columns: SILVER_PARCEL_BOUNDARIES_COLUMNS,
    // No partitions. Root ADR-0063 removed the `pnu` bucket from beside `sigungu_code` and left
    // sigungu in place; measuring afterwards showed sigungu is not earning its keep either. The
    // table is 7.44 GB — fifteen target files — and every consumer reads it whole: the PostGIS
    // mirror and the tile artifacts. A scan that skips nothing gains nothing from partitions,
    // and the 257 partitions capped file size at 29 MB against a 512 MB target while giving two
    // stray district codes a one-row file each (root ADR-0066).
    //
    //   sigungu + bucket    65,280 partitions   43,649 files   0.28 MB each
    //   sigungu only           257 partitions      257 files     29.0 MB each
    //   unpartitioned                          about 15 files    500 MB each
    partition_spec: &[],
    sort_order: &["pnu", "valid_from_utc"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "pnu passes shared PNU validation",
        "geometry_srid = 4326",
        "bbox min/max ordering is valid",
        "geometry_wkb is valid polygon or multipolygon",
        "one active parcel boundary per pnu",
        "geometry_checksum_sha256 is 64 lowercase hex",
    ],
    // 2026-09-01 실측: 값 255개. 짧은 이름으로 실려 있어 root ADR-0068 의 이관 대상이다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// Canonical Silver table for per-parcel land-use plan designations (root ADR-0083).
///
/// One row is one (필지, 용도지역지구, 저촉여부) designation from the D155 attribute CSV.
/// 접함(3) rows are preserved here and excluded only by the zoning projection — the source
/// is carried whole, the judgment happens downstream.
pub const SILVER_LAND_USE_PLAN: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_use_plan",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_LAND_USE_PLAN_COLUMNS,
    // 무분할: 필지 경계 40M 이 무분할로 파일 15개였다(root ADR-0066 실측 방식). 2.6억이라도
    // pnu 정렬이 파일 min/max 로 범위 읽기를 건너뛰게 한다. 시도 분할은 열이 접두사(pnu 앞
    // 두 자리)로 이미 존재한다.
    partition_spec: &[],
    sort_order: &["pnu", "zone_code"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "pnu_not_null",
        "zone_code_not_null",
        "inclusion_code_not_null",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// Canonical Silver table for per-parcel official land price assessments (root ADR-0085).
pub const SILVER_LAND_INDIVIDUAL_PRICE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_individual_price",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_LAND_INDIVIDUAL_PRICE_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "base_year", "base_month"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "pnu_not_null",
        "base_year_not_null",
        "price_per_m2_not_null",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// Canonical Silver table for the LMIS land-use zone code tree (root ADR-0083).
///
/// 1,270 rows measured 2026-09-05. `parent_ucode` edges are what the zoning projection walks
/// to reach the anchor set — the mapping is the tree, not a hand-written list.
pub const SILVER_LAND_USE_ZONE_CODES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.land_use_zone_code",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_LAND_USE_ZONE_CODES_COLUMNS,
    partition_spec: &[],
    sort_order: &["ucode"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &["ucode_not_null", "uname_not_null"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// Canonical Silver table for official building-register floor rows.
pub const SILVER_BUILDING_REGISTER_FLOORS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_floors",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_BUILDING_REGISTER_FLOORS_COLUMNS,
    partition_spec: &["bucket(16, mgm_bldrgst_pk)"],
    sort_order: &["mgm_bldrgst_pk", "floor_index", "floor_row_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "floor_row_id_not_null",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "row_checksum_sha256_valid",
    ],
    // 2026-09-01 실측: 표가 아직 없어 실물로 확인하지 못했다. 형제 두 표와 같은 생산자를 쓴다.
    load: LakehouseLoadUnit::Run {
        column: "source_snapshot_id",
    },
};

/// Quality gate shared by the hub-register Silver tables keyed by `register_parcel_key`.
///
/// A load is refused when the PNU NULL share among ordinary-land rows (대지구분 `0`, which always
/// has a standard PNU) exceeds the ceiling, or rises more than the increase bound above the
/// table's current snapshot; the Spark loader reads both numbers from here
/// (`pnu_null_share_guard.py`). The 2026-09-27 title snapshot lost all of 시도 12 and jumped from
/// one in a million to 0.116. The rise alone would ratchet onto that snapshot while it is current,
/// so the ceiling holds on its own. The corrected title reload, with the hub placeholder codes
/// composing NULL, measures 967 of 7,939,750 (1.2e-4), four times under 0.0005; the smallest
/// 12xxx 시군구 alone is about 0.2% of the title table's ordinary-land rows, four times over it
/// (ADR-0142).
pub const ORDINARY_LAND_PNU_NULL_SHARE_GATE: &str =
    "ordinary_land_pnu_null_share <= 0.0005 and increase <= 0.001";

/// Canonical Silver table for official building-register title (표제부) rows.
///
/// One row per building (동), main and annex alike: the annex rows are real buildings with
/// their own register PKs. This is the table `catalog.building` projects from (root ADR-0073);
/// the units and areas tables join it on the provider's shared `mgm_bldrgst_pk`.
pub const SILVER_BUILDING_REGISTER_TITLES: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_titles",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_BUILDING_REGISTER_TITLES_COLUMNS,
    // No partitions: 8,051,204 rows is a fraction of the unpartitioned units table, and the
    // sort order alone gives the file-level min/max a PNU-range read skips on (root ADR-0066).
    partition_spec: &[],
    sort_order: &["pnu", "mgm_bldrgst_pk"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "title_row_id_not_null",
        "register_parcel_key_not_null",
        "normalization_status_in_allowed_values",
        "main_or_annex_kind_in_allowed_values",
        "row_checksum_sha256_valid",
        ORDINARY_LAND_PNU_NULL_SHARE_GATE,
    ],
    // 표제부도 한 실행이 전국 스냅숏 하나를 통째로 낳는다 — 층·호·면적과 같은 단위다.
    load: LakehouseLoadUnit::Run {
        column: "source_snapshot_id",
    },
};

/// Canonical Silver table for official building-register unit rows.
pub const SILVER_BUILDING_REGISTER_UNITS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.building_register_units",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_BUILDING_REGISTER_UNITS_COLUMNS,
    // No partitions. 1.17 GB fits in three target files, and bucketing on `pnu` fought the
    // sort order below: the bucket scatters neighbouring PNUs across 256 hash buckets while
    // the sort gathers them, so a PNU-range read had to open every bucket. Without it the
    // sort order alone gives file-level min/max that a range read can skip on
    // (root ADR-0066).
    partition_spec: &[],
    sort_order: &[
        "pnu",
        "building_mgm_bldrgst_pk",
        "floor_index",
        "unit_number",
        "unit_row_id",
    ],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "building_link_evidence_valid",
        "unit_row_id_not_null",
        "register_parcel_key_not_null",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "building_link_method_in_allowed_values",
        "row_checksum_sha256_valid",
        ORDINARY_LAND_PNU_NULL_SHARE_GATE,
    ],
    // 2026-09-01 실측: 값 1개, 파이프라인 실행 이름. 19,765,555 행이 한 실행에서 나왔다.
    load: LakehouseLoadUnit::Run {
        column: "source_snapshot_id",
    },
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
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_BUILDING_REGISTER_UNIT_AREAS_COLUMNS,
    partition_spec: &["bucket(32, mgm_bldrgst_pk)"],
    sort_order: &["mgm_bldrgst_pk", "area_kind", "floor_index", "area_row_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "area_row_id_not_null",
        "register_parcel_key_not_null",
        "area_kind_in_allowed_values",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "row_checksum_sha256_valid",
        ORDINARY_LAND_PNU_NULL_SHARE_GATE,
    ],
    // 2026-09-01 실측: 값 1개, 파이프라인 실행 이름. 113,813,264 행이 한 실행에서 나왔다.
    load: LakehouseLoadUnit::Run {
        column: "source_snapshot_id",
    },
};

/// Canonical Silver table for industrial complex to parcel membership.
pub const SILVER_COMPLEX_PARCEL_MEMBERSHIPS: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.complex_parcel_memberships",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_COMPLEX_PARCEL_MEMBERSHIPS_COLUMNS,
    partition_spec: &["sigungu_code", "bucket(256, pnu)"],
    sort_order: &["complex_id", "pnu", "membership_kind"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "pnu passes shared PNU validation",
        "one active inside or intersects membership per complex_id and pnu",
        "overlap_ratio is between 0 and 1 when present",
        "excluded rows include source method and lineage",
    ],
    // 2026-09-01 실측: 표가 아직 없어 실물로 확인하지 못했다. 계약이 source_record_id 를 요구한다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

/// Gold projection for API list/detail and consumer read models.
pub const GOLD_COMPLEX_CATALOG: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.complex_catalog",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_COMPLEX_CATALOG_COLUMNS,
    // Follows the canonical table it projects: the snapshot the rows were published from
    // (root ADR-0035).
    partition_spec: &["source_snapshot_id"],
    sort_order: &["name", "complex_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "one active row per complex_id",
        "parcel_count is non-negative",
        "iceberg_snapshot_id is present",
        "published_at_utc is present",
    ],
    // silver.industrial_complexes 에서 파생. 생산자가 overwrite 로 돌아 덮어쓴다.
    load: LakehouseLoadUnit::Derived,
};

/// Gold spatial locator for bbox, tile, or H3 based pruning.
pub const GOLD_COMPLEX_SPATIAL_LOCATOR: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.complex_spatial_locator",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::SpatialLocator,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_COMPLEX_SPATIAL_LOCATOR_COLUMNS,
    partition_spec: &["spatial_key_prefix"],
    sort_order: &["spatial_key", "complex_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "spatial_key is stable",
        "bbox min/max ordering is valid",
        "object_key points to a source GeoParquet artifact",
        "iceberg_snapshot_id is present",
    ],
    // 파생 표이고 계보 칸 자체가 없다.
    load: LakehouseLoadUnit::Derived,
};
