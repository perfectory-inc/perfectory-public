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
    /// Machine-readable predicate selecting the current logical row, when the table is versioned.
    pub current_row_predicate: Option<&'static str>,
    /// Stable column contract.
    pub columns: &'static [LakehouseColumn],
    /// Iceberg partition spec expressed as stable contract text.
    pub partition_spec: &'static [&'static str],
    /// Sort order expressed as stable contract text.
    pub sort_order: &'static [&'static str],
    /// Quality gates that must pass before publish/promote.
    pub quality_gates: &'static [&'static str],
    /// What one load of this table carries, and how the re-run guard reads it.
    pub load: LakehouseLoadUnit,
}

/// What one load of a table carries.
///
/// The re-run guard compares the identities a batch carries against the ones the table records.
/// It assumed every table identified its loads the same way, and on 2026-09-01 the six live
/// tables between them used three kinds: an object key, a collection run, and — for a derived
/// table — nothing at all. Read as one kind, five of the six recorded no identity the guard could
/// use and would have been appended a second time (root ADR-0069).
///
/// Declared rather than inferred from the values. A table loaded once looks like every kind at
/// once, so inference would answer from whatever happens to be in the table today.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseLoadUnit {
    /// One collected object per identity.
    Object {
        /// Column holding the identity.
        column: &'static str,
        /// Text before the object key, when the value wraps it. `silver.industrial_complexes`
        /// writes `foundation-platform:bronze:{key}#{code}`, so reading the value whole would
        /// report 1,442 objects for one archive.
        object_prefix: Option<&'static str>,
        /// Separator after the object key, when the value carries more than the key.
        object_suffix_separator: Option<&'static str>,
    },
    /// One collection execution per identity — coarser than an object, and still an identity.
    Run {
        /// Column holding the run identity.
        column: &'static str,
    },
    /// Derived from other tables, with no collected source. The producer replaces rather than
    /// appends, so there is nothing for the guard to compare and its absence is not a gap.
    Derived,
}

impl LakehouseLoadUnit {
    /// The column the guard reads, or `None` for a derived table.
    #[must_use]
    pub const fn column(&self) -> Option<&'static str> {
        match self {
            Self::Object { column, .. } | Self::Run { column } => Some(column),
            Self::Derived => None,
        }
    }

    /// The name this unit is written as in the contract artifact.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Object { .. } => "object",
            Self::Run { .. } => "run",
            Self::Derived => "derived",
        }
    }
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
    // Region is optional for the whole administrative triple: the owner deferred per-region
    // industrial-complex work, and requiring a code no source states would only be satisfiable by
    // inventing one. Absent stays `null`, never `""` or a zero code (root ADR-0035).
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
    // Designation, ground-breaking, completion — the three dates in the order they happen.
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
    // What the profile source says about the complex beyond its identity and its dates. Every one
    // of these is optional for the same reason the columns above are: the provider leaves a cell
    // blank rather than stating a value, and one column of its twenty (`rent_hsmp_se_code`) is
    // blank in all 1,442 rows — a provider that empties a whole column empties single cells too.
    // Requiring any of them would mean one blank cell rejects the entire snapshot.
    //
    // `frst_regist_de` is deliberately absent. See root ADR-0044 and the header block in
    // `services/foundation-outbox-publisher/src/industrial_complex_bronze_raw_jsonl_export/\
    // profile_workbook_decoder.rs`.
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
    // The business period as the source wrote it, plus the two months a parse could recover. The
    // raw text is the contract: 1,440 of 1,441 values are `YYYY-MM~YYYY-MM` and one is `2020-~2024-`
    // with no months at all, so a shape that only held the parse would drop that row's fact
    // entirely. The two derived columns are null together whenever the parse does not apply.
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
    // Four free-text columns, carried verbatim. `devlop_mth` has 232 distinct values and
    // `appn_basis_law` 48, and the distinctness is spelling rather than meaning — `공영개발`,
    // `공영개발방식`, and `공영개발 방식` are three of them. Mapping those onto an enumeration would
    // invent a classification nobody published, so the `_raw` suffix is the contract: this column
    // holds what the source wrote and normalization is a separate, evidenced decision.
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

// D155 필지별 토지이용계획 속성 CSV 의 열 순서 그대로다 (root ADR-0083). *_name 열은
// 원천이 준 한국어 표기를 손대지 않고 나른다 — 어휘 번역은 소비자(공짱)의 결정이다.
const SILVER_LAND_USE_PLAN_COLUMNS: &[LakehouseColumn] = &[
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
        name: "drawing_number",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "inclusion_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "inclusion_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "zone_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "zone_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "registered_date",
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
        name: "remark",
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

// D151 필지별 개별공시지가 CSV 의 열 순서 그대로 (root ADR-0085). 값은 원천 표기
// 그대로 문자열로 나른다 — 형 변환은 소비 투영의 몫이고, 원천이 준 것을 바꾸지 않는다.
const SILVER_LAND_INDIVIDUAL_PRICE_COLUMNS: &[LakehouseColumn] = &[
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
        name: "special_land_kind_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "special_land_kind_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "jibun",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "base_year",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "base_month",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "price_per_m2",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "announced_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "standard_parcel_flag",
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

// LMIS 용도지역지구 코드표 (LART_LMISZONE.csv) 열 순서 그대로 (root ADR-0083).
// `parent_ucode` 는 판정 투영이 걸어 올라가는 트리의 간선이라 여기 있어야 하고,
// 원천에 빈 부모(UQA500 등)가 실재하므로 필수로 만들 수 없다.
const SILVER_LAND_USE_ZONE_CODES_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "ucode",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "uname",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "division_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "law_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "area_kind",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "law_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "annex_flag",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "enforcement_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "article_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "article_sub_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "record_seqno",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "parent_ucode",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "deleted_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "deleted_text",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "terms_no",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "first_registered_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "last_updated_date",
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

/// Columns of the 표제부 Silver table, measured off the July 2026 national snapshot
/// (77 pipe-delimited fields; the mapping and the area-column disambiguation are in root
/// ADR-0073).
const SILVER_BUILDING_REGISTER_TITLES_COLUMNS: &[LakehouseColumn] = &[
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
    current_row_predicate: None,
    columns: SILVER_INDUSTRIAL_COMPLEXES_COLUMNS,
    // The snapshot, not the province. `sido_code` cannot partition a table that admits a row
    // without one, and this table is loaded, re-read, and superseded one monthly snapshot at a
    // time — `source_snapshot_id` is required, single-valued per load, and already the predicate
    // the Iceberg read-back filters on. The `bucket(32, complex_id)` fan-out went with it: 1,442
    // rows per snapshot is 45 rows a bucket, which is file overhead rather than pruning
    // (root ADR-0035).
    partition_spec: &["source_snapshot_id"],
    sort_order: &["complex_name_normalized", "official_complex_code"],
    quality_gates: &[
        "(official_complex_code, source_snapshot_id) unique",
        "complex_name non-empty",
        "complex_kind is a supported domain wire value",
        "official_area_sqm > 0 when present",
        "lot_sales_status is a supported domain wire value when present",
        "development_progress_percent is between 0 and 100 when present",
        "business_period_start_month and business_period_end_month are present together",
        "business_period months are yyyy-MM",
        "active rows for the same complex_id do not overlap",
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
    columns: SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES_COLUMNS,
    // No partitions. 1,343 rows in 8 MB were split across 371 partitions, one file each, twenty
    // kilobytes apiece — a layout compaction cannot repair, because it may only merge within a
    // partition and every partition already held one file. Databricks puts the threshold for
    // partitioning at a terabyte and calls a partition under a gigabyte over-partitioned; this
    // table is four orders of magnitude below the first number. The sort order below carries what
    // pruning there is to carry (root ADR-0066).
    partition_spec: &[],
    sort_order: &["complex_id", "boundary_kind", "valid_from_utc"],
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
    columns: SILVER_LAND_USE_PLAN_COLUMNS,
    // 무분할: 필지 경계 40M 이 무분할로 파일 15개였다(root ADR-0066 실측 방식). 2.6억이라도
    // pnu 정렬이 파일 min/max 로 범위 읽기를 건너뛰게 한다. 시도 분할은 열이 접두사(pnu 앞
    // 두 자리)로 이미 존재한다.
    partition_spec: &[],
    sort_order: &["pnu", "zone_code"],
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
    columns: SILVER_LAND_INDIVIDUAL_PRICE_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "base_year", "base_month"],
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
    columns: SILVER_LAND_USE_ZONE_CODES_COLUMNS,
    partition_spec: &[],
    sort_order: &["ucode"],
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
    columns: SILVER_BUILDING_REGISTER_FLOORS_COLUMNS,
    partition_spec: &["bucket(16, mgm_bldrgst_pk)"],
    sort_order: &["mgm_bldrgst_pk", "floor_index", "floor_row_id"],
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
    columns: SILVER_BUILDING_REGISTER_TITLES_COLUMNS,
    // No partitions: 8,051,204 rows is a fraction of the unpartitioned units table, and the
    // sort order alone gives the file-level min/max a PNU-range read skips on (root ADR-0066).
    partition_spec: &[],
    sort_order: &["pnu", "mgm_bldrgst_pk"],
    quality_gates: &[
        "title_row_id_not_null",
        "register_parcel_key_not_null",
        "normalization_status_in_allowed_values",
        "main_or_annex_kind_in_allowed_values",
        "row_checksum_sha256_valid",
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
    quality_gates: &[
        "unit_row_id_not_null",
        "register_parcel_key_not_null",
        "normalization_status_in_allowed_values",
        "proposal_required_rows_preserved",
        "building_link_method_in_allowed_values",
        "row_checksum_sha256_valid",
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
    columns: SILVER_COMPLEX_PARCEL_MEMBERSHIPS_COLUMNS,
    partition_spec: &["sigungu_code", "bucket(256, pnu)"],
    sort_order: &["complex_id", "pnu", "membership_kind"],
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
    columns: GOLD_COMPLEX_CATALOG_COLUMNS,
    // Follows the canonical table it projects: the snapshot the rows were published from
    // (root ADR-0035).
    partition_spec: &["source_snapshot_id"],
    sort_order: &["name", "complex_id"],
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
    columns: GOLD_COMPLEX_SPATIAL_LOCATOR_COLUMNS,
    partition_spec: &["spatial_key_prefix"],
    sort_order: &["spatial_key", "complex_id"],
    quality_gates: &[
        "spatial_key is stable",
        "bbox min/max ordering is valid",
        "object_key points to a source GeoParquet artifact",
        "iceberg_snapshot_id is present",
    ],
    // 파생 표이고 계보 칸 자체가 없다.
    load: LakehouseLoadUnit::Derived,
};

const INDUSTRIAL_COMPLEX_LAKEHOUSE_CONTRACTS: &[LakehouseTableContract] = &[
    SILVER_INDUSTRIAL_COMPLEXES,
    SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES,
    SILVER_PARCEL_BOUNDARIES,
    SILVER_LAND_USE_PLAN,
    SILVER_LAND_USE_ZONE_CODES,
    SILVER_LAND_INDIVIDUAL_PRICE,
    SILVER_BUILDING_REGISTER_FLOORS,
    SILVER_BUILDING_REGISTER_TITLES,
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
