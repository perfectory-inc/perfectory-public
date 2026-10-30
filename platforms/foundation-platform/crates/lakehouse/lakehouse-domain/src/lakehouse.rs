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
    /// Externally issued registry or crosswalk table that other layers join against.
    Reference,
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

/// The current row of a key in an append-only table: the newest by `newest_first`.
///
/// A tie on every `newest_first` column is ambiguous, and readers refuse it rather than pick,
/// unless `keep_ties` says a key's version is a set of rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LakehouseCurrentRowLatest {
    /// Columns naming one logical row.
    pub key: &'static [&'static str],
    /// Columns ordering a key's versions, newest first, each compared descending.
    pub newest_first: &'static [&'static str],
    /// The column whose value `removed` marks the key as gone from its source: a key whose current
    /// row is such a marker has no current row (root ADR-0275). `None` when the table never removes.
    pub removal_column: Option<&'static str>,
    /// Whether a key's newest version may be several rows: every row tying on `newest_first` is
    /// current, none is picked. For a source that repeats a key within one release (root ADR-0275).
    pub keep_ties: bool,
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
    /// For an append-only table that keeps every version, which surviving row is current: the
    /// newest per key. Applied after `current_row_predicate`; readers take both through one helper
    /// (`platform_contracts.current_rows`), never by hand (root ADR-0267).
    pub current_row_latest: Option<LakehouseCurrentRowLatest>,
    /// Stable column contract.
    pub columns: &'static [LakehouseColumn],
    /// Iceberg partition spec expressed as stable contract text.
    pub partition_spec: &'static [&'static str],
    /// Sort order expressed as stable contract text.
    pub sort_order: &'static [&'static str],
    /// How a write spreads the rows over data files (root ADR-0164).
    pub write_distribution: LakehouseWriteDistribution,
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

/// How a write spreads a table's rows over its data files.
///
/// A reader can skip a data file only when the file's column bounds exclude what it looks for. Rows
/// spread by hash land in files whose bounds each span nearly the whole key range, so nothing can
/// be skipped: on 2026-10-08 every one of 126 by-PNU bake shards read all 33 data files of
/// `gold.parcel_panel` to keep one PNU prefix of them, and the bake took about seven hours. Range
/// distribution gives each file one contiguous slice of the sort order instead (root ADR-0164).
///
/// Declared rather than implied by `sort_order`: most tables list a sort order their loads apply
/// within a task, and turning every one of them into a table-wide range sort would change loads
/// nobody measured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseWriteDistribution {
    /// Rows are clustered by partition (Iceberg `write.distribution-mode=hash`). Files of one
    /// partition overlap in every other column.
    Hash,
    /// The table's Iceberg sort order is the contract's `sort_order` and rows are range-partitioned
    /// by it (`ALTER TABLE ... WRITE ORDERED BY`, which sets `write.distribution-mode=range`). Each
    /// data file then holds one contiguous slice of the sort order, so the bounds Iceberg records
    /// for the leading sort column tell a reader which files can hold a key.
    Range,
}

impl LakehouseWriteDistribution {
    /// The Iceberg `write.distribution-mode` value, which is also how the contract artifact
    /// writes it.
    #[must_use]
    pub const fn mode(&self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Range => "range",
        }
    }
}

mod building_and_gold_columns;
mod core_contracts;
mod core_silver_columns;
mod legal_dong_and_place_id;
mod map_edit_and_boundaries;

pub use core_contracts::*;
pub use core_silver_columns::*;
pub use legal_dong_and_place_id::*;
pub use map_edit_and_boundaries::*;

const INDUSTRIAL_COMPLEX_LAKEHOUSE_CONTRACTS: &[LakehouseTableContract] = &[
    crate::SILVER_BUILDING_REGISTER_APARTMENT_PRICE,
    crate::SILVER_BUILDING_REGISTER_EXCLUSIVE_UNIT,
    crate::SILVER_BUILDING_PERMIT_BASIS_OUTLINE,
    crate::SILVER_BUILDING_REGISTER_MASTER,
    crate::SILVER_UNIT_OFFICIAL_PRICE,
    crate::SILVER_BUILDING_REGISTER_APARTMENT_PRICE_VERSIONS,
    crate::SILVER_UNIT_OFFICIAL_PRICE_VERSIONS,
    SILVER_INDUSTRIAL_COMPLEXES,
    SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES,
    SILVER_ADMINISTRATIVE_BOUNDARIES,
    SILVER_PARCEL_BOUNDARIES,
    SILVER_LAND_USE_PLAN,
    SILVER_LAND_USE_ZONE_CODES,
    SILVER_LAND_INDIVIDUAL_PRICE,
    SILVER_LAND_CHARACTERISTIC,
    SILVER_LAND_FOREST_LEDGER,
    SILVER_LAND_TRANSFER_HISTORY,
    SILVER_LAND_RIGHT_REGISTRATION,
    SILVER_BUILDING_REGISTER_FLOORS,
    SILVER_BUILDING_REGISTER_TITLES,
    SILVER_BUILDING_REGISTER_UNITS,
    SILVER_BUILDING_REGISTER_UNIT_AREAS,
    SILVER_COMPLEX_PARCEL_MEMBERSHIPS,
    SILVER_MAP_EDIT_LEDGER,
    GOLD_COMPLEX_CATALOG,
    GOLD_COMPLEX_SPATIAL_LOCATOR,
    GOLD_PARCEL_PANEL,
    GOLD_BUILDING_PANEL,
    crate::GOLD_UNIT_PRICE_PANEL,
    GOLD_INDUSTRIAL_COMPLEX_BOUNDARY_SERVED,
    GOLD_ADMINISTRATIVE_BOUNDARY_SERVED,
    GOLD_PARCEL_BOUNDARY_SERVED,
    REFERENCE_LEGAL_DONG_CODE_SNAPSHOT,
    REFERENCE_LEGAL_DONG_CODE_CHANGE,
    SILVER_PARCEL_NUMBER_CHANGE_HISTORY,
    SILVER_ONBID_REAL_ESTATE_ITEM,
    SILVER_ONBID_ITEM_BID_RESULT,
    SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO,
    SILVER_ONBID_ANNOUNCEMENT,
    SILVER_ONBID_ANNOUNCEMENT_BID_RESULT,
    SILVER_ONBID_ANNOUNCEMENT_DETAIL,
    SILVER_ONBID_REAL_ESTATE_ITEM_DETAIL,
    SILVER_ONBID_ITEM_BID_INFO,
    SILVER_REGISTERED_FACTORY,
    SILVER_REAL_TRANSACTION,
    SILVER_REAL_TRANSACTION_REJECTED,
    SILVER_COURT_AUCTION_ITEM,
    SILVER_COURT_AUCTION_SALE_RESULT,
    SILVER_PARCEL_LINEAGE,
    SILVER_PARCEL_REGISTRY,
    GOLD_PLACE_ID_REGISTRY,
    GOLD_PLACE_ID_BRIDGE,
    GOLD_PLACE_ID_CHANGELOG,
    GOLD_LINEAGE_REVIEW_QUEUE,
];

/// Returns the industrial complex lakehouse `PoC` table contracts in publish order, then the Gold
/// table of every by-PNU data kind the R2 connection contract names (root ADR-0193).
#[must_use]
pub fn industrial_complex_lakehouse_contracts() -> &'static [LakehouseTableContract] {
    static ALL: std::sync::OnceLock<Vec<LakehouseTableContract>> = std::sync::OnceLock::new();
    ALL.get_or_init(|| {
        [
            INDUSTRIAL_COMPLEX_LAKEHOUSE_CONTRACTS,
            crate::by_pnu_kind_tables(),
        ]
        .concat()
    })
}

/// Finds an industrial complex lakehouse contract by fully qualified table name.
#[must_use]
pub fn industrial_complex_lakehouse_contract_by_table_name(
    table_name: &str,
) -> Option<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table_name)
}
