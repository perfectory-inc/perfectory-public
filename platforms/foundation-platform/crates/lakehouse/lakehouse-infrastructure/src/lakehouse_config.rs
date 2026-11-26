//! Lakehouse catalog runtime configuration.
//!
//! The application layer depends on Iceberg REST catalog concepts. This module is the infra
//! boundary that maps environment variables to the initial Cloudflare R2 Data Catalog provider.

use std::collections::BTreeMap;

use foundation_shared_kernel::env_source::Requirement;
use thiserror::Error;

const PROVIDER_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER";
const CATALOG_URI_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI";
const WAREHOUSE_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE";
const TOKEN_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN";
/// The read-only catalog token (`foundation-platform-lakehouse-catalog-reader-prod`, root ADR-0247).
const READER_TOKEN_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN";

/// With the `vended` provider, the snapshots a credential vendor resolved for this run (root
/// ADR-0616 A2): a JSON object `{table: {snapshot_id, snapshot_timestamp_ms, manifest_list_location,
/// metadata_location} | null}` that stands for the catalog, so the run holds no catalog token.
const VENDED_TABLES_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_VENDED_TABLES";
/// Default read-only Iceberg table used by live lakehouse smoke tests.
pub const DEFAULT_LAKEHOUSE_SMOKE_TABLE: &str = "silver.industrial_complexes";

/// Supported lakehouse catalog providers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseCatalogProvider {
    /// Cloudflare managed Apache Iceberg REST catalog for R2.
    R2DataCatalog,
    /// Generic Apache Iceberg REST catalog.
    IcebergRest,
    /// The current snapshots a credential vendor resolved for this run (root ADR-0616 A2). No catalog
    /// is reached and no catalog token is held: the run's own tables' current snapshots answer, every
    /// other call is refused. The vendor's tables are read with its read-only credentials, scoped to
    /// those tables' locations.
    Vended,
}

impl LakehouseCatalogProvider {
    /// Parses a stable provider wire value.
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError::UnknownProvider` for unsupported values.
    pub fn from_wire(raw: &str) -> Result<Self, LakehouseCatalogConfigError> {
        match raw {
            "r2_data_catalog" => Ok(Self::R2DataCatalog),
            "iceberg_rest" => Ok(Self::IcebergRest),
            "vended" => Ok(Self::Vended),
            other => Err(LakehouseCatalogConfigError::UnknownProvider(
                other.to_owned(),
            )),
        }
    }

    /// Returns the stable provider wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::R2DataCatalog => "r2_data_catalog",
            Self::IcebergRest => "iceberg_rest",
            Self::Vended => "vended",
        }
    }
}

/// Runtime configuration for the lakehouse catalog adapter.
#[derive(Clone, Eq, PartialEq)]
pub struct LakehouseCatalogConfig {
    /// Catalog provider implementation selected for this environment.
    pub provider: LakehouseCatalogProvider,
    /// Iceberg REST catalog URI.
    pub catalog_uri: String,
    /// Iceberg warehouse name or URI, depending on the provider.
    pub warehouse: String,
    /// Optional bearer token or provider token used by the infra adapter.
    pub catalog_token: Option<String>,
    /// With the `vended` provider, each table's current snapshot as the vendor resolved it (`None`
    /// for a table with no current snapshot); `None` with every other provider.
    pub vended_tables: Option<BTreeMap<String, Option<VendedSnapshot>>>,
}

/// One table's current snapshot as a credential vendor resolved it (root ADR-0616 A2). A `DuckDB`
/// stage reads the table from `metadata_location` (`iceberg_scan`), the hand-written reader from
/// `manifest_list_location`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
pub struct VendedSnapshot {
    /// The table's current snapshot.
    pub snapshot_id: i64,
    /// The snapshot's `timestamp-ms`.
    pub snapshot_timestamp_ms: i64,
    /// Storage location of the snapshot's manifest list.
    pub manifest_list_location: String,
    /// Storage location of the table metadata document the snapshot was read from.
    pub metadata_location: String,
}

impl LakehouseCatalogConfig {
    /// What [`Self::from_env`] needs: the catalog, and the writer token where the catalog asks one.
    pub const WRITER_REQUIREMENTS: &'static [Requirement] = &[
        Requirement::Required(PROVIDER_ENV),
        Requirement::Required(CATALOG_URI_ENV),
        Requirement::Required(WAREHOUSE_ENV),
        Requirement::Optional(TOKEN_ENV),
    ];

    /// The writer token's name.
    pub const WRITER_TOKEN_ENV: &'static str = TOKEN_ENV;

    /// The read-only token's name: what a `DuckDB` read attaches the catalog with (`LakehouseRead`).
    pub const READER_TOKEN_ENV: &'static str = READER_TOKEN_ENV;

    /// What [`Self::reader_from_env`] needs: the same catalog with the read-only token.
    pub const READER_REQUIREMENTS: &'static [Requirement] = &[
        Requirement::Required(PROVIDER_ENV),
        Requirement::Required(CATALOG_URI_ENV),
        Requirement::Required(WAREHOUSE_ENV),
        Requirement::Optional(READER_TOKEN_ENV),
        Requirement::Optional(VENDED_TABLES_ENV),
    ];
    /// With the `vended` provider, the vendor's tables (root ADR-0616 A2).
    pub const VENDED_TABLES_ENV: &'static str = VENDED_TABLES_ENV;

    /// Reads lakehouse catalog settings from process environment variables.
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError` when required values are missing or invalid.
    pub fn from_env() -> Result<Self, LakehouseCatalogConfigError> {
        let vars = std::env::vars().collect::<BTreeMap<_, _>>();
        Self::from_vars(&vars)
    }

    /// Reads lakehouse catalog settings from a provided key-value map.
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError` when required values are missing or invalid.
    pub fn from_vars(vars: &BTreeMap<String, String>) -> Result<Self, LakehouseCatalogConfigError> {
        Self::with_token(vars, TOKEN_ENV)
    }

    /// Reads the catalog settings with the read-only token, for a reader that must never hold a
    /// write credential (the operations map, root ADR-0247). The writer token is not a fallback.
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError` when required values are missing or invalid.
    pub fn reader_from_env() -> Result<Self, LakehouseCatalogConfigError> {
        let vars = std::env::vars().collect::<BTreeMap<_, _>>();
        Self::reader_from_vars(&vars)
    }

    /// [`Self::reader_from_env`] over a provided key-value map.
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError` when required values are missing or invalid.
    pub fn reader_from_vars(
        vars: &BTreeMap<String, String>,
    ) -> Result<Self, LakehouseCatalogConfigError> {
        Self::with_token(vars, READER_TOKEN_ENV)
    }

    fn with_token(
        vars: &BTreeMap<String, String>,
        token_env: &'static str,
    ) -> Result<Self, LakehouseCatalogConfigError> {
        let provider = LakehouseCatalogProvider::from_wire(required_env(vars, PROVIDER_ENV)?)?;
        let catalog_uri = required_env(vars, CATALOG_URI_ENV)?;
        let warehouse = required_env(vars, WAREHOUSE_ENV)?;
        // The vended provider holds no token, whatever the process holds: it never asks the catalog.
        let (catalog_token, vended_tables) = if provider == LakehouseCatalogProvider::Vended {
            let tables =
                serde_json::from_str(required_env(vars, VENDED_TABLES_ENV)?).map_err(|error| {
                    LakehouseCatalogConfigError::InvalidVendedTables(error.to_string())
                })?;
            (None, Some(tables))
        } else {
            (optional_env(vars, token_env), None)
        };

        Ok(Self {
            provider,
            catalog_uri: catalog_uri.to_owned(),
            warehouse: warehouse.to_owned(),
            catalog_token,
            vended_tables,
        })
    }
}

impl std::fmt::Debug for LakehouseCatalogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let token = if self.catalog_token.is_some() {
            "<redacted>"
        } else {
            "<unset>"
        };

        f.debug_struct("LakehouseCatalogConfig")
            .field("provider", &self.provider)
            .field("catalog_uri", &self.catalog_uri)
            .field("warehouse", &self.warehouse)
            .field("catalog_token", &token)
            .field(
                "vended_tables",
                &self.vended_tables.as_ref().map(BTreeMap::len),
            )
            .finish()
    }
}

/// Lakehouse catalog configuration error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LakehouseCatalogConfigError {
    /// A required environment variable is missing or empty.
    #[error("missing required lakehouse catalog environment variable: {0}")]
    MissingEnv(&'static str),

    /// The selected catalog provider is unsupported.
    #[error("unknown lakehouse catalog provider: {0}")]
    UnknownProvider(String),

    /// The vendor's tables are not the JSON object the `vended` provider reads.
    #[error("FOUNDATION_PLATFORM_LAKEHOUSE_VENDED_TABLES is not the vended tables object: {0}")]
    InvalidVendedTables(String),
    /// A live smoke table name is ambiguous or unsafe.
    #[error("invalid lakehouse smoke table name: {0}")]
    InvalidSmokeTableName(String),
}

/// Returns whether live lakehouse smoke tests should touch the configured catalog.
#[must_use]
pub fn live_lakehouse_smoke_enabled(value: Option<&str>) -> bool {
    matches!(value, Some("1"))
}

/// Validates a table name used by the read-only live smoke.
///
/// # Errors
/// Returns `LakehouseCatalogConfigError::InvalidSmokeTableName` for ambiguous path-like values.
pub fn validate_lakehouse_smoke_table_name(
    table_name: &str,
) -> Result<(), LakehouseCatalogConfigError> {
    if table_name.is_empty() {
        return Err(invalid_smoke_table_name("must not be empty"));
    }
    if table_name.trim() != table_name {
        return Err(invalid_smoke_table_name(
            "must not contain leading or trailing whitespace",
        ));
    }
    if table_name.starts_with('/') {
        return Err(invalid_smoke_table_name("must not be an absolute path"));
    }
    if table_name.contains('/') || table_name.contains('\\') || table_name.contains("..") {
        return Err(invalid_smoke_table_name(
            "must not contain path separators or traversal",
        ));
    }
    let parts = table_name.split('.').collect::<Vec<_>>();
    if parts.len() < 2 {
        return Err(invalid_smoke_table_name(
            "must include at least namespace and table",
        ));
    }
    if parts.iter().any(|part| part.is_empty()) {
        return Err(invalid_smoke_table_name(
            "must not contain empty namespace or table segments",
        ));
    }
    if parts
        .iter()
        .any(|part| !part.bytes().all(is_stable_identifier_byte))
    {
        return Err(invalid_smoke_table_name(
            "must use lowercase ASCII letters, digits, or underscores",
        ));
    }
    Ok(())
}

fn invalid_smoke_table_name(reason: &str) -> LakehouseCatalogConfigError {
    LakehouseCatalogConfigError::InvalidSmokeTableName(reason.to_owned())
}

const fn is_stable_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
}

fn required_env<'a>(
    vars: &'a BTreeMap<String, String>,
    key: &'static str,
) -> Result<&'a str, LakehouseCatalogConfigError> {
    vars.get(key)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(LakehouseCatalogConfigError::MissingEnv(key))
}

fn optional_env(vars: &BTreeMap<String, String>, key: &'static str) -> Option<String> {
    vars.get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}
