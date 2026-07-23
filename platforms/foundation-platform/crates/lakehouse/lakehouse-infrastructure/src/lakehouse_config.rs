//! Lakehouse catalog runtime configuration.
//!
//! The application layer depends on Iceberg REST catalog concepts. This module is the infra
//! boundary that maps environment variables to the initial Cloudflare R2 Data Catalog provider.

use std::collections::BTreeMap;

use thiserror::Error;

const PROVIDER_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER";
const CATALOG_URI_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI";
const WAREHOUSE_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE";
const TOKEN_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN";

/// Default read-only Iceberg table used by live lakehouse smoke tests.
pub const DEFAULT_LAKEHOUSE_SMOKE_TABLE: &str = "silver.industrial_complexes";

/// Supported lakehouse catalog providers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakehouseCatalogProvider {
    /// Cloudflare managed Apache Iceberg REST catalog for R2.
    R2DataCatalog,
    /// Generic Apache Iceberg REST catalog.
    IcebergRest,
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
}

impl LakehouseCatalogConfig {
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
        let provider = required_env(vars, PROVIDER_ENV)?;
        let catalog_uri = required_env(vars, CATALOG_URI_ENV)?;
        let warehouse = required_env(vars, WAREHOUSE_ENV)?;
        let catalog_token = optional_env(vars, TOKEN_ENV);

        Ok(Self {
            provider: LakehouseCatalogProvider::from_wire(provider)?,
            catalog_uri: catalog_uri.to_owned(),
            warehouse: warehouse.to_owned(),
            catalog_token,
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
