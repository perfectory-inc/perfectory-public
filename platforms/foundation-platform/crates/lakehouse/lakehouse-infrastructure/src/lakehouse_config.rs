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
    ];

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
        let provider = required_env(vars, PROVIDER_ENV)?;
        let catalog_uri = required_env(vars, CATALOG_URI_ENV)?;
        let warehouse = required_env(vars, WAREHOUSE_ENV)?;
        let catalog_token = optional_env(vars, token_env);

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

const CLIENT_ID_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_ID";
const CLIENT_SECRET_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_SECRET";
const CLIENT_TOKEN_URI_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_TOKEN_URI";
const CLIENT_SCOPE_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_SCOPE";
const CLIENT_ENVS: [&str; 4] = [
    CLIENT_ID_ENV,
    CLIENT_SECRET_ENV,
    CLIENT_TOKEN_URI_ENV,
    CLIENT_SCOPE_ENV,
];

/// The lane's machine identity at the Lakekeeper catalog (root ADR-0624 §2): an OAuth2 client of the
/// client-credentials flow, whose secret only that lane's role reads. A run that holds one is in
/// Lakekeeper mode: the catalog is opened through iceberg-catalog-rest with it, table files are read
/// and written with the credentials the catalog vends on load, and no catalog token or static R2 key
/// may stand beside it (ADR-0624 P6, P7).
#[derive(Clone, Eq, PartialEq)]
pub struct LakehouseCatalogClient {
    /// The OAuth2 client id (an identifier, not a secret).
    pub client_id: String,
    /// The client's secret.
    pub client_secret: String,
    /// The issuer's token endpoint.
    pub token_uri: String,
    /// The scope the catalog admits (`<resource server>/catalog`).
    pub scope: String,
}

impl LakehouseCatalogClient {
    /// What [`Self::from_env`] reads: all four or none.
    pub const REQUIREMENTS: &'static [Requirement] = &[
        Requirement::Optional(CLIENT_ID_ENV),
        Requirement::Optional(CLIENT_SECRET_ENV),
        Requirement::Optional(CLIENT_TOKEN_URI_ENV),
        Requirement::Optional(CLIENT_SCOPE_ENV),
    ];

    /// The client secret's variable, for a task definition that injects it from its secret store.
    pub const SECRET_ENV: &'static str = CLIENT_SECRET_ENV;

    /// Reads the lane's catalog client from the process environment; `None` when the run holds
    /// none (a run on the current catalog).
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError::PartialClient` when some of the four are set and not all.
    pub fn from_env() -> Result<Option<Self>, LakehouseCatalogConfigError> {
        let vars = std::env::vars().collect::<BTreeMap<_, _>>();
        Self::from_vars(&vars)
    }

    /// [`Self::from_env`] over a provided key-value map.
    ///
    /// # Errors
    /// Returns `LakehouseCatalogConfigError::PartialClient` when some of the four are set and not all.
    pub fn from_vars(
        vars: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, LakehouseCatalogConfigError> {
        let set: Vec<&'static str> = CLIENT_ENVS
            .into_iter()
            .filter(|key| optional_env(vars, key).is_some())
            .collect();
        if set.is_empty() {
            return Ok(None);
        }
        if set.len() != CLIENT_ENVS.len() {
            let missing = CLIENT_ENVS
                .into_iter()
                .find(|key| !set.contains(key))
                .unwrap_or(CLIENT_ID_ENV);
            return Err(LakehouseCatalogConfigError::PartialClient(missing));
        }
        Ok(Some(Self {
            client_id: required_env(vars, CLIENT_ID_ENV)?.to_owned(),
            client_secret: required_env(vars, CLIENT_SECRET_ENV)?.to_owned(),
            token_uri: required_env(vars, CLIENT_TOKEN_URI_ENV)?.to_owned(),
            scope: required_env(vars, CLIENT_SCOPE_ENV)?.to_owned(),
        }))
    }
}

impl std::fmt::Debug for LakehouseCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakehouseCatalogClient")
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("token_uri", &self.token_uri)
            .field("scope", &self.scope)
            .finish()
    }
}

/// Lakehouse catalog configuration error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LakehouseCatalogConfigError {
    /// A required environment variable is missing or empty.
    #[error("missing required lakehouse catalog environment variable: {0}")]
    MissingEnv(&'static str),

    /// Some of the catalog client's four variables are set and not all.
    #[error("the lakehouse catalog client is set in part: {0} is missing")]
    PartialClient(&'static str),

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
