//! The lakehouse catalog a run opens, in the mode its environment names (root ADR-0624 P6, P7).
//!
//! A run that holds a Lakekeeper client (`LakehouseCatalogClient`, the lane's OAuth2 machine identity)
//! is in Lakekeeper mode: the catalog is iceberg-catalog-rest's, opened with that client, and every
//! table file is reached with the credential the catalog vends for that table on load. No catalog
//! token and no static storage key may stand beside it: a key set beside a vending catalog wins over
//! the vended one (iceberg-catalog-rest 0.11 `load_table` lays the properties over the table's config,
//! T8), so such a run refuses to start. A run without a client is on the current catalog, as before.

use std::{collections::HashMap, sync::Arc};

use anyhow::{ensure, Context};
use async_trait::async_trait;
use iceberg::{spec::TableMetadata, Catalog, ErrorKind, NamespaceIdent, TableIdent};
use lakehouse_domain::LakehouseError;
use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogClient, LakehouseCatalogConfig, LakehouseCatalogProvider,
    TableMetadataSource,
};

/// Storage keys a Lakekeeper-mode run holds none of: with none set, the vended credential is the
/// only one (the same list `check-lakehouse-catalog-writes` runs without, T8).
pub const STATIC_KEYS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
    "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
    "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID",
    "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
];

/// The header that asks a REST catalog for the table's own storage credential on load.
const ACCESS_DELEGATION_HEADER: &str = "header.X-Iceberg-Access-Delegation";

/// Refuses a run that holds any of [`STATIC_KEYS`] (root ADR-0624 §3-3: a job with a static R2 key
/// does not start).
///
/// # Errors
/// Names the variables that are set.
pub fn refuse_static_keys(is_set: impl Fn(&str) -> bool) -> anyhow::Result<()> {
    let held: Vec<&str> = STATIC_KEYS
        .iter()
        .copied()
        .filter(|name| is_set(name))
        .collect();
    ensure!(
        held.is_empty(),
        "{held:?} set: a run on the Lakekeeper catalog reaches table files with the vended credential only"
    );
    Ok(())
}

/// The catalog's settings and, in Lakekeeper mode, the lane's client, as the environment names them.
#[derive(Clone, Debug)]
pub struct CatalogSettings {
    /// The catalog's URI, warehouse and provider (and, on the current catalog, its token).
    pub config: LakehouseCatalogConfig,
    /// The lane's Lakekeeper client; `None` on the current catalog.
    pub client: Option<LakehouseCatalogClient>,
}

impl CatalogSettings {
    /// Reads the writer's settings (`LakehouseCatalogConfig::from_env`) and the lane's client.
    ///
    /// # Errors
    /// Returns a missing or partial variable, and refuses Lakekeeper mode beside a catalog token, a
    /// provider other than `iceberg_rest` or a static storage key.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::checked(
            LakehouseCatalogConfig::from_env()?,
            LakehouseCatalogClient::from_env()?,
            |name| std::env::var_os(name).is_some(),
        )
    }

    /// Reads the reader's settings (`LakehouseCatalogConfig::reader_from_env`) and the lane's client:
    /// in Lakekeeper mode the lane's grants, not a token, decide what it may read.
    ///
    /// # Errors
    /// As [`Self::from_env`].
    pub fn reader_from_env() -> anyhow::Result<Self> {
        Self::checked(
            LakehouseCatalogConfig::reader_from_env()?,
            LakehouseCatalogClient::from_env()?,
            |name| std::env::var_os(name).is_some(),
        )
    }

    /// The settings, held to Lakekeeper mode's rules when a client is present.
    ///
    /// # Errors
    /// As [`Self::from_env`].
    pub fn checked(
        config: LakehouseCatalogConfig,
        client: Option<LakehouseCatalogClient>,
        is_set: impl Fn(&str) -> bool,
    ) -> anyhow::Result<Self> {
        if client.is_some() {
            ensure!(
                config.provider == LakehouseCatalogProvider::IcebergRest,
                "a Lakekeeper client needs the iceberg_rest catalog provider, not {}",
                config.provider.wire_name()
            );
            ensure!(
                config.catalog_token.is_none(),
                "a Lakekeeper client and a catalog token are both set: the client is the run's only identity"
            );
            refuse_static_keys(is_set)?;
        }
        Ok(Self { config, client })
    }

    /// The iceberg-catalog-rest properties of this catalog. Lakekeeper mode: the client's credential,
    /// its token endpoint and scope, and vended credentials; no `s3.*` key. The current catalog: its
    /// token, and the storage settings `storage` gives (its static key pair).
    #[must_use]
    pub fn properties(&self, storage: &[(String, String)]) -> HashMap<String, String> {
        let mut properties = HashMap::from([
            ("uri".to_owned(), self.config.catalog_uri.clone()),
            ("warehouse".to_owned(), self.config.warehouse.clone()),
        ]);
        if let Some(client) = &self.client {
            properties.extend([
                (
                    "credential".to_owned(),
                    format!("{}:{}", client.client_id, client.client_secret),
                ),
                ("oauth2-server-uri".to_owned(), client.token_uri.clone()),
                ("scope".to_owned(), client.scope.clone()),
                (
                    ACCESS_DELEGATION_HEADER.to_owned(),
                    "vended-credentials".to_owned(),
                ),
            ]);
        } else {
            properties.extend(storage.iter().cloned());
            if let Some(token) = &self.config.catalog_token {
                properties.insert("token".to_owned(), token.clone());
            }
        }
        properties
    }

    /// The `IcebergRestCatalog` of these settings: on the current catalog its own HTTP client, in
    /// Lakekeeper mode one whose table loads come from iceberg-catalog-rest opened with the client.
    ///
    /// # Errors
    /// Returns the catalog's refusal of the configuration.
    pub async fn rest_catalog(&self) -> anyhow::Result<IcebergRestCatalog> {
        if self.client.is_none() {
            return Ok(IcebergRestCatalog::new(self.config.clone())?);
        }
        let catalog = crate::lakehouse_rest_catalog::open(self.properties(&[])).await?;
        Ok(IcebergRestCatalog::with_table_source(
            self.config.clone(),
            Arc::new(IcebergRustTables { catalog }),
        )?)
    }
}

/// Load-table answers from iceberg-catalog-rest, in the REST specification's own shape.
pub struct IcebergRustTables {
    /// The catalog, opened with the lane's client.
    pub catalog: Arc<dyn Catalog>,
}

impl std::fmt::Debug for IcebergRustTables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IcebergRustTables")
    }
}

/// `namespace.table` as iceberg-rust's identifier.
///
/// # Errors
/// Refuses a name without a namespace or with an empty part.
pub fn table_ident(table_name: &str) -> anyhow::Result<TableIdent> {
    let (namespace, table) = table_name
        .rsplit_once('.')
        .with_context(|| format!("{table_name} names no namespace"))?;
    ensure!(
        !table.is_empty() && namespace.split('.').all(|part| !part.is_empty()),
        "invalid lakehouse table name: {table_name}"
    );
    Ok(TableIdent::new(
        NamespaceIdent::from_strs(namespace.split('.'))?,
        table.to_owned(),
    ))
}

#[async_trait]
impl TableMetadataSource for IcebergRustTables {
    async fn load_table(
        &self,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, LakehouseError> {
        let ident =
            table_ident(table_name).map_err(|error| LakehouseError::Upstream(error.to_string()))?;
        let table = match self.catalog.load_table(&ident).await {
            Ok(table) => table,
            Err(error) if error.kind() == ErrorKind::TableNotFound => return Ok(None),
            Err(error) => {
                return Err(LakehouseError::Upstream(format!(
                    "the catalog did not load {table_name}: {}",
                    error.kind()
                )))
            }
        };
        load_table_answer(table.metadata_location(), table.metadata())
            .map(Some)
            .map_err(|error| LakehouseError::Upstream(format!("{table_name}: {error}")))
    }
}

/// The REST specification's load-table answer (`metadata-location`, and the metadata as the
/// specification writes it) of iceberg-rust's loaded table.
///
/// # Errors
/// Returns the metadata's refusal to serialize.
pub fn load_table_answer(
    metadata_location: Option<&str>,
    metadata: &TableMetadata,
) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::json!({
        "metadata-location": metadata_location.unwrap_or_default(),
        "metadata": serde_json::to_value(metadata).context("the table's metadata does not serialize")?,
    }))
}

#[cfg(test)]
mod tests;
