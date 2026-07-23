//! Iceberg REST catalog adapter.
//!
//! This adapter uses the standard Iceberg REST catalog table-loading endpoint. Cloudflare R2 Data
//! Catalog support enters through configuration only; application code still sees the provider
//! neutral `LakehouseCatalog` port.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use lakehouse_application::ports::{LakehouseCatalog, LakehouseTableSnapshot};
use lakehouse_domain::{LakehouseError, LakehouseTableContract};
use reqwest::StatusCode;
use serde::Deserialize;

use crate::lakehouse_config::{LakehouseCatalogConfig, LakehouseCatalogProvider};
use outbound_http_infrastructure::RequestCircuitBreaker;
use outbound_http_infrastructure::{
    classify_response, execute_retryable, redact_transport_error, shared_http_client, AttemptError,
    ResilienceAudit, ResilienceCtx, RetryDecision, ICEBERG,
};

/// Provider label shared by the circuit breaker, audit events, and error messages.
const PROVIDER: &str = "Iceberg REST catalog";

const ICEBERG_ACCESS_DELEGATION_HEADER: &str = "X-Iceberg-Access-Delegation";
const VENDED_CREDENTIALS_DELEGATION: &str = "vended-credentials";

/// Provider-neutral Iceberg REST catalog client.
#[derive(Clone, Debug)]
pub struct IcebergRestCatalog {
    config: LakehouseCatalogConfig,
    client: reqwest::Client,
    catalog_prefix: Arc<OnceLock<String>>,
    circuit_breaker: RequestCircuitBreaker,
    audit: ResilienceAudit,
}

impl IcebergRestCatalog {
    /// Creates a new Iceberg REST catalog client.
    ///
    /// # Errors
    ///
    /// Returns `LakehouseError` when the resilience-configured HTTP client cannot be built.
    pub fn new(config: LakehouseCatalogConfig) -> Result<Self, LakehouseError> {
        let client = shared_http_client(PROVIDER, &ICEBERG)
            .map_err(crate::outbound_http_error::into_lakehouse_error)?;
        Ok(Self {
            config,
            client,
            catalog_prefix: Arc::new(OnceLock::new()),
            circuit_breaker: RequestCircuitBreaker::new(PROVIDER, ICEBERG.circuit_breaker),
            audit: ResilienceAudit::new(PROVIDER),
        })
    }

    const fn resilience_ctx(&self) -> ResilienceCtx<'_> {
        ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &ICEBERG,
            audit: &self.audit,
        }
    }

    fn config_url(&self) -> Result<reqwest::Url, LakehouseError> {
        let mut url = self.base_v1_url()?;

        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| LakehouseError::Upstream("catalog URI cannot be a base".into()))?;
            segments.push("config");
        }
        url.query_pairs_mut()
            .append_pair("warehouse", &self.config.warehouse);

        Ok(url)
    }

    fn load_table_url(
        &self,
        catalog_prefix: &str,
        table_name: &str,
    ) -> Result<reqwest::Url, LakehouseError> {
        let (namespace, table) = parse_table_name(table_name)?;
        let namespace_segment = namespace.join("\u{001f}");
        let mut url = self.base_v1_url()?;

        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| LakehouseError::Upstream("catalog URI cannot be a base".into()))?;
            segments.push(catalog_prefix);
            segments.push("namespaces");
            segments.push(&namespace_segment);
            segments.push("tables");
            segments.push(table);
        }

        Ok(url)
    }

    fn base_v1_url(&self) -> Result<reqwest::Url, LakehouseError> {
        let mut url = reqwest::Url::parse(&self.config.catalog_uri)
            .map_err(|error| LakehouseError::Upstream(error.to_string()))?;
        let already_has_v1 = url.path_segments().is_some_and(|mut segments| {
            segments.rfind(|segment| !segment.is_empty()) == Some("v1")
        });

        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| LakehouseError::Upstream("catalog URI cannot be a base".into()))?;
            segments.pop_if_empty();
            if !already_has_v1 {
                segments.push("v1");
            }
        }

        Ok(url)
    }

    async fn catalog_prefix(&self) -> Result<String, LakehouseError> {
        if let Some(prefix) = self.catalog_prefix.get() {
            return Ok(prefix.clone());
        }

        let prefix = self.fetch_catalog_prefix().await?;
        let _ = self.catalog_prefix.set(prefix.clone());

        Ok(self.catalog_prefix.get().cloned().unwrap_or(prefix))
    }

    async fn fetch_catalog_prefix(&self) -> Result<String, LakehouseError> {
        let url = self.config_url()?;
        execute_retryable(&self.resilience_ctx(), || {
            self.fetch_catalog_prefix_once(&url)
        })
        .await
        .map_err(crate::outbound_http_error::into_lakehouse_error)
    }

    async fn fetch_catalog_prefix_once(&self, url: &reqwest::Url) -> Result<String, AttemptError> {
        let request = self.with_catalog_headers(self.client.get(url.clone()));
        let response = request
            .send()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: redact_transport_error(&error),
                retry_after: None,
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(match classify_response(status, response.headers()) {
                RetryDecision::Retryable { retry_after } => AttemptError::Retryable {
                    message: format!("HTTP {status}"),
                    retry_after,
                },
                RetryDecision::NotRetryable => {
                    AttemptError::Fatal(outbound_http_infrastructure::OutboundHttpError::new(
                        format!("Iceberg REST catalog config failed with status {status}"),
                    ))
                }
            });
        }

        // Body transport reads are transient-class (retryable); only the decode is fatal —
        // same attempt semantics as the other migrated JSON clients.
        let raw_payload = response
            .bytes()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: format!(
                    "response body read failed: {}",
                    redact_transport_error(&error)
                ),
                retry_after: None,
            })?;
        let payload: CatalogConfigResponse =
            serde_json::from_slice(&raw_payload).map_err(|error| {
                outbound_http_infrastructure::OutboundHttpError::new(error.to_string())
            })?;

        Ok(payload
            .prefix()
            .unwrap_or(&self.config.warehouse)
            .to_owned())
    }

    fn with_catalog_headers(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        if let Some(token) = &self.config.catalog_token {
            request = request.bearer_auth(token);
        }
        if self.config.provider == LakehouseCatalogProvider::R2DataCatalog {
            request = request.header(
                ICEBERG_ACCESS_DELEGATION_HEADER,
                VENDED_CREDENTIALS_DELEGATION,
            );
        }
        request
    }

    async fn load_table(
        &self,
        table_name: &str,
    ) -> Result<Option<LakehouseTableSnapshot>, LakehouseError> {
        let catalog_prefix = self.catalog_prefix().await?;
        let url = self.load_table_url(&catalog_prefix, table_name)?;
        execute_retryable(&self.resilience_ctx(), || {
            self.load_table_once(&url, table_name)
        })
        .await
        .map_err(crate::outbound_http_error::into_lakehouse_error)
    }

    async fn load_table_once(
        &self,
        url: &reqwest::Url,
        table_name: &str,
    ) -> Result<Option<LakehouseTableSnapshot>, AttemptError> {
        let request = self.with_catalog_headers(self.client.get(url.clone()));
        let response = request
            .send()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: redact_transport_error(&error),
                retry_after: None,
            })?;

        let status = response.status();
        // A missing table is a successful outcome, never a retryable failure.
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !status.is_success() {
            return Err(match classify_response(status, response.headers()) {
                RetryDecision::Retryable { retry_after } => AttemptError::Retryable {
                    message: format!("HTTP {status}"),
                    retry_after,
                },
                RetryDecision::NotRetryable => {
                    AttemptError::Fatal(outbound_http_infrastructure::OutboundHttpError::new(
                        format!("Iceberg REST load table failed with status {status}"),
                    ))
                }
            });
        }

        // Body transport reads are transient-class (retryable); only the decode is fatal.
        let raw_payload = response
            .bytes()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: format!(
                    "response body read failed: {}",
                    redact_transport_error(&error)
                ),
                retry_after: None,
            })?;
        let payload: LoadTableResponse = serde_json::from_slice(&raw_payload).map_err(|error| {
            outbound_http_infrastructure::OutboundHttpError::new(error.to_string())
        })?;
        let snapshot_id = payload.current_snapshot_id().ok_or_else(|| {
            outbound_http_infrastructure::OutboundHttpError::new(
                "Iceberg REST load table response omitted current snapshot id",
            )
        })?;

        Ok(Some(LakehouseTableSnapshot {
            table_name: table_name.to_owned(),
            snapshot_id,
            metadata_location: payload.metadata_location,
        }))
    }
}

#[async_trait]
impl LakehouseCatalog for IcebergRestCatalog {
    async fn ensure_table(
        &self,
        contract: &'static LakehouseTableContract,
    ) -> Result<LakehouseTableSnapshot, LakehouseError> {
        self.load_table(contract.table_name).await?.ok_or_else(|| {
            LakehouseError::Upstream(format!(
                "lakehouse table not found: {}",
                contract.table_name
            ))
        })
    }

    async fn get_current_snapshot(
        &self,
        table_name: &str,
    ) -> Result<Option<LakehouseTableSnapshot>, LakehouseError> {
        self.load_table(table_name).await
    }
}

#[derive(Debug, Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: BTreeMap<String, String>,
    #[serde(default)]
    defaults: BTreeMap<String, String>,
}

impl CatalogConfigResponse {
    fn prefix(&self) -> Option<&str> {
        self.overrides
            .get("prefix")
            .or_else(|| self.defaults.get("prefix"))
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Deserialize)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: IcebergTableMetadata,
}

impl LoadTableResponse {
    fn current_snapshot_id(&self) -> Option<String> {
        match &self.metadata.current_snapshot_id {
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::String(value) if !value.is_empty() => Some(value.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct IcebergTableMetadata {
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: serde_json::Value,
}

fn parse_table_name(table_name: &str) -> Result<(Vec<&str>, &str), LakehouseError> {
    let mut parts = table_name.split('.').collect::<Vec<_>>();
    let table = parts
        .pop()
        .ok_or_else(|| LakehouseError::Upstream("lakehouse table name is empty".into()))?;

    if table.is_empty() || parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        return Err(LakehouseError::Upstream(format!(
            "invalid lakehouse table name: {table_name}"
        )));
    }

    Ok((parts, table))
}
