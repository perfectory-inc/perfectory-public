//! HTTP client for data.go.kr `ODCloud` file APIs served through `api.odcloud.kr`.

use collection_domain::CollectionError;
use serde_json::Value as JsonValue;

use crate::DataGoKrRequestPolicy;
use outbound_http_infrastructure::RequestCircuitBreaker;
use outbound_http_infrastructure::{
    classify_response, execute_retryable, redact_transport_error, shared_http_client, AttemptError,
    ResilienceAudit, ResilienceCtx, ResiliencePolicy, RetryDecision,
};

/// Provider label shared by the circuit breaker, audit events, and error messages.
const PROVIDER: &str = "data.go.kr ODCloud";

/// Configuration for a data.go.kr `ODCloud` file API client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGoKrOdCloudApiConfig {
    /// Base URI for `ODCloud`, usually `https://api.odcloud.kr/api`.
    pub base_uri: String,
    /// Decoded public-data portal service key.
    pub service_key: String,
    /// User-Agent header sent to the public-data provider.
    pub user_agent: String,
}

/// Raw JSON page fetched from an `ODCloud` file API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGoKrOdCloudApiPage {
    /// Raw response body bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Parsed JSON response body, suitable for metadata and schema profiling.
    pub payload: JsonValue,
    /// Number of records in the returned `data` array.
    pub logical_record_count: u64,
}

/// `reqwest` backed client for data.go.kr `ODCloud` file API pages.
#[derive(Clone, Debug)]
pub struct DataGoKrOdCloudApiClient {
    base_uri: reqwest::Url,
    service_key: String,
    user_agent: String,
    http: reqwest::Client,
    resilience: ResiliencePolicy,
    circuit_breaker: RequestCircuitBreaker,
    audit: ResilienceAudit,
}

impl DataGoKrOdCloudApiClient {
    /// Creates an `ODCloud` API client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid or credentials are empty.
    pub fn new(config: &DataGoKrOdCloudApiConfig) -> Result<Self, CollectionError> {
        Self::new_with_policy(config, DataGoKrRequestPolicy::default())
    }

    /// Creates an `ODCloud` API client from explicit configuration and request policy.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid, credentials are empty, or the HTTP
    /// client cannot be built.
    pub fn new_with_policy(
        config: &DataGoKrOdCloudApiConfig,
        policy: DataGoKrRequestPolicy,
    ) -> Result<Self, CollectionError> {
        let base_uri_raw = format!("{}/", config.base_uri.trim().trim_end_matches('/'));
        let base_uri = reqwest::Url::parse(&base_uri_raw).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid data.go.kr ODCloud base URI: {error}"))
        })?;
        let service_key = config.service_key.trim().to_owned();
        if service_key.is_empty() {
            return Err(CollectionError::Infrastructure(
                "DATA_GO_KR_SERVICE_KEY is required".to_owned(),
            ));
        }
        let user_agent = config.user_agent.trim().to_owned();
        if user_agent.is_empty() {
            return Err(CollectionError::Infrastructure(
                "data.go.kr ODCloud user_agent is required".to_owned(),
            ));
        }
        let resilience = policy.resilience_policy();
        resilience
            .validate()
            .map_err(crate::outbound_http_error::into_collection_error)?;
        let http = shared_http_client(PROVIDER, &resilience)
            .map_err(crate::outbound_http_error::into_collection_error)?;

        Ok(Self {
            base_uri,
            service_key,
            user_agent,
            http,
            resilience,
            circuit_breaker: RequestCircuitBreaker::new(PROVIDER, policy.circuit_breaker_policy()),
            audit: ResilienceAudit::new(PROVIDER),
        })
    }

    /// Fetches one JSON page from an `ODCloud` file API.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the HTTP request fails, JSON parsing fails, or the response
    /// shape does not contain the expected `data` array.
    pub async fn fetch_page(
        &self,
        endpoint_path: &str,
        page: u32,
        per_page: u32,
    ) -> Result<DataGoKrOdCloudApiPage, CollectionError> {
        if page == 0 || per_page == 0 {
            return Err(CollectionError::Infrastructure(
                "ODCloud page and per_page must be greater than zero".to_owned(),
            ));
        }
        let mut url = self.base_uri.join(endpoint_path).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid data.go.kr ODCloud endpoint: {error}"))
        })?;
        url.query_pairs_mut()
            .append_pair("serviceKey", &self.service_key)
            .append_pair("page", &page.to_string())
            .append_pair("perPage", &per_page.to_string())
            .append_pair("returnType", "JSON");

        let ctx = ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &self.resilience,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_page_once(&url))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_page_once(
        &self,
        url: &reqwest::Url,
    ) -> Result<DataGoKrOdCloudApiPage, AttemptError> {
        let response = self
            .http
            .get(url.clone())
            .header("user-agent", &self.user_agent)
            .send()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: format!(
                    "{PROVIDER} request failed: {}",
                    redact_transport_error(&error)
                ),
                retry_after: None,
            })?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("{PROVIDER} request returned HTTP {status}");
            return Err(match classify_response(status, response.headers()) {
                RetryDecision::Retryable { retry_after } => AttemptError::Retryable {
                    message,
                    retry_after,
                },
                RetryDecision::NotRetryable => AttemptError::Fatal(
                    outbound_http_infrastructure::OutboundHttpError::new(message),
                ),
            });
        }

        let raw_payload = response
            .bytes()
            .await
            .map_err(|error| AttemptError::Retryable {
                message: format!(
                    "{PROVIDER} response body read failed: {}",
                    redact_transport_error(&error)
                ),
                retry_after: None,
            })?;
        let raw_payload = raw_payload.to_vec();
        let payload: JsonValue = serde_json::from_slice(&raw_payload).map_err(|error| {
            outbound_http_infrastructure::OutboundHttpError::new(format!(
                "{PROVIDER} response JSON parse failed: {error}"
            ))
        })?;
        let logical_record_count = payload
            .get("data")
            .and_then(JsonValue::as_array)
            .map(|items| items.len() as u64)
            .ok_or_else(|| {
                outbound_http_infrastructure::OutboundHttpError::new(format!(
                    "{PROVIDER} response omitted data array"
                ))
            })?;

        Ok(DataGoKrOdCloudApiPage {
            raw_payload,
            payload,
            logical_record_count,
        })
    }
}
