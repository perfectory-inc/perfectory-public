//! HTTP client for the `VWorld` 2D Data API served through `api.vworld.kr/req/data`.

use collection_domain::CollectionError;
use serde_json::Value as JsonValue;

use crate::VWorldRequestPolicy;
use outbound_http_infrastructure::RequestCircuitBreaker;
use outbound_http_infrastructure::{
    classify_response, execute_retryable, redact_transport_error, shared_http_client, AttemptError,
    ResilienceAudit, ResilienceCtx, ResiliencePolicy, RetryDecision,
};

/// Provider label shared by the circuit breaker, audit events, and error messages.
const PROVIDER: &str = "VWorld Data API";

/// Configuration for a `VWorld` 2D Data API client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDataApiConfig {
    /// Base URI for the API host, usually `https://api.vworld.kr`.
    pub base_uri: String,
    /// `VWorld` `OpenAPI` key.
    pub api_key: String,
    /// Optional domain value required by some browser-oriented `VWorld` keys.
    pub domain: Option<String>,
    /// User-Agent header sent to `VWorld`.
    pub user_agent: String,
}

/// One `VWorld` 2D Data `GetFeature` page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDataFeatureRequest {
    /// Dataset id, for example `LP_PA_CBND_BUBUN`.
    pub dataset: String,
    /// Required attribute filter such as `emdCd:=:11680103` or `pnu:=:9999900801105800001`.
    pub attr_filter: Option<String>,
    /// Optional response columns. Empty means provider default/all columns.
    pub columns: Vec<String>,
    /// Whether geometry should be returned.
    pub geometry: bool,
    /// Whether attributes should be returned.
    pub attribute: bool,
    /// Optional coordinate reference system, for example `EPSG:4326`.
    pub crs: Option<String>,
    /// One-based page number.
    pub page: u32,
    /// Requested page size. `VWorld` documents a maximum of 1000.
    pub size: u32,
}

/// Raw JSON page fetched from a `VWorld` 2D Data API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldDataApiPage {
    /// Raw response body bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Parsed JSON response body, suitable for metadata and schema profiling.
    pub payload: JsonValue,
}

/// `reqwest` backed client for `VWorld` 2D Data API JSON pages.
#[derive(Clone, Debug)]
pub struct VWorldDataApiClient {
    base_uri: reqwest::Url,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    http: reqwest::Client,
    resilience: ResiliencePolicy,
    circuit_breaker: RequestCircuitBreaker,
    audit: ResilienceAudit,
}

impl VWorldDataApiClient {
    /// Creates a `VWorld` 2D Data API client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid or credentials are empty.
    pub fn new(config: &VWorldDataApiConfig) -> Result<Self, CollectionError> {
        Self::new_with_policy(config, VWorldRequestPolicy::default())
    }

    /// Creates a `VWorld` 2D Data API client from explicit configuration and request policy.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid, credentials are empty, or the HTTP
    /// client cannot be built.
    pub fn new_with_policy(
        config: &VWorldDataApiConfig,
        policy: VWorldRequestPolicy,
    ) -> Result<Self, CollectionError> {
        let base_uri_raw = format!("{}/", config.base_uri.trim().trim_end_matches('/'));
        let base_uri = reqwest::Url::parse(&base_uri_raw).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid VWorld Data API base URI: {error}"))
        })?;
        let api_key = config.api_key.trim().to_owned();
        if api_key.is_empty() {
            return Err(CollectionError::Infrastructure(
                "VWorld API key is required".to_owned(),
            ));
        }
        let domain = config.domain.as_deref().map(str::trim).and_then(|value| {
            if value.is_empty() {
                None
            } else {
                Some(value.to_owned())
            }
        });
        let user_agent = config.user_agent.trim().to_owned();
        if user_agent.is_empty() {
            return Err(CollectionError::Infrastructure(
                "VWorld Data API user_agent is required".to_owned(),
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
            api_key,
            domain,
            user_agent,
            http,
            resilience,
            circuit_breaker: RequestCircuitBreaker::new(PROVIDER, resilience.circuit_breaker),
            audit: ResilienceAudit::new(PROVIDER),
        })
    }

    /// Fetches one JSON `GetFeature` page from the `VWorld` 2D Data API.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the request is invalid, HTTP request fails, JSON parsing fails,
    /// or `VWorld` returns a provider error envelope.
    pub async fn fetch_feature_page(
        &self,
        request: &VWorldDataFeatureRequest,
    ) -> Result<VWorldDataApiPage, CollectionError> {
        validate_request(request)?;
        let ctx = ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &self.resilience,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_feature_page_once(request))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_feature_page_once(
        &self,
        request: &VWorldDataFeatureRequest,
    ) -> Result<VWorldDataApiPage, AttemptError> {
        let url = self
            .request_url(request)
            .map_err(crate::outbound_http_error::into_fatal_attempt)?;
        let response = self
            .http
            .get(url)
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
        validate_vworld_data_success(&payload)
            .map_err(crate::outbound_http_error::into_fatal_attempt)?;

        Ok(VWorldDataApiPage {
            raw_payload,
            payload,
        })
    }

    fn request_url(
        &self,
        request: &VWorldDataFeatureRequest,
    ) -> Result<reqwest::Url, CollectionError> {
        let mut url = self.base_uri.join("req/data").map_err(|error| {
            CollectionError::Infrastructure(format!("invalid VWorld Data API request URL: {error}"))
        })?;
        let mut query = url.query_pairs_mut();
        query
            .append_pair("key", &self.api_key)
            .append_pair("service", "data")
            .append_pair("request", "GetFeature")
            .append_pair("data", &request.dataset)
            .append_pair("format", "json")
            .append_pair("geometry", bool_query_value(request.geometry))
            .append_pair("attribute", bool_query_value(request.attribute))
            .append_pair("page", &request.page.to_string())
            .append_pair("size", &request.size.to_string());
        if let Some(domain) = &self.domain {
            query.append_pair("domain", domain);
        }
        if let Some(attr_filter) = &request.attr_filter {
            query.append_pair("attrFilter", attr_filter);
        }
        if !request.columns.is_empty() {
            query.append_pair("columns", &request.columns.join(","));
        }
        if let Some(crs) = &request.crs {
            query.append_pair("crs", crs);
        }
        drop(query);
        Ok(url)
    }
}

fn validate_request(request: &VWorldDataFeatureRequest) -> Result<(), CollectionError> {
    validate_dataset(&request.dataset)?;
    if request.page == 0 || request.size == 0 {
        return Err(CollectionError::Infrastructure(
            "VWorld Data API page and size must be greater than zero".to_owned(),
        ));
    }
    if request.size > 1_000 {
        return Err(CollectionError::Infrastructure(
            "VWorld Data API size must not exceed 1000".to_owned(),
        ));
    }
    if request.attr_filter.is_none() {
        return Err(CollectionError::Infrastructure(
            "VWorld Data API attr_filter is required".to_owned(),
        ));
    }
    if let Some(value) = &request.attr_filter {
        validate_filter_value("attr_filter", value)?;
    }
    for column in &request.columns {
        validate_identifier("column", column)?;
    }
    if let Some(crs) = &request.crs {
        validate_crs(crs)?;
    }
    Ok(())
}

fn validate_dataset(dataset: &str) -> Result<(), CollectionError> {
    if !dataset.is_empty()
        && dataset
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Ok(());
    }
    Err(CollectionError::Infrastructure(
        "VWorld Data API dataset must contain only uppercase ASCII letters, digits, and '_'"
            .to_owned(),
    ))
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), CollectionError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Ok(());
    }
    Err(CollectionError::Infrastructure(format!(
        "VWorld Data API {field} must contain only ASCII letters, digits, and '_'"
    )))
}

fn validate_filter_value(field: &'static str, value: &str) -> Result<(), CollectionError> {
    if value.trim() != value || value.is_empty() || value.contains('\n') || value.contains('\r') {
        return Err(CollectionError::Infrastructure(format!(
            "VWorld Data API {field} must be non-empty single-line text without surrounding whitespace"
        )));
    }
    Ok(())
}

fn validate_crs(crs: &str) -> Result<(), CollectionError> {
    if let Some(raw) = crs.strip_prefix("EPSG:") {
        if !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return Ok(());
        }
    }
    Err(CollectionError::Infrastructure(
        "VWorld Data API crs must use EPSG:<digits>".to_owned(),
    ))
}

fn validate_vworld_data_success(payload: &JsonValue) -> Result<(), CollectionError> {
    let status = payload
        .pointer("/response/status")
        .and_then(JsonValue::as_str)
        .unwrap_or("<missing>");
    if matches!(status, "OK" | "NOT_FOUND") {
        return Ok(());
    }
    let code = payload
        .pointer("/response/error/code")
        .and_then(JsonValue::as_str)
        .unwrap_or(status);
    let text = payload
        .pointer("/response/error/text")
        .and_then(JsonValue::as_str)
        .unwrap_or("<missing>");
    Err(CollectionError::Infrastructure(format!(
        "VWorld Data API request failed with status={status} code={code} text={text}"
    )))
}

const fn bool_query_value(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}
