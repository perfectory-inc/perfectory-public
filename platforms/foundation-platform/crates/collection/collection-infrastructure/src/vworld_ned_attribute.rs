//! HTTP client for `VWorld` NED attribute APIs served through `api.vworld.kr/ned/data`.

use std::{collections::BTreeMap, time::Duration};

use collection_domain::CollectionError;
use serde_json::Value as JsonValue;

use outbound_http_infrastructure::RequestCircuitBreaker;
use outbound_http_infrastructure::{
    classify_response, execute_retryable, redact_transport_error, shared_http_client, AttemptError,
    ResilienceAudit, ResilienceCtx, ResiliencePolicy, RetryDecision, VWORLD_JSON,
};

/// Provider label shared by the circuit breaker, audit events, and error messages.
const PROVIDER: &str = "VWorld NED attribute";

/// Configuration for a `VWorld` NED attribute API client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldNedAttributeConfig {
    /// Base URI for NED attributes, usually `https://api.vworld.kr/ned/data`.
    pub base_uri: String,
    /// `VWorld` `OpenAPI` key.
    pub api_key: String,
    /// Optional domain value required by some browser-oriented `VWorld` keys.
    pub domain: Option<String>,
    /// User-Agent header sent to `VWorld`.
    pub user_agent: String,
}

/// Bounded retry and timeout policy for `VWorld` requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VWorldRequestPolicy {
    max_attempts: u32,
    request_timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl VWorldRequestPolicy {
    /// Creates a bounded `VWorld` request policy.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when retry attempts or durations are invalid.
    pub fn new(
        max_attempts: u32,
        request_timeout: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, CollectionError> {
        if max_attempts == 0 {
            return Err(CollectionError::Infrastructure(
                "VWorld request max_attempts must be greater than zero".to_owned(),
            ));
        }
        if request_timeout.is_zero() {
            return Err(CollectionError::Infrastructure(
                "VWorld request timeout must be greater than zero".to_owned(),
            ));
        }
        if max_backoff < initial_backoff {
            return Err(CollectionError::Infrastructure(
                "VWorld request max_backoff must be greater than or equal to initial_backoff"
                    .to_owned(),
            ));
        }
        Ok(Self {
            max_attempts,
            request_timeout,
            initial_backoff,
            max_backoff,
        })
    }

    /// Number of attempts before the request fails.
    #[must_use]
    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    /// Per-attempt request timeout.
    #[must_use]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Initial delay before retrying a failed request.
    #[must_use]
    pub const fn initial_backoff(self) -> Duration {
        self.initial_backoff
    }

    /// Maximum delay between retry attempts.
    #[must_use]
    pub const fn max_backoff(self) -> Duration {
        self.max_backoff
    }

    /// Converts this env-overridable legacy surface into the shared [`ResiliencePolicy`].
    ///
    /// The legacy single `request_timeout` bounded the whole attempt with one `.timeout()`,
    /// so it maps onto all three timeout knobs (connect/read/total) — identical effective
    /// behavior, now expressed in the SSOT vocabulary.
    pub(crate) const fn resilience_policy(self) -> ResiliencePolicy {
        ResiliencePolicy {
            connect_timeout: self.request_timeout,
            read_timeout: self.request_timeout,
            total_timeout: Some(self.request_timeout),
            max_attempts: self.max_attempts,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
            jitter: VWORLD_JSON.jitter,
            circuit_breaker: VWORLD_JSON.circuit_breaker,
        }
    }
}

impl Default for VWorldRequestPolicy {
    fn default() -> Self {
        // The provider `const` is the SSOT for default values; `request_timeout` is the
        // legacy name for the whole-attempt bound carried by all VWORLD_JSON timeout knobs.
        Self {
            max_attempts: VWORLD_JSON.max_attempts,
            request_timeout: VWORLD_JSON.read_timeout,
            initial_backoff: VWORLD_JSON.initial_backoff,
            max_backoff: VWORLD_JSON.max_backoff,
        }
    }
}

/// Raw JSON page fetched from a `VWorld` NED attribute API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldNedAttributePage {
    /// Raw response body bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Parsed JSON response body, suitable for metadata and schema profiling.
    pub payload: JsonValue,
}

/// `reqwest` backed client for `VWorld` NED attribute JSON pages.
#[derive(Clone, Debug)]
pub struct VWorldNedAttributeClient {
    base_uri: reqwest::Url,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    http: reqwest::Client,
    resilience: ResiliencePolicy,
    circuit_breaker: RequestCircuitBreaker,
    audit: ResilienceAudit,
}

impl VWorldNedAttributeClient {
    /// Creates a `VWorld` NED attribute client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid or credentials are empty.
    pub fn new(config: &VWorldNedAttributeConfig) -> Result<Self, CollectionError> {
        Self::new_with_policy(config, VWorldRequestPolicy::default())
    }

    /// Creates a `VWorld` NED attribute client from explicit configuration and request policy.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid, credentials are empty, or the HTTP
    /// client cannot be built.
    pub fn new_with_policy(
        config: &VWorldNedAttributeConfig,
        policy: VWorldRequestPolicy,
    ) -> Result<Self, CollectionError> {
        let base_uri_raw = format!("{}/", config.base_uri.trim().trim_end_matches('/'));
        let base_uri = reqwest::Url::parse(&base_uri_raw).map_err(|error| {
            CollectionError::Infrastructure(format!(
                "invalid VWorld NED attribute base URI: {error}"
            ))
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
                "VWorld NED attribute user_agent is required".to_owned(),
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

    /// Fetches one JSON page from a `VWorld` NED attribute operation such as `ladfrlList`.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the HTTP request fails, JSON parsing fails, or `VWorld` returns
    /// a provider error envelope.
    pub async fn fetch_json_page(
        &self,
        operation: &str,
        query_params: &BTreeMap<String, String>,
        page_no: u32,
        num_of_rows: u32,
    ) -> Result<VWorldNedAttributePage, CollectionError> {
        validate_request(operation, query_params, page_no, num_of_rows)?;
        let ctx = ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &self.resilience,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || {
            self.fetch_json_page_once(operation, query_params, page_no, num_of_rows)
        })
        .await
        .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_json_page_once(
        &self,
        operation: &str,
        query_params: &BTreeMap<String, String>,
        page_no: u32,
        num_of_rows: u32,
    ) -> Result<VWorldNedAttributePage, AttemptError> {
        let url = self
            .request_url(operation, query_params, page_no, num_of_rows)
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
        validate_vworld_success(&payload)
            .map_err(crate::outbound_http_error::into_fatal_attempt)?;

        Ok(VWorldNedAttributePage {
            raw_payload,
            payload,
        })
    }

    fn request_url(
        &self,
        operation: &str,
        query_params: &BTreeMap<String, String>,
        page_no: u32,
        num_of_rows: u32,
    ) -> Result<reqwest::Url, CollectionError> {
        let mut url = self.base_uri.join(operation).map_err(|error| {
            CollectionError::Infrastructure(format!(
                "invalid VWorld NED attribute operation URL: {error}"
            ))
        })?;
        let mut query = url.query_pairs_mut();
        query
            .append_pair("key", &self.api_key)
            .append_pair("format", "json");
        if let Some(domain) = &self.domain {
            query.append_pair("domain", domain);
        }
        for (name, value) in query_params {
            query.append_pair(name, value);
        }
        query
            .append_pair("pageNo", &page_no.to_string())
            .append_pair("numOfRows", &num_of_rows.to_string());
        drop(query);
        Ok(url)
    }
}

fn validate_request(
    operation: &str,
    query_params: &BTreeMap<String, String>,
    page_no: u32,
    num_of_rows: u32,
) -> Result<(), CollectionError> {
    validate_operation(operation)?;
    if page_no == 0 || num_of_rows == 0 {
        return Err(CollectionError::Infrastructure(
            "VWorld pageNo and numOfRows must be greater than zero".to_owned(),
        ));
    }
    for (name, value) in query_params {
        validate_query_name(name)?;
        if value.trim() != value {
            return Err(CollectionError::Infrastructure(format!(
                "VWorld query parameter {name} must not contain leading or trailing whitespace"
            )));
        }
    }
    Ok(())
}

fn validate_operation(operation: &str) -> Result<(), CollectionError> {
    if !operation.is_empty() && operation.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Ok(());
    }
    Err(CollectionError::Infrastructure(
        "VWorld NED operation must contain only ASCII letters and digits".to_owned(),
    ))
}

fn validate_query_name(name: &str) -> Result<(), CollectionError> {
    if !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Ok(());
    }
    Err(CollectionError::Infrastructure(
        "VWorld query parameter names must contain only ASCII letters, digits, '_' and '-'"
            .to_owned(),
    ))
}

fn validate_vworld_success(payload: &JsonValue) -> Result<(), CollectionError> {
    let Some(status) = payload
        .pointer("/response/status")
        .and_then(JsonValue::as_str)
    else {
        return validate_vworld_error_envelope(payload);
    };
    if matches!(status, "OK" | "DONE") {
        return validate_vworld_error_envelope(payload);
    }
    let message = payload
        .pointer("/response/message")
        .and_then(JsonValue::as_str)
        .unwrap_or("<missing>");
    Err(CollectionError::Infrastructure(format!(
        "VWorld NED attribute request failed with status={status} message={message}"
    )))
}

fn validate_vworld_error_envelope(payload: &JsonValue) -> Result<(), CollectionError> {
    if let Some(response) = payload.pointer("/response") {
        if let Some(result_code) = response
            .get("resultCode")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|code| !code.is_empty())
        {
            if !is_success_result_code(result_code) {
                let message = response
                    .get("resultMsg")
                    .and_then(JsonValue::as_str)
                    .map(str::trim)
                    .filter(|message| !message.is_empty())
                    .unwrap_or("<missing>");
                return Err(CollectionError::Infrastructure(format!(
                    "VWorld NED attribute request failed with resultCode={result_code} resultMsg={message}"
                )));
            }
        }
    }

    for pointer in ["/ladfrlVOList", "/fields", "/response"] {
        let Some(container) = payload.pointer(pointer) else {
            continue;
        };
        let Some(error) = container
            .get("error")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|error| !error.is_empty())
        else {
            continue;
        };
        let message = container
            .get("message")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .unwrap_or("<missing>");
        return Err(CollectionError::Infrastructure(format!(
            "VWorld NED attribute request failed with error={error} message={message}"
        )));
    }
    Ok(())
}

fn is_success_result_code(result_code: &str) -> bool {
    matches!(result_code, "00" | "000" | "OK" | "DONE")
}
