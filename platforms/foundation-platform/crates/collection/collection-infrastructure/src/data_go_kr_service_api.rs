//! HTTP client for service APIs served through `apis.data.go.kr`.

use std::time::Duration;

use collection_application::PublicDataBronzePageRequest;
use collection_domain::CollectionError;
use serde_json::Value as JsonValue;

use outbound_http_infrastructure::{
    classify_response, execute_retryable, redact_transport_error, shared_http_client, AttemptError,
    ResilienceAudit, ResilienceCtx, ResiliencePolicy, RetryDecision, DATA_GO_KR,
};
use outbound_http_infrastructure::{RequestCircuitBreaker, RequestCircuitBreakerPolicy};

/// Provider label shared by the circuit breaker, audit events, and error messages.
const PROVIDER: &str = "data.go.kr service API";

/// Configuration for an `apis.data.go.kr` service API client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGoKrServiceApiConfig {
    /// Base URI for the target service API.
    pub base_uri: String,
    /// Decoded public-data portal service key.
    pub service_key: String,
    /// User-Agent header sent to the public-data provider.
    pub user_agent: String,
}

/// Bounded retry and timeout policy for data.go.kr requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataGoKrRequestPolicy {
    max_attempts: u32,
    request_timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    circuit_breaker_policy: RequestCircuitBreakerPolicy,
}

impl DataGoKrRequestPolicy {
    /// Creates a bounded request policy.
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
                "data.go.kr request max_attempts must be greater than zero".to_owned(),
            ));
        }
        if request_timeout.is_zero() {
            return Err(CollectionError::Infrastructure(
                "data.go.kr request timeout must be greater than zero".to_owned(),
            ));
        }
        if max_backoff < initial_backoff {
            return Err(CollectionError::Infrastructure(
                "data.go.kr request max_backoff must be greater than or equal to initial_backoff"
                    .to_owned(),
            ));
        }
        Ok(Self {
            max_attempts,
            request_timeout,
            initial_backoff,
            max_backoff,
            circuit_breaker_policy: RequestCircuitBreakerPolicy::default(),
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

    /// Initial delay before retrying a transient failure.
    #[must_use]
    pub const fn initial_backoff(self) -> Duration {
        self.initial_backoff
    }

    /// Maximum retry delay after exponential backoff is applied.
    #[must_use]
    pub const fn max_backoff(self) -> Duration {
        self.max_backoff
    }

    /// Overrides the in-process circuit breaker for high-throughput controlled collectors.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when `failure_threshold` is zero or `open_duration` is zero.
    pub fn with_circuit_breaker(
        mut self,
        failure_threshold: u32,
        open_duration: Duration,
    ) -> Result<Self, CollectionError> {
        if failure_threshold == 0 {
            return Err(CollectionError::Infrastructure(
                "data.go.kr circuit breaker failure_threshold must be greater than zero".to_owned(),
            ));
        }
        if open_duration.is_zero() {
            return Err(CollectionError::Infrastructure(
                "data.go.kr circuit breaker open_duration must be greater than zero".to_owned(),
            ));
        }
        self.circuit_breaker_policy =
            RequestCircuitBreakerPolicy::new(failure_threshold, open_duration);
        Ok(self)
    }

    pub(crate) const fn circuit_breaker_policy(self) -> RequestCircuitBreakerPolicy {
        self.circuit_breaker_policy
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
            jitter: DATA_GO_KR.jitter,
            circuit_breaker: self.circuit_breaker_policy,
        }
    }
}

impl Default for DataGoKrRequestPolicy {
    fn default() -> Self {
        // The provider `const` is the SSOT for default values; `request_timeout` is the
        // legacy name for the whole-attempt bound carried by all DATA_GO_KR timeout knobs.
        Self {
            max_attempts: DATA_GO_KR.max_attempts,
            request_timeout: DATA_GO_KR.read_timeout,
            initial_backoff: DATA_GO_KR.initial_backoff,
            max_backoff: DATA_GO_KR.max_backoff,
            circuit_breaker_policy: DATA_GO_KR.circuit_breaker,
        }
    }
}

/// Raw JSON page fetched from an `apis.data.go.kr` service API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGoKrServiceApiPage {
    /// Raw response body bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Parsed JSON response body, suitable for metadata and schema profiling.
    pub payload: JsonValue,
}

/// `reqwest` backed client for `apis.data.go.kr` JSON service API pages.
#[derive(Clone, Debug)]
pub struct DataGoKrServiceApiClient {
    base_uri: reqwest::Url,
    service_key: String,
    user_agent: String,
    http: reqwest::Client,
    resilience: ResiliencePolicy,
    circuit_breaker: RequestCircuitBreaker,
    audit: ResilienceAudit,
}

impl DataGoKrServiceApiClient {
    /// Creates an `apis.data.go.kr` service API client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid or credentials are empty.
    pub fn new(config: &DataGoKrServiceApiConfig) -> Result<Self, CollectionError> {
        Self::new_with_policy(config, DataGoKrRequestPolicy::default())
    }

    /// Creates an `apis.data.go.kr` service API client from explicit config and request policy.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid, credentials are empty, or the HTTP
    /// client cannot be built.
    pub fn new_with_policy(
        config: &DataGoKrServiceApiConfig,
        policy: DataGoKrRequestPolicy,
    ) -> Result<Self, CollectionError> {
        let base_uri_raw = format!("{}/", config.base_uri.trim().trim_end_matches('/'));
        let base_uri = reqwest::Url::parse(&base_uri_raw).map_err(|error| {
            CollectionError::Infrastructure(format!(
                "invalid data.go.kr service API base URI: {error}"
            ))
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
                "data.go.kr service API user_agent is required".to_owned(),
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

    /// Fetches one JSON page from an `apis.data.go.kr` service API.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the HTTP request fails, JSON parsing fails, or the provider
    /// envelope reports a non-success result code.
    pub async fn fetch_page(
        &self,
        request: &PublicDataBronzePageRequest,
    ) -> Result<DataGoKrServiceApiPage, CollectionError> {
        let ctx = ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &self.resilience,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_page_once(request))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_page_once(
        &self,
        request: &PublicDataBronzePageRequest,
    ) -> Result<DataGoKrServiceApiPage, AttemptError> {
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
        validate_success_envelope(&payload)
            .map_err(crate::outbound_http_error::into_fatal_attempt)?;

        Ok(DataGoKrServiceApiPage {
            raw_payload,
            payload,
        })
    }

    fn request_url(
        &self,
        request: &PublicDataBronzePageRequest,
    ) -> Result<reqwest::Url, CollectionError> {
        let mut url = self.base_uri.join(&request.operation).map_err(|error| {
            CollectionError::Infrastructure(format!(
                "invalid data.go.kr service API operation URL: {error}"
            ))
        })?;
        let mut query = url.query_pairs_mut();
        query.append_pair("serviceKey", &self.service_key);
        for (name, value) in &request.query_params {
            query.append_pair(name, value);
        }
        if let Some(format_query_param) = &request.format_query_param {
            query.append_pair(&format_query_param.name, &format_query_param.value);
        }
        query
            .append_pair("pageNo", &request.page_no.to_string())
            .append_pair("numOfRows", &request.num_of_rows.to_string());
        drop(query);
        Ok(url)
    }
}

fn validate_success_envelope(payload: &JsonValue) -> Result<(), CollectionError> {
    let result_code = payload
        .pointer("/response/header/resultCode")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            CollectionError::Infrastructure(
                "data.go.kr service API response omitted response.header.resultCode".to_owned(),
            )
        })?;
    if matches!(result_code, "00" | "000") {
        return Ok(());
    }

    let result_msg = payload
        .pointer("/response/header/resultMsg")
        .and_then(JsonValue::as_str)
        .unwrap_or("<missing>");
    Err(CollectionError::Infrastructure(format!(
        "data.go.kr service API request failed with resultCode={result_code} resultMsg={result_msg}"
    )))
}
