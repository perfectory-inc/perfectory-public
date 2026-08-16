//! HTTP client for the ILIS (industryland.or.kr) industrial-complex JSON endpoints.
//!
//! The two endpoints this lane needs (`/il/danji/list.do`, `/il/danref/list.do`) are `POST`
//! endpoints that take a JSON body carrying the page number and the page size, and return the whole
//! result set in one response when the page size covers it. That single-bulk shape is deliberate:
//! walking 148 pages to assemble the same 1,473 rows is 148 requests at a provider we are a guest
//! of, and this client is built so a caller cannot accidentally do that — one call is one page, and
//! the caller owns the page budget.
//!
//! Unlike the data.go.kr lanes this client is configured with `max_attempts = 1` by default (see
//! [`IlisRequestPolicy`]): a retry is another request at the provider, and a lane whose whole
//! collection is two requests must not silently turn into six.

use std::time::Duration;

use collection_domain::CollectionError;
use outbound_http_infrastructure::{
    classify_response, execute_retryable, redact_transport_error, shared_http_client, AttemptError,
    OutboundHttpError, RequestCircuitBreaker, RequestCircuitBreakerPolicy, ResilienceAudit,
    ResilienceCtx, ResiliencePolicy, RetryDecision,
};
use serde_json::{json, Value as JsonValue};

/// Provider label shared by the circuit breaker, audit events, and error messages.
const PROVIDER: &str = "industryland.or.kr";

/// Request policy for the ILIS lane.
///
/// `max_attempts` is one on purpose. Every attempt is a request the provider serves, and the whole
/// point of the bulk page size is to keep the total in the single digits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IlisRequestPolicy {
    /// Maximum time allowed to establish the connection.
    pub connect_timeout: Duration,
    /// Maximum idle time allowed while reading response bytes.
    pub read_timeout: Duration,
    /// Whole-request timeout. The bulk responses are megabytes, so this is generous.
    pub total_timeout: Duration,
    /// Maximum number of attempts per logical request, including the first.
    pub max_attempts: u32,
}

impl Default for IlisRequestPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_mins(1),
            total_timeout: Duration::from_mins(3),
            max_attempts: 1,
        }
    }
}

impl IlisRequestPolicy {
    const fn resilience_policy(self) -> ResiliencePolicy {
        ResiliencePolicy {
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            total_timeout: Some(self.total_timeout),
            max_attempts: self.max_attempts,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(8),
            jitter: false,
            circuit_breaker: RequestCircuitBreakerPolicy::DEFAULT,
        }
    }
}

/// Configuration for an ILIS industrial-complex API client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IlisIndustrialComplexApiConfig {
    /// Base URI, usually `https://www.industryland.or.kr`.
    pub base_uri: String,
    /// User-Agent header. It must identify us to the provider.
    pub user_agent: String,
}

/// One raw ILIS page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IlisIndustrialComplexPage {
    /// Raw response body bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed response body, used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// `reqwest` backed client for the ILIS industrial-complex JSON endpoints.
#[derive(Clone, Debug)]
pub struct IlisIndustrialComplexApiClient {
    base_uri: reqwest::Url,
    user_agent: String,
    http: reqwest::Client,
    resilience: ResiliencePolicy,
    circuit_breaker: RequestCircuitBreaker,
    audit: ResilienceAudit,
}

impl IlisIndustrialComplexApiClient {
    /// Creates an ILIS API client from explicit configuration.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when the base URI is invalid, the user agent is blank, or the
    /// HTTP client cannot be built.
    pub fn new(config: &IlisIndustrialComplexApiConfig) -> Result<Self, CollectionError> {
        Self::new_with_policy(config, IlisRequestPolicy::default())
    }

    /// Creates an ILIS API client from explicit configuration and request policy.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when the base URI is invalid, the user agent is blank, the
    /// policy is invalid, or the HTTP client cannot be built.
    pub fn new_with_policy(
        config: &IlisIndustrialComplexApiConfig,
        policy: IlisRequestPolicy,
    ) -> Result<Self, CollectionError> {
        let base_uri_raw = format!("{}/", config.base_uri.trim().trim_end_matches('/'));
        let base_uri = reqwest::Url::parse(&base_uri_raw).map_err(|error| {
            CollectionError::Infrastructure(format!("invalid {PROVIDER} base URI: {error}"))
        })?;
        let user_agent = config.user_agent.trim().to_owned();
        if user_agent.is_empty() {
            return Err(CollectionError::Infrastructure(format!(
                "{PROVIDER} user_agent is required"
            )));
        }
        let resilience = policy.resilience_policy();
        resilience
            .validate()
            .map_err(crate::outbound_http_error::into_collection_error)?;
        let http = shared_http_client(PROVIDER, &resilience)
            .map_err(crate::outbound_http_error::into_collection_error)?;
        Ok(Self {
            base_uri,
            user_agent,
            http,
            resilience,
            circuit_breaker: RequestCircuitBreaker::new(
                PROVIDER,
                RequestCircuitBreakerPolicy::DEFAULT,
            ),
            audit: ResilienceAudit::new(PROVIDER),
        })
    }

    /// Fetches one page from an ILIS list endpoint.
    ///
    /// `endpoint_path` is provider-native, for example `il/danji/list.do`. `page_size` is the whole
    /// point of this lane: one bulk page instead of a page walk.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when `page_no`/`page_size` are zero, the endpoint path is not a
    /// valid relative path, the request fails, the provider answers with a non-success status, or
    /// the body is not JSON.
    pub async fn fetch_page(
        &self,
        endpoint_path: &str,
        page_no: u32,
        page_size: u32,
    ) -> Result<IlisIndustrialComplexPage, CollectionError> {
        if page_no == 0 || page_size == 0 {
            return Err(CollectionError::Infrastructure(format!(
                "{PROVIDER} pageNo and pageSize must be greater than zero"
            )));
        }
        let url = self
            .base_uri
            .join(endpoint_path.trim_start_matches('/'))
            .map_err(|error| {
                CollectionError::Infrastructure(format!("invalid {PROVIDER} endpoint: {error}"))
            })?;
        let body = json!({ "pageNo": page_no, "pageSize": page_size });

        let ctx = ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &self.resilience,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_page_once(&url, &body))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    /// Fetches one complex's detail record.
    ///
    /// The bulk list is the collection path; this is the per-complex fallback for the handful the
    /// list does not carry, and it costs one request each — which is why the caller names the exact
    /// codes rather than the client deciding.
    ///
    /// # Errors
    /// Returns [`CollectionError`] when the complex code is blank or not path-safe, the endpoint
    /// path is invalid, the request fails, the provider answers with a non-success status, or the
    /// body is not JSON.
    pub async fn fetch_detail(
        &self,
        endpoint_path: &str,
        official_complex_code: &str,
    ) -> Result<IlisIndustrialComplexPage, CollectionError> {
        let code = official_complex_code.trim();
        if code.is_empty() || !code.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(CollectionError::Infrastructure(format!(
                "{PROVIDER} detail complex code must be ASCII alphanumeric: {code:?}"
            )));
        }
        let url = self
            .base_uri
            .join(&format!(
                "{}/{code}",
                endpoint_path.trim_start_matches('/').trim_end_matches('/')
            ))
            .map_err(|error| {
                CollectionError::Infrastructure(format!("invalid {PROVIDER} endpoint: {error}"))
            })?;
        let ctx = ResilienceCtx {
            breaker: Some(&self.circuit_breaker),
            policy: &self.resilience,
            audit: &self.audit,
        };
        execute_retryable(&ctx, || self.fetch_once(&url, None))
            .await
            .map_err(crate::outbound_http_error::into_collection_error)
    }

    async fn fetch_page_once(
        &self,
        url: &reqwest::Url,
        body: &JsonValue,
    ) -> Result<IlisIndustrialComplexPage, AttemptError> {
        self.fetch_once(url, Some(body)).await
    }

    async fn fetch_once(
        &self,
        url: &reqwest::Url,
        body: Option<&JsonValue>,
    ) -> Result<IlisIndustrialComplexPage, AttemptError> {
        // A body means the bulk endpoints (POST with the page envelope); no body means the
        // per-complex detail endpoint (GET with the code in the path).
        let request = body.map_or_else(
            || self.http.get(url.clone()),
            |body| self.http.post(url.clone()).json(body),
        );
        let response = request
            .header("user-agent", &self.user_agent)
            .header("accept", "application/json")
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
                RetryDecision::NotRetryable => AttemptError::Fatal(OutboundHttpError::new(message)),
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
            })?
            .to_vec();
        let payload = serde_json::from_slice::<JsonValue>(&raw_payload).map_err(|error| {
            OutboundHttpError::new(format!("{PROVIDER} response JSON parse failed: {error}"))
        })?;
        Ok(IlisIndustrialComplexPage {
            raw_payload,
            payload,
        })
    }
}
