//! Shared external-call resilience core (single SSOT policy + retry/backoff primitive).
//!
//! Consolidates the duplicated retry/backoff/timeout policy previously copy-pasted across the
//! data.go.kr and V-World HTTP clients. [`ResiliencePolicy`] and this module's tests are the
//! executable contract.

use crate::OutboundHttpError;
use reqwest::StatusCode;
use std::future::Future;
use std::time::Duration;

use crate::request_circuit_breaker::{RequestCircuitBreaker, RequestCircuitBreakerPolicy};

/// Single source of truth for one provider's external-call resilience policy.
///
/// Instantiated as provider `const`s (`DATA_GO_KR`, `VWORLD_JSON`, `VWORLD_FILE`, `HUB`,
/// `ICEBERG`);
/// existing env vars override only where they already exist (data.go.kr / V-World JSON).
/// `total_timeout = None` marks a streaming client (no whole-response timeout; connect +
/// read only). See spec §3.1.
#[derive(Clone, Copy, Debug)]
pub struct ResiliencePolicy {
    /// Maximum time allowed to establish the connection.
    pub connect_timeout: Duration,
    /// Maximum idle time allowed while reading response bytes.
    pub read_timeout: Duration,
    /// Optional whole-request timeout; omitted for long-lived streaming transfers.
    pub total_timeout: Option<Duration>,
    /// Maximum number of attempts, including the first request.
    pub max_attempts: u32,
    /// Backoff before the first retry.
    pub initial_backoff: Duration,
    /// Maximum locally computed backoff.
    pub max_backoff: Duration,
    /// Whether to apply full jitter to locally computed backoff.
    pub jitter: bool,
    /// Circuit-breaker policy used by callers that retain shared breaker state.
    pub circuit_breaker: RequestCircuitBreakerPolicy,
}

impl ResiliencePolicy {
    /// Validates invariants shared with the legacy policy constructors.
    ///
    /// # Errors
    /// Returns [`OutboundHttpError`] when `max_attempts` is zero, any timeout is zero, or
    /// `max_backoff < initial_backoff`.
    pub fn validate(&self) -> Result<(), OutboundHttpError> {
        if self.max_attempts == 0 {
            return Err(OutboundHttpError::new(
                "resilience policy max_attempts must be greater than zero".to_owned(),
            ));
        }
        if self.connect_timeout.is_zero() || self.read_timeout.is_zero() {
            return Err(OutboundHttpError::new(
                "resilience policy connect/read timeout must be greater than zero".to_owned(),
            ));
        }
        if self.total_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(OutboundHttpError::new(
                "resilience policy total_timeout must be greater than zero when set".to_owned(),
            ));
        }
        if self.max_backoff < self.initial_backoff {
            return Err(OutboundHttpError::new(
                "resilience policy max_backoff must be >= initial_backoff".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Resilience policy for data.go.kr JSON service APIs (spec §3.1 provider `const` SSOT).
///
/// Values preserve the legacy `DataGoKrRequestPolicy` defaults (3 attempts, 30s per-attempt
/// timeout, 250ms..5s exponential backoff, default breaker). The legacy client bounded the
/// whole attempt with one 30s `.timeout()`, so connect/read carry the same bound here —
/// identical effective behavior. Full jitter is newly applied per spec §5 (thundering-herd
/// avoidance); it only de-correlates delays already capped by `max_backoff`.
pub const DATA_GO_KR: ResiliencePolicy = ResiliencePolicy {
    connect_timeout: Duration::from_secs(30),
    read_timeout: Duration::from_secs(30),
    total_timeout: Some(Duration::from_secs(30)),
    max_attempts: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(5),
    jitter: true,
    circuit_breaker: RequestCircuitBreakerPolicy::DEFAULT,
};

/// Resilience policy for V-World JSON APIs (NED attribute, 2D Data API).
///
/// Values preserve the legacy `VWorldRequestPolicy` defaults (3 attempts, 30s per-attempt
/// timeout, 250ms..5s exponential backoff, default breaker) — the same envelope as
/// `DATA_GO_KR`, kept as a distinct const because the providers tune independently. The
/// legacy client bounded the whole attempt with one 30s `.timeout()`, so connect/read carry
/// the same bound. Full jitter is newly applied per spec §5.
pub const VWORLD_JSON: ResiliencePolicy = ResiliencePolicy {
    connect_timeout: Duration::from_secs(30),
    read_timeout: Duration::from_secs(30),
    total_timeout: Some(Duration::from_secs(30)),
    max_attempts: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(5),
    jitter: true,
    circuit_breaker: RequestCircuitBreakerPolicy::DEFAULT,
};

/// Resilience policy for V-World provider dataset endpoints (file downloads, session login).
///
/// `total_timeout: None` — file downloads stream multi-hundred-MB bodies, so only the connect
/// and idle (`read_timeout`) bounds apply (spec §3.1). These clients were previously
/// unprotected (no timeouts, no retry), so these values add protection rather than preserve
/// legacy timing. Const-only by design: newly protected clients get no env overrides (design
/// debate 5R). The circuit breaker is not applied on these paths — ingest creates a client per
/// job, so breaker state would be job-local and cosmetic (spec §8).
pub const VWORLD_FILE: ResiliencePolicy = ResiliencePolicy {
    connect_timeout: Duration::from_secs(10),
    read_timeout: Duration::from_mins(1),
    total_timeout: None,
    max_attempts: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(5),
    jitter: true,
    circuit_breaker: RequestCircuitBreakerPolicy::DEFAULT,
};

/// Resilience policy for the Iceberg REST catalog (R2 Data Catalog or any standard REST
/// catalog).
///
/// JSON metadata endpoints, so a whole-request `total_timeout` applies. This adapter was
/// previously unprotected (default `reqwest` client, no retry); values add protection rather
/// than preserve legacy timing. Const-only (no env overrides, design debate 5R).
pub const ICEBERG: ResiliencePolicy = ResiliencePolicy {
    connect_timeout: Duration::from_secs(10),
    read_timeout: Duration::from_secs(30),
    total_timeout: Some(Duration::from_secs(30)),
    max_attempts: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(5),
    jitter: true,
    circuit_breaker: RequestCircuitBreakerPolicy::DEFAULT,
};

/// Resilience policy for `hub.go.kr` bulk-file endpoints (streaming downloads, inventory).
///
/// Same streaming shape as `VWORLD_FILE` (`total_timeout: None`, connect + idle bounds only)
/// kept as a distinct const because the providers tune independently. This client was
/// previously unprotected; values add protection rather than preserve legacy timing.
/// Const-only (no env overrides, design debate 5R); no circuit breaker on this path —
/// ingest creates a client per job, so breaker state would be cosmetic (spec §8).
pub const HUB: ResiliencePolicy = ResiliencePolicy {
    connect_timeout: Duration::from_secs(10),
    read_timeout: Duration::from_mins(1),
    total_timeout: None,
    max_attempts: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(5),
    jitter: true,
    circuit_breaker: RequestCircuitBreakerPolicy::DEFAULT,
};

/// Whether a transport-level outcome should be retried, with an optional server-directed delay.
///
/// Shared "retry vocabulary": both the transport classifier and per-client body-envelope checks
/// return this single type, so the retry executor consumes one vocabulary regardless of source
/// (transport vs body). See spec §3.3.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// Retry is allowed; `retry_after` carries a `Retry-After`-derived delay when present.
    Retryable {
        /// Optional provider-directed minimum delay before retrying.
        retry_after: Option<Duration>,
    },
    /// Do not retry (client error, success, or a fatal condition).
    NotRetryable,
}

/// Query parameter names that are known to be non-secret and safe to keep in cleartext in an
/// error message, log, or audit artifact.
///
/// This is an ALLOWLIST: redaction keeps these visible and redacts the value of *every other*
/// query parameter. Safe-by-default — a new or unknown credential parameter (whatever its name)
/// is redacted automatically, instead of leaking until someone remembers to add it to a denylist.
/// The list holds only structural pagination / format / dataset-selector parameters observed in
/// the provider clients; anything genuinely secret (`serviceKey`, `key`, `authkey`, tokens, …) is
/// deliberately absent and therefore redacted.
const NON_SECRET_QUERY_PARAMS: &[&str] = &[
    // pagination / sizing
    "pageno",
    "numofrows",
    "page",
    "perpage",
    "size",
    "datpageindex",
    "datpagesize",
    // response shape / format
    "returntype",
    "_type",
    "type",
    "format",
    "service",
    "request",
    "geometry",
    "attribute",
    "crs",
    "columns",
    // dataset / partition selectors (identifiers, not credentials)
    "data",
    "attrfilter",
    "domain",
    "svccde",
    "dsid",
    "ds_id",
    "ds_file_sq",
    "fileno",
    "lawd_cd",
    "deal_ymd",
    "sigungucd",
    "bjdongcd",
    "operation",
    "warehouse",
];

/// Redacts the value of every query parameter NOT on the [`NON_SECRET_QUERY_PARAMS`] allowlist,
/// anywhere in `text`.
///
/// `reqwest::Error`'s `Display` appends the full request URL (`... for url
/// (https://host/op?serviceKey=ABC&pageNo=1)`), and the provider clients embed credentials as
/// query parameters, so a raw transport error string carries the secret value. An allowlist makes
/// redaction safe-by-default: known structural parameters stay readable for debugging while any
/// other parameter value — including an unknown credential parameter — becomes `<name>=[redacted]`.
/// A parameter is only matched at a query boundary (`?` or `&`) so substrings in ordinary prose
/// are left untouched.
#[must_use]
pub fn redact_url_query_secrets(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        if index == 0 || matches!(bytes[index - 1], b'?' | b'&') {
            if let Some((name, value_start)) = query_param_at(bytes, index) {
                out.push_str(name);
                out.push('=');
                let safe = NON_SECRET_QUERY_PARAMS
                    .iter()
                    .any(|allowed| name.eq_ignore_ascii_case(allowed));
                // Find the end of the value (next query/URL/whitespace delimiter).
                let mut end = value_start;
                while end < bytes.len()
                    && !matches!(
                        bytes[end],
                        b'&' | b')' | b' ' | b'"' | b'\'' | b'\n' | b'\r' | b'\t'
                    )
                {
                    end += 1;
                }
                if safe {
                    out.push_str(&text[value_start..end]);
                } else {
                    out.push_str("[redacted]");
                }
                index = end;
                continue;
            }
        }
        if let Some(ch) = text[index..].chars().next() {
            // `index` is always on a UTF-8 boundary here, so decode one full char.
            out.push(ch);
            index += ch.len_utf8();
        } else {
            break;
        }
    }
    out
}

/// If `bytes[index..]` starts with an ASCII `name=` query parameter, returns the parameter name
/// (borrowed from `text`) and the byte offset where its value begins.
fn query_param_at(bytes: &[u8], index: usize) -> Option<(&str, usize)> {
    let mut cursor = index;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'=' => {
                if cursor == index {
                    return None;
                }
                let name = std::str::from_utf8(&bytes[index..cursor]).ok()?;
                return Some((name, cursor + 1));
            }
            // A query parameter name is ASCII without these delimiters; bail on anything else
            // (including multi-byte UTF-8) so we never slice across a char boundary.
            b'&' | b'?' | b')' | b' ' | b'"' | b'\'' | b'\n' | b'\r' | b'\t' => return None,
            byte if byte.is_ascii() => cursor += 1,
            _ => return None,
        }
    }
    None
}

/// Formats a transport error into a credential-safe string.
///
/// Always use this instead of `format!("{error}")` for `reqwest::Error`s, because their `Display`
/// embeds the request URL — which the provider clients build with credential query parameters.
#[must_use]
pub fn redact_transport_error(error: &reqwest::Error) -> String {
    redact_url_query_secrets(&error.to_string())
}

/// Classifies an HTTP status into a [`RetryDecision`] at the transport layer.
///
/// Preserves the existing `is_retryable_status` set (`data_go_kr_service_api.rs`):
/// 408, 429, 500, 502, 503, 504 are retryable; everything else is not. `Retry-After` parsing is
/// layered on separately (the header is not available from a bare status).
#[must_use]
pub fn classify_status(status: StatusCode) -> RetryDecision {
    if matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    ) {
        RetryDecision::Retryable { retry_after: None }
    } else {
        RetryDecision::NotRetryable
    }
}

/// One attempt's failure, in the shared retry vocabulary the executors consume.
///
/// Client code builds this from either the transport classifier ([`classify_status`]) or its
/// own provider body-envelope judgment — both speak [`RetryDecision`] (spec §3.3); this enum
/// binds that decision to the failure it describes. `message` strings stay out of audit
/// telemetry (they may embed request URLs with credentials) and surface only via the returned
/// `OutboundHttpError`.
#[derive(Debug)]
pub enum AttemptError {
    /// Transient failure; the executor may retry with backoff.
    Retryable {
        /// Human-readable failure description, included in the final error after exhaustion.
        message: String,
        /// Server-directed minimum delay (`Retry-After`), honored by [`retry_with_backoff`].
        retry_after: Option<Duration>,
    },
    /// Non-retryable failure; surfaced to the caller as-is without further attempts.
    Fatal(OutboundHttpError),
}

impl From<OutboundHttpError> for AttemptError {
    fn from(error: OutboundHttpError) -> Self {
        Self::Fatal(error)
    }
}

/// Per-call binding of a provider's resilience pieces (spec §3.5).
///
/// Combines the value-typed [`ResiliencePolicy`] (`const` per provider) with the stateful,
/// shared [`RequestCircuitBreaker`] and the [`ResilienceAudit`] sink at call time.
///
/// `breaker` is `None` for streaming/per-request clients: their breaker state would be
/// client/job-local (a fresh client per file download) and therefore cosmetic, so slice-1
/// does not apply or claim circuit breaking on those paths (spec §8).
pub struct ResilienceCtx<'a> {
    /// Shared circuit breaker, or `None` on streaming paths (breaker scope = client/job-local).
    pub breaker: Option<&'a RequestCircuitBreaker>,
    /// The provider's resilience policy (validated at client construction).
    pub policy: &'a ResiliencePolicy,
    /// Audit sink notified on every attempt outcome.
    pub audit: &'a ResilienceAudit,
}

/// Executes an idempotent operation with retry, backoff, and circuit breaking (spec §3.5).
///
/// The whole `op` (request + body consumption + validation) is one attempt; any
/// [`AttemptError::Retryable`] outcome re-runs it after [`retry_with_backoff`] until the
/// policy's `max_attempts` is spent.
///
/// # Errors
/// Returns the fatal error as-is, an `Infrastructure` error after retry exhaustion, or the
/// circuit-breaker rejection when the circuit is open.
pub async fn execute_retryable<T, F, Fut>(
    ctx: &ResilienceCtx<'_>,
    op: F,
) -> Result<T, OutboundHttpError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
{
    before_request(ctx)?;
    let mut attempt: u32 = 1;
    loop {
        match op().await {
            Ok(value) => {
                record_success(ctx, attempt)?;
                return Ok(value);
            }
            Err(AttemptError::Fatal(error)) => {
                ctx.audit.record(ResilienceEvent::FatalFailure { attempt });
                return Err(error);
            }
            Err(AttemptError::Retryable {
                message,
                retry_after,
            }) => {
                if attempt >= ctx.policy.max_attempts {
                    return Err(record_exhaustion(ctx, attempt, &message)?);
                }
                let delay = retry_with_backoff(ctx.policy, attempt, retry_after);
                ctx.audit
                    .record(ResilienceEvent::RetryScheduled { attempt, delay });
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

/// Executes a streaming download handshake with retry up to the *validated* `Response`.
///
/// `send_and_validate` must perform the send plus header-level validation (status,
/// HTML-guard, `Content-Length`) and return the unconsumed [`reqwest::Response`]. Opening
/// `bytes_stream()` is the caller's responsibility, outside this loop — once the stream
/// starts there are no retries (spec §3.5).
///
/// # Errors
/// Same contract as [`execute_retryable`].
pub async fn execute_streaming_handshake<F, Fut>(
    ctx: &ResilienceCtx<'_>,
    send_and_validate: F,
) -> Result<reqwest::Response, OutboundHttpError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<reqwest::Response, AttemptError>>,
{
    // Identical loop to `execute_retryable`; the distinct entry point encodes the streaming
    // contract in its signature (returns an unconsumed `Response`, body streaming is the
    // caller's, post-loop responsibility).
    execute_retryable(ctx, send_and_validate).await
}

/// Executes a non-idempotent operation (e.g. a login POST) exactly once — no retries.
///
/// A retryable-class failure is still *reported* through the shared vocabulary (audit +
/// error message) but never re-attempted, because replaying a non-idempotent write is unsafe
/// (spec §3.2).
///
/// # Errors
/// Returns the fatal error as-is, an `Infrastructure` error for a transient failure (not
/// retried), or the circuit-breaker rejection when the circuit is open.
pub async fn execute_single<T, F, Fut>(
    ctx: &ResilienceCtx<'_>,
    op: F,
) -> Result<T, OutboundHttpError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
{
    before_request(ctx)?;
    match op().await {
        Ok(value) => {
            record_success(ctx, 1)?;
            Ok(value)
        }
        Err(AttemptError::Fatal(error)) => {
            ctx.audit
                .record(ResilienceEvent::FatalFailure { attempt: 1 });
            Err(error)
        }
        Err(AttemptError::Retryable { message, .. }) => {
            // Retryable-class failure, but the budget for a non-idempotent op is one attempt.
            if let Some(breaker) = ctx.breaker {
                breaker.record_retryable_failure()?;
            }
            ctx.audit
                .record(ResilienceEvent::AttemptsExhausted { attempts: 1 });
            Err(OutboundHttpError::new(format!(
                "{} request failed: {message}",
                ctx.audit.provider
            )))
        }
    }
}

/// Builds the provider HTTP client from one [`ResiliencePolicy`] (spec §3.6).
///
/// Always applies `connect_timeout` and `read_timeout` (idle time between body chunks —
/// streaming-safe). Applies a whole-request `.timeout()` only when `total_timeout` is `Some`
/// (JSON policies); streaming policies (`None`) must never bound the full transfer.
///
/// # Errors
/// Returns [`OutboundHttpError`] when the underlying client cannot be constructed.
pub fn shared_http_client(
    provider: &'static str,
    policy: &ResiliencePolicy,
) -> Result<reqwest::Client, OutboundHttpError> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(policy.connect_timeout)
        .read_timeout(policy.read_timeout);
    if let Some(total_timeout) = policy.total_timeout {
        builder = builder.timeout(total_timeout);
    }
    builder.build().map_err(|error| {
        OutboundHttpError::new(format!("failed to build {provider} HTTP client: {error}"))
    })
}

/// Applies the circuit-breaker admission check (when a breaker is in scope) and audits
/// rejections.
fn before_request(ctx: &ResilienceCtx<'_>) -> Result<(), OutboundHttpError> {
    let Some(breaker) = ctx.breaker else {
        return Ok(());
    };
    breaker.before_request().inspect_err(|_| {
        ctx.audit.record(ResilienceEvent::CircuitRejected);
    })
}

/// Resets the breaker (when in scope) and audits the successful attempt.
fn record_success(ctx: &ResilienceCtx<'_>, attempt: u32) -> Result<(), OutboundHttpError> {
    if let Some(breaker) = ctx.breaker {
        breaker.record_success()?;
    }
    ctx.audit
        .record(ResilienceEvent::AttemptSucceeded { attempt });
    Ok(())
}

/// Feeds the breaker (when in scope), audits exhaustion, and builds the final error.
///
/// Returns `Ok(final_error)` so breaker mutex poisoning can preempt it via `?`, matching the
/// legacy client loops' error precedence.
fn record_exhaustion(
    ctx: &ResilienceCtx<'_>,
    attempts: u32,
    message: &str,
) -> Result<OutboundHttpError, OutboundHttpError> {
    if let Some(breaker) = ctx.breaker {
        breaker.record_retryable_failure()?;
    }
    ctx.audit
        .record(ResilienceEvent::AttemptsExhausted { attempts });
    Ok(OutboundHttpError::new(format!(
        "{} request failed after {attempts} attempts: {message}",
        ctx.audit.provider
    )))
}

/// One observable moment in an external call's resilience lifecycle.
///
/// Carries only structured, PII-free fields (attempt counters, delays); the provider label is
/// supplied by the [`ResilienceAudit`] that records the event. Failure *messages* are
/// deliberately excluded: transport errors can embed full request URLs (including credential
/// query params), so they flow only through the returned `OutboundHttpError`, never through audit
/// telemetry.
#[derive(Clone, Copy, Debug)]
pub enum ResilienceEvent {
    /// An attempt succeeded (possibly after retries).
    AttemptSucceeded {
        /// 1-based attempt number that succeeded.
        attempt: u32,
    },
    /// A retryable failure occurred and a retry is scheduled after `delay`.
    RetryScheduled {
        /// 1-based attempt number that failed.
        attempt: u32,
        /// Backoff delay before the next attempt.
        delay: Duration,
    },
    /// The retry budget is exhausted; the call fails as retryable-but-spent.
    AttemptsExhausted {
        /// Total attempts made (== policy `max_attempts`; 1 for non-idempotent ops).
        attempts: u32,
    },
    /// A non-retryable failure ended the call immediately.
    FatalFailure {
        /// 1-based attempt number that failed fatally.
        attempt: u32,
    },
    /// The circuit breaker rejected the call before any attempt was made.
    CircuitRejected,
}

/// Per-provider audit sink for resilience lifecycle events (spec §3.7).
///
/// Deliberately a concrete struct, not a trait (design debate 4R): the slice-1 implementation
/// emits structured `tracing` events only; the follow-up `RequestLedger` adds a ledger sink
/// *inside* this struct without changing any call site.
#[derive(Clone, Copy, Debug)]
pub struct ResilienceAudit {
    /// Provider label stamped onto every recorded event; also used by the executors for
    /// final error messages.
    pub provider: &'static str,
}

impl ResilienceAudit {
    /// Creates the slice-1 audit sink (structured `tracing` only) for one provider.
    #[must_use]
    pub const fn new(provider: &'static str) -> Self {
        Self { provider }
    }

    /// Records one resilience lifecycle event as a structured, PII-free `tracing` event.
    pub fn record(&self, event: ResilienceEvent) {
        match event {
            ResilienceEvent::AttemptSucceeded { attempt } => {
                tracing::debug!(
                    target: "request_resilience",
                    provider = self.provider,
                    attempt,
                    outcome = "success",
                    "external call attempt succeeded"
                );
            }
            ResilienceEvent::RetryScheduled { attempt, delay } => {
                tracing::warn!(
                    target: "request_resilience",
                    provider = self.provider,
                    attempt,
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    outcome = "retry_scheduled",
                    "external call attempt failed; retry scheduled"
                );
            }
            ResilienceEvent::AttemptsExhausted { attempts } => {
                tracing::warn!(
                    target: "request_resilience",
                    provider = self.provider,
                    attempts,
                    outcome = "exhausted",
                    "external call retry budget exhausted"
                );
            }
            ResilienceEvent::FatalFailure { attempt } => {
                tracing::warn!(
                    target: "request_resilience",
                    provider = self.provider,
                    attempt,
                    outcome = "fatal",
                    "external call failed with a non-retryable error"
                );
            }
            ResilienceEvent::CircuitRejected => {
                tracing::warn!(
                    target: "request_resilience",
                    provider = self.provider,
                    outcome = "circuit_rejected",
                    "external call rejected by open circuit breaker"
                );
            }
        }
    }
}

/// Classifies a transport-level HTTP outcome (status + headers) into a [`RetryDecision`].
///
/// Layers `Retry-After` parsing over [`classify_status`]: when the status is retryable and the
/// provider sent `Retry-After` in delta-seconds form, the decision carries that server-directed
/// delay. The HTTP-date form is ignored (no date parsing dependency; absent hint just falls
/// back to our own backoff curve).
#[must_use]
pub fn classify_response(
    status: StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> RetryDecision {
    match classify_status(status) {
        RetryDecision::Retryable { .. } => RetryDecision::Retryable {
            retry_after: retry_after_hint(headers),
        },
        RetryDecision::NotRetryable => RetryDecision::NotRetryable,
    }
}

/// Parses a delta-seconds `Retry-After` header value, if present and well-formed.
fn retry_after_hint(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// Applies full jitter: a uniform delay in `[0, base]`.
///
/// Uses process-seeded std entropy (`RandomState`) — sufficient to de-correlate retry timing
/// across clients (thundering-herd avoidance); cryptographic randomness is not required and no
/// extra dependency is taken (dep-0 per the design debate).
fn apply_full_jitter(base: Duration) -> Duration {
    use std::hash::{BuildHasher, RandomState};

    let base_nanos = u64::try_from(base.as_nanos()).unwrap_or(u64::MAX);
    if base_nanos == 0 {
        return Duration::ZERO;
    }
    // A freshly-seeded RandomState yields a different value per call.
    let entropy = RandomState::new().hash_one(());
    // Saturated bases keep the full entropy range (off by one nanosecond at ~584 years —
    // irrelevant) instead of overflowing the `+ 1` divisor.
    if base_nanos == u64::MAX {
        return Duration::from_nanos(entropy);
    }
    Duration::from_nanos(entropy % (base_nanos + 1))
}

/// Computes the delay before the next retry attempt (the `retry_with_backoff` primitive,
/// spec §3.4).
///
/// Exponential [`base_backoff`] with full jitter when `policy.jitter` is set; a server-directed
/// `Retry-After` delay is honored as a lower bound (the result is never earlier than either
/// constraint). Server-directed delays are trusted as-is, even beyond `max_backoff` — the cap
/// applies to our own backoff curve, not to explicit provider throttle directives.
fn retry_with_backoff(
    policy: &ResiliencePolicy,
    attempt: u32,
    retry_after: Option<Duration>,
) -> Duration {
    let base = base_backoff(attempt, policy.initial_backoff, policy.max_backoff);
    let computed = if policy.jitter {
        apply_full_jitter(base)
    } else {
        base
    };
    retry_after.map_or(computed, |server_directed| server_directed.max(computed))
}

/// Exponential backoff delay for a 1-based attempt: `initial * 2^(attempt-1)`, capped at `max`.
///
/// Preserves the existing `DataGoKrRequestPolicy::backoff_for_attempt` behavior
/// (`data_go_kr_service_api.rs`) so migrated clients keep identical timing. Saturating math
/// avoids overflow/panic for large attempt counts.
fn base_backoff(attempt: u32, initial: Duration, max: Duration) -> Duration {
    let multiplier = 1_u32
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u32::MAX);
    initial.saturating_mul(multiplier).min(max)
}

#[cfg(test)]
mod tests;
