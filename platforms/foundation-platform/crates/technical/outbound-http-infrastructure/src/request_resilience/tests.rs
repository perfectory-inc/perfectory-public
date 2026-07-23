use super::{
    base_backoff, classify_status, execute_retryable, execute_single, execute_streaming_handshake,
    AttemptError, ResilienceAudit, ResilienceCtx, ResilienceEvent, ResiliencePolicy, RetryDecision,
};
use crate::request_circuit_breaker::{RequestCircuitBreaker, RequestCircuitBreakerPolicy};
use crate::OutboundHttpError;
use reqwest::StatusCode;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Minimal `tracing` subscriber that flattens each emitted event's fields into one string,
/// so tests can assert on structured audit output without extra dependencies.
#[derive(Clone, Default)]
struct CapturedEvents(Arc<Mutex<Vec<String>>>);

impl CapturedEvents {
    fn drain(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl tracing::Subscriber for CapturedEvents {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Flatten(String);
        impl tracing::field::Visit for Flatten {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, "{}={:?} ", field.name(), value);
            }
        }
        let mut flat = Flatten(String::new());
        event.record(&mut flat);
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(flat.0);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

fn sample_policy() -> ResiliencePolicy {
    ResiliencePolicy {
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(30),
        total_timeout: Some(Duration::from_secs(30)),
        max_attempts: 3,
        initial_backoff: Duration::from_millis(250),
        max_backoff: Duration::from_secs(5),
        jitter: true,
        circuit_breaker: RequestCircuitBreakerPolicy::default(),
    }
}

#[test]
fn validate_accepts_a_sane_policy() -> Result<(), Box<dyn std::error::Error>> {
    sample_policy().validate()?;
    Ok(())
}

#[test]
fn validate_rejects_zero_max_attempts() {
    let mut policy = sample_policy();
    policy.max_attempts = 0;
    assert!(policy.validate().is_err());
}

#[test]
fn validate_rejects_max_backoff_below_initial() {
    let mut policy = sample_policy();
    policy.max_backoff = Duration::from_millis(100);
    assert!(policy.validate().is_err());
}

#[test]
fn validate_rejects_zero_timeouts() {
    let mut policy = sample_policy();
    policy.connect_timeout = Duration::ZERO;
    assert!(policy.validate().is_err());
}

#[test]
fn jitter_stays_within_base_and_varies() {
    use super::apply_full_jitter;
    let base = Duration::from_secs(1);
    let mut seen = std::collections::HashSet::new();
    for _ in 0..64 {
        let delay = apply_full_jitter(base);
        assert!(delay <= base, "jitter must not exceed base");
        seen.insert(delay.as_nanos());
    }
    assert!(
        seen.len() > 1,
        "full jitter should de-correlate (vary across calls)"
    );
}

#[test]
fn jitter_of_zero_base_is_zero() {
    use super::apply_full_jitter;
    assert_eq!(apply_full_jitter(Duration::ZERO), Duration::ZERO);
}

#[test]
fn jitter_of_saturated_base_does_not_panic() {
    use super::apply_full_jitter;
    // A base beyond u64 nanoseconds saturates; the divisor math must not overflow.
    let delay = apply_full_jitter(Duration::MAX);
    assert!(delay <= Duration::MAX);
}

#[test]
fn redact_url_query_secrets_masks_data_go_kr_service_key_value() {
    use super::redact_url_query_secrets;
    let raw = "data.go.kr service API request failed: error sending request \
               for url (https://apis.data.go.kr/op?serviceKey=DECODED-SECRET-1234&pageNo=1&numOfRows=100)";
    let redacted = redact_url_query_secrets(raw);
    assert!(
        !redacted.contains("DECODED-SECRET-1234"),
        "service key value must not survive: {redacted}"
    );
    assert!(redacted.contains("serviceKey=[redacted]"));
    // Non-sensitive params and surrounding text are preserved.
    assert!(redacted.contains("pageNo=1"));
    assert!(redacted.contains("numOfRows=100"));
}

#[test]
fn redact_url_query_secrets_masks_vworld_key_value() {
    use super::redact_url_query_secrets;
    let raw =
        "for url (https://api.vworld.kr/req/data?key=VWORLD-OPENAPI-KEY&data=LP_PA_CBND_BUBUN)";
    let redacted = redact_url_query_secrets(raw);
    assert!(!redacted.contains("VWORLD-OPENAPI-KEY"), "got: {redacted}");
    assert!(redacted.contains("key=[redacted]"));
    assert!(redacted.contains("data=LP_PA_CBND_BUBUN"));
}

#[test]
fn redact_url_query_secrets_leaves_non_query_text_untouched() {
    use super::redact_url_query_secrets;
    // "key" as a bare word (not a query parameter) must not be altered, and Korean text
    // must round-trip without corruption.
    let raw = "the api key rotation 정책은 매월 적용됩니다";
    assert_eq!(redact_url_query_secrets(raw), raw);
}

#[test]
fn redact_url_query_secrets_masks_value_at_end_of_string() {
    use super::redact_url_query_secrets;
    let redacted = redact_url_query_secrets("https://host/op?serviceKey=TAIL-SECRET");
    assert_eq!(redacted, "https://host/op?serviceKey=[redacted]");
}

#[test]
fn redact_url_query_secrets_redacts_unknown_parameters_by_default() {
    use super::redact_url_query_secrets;
    // Allowlist semantics: a credential parameter whose name is NOT a known structural param is
    // redacted automatically — a denylist would have leaked it. Known params stay readable.
    let raw = "for url (https://host/op?authToken=UNKNOWN-SECRET&sessionKey=ALSO-SECRET&pageNo=2)";
    let redacted = redact_url_query_secrets(raw);
    assert!(!redacted.contains("UNKNOWN-SECRET"), "got: {redacted}");
    assert!(!redacted.contains("ALSO-SECRET"), "got: {redacted}");
    assert!(redacted.contains("authToken=[redacted]"));
    assert!(redacted.contains("sessionKey=[redacted]"));
    assert!(
        redacted.contains("pageNo=2"),
        "known param stays readable: {redacted}"
    );
}

#[test]
fn classify_status_marks_transient_as_retryable() {
    for status in [
        StatusCode::REQUEST_TIMEOUT,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
    ] {
        assert!(
            matches!(classify_status(status), RetryDecision::Retryable { .. }),
            "{status} should be retryable"
        );
    }
}

#[test]
fn classify_status_marks_client_and_success_as_not_retryable() {
    for status in [
        StatusCode::OK,
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::NOT_FOUND,
    ] {
        assert!(
            matches!(classify_status(status), RetryDecision::NotRetryable),
            "{status} should not be retryable"
        );
    }
}

#[test]
fn retry_with_backoff_without_jitter_matches_base_backoff() {
    use super::retry_with_backoff;
    let mut policy = sample_policy();
    policy.jitter = false;

    assert_eq!(
        retry_with_backoff(&policy, 1, None),
        Duration::from_millis(250)
    );
    assert_eq!(retry_with_backoff(&policy, 3, None), Duration::from_secs(1));
    // 2^5 * 250 = 8000 -> capped to max_backoff
    assert_eq!(retry_with_backoff(&policy, 6, None), Duration::from_secs(5));
}

#[test]
fn retry_with_backoff_with_jitter_stays_within_base() {
    use super::retry_with_backoff;
    let policy = sample_policy();
    for _ in 0..32 {
        let delay = retry_with_backoff(&policy, 3, None);
        assert!(
            delay <= Duration::from_secs(1),
            "jittered delay must stay within the un-jittered base"
        );
    }
}

#[test]
fn retry_with_backoff_honors_longer_server_retry_after() {
    use super::retry_with_backoff;
    let mut policy = sample_policy();
    policy.jitter = false;

    // Server-directed delay wins even beyond max_backoff (explicit throttle directive).
    assert_eq!(
        retry_with_backoff(&policy, 1, Some(Duration::from_secs(7))),
        Duration::from_secs(7)
    );
}

#[test]
fn retry_with_backoff_keeps_computed_backoff_when_retry_after_is_shorter() {
    use super::retry_with_backoff;
    let mut policy = sample_policy();
    policy.jitter = false;

    // Waiting the longer of the two satisfies both the server and our own curve.
    assert_eq!(
        retry_with_backoff(&policy, 3, Some(Duration::from_millis(10))),
        Duration::from_secs(1)
    );
}

/// Fast test policy: zero backoff, no jitter, so retry loops finish instantly.
fn fast_policy(max_attempts: u32) -> ResiliencePolicy {
    ResiliencePolicy {
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        total_timeout: Some(Duration::from_secs(5)),
        max_attempts,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
        jitter: false,
        circuit_breaker: RequestCircuitBreakerPolicy::default(),
    }
}

fn counting_op<F>(
    calls: &Arc<AtomicU32>,
    outcome_for_call: F,
) -> impl Fn() -> std::future::Ready<Result<u32, AttemptError>>
where
    F: Fn(u32) -> Result<u32, AttemptError>,
{
    let calls = Arc::clone(calls);
    move || {
        let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
        std::future::ready(outcome_for_call(call))
    }
}

fn retryable(message: &str) -> AttemptError {
    AttemptError::Retryable {
        message: message.to_owned(),
        retry_after: None,
    }
}

#[tokio::test]
async fn execute_retryable_returns_first_success_without_retry(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let breaker = RequestCircuitBreaker::new("test provider", policy.circuit_breaker);
    let ctx = ResilienceCtx {
        breaker: Some(&breaker),
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let result = execute_retryable(&ctx, counting_op(&calls, |_| Ok(7))).await?;

    assert_eq!(result, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn execute_retryable_retries_transient_failure_until_success(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let breaker = RequestCircuitBreaker::new("test provider", policy.circuit_breaker);
    let ctx = ResilienceCtx {
        breaker: Some(&breaker),
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let result = execute_retryable(
        &ctx,
        counting_op(&calls, |call| {
            if call < 3 {
                Err(retryable("boom"))
            } else {
                Ok(42)
            }
        }),
    )
    .await?;

    assert_eq!(result, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn execute_retryable_exhausts_attempts_then_breaker_opens(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(2);
    let audit = ResilienceAudit::new("test provider");
    // Default breaker policy: threshold 1, so one exhausted call opens the circuit.
    let breaker = RequestCircuitBreaker::new("test provider", policy.circuit_breaker);
    let ctx = ResilienceCtx {
        breaker: Some(&breaker),
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let error = execute_retryable(&ctx, counting_op(&calls, |_| Err(retryable("boom"))))
        .await
        .err()
        .ok_or("retry budget must exhaust")?;
    assert_eq!(
        error.to_string(),
        "test provider request failed after 2 attempts: boom"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let rejected = execute_retryable(&ctx, counting_op(&calls, |_| Ok(1)))
        .await
        .err()
        .ok_or("open circuit must reject before any attempt")?;
    assert!(
        rejected.to_string().contains("circuit breaker is open"),
        "unexpected rejection: {rejected}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "op must not run while open"
    );
    Ok(())
}

#[tokio::test]
async fn execute_retryable_does_not_retry_fatal_errors_or_trip_breaker(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let breaker = RequestCircuitBreaker::new("test provider", policy.circuit_breaker);
    let ctx = ResilienceCtx {
        breaker: Some(&breaker),
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let error = execute_retryable(
        &ctx,
        counting_op(&calls, |_| {
            Err(AttemptError::Fatal(OutboundHttpError::new(
                "fatal boom".to_owned(),
            )))
        }),
    )
    .await
    .err()
    .ok_or("fatal error must surface")?;
    assert_eq!(error.to_string(), "fatal boom");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "fatal must not retry");

    // The breaker must not have tripped: the next call still reaches the op.
    let result = execute_retryable(&ctx, counting_op(&calls, |_| Ok(5))).await?;
    assert_eq!(result, 5);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn execute_retryable_waits_at_least_server_retry_after(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(2);
    let audit = ResilienceAudit::new("test provider");
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let started = std::time::Instant::now();
    let result = execute_retryable(
        &ctx,
        counting_op(&calls, |call| {
            if call == 1 {
                Err(AttemptError::Retryable {
                    message: "throttled".to_owned(),
                    retry_after: Some(Duration::from_millis(30)),
                })
            } else {
                Ok(9)
            }
        }),
    )
    .await?;

    assert_eq!(result, 9);
    assert!(
        started.elapsed() >= Duration::from_millis(30),
        "server-directed Retry-After must be honored, got {:?}",
        started.elapsed()
    );
    Ok(())
}

#[tokio::test]
async fn execute_retryable_without_breaker_never_opens_circuit(
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(1);
    let audit = ResilienceAudit::new("test provider");
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let error = execute_retryable(&ctx, counting_op(&calls, |_| Err(retryable("boom"))))
        .await
        .err()
        .ok_or("single attempt budget must exhaust")?;
    assert!(error.to_string().contains("failed after 1 attempts"));

    // No breaker: the next call must still reach the op.
    let result = execute_retryable(&ctx, counting_op(&calls, |_| Ok(3))).await?;
    assert_eq!(result, 3);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn execute_retryable_records_audit_lifecycle_events() -> Result<(), Box<dyn std::error::Error>>
{
    let captured = CapturedEvents::default();
    let _guard = tracing::subscriber::set_default(captured.clone());

    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let result = execute_retryable(
        &ctx,
        counting_op(&calls, |call| {
            if call == 1 {
                Err(retryable("boom"))
            } else {
                Ok(1)
            }
        }),
    )
    .await?;
    assert_eq!(result, 1);

    let events = captured.drain();
    assert_eq!(events.len(), 2, "one retry + one success event: {events:?}");
    assert!(events[0].contains("outcome=\"retry_scheduled\""));
    assert!(events[1].contains("outcome=\"success\""));
    assert!(
        events[1].contains("attempt=2"),
        "success event must carry the succeeding attempt number: {}",
        events[1]
    );
    Ok(())
}

#[tokio::test]
async fn execute_single_returns_success() -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };

    let result = execute_single(&ctx, || std::future::ready(Ok::<_, AttemptError>(11))).await?;
    assert_eq!(result, 11);
    Ok(())
}

#[tokio::test]
async fn execute_single_never_retries_transient_failures() -> Result<(), Box<dyn std::error::Error>>
{
    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };
    let calls = Arc::new(AtomicU32::new(0));

    let error = execute_single(&ctx, counting_op(&calls, |_| Err(retryable("boom"))))
        .await
        .err()
        .ok_or("transient failure must not be retried")?;
    assert_eq!(error.to_string(), "test provider request failed: boom");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "non-idempotent op must run exactly once even though max_attempts is 3"
    );
    Ok(())
}

#[tokio::test]
async fn execute_single_passes_through_fatal_errors() -> Result<(), Box<dyn std::error::Error>> {
    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test provider");
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };

    let error = execute_single(&ctx, || {
        std::future::ready(Err::<u32, _>(AttemptError::Fatal(OutboundHttpError::new(
            "fatal boom".to_owned(),
        ))))
    })
    .await
    .err()
    .ok_or("fatal error must surface")?;
    assert_eq!(error.to_string(), "fatal boom");
    Ok(())
}

#[tokio::test]
async fn execute_streaming_handshake_retries_until_validated_response(
) -> Result<(), Box<dyn std::error::Error>> {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/file"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"chunk".to_vec()))
        .expect(1)
        .mount(&server)
        .await;

    let policy = fast_policy(3);
    let audit = ResilienceAudit::new("test streaming provider");
    // Streaming path: breaker is None by design (breaker scope = client/job-local, spec §8).
    let ctx = ResilienceCtx {
        breaker: None,
        policy: &policy,
        audit: &audit,
    };
    let http = reqwest::Client::new();
    let url = format!("{}/file", server.uri());

    let response = execute_streaming_handshake(&ctx, || {
        let http = http.clone();
        let url = url.clone();
        async move {
            let response =
                http.get(&url)
                    .send()
                    .await
                    .map_err(|error| AttemptError::Retryable {
                        message: format!("send failed: {error}"),
                        retry_after: None,
                    })?;
            match classify_status(response.status()) {
                RetryDecision::Retryable { retry_after } => Err(AttemptError::Retryable {
                    message: format!("HTTP {}", response.status()),
                    retry_after,
                }),
                RetryDecision::NotRetryable if response.status().is_success() => Ok(response),
                RetryDecision::NotRetryable => Err(AttemptError::Fatal(OutboundHttpError::new(
                    format!("HTTP {}", response.status()),
                ))),
            }
        }
    })
    .await?;

    // The body is still unconsumed: the caller owns streaming after the handshake.
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await?;
    assert_eq!(body.as_ref(), b"chunk");
    Ok(())
}

#[tokio::test]
async fn shared_http_client_enforces_total_timeout_for_json_policies(
) -> Result<(), Box<dyn std::error::Error>> {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .mount(&server)
        .await;

    let mut policy = fast_policy(1);
    policy.total_timeout = Some(Duration::from_millis(100));
    let http = super::shared_http_client("test provider", &policy)?;

    let error = http
        .get(format!("{}/slow", server.uri()))
        .send()
        .await
        .err()
        .ok_or("total_timeout must cut the slow response off")?;
    assert!(error.is_timeout(), "expected timeout, got: {error}");
    Ok(())
}

#[tokio::test]
async fn shared_http_client_streaming_policy_has_no_total_timeout(
) -> Result<(), Box<dyn std::error::Error>> {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"payload".to_vec())
                .set_delay(Duration::from_millis(300)),
        )
        .mount(&server)
        .await;

    // Streaming policy: no total timeout; generous read timeout tolerates the slow start.
    let mut policy = fast_policy(1);
    policy.total_timeout = None;
    policy.read_timeout = Duration::from_secs(5);
    let http = super::shared_http_client("test provider", &policy)?;

    let response = http.get(format!("{}/slow", server.uri())).send().await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn shared_http_client_applies_read_timeout_without_total_timeout(
) -> Result<(), Box<dyn std::error::Error>> {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stalled"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .mount(&server)
        .await;

    let mut policy = fast_policy(1);
    policy.total_timeout = None;
    policy.read_timeout = Duration::from_millis(100);
    let http = super::shared_http_client("test provider", &policy)?;

    let error = http
        .get(format!("{}/stalled", server.uri()))
        .send()
        .await
        .err()
        .ok_or("read_timeout must fire while the response stalls")?;
    assert!(error.is_timeout(), "expected timeout, got: {error}");
    Ok(())
}

#[test]
fn audit_record_emits_structured_fields_for_retry_scheduled() {
    let captured = CapturedEvents::default();
    tracing::subscriber::with_default(captured.clone(), || {
        ResilienceAudit::new("test provider").record(ResilienceEvent::RetryScheduled {
            attempt: 2,
            delay: Duration::from_millis(250),
        });
    });

    let events = captured.drain();
    assert_eq!(events.len(), 1, "exactly one tracing event per record call");
    assert!(
        events[0].contains("provider=\"test provider\""),
        "missing provider field: {}",
        events[0]
    );
    assert!(
        events[0].contains("attempt=2"),
        "missing attempt: {}",
        events[0]
    );
    assert!(
        events[0].contains("delay_ms=250"),
        "missing delay_ms: {}",
        events[0]
    );
}

#[test]
fn audit_record_emits_one_event_per_lifecycle_variant() {
    let captured = CapturedEvents::default();
    tracing::subscriber::with_default(captured.clone(), || {
        let audit = ResilienceAudit::new("p");
        audit.record(ResilienceEvent::AttemptSucceeded { attempt: 1 });
        audit.record(ResilienceEvent::RetryScheduled {
            attempt: 1,
            delay: Duration::ZERO,
        });
        audit.record(ResilienceEvent::AttemptsExhausted { attempts: 3 });
        audit.record(ResilienceEvent::FatalFailure { attempt: 1 });
        audit.record(ResilienceEvent::CircuitRejected);
    });

    let events = captured.drain();
    assert_eq!(events.len(), 5, "every variant must emit exactly one event");
    for (event, outcome) in events.iter().zip([
        "success",
        "retry_scheduled",
        "exhausted",
        "fatal",
        "circuit_rejected",
    ]) {
        assert!(
            event.contains(&format!("outcome=\"{outcome}\"")),
            "expected outcome {outcome} in: {event}"
        );
    }
}

#[test]
fn data_go_kr_const_policy_is_valid_and_preserves_legacy_defaults(
) -> Result<(), Box<dyn std::error::Error>> {
    use super::DATA_GO_KR;
    DATA_GO_KR.validate()?;

    // Golden: legacy DataGoKrRequestPolicy defaults (3 / 30s / 250ms..5s).
    assert_eq!(DATA_GO_KR.max_attempts, 3);
    assert_eq!(DATA_GO_KR.total_timeout, Some(Duration::from_secs(30)));
    assert_eq!(DATA_GO_KR.initial_backoff, Duration::from_millis(250));
    assert_eq!(DATA_GO_KR.max_backoff, Duration::from_secs(5));
    assert_eq!(
        DATA_GO_KR.circuit_breaker,
        RequestCircuitBreakerPolicy::default()
    );
    Ok(())
}

#[test]
fn vworld_json_const_policy_is_valid_and_preserves_legacy_defaults(
) -> Result<(), Box<dyn std::error::Error>> {
    use super::VWORLD_JSON;
    VWORLD_JSON.validate()?;

    // Golden: legacy VWorldRequestPolicy defaults (3 / 30s / 250ms..5s).
    assert_eq!(VWORLD_JSON.max_attempts, 3);
    assert_eq!(VWORLD_JSON.total_timeout, Some(Duration::from_secs(30)));
    assert_eq!(VWORLD_JSON.initial_backoff, Duration::from_millis(250));
    assert_eq!(VWORLD_JSON.max_backoff, Duration::from_secs(5));
    Ok(())
}

#[test]
fn vworld_file_const_policy_is_valid_and_streaming_safe() -> Result<(), Box<dyn std::error::Error>>
{
    use super::VWORLD_FILE;
    VWORLD_FILE.validate()?;

    // Streaming contract: no whole-transfer timeout, bounded retries with backoff.
    assert_eq!(VWORLD_FILE.total_timeout, None);
    assert_eq!(VWORLD_FILE.max_attempts, 3);
    Ok(())
}

#[test]
fn hub_const_policy_is_valid_and_streaming_safe() -> Result<(), Box<dyn std::error::Error>> {
    use super::HUB;
    HUB.validate()?;

    // Streaming contract: no whole-transfer timeout, bounded retries with backoff.
    assert_eq!(HUB.total_timeout, None);
    assert_eq!(HUB.max_attempts, 3);
    Ok(())
}

#[test]
fn iceberg_const_policy_is_valid_with_total_timeout() -> Result<(), Box<dyn std::error::Error>> {
    use super::ICEBERG;
    ICEBERG.validate()?;

    // JSON metadata endpoints: the whole request is bounded.
    assert_eq!(ICEBERG.total_timeout, Some(Duration::from_secs(30)));
    assert_eq!(ICEBERG.max_attempts, 3);
    Ok(())
}

#[test]
fn classify_response_carries_retry_after_seconds_for_retryable_statuses() {
    use super::classify_response;
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::RETRY_AFTER,
        reqwest::header::HeaderValue::from_static("2"),
    );

    assert_eq!(
        classify_response(StatusCode::TOO_MANY_REQUESTS, &headers),
        RetryDecision::Retryable {
            retry_after: Some(Duration::from_secs(2))
        }
    );
}

#[test]
fn classify_response_without_retry_after_header_has_no_hint() {
    use super::classify_response;
    let headers = reqwest::header::HeaderMap::new();

    assert_eq!(
        classify_response(StatusCode::SERVICE_UNAVAILABLE, &headers),
        RetryDecision::Retryable { retry_after: None }
    );
}

#[test]
fn classify_response_ignores_retry_after_on_non_retryable_status() {
    use super::classify_response;
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::RETRY_AFTER,
        reqwest::header::HeaderValue::from_static("2"),
    );

    assert_eq!(
        classify_response(StatusCode::NOT_FOUND, &headers),
        RetryDecision::NotRetryable
    );
}

#[test]
fn classify_response_ignores_malformed_retry_after_values() -> Result<(), Box<dyn std::error::Error>>
{
    use super::classify_response;
    for malformed in ["Wed, 21 Oct 2026 07:28:00 GMT", "abc", "-1", ""] {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_str(malformed)?,
        );
        assert_eq!(
            classify_response(StatusCode::TOO_MANY_REQUESTS, &headers),
            RetryDecision::Retryable { retry_after: None },
            "malformed Retry-After {malformed:?} must fall back to our own backoff"
        );
    }
    Ok(())
}

#[test]
fn base_backoff_is_exponential_and_capped() {
    let initial = Duration::from_millis(250);
    let max = Duration::from_secs(5);

    // attempt is 1-based: 2^(attempt-1) * initial, capped at max.
    assert_eq!(base_backoff(1, initial, max), Duration::from_millis(250));
    assert_eq!(base_backoff(2, initial, max), Duration::from_millis(500));
    assert_eq!(base_backoff(3, initial, max), Duration::from_secs(1));
    assert_eq!(base_backoff(4, initial, max), Duration::from_secs(2));
    // 2^4 * 250 = 4000
    assert_eq!(base_backoff(5, initial, max), Duration::from_secs(4));
    // 2^5 * 250 = 8000 -> capped to 5000
    assert_eq!(base_backoff(6, initial, max), Duration::from_secs(5));
    // very large attempt stays capped, no overflow/panic
    assert_eq!(base_backoff(99, initial, max), Duration::from_secs(5));
}
