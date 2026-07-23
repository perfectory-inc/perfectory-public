// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use intelligence_api::state::{AppState, RateLimitRoutePolicy, RateLimitRuntimePolicy};
use intelligence_normalization_application::{
    RateLimitDecision, RateLimitError, RateLimitQuota, RateLimitRequest, RateLimitRouteClass,
    RateLimiterPort,
};
use intelligence_normalization_infrastructure::{RedisRateLimitConfig, RedisRateLimiter};
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn chat_rate_limit_returns_429_with_retry_after() {
    let limiter = Arc::new(CapturingLimiter::new(RateLimitDecision::denied(3)));
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(limiter);
    let app = intelligence_api::app(state);

    let response = app
        .oneshot(authenticated_json_request(
            "/v1/chat/completions",
            Body::from(r#"{"messages":[{"role":"user","content":"hi"}]}"#),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .unwrap()
            .to_str()
            .unwrap(),
        "3"
    );
}

#[tokio::test]
async fn health_ready_and_metrics_bypass_rate_limit_bucket() {
    let limiter = Arc::new(CapturingLimiter::new(RateLimitDecision::denied(30)));
    let captured = limiter.captured_requests();
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(limiter);
    let app = intelligence_api::app(state);

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);

    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer local-dev-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    assert!(captured.lock().unwrap().is_empty());
}

#[tokio::test]
async fn limiter_unavailable_fails_closed_with_503_and_retry_after() {
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(Arc::new(UnavailableLimiter));
    let app = intelligence_api::app(state);

    let response = app
        .oneshot(authenticated_json_request(
            "/intelligence/v1/normalization/submit-proposal",
            Body::from(normalization_submit_payload().to_string()),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .unwrap()
            .to_str()
            .unwrap(),
        "1"
    );
}

#[tokio::test]
async fn redis_limiter_connects_lazily_and_fails_closed_on_first_request() {
    let limiter = RedisRateLimiter::connect(RedisRateLimitConfig {
        redis_url: "redis://127.0.0.1:1/".to_string(),
        key_prefix: "ip-test".to_string(),
        ttl_seconds: 60,
        timeout_ms: 50,
    })
    .await
    .unwrap();
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(Arc::new(limiter));
    let app = intelligence_api::app(state);

    let response = app
        .oneshot(authenticated_json_request(
            "/intelligence/v1/normalization/submit-proposal",
            Body::from(normalization_submit_payload().to_string()),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .unwrap()
            .to_str()
            .unwrap(),
        "1"
    );
}

#[tokio::test]
async fn limiter_uses_verified_principal_identity() {
    let limiter = Arc::new(CapturingLimiter::new(RateLimitDecision::allowed(99)));
    let captured = limiter.captured_requests();
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(limiter);
    let app = intelligence_api::app(state);

    let response = app
        .oneshot(authenticated_json_request(
            "/v1/chat/completions",
            Body::from(r#"{"messages":[{"role":"user","content":"hi"}]}"#),
        ))
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].subject.tenant_id, "tenant:local");
    assert_eq!(
        requests[0].subject.subject_id,
        "service:intelligence-api-test"
    );
}

#[tokio::test]
async fn limiter_receives_route_specific_quota_and_cost() {
    let limiter = Arc::new(CapturingLimiter::new(RateLimitDecision::allowed(99)));
    let captured = limiter.captured_requests();
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(limiter)
        .with_rate_limit_policy(RateLimitRuntimePolicy::default().with_route_policy(
            RateLimitRouteClass::Chat,
            RateLimitRoutePolicy {
                quota: RateLimitQuota {
                    capacity: 11,
                    refill_per_second: 2.5,
                },
                cost: 4,
            },
        ));
    let app = intelligence_api::app(state);

    let response = app
        .oneshot(authenticated_json_request(
            "/v1/chat/completions",
            Body::from(r#"{"messages":[{"role":"user","content":"hi"}]}"#),
        ))
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].cost, 4);
    assert_eq!(requests[0].quota.capacity, 11);
    assert_eq!(requests[0].quota.refill_per_second, 2.5);
}

#[tokio::test]
async fn models_route_stays_protected_but_not_token_bucketed() {
    let limiter = Arc::new(CapturingLimiter::new(RateLimitDecision::denied(30)));
    let captured = limiter.captured_requests();
    let state = AppState::default()
        .with_required_inbound_auth("local-dev-token")
        .with_rate_limiter_dyn(limiter);
    let app = intelligence_api::app(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer local-dev-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "service")
                .header("x-intelligence-tenant-id", "tenant-1")
                .header("x-intelligence-product-id", "foundation-platform")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(captured.lock().unwrap().is_empty());
}

#[derive(Clone, Debug)]
struct CapturingLimiter {
    decision: RateLimitDecision,
    captured: Arc<Mutex<Vec<RateLimitRequest>>>,
}

impl CapturingLimiter {
    fn new(decision: RateLimitDecision) -> Self {
        Self {
            decision,
            captured: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn captured_requests(&self) -> Arc<Mutex<Vec<RateLimitRequest>>> {
        self.captured.clone()
    }
}

#[async_trait]
impl RateLimiterPort for CapturingLimiter {
    async fn check(&self, request: RateLimitRequest) -> Result<RateLimitDecision, RateLimitError> {
        self.captured
            .lock()
            .map_err(|_| RateLimitError::Unavailable {
                message: "capturing limiter lock poisoned".to_string(),
            })?
            .push(request);
        Ok(self.decision)
    }
}

struct UnavailableLimiter;

#[async_trait]
impl RateLimiterPort for UnavailableLimiter {
    async fn check(&self, _request: RateLimitRequest) -> Result<RateLimitDecision, RateLimitError> {
        Err(RateLimitError::Unavailable {
            message: "redis unavailable".to_string(),
        })
    }
}

fn authenticated_json_request(path: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", "Bearer local-dev-token")
        .header("x-intelligence-subject-id", "service:test-client")
        .header("x-intelligence-principal-kind", "service")
        .header("x-intelligence-tenant-id", "tenant-1")
        .header("x-intelligence-product-id", "foundation-platform")
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

fn normalization_submit_payload() -> serde_json::Value {
    json!({
        "request": {
            "tenant_id": "tenant-1",
            "source_system": "foundation-platform-r2",
            "raw_record_id": "raw-1",
            "raw_record": { "name": "Acme" },
            "trace_context": {
                "trace_id": "trace-1",
                "tenant_id": "tenant-1",
                "human_user_id": "user-1",
                "product_id": "foundation-platform"
            },
            "target_schema": { "required": ["normalized_name"] },
            "target_schema_version": "v1",
            "target_kind": "industrial_complex",
            "target_identity": { "industrial_complex_id": "complex-1" }
        },
        "proposal": {
            "raw_record_id": "raw-1",
            "proposed_record": { "normalized_name": "Acme" },
            "confidence": 0.91,
            "reasons": ["field matched source name"],
            "schema_version": "v1"
        }
    })
}
