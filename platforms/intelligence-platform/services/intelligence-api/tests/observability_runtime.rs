// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use intelligence_api::admission::{apply_admission_layers, AdmissionConfig};
use intelligence_api::health_routes;
use intelligence_api::state::AppState;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationProposalSubmission,
    NormalizationReconcileQueuePort, OutboxTransitionError, ReconcileQueueStats,
};
use intelligence_normalization_domain::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};
use intelligence_normalization_infrastructure::InMemoryWorkflowState;
use tower::ServiceExt;

fn metrics_test_mutex() -> &'static tokio::sync::Mutex<()> {
    static METRICS_TEST_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    METRICS_TEST_MUTEX.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn metric_sample_value(text: &str, metric: &str) -> f64 {
    text.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == metric).then(|| value.parse::<f64>().unwrap())
        })
        .unwrap_or_else(|| panic!("missing metric sample for {metric}: {text}"))
}

#[tokio::test]
async fn readyz_reports_missing_dependencies() {
    let app = intelligence_api::app(AppState::default());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["status"], "degraded");
}

#[tokio::test]
async fn metrics_endpoint_exposes_prometheus_text() {
    let _guard = metrics_test_mutex().lock().await;
    let metrics = intelligence_api::observability::install_metrics_recorder().ok();
    let app = intelligence_api::app_with_metrics(AppState::default(), metrics);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("# HELP")
            || text.contains("# TYPE")
            || text.contains("http_requests")
            || text.contains("intelligence_metrics_unavailable"),
        "expected prometheus text format, got: {text}",
    );
    // This test is the sole recorder installer in its process so the label is
    // deterministic: the /healthz hit above must appear with its real path.
    assert!(
        text.contains("path=\"/healthz\""),
        "expected path label for /healthz in metrics text, got: {text}",
    );
}

/// Proves health endpoints are OUTSIDE the load-shed / concurrency stack.
///
/// Composition mirrors `app_full` in lib.rs:
///   - protected router wrapped in `apply_admission_layers` (max_concurrency=1)
///   - health router merged OUTSIDE the shed stack
///
/// While `/v1/slow` holds the sole concurrency permit, `/healthz` must still
/// return 200 because it is served from the health router which is not wrapped
/// in the admission layers.
#[tokio::test]
async fn health_endpoints_are_exempt_from_load_shedding() {
    // Slow route that holds the global concurrency permit.
    let slow_protected = Router::new().route(
        "/v1/slow",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            "done"
        }),
    );
    let admitted = apply_admission_layers(
        slow_protected,
        &AdmissionConfig {
            max_body_bytes: 1024,
            request_timeout_seconds: 30,
            max_concurrency: 1,
        },
    );

    // Health router merged OUTSIDE the shed stack — mirrors lib.rs composition.
    let health = health_routes::router().with_state(AppState::default());
    let app = health.merge(admitted);

    // Saturate the concurrency limit.
    let saturating = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .uri("/v1/slow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Health must answer 200 even though the protected side is saturated.
    let health_response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        health_response.status(),
        StatusCode::OK,
        "/healthz must not be shed when protected routes are saturated"
    );

    // Slow request should still complete successfully.
    let slow_status = saturating.await.unwrap().status();
    assert_eq!(slow_status, StatusCode::OK);
}

/// Integration smoke test: /healthz returns 200 through the public `app` entry-point.
#[tokio::test]
async fn healthz_returns_ok_through_public_app() {
    let app = intelligence_api::app(AppState::default());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["status"], "ok");
    assert_eq!(value["service"], "intelligence-platform");
}

/// When inbound auth is required, /metrics must be bearer-gated while
/// /healthz and /readyz remain open (so K8s probes still work).
#[tokio::test]
async fn metrics_auth_gates_metrics_not_health() {
    let _guard = metrics_test_mutex().lock().await;
    let state = AppState::default().with_required_inbound_auth("t");
    let app = intelligence_api::app(state);

    // /metrics without auth → 401
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "/metrics must require auth when auth is enabled"
    );

    // /metrics with correct Bearer → 200
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer t")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "/metrics must accept the correct bearer token"
    );

    // /readyz stays open without auth
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "/readyz must not require auth"
    );
}

#[tokio::test]
async fn metrics_refreshes_reconcile_queue_gauges() {
    let _guard = metrics_test_mutex().lock().await;
    let metrics = intelligence_api::observability::install_metrics_recorder().ok();
    let workflow = Arc::new(InMemoryWorkflowState::default());
    let state = AppState::default()
        .with_workflow_state(workflow.clone())
        .with_metrics(metrics);

    let app = intelligence_api::app(state);
    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let first_body = to_bytes(first_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let first_text = String::from_utf8(first_body.to_vec()).unwrap();

    assert_eq!(
        metric_sample_value(&first_text, "outbox_reconcile_required_depth"),
        0.0
    );

    let record =
        NormalizationOutboxRecord::new("key-reconcile-metric".to_string(), default_submission());
    workflow
        .enqueue(record, Duration::from_secs(60))
        .await
        .unwrap();
    workflow
        .mark_reconcile_required("key-reconcile-metric", "ambiguous".to_string())
        .await
        .unwrap();

    let second_response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let second_body = to_bytes(second_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let second_text = String::from_utf8(second_body.to_vec()).unwrap();

    assert_eq!(
        metric_sample_value(&second_text, "outbox_reconcile_required_depth"),
        1.0
    );
    assert!(metric_sample_value(&second_text, "outbox_reconcile_oldest_age_seconds") >= 0.0);
}

#[tokio::test]
async fn metrics_renders_existing_handle_when_reconcile_stats_stall() {
    let _guard = metrics_test_mutex().lock().await;
    let metrics = intelligence_api::observability::install_metrics_recorder().ok();
    let state = AppState::default()
        .with_reconcile_queue(Arc::new(SlowReconcileQueue))
        .with_metrics(metrics);
    let app = intelligence_api::app(state);

    let started = std::time::Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "metrics should use a local reconcile stats timeout, not the outer request timeout"
    );
}

struct SlowReconcileQueue;

#[async_trait]
impl NormalizationReconcileQueuePort for SlowReconcileQueue {
    async fn stats(&self) -> Result<ReconcileQueueStats, OutboxTransitionError> {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(ReconcileQueueStats {
            depth: 1,
            oldest_age_seconds: 10.0,
        })
    }
}

fn default_submission() -> NormalizationProposalSubmission {
    let request = NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "test-system".to_string(),
        raw_record_id: "raw-record-1".to_string(),
        raw_record: serde_json::json!({"raw": "data"}),
        trace_context: TraceContext {
            trace_id: "trace-raw-record-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            human_user_id: "test-user".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: serde_json::json!({"required": ["field_a"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "test_kind".to_string(),
        target_identity: serde_json::json!({"id": "raw-record-1"}),
        dictionaries: BTreeMap::new(),
    };
    let proposal = NormalizationProposal {
        raw_record_id: "raw-record-1".to_string(),
        proposed_record: serde_json::json!({"field_a": "value"}),
        confidence: 0.92,
        reasons: vec!["test reason".to_string()],
        schema_version: "v1".to_string(),
        policy_id: "test-policy".to_string(),
        policy_version: "v1".to_string(),
        model_profile_id: None,
        model_id: None,
        prompt_id: None,
        prompt_version: None,
    };
    let validation = NormalizationValidationResult {
        accepted: true,
        raw_record_id: "raw-record-1".to_string(),
        confidence: 0.92,
        errors: vec![],
    };

    NormalizationProposalSubmission {
        trace_context: request.trace_context.clone(),
        request,
        proposal,
        validation,
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    }
}
