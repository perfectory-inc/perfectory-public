// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use intelligence_api::{app, app_with_metrics, state::AppState};
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationProposalSubmission,
};
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn submit_proposal_returns_501_when_submitter_is_missing() {
    let response = app(AppState::default())
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn submit_proposal_skips_foundation_platform_when_validation_fails() {
    let submitter = Arc::new(FakeSubmitter::default());
    let state = AppState::default().with_foundation_submitter(submitter.clone());
    let mut payload = valid_submit_payload();
    payload["proposal"]["confidence"] = json!(0.1);

    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            payload,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["submission_attempted"], false);
    assert_eq!(body["metadata"]["reason"], "validation_failed");
    assert_eq!(submitter.calls(), 0);
}

#[tokio::test]
async fn submit_proposal_enqueues_sends_and_deduplicates_by_idempotency_key() {
    let submitter = Arc::new(FakeSubmitter::default());
    let state = AppState::default().with_foundation_submitter(submitter.clone());

    let first = app(state.clone())
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(first.status(), StatusCode::OK);
    let first_body = response_json(first).await;
    assert_eq!(first_body["submission_attempted"], true);
    assert_eq!(first_body["outbox_status"], "sent");
    assert_eq!(
        first_body["submission_result"]["submission_id"],
        "018f7c6a-0000-7000-8000-000000000001"
    );
    assert_eq!(first_body["submission_result"]["status"], "queued");
    assert_eq!(
        first_body["submission_result"]["metadata"]["storage"],
        "proposal_inbox"
    );

    let second = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::OK);
    let second_body = response_json(second).await;
    assert_eq!(second_body["submission_attempted"], false);
    assert_eq!(second_body["metadata"]["reason"], "duplicate_sent");
    assert_eq!(submitter.calls(), 1);
}

#[tokio::test]
async fn ambiguous_submission_increments_reconcile_required_metric() {
    let metrics = intelligence_api::observability::install_metrics_recorder().unwrap();
    let state = AppState::default().with_foundation_submitter(Arc::new(AmbiguousSubmitter));

    let response = app_with_metrics(state, Some(metrics.clone()))
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(metrics
        .render()
        .contains("outbox_reconcile_required_total 1"));
}

struct AmbiguousSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for AmbiguousSubmitter {
    async fn submit(
        &self,
        _: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::AmbiguousOutcome {
            message: "delivery outcome unknown".to_string(),
        })
    }
}

#[derive(Default)]
struct FakeSubmitter {
    calls: AtomicUsize,
}

impl FakeSubmitter {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl FoundationNormalizationSubmitter for FakeSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(FoundationSubmissionResult {
            submission_id: "018f7c6a-0000-7000-8000-000000000001".to_string(),
            status: FoundationSubmissionStatus::Queued,
            review_required: true,
            platform: "foundation-platform".to_string(),
            metadata: std::collections::BTreeMap::from([(
                "storage".to_string(),
                "proposal_inbox".to_string(),
            )]),
        })
    }
}

fn json_post(uri: &str, payload: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn valid_submit_payload() -> Value {
    json!({
        "request": {
            "tenant_id": "tenant-1",
            "source_system": "foundation-platform-r2",
            "raw_record_id": "raw-1",
            "raw_record": {"name": "Acme"},
            "trace_context": {
                "trace_id": "trace-1",
                "tenant_id": "tenant-1",
                "human_user_id": "user-1",
                "product_id": "foundation-platform"
            },
            "target_schema": {"required": ["normalized_name"]},
            "target_schema_version": "v1",
            "target_kind": "industrial_complex",
            "target_identity": {"industrial_complex_id": "complex-1"}
        },
        "proposal": {
            "raw_record_id": "raw-1",
            "proposed_record": {"normalized_name": "Acme"},
            "confidence": 0.91,
            "reasons": ["field matched source name"],
            "schema_version": "v1"
        }
    })
}
