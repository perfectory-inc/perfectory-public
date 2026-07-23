//! Task 8 durable-outbox integration tests.
//!
//! These tests drive the HTTP layer end-to-end while inspecting state through
//! the port API rather than reaching into `AppState` fields directly, verifying
//! that routes call through the workflow ports correctly.

// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use intelligence_api::{app, state::AppState};
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationAuditEvent, NormalizationAuditPort,
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationProposalSubmission, OutboxAcquireResult, OutboxTransitionError,
};
use intelligence_normalization_domain::{
    normalization_idempotency_key, validate_normalization_proposal, NormalizationProposal,
    NormalizationRequest,
};
use intelligence_normalization_infrastructure::InMemoryWorkflowState;
use serde_json::{json, Value};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CountingSubmitter — returns a fixed successful result and counts calls.
// ---------------------------------------------------------------------------

struct CountingSubmitter {
    count: Arc<AtomicUsize>,
}

impl CountingSubmitter {
    fn new(count: Arc<AtomicUsize>) -> Self {
        Self { count }
    }
}

#[async_trait]
impl FoundationNormalizationSubmitter for CountingSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(FoundationSubmissionResult {
            submission_id: "test-sub-shared-dedup-001".to_string(),
            status: FoundationSubmissionStatus::Queued,
            review_required: true,
            platform: "foundation-platform".to_string(),
            metadata: BTreeMap::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// FailMarkSentOutbox — delegates everything to InMemoryWorkflowState but
// returns StoreFailed from mark_sent, simulating a store failure after
// delivery has already succeeded (R3-class scenario).
// ---------------------------------------------------------------------------

struct AmbiguousSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for AmbiguousSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::AmbiguousOutcome {
            message: "timeout after request may have committed".to_string(),
        })
    }
}

struct InvalidResponseSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for InvalidResponseSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::InvalidResponse {
            message: "missing submission id".to_string(),
        })
    }
}

struct TerminalRejectedSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for TerminalRejectedSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::Rejected {
            status: 422,
            body: "invalid payload".to_string(),
            retryable: false,
        })
    }
}

struct FailMarkSentOutbox {
    inner: Arc<InMemoryWorkflowState>,
}

#[async_trait]
impl NormalizationOutboxPort for FailMarkSentOutbox {
    async fn enqueue(
        &self,
        record: NormalizationOutboxRecord,
        lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        self.inner.enqueue(record, lease).await
    }

    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        self.inner.get_sent(idempotency_key).await
    }

    async fn mark_sent(
        &self,
        _idempotency_key: &str,
        _result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        Err(OutboxTransitionError::StoreFailed {
            message: "injected mark_sent failure for test".to_string(),
        })
    }

    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner
            .mark_retryable_failure(idempotency_key, error)
            .await
    }

    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner.mark_dead_letter(idempotency_key, error).await
    }

    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner
            .mark_terminal_failure(idempotency_key, error)
            .await
    }

    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner
            .mark_reconcile_required(idempotency_key, error)
            .await
    }

    async fn claim_next_pending(
        &self,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        self.inner.claim_next_pending(limit, lease).await
    }
}

#[async_trait]
impl NormalizationAuditPort for FailMarkSentOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

// ---------------------------------------------------------------------------
// Helper: build the NormalizationProposalSubmission exactly as the route does,
// so we can compute the same payload fingerprint for direct enqueue in tests.
// ---------------------------------------------------------------------------

fn submission_for_valid_payload() -> (NormalizationOutboxRecord, String) {
    let request = NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "foundation-platform-r2".to_string(),
        raw_record_id: "raw-1".to_string(),
        raw_record: json!({"name": "Acme"}),
        trace_context: TraceContext {
            trace_id: "trace-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            human_user_id: "user-1".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: json!({"required": ["normalized_name"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "industrial_complex".to_string(),
        target_identity: json!({"industrial_complex_id": "complex-1"}),
        dictionaries: BTreeMap::new(),
    };

    // Match the serde-default-filled NormalizationProposal that the route gets
    // after deserialising valid_submit_payload() from JSON.
    let proposal = NormalizationProposal {
        raw_record_id: "raw-1".to_string(),
        proposed_record: json!({"normalized_name": "Acme"}),
        confidence: 0.91,
        reasons: vec!["field matched source name".to_string()],
        schema_version: "v1".to_string(),
        policy_id: "normalization-proposal-policy".to_string(),
        policy_version: "v1".to_string(),
        model_profile_id: None,
        model_id: None,
        prompt_id: None,
        prompt_version: None,
    };

    let validation = validate_normalization_proposal(&request, &proposal);

    let submission = NormalizationProposalSubmission {
        request: request.clone(),
        proposal,
        validation,
        trace_context: request.trace_context.clone(),
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::from([
            ("source_system".to_string(), request.source_system.clone()),
            ("raw_record_id".to_string(), request.raw_record_id.clone()),
            (
                "target_schema_version".to_string(),
                request.target_schema_version.clone(),
            ),
        ]),
    };

    let key = normalization_idempotency_key(&request);
    let record = NormalizationOutboxRecord::new(key.clone(), submission);
    (record, key)
}

// ---------------------------------------------------------------------------
// Test 1: shared workflow state deduplicates across two "replicas"
// ---------------------------------------------------------------------------

/// Two `AppState` instances sharing one `Arc<InMemoryWorkflowState>` simulate
/// two horizontally scaled replicas sharing a durable outbox.  The first POST
/// acquires and delivers; the second POST must detect `AlreadySent` and return
/// the duplicate_sent shape without calling the submitter again.
#[tokio::test]
async fn submit_uses_shared_workflow_state_for_deduplication() {
    let workflow = Arc::new(InMemoryWorkflowState::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let submitter = Arc::new(CountingSubmitter::new(counter.clone()));

    // Two independent AppState instances sharing the same workflow port —
    // analogous to two replicas pointing at the same Postgres database.
    let state_a = AppState::default()
        .with_workflow_state(workflow.clone())
        .with_foundation_submitter(submitter.clone());

    let state_b = AppState::default()
        .with_workflow_state(workflow.clone())
        .with_foundation_submitter(submitter.clone());

    // First request through replica A: should acquire, deliver, mark Sent.
    let first = app(state_a)
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

    // Second request through replica B: same payload → AlreadySent → duplicate.
    let second = app(state_b)
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

    // The submitter must have been called exactly once across both replicas.
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // A dedup audit event must have been appended for the second request.
    let events = workflow.audit_events();
    assert!(
        events
            .iter()
            .any(|e| e.event_type == "normalization.submission.deduplicated"),
        "expected a normalization.submission.deduplicated audit event for the duplicate request"
    );
}

// ---------------------------------------------------------------------------
// Test 2: reusing the same idempotency key with a different payload → 422
// ---------------------------------------------------------------------------

/// Amendment A6: a second submission that shares all key-determining fields
/// (`tenant_id`, `target_kind`, `raw_record_id`, `target_schema_version`) but
/// differs in proposal content must be rejected with 422 and code
/// `idempotency_payload_mismatch`.
#[tokio::test]
async fn payload_mismatch_returns_422() {
    let workflow = Arc::new(InMemoryWorkflowState::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let submitter = Arc::new(CountingSubmitter::new(counter.clone()));

    let state = AppState::default()
        .with_workflow_state(workflow.clone())
        .with_foundation_submitter(submitter.clone());

    // First POST: succeeds (Acquired → Sent).
    let first = app(state.clone())
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    // Second POST: same idempotency-key fields, mutated proposal content
    // → different payload fingerprint → PayloadMismatch → 422.
    let mut mutated = valid_submit_payload();
    mutated["proposal"]["proposed_record"]["mutated_extra"] = json!("different_value");

    let second = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            mutated,
        ))
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response_json(second).await;
    assert_eq!(body["code"], "idempotency_payload_mismatch");
    assert_eq!(
        body["message"],
        "idempotency key was reused with a different payload"
    );

    // Submitter must only have been called for the first (successful) request.
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    let events = workflow.audit_events();
    let mismatch_events = events
        .iter()
        .filter(|event| event.event_type == "normalization.submission.payload_mismatch")
        .collect::<Vec<_>>();
    assert_eq!(mismatch_events.len(), 1);
    assert_eq!(
        mismatch_events[0]
            .metadata
            .get("raw_record_id")
            .map(String::as_str),
        Some("raw-1")
    );
    let idempotency_key = mismatch_events[0]
        .metadata
        .get("idempotency_key")
        .expect("idempotency key metadata");
    assert!(idempotency_key.starts_with("normalization-v1-"));
}

// ---------------------------------------------------------------------------
// Test 3: a record already InFlight → 409 submission_in_progress
// ---------------------------------------------------------------------------

/// Pre-enqueuing a record directly via the port (simulating a concurrent
/// in-flight drain worker) causes the HTTP route to observe `AlreadyInFlight`
/// and return 409 without calling the submitter.
#[tokio::test]
async fn concurrent_inflight_returns_409() {
    let workflow = Arc::new(InMemoryWorkflowState::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let submitter = Arc::new(CountingSubmitter::new(counter.clone()));

    let state = AppState::default()
        .with_workflow_state(workflow.clone())
        .with_foundation_submitter(submitter.clone());

    // Build the same NormalizationOutboxRecord the route would build for
    // valid_submit_payload() so the fingerprints match.
    let (record, _key) = submission_for_valid_payload();

    // Pre-enqueue directly through the port with a 60-second lease — the
    // record is now InFlight and the lease has not yet expired.
    let acquire = workflow
        .enqueue(record, Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        matches!(acquire, OutboxAcquireResult::Acquired),
        "pre-enqueue should have acquired the record"
    );

    // HTTP POST with the same payload: route calls enqueue → finds InFlight
    // (same key, same fingerprint, active lease) → AlreadyInFlight → 409.
    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response_json(response).await;
    assert_eq!(body["code"], "submission_in_progress");

    // The submitter must never have been called.
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Test 4: a record that has reached a terminal state → 409 submission_not_retryable
// ---------------------------------------------------------------------------

/// Enqueuing a record via the port and then marking it dead-letter (terminal)
/// causes the HTTP route to observe a `Rejected` transition and return 409 with
/// code `submission_not_retryable` without calling the submitter.
#[tokio::test]
async fn terminal_record_returns_409_not_retryable() {
    let workflow = Arc::new(InMemoryWorkflowState::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let submitter = Arc::new(CountingSubmitter::new(counter.clone()));

    let state = AppState::default()
        .with_workflow_state(workflow.clone())
        .with_foundation_submitter(submitter.clone());

    let (record, key) = submission_for_valid_payload();

    // Acquire → InFlight.
    let acquire = workflow
        .enqueue(record, Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        matches!(acquire, OutboxAcquireResult::Acquired),
        "pre-enqueue should have acquired the record"
    );

    // Mark dead-letter → terminal state.
    workflow
        .mark_dead_letter(&key, "exhausted retries in test".to_string())
        .await
        .expect("mark_dead_letter should succeed on InFlight record");

    // HTTP POST with the same payload: route finds DeadLetter → Rejected → 409.
    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response_json(response).await;
    assert_eq!(body["code"], "submission_not_retryable");

    // The submitter must never have been called.
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Test 5: delivery succeeds but mark_sent fails → 502 with submitter count 1
// ---------------------------------------------------------------------------

/// Uses `FailMarkSentOutbox` to simulate a store failure that occurs after the
/// submission was already delivered to Foundation Platform (R3-class ambiguity).
/// The route must return 502 `normalization_outbox_store_failed` and the
/// submitter must have been called exactly once (delivery did happen).
#[tokio::test]
async fn failing_mark_sent_returns_502_and_submitter_was_called() {
    let counter = Arc::new(AtomicUsize::new(0));
    let submitter = Arc::new(CountingSubmitter::new(counter.clone()));
    let inner = Arc::new(InMemoryWorkflowState::default());
    let failing_outbox = Arc::new(FailMarkSentOutbox {
        inner: inner.clone(),
    });

    // Wire both workflow ports through FailMarkSentOutbox so audit appends
    // (delegated to inner) still work while mark_sent always fails.
    let state = AppState::default()
        .with_outbox_and_audit_ports(failing_outbox)
        .with_reconcile_queue(inner.clone())
        .with_foundation_submitter(submitter);

    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response_json(response).await;
    assert_eq!(body["code"], "normalization_outbox_store_failed");

    // Delivery happened (submitter was called) even though mark_sent failed.
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Test 6: mark_sent returns Rejected{Sent} → 200-family duplicate response
// ---------------------------------------------------------------------------

/// Outbox decorator that actually commits the Sent transition on the inner
/// store (so `get_sent` returns `Some(_)`) but then returns
/// `Rejected { current: Sent }` to simulate the benign lease race where a
/// concurrent worker already recorded delivery.
struct CountingFailureTransitionOutbox {
    inner: Arc<InMemoryWorkflowState>,
    retryable_failures: Arc<AtomicUsize>,
    terminal_failures: Arc<AtomicUsize>,
    reconcile_required: Arc<AtomicUsize>,
}

impl CountingFailureTransitionOutbox {
    async fn persisted_status_for_record(
        &self,
        record: NormalizationOutboxRecord,
    ) -> NormalizationOutboxStatus {
        let error = self
            .enqueue(record, Duration::from_secs(60))
            .await
            .expect_err("record should persist in a terminal status after route failure");

        let OutboxTransitionError::Rejected { current, .. } = error else {
            panic!("expected Rejected with current status, got {error:?}");
        };

        current
    }
}

#[async_trait]
impl NormalizationOutboxPort for CountingFailureTransitionOutbox {
    async fn enqueue(
        &self,
        record: NormalizationOutboxRecord,
        lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        self.inner.enqueue(record, lease).await
    }

    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        self.inner.get_sent(idempotency_key).await
    }

    async fn mark_sent(
        &self,
        idempotency_key: &str,
        result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner.mark_sent(idempotency_key, result).await
    }

    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.retryable_failures.fetch_add(1, Ordering::SeqCst);
        self.inner
            .mark_retryable_failure(idempotency_key, error)
            .await
    }

    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner.mark_dead_letter(idempotency_key, error).await
    }

    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.terminal_failures.fetch_add(1, Ordering::SeqCst);
        self.inner
            .mark_terminal_failure(idempotency_key, error)
            .await
    }

    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.reconcile_required.fetch_add(1, Ordering::SeqCst);
        self.inner
            .mark_reconcile_required(idempotency_key, error)
            .await
    }

    async fn claim_next_pending(
        &self,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        self.inner.claim_next_pending(limit, lease).await
    }
}

#[async_trait]
impl NormalizationAuditPort for CountingFailureTransitionOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

#[tokio::test]
async fn ambiguous_submission_marks_reconcile_required_in_inline_route() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    let retryable_failures = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let reconcile_required = Arc::new(AtomicUsize::new(0));
    let outbox = Arc::new(CountingFailureTransitionOutbox {
        inner: inner.clone(),
        retryable_failures: retryable_failures.clone(),
        terminal_failures: terminal_failures.clone(),
        reconcile_required: reconcile_required.clone(),
    });

    let state = AppState::default()
        .with_outbox_and_audit_ports(outbox.clone())
        .with_reconcile_queue(inner.clone())
        .with_foundation_submitter(Arc::new(AmbiguousSubmitter));
    let (record, _) = submission_for_valid_payload();

    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response_json(response).await;
    assert_eq!(body["code"], "foundation_platform_submission_failed");
    assert_eq!(retryable_failures.load(Ordering::SeqCst), 0);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 0);
    assert_eq!(reconcile_required.load(Ordering::SeqCst), 1);
    assert_eq!(
        outbox.persisted_status_for_record(record).await,
        NormalizationOutboxStatus::ReconcileRequired
    );
}

#[tokio::test]
async fn invalid_response_marks_reconcile_required_in_inline_route() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    let retryable_failures = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let reconcile_required = Arc::new(AtomicUsize::new(0));
    let outbox = Arc::new(CountingFailureTransitionOutbox {
        inner: inner.clone(),
        retryable_failures: retryable_failures.clone(),
        terminal_failures: terminal_failures.clone(),
        reconcile_required: reconcile_required.clone(),
    });

    let state = AppState::default()
        .with_outbox_and_audit_ports(outbox.clone())
        .with_reconcile_queue(inner.clone())
        .with_foundation_submitter(Arc::new(InvalidResponseSubmitter));
    let (record, _) = submission_for_valid_payload();

    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response_json(response).await;
    assert_eq!(body["code"], "foundation_platform_submission_failed");
    assert_eq!(retryable_failures.load(Ordering::SeqCst), 0);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 0);
    assert_eq!(reconcile_required.load(Ordering::SeqCst), 1);
    assert_eq!(
        outbox.persisted_status_for_record(record).await,
        NormalizationOutboxStatus::ReconcileRequired
    );
}

#[tokio::test]
async fn terminal_rejection_marks_failed_terminal_in_inline_route() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    let retryable_failures = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let reconcile_required = Arc::new(AtomicUsize::new(0));
    let outbox = Arc::new(CountingFailureTransitionOutbox {
        inner: inner.clone(),
        retryable_failures: retryable_failures.clone(),
        terminal_failures: terminal_failures.clone(),
        reconcile_required: reconcile_required.clone(),
    });

    let state = AppState::default()
        .with_outbox_and_audit_ports(outbox.clone())
        .with_reconcile_queue(inner.clone())
        .with_foundation_submitter(Arc::new(TerminalRejectedSubmitter));
    let (record, _) = submission_for_valid_payload();

    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response_json(response).await;
    assert_eq!(body["code"], "foundation_platform_submission_failed");
    assert_eq!(retryable_failures.load(Ordering::SeqCst), 0);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 1);
    assert_eq!(reconcile_required.load(Ordering::SeqCst), 0);
    assert_eq!(
        outbox.persisted_status_for_record(record).await,
        NormalizationOutboxStatus::FailedTerminal
    );
}

struct MarkSentRejectedSentOutbox {
    inner: Arc<InMemoryWorkflowState>,
}

#[async_trait]
impl NormalizationOutboxPort for MarkSentRejectedSentOutbox {
    async fn enqueue(
        &self,
        record: NormalizationOutboxRecord,
        lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        self.inner.enqueue(record, lease).await
    }

    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        self.inner.get_sent(idempotency_key).await
    }

    async fn mark_sent(
        &self,
        idempotency_key: &str,
        result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        // Commit Sent in the inner store so get_sent returns Some(_).
        let _ = self.inner.mark_sent(idempotency_key, result).await;
        // Then return Rejected{Sent} to simulate the lease race.
        Err(OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            message: "injected lease-race: another worker already marked Sent".to_string(),
        })
    }

    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner
            .mark_retryable_failure(idempotency_key, error)
            .await
    }

    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner.mark_dead_letter(idempotency_key, error).await
    }

    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner
            .mark_terminal_failure(idempotency_key, error)
            .await
    }

    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.inner
            .mark_reconcile_required(idempotency_key, error)
            .await
    }

    async fn claim_next_pending(
        &self,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        self.inner.claim_next_pending(limit, lease).await
    }
}

#[async_trait]
impl NormalizationAuditPort for MarkSentRejectedSentOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

/// Fix 1 contract: when `mark_sent` returns `Rejected { current: Sent }` (a
/// benign lease race — another worker already recorded delivery), the route
/// must NOT return 502.  Instead it fetches the existing Sent record via
/// `get_sent` and returns the same 200-family `duplicate_sent` response that
/// the `AlreadySent` enqueue path returns.
#[tokio::test]
async fn mark_sent_lease_race_returns_duplicate_sent_not_502() {
    let counter = Arc::new(AtomicUsize::new(0));
    let submitter = Arc::new(CountingSubmitter::new(counter.clone()));
    let inner = Arc::new(InMemoryWorkflowState::default());
    let outbox = Arc::new(MarkSentRejectedSentOutbox {
        inner: inner.clone(),
    });

    let state = AppState::default()
        .with_outbox_and_audit_ports(outbox)
        .with_reconcile_queue(inner.clone())
        .with_foundation_submitter(submitter);

    let response = app(state)
        .oneshot(json_post(
            "/intelligence/v1/normalization/submit-proposal",
            valid_submit_payload(),
        ))
        .await
        .unwrap();

    // Must NOT be 502 — Rejected{Sent} is a benign lease race, not a store error.
    assert_ne!(
        response.status(),
        StatusCode::BAD_GATEWAY,
        "Rejected{{Sent}} must not map to 502"
    );
    assert_eq!(response.status(), StatusCode::OK);

    let body = response_json(response).await;
    assert_eq!(
        body["metadata"]["reason"], "duplicate_sent",
        "must return duplicate_sent reason"
    );
    assert_eq!(
        body["submission_attempted"], false,
        "submission_attempted must be false for a lease-race duplicate"
    );

    // The submitter was called once — delivery happened before the race.
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
