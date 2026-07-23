//! Integration tests for the normalization outbox drain worker.
//!
//! Tests use [`InMemoryWorkflowState`] as the outbox port and in-process
//! submitter stubs so they run without any external services.

// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationAuditEvent, NormalizationAuditPort,
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationProposalSubmission, OutboxAcquireResult, OutboxTransitionError,
};
use intelligence_normalization_domain::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};
use intelligence_normalization_infrastructure::InMemoryWorkflowState;
use intelligence_worker::outbox_worker::{
    drain_config_from_lookup, drain_once, run_drain_loop, DrainConfig,
};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn make_request(tenant_id: &str, raw_record_id: &str) -> NormalizationRequest {
    NormalizationRequest {
        tenant_id: tenant_id.to_string(),
        source_system: "test-system".to_string(),
        raw_record_id: raw_record_id.to_string(),
        raw_record: serde_json::json!({"raw": "data"}),
        trace_context: TraceContext {
            trace_id: format!("trace-{raw_record_id}"),
            tenant_id: tenant_id.to_string(),
            human_user_id: "test-user".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: serde_json::json!({"required": ["field_a"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "test_kind".to_string(),
        target_identity: serde_json::json!({"id": raw_record_id}),
        dictionaries: BTreeMap::new(),
    }
}

fn make_submission(tenant_id: &str, raw_record_id: &str) -> NormalizationProposalSubmission {
    let request = make_request(tenant_id, raw_record_id);
    let proposal = NormalizationProposal {
        raw_record_id: raw_record_id.to_string(),
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
        raw_record_id: raw_record_id.to_string(),
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

fn make_record(key: &str, tenant_id: &str, raw_record_id: &str) -> NormalizationOutboxRecord {
    NormalizationOutboxRecord::new(key.to_string(), make_submission(tenant_id, raw_record_id))
}

fn default_result() -> FoundationSubmissionResult {
    FoundationSubmissionResult {
        submission_id: "sub-test-001".to_string(),
        status: FoundationSubmissionStatus::Queued,
        review_required: true,
        platform: "foundation-platform".to_string(),
        metadata: BTreeMap::new(),
    }
}

fn test_config() -> DrainConfig {
    DrainConfig {
        batch_size: 32,
        lease: Duration::from_secs(60),
        max_attempts: 8,
        idle_sleep: Duration::from_millis(500),
    }
}

async fn enqueue_default(outbox: &Arc<InMemoryWorkflowState>, key: &str) {
    let record = make_record(key, "tenant-1", key);
    let result = outbox
        .enqueue(record, Duration::from_secs(60))
        .await
        .expect("enqueue should succeed");
    assert_eq!(result, OutboxAcquireResult::Acquired);

    outbox
        .mark_retryable_failure(key, "pre-test failure".to_string())
        .await
        .expect("mark_retryable_failure should succeed");
}

/// Enqueue a record then immediately mark it FailedRetryable so it is
/// claimable by `drain_once` without waiting for a lease to expire.
async fn enqueue_and_fail(
    outbox: &InMemoryWorkflowState,
    key: &str,
    tenant_id: &str,
    raw_record_id: &str,
) {
    let record = make_record(key, tenant_id, raw_record_id);
    let result = outbox
        .enqueue(record, Duration::from_secs(60))
        .await
        .expect("enqueue should succeed");
    assert_eq!(result, OutboxAcquireResult::Acquired);

    outbox
        .mark_retryable_failure(key, "pre-test failure".to_string())
        .await
        .expect("mark_retryable_failure should succeed");
}

// ---------------------------------------------------------------------------
// Submitter stubs
// ---------------------------------------------------------------------------

/// Always returns a successful submission result.
struct AlwaysOkSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for AlwaysOkSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Ok(default_result())
    }
}

/// Always returns a pre-send failure.
struct AlwaysFailSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for AlwaysFailSubmitter {
    async fn submit(
        &self,
        _submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::PreSendFailure {
            message: "injected transport failure".to_string(),
        })
    }
}

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

/// Fails submissions whose `request.tenant_id` equals the configured poison
/// tenant; succeeds for all others.
struct SelectiveFailSubmitter {
    poison_tenant_id: String,
}

#[async_trait]
impl FoundationNormalizationSubmitter for SelectiveFailSubmitter {
    async fn submit(
        &self,
        submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        if submission.request.tenant_id == self.poison_tenant_id {
            Err(FoundationSubmissionError::PreSendFailure {
                message: "injected failure for poison tenant".to_string(),
            })
        } else {
            Ok(default_result())
        }
    }
}

// ---------------------------------------------------------------------------
// Outbox decorators for lease-race simulation
// ---------------------------------------------------------------------------

/// Wraps [`InMemoryWorkflowState`] but makes `mark_sent` always return
/// `Rejected { current: Sent }`, simulating a lease race where another
/// worker already marked the record Sent.
struct SentRaceOutbox {
    inner: Arc<InMemoryWorkflowState>,
}

#[async_trait]
impl NormalizationOutboxPort for SentRaceOutbox {
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
        Err(OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            message: "injected lease race: already Sent by another worker".to_string(),
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
impl NormalizationAuditPort for SentRaceOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

// ---------------------------------------------------------------------------
// Test 1: drain_once submits claimable records and marks them Sent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_submits_claimable_records_and_marks_sent() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    let submitter = Arc::new(AlwaysOkSubmitter);

    enqueue_and_fail(&outbox, "key-a", "tenant-1", "raw-a").await;
    enqueue_and_fail(&outbox, "key-b", "tenant-1", "raw-b").await;

    let summary = drain_once(outbox.clone(), submitter, &test_config())
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 2, "expected 2 claimed");
    assert_eq!(summary.submitted, 2, "expected 2 submitted");
    assert_eq!(summary.failed_retryable, 0);
    assert_eq!(summary.dead_lettered, 0);
    assert!(summary.transition_failures.is_empty());
    assert_eq!(summary.lease_races, 0);

    // Both records must now be in Sent state.
    let sent_a = outbox
        .get_sent("key-a")
        .await
        .expect("get_sent should not error");
    assert!(sent_a.is_some(), "key-a should be Sent");

    let sent_b = outbox
        .get_sent("key-b")
        .await
        .expect("get_sent should not error");
    assert!(sent_b.is_some(), "key-b should be Sent");
}

// ---------------------------------------------------------------------------
// Test 2: drain_once marks record FailedRetryable on submission failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_marks_retryable_on_submit_failure() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    let submitter = Arc::new(AlwaysFailSubmitter);

    enqueue_and_fail(&outbox, "key-fail", "tenant-1", "raw-fail").await;

    let summary = drain_once(outbox.clone(), submitter, &test_config())
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 1);
    assert_eq!(summary.submitted, 0);
    assert_eq!(summary.failed_retryable, 1);
    assert_eq!(summary.lease_races, 0);

    // Record must NOT be in Sent state.
    let sent = outbox
        .get_sent("key-fail")
        .await
        .expect("get_sent should not error");
    assert!(
        sent.is_none(),
        "key-fail must not be Sent after failed submission"
    );

    // Record must be claimable again (FailedRetryable).
    let reclaimable = outbox
        .claim_next_pending(10, Duration::from_secs(60))
        .await
        .expect("claim_next_pending should not error");
    assert_eq!(
        reclaimable.len(),
        1,
        "record should be claimable after retryable failure"
    );
    assert_eq!(reclaimable[0].idempotency_key, "key-fail");
}

#[tokio::test]
async fn ambiguous_submission_moves_to_reconcile_required() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    enqueue_default(&outbox, "key-ambiguous").await;

    let summary = drain_once(
        outbox.clone(),
        Arc::new(AmbiguousSubmitter),
        &DrainConfig::default(),
    )
    .await
    .expect("drain_once must not error");

    assert_eq!(summary.reconcile_required, 1);

    let claimed = outbox
        .claim_next_pending(1, Duration::from_secs(60))
        .await
        .expect("claim_next_pending should not error");
    assert!(
        claimed.is_empty(),
        "reconcile records must not be claimed by normal drain"
    );
}

#[tokio::test]
async fn invalid_success_response_moves_to_reconcile_required() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    enqueue_default(&outbox, "key-invalid-response").await;

    let summary = drain_once(
        outbox.clone(),
        Arc::new(InvalidResponseSubmitter),
        &DrainConfig::default(),
    )
    .await
    .expect("drain_once must not error");

    assert_eq!(summary.reconcile_required, 1);

    let claimed = outbox
        .claim_next_pending(1, Duration::from_secs(60))
        .await
        .expect("claim_next_pending should not error");
    assert!(
        claimed.is_empty(),
        "reconcile records must not be claimed by normal drain"
    );
}

#[tokio::test]
async fn terminal_rejection_moves_to_failed_terminal() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    enqueue_default(&outbox, "key-terminal-reject").await;

    let summary = drain_once(
        outbox.clone(),
        Arc::new(TerminalRejectedSubmitter),
        &DrainConfig::default(),
    )
    .await
    .expect("drain_once must not error");

    assert_eq!(summary.failed_terminal, 1);
}

// ---------------------------------------------------------------------------
// Test 3: drain_once dead-letters a record that has exhausted its retry budget
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_dead_letters_after_max_attempts() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    let submitter = Arc::new(AlwaysOkSubmitter);

    // Set max_attempts=1; after one pre-failure the record has attempts=1 which
    // equals max_attempts and must be dead-lettered rather than retried.
    let config = DrainConfig {
        max_attempts: 1,
        ..test_config()
    };

    enqueue_and_fail(&outbox, "key-dead", "tenant-1", "raw-dead").await;
    // attempts is now 1 (from the initial mark_retryable_failure in enqueue_and_fail)

    let summary = drain_once(outbox.clone(), submitter, &config)
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 1);
    assert_eq!(summary.dead_lettered, 1, "expected dead_lettered=1");
    assert_eq!(summary.submitted, 0);
    assert_eq!(summary.failed_retryable, 0);

    // Record is now DeadLetter — get_sent returns None.
    let sent = outbox
        .get_sent("key-dead")
        .await
        .expect("get_sent should not error");
    assert!(
        sent.is_none(),
        "dead-lettered record must not appear as Sent"
    );

    // Attempting to re-enqueue the same key with same fingerprint must fail
    // with a Rejected transition (record is terminal).
    let record = make_record("key-dead", "tenant-1", "raw-dead");
    let re_enqueue = outbox.enqueue(record, Duration::from_secs(60)).await;
    assert!(
        matches!(re_enqueue, Err(OutboxTransitionError::Rejected { .. })),
        "re-enqueuing a dead-lettered record must return Rejected"
    );
}

// ---------------------------------------------------------------------------
// Test 4: drain_once isolates poison records — one failure does not abort batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_isolates_poison_records() {
    let outbox = Arc::new(InMemoryWorkflowState::default());

    // Submitter that fails for "tenant-poison" only.
    let submitter = Arc::new(SelectiveFailSubmitter {
        poison_tenant_id: "tenant-poison".to_string(),
    });

    // Record A (poison): tenant-poison → submission will fail.
    enqueue_and_fail(&outbox, "key-poison", "tenant-poison", "raw-poison").await;
    // Record B (healthy): tenant-ok → submission will succeed.
    enqueue_and_fail(&outbox, "key-ok", "tenant-ok", "raw-ok").await;

    let summary = drain_once(outbox.clone(), submitter, &test_config())
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 2, "both records must be claimed");
    assert_eq!(summary.submitted, 1, "healthy record must be submitted");
    assert_eq!(
        summary.failed_retryable, 1,
        "poison record must be marked retryable"
    );
    assert_eq!(summary.dead_lettered, 0);
    assert_eq!(summary.lease_races, 0);

    // Healthy record is Sent.
    let sent_ok = outbox
        .get_sent("key-ok")
        .await
        .expect("get_sent should not error");
    assert!(sent_ok.is_some(), "key-ok must be Sent");

    // Poison record is NOT Sent.
    let sent_poison = outbox
        .get_sent("key-poison")
        .await
        .expect("get_sent should not error");
    assert!(sent_poison.is_none(), "key-poison must not be Sent");
}

// ---------------------------------------------------------------------------
// Test 5: drain_once treats Rejected{current:Sent} from mark_sent as benign
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_treats_sent_race_as_benign() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    let outbox: Arc<dyn NormalizationOutboxPort> = Arc::new(SentRaceOutbox {
        inner: inner.clone(),
    });
    let submitter = Arc::new(AlwaysOkSubmitter);

    enqueue_and_fail(&inner, "key-race", "tenant-1", "raw-race").await;

    let config = test_config();
    let summary = drain_once(outbox.clone(), submitter, &config)
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 1);
    assert_eq!(
        summary.submitted, 0,
        "mark_sent raced — not counted as submitted"
    );
    assert_eq!(
        summary.lease_races, 1,
        "Rejected{{Sent}} must be counted as a lease race"
    );
    assert_eq!(summary.failed_retryable, 0);
    assert_eq!(summary.dead_lettered, 0);
}

// ---------------------------------------------------------------------------
// Test 6: run_drain_loop exits cleanly on cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_drain_loop_exits_on_cancellation() {
    let outbox: Arc<dyn NormalizationOutboxPort> = Arc::new(InMemoryWorkflowState::default());
    let submitter: Arc<dyn FoundationNormalizationSubmitter> = Arc::new(AlwaysOkSubmitter);

    let config = DrainConfig {
        idle_sleep: Duration::from_millis(500),
        ..test_config()
    };

    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();

    let handle = tokio::spawn(run_drain_loop(outbox, submitter, config, cancel));

    // Give the loop one tick to start, then cancel.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel_for_signal.cancel();

    // The loop must complete within 1 second of cancellation.
    let join_result = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(
        join_result.is_ok(),
        "run_drain_loop must exit within 1s of cancellation"
    );
    join_result
        .unwrap()
        .expect("run_drain_loop task must not panic");
}

// ---------------------------------------------------------------------------
// Test 7: drain_config_from_lookup — defaults and validation
// ---------------------------------------------------------------------------

#[test]
fn drain_config_defaults_when_no_env_vars() {
    let config = drain_config_from_lookup(|_| None).unwrap();
    assert_eq!(config.batch_size, 4);
    assert_eq!(config.lease.as_secs(), 60);
    assert_eq!(config.max_attempts, 8);
    assert_eq!(config.idle_sleep.as_secs(), 2);
}

#[test]
fn drain_config_zero_batch_size_errors_naming_var() {
    let values = BTreeMap::from([("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE", "0")]);
    let err = drain_config_from_lookup(|k| values.get(k).map(|v| v.to_string())).unwrap_err();
    assert!(
        err.contains("NORMALIZATION_OUTBOX_DRAIN_BATCH_SIZE"),
        "error must name the env var; got: {err}"
    );
}

#[test]
fn drain_config_invalid_lease_seconds_errors_naming_var() {
    let values = BTreeMap::from([("NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS", "abc")]);
    let err = drain_config_from_lookup(|k| values.get(k).map(|v| v.to_string())).unwrap_err();
    assert!(
        err.contains("NORMALIZATION_OUTBOX_DRAIN_LEASE_SECONDS is invalid"),
        "error must name the env var; got: {err}"
    );
}

#[test]
fn drain_config_zero_max_attempts_errors_naming_var() {
    let values = BTreeMap::from([("NORMALIZATION_OUTBOX_MAX_ATTEMPTS", "0")]);
    let err = drain_config_from_lookup(|k| values.get(k).map(|v| v.to_string())).unwrap_err();
    assert!(
        err.contains("NORMALIZATION_OUTBOX_MAX_ATTEMPTS must be greater than zero"),
        "error must name the env var; got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Additional decorators for Fix 5 tests
// ---------------------------------------------------------------------------

/// Wraps [`InMemoryWorkflowState`] but makes `mark_retryable_failure` always
/// return `Rejected { current: Sent }`, simulating a lease race where another
/// worker already delivered the record between our submission failure and our
/// attempt to record the retryable failure.
struct RetryableRaceOutbox {
    inner: Arc<InMemoryWorkflowState>,
}

#[async_trait]
impl NormalizationOutboxPort for RetryableRaceOutbox {
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
        _idempotency_key: &str,
        _error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        Err(OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            message: "injected lease race: already Sent by another worker".to_string(),
        })
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
        _idempotency_key: &str,
        _error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        Err(OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            message: "injected lease race: already Sent by another worker".to_string(),
        })
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
impl NormalizationAuditPort for RetryableRaceOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

/// Wraps [`InMemoryWorkflowState`] but makes `mark_dead_letter` always return
/// `Rejected { current: Sent }`, simulating a lease race where another worker
/// already delivered a record that was about to be dead-lettered.
struct DeadLetterRaceOutbox {
    inner: Arc<InMemoryWorkflowState>,
}

#[async_trait]
impl NormalizationOutboxPort for DeadLetterRaceOutbox {
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
        self.inner
            .mark_retryable_failure(idempotency_key, error)
            .await
    }

    async fn mark_dead_letter(
        &self,
        _idempotency_key: &str,
        _error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        Err(OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            message: "injected lease race: already Sent by another worker".to_string(),
        })
    }

    async fn mark_terminal_failure(
        &self,
        _idempotency_key: &str,
        _error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        Err(OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            message: "injected lease race: already Sent by another worker".to_string(),
        })
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
impl NormalizationAuditPort for DeadLetterRaceOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

/// Wraps [`InMemoryWorkflowState`] but makes `mark_sent` return `StoreFailed`
/// for a specific idempotency key (once) so that batches with multiple records
/// can exercise the "delivered but unrecorded" (R3-class) branch without
/// aborting the rest of the batch.
struct FailKeyMarkSentOutbox {
    inner: Arc<InMemoryWorkflowState>,
    /// The idempotency key whose `mark_sent` call will be made to fail.
    fail_key: String,
    /// Set to `true` after the first failure is injected; subsequent calls for
    /// `fail_key` (if any) delegate normally.
    has_failed: AtomicBool,
}

#[async_trait]
impl NormalizationOutboxPort for FailKeyMarkSentOutbox {
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
        if idempotency_key == self.fail_key && !self.has_failed.swap(true, Ordering::SeqCst) {
            Err(OutboxTransitionError::StoreFailed {
                message: "injected mark_sent failure for test".to_string(),
            })
        } else {
            self.inner.mark_sent(idempotency_key, result).await
        }
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
impl NormalizationAuditPort for FailKeyMarkSentOutbox {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.inner.append(event).await
    }
}

// ---------------------------------------------------------------------------
// Test 8: drain_once treats Rejected{current:Sent} from mark_retryable_failure
//          as a benign lease race (another worker delivered before we could mark
//          the failure).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_treats_sent_race_on_retryable_mark_as_benign() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    let outbox: Arc<dyn NormalizationOutboxPort> = Arc::new(RetryableRaceOutbox {
        inner: inner.clone(),
    });
    // Failing submitter: submission will fail, triggering mark_retryable_failure.
    let submitter = Arc::new(AlwaysFailSubmitter);

    enqueue_and_fail(&inner, "key-race-retry", "tenant-1", "raw-race-retry").await;

    let summary = drain_once(outbox, submitter, &test_config())
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 1);
    assert_eq!(
        summary.lease_races, 1,
        "Rejected{{Sent}} from mark_retryable_failure must be counted as a lease race"
    );
    assert_eq!(
        summary.failed_retryable, 0,
        "must not be counted as failed_retryable when the race was benign"
    );
    assert_eq!(summary.submitted, 0);
    assert_eq!(summary.dead_lettered, 0);
}

// ---------------------------------------------------------------------------
// Test 9: drain_once treats Rejected{current:Sent} from mark_dead_letter
//          as a benign lease race (another worker delivered before we could
//          dead-letter the record).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_treats_sent_race_on_dead_letter_as_benign() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    let outbox: Arc<dyn NormalizationOutboxPort> = Arc::new(DeadLetterRaceOutbox {
        inner: inner.clone(),
    });
    let submitter = Arc::new(AlwaysOkSubmitter);

    // max_attempts=1 so that after one pre-failure the record's retry budget is
    // exhausted and drain_once calls mark_dead_letter.
    let config = DrainConfig {
        max_attempts: 1,
        ..test_config()
    };

    enqueue_and_fail(&inner, "key-race-dead", "tenant-1", "raw-race-dead").await;
    // attempts is now 1 == max_attempts → dead-letter path.

    let summary = drain_once(outbox, submitter, &config)
        .await
        .expect("drain_once must not error");

    assert_eq!(summary.claimed, 1);
    assert_eq!(
        summary.lease_races, 1,
        "Rejected{{Sent}} from mark_dead_letter must be counted as a lease race"
    );
    assert_eq!(
        summary.dead_lettered, 0,
        "must not be counted as dead_lettered when the race was benign"
    );
    assert_eq!(summary.submitted, 0);
    assert_eq!(summary.failed_retryable, 0);
}

// ---------------------------------------------------------------------------
// Test 10: drain_once continues after mark_sent StoreFailed for one record —
//           the second (healthy) record is still submitted successfully and the
//           batch returns Ok with submitted == 1, no lease_races increment.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drain_once_continues_after_mark_sent_store_failure() {
    let inner = Arc::new(InMemoryWorkflowState::default());
    // mark_sent fails exactly once for "key-store-fail"; "key-store-ok" succeeds.
    let outbox: Arc<dyn NormalizationOutboxPort> = Arc::new(FailKeyMarkSentOutbox {
        inner: inner.clone(),
        fail_key: "key-store-fail".to_string(),
        has_failed: AtomicBool::new(false),
    });
    let submitter = Arc::new(AlwaysOkSubmitter);

    // Enqueue both records as retryable so they are claimable.
    enqueue_and_fail(&inner, "key-store-fail", "tenant-1", "raw-store-fail").await;
    enqueue_and_fail(&inner, "key-store-ok", "tenant-1", "raw-store-ok").await;

    let summary = drain_once(outbox, submitter, &test_config())
        .await
        .expect("drain_once must return Ok even when mark_sent fails");

    assert_eq!(summary.claimed, 2, "both records must be claimed");
    assert_eq!(
        summary.submitted, 1,
        "healthy record must be counted as submitted"
    );
    assert_eq!(
        summary.lease_races, 0,
        "StoreFailed from mark_sent must not increment lease_races"
    );
    assert_eq!(summary.failed_retryable, 0);
    assert_eq!(summary.dead_lettered, 0);
    assert_eq!(summary.transition_failures.len(), 1);
    assert_eq!(
        summary.transition_failures[0].stage,
        intelligence_normalization_application::DrainTransitionStage::MarkSent
    );
    assert_eq!(
        summary.transition_failures[0].idempotency_key,
        "key-store-fail"
    );

    // The healthy record must now be in Sent state.
    let sent_ok = inner
        .get_sent("key-store-ok")
        .await
        .expect("get_sent should not error");
    assert!(sent_ok.is_some(), "key-store-ok must be Sent");

    // The record whose mark_sent failed is NOT in Sent state (still InFlight).
    let sent_fail = inner
        .get_sent("key-store-fail")
        .await
        .expect("get_sent should not error");
    assert!(
        sent_fail.is_none(),
        "key-store-fail must not be Sent after mark_sent StoreFailed"
    );
}
