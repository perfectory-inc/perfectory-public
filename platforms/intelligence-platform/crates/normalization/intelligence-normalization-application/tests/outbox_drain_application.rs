#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    drain_once, DrainOnceConfig, DrainOutcomeKind, DrainTransitionCause, DrainTransitionClass,
    DrainTransitionStage, FoundationNormalizationSubmitter, FoundationSubmissionError,
    FoundationSubmissionResult, FoundationSubmissionStatus, NormalizationOutboxPort,
    NormalizationOutboxRecord, NormalizationProposalSubmission, OutboxAcquireResult,
    OutboxTransitionError,
};
use intelligence_normalization_domain::normalization::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum FailureMode {
    None,
    MarkSent,
    RecordFailure,
    DeadLetter,
    LeaseRace,
}

struct TestOutbox {
    records: Vec<NormalizationOutboxRecord>,
    failure_mode: FailureMode,
    completed: Mutex<Vec<String>>,
}

impl TestOutbox {
    fn new(records: Vec<NormalizationOutboxRecord>, failure_mode: FailureMode) -> Self {
        Self {
            records,
            failure_mode,
            completed: Mutex::new(Vec::new()),
        }
    }

    fn transition(
        &self,
        key: &str,
        stage: FailureMode,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        if self.failure_mode == FailureMode::LeaseRace && key == "failed" {
            return Err(OutboxTransitionError::Rejected {
                current: intelligence_normalization_application::NormalizationOutboxStatus::Sent,
                message: "injected lease race".to_string(),
            });
        }
        if matches!(
            (self.failure_mode, stage),
            (FailureMode::MarkSent, FailureMode::MarkSent)
                | (FailureMode::RecordFailure, FailureMode::RecordFailure)
                | (FailureMode::DeadLetter, FailureMode::DeadLetter)
        ) && key == "failed"
        {
            return Err(OutboxTransitionError::StoreFailed {
                message: "injected transition failure".to_string(),
            });
        }
        self.completed.lock().unwrap().push(key.to_string());
        Ok(record(key))
    }
}

#[async_trait]
impl NormalizationOutboxPort for TestOutbox {
    async fn enqueue(
        &self,
        _: NormalizationOutboxRecord,
        _: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        Ok(OutboxAcquireResult::Acquired)
    }

    async fn get_sent(
        &self,
        _: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        Ok(None)
    }

    async fn mark_sent(
        &self,
        key: &str,
        _: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(key, FailureMode::MarkSent)
    }

    async fn mark_retryable_failure(
        &self,
        key: &str,
        _: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(key, FailureMode::RecordFailure)
    }

    async fn mark_dead_letter(
        &self,
        key: &str,
        _: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(key, FailureMode::DeadLetter)
    }

    async fn mark_terminal_failure(
        &self,
        key: &str,
        _: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(key, FailureMode::RecordFailure)
    }

    async fn mark_reconcile_required(
        &self,
        key: &str,
        _: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(key, FailureMode::RecordFailure)
    }

    async fn claim_next_pending(
        &self,
        _: usize,
        _: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        Ok(self.records.clone())
    }
}

struct OkSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for OkSubmitter {
    async fn submit(
        &self,
        _: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Ok(result())
    }
}

struct RetryableSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for RetryableSubmitter {
    async fn submit(
        &self,
        _: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::PreSendFailure {
            message: "injected send failure".to_string(),
        })
    }
}

struct TerminalSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for TerminalSubmitter {
    async fn submit(
        &self,
        _: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::Rejected {
            status: 422,
            body: "invalid".to_string(),
            retryable: false,
        })
    }
}

struct ReconcileSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for ReconcileSubmitter {
    async fn submit(
        &self,
        _: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Err(FoundationSubmissionError::AmbiguousOutcome {
            message: "ambiguous".to_string(),
        })
    }
}

#[tokio::test]
async fn mark_sent_failure_is_reported_without_aborting_the_batch() {
    let outbox = Arc::new(TestOutbox::new(
        vec![record("failed"), record("healthy")],
        FailureMode::MarkSent,
    ));
    let summary = drain_once(outbox.clone(), Arc::new(OkSubmitter), &config())
        .await
        .unwrap();

    assert_eq!(summary.submitted, 1);
    assert_eq!(summary.transition_failures.len(), 1);
    assert_eq!(summary.transition_failures[0].idempotency_key, "failed");
    assert_eq!(
        summary.transition_failures[0].stage,
        DrainTransitionStage::MarkSent
    );
    assert_eq!(
        summary.transition_failures[0].class,
        DrainTransitionClass::SuccessfulSubmission
    );
    assert_eq!(
        summary.transition_failures[0].safe_diagnostic,
        "outbox store failed"
    );
    assert_eq!(
        summary.transition_failures[0].cause,
        DrainTransitionCause::StoreFailed
    );
    assert_eq!(outbox.completed.lock().unwrap().as_slice(), ["healthy"]);
}

#[tokio::test]
async fn failure_recording_and_dead_letter_failures_are_reported() {
    let failure_outbox = Arc::new(TestOutbox::new(
        vec![record("failed")],
        FailureMode::RecordFailure,
    ));
    let failure_summary = drain_once(failure_outbox, Arc::new(RetryableSubmitter), &config())
        .await
        .unwrap();
    assert_eq!(
        failure_summary.transition_failures[0].stage,
        DrainTransitionStage::RecordSubmissionFailure
    );
    assert_eq!(
        failure_summary.transition_failures[0].class,
        DrainTransitionClass::Retryable
    );

    let mut exhausted = record("failed");
    exhausted.attempts = 1;
    let dead_letter_outbox = Arc::new(TestOutbox::new(vec![exhausted], FailureMode::DeadLetter));
    let dead_letter_summary = drain_once(
        dead_letter_outbox,
        Arc::new(OkSubmitter),
        &DrainOnceConfig {
            max_attempts: 1,
            ..config()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        dead_letter_summary.transition_failures[0].stage,
        DrainTransitionStage::MarkDeadLetter
    );
    assert_eq!(
        dead_letter_summary.transition_failures[0].class,
        DrainTransitionClass::RetryBudgetExhausted
    );
}

#[tokio::test]
async fn drain_once_classifies_success_retry_terminal_reconcile_and_lease_race() {
    let successful = drain_once(
        Arc::new(TestOutbox::new(vec![record("ok")], FailureMode::None)),
        Arc::new(OkSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(successful.submitted, 1);

    let retryable = drain_once(
        Arc::new(TestOutbox::new(vec![record("retry")], FailureMode::None)),
        Arc::new(RetryableSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(retryable.failed_retryable, 1);

    let terminal = drain_once(
        Arc::new(TestOutbox::new(vec![record("terminal")], FailureMode::None)),
        Arc::new(TerminalSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(terminal.failed_terminal, 1);

    let reconcile = drain_once(
        Arc::new(TestOutbox::new(
            vec![record("reconcile")],
            FailureMode::None,
        )),
        Arc::new(ReconcileSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(reconcile.reconcile_required, 1);

    let lease_race = drain_once(
        Arc::new(TestOutbox::new(
            vec![record("failed")],
            FailureMode::LeaseRace,
        )),
        Arc::new(OkSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(lease_race.lease_races, 1);
    assert!(lease_race.transition_failures.is_empty());
}

#[tokio::test]
async fn all_sent_race_stages_are_benign_and_not_actionable() {
    let mark_sent = drain_once(
        Arc::new(TestOutbox::new(
            vec![record("failed")],
            FailureMode::LeaseRace,
        )),
        Arc::new(OkSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(mark_sent.lease_races, 1);
    assert!(mark_sent.transition_failures.is_empty());

    let record_failure = drain_once(
        Arc::new(TestOutbox::new(
            vec![record("failed")],
            FailureMode::LeaseRace,
        )),
        Arc::new(RetryableSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(record_failure.lease_races, 1);
    assert!(record_failure.transition_failures.is_empty());

    let mut exhausted = record("failed");
    exhausted.attempts = 1;
    let dead_letter = drain_once(
        Arc::new(TestOutbox::new(vec![exhausted], FailureMode::LeaseRace)),
        Arc::new(OkSubmitter),
        &DrainOnceConfig {
            max_attempts: 1,
            ..config()
        },
    )
    .await
    .unwrap();
    assert_eq!(dead_letter.lease_races, 1);
    assert!(dead_letter.transition_failures.is_empty());
}

#[tokio::test]
async fn successful_dead_letter_and_transition_failure_batches_return_typed_outcomes() {
    let mut exhausted = record("dead");
    exhausted.attempts = 1;
    let dead_letter = drain_once(
        Arc::new(TestOutbox::new(vec![exhausted], FailureMode::None)),
        Arc::new(OkSubmitter),
        &DrainOnceConfig {
            max_attempts: 1,
            ..config()
        },
    )
    .await
    .unwrap();
    assert_eq!(dead_letter.dead_lettered, 1);
    assert_eq!(
        dead_letter.outcome_events[0].kind,
        DrainOutcomeKind::DeadLettered
    );
    assert_eq!(dead_letter.outcome_events[0].attempts, Some(1));

    let retry_batch = drain_once(
        Arc::new(TestOutbox::new(
            vec![record("failed"), record("healthy")],
            FailureMode::RecordFailure,
        )),
        Arc::new(RetryableSubmitter),
        &config(),
    )
    .await
    .unwrap();
    assert_eq!(retry_batch.transition_failures.len(), 1);
    assert_eq!(retry_batch.failed_retryable, 1);

    let mut failed = record("failed");
    failed.attempts = 1;
    let mut healthy = record("healthy");
    healthy.attempts = 1;
    let dead_letter_batch = drain_once(
        Arc::new(TestOutbox::new(
            vec![failed, healthy],
            FailureMode::DeadLetter,
        )),
        Arc::new(OkSubmitter),
        &DrainOnceConfig {
            max_attempts: 1,
            ..config()
        },
    )
    .await
    .unwrap();
    assert_eq!(dead_letter_batch.transition_failures.len(), 1);
    assert_eq!(dead_letter_batch.dead_lettered, 1);
}

fn config() -> DrainOnceConfig {
    DrainOnceConfig {
        batch_size: 8,
        lease: Duration::from_secs(60),
        max_attempts: 8,
    }
}

fn result() -> FoundationSubmissionResult {
    FoundationSubmissionResult {
        submission_id: "submission-1".to_string(),
        status: FoundationSubmissionStatus::Queued,
        review_required: true,
        platform: "foundation-platform".to_string(),
        metadata: BTreeMap::new(),
    }
}

fn record(key: &str) -> NormalizationOutboxRecord {
    NormalizationOutboxRecord::new(key.to_string(), submission(key))
}

fn submission(key: &str) -> NormalizationProposalSubmission {
    let trace_context = TraceContext {
        trace_id: format!("trace-{key}"),
        tenant_id: "tenant-1".to_string(),
        human_user_id: "user-1".to_string(),
        product_id: "foundation-platform".to_string(),
    };
    NormalizationProposalSubmission {
        request: NormalizationRequest {
            tenant_id: "tenant-1".to_string(),
            source_system: "test".to_string(),
            raw_record_id: key.to_string(),
            raw_record: serde_json::json!({}),
            trace_context: trace_context.clone(),
            target_schema: serde_json::json!({}),
            target_schema_version: "v1".to_string(),
            raw_object_key: None,
            raw_checksum_sha256: None,
            target_kind: "test".to_string(),
            target_identity: serde_json::json!({}),
            dictionaries: BTreeMap::new(),
        },
        proposal: NormalizationProposal {
            raw_record_id: key.to_string(),
            proposed_record: serde_json::json!({}),
            confidence: 1.0,
            reasons: vec![],
            schema_version: "v1".to_string(),
            policy_id: "policy".to_string(),
            policy_version: "v1".to_string(),
            model_profile_id: None,
            model_id: None,
            prompt_id: None,
            prompt_version: None,
        },
        validation: NormalizationValidationResult {
            accepted: true,
            raw_record_id: key.to_string(),
            confidence: 1.0,
            errors: vec![],
        },
        trace_context,
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    }
}
