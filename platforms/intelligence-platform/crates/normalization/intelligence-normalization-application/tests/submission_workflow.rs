#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationAuditEvent, NormalizationAuditPort,
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationSubmissionWorkflow, OutboxAcquireResult, OutboxTransitionError,
    SubmitProposalError, SubmitProposalEvent,
};
use intelligence_normalization_domain::{
    normalization_idempotency_key, NormalizationProposal, NormalizationRequest,
};
use serde_json::json;

#[tokio::test]
async fn rejected_validation_is_a_completed_non_submission_and_is_audited() {
    let outbox = Arc::new(TestOutbox::default());
    let audit = Arc::new(TestAudit::default());
    let submitter = Arc::new(TestSubmitter::successful());
    let workflow = workflow(outbox, audit.clone(), submitter.clone());
    let request = request();
    let mut proposal = proposal();
    proposal.confidence = 0.2;

    let execution = workflow.submit(request, proposal).await;

    let result = execution
        .outcome
        .expect("validation rejection is not an error");
    assert!(!result.submission_attempted);
    assert_eq!(
        result.metadata.get("reason").map(String::as_str),
        Some("validation_failed")
    );
    assert_eq!(submitter.calls(), 0);
    assert_eq!(
        audit.event_types(),
        vec!["normalization.proposal.validated"]
    );
    assert!(execution.events.is_empty());
}

#[tokio::test]
async fn sent_submission_is_deduplicated_without_a_second_provider_call() {
    let outbox = Arc::new(TestOutbox::default());
    let audit = Arc::new(TestAudit::default());
    let submitter = Arc::new(TestSubmitter::successful());
    let workflow = workflow(outbox, audit.clone(), submitter.clone());

    let first = workflow.submit(request(), proposal()).await;
    let second = workflow.submit(request(), proposal()).await;

    let first = first.outcome.expect("first submission must complete");
    assert!(first.submission_attempted);
    assert_eq!(first.outbox_status, Some(NormalizationOutboxStatus::Sent));
    let second = second.outcome.expect("duplicate submission must complete");
    assert!(!second.submission_attempted);
    assert_eq!(
        second.metadata.get("reason").map(String::as_str),
        Some("duplicate_sent")
    );
    assert_eq!(submitter.calls(), 1);
    assert_eq!(
        audit.event_types(),
        vec![
            "normalization.proposal.validated",
            "normalization.submission.sent",
            "normalization.proposal.validated",
            "normalization.submission.deduplicated",
        ]
    );
}

#[tokio::test]
async fn duplicate_key_with_changed_payload_returns_typed_mismatch_and_audits_it() {
    let outbox = Arc::new(TestOutbox::default());
    let audit = Arc::new(TestAudit::default());
    let submitter = Arc::new(TestSubmitter::successful());
    let workflow = workflow(outbox, audit.clone(), submitter.clone());
    workflow
        .submit(request(), proposal())
        .await
        .outcome
        .expect("initial submission must complete");
    let mut changed = proposal();
    changed.reasons = vec!["changed payload".to_string()];

    let execution = workflow.submit(request(), changed).await;

    assert_eq!(execution.outcome, Err(SubmitProposalError::PayloadMismatch));
    assert!(audit
        .event_types()
        .contains(&"normalization.submission.payload_mismatch"));
    assert_eq!(submitter.calls(), 1);
}

#[tokio::test]
async fn ambiguous_provider_failure_returns_reconcile_event_after_transition() {
    let outbox = Arc::new(TestOutbox::default());
    let audit = Arc::new(TestAudit::default());
    let submitter = Arc::new(TestSubmitter::ambiguous());
    let workflow = workflow(outbox.clone(), audit, submitter);

    let execution = workflow.submit(request(), proposal()).await;

    assert_eq!(
        execution.outcome,
        Err(SubmitProposalError::FoundationSubmissionFailed {
            safe_message: "foundation-platform submission outcome is ambiguous",
        })
    );
    assert_eq!(
        execution.events,
        vec![SubmitProposalEvent::ReconcileRequired {
            idempotency_key: normalization_idempotency_key(&request()),
        }]
    );
    assert_eq!(
        outbox.status(),
        Some(NormalizationOutboxStatus::ReconcileRequired)
    );
}

fn workflow(
    outbox: Arc<TestOutbox>,
    audit: Arc<TestAudit>,
    submitter: Arc<TestSubmitter>,
) -> NormalizationSubmissionWorkflow {
    NormalizationSubmissionWorkflow::new(outbox, audit, Some(submitter), Duration::from_secs(60))
}

fn request() -> NormalizationRequest {
    NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "foundation-platform-r2".to_string(),
        raw_record_id: "raw-1".to_string(),
        raw_record: json!({"name": "Acme"}),
        trace_context: TraceContext {
            trace_id: "trace-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            human_user_id: "staff-1".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: json!({"required": ["normalized_name"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "industrial_complex".to_string(),
        target_identity: json!({"industrial_complex_id": "complex-1"}),
        dictionaries: BTreeMap::new(),
    }
}

fn proposal() -> NormalizationProposal {
    NormalizationProposal {
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
    }
}

#[derive(Default)]
struct TestOutbox {
    record: Mutex<Option<NormalizationOutboxRecord>>,
}

impl TestOutbox {
    fn status(&self) -> Option<NormalizationOutboxStatus> {
        self.record
            .lock()
            .expect("test outbox mutex")
            .as_ref()
            .map(|record| record.status.clone())
    }

    fn transition(
        &self,
        idempotency_key: &str,
        status: NormalizationOutboxStatus,
        result: Option<FoundationSubmissionResult>,
        error: Option<String>,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        let mut guard = self.record.lock().expect("test outbox mutex");
        let record = guard.as_mut().ok_or(OutboxTransitionError::NotFound)?;
        if record.idempotency_key != idempotency_key {
            return Err(OutboxTransitionError::NotFound);
        }
        if record.status != NormalizationOutboxStatus::InFlight {
            return Err(OutboxTransitionError::Rejected {
                current: record.status.clone(),
                message: "test transition rejected".to_string(),
            });
        }
        record.status = status;
        record.attempts += 1;
        record.submission_result = result;
        record.last_error = error;
        Ok(record.clone())
    }
}

#[async_trait]
impl NormalizationOutboxPort for TestOutbox {
    async fn enqueue(
        &self,
        mut record: NormalizationOutboxRecord,
        _lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        let mut guard = self.record.lock().expect("test outbox mutex");
        let Some(existing) = guard.as_ref() else {
            record.status = NormalizationOutboxStatus::InFlight;
            *guard = Some(record);
            return Ok(OutboxAcquireResult::Acquired);
        };
        if existing.payload_fingerprint != record.payload_fingerprint {
            return Ok(OutboxAcquireResult::PayloadMismatch);
        }
        match existing.status {
            NormalizationOutboxStatus::Sent => Ok(OutboxAcquireResult::AlreadySent),
            NormalizationOutboxStatus::Pending | NormalizationOutboxStatus::InFlight => {
                Ok(OutboxAcquireResult::AlreadyInFlight)
            }
            _ => Err(OutboxTransitionError::Rejected {
                current: existing.status.clone(),
                message: "test terminal record".to_string(),
            }),
        }
    }

    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        Ok(self
            .record
            .lock()
            .expect("test outbox mutex")
            .as_ref()
            .filter(|record| {
                record.idempotency_key == idempotency_key
                    && record.status == NormalizationOutboxStatus::Sent
            })
            .cloned())
    }

    async fn mark_sent(
        &self,
        idempotency_key: &str,
        result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(
            idempotency_key,
            NormalizationOutboxStatus::Sent,
            Some(result),
            None,
        )
    }

    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(
            idempotency_key,
            NormalizationOutboxStatus::FailedRetryable,
            None,
            Some(error),
        )
    }

    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(
            idempotency_key,
            NormalizationOutboxStatus::DeadLetter,
            None,
            Some(error),
        )
    }

    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(
            idempotency_key,
            NormalizationOutboxStatus::FailedTerminal,
            None,
            Some(error),
        )
    }

    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.transition(
            idempotency_key,
            NormalizationOutboxStatus::ReconcileRequired,
            None,
            Some(error),
        )
    }

    async fn claim_next_pending(
        &self,
        _limit: usize,
        _lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct TestAudit {
    events: Mutex<Vec<NormalizationAuditEvent>>,
}

impl TestAudit {
    fn event_types(&self) -> Vec<&'static str> {
        self.events
            .lock()
            .expect("test audit mutex")
            .iter()
            .map(|event| match event.event_type.as_str() {
                "normalization.proposal.validated" => "normalization.proposal.validated",
                "normalization.submission.sent" => "normalization.submission.sent",
                "normalization.submission.deduplicated" => "normalization.submission.deduplicated",
                "normalization.submission.payload_mismatch" => {
                    "normalization.submission.payload_mismatch"
                }
                other => panic!("unexpected audit event {other}"),
            })
            .collect()
    }
}

#[async_trait]
impl NormalizationAuditPort for TestAudit {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        self.events.lock().expect("test audit mutex").push(event);
        Ok(())
    }
}

enum SubmitterMode {
    Success,
    Ambiguous,
}

struct TestSubmitter {
    mode: SubmitterMode,
    calls: AtomicUsize,
}

impl TestSubmitter {
    fn successful() -> Self {
        Self {
            mode: SubmitterMode::Success,
            calls: AtomicUsize::new(0),
        }
    }

    fn ambiguous() -> Self {
        Self {
            mode: SubmitterMode::Ambiguous,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl FoundationNormalizationSubmitter for TestSubmitter {
    async fn submit(
        &self,
        _submission: &intelligence_normalization_application::NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            SubmitterMode::Success => Ok(FoundationSubmissionResult {
                submission_id: "submission-1".to_string(),
                status: FoundationSubmissionStatus::Queued,
                review_required: true,
                platform: "foundation-platform".to_string(),
                metadata: BTreeMap::new(),
            }),
            SubmitterMode::Ambiguous => Err(FoundationSubmissionError::AmbiguousOutcome {
                message: "provider accepted request before connection reset".to_string(),
            }),
        }
    }
}
