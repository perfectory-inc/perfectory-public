//! Durable normalization outbox integration tests at the worker boundary.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationOutboxPort, NormalizationOutboxRecord,
    NormalizationOutboxStatus, NormalizationProposalSubmission, OutboxAcquireResult,
};
use intelligence_normalization_domain::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};
use intelligence_normalization_infrastructure::InMemoryWorkflowState;
use intelligence_worker::outbox_worker::{drain_once, record_submission_failure, DrainConfig};

struct SuccessfulSubmitter;

#[async_trait]
impl FoundationNormalizationSubmitter for SuccessfulSubmitter {
    async fn submit(
        &self,
        _: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        Ok(FoundationSubmissionResult {
            submission_id: "foundation-submission-1".to_string(),
            status: FoundationSubmissionStatus::Queued,
            review_required: true,
            platform: "foundation-platform".to_string(),
            metadata: BTreeMap::new(),
        })
    }
}

#[tokio::test]
async fn shared_outbox_preserves_deduplication_and_payload_mismatch() {
    let outbox = InMemoryWorkflowState::default();
    let record = outbox_record("durable-key", "raw-1");

    assert_eq!(
        outbox
            .enqueue(record.clone(), Duration::from_secs(60))
            .await
            .unwrap(),
        OutboxAcquireResult::Acquired
    );
    assert_eq!(
        outbox
            .enqueue(record, Duration::from_secs(60))
            .await
            .unwrap(),
        OutboxAcquireResult::AlreadyInFlight
    );
    assert_eq!(
        outbox
            .enqueue(
                outbox_record("durable-key", "raw-2"),
                Duration::from_secs(60),
            )
            .await
            .unwrap(),
        OutboxAcquireResult::PayloadMismatch
    );
}

#[tokio::test]
async fn worker_reclaims_retryable_records_and_marks_them_sent() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    let record = outbox_record("retryable-key", "raw-1");
    outbox
        .enqueue(record, Duration::from_secs(60))
        .await
        .unwrap();
    outbox
        .mark_retryable_failure("retryable-key", "temporary failure".to_string())
        .await
        .unwrap();

    let summary = drain_once(
        outbox.clone(),
        Arc::new(SuccessfulSubmitter),
        &DrainConfig {
            batch_size: 1,
            lease: Duration::from_secs(60),
            max_attempts: 8,
            idle_sleep: Duration::from_millis(1),
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.claimed, 1);
    assert_eq!(summary.submitted, 1);
    assert_eq!(
        outbox
            .get_sent("retryable-key")
            .await
            .unwrap()
            .unwrap()
            .status,
        NormalizationOutboxStatus::Sent
    );
}

#[tokio::test]
async fn worker_delegates_failure_classification_to_the_application() {
    let outbox = Arc::new(InMemoryWorkflowState::default());
    for key in ["ambiguous-key", "terminal-key"] {
        outbox
            .enqueue(outbox_record(key, key), Duration::from_secs(60))
            .await
            .unwrap();
    }

    let ambiguous = record_submission_failure(
        outbox.clone(),
        "ambiguous-key",
        &FoundationSubmissionError::AmbiguousOutcome {
            message: "delivery outcome unknown".to_string(),
        },
    )
    .await
    .unwrap();
    let terminal = record_submission_failure(
        outbox,
        "terminal-key",
        &FoundationSubmissionError::Rejected {
            status: 422,
            body: "rejected".to_string(),
            retryable: false,
        },
    )
    .await
    .unwrap();

    assert_eq!(ambiguous, NormalizationOutboxStatus::ReconcileRequired);
    assert_eq!(terminal, NormalizationOutboxStatus::FailedTerminal);
}

fn outbox_record(key: &str, raw_record_id: &str) -> NormalizationOutboxRecord {
    let trace_context = TraceContext {
        trace_id: format!("trace-{raw_record_id}"),
        tenant_id: "tenant-1".to_string(),
        human_user_id: "service:intelligence-platform".to_string(),
        product_id: "foundation-platform".to_string(),
    };
    let request = NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "foundation-platform".to_string(),
        raw_record_id: raw_record_id.to_string(),
        raw_record: serde_json::json!({"name": "Acme"}),
        trace_context: trace_context.clone(),
        target_schema: serde_json::json!({"required": ["normalized_name"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "industrial_complex".to_string(),
        target_identity: serde_json::json!({"industrial_complex_id": "complex-1"}),
        dictionaries: BTreeMap::new(),
    };
    let proposal = NormalizationProposal {
        raw_record_id: raw_record_id.to_string(),
        proposed_record: serde_json::json!({"normalized_name": "Acme"}),
        confidence: 0.91,
        reasons: vec!["field matched source name".to_string()],
        schema_version: "v1".to_string(),
        policy_id: "normalization-policy-v1".to_string(),
        policy_version: "v1".to_string(),
        model_profile_id: None,
        model_id: None,
        prompt_id: None,
        prompt_version: None,
    };
    let submission = NormalizationProposalSubmission {
        request,
        proposal,
        validation: NormalizationValidationResult {
            accepted: true,
            raw_record_id: raw_record_id.to_string(),
            confidence: 0.91,
            errors: Vec::new(),
        },
        trace_context,
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    };

    NormalizationOutboxRecord::new(key.to_string(), submission)
}
