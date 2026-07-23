use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionFailureClass,
    NormalizationOutboxPort, NormalizationOutboxStatus, OutboxTransitionError,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainTransitionStage {
    MarkSent,
    RecordSubmissionFailure,
    MarkDeadLetter,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainTransitionClass {
    SuccessfulSubmission,
    Retryable,
    Terminal,
    ReconcileRequired,
    RetryBudgetExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainTransitionCause {
    NotFound,
    StoreFailed,
    Rejected { current: NormalizationOutboxStatus },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DrainTransitionFailure {
    pub idempotency_key: String,
    pub stage: DrainTransitionStage,
    pub class: DrainTransitionClass,
    pub cause: DrainTransitionCause,
    pub safe_diagnostic: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainOutcomeKind {
    SubmissionFailureRecorded,
    DeadLettered,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DrainOutcomeEvent {
    pub idempotency_key: String,
    pub kind: DrainOutcomeKind,
    pub class: DrainTransitionClass,
    #[serde(default)]
    pub attempts: Option<u32>,
    #[serde(default)]
    pub safe_diagnostic: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct DrainSummary {
    pub claimed: usize,
    pub submitted: usize,
    pub failed_retryable: usize,
    pub failed_terminal: usize,
    pub dead_lettered: usize,
    pub reconcile_required: usize,
    pub lease_races: usize,
    #[serde(default)]
    pub transition_failures: Vec<DrainTransitionFailure>,
    #[serde(default)]
    pub outcome_events: Vec<DrainOutcomeEvent>,
}

#[derive(Clone, Debug)]
pub struct DrainOnceConfig {
    pub batch_size: usize,
    pub lease: Duration,
    pub max_attempts: u32,
}

pub async fn record_submission_failure(
    outbox: Arc<dyn NormalizationOutboxPort>,
    idempotency_key: &str,
    error: &FoundationSubmissionError,
) -> Result<NormalizationOutboxStatus, OutboxTransitionError> {
    record_submission_failure_record(outbox, idempotency_key, error)
        .await
        .map(|record| record.status)
}

async fn record_submission_failure_record(
    outbox: Arc<dyn NormalizationOutboxPort>,
    idempotency_key: &str,
    error: &FoundationSubmissionError,
) -> Result<crate::NormalizationOutboxRecord, OutboxTransitionError> {
    match error.failure_class() {
        FoundationSubmissionFailureClass::Retryable => {
            outbox
                .mark_retryable_failure(idempotency_key, error.to_string())
                .await
        }
        FoundationSubmissionFailureClass::Terminal => {
            outbox
                .mark_terminal_failure(idempotency_key, error.to_string())
                .await
        }
        FoundationSubmissionFailureClass::ReconcileRequired => {
            outbox
                .mark_reconcile_required(idempotency_key, error.to_string())
                .await
        }
    }
}

async fn apply_submission_failure(
    outbox: &Arc<dyn NormalizationOutboxPort>,
    idempotency_key: &str,
    error: &FoundationSubmissionError,
    summary: &mut DrainSummary,
) {
    let class = failure_class(error.failure_class());
    match record_submission_failure_record(outbox.clone(), idempotency_key, error).await {
        Ok(record) => {
            match class {
                DrainTransitionClass::Retryable => summary.failed_retryable += 1,
                DrainTransitionClass::Terminal => summary.failed_terminal += 1,
                DrainTransitionClass::ReconcileRequired => summary.reconcile_required += 1,
                DrainTransitionClass::SuccessfulSubmission
                | DrainTransitionClass::RetryBudgetExhausted => {}
            }
            summary.outcome_events.push(DrainOutcomeEvent {
                idempotency_key: idempotency_key.to_string(),
                kind: DrainOutcomeKind::SubmissionFailureRecorded,
                class,
                attempts: Some(record.attempts),
                safe_diagnostic: Some(error.to_string()),
            });
        }
        Err(transition_error) => {
            record_transition_failure(
                summary,
                idempotency_key,
                DrainTransitionStage::RecordSubmissionFailure,
                class,
                transition_error,
            );
        }
    }
}

pub async fn drain_once(
    outbox: Arc<dyn NormalizationOutboxPort>,
    submitter: Arc<dyn FoundationNormalizationSubmitter>,
    config: &DrainOnceConfig,
) -> Result<DrainSummary, OutboxTransitionError> {
    let records = outbox
        .claim_next_pending(config.batch_size, config.lease)
        .await?;
    let mut summary = DrainSummary {
        claimed: records.len(),
        ..DrainSummary::default()
    };

    for record in &records {
        let key = record.idempotency_key.as_str();
        if record.attempts >= config.max_attempts {
            let reason = format!("retry budget exhausted after {} attempts", record.attempts);
            match outbox.mark_dead_letter(key, reason).await {
                Ok(_) => {
                    summary.dead_lettered += 1;
                    summary.outcome_events.push(DrainOutcomeEvent {
                        idempotency_key: key.to_string(),
                        kind: DrainOutcomeKind::DeadLettered,
                        class: DrainTransitionClass::RetryBudgetExhausted,
                        attempts: Some(record.attempts),
                        safe_diagnostic: None,
                    });
                }
                Err(transition_error) => record_transition_failure(
                    &mut summary,
                    key,
                    DrainTransitionStage::MarkDeadLetter,
                    DrainTransitionClass::RetryBudgetExhausted,
                    transition_error,
                ),
            }
            continue;
        }

        match submitter.submit(&record.submission).await {
            Ok(result) => match outbox.mark_sent(key, result).await {
                Ok(_) => summary.submitted += 1,
                Err(transition_error) => record_transition_failure(
                    &mut summary,
                    key,
                    DrainTransitionStage::MarkSent,
                    DrainTransitionClass::SuccessfulSubmission,
                    transition_error,
                ),
            },
            Err(submit_error) => {
                apply_submission_failure(&outbox, key, &submit_error, &mut summary).await;
            }
        }
    }

    Ok(summary)
}

fn failure_class(class: FoundationSubmissionFailureClass) -> DrainTransitionClass {
    match class {
        FoundationSubmissionFailureClass::Retryable => DrainTransitionClass::Retryable,
        FoundationSubmissionFailureClass::Terminal => DrainTransitionClass::Terminal,
        FoundationSubmissionFailureClass::ReconcileRequired => {
            DrainTransitionClass::ReconcileRequired
        }
    }
}

fn record_transition_failure(
    summary: &mut DrainSummary,
    idempotency_key: &str,
    stage: DrainTransitionStage,
    class: DrainTransitionClass,
    error: OutboxTransitionError,
) {
    if matches!(
        &error,
        OutboxTransitionError::Rejected {
            current: NormalizationOutboxStatus::Sent,
            ..
        }
    ) {
        summary.lease_races += 1;
        return;
    }
    summary.transition_failures.push(DrainTransitionFailure {
        idempotency_key: idempotency_key.to_string(),
        stage,
        class,
        cause: transition_cause(&error),
        safe_diagnostic: error.safe_message().to_string(),
    });
}

fn transition_cause(error: &OutboxTransitionError) -> DrainTransitionCause {
    match error {
        OutboxTransitionError::NotFound => DrainTransitionCause::NotFound,
        OutboxTransitionError::StoreFailed { .. } => DrainTransitionCause::StoreFailed,
        OutboxTransitionError::Rejected { current, .. } => DrainTransitionCause::Rejected {
            current: current.clone(),
        },
    }
}
