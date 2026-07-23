use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use intelligence_contracts::TraceContext;
use intelligence_normalization_domain::{
    normalization_idempotency_key, validate_normalization_proposal, NormalizationProposal,
    NormalizationRequest, NormalizationValidationResult,
};

use crate::{
    record_submission_failure, FoundationNormalizationSubmitter, NormalizationAuditEvent,
    NormalizationAuditPort, NormalizationOutboxPort, NormalizationOutboxRecord,
    NormalizationOutboxStatus, NormalizationProposalSubmission, NormalizationRunResult,
    NormalizationSubmissionRunResult, OutboxAcquireResult, OutboxTransitionError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitProposalError {
    SubmitterNotConfigured,
    SubmissionNotRetryable,
    PayloadMismatch,
    SubmissionInProgress,
    AuditAppendFailed,
    OutboxStoreFailed { safe_message: &'static str },
    FoundationSubmissionFailed { safe_message: &'static str },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitProposalEvent {
    AlreadySentRecordMissing {
        idempotency_key: String,
    },
    MarkSentLeaseRace {
        idempotency_key: String,
    },
    DeliveredMarkSentFailed {
        idempotency_key: String,
    },
    SubmissionFailureRecordingFailed {
        idempotency_key: String,
        safe_diagnostic: String,
    },
    ReconcileRequired {
        idempotency_key: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubmitProposalExecution {
    pub outcome: Result<NormalizationSubmissionRunResult, SubmitProposalError>,
    pub events: Vec<SubmitProposalEvent>,
}

pub struct NormalizationSubmissionWorkflow {
    outbox: Arc<dyn NormalizationOutboxPort>,
    audit: Arc<dyn NormalizationAuditPort>,
    submitter: Option<Arc<dyn FoundationNormalizationSubmitter>>,
    lease: Duration,
}

impl NormalizationSubmissionWorkflow {
    pub fn new(
        outbox: Arc<dyn NormalizationOutboxPort>,
        audit: Arc<dyn NormalizationAuditPort>,
        submitter: Option<Arc<dyn FoundationNormalizationSubmitter>>,
        lease: Duration,
    ) -> Self {
        Self {
            outbox,
            audit,
            submitter,
            lease,
        }
    }

    pub async fn submit(
        &self,
        request: NormalizationRequest,
        proposal: NormalizationProposal,
    ) -> SubmitProposalExecution {
        let validation = validate_normalization_proposal(&request, &proposal);
        let generation = run_result(&request, proposal.clone(), validation.clone());
        let idempotency_key = normalization_idempotency_key(&request);
        let mut events = Vec::new();

        if self
            .append_audit(
                "normalization.proposal.validated",
                request.trace_context.clone(),
                metadata([
                    ("raw_record_id", request.raw_record_id.as_str()),
                    (
                        "accepted",
                        if validation.accepted { "true" } else { "false" },
                    ),
                ]),
            )
            .await
            .is_err()
        {
            return execution(Err(SubmitProposalError::AuditAppendFailed), events);
        }

        if !validation.accepted {
            return execution(
                Ok(NormalizationSubmissionRunResult {
                    generation,
                    submission_attempted: false,
                    submission_result: None,
                    idempotency_key: Some(idempotency_key),
                    outbox_status: None,
                    metadata: metadata([
                        ("reason", "validation_failed"),
                        ("raw_record_id", request.raw_record_id.as_str()),
                    ]),
                }),
                events,
            );
        }

        let Some(submitter) = self.submitter.as_ref() else {
            return execution(Err(SubmitProposalError::SubmitterNotConfigured), events);
        };
        let submission = NormalizationProposalSubmission {
            request: request.clone(),
            proposal,
            validation,
            trace_context: request.trace_context.clone(),
            commit_allowed: false,
            requires_human_review: true,
            submission_metadata: metadata([
                ("source_system", request.source_system.as_str()),
                ("raw_record_id", request.raw_record_id.as_str()),
                (
                    "target_schema_version",
                    request.target_schema_version.as_str(),
                ),
            ]),
        };
        let record = NormalizationOutboxRecord::new(idempotency_key.clone(), submission.clone());

        match self.outbox.enqueue(record, self.lease).await {
            Err(OutboxTransitionError::Rejected { .. }) => {
                return execution(Err(SubmitProposalError::SubmissionNotRetryable), events);
            }
            Err(error) => return execution(Err(outbox_error(&error)), events),
            Ok(OutboxAcquireResult::PayloadMismatch) => {
                if self
                    .append_audit(
                        "normalization.submission.payload_mismatch",
                        request.trace_context.clone(),
                        metadata([
                            ("raw_record_id", request.raw_record_id.as_str()),
                            ("idempotency_key", idempotency_key.as_str()),
                        ]),
                    )
                    .await
                    .is_err()
                {
                    return execution(Err(SubmitProposalError::AuditAppendFailed), events);
                }
                return execution(Err(SubmitProposalError::PayloadMismatch), events);
            }
            Ok(OutboxAcquireResult::AlreadyInFlight) => {
                return execution(Err(SubmitProposalError::SubmissionInProgress), events);
            }
            Ok(OutboxAcquireResult::AlreadySent) => {
                let sent = match self.outbox.get_sent(&idempotency_key).await {
                    Ok(sent) => sent,
                    Err(error) => return execution(Err(outbox_error(&error)), events),
                };
                if self
                    .append_audit(
                        "normalization.submission.deduplicated",
                        request.trace_context.clone(),
                        metadata([
                            ("raw_record_id", request.raw_record_id.as_str()),
                            ("idempotency_key", idempotency_key.as_str()),
                        ]),
                    )
                    .await
                    .is_err()
                {
                    return execution(Err(SubmitProposalError::AuditAppendFailed), events);
                }
                let (submission_result, outbox_status) = match sent {
                    Some(record) => (record.submission_result, Some(record.status)),
                    None => {
                        events.push(SubmitProposalEvent::AlreadySentRecordMissing {
                            idempotency_key: idempotency_key.clone(),
                        });
                        (None, None)
                    }
                };
                return execution(
                    Ok(duplicate_result(
                        generation,
                        &request,
                        idempotency_key,
                        submission_result,
                        outbox_status,
                    )),
                    events,
                );
            }
            Ok(OutboxAcquireResult::Acquired) => {}
        }

        match submitter.submit(&submission).await {
            Ok(result) => {
                match self
                    .outbox
                    .mark_sent(&idempotency_key, result.clone())
                    .await
                {
                    Ok(_) => {}
                    Err(OutboxTransitionError::Rejected {
                        current: NormalizationOutboxStatus::Sent,
                        ..
                    }) => {
                        events.push(SubmitProposalEvent::MarkSentLeaseRace {
                            idempotency_key: idempotency_key.clone(),
                        });
                        let sent = match self.outbox.get_sent(&idempotency_key).await {
                            Ok(Some(sent)) => sent,
                            Ok(None) => {
                                return execution(
                                    Err(SubmitProposalError::OutboxStoreFailed {
                                        safe_message: "outbox store failed",
                                    }),
                                    events,
                                );
                            }
                            Err(error) => return execution(Err(outbox_error(&error)), events),
                        };
                        return execution(
                            Ok(duplicate_result(
                                generation,
                                &request,
                                idempotency_key,
                                sent.submission_result,
                                Some(sent.status),
                            )),
                            events,
                        );
                    }
                    Err(error) => {
                        events.push(SubmitProposalEvent::DeliveredMarkSentFailed {
                            idempotency_key: idempotency_key.clone(),
                        });
                        return execution(Err(outbox_error(&error)), events);
                    }
                }

                if self
                    .append_audit(
                        "normalization.submission.sent",
                        request.trace_context,
                        metadata([
                            ("raw_record_id", request.raw_record_id.as_str()),
                            ("idempotency_key", idempotency_key.as_str()),
                            ("submission_id", result.submission_id.as_str()),
                        ]),
                    )
                    .await
                    .is_err()
                {
                    return execution(Err(SubmitProposalError::AuditAppendFailed), events);
                }
                execution(
                    Ok(NormalizationSubmissionRunResult {
                        generation,
                        submission_attempted: true,
                        submission_result: Some(result),
                        idempotency_key: Some(idempotency_key),
                        outbox_status: Some(NormalizationOutboxStatus::Sent),
                        metadata: metadata([("raw_record_id", request.raw_record_id.as_str())]),
                    }),
                    events,
                )
            }
            Err(error) => {
                match record_submission_failure(self.outbox.clone(), &idempotency_key, &error).await
                {
                    Ok(NormalizationOutboxStatus::ReconcileRequired) => {
                        events.push(SubmitProposalEvent::ReconcileRequired {
                            idempotency_key: idempotency_key.clone(),
                        });
                    }
                    Ok(_) => {}
                    Err(OutboxTransitionError::Rejected {
                        current: NormalizationOutboxStatus::Sent,
                        ..
                    }) => {}
                    Err(transition_error) => {
                        events.push(SubmitProposalEvent::SubmissionFailureRecordingFailed {
                            idempotency_key: idempotency_key.clone(),
                            safe_diagnostic: transition_error.to_string(),
                        });
                    }
                }
                execution(
                    Err(SubmitProposalError::FoundationSubmissionFailed {
                        safe_message: error.safe_message(),
                    }),
                    events,
                )
            }
        }
    }

    async fn append_audit(
        &self,
        event_type: &str,
        trace_context: TraceContext,
        event_metadata: BTreeMap<String, String>,
    ) -> Result<(), OutboxTransitionError> {
        self.audit
            .append(NormalizationAuditEvent::new(
                event_type,
                trace_context,
                event_metadata,
            ))
            .await
    }
}

fn execution(
    outcome: Result<NormalizationSubmissionRunResult, SubmitProposalError>,
    events: Vec<SubmitProposalEvent>,
) -> SubmitProposalExecution {
    SubmitProposalExecution { outcome, events }
}

fn outbox_error(error: &OutboxTransitionError) -> SubmitProposalError {
    SubmitProposalError::OutboxStoreFailed {
        safe_message: error.safe_message(),
    }
}

fn duplicate_result(
    generation: NormalizationRunResult,
    request: &NormalizationRequest,
    idempotency_key: String,
    submission_result: Option<crate::FoundationSubmissionResult>,
    outbox_status: Option<NormalizationOutboxStatus>,
) -> NormalizationSubmissionRunResult {
    NormalizationSubmissionRunResult {
        generation,
        submission_attempted: false,
        submission_result,
        idempotency_key: Some(idempotency_key),
        outbox_status,
        metadata: metadata([
            ("reason", "duplicate_sent"),
            ("raw_record_id", request.raw_record_id.as_str()),
        ]),
    }
}

fn run_result(
    request: &NormalizationRequest,
    proposal: NormalizationProposal,
    validation: NormalizationValidationResult,
) -> NormalizationRunResult {
    NormalizationRunResult {
        proposal,
        validation,
        commit_allowed: false,
        requires_human_review: true,
        metadata: metadata([
            ("source_system", request.source_system.as_str()),
            ("raw_record_id", request.raw_record_id.as_str()),
            (
                "target_schema_version",
                request.target_schema_version.as_str(),
            ),
        ]),
    }
}

fn metadata<const N: usize>(items: [(&str, &str); N]) -> BTreeMap<String, String> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
