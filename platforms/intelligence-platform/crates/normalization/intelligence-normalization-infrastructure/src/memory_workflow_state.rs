use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use intelligence_normalization_application::{
    FoundationSubmissionResult, NormalizationAuditEvent, NormalizationAuditPort,
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationReconcileQueuePort, OutboxAcquireResult, OutboxTransitionError,
    ReconcileQueueStats,
};

type OutboxMap = BTreeMap<String, NormalizationOutboxRecord>;
type AuditLog = Vec<NormalizationAuditEvent>;

/// In-memory implementation of both [`NormalizationOutboxPort`] and
/// [`NormalizationAuditPort`] backed by `Arc<Mutex<_>>`.
///
/// Designed for testing and for use during the initial service deployment before
/// the Postgres adapter (Task 9) is available. The adapter is `Clone` so it can
/// be cheaply shared across Axum handler clones — all clones share the same
/// underlying maps.
///
/// Mutex poisoning is mapped to [`OutboxTransitionError::StoreFailed`] in all
/// trait-impl methods. The test-support [`InMemoryWorkflowState::audit_events`]
/// helper recovers from poisoning via `into_inner()` and returns whatever
/// events were recorded before the panic.
#[derive(Clone, Default)]
pub struct InMemoryWorkflowState {
    outbox: Arc<Mutex<OutboxMap>>,
    audit: Arc<Mutex<AuditLog>>,
}

impl InMemoryWorkflowState {
    /// Returns a snapshot of all audit events in insertion order.
    ///
    /// This is a test-support method; Task 8 integration tests use it to verify
    /// that audit events were appended correctly.
    pub fn audit_events(&self) -> Vec<NormalizationAuditEvent> {
        self.audit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn mark_failure_status(
        &self,
        idempotency_key: &str,
        error: String,
        status: NormalizationOutboxStatus,
        transition_name: &str,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        let mut map = lock_outbox(&self.outbox)?;
        let record = map
            .get_mut(idempotency_key)
            .ok_or(OutboxTransitionError::NotFound)?;

        if record.status != NormalizationOutboxStatus::InFlight {
            return Err(OutboxTransitionError::Rejected {
                current: record.status.clone(),
                message: format!("{transition_name} requires InFlight status"),
            });
        }

        record.status = status;
        record.attempts += 1;
        record.last_error = Some(error);
        record.claimed_until = None;
        record.updated_at = Utc::now();

        Ok(record.clone())
    }
}

fn lock_outbox(
    outbox: &Mutex<OutboxMap>,
) -> Result<std::sync::MutexGuard<'_, OutboxMap>, OutboxTransitionError> {
    outbox
        .lock()
        .map_err(|e| OutboxTransitionError::StoreFailed {
            message: format!("outbox mutex poisoned: {e}"),
        })
}

fn lock_audit(
    audit: &Mutex<AuditLog>,
) -> Result<std::sync::MutexGuard<'_, AuditLog>, OutboxTransitionError> {
    audit
        .lock()
        .map_err(|e| OutboxTransitionError::StoreFailed {
            message: format!("audit mutex poisoned: {e}"),
        })
}

fn convert_lease(lease: Duration) -> Result<chrono::Duration, OutboxTransitionError> {
    chrono::Duration::from_std(lease).map_err(|e| OutboxTransitionError::StoreFailed {
        message: format!("lease duration conversion failed (absurd duration): {e}"),
    })
}

fn is_claimable(record: &NormalizationOutboxRecord, now: DateTime<Utc>) -> bool {
    match &record.status {
        NormalizationOutboxStatus::Pending | NormalizationOutboxStatus::FailedRetryable => true,
        NormalizationOutboxStatus::InFlight => {
            // Eligible only when the lease has expired.
            record.claimed_until.is_none_or(|t| t <= now)
        }
        _ => false,
    }
}

#[async_trait]
impl NormalizationOutboxPort for InMemoryWorkflowState {
    async fn enqueue(
        &self,
        record: NormalizationOutboxRecord,
        lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        let lease_duration = convert_lease(lease)?;
        let now = Utc::now();
        let claimed_until = now + lease_duration;

        let mut map = lock_outbox(&self.outbox)?;

        if let Some(existing) = map.get(&record.idempotency_key) {
            // Different fingerprint is checked FIRST, regardless of status.
            if existing.payload_fingerprint != record.payload_fingerprint {
                return Ok(OutboxAcquireResult::PayloadMismatch);
            }

            // Same fingerprint: branch on status.
            return match &existing.status {
                NormalizationOutboxStatus::DeadLetter
                | NormalizationOutboxStatus::FailedTerminal
                | NormalizationOutboxStatus::ReconcileRequired => {
                    Err(OutboxTransitionError::Rejected {
                        current: existing.status.clone(),
                        message: "record is terminal; operator action required".to_string(),
                    })
                }
                NormalizationOutboxStatus::Sent => Ok(OutboxAcquireResult::AlreadySent),
                _ => Ok(OutboxAcquireResult::AlreadyInFlight),
            };
        }

        // New key: store as InFlight with the acquired lease.
        let mut stored = record;
        stored.status = NormalizationOutboxStatus::InFlight;
        stored.claimed_until = Some(claimed_until);
        stored.updated_at = now;

        map.insert(stored.idempotency_key.clone(), stored);
        Ok(OutboxAcquireResult::Acquired)
    }

    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        let map = lock_outbox(&self.outbox)?;
        Ok(map
            .get(idempotency_key)
            .filter(|r| r.status == NormalizationOutboxStatus::Sent)
            .cloned())
    }

    async fn mark_sent(
        &self,
        idempotency_key: &str,
        result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        let mut map = lock_outbox(&self.outbox)?;
        let record = map
            .get_mut(idempotency_key)
            .ok_or(OutboxTransitionError::NotFound)?;

        if record.status != NormalizationOutboxStatus::InFlight {
            return Err(OutboxTransitionError::Rejected {
                current: record.status.clone(),
                message: "mark_sent requires InFlight status".to_string(),
            });
        }

        record.status = NormalizationOutboxStatus::Sent;
        record.attempts += 1;
        record.last_error = None;
        record.submission_result = Some(result);
        record.claimed_until = None;
        record.updated_at = Utc::now();

        Ok(record.clone())
    }

    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::FailedRetryable,
            "mark_retryable_failure",
        )
    }

    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::DeadLetter,
            "mark_dead_letter",
        )
    }

    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::FailedTerminal,
            "mark_terminal_failure",
        )
    }

    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::ReconcileRequired,
            "mark_reconcile_required",
        )
    }

    async fn claim_next_pending(
        &self,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        let lease_duration = convert_lease(lease)?;
        let now = Utc::now();
        let claimed_until = now + lease_duration;

        let mut map = lock_outbox(&self.outbox)?;

        // Collect eligible keys with their updated_at for oldest-first ordering.
        let mut eligible: Vec<(String, DateTime<Utc>)> = map
            .iter()
            .filter(|(_, r)| is_claimable(r, now))
            .map(|(k, r)| (k.clone(), r.updated_at))
            .collect();

        // Oldest-first by updated_at ascending.
        eligible.sort_by_key(|(_, updated_at)| *updated_at);
        eligible.truncate(limit);

        let mut claimed = Vec::with_capacity(eligible.len());
        for (key, _) in &eligible {
            if let Some(record) = map.get_mut(key) {
                record.status = NormalizationOutboxStatus::InFlight;
                record.claimed_until = Some(claimed_until);
                record.updated_at = now;
                claimed.push(record.clone());
            }
        }

        Ok(claimed)
    }
}

#[async_trait]
impl NormalizationReconcileQueuePort for InMemoryWorkflowState {
    async fn stats(&self) -> Result<ReconcileQueueStats, OutboxTransitionError> {
        let map = lock_outbox(&self.outbox)?;
        let now = Utc::now();
        let mut depth = 0_u64;
        let mut oldest_age_seconds = 0.0_f64;

        for record in map.values() {
            if record.status == NormalizationOutboxStatus::ReconcileRequired {
                depth += 1;
                let age = (now - record.updated_at).num_milliseconds().max(0) as f64 / 1000.0;
                oldest_age_seconds = oldest_age_seconds.max(age);
            }
        }

        Ok(ReconcileQueueStats {
            depth,
            oldest_age_seconds,
        })
    }
}

#[async_trait]
impl NormalizationAuditPort for InMemoryWorkflowState {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        let mut log = lock_audit(&self.audit)?;
        log.push(event);
        Ok(())
    }
}
