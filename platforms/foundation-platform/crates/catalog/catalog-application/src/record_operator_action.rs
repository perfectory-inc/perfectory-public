//! Use case for recording an operator switch of a scheduled job (root ADR-0282 §1, §2).

use std::sync::Arc;

use catalog_domain::operator_action::{
    check_reason, OperatorAction, OperatorActionError, RUN_NOW_EXPIRY_MINUTES,
};
use chrono::{DateTime, Duration, Utc};
use foundation_shared_kernel::ids::StaffId;
use uuid::Uuid;

use crate::ports::{OperatorActionLedger, OperatorActionLedgerError, OperatorActionRecord};

/// A request as the caller sent it, for a job the caller has checked is listed.
pub struct RecordOperatorActionInput {
    /// The job's id in `orchestration/jobs.v1.json`.
    pub job_id: String,
    /// `pause`, `resume`, `run_now` or `stop`.
    pub action: String,
    /// Drop the pending runs too (a pause only).
    pub drop_pending: bool,
    /// Why, in one line.
    pub reason: String,
    /// The authorized staff member.
    pub actor: StaffId,
}

/// Why a request was not recorded.
#[derive(Debug, thiserror::Error)]
pub enum RecordOperatorActionError {
    /// The request failed a rule and never reached the ledger.
    #[error(transparent)]
    Invalid(#[from] OperatorActionError),
    /// The ledger could not be written; nothing was recorded.
    #[error(transparent)]
    Ledger(#[from] OperatorActionLedgerError),
}

/// Checks a request and appends it to the operator action ledger.
pub struct RecordOperatorAction {
    ledger: Arc<dyn OperatorActionLedger>,
}

impl RecordOperatorAction {
    /// Creates the use case over a ledger.
    #[must_use]
    pub fn new(ledger: Arc<dyn OperatorActionLedger>) -> Self {
        Self { ledger }
    }

    /// Records one request at `now`; a `run_now` expires [`RUN_NOW_EXPIRY_MINUTES`] later.
    ///
    /// # Errors
    ///
    /// Returns [`RecordOperatorActionError::Invalid`] for an unknown action, a reason that is not
    /// one line of 1 to 300 characters, or `drop_pending` on anything but a pause; the ledger is
    /// not called. Returns [`RecordOperatorActionError::Ledger`] when the ledger cannot be written.
    pub async fn execute(
        &self,
        input: RecordOperatorActionInput,
        now: DateTime<Utc>,
    ) -> Result<OperatorActionRecord, RecordOperatorActionError> {
        let action = OperatorAction::parse(&input.action)?;
        check_reason(&input.reason)?;
        if input.drop_pending && action != OperatorAction::Pause {
            return Err(OperatorActionError::DropPendingIsAPause.into());
        }
        let record = OperatorActionRecord {
            id: Uuid::new_v4(),
            job_id: input.job_id,
            action,
            drop_pending: input.drop_pending,
            actor: input.actor,
            reason: input.reason,
            requested_at: now,
            expires_at: action
                .expires()
                .then(|| now + Duration::minutes(RUN_NOW_EXPIRY_MINUTES)),
        };
        self.ledger.record(&record).await?;
        Ok(record)
    }
}
