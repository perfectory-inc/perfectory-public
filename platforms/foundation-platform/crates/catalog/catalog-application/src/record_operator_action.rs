//! Use case for recording an operator switch of a scheduled job (root ADR-0282 §1, §2).

use std::sync::Arc;

use catalog_domain::operator_action::{
    check_edition, check_generation, check_lanes, check_reason, check_window, is_by_pnu_bake,
    OperatorAction, OperatorActionError, RunWindow, CUTOVER_EXPIRY_HOURS, CUTOVER_SUBJECT,
    FORCE_FULL_EXPIRY_HOURS, PROMOTION_EXPIRY_HOURS, PROMOTION_JOB, RUN_NOW_EXPIRY_MINUTES,
};
use chrono::{DateTime, Duration, Utc};
use foundation_shared_kernel::ids::StaffId;
use uuid::Uuid;

use crate::ports::{
    JobSwitchBoard, JobSwitchBoardError, OperatorActionLedger, OperatorActionLedgerError,
    OperatorActionRecord,
};

/// A request as the caller sent it, for a job the caller has checked is listed.
pub struct RecordOperatorActionInput {
    /// The job's id in `orchestration/jobs.v1.json`.
    pub job_id: String,
    /// `pause`, `resume`, `run_now`, `stop`, `set_window` or `clear_window`.
    pub action: String,
    /// The window a `set_window` sets; none for the other actions.
    pub window: Option<RunWindow>,
    /// The edition a `promote_edition` asks to serve, `YYYYMM`; none for the other actions.
    pub edition: Option<String>,
    /// The lanes an `approve_cutover` cuts over, or the one lane a `force_full` re-bases; none
    /// for the other actions (root ADR-0433).
    pub lanes: Option<String>,
    /// The pack generation an `approve_cutover` cuts over at; none for the other actions.
    pub generation: Option<i32>,
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
    /// The switch board could not be written: before the ledger, nothing was recorded; after it,
    /// the request is in the ledger but not switched, and asking again switches it.
    #[error(transparent)]
    SwitchBoard(#[from] JobSwitchBoardError),
}

/// Checks a request, appends it to the operator action ledger, and switches the job on the host.
pub struct RecordOperatorAction {
    ledger: Arc<dyn OperatorActionLedger>,
    switches: Arc<dyn JobSwitchBoard>,
}

impl RecordOperatorAction {
    /// Creates the use case over a ledger and the host's switch board.
    #[must_use]
    pub fn new(ledger: Arc<dyn OperatorActionLedger>, switches: Arc<dyn JobSwitchBoard>) -> Self {
        Self { ledger, switches }
    }

    /// Records one request at `now`; a `run_now` expires [`RUN_NOW_EXPIRY_MINUTES`] later.
    ///
    /// # Errors
    ///
    /// Returns [`RecordOperatorActionError::Invalid`] for an unknown action, a reason that is not
    /// one line of 1 to 300 characters, or `drop_pending` on anything but a pause; the ledger is
    /// not called. Returns [`RecordOperatorActionError::SwitchBoard`] when the switch board cannot
    /// be written, before or after the ledger, and [`RecordOperatorActionError::Ledger`] when the
    /// ledger cannot be written.
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
        if (action == OperatorAction::SetWindow) != input.window.is_some() {
            return Err(OperatorActionError::WindowIsASetWindow.into());
        }
        if let Some(window) = &input.window {
            check_window(window)?;
        }
        match (&input.edition, action) {
            (Some(edition), OperatorAction::PromoteEdition) => check_edition(edition)?,
            (None, OperatorAction::PromoteEdition) | (Some(_), _) => {
                return Err(OperatorActionError::EditionIsAPromotion.into())
            }
            (None, _) => {}
        }
        if action == OperatorAction::PromoteEdition && input.job_id != PROMOTION_JOB {
            return Err(OperatorActionError::PromotionIsTheReadinessChecks.into());
        }
        check_release(&input, action)?;
        let expires_at = match (&input.window, action) {
            (Some(window), _) => Some(now + Duration::hours(window.hours)),
            (None, OperatorAction::RunNow) => Some(now + Duration::minutes(RUN_NOW_EXPIRY_MINUTES)),
            (None, OperatorAction::PromoteEdition) => {
                Some(now + Duration::hours(PROMOTION_EXPIRY_HOURS))
            }
            (None, OperatorAction::ApproveCutover) => {
                Some(now + Duration::hours(CUTOVER_EXPIRY_HOURS))
            }
            (None, OperatorAction::ForceFull) => {
                Some(now + Duration::hours(FORCE_FULL_EXPIRY_HOURS))
            }
            (None, _) => None,
        };
        let record = OperatorActionRecord {
            id: Uuid::new_v4(),
            job_id: input.job_id,
            action,
            drop_pending: input.drop_pending,
            actor: input.actor,
            reason: input.reason,
            requested_at: now,
            expires_at,
            window_start: input.window.as_ref().map(|window| window.start.clone()),
            window_end: input.window.map(|window| window.end),
            edition: input.edition,
            lanes: input.lanes,
            generation: input.generation,
        };
        self.switches.ready()?;
        self.ledger.record(&record).await?;
        self.switches.apply(&record)?;
        Ok(record)
    }
}

/// The rules of the two release approvals (root ADR-0433): a cut-over names its lanes and
/// generation and is the cut-over runner's; a full bake names one lane of a by-PNU bake job; the
/// runner takes nothing but a cut-over approval and a stop; nothing else names a lane or a
/// generation.
fn check_release(
    input: &RecordOperatorActionInput,
    action: OperatorAction,
) -> Result<(), OperatorActionError> {
    let runner = input.job_id == CUTOVER_SUBJECT;
    match action {
        OperatorAction::ApproveCutover | OperatorAction::Stop if runner => {}
        OperatorAction::ApproveCutover => return Err(OperatorActionError::CutoverIsTheRunners),
        _ if runner => return Err(OperatorActionError::CutoverIsTheRunners),
        _ => {}
    }
    if action == OperatorAction::ForceFull && !is_by_pnu_bake(&input.job_id) {
        return Err(OperatorActionError::FullBakeIsABakes);
    }
    match (&input.lanes, action) {
        (Some(lanes), OperatorAction::ApproveCutover) => check_lanes(lanes, false)?,
        (Some(lanes), OperatorAction::ForceFull) => check_lanes(lanes, true)?,
        (None, OperatorAction::ApproveCutover | OperatorAction::ForceFull) | (Some(_), _) => {
            return Err(OperatorActionError::LanesAreARelease)
        }
        (None, _) => {}
    }
    match (input.generation, action) {
        (Some(generation), OperatorAction::ApproveCutover) => check_generation(generation),
        (None, OperatorAction::ApproveCutover) | (Some(_), _) => {
            Err(OperatorActionError::GenerationIsACutover)
        }
        (None, _) => Ok(()),
    }
}
