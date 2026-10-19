//! An operator switch of a scheduled job and the rules a request must meet (root ADR-0282).
//!
//! The ledger (`catalog.operator_action`) refuses the same things; these are checked first so the
//! caller is told which rule failed instead of receiving a database error.

/// How long a `run_now` request may wait to be consumed (root ADR-0282 §4).
pub const RUN_NOW_EXPIRY_MINUTES: i64 = 10;

/// The longest reason the ledger keeps, in characters.
pub const MAX_REASON_CHARS: usize = 300;

/// What an operator asks of one scheduled job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperatorAction {
    /// Start no new run until resumed.
    Pause,
    /// Undo a pause.
    Resume,
    /// Run once now, through the same queue as a scheduled start.
    RunNow,
    /// End the run in progress.
    Stop,
}

impl OperatorAction {
    /// The wire and ledger name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::RunNow => "run_now",
            Self::Stop => "stop",
        }
    }

    /// Parses the wire name.
    ///
    /// # Errors
    /// Returns [`OperatorActionError::UnknownAction`] for anything but the four names.
    pub fn parse(value: &str) -> Result<Self, OperatorActionError> {
        match value {
            "pause" => Ok(Self::Pause),
            "resume" => Ok(Self::Resume),
            "run_now" => Ok(Self::RunNow),
            "stop" => Ok(Self::Stop),
            _ => Err(OperatorActionError::UnknownAction),
        }
    }

    /// Whether the request expires (only `run_now`, root ADR-0282 §4).
    #[must_use]
    pub const fn expires(self) -> bool {
        matches!(self, Self::RunNow)
    }
}

/// Why an operator request is refused before it reaches the ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OperatorActionError {
    /// The action is not `pause`, `resume`, `run_now` or `stop`.
    #[error("the action is not pause, resume, run_now or stop")]
    UnknownAction,
    /// The reason is blank, longer than [`MAX_REASON_CHARS`], or holds a control character.
    #[error("the reason must be one line of 1 to 300 characters")]
    InvalidReason,
    /// `drop_pending` was asked of an action other than `pause`.
    #[error("only a pause drops pending runs")]
    DropPendingIsAPause,
}

/// Checks a reason the way the ledger does: not blank, at most [`MAX_REASON_CHARS`], no control
/// character (one line).
///
/// # Errors
/// Returns [`OperatorActionError::InvalidReason`] when it is not.
pub fn check_reason(reason: &str) -> Result<(), OperatorActionError> {
    let fits = !reason.trim().is_empty()
        && reason.chars().count() <= MAX_REASON_CHARS
        && !reason.chars().any(char::is_control);
    if fits {
        Ok(())
    } else {
        Err(OperatorActionError::InvalidReason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_four_actions_round_trip_and_nothing_else_parses() {
        for action in [
            OperatorAction::Pause,
            OperatorAction::Resume,
            OperatorAction::RunNow,
            OperatorAction::Stop,
        ] {
            assert_eq!(OperatorAction::parse(action.as_str()), Ok(action));
        }
        for wrong in ["", "Pause", "run-now", "drop", "pause "] {
            assert_eq!(
                OperatorAction::parse(wrong),
                Err(OperatorActionError::UnknownAction)
            );
        }
        assert!(OperatorAction::RunNow.expires());
        assert!(!OperatorAction::Pause.expires());
    }

    #[test]
    fn a_reason_is_one_line_of_one_to_three_hundred_characters() {
        assert_eq!(check_reason("원천 회차 확인 뒤 한 번 돌림"), Ok(()));
        assert_eq!(check_reason(&"가".repeat(300)), Ok(()));
        for wrong in [
            String::new(),
            "   ".to_owned(),
            "two\nlines".to_owned(),
            "a\ttab".to_owned(),
            "가".repeat(301),
        ] {
            assert_eq!(
                check_reason(&wrong),
                Err(OperatorActionError::InvalidReason)
            );
        }
    }
}
