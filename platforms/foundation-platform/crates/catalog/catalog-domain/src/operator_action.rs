//! An operator switch of a scheduled job and the rules a request must meet (root ADR-0282).
//!
//! The ledger (`catalog.operator_action`) refuses the same things; these are checked first so the
//! caller is told which rule failed instead of receiving a database error.

/// How long a `run_now` request may wait to be consumed (root ADR-0282 §4).
pub const RUN_NOW_EXPIRY_MINUTES: i64 = 10;

/// The longest reason the ledger keeps, in characters.
pub const MAX_REASON_CHARS: usize = 300;

/// The longest a window override lasts, in hours: the contract's window always comes back.
pub const MAX_WINDOW_HOURS: i64 = 7 * 24;

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
    /// Let the job run inside another window (KST) for a bounded time, instead of its contract's.
    SetWindow,
    /// End a window override early: the contract's window again.
    ClearWindow,
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
            Self::SetWindow => "set_window",
            Self::ClearWindow => "clear_window",
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
            "set_window" => Ok(Self::SetWindow),
            "clear_window" => Ok(Self::ClearWindow),
            _ => Err(OperatorActionError::UnknownAction),
        }
    }

    /// Whether the request expires: a `run_now` after [`RUN_NOW_EXPIRY_MINUTES`] (root ADR-0282 §4),
    /// a `set_window` after the hours it asked for.
    #[must_use]
    pub const fn expires(self) -> bool {
        matches!(self, Self::RunNow | Self::SetWindow)
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
    /// A window is not two `HH:MM` clock times, or lasts not 1 hour to a week.
    #[error("a window is two HH:MM times (KST) and lasts 1 to 168 hours")]
    InvalidWindow,
    /// A window was given to an action other than `set_window`, or none to `set_window`.
    #[error("only set_window takes a window, and it must")]
    WindowIsASetWindow,
}

/// A run window override: two KST clock times (start inclusive, end exclusive; across midnight
/// when the end comes first; the whole day when they are equal) and how many hours it lasts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunWindow {
    /// `HH:MM`, KST.
    pub start: String,
    /// `HH:MM`, KST.
    pub end: String,
    /// How long the override lasts, 1 to [`MAX_WINDOW_HOURS`].
    pub hours: i64,
}

const fn is_clock(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 5
        && bytes[2] == b':'
        && matches!(
            (bytes[0], bytes[1]),
            (b'0' | b'1', b'0'..=b'9') | (b'2', b'0'..=b'3')
        )
        && matches!((bytes[3], bytes[4]), (b'0'..=b'5', b'0'..=b'9'))
}

/// Checks a window the way the ledger does.
///
/// # Errors
/// Returns [`OperatorActionError::InvalidWindow`] when it is not.
pub fn check_window(window: &RunWindow) -> Result<(), OperatorActionError> {
    if is_clock(&window.start)
        && is_clock(&window.end)
        && (1..=MAX_WINDOW_HOURS).contains(&window.hours)
    {
        Ok(())
    } else {
        Err(OperatorActionError::InvalidWindow)
    }
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
            OperatorAction::SetWindow,
            OperatorAction::ClearWindow,
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
    fn a_window_is_two_clock_times_for_one_hour_to_a_week() {
        let window = |start: &str, end: &str, hours| RunWindow {
            start: start.to_owned(),
            end: end.to_owned(),
            hours,
        };
        for good in [
            window("09:00", "18:00", 2),
            window("22:00", "06:00", 168),
            window("00:00", "00:00", 1),
        ] {
            assert_eq!(check_window(&good), Ok(()), "{good:?}");
        }
        for bad in [
            window("24:00", "06:00", 2),
            window("9:00", "18:00", 2),
            window("09:60", "18:00", 2),
            window("09:00", "18:00", 0),
            window("09:00", "18:00", 169),
            window("09:00 ", "18:00", 2),
        ] {
            assert_eq!(
                check_window(&bad),
                Err(OperatorActionError::InvalidWindow),
                "{bad:?}"
            );
        }
        assert!(OperatorAction::SetWindow.expires());
        assert!(!OperatorAction::ClearWindow.expires());
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
