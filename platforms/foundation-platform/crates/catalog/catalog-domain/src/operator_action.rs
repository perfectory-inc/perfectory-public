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

/// The job whose run consumes a promotion request: the edition readiness check (root ADR-0381).
pub const PROMOTION_JOB: &str = "parcel_edition_readiness";

/// How long a promotion request may wait for that check, in hours: the age a readiness report may
/// have (`readiness.max_report_age_hours` of the cadastral source contract).
pub const PROMOTION_EXPIRY_HOURS: i64 = 48;

/// The subject of a cut-over approval: the by-PNU lane cut-over runner (root ADR-0433).
///
/// `by-pnu-pack-operator.sh` starts it as `foundation-by-pnu-lane-cutover-g<generation>`. It is not
/// a scheduled job; a `stop` of it cancels an approval or ends its run.
pub const CUTOVER_SUBJECT: &str = "by_pnu_lane_cutover";

/// How long a cut-over approval may wait for its start, in hours: the operator window opens once a
/// day, so its next opening is always within this (root ADR-0433 §2).
pub const CUTOVER_EXPIRY_HOURS: i64 = 48;

/// How long a full bake request may wait for its bake's next run, in hours (root ADR-0433 §4).
pub const FORCE_FULL_EXPIRY_HOURS: i64 = 7 * 24;

/// The highest pack generation the operator script takes (`^[1-9][0-9]{0,3}$`).
pub const MAX_GENERATION: i32 = 9999;

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
    /// Serve another cadastral edition: a request the readiness check grants only for a ready
    /// edition, and only through a reviewed change of the source contract (root ADR-0381).
    PromoteEdition,
    /// Cut over by-PNU lanes at a pack generation: the cut-over runner starts once, inside the
    /// operator window, and runs its gates as its steps (root ADR-0433).
    ApproveCutover,
    /// Re-base one by-PNU lane with a full bake on its bake's next run (root ADR-0433 §4).
    ForceFull,
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
            Self::PromoteEdition => "promote_edition",
            Self::ApproveCutover => "approve_cutover",
            Self::ForceFull => "force_full",
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
            "promote_edition" => Ok(Self::PromoteEdition),
            "approve_cutover" => Ok(Self::ApproveCutover),
            "force_full" => Ok(Self::ForceFull),
            _ => Err(OperatorActionError::UnknownAction),
        }
    }

    /// Whether the request expires: a `run_now` after [`RUN_NOW_EXPIRY_MINUTES`] (root ADR-0282 §4),
    /// a `set_window` after the hours it asked for, a `promote_edition` after
    /// [`PROMOTION_EXPIRY_HOURS`], an `approve_cutover` after [`CUTOVER_EXPIRY_HOURS`], a
    /// `force_full` after [`FORCE_FULL_EXPIRY_HOURS`].
    #[must_use]
    pub const fn expires(self) -> bool {
        matches!(
            self,
            Self::RunNow
                | Self::SetWindow
                | Self::PromoteEdition
                | Self::ApproveCutover
                | Self::ForceFull
        )
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
    /// An edition was given to an action other than `promote_edition`, or none (or not `YYYYMM`) to
    /// `promote_edition`.
    #[error("only promote_edition names an edition, YYYYMM, and it must")]
    EditionIsAPromotion,
    /// A `promote_edition` was asked of a job other than the readiness check.
    #[error("a promotion is asked of the parcel_edition_readiness job")]
    PromotionIsTheReadinessChecks,
    /// Lanes were given to an action other than `approve_cutover` or `force_full`, none to them, or
    /// they are not `lanes`, a lane or `lane,lane,...` (one lane for a `force_full`).
    #[error("only approve_cutover (a lane, lanes or lane,lane,...) and force_full (one lane) name lanes, and they must")]
    LanesAreARelease,
    /// A generation was given to an action other than `approve_cutover`, none to it, or it is not
    /// 1 to 9999.
    #[error("only approve_cutover names a pack generation, 1 to 9999, and it must")]
    GenerationIsACutover,
    /// An `approve_cutover` was asked of another subject than the cut-over runner, or the runner
    /// was asked anything but `approve_cutover` or `stop`.
    #[error(
        "a cut-over is approved of by_pnu_lane_cutover, which takes approve_cutover or stop only"
    )]
    CutoverIsTheRunners,
    /// A `force_full` was asked of a job that is not a by-PNU bake.
    #[error("a full bake is asked of a by-PNU bake job (by_pnu_<lane>_bake)")]
    FullBakeIsABakes,
}

/// Whether `lane` has the shape of a by-PNU contract lane: lowercase words joined by `-`.
fn is_lane(lane: &str) -> bool {
    !lane.is_empty()
        && !lane.starts_with('-')
        && !lane.ends_with('-')
        && lane
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Checks the lanes of a cut-over (`lanes`, one lane, or lane,lane,... each once) or, `single`, of
/// a full bake (one lane). The host refuses a lane its contract does not name.
///
/// # Errors
/// Returns [`OperatorActionError::LanesAreARelease`] when they are not.
pub fn check_lanes(lanes: &str, single: bool) -> Result<(), OperatorActionError> {
    let listed: Vec<&str> = lanes.split(',').collect();
    let mut unique = listed.clone();
    unique.sort_unstable();
    unique.dedup();
    let fits = listed.iter().all(|lane| is_lane(lane))
        && unique.len() == listed.len()
        && (listed.len() == 1 || !listed.contains(&"lanes"))
        && (!single || (listed.len() == 1 && lanes != "lanes"));
    if fits {
        Ok(())
    } else {
        Err(OperatorActionError::LanesAreARelease)
    }
}

/// Checks a pack generation: 1 to [`MAX_GENERATION`].
///
/// # Errors
/// Returns [`OperatorActionError::GenerationIsACutover`] when it is not.
pub const fn check_generation(generation: i32) -> Result<(), OperatorActionError> {
    if generation >= 1 && generation <= MAX_GENERATION {
        Ok(())
    } else {
        Err(OperatorActionError::GenerationIsACutover)
    }
}

/// Whether `job_id` names a by-PNU bake job (`by_pnu_<lane>_bake`), the only jobs a full bake is
/// asked of.
#[must_use]
pub fn is_by_pnu_bake(job_id: &str) -> bool {
    job_id
        .strip_prefix("by_pnu_")
        .and_then(|rest| rest.strip_suffix("_bake"))
        .is_some_and(|lane| !lane.is_empty())
}

/// Checks a promotion's edition: `YYYYMM`, a month 01 to 12.
///
/// # Errors
/// Returns [`OperatorActionError::EditionIsAPromotion`] when it is not.
pub fn check_edition(edition: &str) -> Result<(), OperatorActionError> {
    let bytes = edition.as_bytes();
    let month = edition.get(4..).and_then(|month| month.parse::<u8>().ok());
    if bytes.len() == 6 && bytes.iter().all(u8::is_ascii_digit) && matches!(month, Some(1..=12)) {
        Ok(())
    } else {
        Err(OperatorActionError::EditionIsAPromotion)
    }
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
            OperatorAction::PromoteEdition,
            OperatorAction::ApproveCutover,
            OperatorAction::ForceFull,
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
    fn a_promotion_names_one_edition_and_expires() {
        for good in ["209906", "209912", "202610"] {
            assert_eq!(check_edition(good), Ok(()), "{good}");
        }
        for bad in [
            "", "2099-06", "209913", "209900", "20990", "2099061", "2099ab",
        ] {
            assert_eq!(
                check_edition(bad),
                Err(OperatorActionError::EditionIsAPromotion),
                "{bad}"
            );
        }
        assert!(OperatorAction::PromoteEdition.expires());
    }

    #[test]
    fn a_cut_over_names_its_lanes_and_a_full_bake_one_lane() {
        for good in [
            "lanes",
            "parcel-ownership",
            "parcel-energy,parcel-ownership",
        ] {
            assert_eq!(check_lanes(good, false), Ok(()), "{good}");
        }
        for bad in [
            "",
            "Parcel",
            "parcel-energy,parcel-energy",
            "lanes,parcel-energy",
            "parcel energy",
            "-parcel",
            "parcel,",
        ] {
            assert_eq!(
                check_lanes(bad, false),
                Err(OperatorActionError::LanesAreARelease),
                "{bad}"
            );
        }
        assert_eq!(check_lanes("parcel", true), Ok(()));
        for bad in ["lanes", "parcel,building"] {
            assert_eq!(
                check_lanes(bad, true),
                Err(OperatorActionError::LanesAreARelease),
                "{bad}"
            );
        }
        assert_eq!(check_generation(1), Ok(()));
        assert_eq!(check_generation(MAX_GENERATION), Ok(()));
        for bad in [0, -1, 10_000] {
            assert_eq!(
                check_generation(bad),
                Err(OperatorActionError::GenerationIsACutover)
            );
        }
        assert!(is_by_pnu_bake("by_pnu_serving_bake"));
        assert!(is_by_pnu_bake("by_pnu_parcel_ownership_bake"));
        for wrong in [
            "by_pnu__bake",
            "by_pnu_bake",
            "parcel_tile_bake",
            CUTOVER_SUBJECT,
        ] {
            assert!(!is_by_pnu_bake(wrong), "{wrong}");
        }
        assert!(OperatorAction::ApproveCutover.expires());
        assert!(OperatorAction::ForceFull.expires());
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
