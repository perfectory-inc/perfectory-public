//! The host's switch board: files under one directory the runner and the gate read (root ADR-0282 §3).
//!
//! `<job>`: the job is paused; `key=value` lines say by whom, when, why and whether pending runs
//! are dropped. A resume removes it. `<job>.window`: the job's run window (KST `start`, `end`) until
//! `until`, instead of its contract's; `clear_window` removes it. `<job>.stop`: end the run in progress (the runner consumes it).
//! `run-now.d/<request id>`: a single-use request to run once, until it expires (the gate consumes
//! it). `operator-actions.log`: one line per request, appended. A job id is `[a-z][a-z0-9_]*`, so it
//! never collides with the dotted names.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use catalog_application::ports::{JobSwitchBoard, JobSwitchBoardError, OperatorActionRecord};
use catalog_domain::operator_action::OperatorAction;

/// The operator log's file name inside the switch directory.
pub const OPERATOR_LOG: &str = "operator-actions.log";
/// The directory of pending `run_now` requests inside the switch directory.
pub const RUN_NOW_DIR: &str = "run-now.d";

/// Writes switches under `dir` (the host's `/var/lib/foundation-jobs/switches`).
pub struct FileJobSwitchBoard {
    dir: PathBuf,
}

impl FileJobSwitchBoard {
    /// A switch board over `dir`.
    #[must_use]
    pub const fn new(dir: PathBuf) -> Self {
        Self { dir }
    }
}

fn unavailable(what: &Path, error: &std::io::Error) -> JobSwitchBoardError {
    JobSwitchBoardError::Unavailable(format!("{}: {error}", what.display()))
}

/// Writes `text` to `path` whole or not at all (a temporary file, then a rename).
fn write_whole(path: &Path, text: &str) -> Result<(), JobSwitchBoardError> {
    let partial = path.with_extension("partial");
    fs::write(&partial, text).map_err(|error| unavailable(&partial, &error))?;
    fs::rename(&partial, path).map_err(|error| unavailable(path, &error))
}

fn fields(record: &OperatorActionRecord) -> String {
    let mut lines = vec![
        format!("request={}", record.id),
        format!("actor={}", record.actor),
        format!("requested_at={}", record.requested_at.to_rfc3339()),
        format!("reason={}", record.reason),
    ];
    if record.action == OperatorAction::Pause {
        lines.push(format!("drop_pending={}", record.drop_pending));
    }
    if let (OperatorAction::RunNow, Some(expires_at)) = (record.action, record.expires_at) {
        lines.push(format!("job={}", record.job_id));
        lines.push(format!("expires_at={}", expires_at.to_rfc3339()));
    }
    lines.join("\n") + "\n"
}

impl JobSwitchBoard for FileJobSwitchBoard {
    fn ready(&self) -> Result<(), JobSwitchBoardError> {
        let probe = self.dir.join(".ready");
        fs::write(&probe, b"").map_err(|error| unavailable(&probe, &error))?;
        fs::remove_file(&probe).map_err(|error| unavailable(&probe, &error))
    }

    fn apply(&self, record: &OperatorActionRecord) -> Result<(), JobSwitchBoardError> {
        let job = self.dir.join(&record.job_id);
        match record.action {
            OperatorAction::Pause => write_whole(&job, &format!("paused\n{}", fields(record)))?,
            OperatorAction::Resume => match fs::remove_file(&job) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(unavailable(&job, &error)),
            },
            OperatorAction::Stop => {
                write_whole(
                    &self.dir.join(format!("{}.stop", record.job_id)),
                    &fields(record),
                )?;
            }
            OperatorAction::SetWindow => {
                let until = record
                    .expires_at
                    .map(|at| at.to_rfc3339())
                    .unwrap_or_default();
                let window = format!(
                    "start={}\nend={}\nuntil={until}\n{}",
                    record.window_start.as_deref().unwrap_or_default(),
                    record.window_end.as_deref().unwrap_or_default(),
                    fields(record)
                );
                write_whole(&self.dir.join(format!("{}.window", record.job_id)), &window)?;
            }
            OperatorAction::ClearWindow => {
                let window = self.dir.join(format!("{}.window", record.job_id));
                match fs::remove_file(&window) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(unavailable(&window, &error)),
                }
            }
            OperatorAction::RunNow => {
                let requests = self.dir.join(RUN_NOW_DIR);
                fs::create_dir_all(&requests).map_err(|error| unavailable(&requests, &error))?;
                write_whole(&requests.join(record.id.to_string()), &fields(record))?;
            }
        }
        let log = self.dir.join(OPERATOR_LOG);
        let line = format!(
            "{} {} {} {} drop_pending={} request={} reason={}\n",
            record.requested_at.to_rfc3339(),
            record.actor,
            record.action.as_str(),
            record.job_id,
            record.drop_pending,
            record.id,
            record.reason
        );
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .and_then(|mut file| file.write_all(line.as_bytes()))
            .map_err(|error| unavailable(&log, &error))
    }
}

/// A switch board this deployment did not configure: every request is refused before the ledger.
pub struct UnconfiguredJobSwitchBoard;

impl JobSwitchBoard for UnconfiguredJobSwitchBoard {
    fn ready(&self) -> Result<(), JobSwitchBoardError> {
        Err(JobSwitchBoardError::Unavailable(
            "FOUNDATION_PLATFORM_JOB_SWITCHES_DIR is not set".to_owned(),
        ))
    }

    fn apply(&self, _record: &OperatorActionRecord) -> Result<(), JobSwitchBoardError> {
        self.ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use foundation_shared_kernel::ids::StaffId;
    use uuid::Uuid;

    fn record(action: OperatorAction) -> OperatorActionRecord {
        let at = Utc
            .with_ymd_and_hms(2026, 10, 19, 9, 0, 0)
            .single()
            .unwrap_or_default();
        OperatorActionRecord {
            id: Uuid::nil(),
            job_id: "shape_ledger_check".to_owned(),
            action,
            drop_pending: action == OperatorAction::Pause,
            actor: StaffId::new(Uuid::nil()),
            reason: "a supervised run".to_owned(),
            requested_at: at,
            expires_at: action.expires().then(|| at + chrono::Duration::minutes(10)),
            window_start: (action == OperatorAction::SetWindow).then(|| "09:00".to_owned()),
            window_end: (action == OperatorAction::SetWindow).then(|| "18:00".to_owned()),
        }
    }

    fn board() -> Result<(tempfile::TempDir, FileJobSwitchBoard), std::io::Error> {
        let dir = tempfile::tempdir()?;
        let board = FileJobSwitchBoard::new(dir.path().to_path_buf());
        Ok((dir, board))
    }

    #[test]
    fn a_pause_writes_the_job_file_and_a_resume_removes_it(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (dir, board) = board()?;
        board.ready()?;
        board.apply(&record(OperatorAction::Pause))?;
        let paused = fs::read_to_string(dir.path().join("shape_ledger_check"))?;
        assert!(paused.starts_with("paused\n"), "{paused}");
        assert!(paused.contains("drop_pending=true\n"));
        assert!(paused.contains("reason=a supervised run\n"));
        board.apply(&record(OperatorAction::Resume))?;
        assert!(!dir.path().join("shape_ledger_check").exists());
        // A resume of a job that is not paused changes nothing and is not an error.
        board.apply(&record(OperatorAction::Resume))?;
        let log = fs::read_to_string(dir.path().join(OPERATOR_LOG))?;
        assert_eq!(
            log.lines().count(),
            3,
            "every request is one appended line: {log}"
        );
        assert!(log
            .lines()
            .next()
            .is_some_and(|line| line.contains(" pause shape_ledger_check ")));
        Ok(())
    }

    #[test]
    fn a_stop_and_a_run_now_leave_what_the_runner_and_the_gate_consume(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (dir, board) = board()?;
        board.apply(&record(OperatorAction::Stop))?;
        assert!(dir.path().join("shape_ledger_check.stop").is_file());
        board.apply(&record(OperatorAction::RunNow))?;
        let request =
            fs::read_to_string(dir.path().join(RUN_NOW_DIR).join(Uuid::nil().to_string()))?;
        assert!(request.contains("job=shape_ledger_check\n"), "{request}");
        assert!(
            request.contains("expires_at=2026-10-19T09:10:00+00:00\n"),
            "{request}"
        );
        assert!(
            !dir.path().join("shape_ledger_check").exists(),
            "a run-now is not a pause"
        );
        Ok(())
    }

    #[test]
    fn a_window_override_is_set_with_its_end_and_cleared() -> Result<(), Box<dyn std::error::Error>>
    {
        let (dir, board) = board()?;
        board.apply(&record(OperatorAction::SetWindow))?;
        let window = fs::read_to_string(dir.path().join("shape_ledger_check.window"))?;
        assert!(
            window.starts_with("start=09:00\nend=18:00\nuntil=2026-10-19T09:10:00+00:00\n"),
            "{window}"
        );
        assert!(
            !window.contains("expires_at="),
            "a window is not a run-now request"
        );
        board.apply(&record(OperatorAction::ClearWindow))?;
        assert!(!dir.path().join("shape_ledger_check.window").exists());
        board.apply(&record(OperatorAction::ClearWindow))?;
        Ok(())
    }

    #[test]
    fn a_board_that_cannot_be_written_is_not_ready() {
        let board = FileJobSwitchBoard::new(PathBuf::from("/nonexistent/foundation-jobs/switches"));
        assert!(board.ready().is_err());
        assert!(UnconfiguredJobSwitchBoard.ready().is_err());
    }
}
