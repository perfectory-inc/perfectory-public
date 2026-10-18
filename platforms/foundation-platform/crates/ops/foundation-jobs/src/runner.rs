//! Runs one job's command and says how it ended (root ADR-0270 §2, ADR-0260).
//!
//! The runner, not the command, prints the run's last line, `foundation-job-outcome <outcome>`,
//! and exits with the code that line means: a command cannot end on a line its exit status
//! contradicts (#656). It also ends a run that outlives its time limit, or that prints nothing
//! for its stall limit (2026-10-17: a one-second state read waited six hours on a request that
//! never answered), by stopping the command's whole process group.

use std::time::Duration;

/// The prefix of an outcome line (root ADR-0260 §1).
pub const OUTCOME_PREFIX: &str = "foundation-job-outcome";

/// How a run ended, in the outcome vocabulary of root ADR-0260.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It changed its outputs.
    Changed,
    /// It ran and changed nothing.
    Unchanged,
    /// It finished its part and waits for a person or another job, for this reason.
    Waiting(String),
    /// It failed, for this reason.
    Failed(String),
}

impl Outcome {
    /// The outcome line the runner prints last.
    #[must_use]
    pub fn line(&self) -> String {
        match self {
            Self::Changed => format!("{OUTCOME_PREFIX} changed"),
            Self::Unchanged => format!("{OUTCOME_PREFIX} unchanged"),
            Self::Waiting(reason) => format!("{OUTCOME_PREFIX} waiting {}", one_line(reason)),
            Self::Failed(reason) => format!("{OUTCOME_PREFIX} failed {}", one_line(reason)),
        }
    }

    /// The exit code the line means: 0 for `changed`, `unchanged` and `waiting`, 1 for `failed`.
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::Changed | Self::Unchanged | Self::Waiting(_) => 0,
            Self::Failed(_) => 1,
        }
    }
}

fn one_line(reason: &str) -> String {
    let joined = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    if joined.is_empty() {
        "no reason given".to_owned()
    } else {
        joined
    }
}

/// The outcome a line states, if it is an outcome line.
#[must_use]
pub fn parse_outcome_line(line: &str) -> Option<Outcome> {
    let rest = line
        .trim_end()
        .strip_prefix(OUTCOME_PREFIX)?
        .strip_prefix(' ')?;
    let (word, reason) = rest.split_once(' ').unwrap_or((rest, ""));
    match word {
        "changed" if reason.is_empty() => Some(Outcome::Changed),
        "unchanged" if reason.is_empty() => Some(Outcome::Unchanged),
        "waiting" => Some(Outcome::Waiting(one_line(reason))),
        "failed" => Some(Outcome::Failed(one_line(reason))),
        _ => None,
    }
}

/// How the command itself ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// It exited 0.
    Succeeded,
    /// It exited with this non-zero code.
    Exited(i32),
    /// A signal ended it.
    Signalled,
    /// The runner stopped it after its time limit.
    OutlivedTimeLimit(Duration),
    /// The runner stopped it after it printed nothing for its stall limit.
    Stalled(Duration),
}

/// The run's outcome from how the command ended and the last outcome line it printed.
///
/// Root ADR-0260 §2: a success with `changed`, `unchanged` or `waiting` keeps it; a success that
/// said nothing, or said `failed`, failed; any other ending failed, with the command's own
/// `failed` reason when it gave one.
#[must_use]
pub fn verdict(ending: &Ending, said: Option<Outcome>) -> Outcome {
    match (ending, said) {
        (Ending::Succeeded, Some(outcome)) => outcome,
        (Ending::Succeeded, None) => Outcome::Failed("no outcome reported".to_owned()),
        (Ending::OutlivedTimeLimit(limit), _) => Outcome::Failed(format!(
            "time limit {} minutes reached",
            limit.as_secs() / 60
        )),
        (Ending::Stalled(limit), _) => Outcome::Failed(format!(
            "no output for {} minutes (stall limit)",
            limit.as_secs() / 60
        )),
        (_, Some(Outcome::Failed(reason))) => Outcome::Failed(reason),
        (Ending::Exited(code), _) => Outcome::Failed(format!("exit status {code}")),
        (Ending::Signalled, _) => Outcome::Failed("ended by a signal".to_owned()),
    }
}

/// How long a run may take, and how long it may stay silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The whole run (`time_limit_minutes`).
    pub time_limit: Duration,
    /// The longest stretch without one line of output (`stall_limit_minutes`).
    pub stall_limit: Duration,
    /// How long the command's process group gets between TERM and KILL.
    pub grace: Duration,
}

#[cfg(unix)]
pub use process::run;

#[cfg(unix)]
mod process {
    use std::io::{BufRead as _, BufReader, Read, Write};
    use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use rustix::process::{kill_process_group, Pid, Signal};

    use super::{parse_outcome_line, verdict, Ending, Limits, Outcome};

    const POLL: Duration = Duration::from_millis(200);

    enum Event {
        Line(String),
        Closed,
    }

    /// Runs `command` in `run_dir` and returns the run's outcome.
    ///
    /// The command leads its own process group, and every line it prints is copied to `out`. The
    /// runner's own outcome line is not printed here: the caller prints [`Outcome::line`] last.
    ///
    /// # Errors
    /// When the command cannot be started or waited for.
    pub fn run(
        command: &[String],
        run_dir: &Path,
        limits: Limits,
        out: &mut dyn Write,
    ) -> std::io::Result<Outcome> {
        let (program, args) = command
            .split_first()
            .ok_or_else(|| std::io::Error::other("the job's command is empty"))?;
        let mut child = Command::new(program)
            .args(args)
            .current_dir(run_dir)
            .env("FOUNDATION_JOB_RUN_DIR", run_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()?;
        let (sender, lines) = mpsc::channel();
        let readers = [
            child
                .stdout
                .take()
                .map(|stdout| read_lines(stdout, sender.clone())),
            child
                .stderr
                .take()
                .map(|stderr| read_lines(stderr, sender.clone())),
        ];
        drop(sender);

        let started = Instant::now();
        let mut last_output = started;
        let mut said = None;
        let mut open = readers.iter().flatten().count();
        let ending = loop {
            while let Ok(event) = lines.recv_timeout(Duration::ZERO) {
                last_output = Instant::now();
                match event {
                    Event::Line(line) => {
                        if let Some(outcome) = parse_outcome_line(&line) {
                            said = Some(outcome);
                        }
                        writeln!(out, "{line}")?;
                    }
                    Event::Closed => open = open.saturating_sub(1),
                }
            }
            if let Some(status) = child.try_wait()? {
                break match status.code() {
                    Some(0) => Ending::Succeeded,
                    Some(code) => Ending::Exited(code),
                    None if status.signal().is_some() => Ending::Signalled,
                    None => Ending::Exited(-1),
                };
            }
            if started.elapsed() >= limits.time_limit {
                stop(&mut child, limits.grace)?;
                break Ending::OutlivedTimeLimit(limits.time_limit);
            }
            if last_output.elapsed() >= limits.stall_limit {
                stop(&mut child, limits.grace)?;
                break Ending::Stalled(limits.stall_limit);
            }
            thread::sleep(POLL);
        };
        // What the command printed before it ended still counts; its pipes close with it (a
        // child it left behind holding them is stopped with the group, or bounded here).
        let drained = Instant::now();
        while open > 0 && drained.elapsed() < limits.grace {
            match lines.recv_timeout(POLL) {
                Ok(Event::Line(line)) => {
                    if let Some(outcome) = parse_outcome_line(&line) {
                        said = Some(outcome);
                    }
                    writeln!(out, "{line}")?;
                }
                Ok(Event::Closed) => open -= 1,
                Err(_) => {}
            }
        }
        Ok(verdict(&ending, said))
    }

    fn read_lines(
        stream: impl Read + Send + 'static,
        sender: mpsc::Sender<Event>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                let Ok(line) = line else { break };
                if sender.send(Event::Line(line)).is_err() {
                    return;
                }
            }
            let _ = sender.send(Event::Closed);
        })
    }

    /// TERM to the command's whole process group, KILL after `grace`, then reap it.
    fn stop(child: &mut Child, grace: Duration) -> std::io::Result<()> {
        let group = i32::try_from(child.id())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| std::io::Error::other("the command has no process id"))?;
        let _ = kill_process_group(group, Signal::TERM);
        let asked = Instant::now();
        while asked.elapsed() < grace {
            if child.try_wait()?.is_some() {
                let _ = kill_process_group(group, Signal::KILL);
                return Ok(());
            }
            thread::sleep(POLL);
        }
        let _ = kill_process_group(group, Signal::KILL);
        child.wait().map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outcome_line_is_read_and_written_back_the_same() {
        for line in [
            "foundation-job-outcome changed",
            "foundation-job-outcome unchanged",
            "foundation-job-outcome waiting edition 202607 has no contract entry",
            "foundation-job-outcome failed shard 26",
        ] {
            let outcome = parse_outcome_line(line);
            assert_eq!(outcome.as_ref().map(Outcome::line).as_deref(), Some(line));
        }
        assert_eq!(
            parse_outcome_line("foundation-job-outcome changed now"),
            None
        );
        assert_eq!(parse_outcome_line("foundation-job-outcomes changed"), None);
        assert_eq!(
            parse_outcome_line("said foundation-job-outcome changed"),
            None
        );
    }

    #[test]
    fn a_run_ends_on_the_line_its_exit_status_means() {
        // #656: the bake printed `changed` and exited 1.
        let changed_then_failed = verdict(&Ending::Exited(1), Some(Outcome::Changed));
        assert_eq!(
            changed_then_failed,
            Outcome::Failed("exit status 1".to_owned())
        );
        assert_eq!(changed_then_failed.exit_code(), 1);
        let silent = verdict(&Ending::Succeeded, None);
        assert_eq!(silent, Outcome::Failed("no outcome reported".to_owned()));
        let said_failed = verdict(&Ending::Succeeded, Some(Outcome::Failed("x".to_owned())));
        assert_eq!(said_failed.exit_code(), 1);
        let reason = verdict(
            &Ending::Exited(2),
            Some(Outcome::Failed("shard 26".to_owned())),
        );
        assert_eq!(reason, Outcome::Failed("shard 26".to_owned()));
        let waiting = verdict(
            &Ending::Succeeded,
            Some(Outcome::Waiting("steward".to_owned())),
        );
        assert_eq!(waiting.exit_code(), 0);
        let stalled = verdict(
            &Ending::Stalled(Duration::from_mins(10)),
            Some(Outcome::Changed),
        );
        assert_eq!(stalled.exit_code(), 1);
    }

    #[test]
    fn a_reason_stays_one_line() {
        assert_eq!(
            Outcome::Failed("a\nb\tc".to_owned()).line(),
            "foundation-job-outcome failed a b c"
        );
        assert_eq!(
            Outcome::Waiting(String::new()).line(),
            "foundation-job-outcome waiting no reason given"
        );
    }

    #[cfg(unix)]
    mod processes {
        use super::super::*;
        use std::time::{Duration, Instant};

        fn limits(time_limit: u64, stall_limit: u64) -> Limits {
            Limits {
                time_limit: Duration::from_secs(time_limit),
                stall_limit: Duration::from_secs(stall_limit),
                grace: Duration::from_secs(2),
            }
        }

        fn shell(script: &str, limits: Limits) -> Result<(Outcome, String), String> {
            let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
            let mut out = Vec::new();
            let command = ["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()];
            let outcome =
                run(&command, dir.path(), limits, &mut out).map_err(|error| error.to_string())?;
            Ok((outcome, String::from_utf8_lossy(&out).into_owned()))
        }

        #[test]
        fn the_commands_last_outcome_line_is_the_runs() -> Result<(), String> {
            let (outcome, out) = shell(
                "echo foundation-job-outcome unchanged; echo working; echo foundation-job-outcome changed",
                limits(30, 30),
            )?;
            assert_eq!(outcome, Outcome::Changed);
            assert!(out.contains("working"), "{out}");
            Ok(())
        }

        #[test]
        fn a_command_that_says_nothing_or_contradicts_its_exit_fails() -> Result<(), String> {
            assert_eq!(
                shell("true", limits(30, 30))?.0,
                Outcome::Failed("no outcome reported".to_owned())
            );
            assert_eq!(
                shell(
                    "echo foundation-job-outcome changed; exit 3",
                    limits(30, 30)
                )?
                .0,
                Outcome::Failed("exit status 3".to_owned())
            );
            Ok(())
        }

        #[test]
        fn the_time_limit_stops_the_whole_process_group() -> Result<(), String> {
            let started = Instant::now();
            // A child that leaves a grandchild behind holding the pipes, as a compose client does.
            let (outcome, _) = shell(
                "sleep 60 & while true; do echo tick; sleep 0.2; done",
                limits(1, 30),
            )?;
            assert!(matches!(outcome, Outcome::Failed(reason) if reason.starts_with("time limit")));
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the group outlived its limit"
            );
            Ok(())
        }

        #[test]
        fn a_command_silent_for_its_stall_limit_is_stopped() -> Result<(), String> {
            // 2026-10-17: a one-second read waited six hours on an answer that never came.
            let started = Instant::now();
            let (outcome, out) = shell("echo started; sleep 60", limits(600, 1))?;
            assert!(matches!(outcome, Outcome::Failed(reason) if reason.contains("stall limit")));
            assert!(out.contains("started"));
            assert!(started.elapsed() < Duration::from_secs(20));
            Ok(())
        }

        #[test]
        fn the_command_runs_in_its_run_directory() -> Result<(), String> {
            let (outcome, out) = shell(
                "test \"$PWD\" = \"$FOUNDATION_JOB_RUN_DIR\" && echo foundation-job-outcome unchanged",
                limits(30, 30),
            )?;
            assert_eq!(outcome, Outcome::Unchanged, "{out}");
            Ok(())
        }
    }
}
