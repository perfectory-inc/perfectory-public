//! `foundation-outbox-publisher job-run`: runs one listed job through the runner (root ADR-0270 §2).
//!
//! The job's `command` (relative to its release), `time_limit_minutes` and `stall_limit_minutes`
//! come from the job list of the release the entry script pinned. Each run gets its own directory,
//! removed when the run succeeds and kept when it fails, and leaves one record file. The run's
//! outcome line is printed last, and the exit code is the one that line means.

#[cfg(unix)]
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::Serialize;

use crate::runner::Limits;
#[cfg(unix)]
use crate::runner::Outcome;
use crate::JobList;

/// How long the command's process group gets between TERM and KILL.
pub const STOP_GRACE: Duration = Duration::from_secs(30);

/// Where and what one run runs, from the environment the entry script and systemd set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRun {
    /// The job's id in the list.
    pub job: String,
    /// The pinned release (`releases/<sha>`), whose job list and command are used.
    pub release_root: PathBuf,
    /// Where run directories and records go.
    pub run_root: PathBuf,
    /// This run's id (systemd's `INVOCATION_ID`).
    pub invocation: String,
}

impl JobRun {
    /// Reads `FOUNDATION_JOB_RUN_JOB`, `FOUNDATION_JOB_RUN_RELEASE_ROOT`, `FOUNDATION_JOB_RUN_ROOT`
    /// and `INVOCATION_ID` through `var`.
    ///
    /// # Errors
    /// When one is missing, the paths are not absolute, or an id holds anything but `[a-z0-9_-]`.
    pub fn from_env(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let get = |name: &str| {
            var(name)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| format!("{name} is not set"))
        };
        let run = Self {
            job: get("FOUNDATION_JOB_RUN_JOB")?,
            release_root: PathBuf::from(get("FOUNDATION_JOB_RUN_RELEASE_ROOT")?),
            run_root: PathBuf::from(get("FOUNDATION_JOB_RUN_ROOT")?),
            invocation: get("INVOCATION_ID")?,
        };
        for (name, id) in [("job", &run.job), ("invocation", &run.invocation)] {
            if !id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
            }) {
                return Err(format!(
                    "the {name} id {id:?} holds a character other than [a-z0-9_-]"
                ));
            }
        }
        if !run.release_root.has_root() || !run.run_root.has_root() {
            return Err("the release root and the run root must be absolute".to_owned());
        }
        Ok(run)
    }

    /// The run's own directory.
    #[must_use]
    pub fn run_dir(&self) -> PathBuf {
        self.run_root.join(&self.job).join(&self.invocation)
    }

    /// The run's record file.
    #[must_use]
    pub fn record_path(&self) -> PathBuf {
        self.run_root
            .join(&self.job)
            .join("records")
            .join(format!("{}.json", self.invocation))
    }
}

/// What the runner needs from the job's entry in the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The command, its program resolved inside the release.
    pub command: Vec<String>,
    /// The time and stall limits.
    pub limits: Limits,
}

/// The job's plan from the list.
///
/// # Errors
/// When the job is not listed, has no `command` or `stall_limit_minutes`, or its program is not a
/// plain relative path inside the release (no `..`, not absolute).
pub fn plan(list: &JobList, job: &str, release_root: &Path) -> Result<Plan, String> {
    let entry = list
        .jobs
        .iter()
        .find(|entry| entry.id == job)
        .ok_or_else(|| format!("{job} is not in the job list"))?;
    let command = entry
        .command
        .as_ref()
        .filter(|command| !command.is_empty())
        .ok_or_else(|| format!("{job} has no command in the job list"))?;
    let stall = entry
        .stall_limit_minutes
        .ok_or_else(|| format!("{job} has no stall_limit_minutes in the job list"))?;
    let (program, args) = command
        .split_first()
        .ok_or_else(|| format!("{job} has an empty command"))?;
    let relative = Path::new(program);
    if !relative
        .components()
        .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(format!(
            "{job}: the command {program} is not a path inside the release"
        ));
    }
    let mut resolved = vec![release_root.join(relative).to_string_lossy().into_owned()];
    resolved.extend(args.iter().cloned());
    Ok(Plan {
        command: resolved,
        limits: Limits {
            time_limit: Duration::from_secs(u64::from(entry.time_limit_minutes) * 60),
            stall_limit: Duration::from_secs(u64::from(stall) * 60),
            grace: STOP_GRACE,
        },
    })
}

/// One run's record (`<run root>/<job>/records/<invocation>.json`).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Record {
    /// The job's id.
    pub job: String,
    /// This run's id.
    pub invocation: String,
    /// The release it ran from.
    pub release: String,
    /// When it started and ended, seconds since the epoch.
    pub started_at: u64,
    /// When it ended.
    pub ended_at: u64,
    /// Its outcome line.
    pub outcome: String,
    /// Its run directory, when it was kept (a failed run).
    pub kept_run_dir: Option<String>,
}

#[cfg(unix)]
fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Runs the job and prints its outcome line last to `out`; returns the run's outcome.
///
/// Anything that keeps the run from starting is a `failed` outcome with its reason, never silence.
#[cfg(unix)]
pub fn execute(run: &JobRun, out: &mut dyn Write) -> Outcome {
    let started_at = now();
    let outcome = prepare_and_run(run, out).unwrap_or_else(Outcome::Failed);
    let kept = outcome.exit_code() != 0;
    if !kept {
        let _ = std::fs::remove_dir_all(run.run_dir());
    }
    let record = Record {
        job: run.job.clone(),
        invocation: run.invocation.clone(),
        release: run.release_root.to_string_lossy().into_owned(),
        started_at,
        ended_at: now(),
        outcome: outcome.line(),
        kept_run_dir: kept.then(|| run.run_dir().to_string_lossy().into_owned()),
    };
    // A success whose record is lost is not one the map can show: it fails, and says why.
    let outcome = match write_record(run, &record) {
        Err(error) if outcome.exit_code() == 0 => {
            Outcome::Failed(format!("the run record could not be written: {error}"))
        }
        _ => outcome,
    };
    let _ = writeln!(out, "{}", outcome.line());
    outcome
}

/// The runner stops a command's process group, which only Unix has: elsewhere a run ends on a
/// `failed` line at once (the publisher is built on Windows too).
#[cfg(not(unix))]
pub fn execute(_run: &JobRun, out: &mut dyn std::io::Write) -> crate::runner::Outcome {
    let outcome =
        crate::runner::Outcome::Failed("the job runner runs only on Unix hosts".to_owned());
    let _ = writeln!(out, "{}", outcome.line());
    outcome
}

#[cfg(unix)]
fn prepare_and_run(run: &JobRun, out: &mut dyn Write) -> Result<Outcome, String> {
    let list = JobList::read(&run.release_root)?;
    let plan = plan(&list, &run.job, &run.release_root)?;
    std::fs::create_dir_all(run.run_dir())
        .map_err(|error| format!("the run directory: {error}"))?;
    crate::runner::run(&plan.command, &run.run_dir(), plan.limits, out)
        .map_err(|error| format!("the command did not start: {error}"))
}

#[cfg(unix)]
fn write_record(run: &JobRun, record: &Record) -> std::io::Result<()> {
    let path = run.record_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(record).map_err(std::io::Error::other)?;
    std::fs::write(path, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Job;
    use std::collections::BTreeMap;

    fn list(command: Option<Vec<&str>>, stall: Option<u32>) -> JobList {
        JobList {
            jobs: vec![Job {
                id: "shape_ledger_check".to_owned(),
                systemd_service: "foundation-shape-ledger-check.service".to_owned(),
                time_limit_minutes: 15,
                stop_grace_minutes: 5,
                state_paths: Vec::new(),
                command: command.map(|parts| parts.into_iter().map(str::to_owned).collect()),
                stall_limit_minutes: stall,
            }],
            state_directories: Vec::new(),
            state_paths_not_created_here: BTreeMap::new(),
        }
    }

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    #[test]
    fn a_run_is_read_from_its_environment() -> Result<(), String> {
        let run = JobRun::from_env(env(&[
            ("FOUNDATION_JOB_RUN_JOB", "shape_ledger_check"),
            (
                "FOUNDATION_JOB_RUN_RELEASE_ROOT",
                "/opt/foundation-platform/releases/abc",
            ),
            (
                "FOUNDATION_JOB_RUN_ROOT",
                "/data/foundation-platform/job-runs",
            ),
            ("INVOCATION_ID", "0123abcd"),
        ]))?;
        assert_eq!(
            run.run_dir(),
            PathBuf::from("/data/foundation-platform/job-runs/shape_ledger_check/0123abcd")
        );
        assert!(JobRun::from_env(env(&[("FOUNDATION_JOB_RUN_JOB", "x")])).is_err());
        assert!(JobRun::from_env(env(&[
            ("FOUNDATION_JOB_RUN_JOB", "../etc"),
            ("FOUNDATION_JOB_RUN_RELEASE_ROOT", "/r"),
            ("FOUNDATION_JOB_RUN_ROOT", "/w"),
            ("INVOCATION_ID", "1"),
        ]))
        .is_err());
        Ok(())
    }

    #[test]
    fn the_plan_resolves_the_command_inside_the_release() -> Result<(), String> {
        let release = Path::new("/opt/foundation-platform/releases/abc");
        let planned = plan(
            &list(
                Some(vec!["scripts/ops/shape-ledger-check.py", "--x"]),
                Some(5),
            ),
            "shape_ledger_check",
            release,
        )?;
        assert_eq!(
            planned.command.first().map(String::as_str),
            Some(
                release
                    .join("scripts/ops/shape-ledger-check.py")
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert_eq!(planned.limits.time_limit, Duration::from_mins(15));
        assert_eq!(planned.limits.stall_limit, Duration::from_mins(5));
        for escape in ["../bin/sh", "/bin/sh"] {
            assert!(
                plan(
                    &list(Some(vec![escape]), Some(5)),
                    "shape_ledger_check",
                    release
                )
                .is_err(),
                "{escape}"
            );
        }
        assert!(plan(&list(None, Some(5)), "shape_ledger_check", release).is_err());
        assert!(plan(&list(Some(vec!["a"]), None), "shape_ledger_check", release).is_err());
        assert!(plan(&list(Some(vec!["a"]), Some(5)), "other", release).is_err());
        Ok(())
    }

    #[cfg(unix)]
    mod runs {
        use super::super::*;

        fn release(script: &str, stall: u32) -> Result<(tempfile::TempDir, JobRun), String> {
            let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
            let root = dir.path().join("release");
            std::fs::create_dir_all(root.join("orchestration"))
                .map_err(|error| error.to_string())?;
            std::fs::write(root.join("job.sh"), script).map_err(|error| error.to_string())?;
            let list = serde_json::json!({"jobs": [{"id": "probe", "systemd_service": "foundation-probe.service",
                "time_limit_minutes": 1, "stop_grace_minutes": 1, "state_paths": [],
                "command": ["job.sh"], "stall_limit_minutes": stall}],
                "state_directories": [], "state_paths_not_created_here": {}});
            std::fs::write(root.join("orchestration/jobs.v1.json"), list.to_string())
                .map_err(|error| error.to_string())?;
            let run = JobRun {
                job: "probe".to_owned(),
                release_root: root,
                run_root: dir.path().join("runs"),
                invocation: "inv1".to_owned(),
            };
            Ok((dir, run))
        }

        fn sh(body: &str) -> String {
            format!("#!/bin/sh\n{body}\n")
        }

        fn executable(run: &JobRun) -> Result<(), String> {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(
                run.release_root.join("job.sh"),
                std::fs::Permissions::from_mode(0o755),
            )
            .map_err(|error| error.to_string())
        }

        #[test]
        fn a_successful_run_removes_its_directory_and_records_its_line_last() -> Result<(), String>
        {
            let (_dir, run) = release(
                &sh("echo work > data; echo foundation-job-outcome changed"),
                1,
            )?;
            executable(&run)?;
            let mut out = Vec::new();
            assert_eq!(execute(&run, &mut out), Outcome::Changed);
            let printed = String::from_utf8_lossy(&out);
            assert_eq!(
                printed.lines().last(),
                Some("foundation-job-outcome changed")
            );
            assert!(
                !run.run_dir().exists(),
                "a successful run's directory is removed"
            );
            let record =
                std::fs::read_to_string(run.record_path()).map_err(|error| error.to_string())?;
            assert!(
                record.contains("\"outcome\": \"foundation-job-outcome changed\""),
                "{record}"
            );
            Ok(())
        }

        #[test]
        fn a_failed_run_keeps_its_directory() -> Result<(), String> {
            let (_dir, run) = release(&sh("echo partial > data; exit 4"), 1)?;
            executable(&run)?;
            let mut out = Vec::new();
            assert_eq!(
                execute(&run, &mut out),
                Outcome::Failed("exit status 4".to_owned())
            );
            assert!(
                run.run_dir().join("data").exists(),
                "a failed run's directory is kept"
            );
            Ok(())
        }

        #[test]
        fn a_run_that_cannot_start_fails_with_its_reason() -> Result<(), String> {
            let (_dir, run) = release(&sh("true"), 1)?;
            let mut out = Vec::new();
            let outcome = execute(&run, &mut out);
            assert!(
                matches!(&outcome, Outcome::Failed(reason) if reason.starts_with("the command did not start")),
                "{outcome:?}"
            );
            assert_eq!(
                String::from_utf8_lossy(&out).lines().last(),
                Some(outcome.line().as_str())
            );
            Ok(())
        }
    }
}
