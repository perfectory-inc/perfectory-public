//! A job's open-files limit, rendered into its service's drop-in (root ADR-0574 §8).
//!
//! systemd starts a unit with a soft limit of 1024 open files (the hard one is 524288): the legacy
//! `select()` bound, which a process is expected to raise for itself. A job whose run opens more
//! states `open_files` beside its other limits, with `open_files_measured` (where the number comes
//! from), and its service gets `<service>.d/50-open-files.conf` holding `LimitNOFILE=`. A job that
//! states none keeps systemd's default: the drop-in exists only where a run was measured to need it.
//!
//! What real incident does failing this prevent? 2026-11-23 13:44Z..22:51Z:
//! `foundation-gold-versions@parcel-real-transaction` failed 11 runs of 11 with `DuckDB`'s
//! "Too many open files" while it read back its spool of 1,790 files by 시군구 (7 tables of about 255),
//! so the 실거래 Gold's shadow never committed and its takeover had no verdict.

use std::collections::BTreeMap;

use crate::{Job, Unit, GENERATE};

/// The drop-in a service gets its open-files limit from, beside the units.
pub const OPEN_FILES_DROPIN: &str = "50-open-files.conf";
/// systemd's default soft limit: a job that needs no more states nothing.
pub const DEFAULT_SOFT_LIMIT: u32 = 1024;
/// systemd's default hard limit: no unit of ours gets more than any unit has.
pub const DEFAULT_HARD_LIMIT: u32 = 524_288;

/// The open-files drop-in of `service`, relative to the unit directory.
#[must_use]
pub fn open_files_dropin(service: &str) -> String {
    format!("{service}.d/{OPEN_FILES_DROPIN}")
}

/// A service's open-files drop-in: its job's `LimitNOFILE=`, soft and hard alike.
#[must_use]
pub fn render_open_files_dropin(open_files: u32) -> String {
    format!(
        "# Rendered from orchestration/jobs.v1.json open_files (root ADR-0574 §8); do not edit.
         # Run: {GENERATE}
[Service]
LimitNOFILE={open_files}
"
    )
}

/// Every job's open-files drop-in, and every problem with what the jobs state.
///
/// A problem is: a limit without `open_files_measured` (or the reverse), a limit that is not above
/// systemd's default or is above its hard limit, and jobs that run one service but state different
/// limits (a service has one).
#[must_use]
pub fn render(
    jobs: &[Job],
    read: &dyn Fn(&str) -> Result<String, String>,
) -> (Vec<Unit>, Vec<String>) {
    let mut problems = Vec::new();
    let mut by_service: BTreeMap<&str, (u32, &str)> = BTreeMap::new();
    for job in jobs {
        let measured = job
            .open_files_measured
            .as_deref()
            .is_some_and(|text| !text.trim().is_empty());
        let Some(open_files) = job.open_files else {
            if job.open_files_measured.is_some() {
                problems.push(format!(
                    "{}: states open_files_measured without open_files",
                    job.id
                ));
            }
            continue;
        };
        if !measured {
            problems.push(format!(
                "{}: open_files {open_files} states no open_files_measured: where the number comes from",
                job.id
            ));
        }
        if open_files <= DEFAULT_SOFT_LIMIT || open_files > DEFAULT_HARD_LIMIT {
            problems.push(format!(
                "{}: open_files {open_files} is not above systemd's default {DEFAULT_SOFT_LIMIT} \
                 and at most its hard limit {DEFAULT_HARD_LIMIT}",
                job.id
            ));
        }
        match by_service.get(job.systemd_service.as_str()) {
            Some((other, id)) if *other != open_files => problems.push(format!(
                "{}: runs {} as {id} does, which states open_files {other}, not {open_files}",
                job.id, job.systemd_service
            )),
            Some(_) => {}
            None => {
                by_service.insert(&job.systemd_service, (open_files, &job.id));
            }
        }
    }
    let units = by_service
        .into_iter()
        .map(|(service, (open_files, _))| {
            let file = open_files_dropin(service);
            Unit {
                current: read(&file).unwrap_or_default(),
                rendered: render_open_files_dropin(open_files),
                file,
            }
        })
        .collect();
    (units, problems)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: &str, service: &str, open_files: Option<u32>, measured: Option<&str>) -> Job {
        Job {
            id: id.to_owned(),
            systemd_service: service.to_owned(),
            time_limit_minutes: 60,
            stop_grace_minutes: 5,
            state_paths: Vec::new(),
            command: None,
            stall_limit_minutes: None,
            open_files,
            open_files_measured: measured.map(str::to_owned),
        }
    }

    fn absent(_: &str) -> Result<String, String> {
        Err("absent".to_owned())
    }

    #[test]
    fn a_job_that_states_its_open_files_gets_its_services_drop_in() {
        let jobs = [
            job(
                "spool",
                "foundation-x@spool.service",
                Some(65_536),
                Some("failed at 1024"),
            ),
            job("plain", "foundation-y.service", None, None),
        ];
        let (units, problems) = render(&jobs, &absent);
        assert_eq!(problems, Vec::<String>::new());
        assert_eq!(units.len(), 1);
        let unit = units
            .first()
            .map(|unit| (unit.file.as_str(), unit.rendered.as_str()));
        assert_eq!(
            unit.map(|(file, _)| file),
            Some("foundation-x@spool.service.d/50-open-files.conf")
        );
        assert!(unit.is_some_and(|(_, text)| text.ends_with("[Service]\nLimitNOFILE=65536\n")));
    }

    #[test]
    fn a_limit_without_where_it_comes_from_is_refused() {
        let (_, problems) = render(&[job("a", "a.service", Some(65_536), None)], &absent);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("open_files_measured")),
            "{problems:?}"
        );
        let (_, problems) = render(&[job("a", "a.service", None, Some("why"))], &absent);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("without open_files")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_limit_at_the_default_or_past_the_hard_limit_is_refused() {
        for open_files in [1024, 524_289] {
            let (_, problems) = render(
                &[job("a", "a.service", Some(open_files), Some("why"))],
                &absent,
            );
            assert_eq!(problems.len(), 1, "{open_files}: {problems:?}");
        }
    }

    #[test]
    fn jobs_that_run_one_service_state_one_limit() {
        let jobs = [
            job("a", "foundation-x.service", Some(4096), Some("why")),
            job("b", "foundation-x.service", Some(8192), Some("why")),
        ];
        let (units, problems) = render(&jobs, &absent);
        assert_eq!(units.len(), 1);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("runs foundation-x.service")),
            "{problems:?}"
        );
    }
}
