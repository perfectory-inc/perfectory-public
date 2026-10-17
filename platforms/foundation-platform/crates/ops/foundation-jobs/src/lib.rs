//! The scheduled job list, `orchestration/jobs.v1.json`, and what is rendered from it (root ADR-0270 §1).
//!
//! A job's facts are stated once, in the list. The parts of its systemd unit that repeat one of them
//! are rendered from the list, and this crate's test refuses a committed unit that says anything
//! else. Today that is the unit's `TimeoutStartSec=` line, from the job's `time_limit_minutes`.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

/// The job list, relative to the platform root.
pub const JOB_LIST: &str = "orchestration/jobs.v1.json";
/// The systemd units, relative to the platform root.
pub const UNIT_DIR: &str = "infra/systemd";
/// What a reader of the units is told when one differs from its rendering.
pub const GENERATE: &str =
    "cargo run -p foundation-jobs -- generate (from platforms/foundation-platform)";

/// The job list, as far as rendering reads it.
#[derive(Debug, Deserialize)]
pub struct JobList {
    /// Every scheduled job.
    pub jobs: Vec<Job>,
}

/// One job, as far as rendering reads it.
#[derive(Debug, Deserialize)]
pub struct Job {
    /// The job's id (its DAG is `foundation_<id>`).
    pub id: String,
    /// The systemd service that runs it; an instance `a@b.service` runs from the unit `a@.service`.
    pub systemd_service: String,
    /// How long one run may run, in minutes.
    pub time_limit_minutes: u32,
    /// How much longer than its run the scheduler gives one start, in minutes.
    pub stop_grace_minutes: u32,
}

impl JobList {
    /// Parses the job list.
    ///
    /// # Errors
    /// When the text is not a job list with the fields rendering reads.
    pub fn parse(text: &str) -> Result<Self, String> {
        serde_json::from_str(text).map_err(|error| format!("{JOB_LIST}: {error}"))
    }

    /// Reads the job list under `platform_root`.
    ///
    /// # Errors
    /// When the file cannot be read or parsed.
    pub fn read(platform_root: &Path) -> Result<Self, String> {
        let path = platform_root.join(JOB_LIST);
        let text = std::fs::read_to_string(&path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        Self::parse(&text)
    }
}

/// The unit file a service runs from: an instance `a@b.service` runs from its template `a@.service`.
#[must_use]
pub fn unit_file_name(service: &str) -> String {
    match service.split_once('@') {
        Some((name, instance)) => match instance.rsplit_once('.') {
            Some((_, suffix)) => format!("{name}@.{suffix}"),
            None => service.to_owned(),
        },
        None => service.to_owned(),
    }
}

/// The `TimeoutStartSec=` line for a limit: whole hours as `<n>h`, otherwise `<n>m`.
#[must_use]
pub fn time_limit_line(minutes: u32) -> String {
    if minutes.is_multiple_of(60) {
        format!("TimeoutStartSec={}h", minutes / 60)
    } else {
        format!("TimeoutStartSec={minutes}m")
    }
}

/// The unit text with its one `TimeoutStartSec=` line set to `minutes`.
///
/// # Errors
/// When the unit has no such line, or more than one.
pub fn render_unit(unit: &str, minutes: u32) -> Result<String, String> {
    let count = unit
        .lines()
        .filter(|line| line.starts_with("TimeoutStartSec="))
        .count();
    if count != 1 {
        return Err(format!(
            "has {count} TimeoutStartSec= lines; a job unit has exactly one"
        ));
    }
    Ok(unit
        .split_inclusive('\n')
        .map(|line| {
            if line.starts_with("TimeoutStartSec=") {
                let end = if line.ends_with('\n') { "\n" } else { "" };
                format!("{}{end}", time_limit_line(minutes))
            } else {
                line.to_owned()
            }
        })
        .collect())
}

/// The seconds a unit gives its stop (`TimeoutStopSec=`; systemd's default is 90).
///
/// # Errors
/// When the value is not seconds (`120`, `120s`) or minutes (`2min`, `2m`), or is given twice.
pub fn stop_seconds(unit: &str) -> Result<u32, String> {
    let values: Vec<&str> = unit
        .lines()
        .filter_map(|line| line.strip_prefix("TimeoutStopSec="))
        .collect();
    let value = match values.as_slice() {
        [] => return Ok(90),
        [value] => *value,
        _ => return Err("has more than one TimeoutStopSec= line".to_owned()),
    };
    let (number, unit_seconds) = value
        .strip_suffix("min")
        .or_else(|| value.strip_suffix('m'))
        .map_or_else(
            || (value.strip_suffix('s').unwrap_or(value), 1),
            |minutes| (minutes, 60),
        );
    number
        .parse::<u32>()
        .ok()
        .and_then(|number| number.checked_mul(unit_seconds))
        .ok_or_else(|| format!("TimeoutStopSec={value} is not seconds or minutes"))
}

/// One job unit: what its file holds now and what the job list says it should hold.
#[derive(Debug)]
pub struct Unit {
    /// The unit's file name under [`UNIT_DIR`].
    pub file: String,
    /// The file as it is.
    pub current: String,
    /// The file as rendered from the job list.
    pub rendered: String,
}

/// Every job unit, rendered from the list. `read` returns a unit file's text by its file name.
///
/// # Errors
/// Every problem found: a limit of zero, jobs that share a unit but state different limits, a unit
/// that cannot be read or has no single `TimeoutStartSec=` line, and a stop grace no longer than the
/// unit's `TimeoutStopSec=` (the scheduler would give up while the unit still stops).
pub fn render(
    list: &JobList,
    read: &dyn Fn(&str) -> Result<String, String>,
) -> Result<Vec<Unit>, Vec<String>> {
    let mut problems = Vec::new();
    let mut limits: BTreeMap<String, (u32, &str)> = BTreeMap::new();
    let mut graces: BTreeMap<String, (u32, &str)> = BTreeMap::new();
    for job in &list.jobs {
        if job.time_limit_minutes == 0 {
            problems.push(format!("{}: time_limit_minutes must be above 0", job.id));
        }
        let file = unit_file_name(&job.systemd_service);
        let shortest = graces
            .entry(file.clone())
            .or_insert((job.stop_grace_minutes, &job.id));
        if job.stop_grace_minutes < shortest.0 {
            *shortest = (job.stop_grace_minutes, &job.id);
        }
        match limits.get(&file) {
            Some((minutes, other)) if *minutes != job.time_limit_minutes => problems.push(format!(
                "{}: shares {file} with {other}, which states time_limit_minutes {minutes}, not {}",
                job.id, job.time_limit_minutes
            )),
            Some(_) => {}
            None => {
                limits.insert(file, (job.time_limit_minutes, &job.id));
            }
        }
    }
    let mut units = Vec::new();
    for (file, (minutes, _)) in limits {
        let unit = read(&file).and_then(|current| {
            render_unit(&current, minutes).map(|rendered| Unit {
                file: file.clone(),
                current,
                rendered,
            })
        });
        match unit {
            Ok(unit) => {
                if let Some((grace, job)) = graces.get(&unit.file) {
                    match stop_seconds(&unit.current) {
                        Ok(stop) if u64::from(*grace) * 60 > u64::from(stop) => {}
                        Ok(stop) => problems.push(format!(
                            "{job}: stop_grace_minutes {grace} is not longer than {}'s TimeoutStopSec ({stop}s)",
                            unit.file
                        )),
                        Err(error) => problems.push(format!("{}: {error}", unit.file)),
                    }
                }
                units.push(unit);
            }
            Err(error) => problems.push(format!("{file}: {error}")),
        }
    }
    if problems.is_empty() {
        Ok(units)
    } else {
        Err(problems)
    }
}

/// Every way the committed units differ from the job list: each [`render`] problem, and each unit
/// whose file is not its rendering. Empty is a pass.
#[must_use]
pub fn check(list: &JobList, read: &dyn Fn(&str) -> Result<String, String>) -> Vec<String> {
    match render(list, read) {
        Err(problems) => problems,
        Ok(units) => units
            .into_iter()
            .filter(|unit| unit.current != unit.rendered)
            .map(|unit| {
                format!(
                    "{}: TimeoutStartSec is not the job list's time_limit_minutes; run: {GENERATE}",
                    unit.file
                )
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), String>;

    const UNIT: &str = "[Service]\nType=oneshot\nTimeoutStartSec=150m\nNoNewPrivileges=true\n";

    fn list(jobs: &[(&str, &str, u32)]) -> JobList {
        JobList {
            jobs: jobs
                .iter()
                .map(|(id, service, minutes)| Job {
                    id: (*id).to_owned(),
                    systemd_service: (*service).to_owned(),
                    time_limit_minutes: *minutes,
                    stop_grace_minutes: 5,
                })
                .collect(),
        }
    }

    fn reading(text: &'static str) -> impl Fn(&str) -> Result<String, String> {
        move |_| Ok(text.to_owned())
    }

    #[test]
    fn an_instance_runs_from_its_template() {
        assert_eq!(
            unit_file_name("foundation-silver-refresh@land_use_plan.service"),
            "foundation-silver-refresh@.service"
        );
        assert_eq!(
            unit_file_name("foundation-data-quality.service"),
            "foundation-data-quality.service"
        );
    }

    #[test]
    fn a_stop_grace_must_outlast_the_units_stop() {
        assert_eq!(stop_seconds("[Service]\n"), Ok(90));
        assert_eq!(stop_seconds("TimeoutStopSec=120\n"), Ok(120));
        assert_eq!(stop_seconds("TimeoutStopSec=2min\n"), Ok(120));
        assert!(stop_seconds("TimeoutStopSec=infinity\n").is_err());
        let mut jobs = list(&[("a", "a.service", 150)]);
        let slow_stop = "[Service]\nTimeoutStartSec=150m\nTimeoutStopSec=6min\n";
        assert_eq!(
            check(&jobs, &move |_: &str| Ok(slow_stop.to_owned())).len(),
            1
        );
        if let Some(job) = jobs.jobs.first_mut() {
            job.stop_grace_minutes = 7;
        }
        assert!(check(&jobs, &move |_: &str| Ok(slow_stop.to_owned())).is_empty());
    }

    #[test]
    fn whole_hours_are_written_in_hours() {
        assert_eq!(time_limit_line(240), "TimeoutStartSec=4h");
        assert_eq!(time_limit_line(150), "TimeoutStartSec=150m");
    }

    #[test]
    fn a_unit_that_says_the_lists_limit_passes() {
        let jobs = list(&[("a", "a.service", 150)]);
        assert_eq!(check(&jobs, &reading(UNIT)), Vec::<String>::new());
    }

    #[test]
    fn a_unit_that_says_another_limit_is_refused_and_generate_fixes_it() -> TestResult {
        let jobs = list(&[("a", "a.service", 160)]);
        let problems = check(&jobs, &reading(UNIT));
        assert_eq!(problems.len(), 1, "{problems:?}");
        let units = render(&jobs, &reading(UNIT)).map_err(|problems| problems.join("; "))?;
        let rendered = &units.first().ok_or("no unit rendered")?.rendered;
        assert!(rendered.contains("\nTimeoutStartSec=160m\n"), "{rendered}");
        assert!(rendered.ends_with("NoNewPrivileges=true\n"), "{rendered}");
        Ok(())
    }

    #[test]
    fn a_unit_without_the_line_or_with_two_is_refused() {
        let jobs = list(&[("a", "a.service", 150)]);
        let none = "[Service]\nType=oneshot\n";
        let two = "[Service]\nTimeoutStartSec=1h\nTimeoutStartSec=2h\n";
        assert_eq!(check(&jobs, &reading(none)).len(), 1);
        assert_eq!(check(&jobs, &reading(two)).len(), 1);
    }

    #[test]
    fn jobs_sharing_a_unit_must_state_one_limit() {
        let jobs = list(&[
            ("lane_a", "foundation-x@a.service", 150),
            ("lane_b", "foundation-x@b.service", 160),
        ]);
        let problems = check(&jobs, &reading(UNIT));
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("shares foundation-x@.service")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_limit_of_zero_is_refused() {
        let jobs = list(&[("a", "a.service", 0)]);
        assert!(!check(&jobs, &reading(UNIT)).is_empty());
    }

    #[test]
    fn a_unit_that_cannot_be_read_is_a_problem() {
        let jobs = list(&[("a", "a.service", 150)]);
        let problems = check(&jobs, &|_| Err("missing".to_owned()));
        assert_eq!(problems, vec!["a.service: missing".to_owned()]);
    }
}
