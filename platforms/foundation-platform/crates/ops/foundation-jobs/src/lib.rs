//! The scheduled job list, `orchestration/jobs.v1.json`, and what is rendered from it (root ADR-0270 §1).
//!
//! A job's facts are stated once, in the list. The parts of its systemd unit that repeat one of them
//! are rendered from the list, and this crate's test refuses a committed unit that says anything
//! else. Today that is the unit's `TimeoutStartSec=` line, from the job's `time_limit_minutes`, and
//! the `ReadWritePaths=` line of a `ProtectSystem=strict` unit, from its `state_paths` (root ADR-0266).

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

/// The job list, relative to the platform root.
pub const JOB_LIST: &str = "orchestration/jobs.v1.json";
/// The systemd units, relative to the platform root.
pub const UNIT_DIR: &str = "infra/systemd";
/// Lines every job unit carries, the same for every job (root ADR-0270 §1).
///
/// A one-shot run as the platform account, its failure reported, inside the sandbox; the list does
/// not repeat them. A unit without one is refused, not rendered: where a line goes is the unit's.
pub const JOB_UNIT_LINES: &[&str] = &[
    "Type=oneshot",
    "User=foundation-platform",
    "Group=foundation-platform",
    "OnFailure=foundation-unit-failed@%n.service",
    "NoNewPrivileges=true",
    "PrivateTmp=true",
    "ProtectHome=true",
    "ProtectSystem=strict",
];
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
    /// Where the job writes and keeps state across runs; absolute paths.
    pub state_paths: Vec<String>,
}

/// What a job unit repeats from the job list. Jobs that share a unit must state the same.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitFacts {
    /// The unit's `TimeoutStartSec=`, in minutes.
    pub time_limit_minutes: u32,
    /// The unit's `ReadWritePaths=`, when it is a `ProtectSystem=strict` unit.
    pub state_paths: Vec<String>,
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

/// The unit text with what it repeats from the job list set to `facts`.
///
/// # Errors
/// When the unit has no `TimeoutStartSec=` line, or more than one.
pub fn render_unit(unit: &str, facts: &UnitFacts) -> Result<String, String> {
    let timed = render_time_limit(unit, facts.time_limit_minutes)?;
    Ok(render_write_paths(&timed, &facts.state_paths))
}

/// The unit with one `ReadWritePaths=` line holding `paths` (none when empty).
///
/// The line goes where the first one was, or after the last `ProtectSystem=` line. A unit that is
/// not `ProtectSystem=strict` may write anywhere its user may, so it is left as it is.
#[must_use]
pub fn render_write_paths(unit: &str, paths: &[String]) -> String {
    if !unit.lines().any(|line| line == "ProtectSystem=strict") {
        return unit.to_owned();
    }
    let block = if paths.is_empty() {
        String::new()
    } else {
        format!("ReadWritePaths={}\n", paths.join(" "))
    };
    let lines: Vec<&str> = unit.split_inclusive('\n').collect();
    let first = lines
        .iter()
        .position(|line| line.starts_with("ReadWritePaths="));
    let kept: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|line| !line.starts_with("ReadWritePaths="))
        .collect();
    let at = first.or_else(|| {
        kept.iter()
            .rposition(|line| line.starts_with("ProtectSystem="))
            .map(|index| index + 1)
    });
    let mut out = String::with_capacity(unit.len() + block.len());
    for (index, line) in kept.iter().enumerate() {
        if Some(index) == at {
            out.push_str(&block);
        }
        out.push_str(line);
    }
    if at.is_none_or(|index| index >= kept.len()) {
        out.push_str(&block);
    }
    out
}

fn render_time_limit(unit: &str, minutes: u32) -> Result<String, String> {
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

/// The [`JOB_UNIT_LINES`] a unit does not carry, each as its own problem, and any second value of
/// one of their keys (a later line would override it).
#[must_use]
pub fn job_unit_line_problems(file: &str, unit: &str) -> Vec<String> {
    let lines: Vec<&str> = unit.lines().collect();
    JOB_UNIT_LINES
        .iter()
        .filter_map(|required| {
            let key = required.split_once('=').map_or(*required, |(key, _)| key);
            let values: Vec<&&str> = lines
                .iter()
                .filter(|line| line.split_once('=').is_some_and(|(found, _)| found == key))
                .collect();
            (values.len() != 1 || values.first().is_none_or(|line| **line != *required)).then(
                || format!("{file}: a job unit carries exactly one {required} (root ADR-0270 §1)"),
            )
        })
        .collect()
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
/// Every problem found: a limit of zero, a state path that is not absolute, jobs that share a unit
/// but state different facts, a unit that cannot be read or has no single `TimeoutStartSec=`, and
/// a stop grace no longer than the unit's `TimeoutStopSec=` (the scheduler would give up while the
/// unit still stops).
pub fn render(
    list: &JobList,
    read: &dyn Fn(&str) -> Result<String, String>,
) -> Result<Vec<Unit>, Vec<String>> {
    let mut problems = Vec::new();
    let mut by_unit: BTreeMap<String, (UnitFacts, &str)> = BTreeMap::new();
    let mut graces: BTreeMap<String, (u32, &str)> = BTreeMap::new();
    for job in &list.jobs {
        if job.time_limit_minutes == 0 {
            problems.push(format!("{}: time_limit_minutes must be above 0", job.id));
        }
        if let Some(path) = job.state_paths.iter().find(|path| !path.starts_with('/')) {
            problems.push(format!("{}: state path {path} is not absolute", job.id));
        }
        let facts = UnitFacts {
            time_limit_minutes: job.time_limit_minutes,
            state_paths: job.state_paths.clone(),
        };
        let file = unit_file_name(&job.systemd_service);
        let shortest = graces
            .entry(file.clone())
            .or_insert((job.stop_grace_minutes, &job.id));
        if job.stop_grace_minutes < shortest.0 {
            *shortest = (job.stop_grace_minutes, &job.id);
        }
        match by_unit.get(&file) {
            Some((shared, other)) if *shared != facts => problems.push(format!(
                "{}: shares {file} with {other}, which states {shared:?}, not {facts:?}",
                job.id
            )),
            Some(_) => {}
            None => {
                by_unit.insert(file, (facts, &job.id));
            }
        }
    }
    let mut units = Vec::new();
    for (file, (facts, _)) in by_unit {
        let unit = read(&file).and_then(|current| {
            render_unit(&current, &facts).map(|rendered| Unit {
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
                problems.extend(job_unit_line_problems(&unit.file, &unit.current));
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
                    "{}: TimeoutStartSec or ReadWritePaths is not what the job list says; run: {GENERATE}",
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

    const UNIT: &str = "[Unit]\nOnFailure=foundation-unit-failed@%n.service\n\n[Service]\n\
        Type=oneshot\nUser=foundation-platform\nGroup=foundation-platform\nTimeoutStartSec=150m\n\
        NoNewPrivileges=true\nPrivateTmp=true\nProtectHome=true\nProtectSystem=strict\n";

    #[test]
    fn a_job_unit_without_its_sandbox_or_alert_is_refused() {
        assert_eq!(
            job_unit_line_problems("a.service", UNIT),
            Vec::<String>::new()
        );
        let unsandboxed = UNIT.replace("ProtectSystem=strict\n", "");
        assert_eq!(job_unit_line_problems("a.service", &unsandboxed).len(), 1);
        let silent = UNIT.replace("OnFailure=foundation-unit-failed@%n.service\n", "");
        assert_eq!(job_unit_line_problems("a.service", &silent).len(), 1);
        let loosened = format!("{UNIT}ProtectSystem=full\n");
        assert_eq!(job_unit_line_problems("a.service", &loosened).len(), 1);
        let jobs = list(&[("a", "a.service", 150)]);
        let reading_unsandboxed = move |_: &str| Ok(unsandboxed.clone());
        assert_eq!(check(&jobs, &reading_unsandboxed).len(), 1);
    }

    fn list(jobs: &[(&str, &str, u32)]) -> JobList {
        JobList {
            jobs: jobs
                .iter()
                .map(|(id, service, minutes)| Job {
                    id: (*id).to_owned(),
                    systemd_service: (*service).to_owned(),
                    time_limit_minutes: *minutes,
                    stop_grace_minutes: 5,
                    state_paths: Vec::new(),
                })
                .collect(),
        }
    }

    fn paths(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_owned()).collect()
    }

    #[test]
    fn a_strict_units_write_paths_are_its_state_paths() {
        let unit = "[Service]\nProtectSystem=strict\nReadWritePaths=/old\nExecStart=/bin/true\n";
        assert_eq!(
            render_write_paths(unit, &paths(&["/a", "/b"])),
            "[Service]\nProtectSystem=strict\nReadWritePaths=/a /b\nExecStart=/bin/true\n"
        );
        assert_eq!(
            render_write_paths(unit, &[]),
            "[Service]\nProtectSystem=strict\nExecStart=/bin/true\n"
        );
        assert_eq!(
            render_write_paths("[Service]\nProtectSystem=strict\nX=1\n", &paths(&["/a"])),
            "[Service]\nProtectSystem=strict\nReadWritePaths=/a\nX=1\n"
        );
        assert_eq!(
            render_write_paths("[Service]\nProtectSystem=strict\n", &paths(&["/a"])),
            "[Service]\nProtectSystem=strict\nReadWritePaths=/a\n"
        );
        let open = "[Service]\nReadWritePaths=/kept\n";
        assert_eq!(render_write_paths(open, &paths(&["/a"])), open);
    }

    #[test]
    fn a_state_path_the_unit_does_not_allow_is_refused() {
        // The 2026-10-16 failure (#647): the job wrote where its unit's sandbox did not allow.
        let mut jobs = list(&[("parcel_registry", "a.service", 150)]);
        if let Some(job) = jobs.jobs.first_mut() {
            job.state_paths = paths(&["/var/lib/foundation-platform/lineage-stewardship"]);
        }
        let unit = "[Service]\nProtectSystem=strict\nReadWritePaths=/var/lib/foundation-platform/map-edit-fold\nTimeoutStartSec=150m\n";
        assert_eq!(check(&jobs, &move |_| Ok(unit.to_owned())).len(), 1);
    }

    #[test]
    fn a_relative_state_path_is_refused() {
        let mut jobs = list(&[("a", "a.service", 150)]);
        if let Some(job) = jobs.jobs.first_mut() {
            job.state_paths = paths(&["var/lib/x"]);
        }
        assert!(check(&jobs, &reading(UNIT))
            .iter()
            .any(|problem| problem.contains("not absolute")));
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
        let slow_stop = format!("{UNIT}TimeoutStopSec=6min\n");
        let reading_slow_stop = move |_: &str| Ok(slow_stop.clone());
        assert_eq!(check(&jobs, &reading_slow_stop).len(), 1);
        if let Some(job) = jobs.jobs.first_mut() {
            job.stop_grace_minutes = 7;
        }
        assert!(check(&jobs, &reading_slow_stop).is_empty());
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
        assert!(rendered.ends_with("ProtectSystem=strict\n"), "{rendered}");
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
