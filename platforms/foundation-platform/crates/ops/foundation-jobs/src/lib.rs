//! The scheduled job list, `orchestration/jobs.v1.json`, and what is rendered from it (root ADR-0270 §1).
//!
//! A job's facts are stated once, in the list. The parts of its systemd unit that repeat one of them
//! are rendered from the list, and this crate's test refuses a committed unit that says anything
//! else. Today that is the unit's `TimeoutStartSec=` line, from the job's `time_limit_minutes`, and
//! the `ReadWritePaths=` line of a `ProtectSystem=strict` unit, from its `state_paths` (root ADR-0266).
//! It also renders the host's `tmpfiles.d` file that creates the job state directories, with their
//! mode and owner, before any unit or container uses them.

pub mod runner;

use std::collections::{BTreeMap, BTreeSet};
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
/// The generated `tmpfiles.d` file, relative to the platform root.
pub const TMPFILES: &str = "infra/tmpfiles/foundation-job-state.conf";
/// The account and group that own every job state directory.
pub const STATE_OWNER: &str = "foundation-platform";
/// What a reader of the units is told when one differs from its rendering.
pub const GENERATE: &str =
    "cargo run -p foundation-jobs -- generate (from platforms/foundation-platform)";

/// The job list, as far as rendering reads it.
#[derive(Debug, Deserialize)]
pub struct JobList {
    /// Every scheduled job.
    pub jobs: Vec<Job>,
    /// Every directory the host makes for the jobs, with its mode.
    pub state_directories: Vec<StateDirectory>,
    /// Job state paths this list does not make, each with why.
    pub state_paths_not_created_here: BTreeMap<String, String>,
}

/// A directory the host makes for the jobs.
#[derive(Debug, Deserialize)]
pub struct StateDirectory {
    /// The directory, absolute.
    pub path: String,
    /// Its octal mode, four digits (`0755`, `2770`, `0777`).
    pub mode: String,
    /// The account and group that own it; [`STATE_OWNER`] when absent.
    #[serde(default)]
    pub owner: Option<String>,
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

/// Every way the state directories disagree with the jobs' `state_paths`. Empty is a pass.
///
/// Each job state path is a state directory or is named in `state_paths_not_created_here`; each
/// state directory is a job state path or lies under one; a mode is four octal digits.
#[must_use]
pub fn state_directory_problems(list: &JobList) -> Vec<String> {
    let state_paths: BTreeSet<&str> = list
        .jobs
        .iter()
        .flat_map(|job| job.state_paths.iter().map(String::as_str))
        .collect();
    let mut problems = Vec::new();
    let mut seen = BTreeSet::new();
    for directory in &list.state_directories {
        let path = directory.path.as_str();
        if !seen.insert(path) {
            problems.push(format!("{path}: listed twice in state_directories"));
        }
        let mode = directory.mode.as_bytes();
        if mode.len() != 4 || !mode.iter().all(|digit| (b'0'..=b'7').contains(digit)) {
            problems.push(format!(
                "{path}: mode {} is not four octal digits",
                directory.mode
            ));
        }
        let under_a_state_path = state_paths
            .iter()
            .any(|state| path == *state || path.starts_with(&format!("{state}/")));
        if !under_a_state_path {
            problems.push(format!(
                "{path}: a state directory under no job's state_paths"
            ));
        }
    }
    for path in &state_paths {
        let made = seen.contains(path);
        let named = list.state_paths_not_created_here.contains_key(*path);
        if made == named {
            problems.push(format!(
                "{path}: a job state path must be in state_directories or in state_paths_not_created_here, not {}",
                if made { "both" } else { "neither" }
            ));
        }
    }
    for path in list.state_paths_not_created_here.keys() {
        if !state_paths.contains(path.as_str()) {
            problems.push(format!(
                "{path}: in state_paths_not_created_here but no job's state path"
            ));
        }
    }
    problems
}

/// The `tmpfiles.d` file that makes every state directory with its mode and owner. `d` creates a
/// missing directory and sets the mode and owner of one that exists; no age, so nothing is cleaned.
///
/// # Errors
/// Every [`state_directory_problems`] problem.
pub fn render_tmpfiles(list: &JobList) -> Result<String, Vec<String>> {
    let problems = state_directory_problems(list);
    if !problems.is_empty() {
        return Err(problems);
    }
    let mut out = format!(
        "# Generated from orchestration/jobs.v1.json state_directories (root ADR-0270 §1); do not edit.\n\
         # Run: {GENERATE}\n"
    );
    out.extend(list.state_directories.iter().map(|directory| {
        let owner = directory.owner.as_deref().unwrap_or(STATE_OWNER);
        format!(
            "d {} {} {owner} {owner} -\n",
            directory.path, directory.mode
        )
    }));
    Ok(out)
}

/// Every way the committed `tmpfiles.d` file differs from the job list. Empty is a pass.
#[must_use]
pub fn check_tmpfiles(list: &JobList, committed: Result<String, String>) -> Vec<String> {
    match (render_tmpfiles(list), committed) {
        (Err(problems), _) => problems,
        (Ok(_), Err(error)) => vec![format!("{TMPFILES}: {error}")],
        (Ok(rendered), Ok(committed)) if rendered != committed => vec![format!(
            "{TMPFILES}: is not what the job list's state_directories say; run: {GENERATE}"
        )],
        (Ok(_), Ok(_)) => Vec::new(),
    }
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
            state_directories: Vec::new(),
            state_paths_not_created_here: BTreeMap::new(),
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

    fn with_state(
        paths_of_job: &[&str],
        directories: &[(&str, &str)],
        elsewhere: &[&str],
    ) -> JobList {
        let mut jobs = list(&[("a", "a.service", 150)]);
        if let Some(job) = jobs.jobs.first_mut() {
            job.state_paths = paths(paths_of_job);
        }
        jobs.state_directories = directories
            .iter()
            .map(|(path, mode)| StateDirectory {
                path: (*path).to_owned(),
                mode: (*mode).to_owned(),
                owner: None,
            })
            .collect();
        jobs.state_paths_not_created_here = elsewhere
            .iter()
            .map(|path| ((*path).to_owned(), "made by Docker".to_owned()))
            .collect();
        jobs
    }

    #[test]
    fn the_tmpfiles_file_makes_each_state_directory_with_its_mode() -> TestResult {
        let jobs = with_state(
            &["/a", "/b"],
            &[("/a", "0755"), ("/a/lake", "0777")],
            &["/b"],
        );
        let rendered = render_tmpfiles(&jobs).map_err(|problems| problems.join("; "))?;
        assert!(
            rendered.contains("\nd /a 0755 foundation-platform foundation-platform -\n"),
            "{rendered}"
        );
        assert!(
            rendered.ends_with("\nd /a/lake 0777 foundation-platform foundation-platform -\n"),
            "{rendered}"
        );
        assert_eq!(
            check_tmpfiles(&jobs, Ok(rendered.clone())),
            Vec::<String>::new()
        );
        assert_eq!(
            check_tmpfiles(&jobs, Ok(rendered.replace("0777", "0755"))).len(),
            1
        );
        let mut owned = with_state(&["/a"], &[("/a", "0755")], &[]);
        if let Some(directory) = owned.state_directories.first_mut() {
            directory.owner = Some("foundation-spark".to_owned());
        }
        let rendered_owned = render_tmpfiles(&owned).map_err(|problems| problems.join("; "))?;
        assert!(
            rendered_owned.ends_with("\nd /a 0755 foundation-spark foundation-spark -\n"),
            "{rendered_owned}"
        );
        Ok(())
    }

    #[test]
    fn a_state_path_nobody_makes_is_refused() {
        // #625's class: a path a unit or container uses that the host never made.
        let jobs = with_state(&["/a", "/b"], &[("/a", "0755")], &[]);
        assert!(state_directory_problems(&jobs)
            .iter()
            .any(|problem| problem.starts_with("/b:")));
    }

    #[test]
    fn a_directory_outside_every_state_path_a_bad_mode_and_a_stale_exception_are_refused() {
        let jobs = with_state(
            &["/a"],
            &[("/a", "755"), ("/elsewhere", "0755")],
            &["/gone"],
        );
        let problems = state_directory_problems(&jobs);
        assert_eq!(problems.len(), 3, "{problems:?}");
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
        let unit = format!("{UNIT}ReadWritePaths=/var/lib/foundation-platform/map-edit-fold\n");
        let problems = check(&jobs, &move |_: &str| Ok(unit.clone()));
        assert_eq!(problems.len(), 1, "{problems:?}");
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
