//! Whether main is red, as Prometheus series, so the existing Alertmanager tells `#alerts` (root
//! ADR-0554), the way the security advisories go (root ADR-0278).
//!
//! `main-health.yml` decides it every 10 minutes from the runs GitHub already made; the host's
//! service checks (`scripts/ops/service-checks.py`) read its newest verdict every hour into the
//! `main` part of the file they already write; this only reads that file. One series per red job,
//! with labels that stay the same while the job stays red, so Alertmanager sends one notice when it
//! turns red and one when it is green again. Until 2026-11-21 main-health kept a GitHub issue, and
//! issues are off in this repository: 11-14..18 the red Foundation nightly reached no one.

use std::fmt::Write as _;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::routes::prometheus_label_value as escape;

const HELP: &str = concat!(
    "# HELP foundation_main_health_readable Whether the service checks could read main-health's newest verdict on their last run (root ADR-0554).\n",
    "# TYPE foundation_main_health_readable gauge\n",
    "# HELP foundation_main_red_jobs How many jobs are red on main, as main-health's newest verdict says.\n",
    "# TYPE foundation_main_red_jobs gauge\n",
    "# HELP foundation_main_red_job A job red on main; one series per workflow and job.\n",
    "# TYPE foundation_main_red_job gauge\n",
);

/// A file the service checks last wrote this long ago says nothing about now: they run hourly.
const STALE_HOURS: i64 = 3;

#[derive(Deserialize)]
struct File {
    main: Option<MainHealth>,
}

#[derive(Deserialize)]
struct MainHealth {
    checked_at: DateTime<Utc>,
    readable: bool,
    reason: Option<String>,
    link: Option<String>,
    red_jobs: u32,
    jobs: Vec<RedJob>,
}

#[derive(Deserialize)]
struct RedJob {
    workflow: String,
    job: String,
}

/// The series of the file the service checks wrote; none when there is no file or no `main` part,
/// which the rule reads as unreadable (`absent`).
pub(in crate::routes) fn main_red_metrics() -> String {
    let main = std::env::var_os(super::service_checks::PATH_ENV)
        .and_then(|path| std::fs::read(Path::new(&path)).ok())
        .and_then(|bytes| serde_json::from_slice::<File>(&bytes).ok())
        .and_then(|file| file.main);
    render(main.as_ref(), Utc::now())
}

fn render(main: Option<&MainHealth>, now: DateTime<Utc>) -> String {
    let mut body = String::from(HELP);
    let Some(main) = main else {
        return body;
    };
    let stale = now.signed_duration_since(main.checked_at).num_hours() >= STALE_HOURS;
    let (readable, reason) = if stale {
        (false, "stale")
    } else {
        (main.readable, main.reason.as_deref().unwrap_or_default())
    };
    let _ = writeln!(
        body,
        "foundation_main_health_readable{{reason=\"{}\"}} {}",
        escape(reason),
        u8::from(readable)
    );
    // What was last known stays while it cannot be read: a red job is not announced as fixed and
    // then as red again.
    let _ = writeln!(body, "foundation_main_red_jobs {}", main.red_jobs);
    let link = main.link.as_deref().unwrap_or_default();
    for job in &main.jobs {
        let _ = writeln!(
            body,
            "foundation_main_red_job{{workflow=\"{}\",job=\"{}\",link=\"{}\"}} 1",
            escape(&job.workflow),
            escape(&job.job),
            escape(link),
        );
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example both sides test with (`orchestration/tests/test_service_checks.py`).
    fn example() -> Result<MainHealth, Box<dyn std::error::Error>> {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        let text = std::fs::read_to_string(
            Path::new(&crate_dir).join("src/routes/data_catalog/service-checks.example.json"),
        )?;
        serde_json::from_str::<File>(&text)?
            .main
            .ok_or_else(|| "the example has no main part".into())
    }

    fn at(text: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        DateTime::parse_from_rfc3339(text).map(|time| time.with_timezone(&Utc))
    }

    fn series<'a>(body: &'a str, name: &str) -> Vec<&'a str> {
        body.lines()
            .filter(|line| {
                line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} "))
            })
            .collect()
    }

    #[test]
    fn each_red_job_is_one_series_beside_the_count_and_whether_it_was_read(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let body = render(Some(&example()?), at("2026-10-13T03:00:00Z")?);
        assert_eq!(
            series(&body, "foundation_main_red_job"),
            vec!["foundation_main_red_job{workflow=\"Foundation Platform CI\",job=\"Postgres integration tests\",link=\"https://github.com/example-org/example-repo/actions/workflows/main-health.yml\"} 1"]
        );
        assert_eq!(
            series(&body, "foundation_main_red_jobs"),
            vec!["foundation_main_red_jobs 1"]
        );
        assert_eq!(
            series(&body, "foundation_main_health_readable"),
            vec!["foundation_main_health_readable{reason=\"\"} 1"]
        );
        Ok(())
    }

    #[test]
    fn a_stale_file_is_unreadable_but_keeps_what_it_knew() -> Result<(), Box<dyn std::error::Error>>
    {
        let body = render(Some(&example()?), at("2026-10-13T06:00:00Z")?);
        assert_eq!(
            series(&body, "foundation_main_health_readable"),
            vec!["foundation_main_health_readable{reason=\"stale\"} 0"]
        );
        assert_eq!(series(&body, "foundation_main_red_job").len(), 1);
        Ok(())
    }

    #[test]
    fn no_file_or_no_main_part_is_no_series() {
        assert_eq!(render(None, Utc::now()), HELP);
        assert!(
            serde_json::from_str::<File>(r#"{"edge": null}"#).is_ok_and(|file| file.main.is_none())
        );
    }
}
