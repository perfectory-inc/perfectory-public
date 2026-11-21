//! Whether the repository is what `tools/github` declares and Renovate has a pulse, as Prometheus
//! series, so the existing Alertmanager tells `#alerts` (root ADR-0570), the way main's health
//! (`main_red.rs`, root ADR-0554) and the security advisories (root ADR-0278) go.
//!
//! The daily `dependency-advisories.yml` job `Repository drift and Renovate pulse` fails and names
//! each finding in an annotation; the host's service checks (`scripts/ops/service-checks.py`) read
//! the newest verdict into the `repository` part of the file they already write; this only reads that
//! file. One series per finding, whose label is the finding's own sentence, so Alertmanager sends one
//! notice when it appears and one when it is gone. Until 2026-11-21 the job kept a GitHub issue, and
//! issues are off in this repository.

use std::fmt::Write as _;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::routes::prometheus_label_value as escape;

const HELP: &str = concat!(
    "# HELP foundation_repository_drift_readable Whether the service checks could read the repository drift job's newest verdict on their last run (root ADR-0570).\n",
    "# TYPE foundation_repository_drift_readable gauge\n",
    "# HELP foundation_repository_drift_findings How many findings the repository drift job's newest verdict names.\n",
    "# TYPE foundation_repository_drift_findings gauge\n",
    "# HELP foundation_repository_drift A finding of the repository drift job; one series per finding.\n",
    "# TYPE foundation_repository_drift gauge\n",
);

/// The job runs daily and the service checks hourly: a file this old says nothing about now.
const STALE_HOURS: i64 = 3;

#[derive(Deserialize)]
struct File {
    repository: Option<Drift>,
}

#[derive(Deserialize)]
struct Drift {
    checked_at: DateTime<Utc>,
    readable: bool,
    reason: Option<String>,
    link: Option<String>,
    finding_count: u32,
    findings: Vec<String>,
}

/// The series of the file the service checks wrote; none when there is no file or no `repository`
/// part, which the rule reads as unreadable (`absent`).
pub(in crate::routes) fn repository_drift_metrics() -> String {
    let drift = std::env::var_os(super::service_checks::PATH_ENV)
        .and_then(|path| std::fs::read(Path::new(&path)).ok())
        .and_then(|bytes| serde_json::from_slice::<File>(&bytes).ok())
        .and_then(|file| file.repository);
    render(drift.as_ref(), Utc::now())
}

fn render(drift: Option<&Drift>, now: DateTime<Utc>) -> String {
    let mut body = String::from(HELP);
    let Some(drift) = drift else {
        return body;
    };
    let stale = now.signed_duration_since(drift.checked_at).num_hours() >= STALE_HOURS;
    let (readable, reason) = if stale {
        (false, "stale")
    } else {
        (drift.readable, drift.reason.as_deref().unwrap_or_default())
    };
    let _ = writeln!(
        body,
        "foundation_repository_drift_readable{{reason=\"{}\"}} {}",
        escape(reason),
        u8::from(readable)
    );
    // What was last known stays while it cannot be read.
    let _ = writeln!(
        body,
        "foundation_repository_drift_findings {}",
        drift.finding_count
    );
    let link = drift.link.as_deref().unwrap_or_default();
    for finding in &drift.findings {
        let _ = writeln!(
            body,
            "foundation_repository_drift{{finding=\"{}\",link=\"{}\"}} 1",
            escape(finding),
            escape(link),
        );
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example both sides test with (`orchestration/tests/test_service_checks.py`).
    fn example() -> Result<Drift, Box<dyn std::error::Error>> {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        let text = std::fs::read_to_string(
            Path::new(&crate_dir).join("src/routes/data_catalog/service-checks.example.json"),
        )?;
        serde_json::from_str::<File>(&text)?
            .repository
            .ok_or_else(|| "the example has no repository part".into())
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
    fn each_finding_is_one_series_beside_the_count_and_whether_it_was_read(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let body = render(Some(&example()?), at("2026-10-13T03:00:00Z")?);
        assert_eq!(
            series(&body, "foundation_repository_drift"),
            vec!["foundation_repository_drift{finding=\"settings: has_issues is True, declared False\",link=\"https://github.com/example-org/example-repo/actions/workflows/dependency-advisories.yml\"} 1"]
        );
        assert_eq!(
            series(&body, "foundation_repository_drift_findings"),
            vec!["foundation_repository_drift_findings 1"]
        );
        assert_eq!(
            series(&body, "foundation_repository_drift_readable"),
            vec!["foundation_repository_drift_readable{reason=\"\"} 1"]
        );
        Ok(())
    }

    #[test]
    fn a_stale_file_is_unreadable_but_keeps_what_it_knew() -> Result<(), Box<dyn std::error::Error>>
    {
        let body = render(Some(&example()?), at("2026-10-13T06:00:00Z")?);
        assert_eq!(
            series(&body, "foundation_repository_drift_readable"),
            vec!["foundation_repository_drift_readable{reason=\"stale\"} 0"]
        );
        assert_eq!(series(&body, "foundation_repository_drift").len(), 1);
        Ok(())
    }

    #[test]
    fn no_file_or_no_repository_part_is_no_series() {
        assert_eq!(render(None, Utc::now()), HELP);
        assert!(serde_json::from_str::<File>(r#"{"edge": null}"#)
            .is_ok_and(|file| file.repository.is_none()));
    }
}
