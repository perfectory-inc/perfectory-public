//! The repository's open security advisories as Prometheus series, so the existing Alertmanager
//! tells `#alerts` (root ADR-0278), the same way the operations map's problems go (root ADR-0208).
//!
//! The host's service checks (`scripts/ops/service-checks.py`) read them from GitHub every hour into
//! the `security` part of the file they already write; this only reads that file. One series per
//! source, advisory and package, with labels that stay the same while the advisory stays open, so
//! Alertmanager sends one notice when it opens and one when it is gone. A source that could not be
//! read keeps what it last knew there, so it is not announced as fixed and then as new again; it
//! says it could not be read on its own series.

use std::fmt::Write as _;
use std::path::Path;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;

use crate::routes::prometheus_label_value as escape;

const HELP: &str = concat!(
    "# HELP foundation_security_advisories_readable Whether the service checks could read a source of security advisories on their last run (root ADR-0278).\n",
    "# TYPE foundation_security_advisories_readable gauge\n",
    "# HELP foundation_security_advisory An open security advisory of the repository; one series per source, advisory and package.\n",
    "# TYPE foundation_security_advisory gauge\n",
    "# HELP foundation_security_token_expires_days Days until the host's read-only GitHub token for Dependabot alerts expires.\n",
    "# TYPE foundation_security_token_expires_days gauge\n",
);

/// A file the service checks last wrote this long ago says nothing about now: they run hourly.
const STALE_HOURS: i64 = 3;

#[derive(Deserialize)]
struct File {
    security: Option<Security>,
}

#[derive(Deserialize)]
struct Security {
    checked_at: DateTime<Utc>,
    token_expires_at: Option<String>,
    sources: std::collections::BTreeMap<String, Source>,
}

#[derive(Deserialize)]
struct Source {
    readable: bool,
    reason: Option<String>,
    findings: Vec<Finding>,
}

#[derive(Deserialize)]
struct Finding {
    advisory: String,
    package: String,
    severity: String,
    link: String,
}

/// The series of the file the service checks wrote; none when there is no file or no `security`
/// part, which the rule reads as unreadable (`absent`).
pub(in crate::routes) fn advisory_metrics() -> String {
    let security = std::env::var_os(super::service_checks::PATH_ENV)
        .and_then(|path| std::fs::read(Path::new(&path)).ok())
        .and_then(|bytes| serde_json::from_slice::<File>(&bytes).ok())
        .and_then(|file| file.security);
    render(security.as_ref(), Utc::now())
}

fn render(security: Option<&Security>, now: DateTime<Utc>) -> String {
    let mut body = String::from(HELP);
    let Some(security) = security else {
        return body;
    };
    let stale = now.signed_duration_since(security.checked_at).num_hours() >= STALE_HOURS;
    for (name, source) in &security.sources {
        let (readable, reason) = if stale {
            (false, "stale")
        } else {
            (
                source.readable,
                source.reason.as_deref().unwrap_or_default(),
            )
        };
        let _ = writeln!(
            body,
            "foundation_security_advisories_readable{{source=\"{}\",reason=\"{}\"}} {}",
            escape(name),
            escape(reason),
            u8::from(readable)
        );
        for finding in &source.findings {
            // `advisory_severity`, not `severity`: the rule's own `severity` label routes the alert.
            let _ = writeln!(
                body,
                "foundation_security_advisory{{source=\"{}\",advisory=\"{}\",package=\"{}\",advisory_severity=\"{}\",link=\"{}\"}} 1",
                escape(name),
                escape(&finding.advisory),
                escape(&finding.package),
                escape(&finding.severity),
                escape(&finding.link),
            );
        }
    }
    if let Some(days) = security
        .token_expires_at
        .as_deref()
        .and_then(|value| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S UTC").ok())
        .map(|expires| expires.and_utc().signed_duration_since(now).num_days())
    {
        let _ = writeln!(body, "foundation_security_token_expires_days {days}");
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example both sides test with (`orchestration/tests/test_service_checks.py`).
    fn example() -> Result<Security, Box<dyn std::error::Error>> {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        let text = std::fs::read_to_string(
            Path::new(&crate_dir).join("src/routes/data_catalog/service-checks.example.json"),
        )?;
        serde_json::from_str::<File>(&text)?
            .security
            .ok_or_else(|| "the example has no security part".into())
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
    fn each_open_advisory_is_one_series_and_each_source_says_whether_it_was_read(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let body = render(Some(&example()?), at("2026-10-13T03:00:00Z")?);
        assert_eq!(
            series(&body, "foundation_security_advisory"),
            vec![
                "foundation_security_advisory{source=\"dependabot\",advisory=\"GHSA-aaaa-bbbb-cccc\",package=\"npm/example-package\",advisory_severity=\"high\",link=\"https://github.com/example-org/example-repo/security/dependabot/1\"} 1",
                "foundation_security_advisory{source=\"osv-ratchet\",advisory=\"GHSA-dddd-eeee-ffff\",package=\"crates.io/example-crate\",advisory_severity=\"\",link=\"https://github.com/example-org/example-repo/actions/workflows/monorepo-guard.yml?query=branch%3Amain\"} 1",
            ]
        );
        assert_eq!(
            series(&body, "foundation_security_advisories_readable"),
            vec![
                "foundation_security_advisories_readable{source=\"cargo-advisories\",reason=\"github-error\"} 0",
                "foundation_security_advisories_readable{source=\"dependabot\",reason=\"\"} 1",
                "foundation_security_advisories_readable{source=\"osv-ratchet\",reason=\"\"} 1",
            ]
        );
        assert_eq!(
            series(&body, "foundation_security_token_expires_days"),
            vec!["foundation_security_token_expires_days 364"]
        );
        Ok(())
    }

    #[test]
    fn the_labels_do_not_change_while_the_advisory_stays_open(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Only the expiry's value moves with time; every label is the same an hour later.
        let security = example()?;
        let labels = |body: String| {
            body.lines()
                .filter(|line| !line.starts_with("foundation_security_token_expires_days"))
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            labels(render(Some(&security), at("2026-10-13T02:20:00Z")?)),
            labels(render(Some(&security), at("2026-10-13T04:10:00Z")?))
        );
        Ok(())
    }

    #[test]
    fn a_stale_file_is_unreadable_but_keeps_what_it_knew() -> Result<(), Box<dyn std::error::Error>>
    {
        let body = render(Some(&example()?), at("2026-10-13T06:00:00Z")?);
        let readable = series(&body, "foundation_security_advisories_readable");
        assert_eq!(readable.len(), 3);
        assert!(readable
            .iter()
            .all(|line| line.contains("reason=\"stale\"") && line.ends_with(" 0")));
        assert_eq!(series(&body, "foundation_security_advisory").len(), 2);
        Ok(())
    }

    #[test]
    fn no_file_or_no_security_part_is_no_series() {
        assert_eq!(render(None, Utc::now()), HELP);
        assert!(serde_json::from_str::<File>(r#"{"edge": null}"#)
            .is_ok_and(|file| file.security.is_none()));
    }
}
