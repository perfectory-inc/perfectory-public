//! The operations map's problems as Prometheus series, so the existing Alertmanager tells Slack
//! (root ADR-0195 follow-up).
//!
//! The platform already pages `#alerts` from Alertmanager (`infra/observability/alertmanager`),
//! with the bot token mounted there; this adds no sender and no state of its own. Alertmanager
//! already does what "only on a change" needs: one notice when a series starts firing, one when it
//! resolves, deduplicated in its notification log. So each label here must stay the same for as
//! long as the problem is the same problem — a label that ticks (a running job's seconds so far)
//! would resolve and re-fire on every scrape. What the labels say changes only when a new run
//! changes it: a new failure cause, a new scheduled start missed, a new run's row count.
//!
//! The rule (`foundation-api.rules.yml`, `FoundationScheduledJobProblem`) holds a series through a
//! short gap (`keep_firing_for`) so a restart or one unread scrape is not a resolve.

use std::fmt::Write as _;
use std::time::Duration;

use crate::routes::prometheus_label_value as escape;
use chrono::{DateTime, TimeDelta, Utc};
use foundation_contracts::data_catalog::{
    FailureCause, ScheduledJobProblem, ScheduledJobStatus, ScheduledJobsStatus,
};

/// The console's own address (`#/flow?node=` opens a dataset's panel); without it the notice
/// carries no link.
const CONSOLE_URL_ENV: &str = "FOUNDATION_PLATFORM_DAWNEER_UI_URL";
/// The longest one-line cause a label carries; Slack shows one line, and the log line it quotes
/// is already masked.
const SUMMARY_CHARS: usize = 240;

/// How long a scrape waits for the map's read; Prometheus gives a scrape 10 seconds.
const READ_BUDGET: Duration = Duration::from_secs(4);

const HELP: &str = concat!(
    "# HELP foundation_scheduled_jobs_readable Whether the operations map could read the scheduled jobs' runs (root ADR-0195).\n",
    "# TYPE foundation_scheduled_jobs_readable gauge\n",
    "# HELP foundation_scheduled_job_problem A problem the operations map shows for an enabled scheduled job; one series per job and kind.\n",
    "# TYPE foundation_scheduled_job_problem gauge\n",
);

/// The series of one read of the operations map. When the runs could not be read, the map does not
/// know the problems, so it reports that and no problem series.
pub(crate) async fn problem_metrics() -> String {
    // The map's read is kept and refreshed in the background, so this is instant after the first
    // read; the first one reads three stores. A scrape must never wait on them: an unanswered
    // /metrics would page as the API being down.
    let status = tokio::time::timeout(READ_BUDGET, super::operations::jobs_status())
        .await
        .ok()
        .and_then(Result::ok);
    let console = std::env::var(CONSOLE_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    render(status.as_ref(), console.as_deref())
}

fn render(status: Option<&ScheduledJobsStatus>, console: Option<&str>) -> String {
    let mut body = String::from(HELP);
    let readable = status.filter(|status| status.run_history_error.is_none());
    let _ = writeln!(
        body,
        "foundation_scheduled_jobs_readable {}",
        u8::from(readable.is_some())
    );
    let Some(status) = readable else {
        return body;
    };
    for job in status.jobs.iter().filter(|job| job.enabled) {
        for problem in &job.problems {
            let node_id = node_of(job, problem).unwrap_or_default();
            let link = match (console, node_id.is_empty()) {
                (Some(base), false) => format!(
                    "{}/#/flow?node={}",
                    base.trim_end_matches('/'),
                    url_component(&node_id)
                ),
                (Some(base), true) => format!(
                    "{}/#/flow?job={}",
                    base.trim_end_matches('/'),
                    url_component(&job.job_id)
                ),
                (None, _) => String::new(),
            };
            let _ = writeln!(
                body,
                "foundation_scheduled_job_problem{{job_id=\"{}\",kind=\"{}\",node_id=\"{}\",summary=\"{}\",link=\"{}\"}} 1",
                escape(&job.job_id),
                kind(problem),
                escape(&node_id),
                escape(&clip(&sentence(problem))),
                escape(&link),
            );
        }
    }
    body
}

/// The dataset a problem opens, as the console's problem list picks it: the table a volume problem
/// names, else the first one the job writes.
fn node_of(job: &ScheduledJobStatus, problem: &ScheduledJobProblem) -> Option<String> {
    match problem {
        ScheduledJobProblem::Volume { node_id, .. } => Some(node_id.clone()),
        _ => job.writes.first().map(|dataset| dataset.node_id.clone()),
    }
}

fn kind(problem: &ScheduledJobProblem) -> &'static str {
    match problem {
        ScheduledJobProblem::Failed { .. } => "failed",
        ScheduledJobProblem::Blocked { .. } => "blocked",
        ScheduledJobProblem::Late { .. } => "late",
        ScheduledJobProblem::Slow { .. } => "slow",
        ScheduledJobProblem::Volume { .. } => "volume",
    }
}

/// One problem as the one line Slack shows, in the console's words (`problemSentence` in
/// `consoles/dawneer/web/src/lib/dataFlow.ts`), minus anything that changes while the problem
/// stays the same: a slow run's seconds so far, and "how long ago".
fn sentence(problem: &ScheduledJobProblem) -> String {
    match problem {
        ScheduledJobProblem::Failed {
            cause, cause_line, ..
        } => match (cause, cause_line) {
            (None, _) => "실패 · 로그를 못 읽어 원인을 몰라요".to_owned(),
            (Some(cause), Some(line)) => format!("{} · {line}", cause_word(*cause)),
            (Some(cause), None) => cause_word(*cause).to_owned(),
        },
        ScheduledJobProblem::Blocked { upstream_job_ids } => format!(
            "앞 단계가 실패해서 멈춰 있어요 ({})",
            upstream_job_ids.join(", ")
        ),
        ScheduledJobProblem::Late {
            due_at,
            last_success_at,
        } => format!(
            "{} 에 돌았어야 하는데 아직 성공 못 했어요 · {}",
            kst(*due_at),
            last_success_at.map_or_else(
                || "성공 기록 없음".to_owned(),
                |at| format!("마지막 성공 {}", kst(at))
            )
        ),
        ScheduledJobProblem::Slow {
            typical_seconds, ..
        } => format!(
            "보통 {}인데 이번 실행은 그보다 훨씬 오래 걸려요",
            duration_words(*typical_seconds)
        ),
        ScheduledJobProblem::Volume {
            rows, typical_rows, ..
        } => format!("보통 {typical_rows}행인데 이번엔 {rows}행"),
    }
}

fn cause_word(cause: FailureCause) -> &'static str {
    match cause {
        FailureCause::OutOfMemory => "메모리 부족",
        FailureCause::TimedOut => "시간 초과",
        FailureCause::SourceMissing => "원천 자료 없음",
        FailureCause::Other => "오류",
    }
}

fn kst(at: DateTime<Utc>) -> String {
    // Korea keeps no daylight saving, so a fixed nine hours is exact.
    format!("{} KST", (at + TimeDelta::hours(9)).format("%m-%d %H:%M"))
}

fn duration_words(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}초"),
        60..3600 => format!("{}분", seconds.div_ceil(60)),
        _ => format!("{}시간 {}분", seconds / 3600, (seconds % 3600) / 60),
    }
}

fn clip(text: &str) -> String {
    let one_line = text.split(['\r', '\n']).next().unwrap_or_default();
    if one_line.chars().count() <= SUMMARY_CHARS {
        return one_line.to_owned();
    }
    let mut clipped: String = one_line.chars().take(SUMMARY_CHARS - 1).collect();
    clipped.push('…');
    clipped
}

/// A query value: node ids are lowercase words and dashes, but nothing here assumes it.
fn url_component(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundation_contracts::data_catalog::{
        OperationsLinks, ScheduledJobDataset, ScheduledJobStart,
    };

    fn at(iso: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(iso)
            .map(|time| time.with_timezone(&Utc))
            .unwrap_or_default()
    }

    fn job(id: &str, enabled: bool, problems: Vec<ScheduledJobProblem>) -> ScheduledJobStatus {
        ScheduledJobStatus {
            job_id: id.to_owned(),
            dag_id: format!("synthetic_{id}"),
            description: String::new(),
            schedule: "0 6 * * *".to_owned(),
            enabled,
            disabled_reason: None,
            next_run_at: None,
            catalog_job_urn: None,
            last_run: None,
            typical_duration_seconds: None,
            typical_duration_runs: 0,
            recent_runs: Vec::new(),
            started_by: ScheduledJobStart::Schedule,
            upstream_job_ids: Vec::new(),
            pipeline_graph_edges: Vec::new(),
            reads: Vec::new(),
            writes: vec![ScheduledJobDataset {
                dataset: format!("iceberg://silver.{id}"),
                catalog_urn: String::new(),
                node_id: format!("node-{id}"),
                title: String::new(),
                kind: "silver_table".to_owned(),
                surface_kind: None,
            }],
            problems,
        }
    }

    fn status(
        jobs: Vec<ScheduledJobStatus>,
        run_history_error: Option<&str>,
    ) -> ScheduledJobsStatus {
        ScheduledJobsStatus {
            read_at: at("2026-10-13T00:00:00Z"),
            jobs,
            run_history_error: run_history_error.map(str::to_owned),
            lakehouse_error: None,
            scheduler_error: None,
            links: OperationsLinks::default(),
        }
    }

    fn problem_lines(body: &str) -> Vec<&str> {
        body.lines()
            .filter(|line| line.starts_with("foundation_scheduled_job_problem{"))
            .collect()
    }

    #[test]
    fn a_failure_is_one_series_with_its_cause_and_a_link_to_its_node() {
        let failed = ScheduledJobProblem::Failed {
            cause: Some(FailureCause::OutOfMemory),
            cause_line: Some("FAILED: synthetic \"gold\" step\nsecond line".to_owned()),
            scheduler_run_id: Some("scheduled__2026".to_owned()),
        };
        let body = render(
            Some(&status(
                vec![job("gold_synthetic", true, vec![failed])],
                None,
            )),
            Some("https://console.example.test/"),
        );
        assert!(body.contains("foundation_scheduled_jobs_readable 1"));
        assert_eq!(
            problem_lines(&body),
            vec![concat!(
                "foundation_scheduled_job_problem{job_id=\"gold_synthetic\",kind=\"failed\",",
                "node_id=\"node-gold_synthetic\",",
                "summary=\"메모리 부족 · FAILED: synthetic \\\"gold\\\" step\",",
                "link=\"https://console.example.test/#/flow?node=node-gold_synthetic\"} 1"
            )]
        );
    }

    #[test]
    fn a_slow_run_keeps_the_same_labels_while_it_keeps_running() {
        let slow = |seconds| ScheduledJobProblem::Slow {
            seconds,
            typical_seconds: 600,
            running: true,
        };
        let first = render(
            Some(&status(vec![job("a", true, vec![slow(1_900)])], None)),
            None,
        );
        let later = render(
            Some(&status(vec![job("a", true, vec![slow(2_700)])], None)),
            None,
        );
        assert_eq!(problem_lines(&first), problem_lines(&later));
        assert_eq!(problem_lines(&first).len(), 1);
    }

    #[test]
    fn unread_runs_say_so_and_claim_no_problem_either_way() {
        let failed = ScheduledJobProblem::Failed {
            cause: None,
            cause_line: None,
            scheduler_run_id: None,
        };
        let body = render(
            Some(&status(
                vec![job("a", true, vec![failed])],
                Some("catalog did not answer"),
            )),
            None,
        );
        assert!(body.contains("foundation_scheduled_jobs_readable 0"));
        assert!(problem_lines(&body).is_empty());
        let none = render(None, None);
        assert!(none.contains("foundation_scheduled_jobs_readable 0"));
    }

    #[test]
    fn a_job_that_is_off_never_alerts_and_volume_opens_its_table() {
        let late = ScheduledJobProblem::Late {
            due_at: at("2026-10-12T22:00:00Z"),
            last_success_at: None,
        };
        let volume = ScheduledJobProblem::Volume {
            node_id: "silver-other".to_owned(),
            rows: 10,
            typical_rows: 1_000,
        };
        let body = render(
            Some(&status(
                vec![
                    job("off", false, vec![late.clone()]),
                    job("on", true, vec![late, volume]),
                ],
                None,
            )),
            Some("https://console.example.test"),
        );
        let lines = problem_lines(&body);
        assert_eq!(lines.len(), 2, "{body}");
        assert!(lines[0].contains(
            "summary=\"10-13 07:00 KST 에 돌았어야 하는데 아직 성공 못 했어요 · 성공 기록 없음\""
        ));
        assert!(lines[1].contains("node_id=\"silver-other\""));
        assert!(lines[1].contains("link=\"https://console.example.test/#/flow?node=silver-other\""));
    }

    #[test]
    fn a_long_cause_is_cut_to_one_short_line() {
        let long = "x".repeat(SUMMARY_CHARS * 2);
        assert_eq!(clip(&long).chars().count(), SUMMARY_CHARS);
        assert_eq!(url_component("a b/c"), "a%20b%2Fc");
    }
}
