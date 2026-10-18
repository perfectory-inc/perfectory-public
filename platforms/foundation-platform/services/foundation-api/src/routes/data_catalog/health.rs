//! Whether each scheduled job is fine, and what to look at when it is not (root ADR-0195).
//!
//! Five signals, each read from a fact Foundation already holds, never from a second list: a
//! failed latest run (and the cause its scheduler log gives), a job whose inputs will not come
//! because a job it waits on failed (`upstream_job_ids`, rendered by `job_specs.py`), a scheduled
//! start whose time limit has passed with no success since and no run of it queued or running in
//! the scheduler (the job list's `schedule`, and `time_limit_minutes` + `stop_grace_minutes`; a run waiting for its pool's
//! slots is not late, `job_specs.py` bounds that wait), a run much longer than the job's median,
//! and a run that wrote far fewer or far more rows than the runs before it of the same kind (the
//! `added-records` of the snapshots it committed to the Iceberg tables it writes; a run that
//! rewrote a whole table is compared only with runs that did too, an incremental one only with
//! incremental ones). A new job in the job list gets all five without a line here.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use croner::Cron;
use foundation_contracts::data_catalog::{
    FailureCause, ScheduledJobProblem, ScheduledJobRun, ScheduledJobRunOutcome, ScheduledJobStatus,
};
use lakehouse_infrastructure::IcebergSnapshotSummary;

/// A run is slow past half again its median duration and ten minutes more: the margin keeps a
/// five-minute job that took eight from paging anyone.
const SLOW_PERCENT: u64 = 150;
const SLOW_MARGIN_SECONDS: u64 = 600;
/// The median is a fact only over this many succeeded runs.
const SLOW_MIN_RUNS: u32 = 3;
/// A run's rows are unusual at half or twice the median of the runs before it that wrote rows, and
/// only over this many of them.
const VOLUME_FACTOR: u64 = 2;
const VOLUME_MIN_RUNS: usize = 3;

/// The Iceberg table of a dataset the scheduler names `iceberg://<namespace>.<table>`.
pub(super) fn iceberg_table(dataset: &str) -> Option<&str> {
    dataset.strip_prefix("iceberg://")
}

/// What the scheduler log of a run that did not succeed says (it failed, or it ended waiting), read
/// for one data catalog run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FailedRunLog {
    /// The data catalog run it was read for.
    pub run_urn: String,
    pub scheduler_run_id: Option<String>,
    pub cause: FailureCause,
    pub cause_line: Option<String>,
    /// What the run said it waits for: its last `foundation-job-outcome waiting` line (root ADR-0260).
    pub waiting: Option<String>,
}

/// The facts the signals read besides the job statuses.
pub(super) struct HealthFacts<'a> {
    /// Each job's time limit in minutes, by job id.
    pub timeouts: &'a BTreeMap<String, u32>,
    /// When each job was switched on, by job id (root ADR-0239).
    pub enabled_since: &'a BTreeMap<String, DateTime<Utc>>,
    /// Every table's `main` snapshots that could be read, newest first.
    pub tables: &'a BTreeMap<String, Vec<IcebergSnapshotSummary>>,
    /// The failed latest runs' logs, by DAG id.
    pub failures: &'a BTreeMap<String, FailedRunLog>,
    /// By DAG id, when the newest run the scheduler holds queued or running was due
    /// (`run_after`); a job with one is not late.
    pub in_progress: &'a BTreeMap<String, DateTime<Utc>>,
    /// False when the data catalog did not answer: then no run is known and nothing is late.
    pub runs_readable: bool,
    pub now: DateTime<Utc>,
}

/// Fills each run's `rows_written` and each job's `problems`, worst first. A job that failed or
/// is blocked is not also called late: its lateness is that problem, counted once.
pub(super) fn annotate(statuses: &mut [ScheduledJobStatus], facts: &HealthFacts<'_>) {
    let blocked = blocked(statuses);
    for status in statuses.iter_mut() {
        let tables = written_tables(status, facts.tables);
        if let Some(tables) = &tables {
            for run in &mut status.recent_runs {
                run.rows_written = rows_of(run, tables.values().copied());
            }
            if let (Some(last), Some(first)) =
                (status.last_run.as_mut(), status.recent_runs.first())
            {
                if last.run_urn == first.run_urn {
                    last.rows_written = first.rows_written;
                }
            }
        }
        let mut problems = Vec::new();
        problems.extend(failed(status, facts.failures));
        // A wait is shown with what it waits for, and is not also late: the run came and said so.
        problems.extend(waiting(status, facts.failures));
        if let Some(upstream_job_ids) = blocked.get(&status.job_id) {
            problems.push(ScheduledJobProblem::Blocked {
                upstream_job_ids: upstream_job_ids.clone(),
            });
        }
        if problems.is_empty() && facts.runs_readable {
            let timeout = facts.timeouts.get(&status.job_id).copied().unwrap_or(0);
            if !facts.in_progress.contains_key(&status.dag_id) {
                let since = facts.enabled_since.get(&status.job_id).copied();
                problems.extend(late(status, timeout, since, facts.now));
            }
        }
        problems.extend(slow(status, facts.now));
        if let Some(tables) = &tables {
            problems.extend(volume(status, tables));
        }
        status.problems = problems;
    }
}

fn is_failed(status: &ScheduledJobStatus) -> bool {
    status.last_run.as_ref().is_some_and(|run| {
        matches!(
            run.outcome,
            ScheduledJobRunOutcome::Failed | ScheduledJobRunOutcome::UpForRetry
        )
    })
}

/// The Iceberg tables a job writes with their snapshots, by node id; `None` when it writes none
/// or one of them was not read, so no run's rows are claimed from a partial read.
fn written_tables<'a>(
    status: &ScheduledJobStatus,
    tables: &'a BTreeMap<String, Vec<IcebergSnapshotSummary>>,
) -> Option<BTreeMap<String, &'a [IcebergSnapshotSummary]>> {
    let mut written = BTreeMap::new();
    for dataset in &status.writes {
        let Some(table) = iceberg_table(&dataset.dataset) else {
            continue;
        };
        written.insert(dataset.node_id.clone(), tables.get(table)?.as_slice());
    }
    (!written.is_empty()).then_some(written)
}

/// The snapshots committed to `tables` between a finished run's start and its finish.
fn committed_by<'a>(
    run: &ScheduledJobRun,
    tables: impl Iterator<Item = &'a [IcebergSnapshotSummary]>,
) -> Option<Vec<&'a IcebergSnapshotSummary>> {
    let start = run.started_at?.timestamp_millis();
    let finish = run.finished_at?.timestamp_millis();
    Some(
        tables
            .flatten()
            .filter(|snapshot| {
                snapshot
                    .timestamp_ms
                    .is_some_and(|at| start <= at && at <= finish)
            })
            .collect(),
    )
}

/// The rows a finished run added to `tables`: the `added-records` of the snapshots it committed.
fn rows_of<'a>(
    run: &ScheduledJobRun,
    tables: impl Iterator<Item = &'a [IcebergSnapshotSummary]>,
) -> Option<u64> {
    Some(
        committed_by(run, tables)?
            .into_iter()
            .filter_map(IcebergSnapshotSummary::added_records)
            .sum(),
    )
}

/// Whether the commit left no row it did not add: a whole-table rewrite (an overwrite of every
/// partition, a replace, a first load). Iceberg calls a merge of a few partitions an `overwrite`
/// too, so the `operation` cannot tell them apart; the counts can.
fn rewrote_table(snapshot: &IcebergSnapshotSummary) -> bool {
    let total = snapshot
        .summary
        .get("total-records")
        .and_then(|total| total.parse::<u64>().ok());
    matches!((snapshot.added_records(), total), (Some(added), Some(total)) if added >= total)
}

fn failed(
    status: &ScheduledJobStatus,
    failures: &BTreeMap<String, FailedRunLog>,
) -> Option<ScheduledJobProblem> {
    if !is_failed(status) {
        return None;
    }
    let run_urn = status.last_run.as_ref().map(|run| run.run_urn.as_str());
    let log = failures
        .get(&status.dag_id)
        .filter(|log| Some(log.run_urn.as_str()) == run_urn);
    Some(ScheduledJobProblem::Failed {
        cause: log.map(|log| log.cause),
        cause_line: log.and_then(|log| log.cause_line.clone()),
        scheduler_run_id: log.and_then(|log| log.scheduler_run_id.clone()),
    })
}

/// Waiting: the latest run ended skipped, which the DAG does to a run that said it waits (root
/// ADR-0260, `orchestration/dags/foundation_jobs.py`).
fn waiting(
    status: &ScheduledJobStatus,
    logs: &BTreeMap<String, FailedRunLog>,
) -> Option<ScheduledJobProblem> {
    let run = status
        .last_run
        .as_ref()
        .filter(|run| run.outcome == ScheduledJobRunOutcome::Skipped)?;
    let log = logs
        .get(&status.dag_id)
        .filter(|log| log.run_urn == run.run_urn);
    Some(ScheduledJobProblem::Waiting {
        reason: log.and_then(|log| log.waiting.clone()),
        scheduler_run_id: log.and_then(|log| log.scheduler_run_id.clone()),
    })
}

/// Late: the latest scheduled start whose time limit has passed, with no success finished since.
/// A start before the job was switched on (`enabled_since`, root ADR-0239) is not one it missed:
/// the five Silver lanes switched on 2026-10-14 were called late for the day before.
pub(super) fn late(
    status: &ScheduledJobStatus,
    timeout_minutes: u32,
    enabled_since: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<ScheduledJobProblem> {
    if !status.enabled {
        return None;
    }
    let cron = Cron::from_str(&status.schedule).ok()?;
    let limit = now - Duration::minutes(i64::from(timeout_minutes));
    let due_at = cron.find_previous_occurrence(&limit, true).ok()?;
    if enabled_since.is_some_and(|since| due_at < since) {
        return None;
    }
    let last_success_at = status
        .recent_runs
        .iter()
        .filter(|run| run.outcome == ScheduledJobRunOutcome::Succeeded)
        .filter_map(|run| run.finished_at)
        .max();
    (last_success_at.is_none_or(|at| at < due_at)).then_some(ScheduledJobProblem::Late {
        due_at,
        last_success_at,
    })
}

fn slow(status: &ScheduledJobStatus, now: DateTime<Utc>) -> Option<ScheduledJobProblem> {
    if status.typical_duration_runs < SLOW_MIN_RUNS || is_failed(status) {
        return None;
    }
    let typical = status.typical_duration_seconds?;
    let run = status.last_run.as_ref()?;
    let running = run.outcome == ScheduledJobRunOutcome::Running;
    let end = if running { now } else { run.finished_at? };
    let seconds = u64::try_from(end.signed_duration_since(run.started_at?).num_seconds()).ok()?;
    let over = seconds * 100 > typical * SLOW_PERCENT && seconds >= typical + SLOW_MARGIN_SECONDS;
    over.then_some(ScheduledJobProblem::Slow {
        seconds,
        typical_seconds: typical,
        running,
    })
}

/// Per table, the latest succeeded run that wrote rows against the median of the earlier ones of
/// its kind: a full rewrite writes every row and an incremental run only the changed ones, so a
/// rewrite after incremental runs (or the reverse) is no change in volume.
fn volume(
    status: &ScheduledJobStatus,
    tables: &BTreeMap<String, &[IcebergSnapshotSummary]>,
) -> Vec<ScheduledJobProblem> {
    let finished: Vec<&ScheduledJobRun> = status
        .recent_runs
        .iter()
        .filter(|run| run.outcome == ScheduledJobRunOutcome::Succeeded)
        .collect();
    tables
        .iter()
        .filter_map(|(node_id, snapshots)| {
            // A run that changed nothing wrote no rows; it is not a small batch.
            let rows: Vec<(u64, bool)> = finished
                .iter()
                .filter_map(|run| {
                    let committed = committed_by(run, std::iter::once(*snapshots))?;
                    let rows: u64 = committed
                        .iter()
                        .filter_map(|snapshot| snapshot.added_records())
                        .sum();
                    let rewrote = committed.iter().any(|snapshot| rewrote_table(snapshot));
                    Some((rows, rewrote))
                })
                .filter(|(rows, _)| *rows > 0)
                .collect();
            let ((latest, rewrote), earlier) = rows.split_first()?;
            let earlier: Vec<u64> = earlier
                .iter()
                .filter(|(_, earlier_rewrote)| earlier_rewrote == rewrote)
                .map(|(rows, _)| *rows)
                .collect();
            if earlier.len() < VOLUME_MIN_RUNS {
                return None;
            }
            let typical = median(&earlier);
            let unusual = latest * VOLUME_FACTOR <= typical || *latest >= typical * VOLUME_FACTOR;
            unusual.then(|| ScheduledJobProblem::Volume {
                node_id: node_id.clone(),
                rows: *latest,
                typical_rows: typical,
            })
        })
        .collect()
}

fn median(values: &[u64]) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match (sorted.len() % 2, sorted.get(middle)) {
        (1, Some(value)) => *value,
        (_, Some(upper)) => {
            let lower = sorted.get(middle - 1).copied().unwrap_or(*upper);
            lower + (upper - lower) / 2
        }
        _ => 0,
    }
}

/// Every job a failed or blocked job it waits on keeps from its inputs, with those jobs. The
/// graph has no cycles (`job_specs.py` refuses one), so walking it in rounds ends.
fn blocked(statuses: &[ScheduledJobStatus]) -> BTreeMap<String, Vec<String>> {
    let mut stopped: BTreeSet<&str> = statuses
        .iter()
        .filter(|status| status.enabled && is_failed(status))
        .map(|status| status.job_id.as_str())
        .collect();
    let mut blocked = BTreeMap::new();
    loop {
        let mut grew = false;
        for status in statuses.iter().filter(|status| status.enabled) {
            if blocked.contains_key(&status.job_id) {
                continue;
            }
            let by: Vec<String> = status
                .upstream_job_ids
                .iter()
                .filter(|upstream| stopped.contains(upstream.as_str()))
                .cloned()
                .collect();
            if !by.is_empty() {
                stopped.insert(status.job_id.as_str());
                blocked.insert(status.job_id.clone(), by);
                grew = true;
            }
        }
        if !grew {
            return blocked;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundation_contracts::data_catalog::{ScheduledJobDataset, ScheduledJobStart};

    fn at(iso: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(iso)
            .map(|time| time.with_timezone(&Utc))
            .unwrap_or_default()
    }

    fn ran(
        urn: &str,
        start: &str,
        minutes: i64,
        outcome: ScheduledJobRunOutcome,
    ) -> ScheduledJobRun {
        let started = at(start);
        ScheduledJobRun {
            run_urn: urn.to_owned(),
            queued_at: None,
            started_at: Some(started),
            finished_at: (outcome != ScheduledJobRunOutcome::Running)
                .then(|| started + Duration::minutes(minutes)),
            outcome,
            native_result: None,
            rows_written: None,
        }
    }

    fn job(id: &str, upstream: &[&str], runs: Vec<ScheduledJobRun>) -> ScheduledJobStatus {
        ScheduledJobStatus {
            job_id: id.to_owned(),
            dag_id: format!("synthetic_{id}"),
            description: String::new(),
            schedule: "0 6 * * *".to_owned(),
            enabled: true,
            disabled_reason: None,
            next_run_at: None,
            catalog_job_urn: None,
            last_run: runs.first().cloned(),
            typical_duration_seconds: None,
            typical_duration_runs: 0,
            recent_runs: runs,
            started_by: ScheduledJobStart::Schedule,
            upstream_job_ids: upstream.iter().map(|id| (*id).to_owned()).collect(),
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
            problems: Vec::new(),
        }
    }

    fn snapshot(iso: &str, added: u64) -> IcebergSnapshotSummary {
        IcebergSnapshotSummary {
            snapshot_id: at(iso).timestamp(),
            timestamp_ms: Some(at(iso).timestamp_millis()),
            summary: [("added-records".to_owned(), added.to_string())].into(),
        }
    }

    /// A commit as Iceberg summarises it: the rows it added and the rows the table holds after it.
    fn commit(iso: &str, added: Option<u64>, total: u64) -> IcebergSnapshotSummary {
        let mut summary = BTreeMap::from([("total-records".to_owned(), total.to_string())]);
        if let Some(added) = added {
            summary.insert("added-records".to_owned(), added.to_string());
        }
        IcebergSnapshotSummary {
            snapshot_id: at(iso).timestamp(),
            timestamp_ms: Some(at(iso).timestamp_millis()),
            summary,
        }
    }

    fn kinds(status: &ScheduledJobStatus) -> Vec<&'static str> {
        status
            .problems
            .iter()
            .map(|problem| match problem {
                ScheduledJobProblem::Failed { .. } => "failed",
                ScheduledJobProblem::Blocked { .. } => "blocked",
                ScheduledJobProblem::Late { .. } => "late",
                ScheduledJobProblem::Slow { .. } => "slow",
                ScheduledJobProblem::Volume { .. } => "volume",
                ScheduledJobProblem::Waiting { .. } => "waiting",
            })
            .collect()
    }

    fn annotate_at(
        statuses: &mut [ScheduledJobStatus],
        tables: &BTreeMap<String, Vec<IcebergSnapshotSummary>>,
        failures: &BTreeMap<String, FailedRunLog>,
        now: &str,
    ) {
        let timeouts = statuses
            .iter()
            .map(|status| (status.job_id.clone(), 60))
            .collect();
        annotate(
            statuses,
            &HealthFacts {
                timeouts: &timeouts,
                enabled_since: &BTreeMap::new(),
                tables,
                failures,
                in_progress: &BTreeMap::new(),
                runs_readable: true,
                now: at(now),
            },
        );
    }

    #[test]
    fn a_failed_job_carries_its_logs_cause_and_blocks_what_waits_on_it_downstream() {
        use ScheduledJobRunOutcome::{Failed, Succeeded};
        let mut statuses = vec![
            job(
                "source",
                &[],
                vec![ran("run-s", "2026-01-02T06:00:00Z", 5, Failed)],
            ),
            job(
                "panel",
                &["source"],
                vec![ran("run-p", "2026-01-02T06:00:00Z", 5, Succeeded)],
            ),
            job(
                "bake",
                &["panel"],
                vec![ran("run-b", "2026-01-02T06:00:00Z", 5, Succeeded)],
            ),
            job(
                "other",
                &[],
                vec![ran("run-o", "2026-01-02T06:00:00Z", 5, Succeeded)],
            ),
        ];
        let failures = BTreeMap::from([(
            "synthetic_source".to_owned(),
            FailedRunLog {
                run_urn: "run-s".to_owned(),
                scheduler_run_id: Some("scheduled__x".to_owned()),
                cause: FailureCause::OutOfMemory,
                cause_line: Some("result=oom-kill".to_owned()),
                waiting: None,
            },
        )]);
        annotate_at(
            &mut statuses,
            &BTreeMap::new(),
            &failures,
            "2026-01-02T08:00:00Z",
        );
        assert_eq!(
            kinds(&statuses[0]),
            vec!["failed"],
            "failed, and not late as well"
        );
        assert_eq!(
            statuses[0].problems[0],
            ScheduledJobProblem::Failed {
                cause: Some(FailureCause::OutOfMemory),
                cause_line: Some("result=oom-kill".to_owned()),
                scheduler_run_id: Some("scheduled__x".to_owned()),
            }
        );
        assert_eq!(kinds(&statuses[1]), vec!["blocked"]);
        assert_eq!(
            statuses[2].problems,
            vec![ScheduledJobProblem::Blocked {
                upstream_job_ids: vec!["panel".to_owned()]
            }],
            "blocked passes down the chain"
        );
        assert!(
            statuses[3].problems.is_empty(),
            "{:?}",
            statuses[3].problems
        );
    }

    #[test]
    fn a_run_that_ended_waiting_shows_what_it_waits_for_and_is_neither_late_nor_blocking() {
        // Root ADR-0260: a fold due while a release built was refused, failed and alerted; it now
        // ends waiting, the DAG skips it, and the map says why.
        use ScheduledJobRunOutcome::{Skipped, Succeeded};
        let mut statuses = vec![
            job(
                "fold",
                &[],
                vec![ran("run-f", "2026-01-02T06:00:00Z", 1, Skipped)],
            ),
            job(
                "bake",
                &["fold"],
                vec![ran("run-b", "2026-01-02T06:00:00Z", 5, Succeeded)],
            ),
        ];
        let logs = BTreeMap::from([(
            "synthetic_fold".to_owned(),
            FailedRunLog {
                run_urn: "run-f".to_owned(),
                scheduler_run_id: Some("scheduled__f".to_owned()),
                cause: FailureCause::Other,
                cause_line: None,
                waiting: Some("a release build is running".to_owned()),
            },
        )]);
        // Well past the slot and its time limit, with no success since: a failure would be late.
        annotate_at(
            &mut statuses,
            &BTreeMap::new(),
            &logs,
            "2026-01-02T09:00:00Z",
        );
        assert_eq!(
            statuses[0].problems,
            vec![ScheduledJobProblem::Waiting {
                reason: Some("a release build is running".to_owned()),
                scheduler_run_id: Some("scheduled__f".to_owned()),
            }]
        );
        assert!(
            statuses[1].problems.is_empty(),
            "{:?}",
            statuses[1].problems
        );
        // A log not yet read still shows the wait, without its reason.
        annotate_at(
            &mut statuses,
            &BTreeMap::new(),
            &BTreeMap::new(),
            "2026-01-02T09:00:00Z",
        );
        assert_eq!(kinds(&statuses[0]), vec!["waiting"]);
    }

    #[test]
    fn blocked_passes_down_a_chain_listed_backwards() {
        use ScheduledJobRunOutcome::{Failed, Succeeded};
        let ok = |urn: &str| vec![ran(urn, "2026-01-02T06:00:00Z", 5, Succeeded)];
        let mut statuses = vec![
            job("bake", &["panel"], ok("run-b")),
            job("panel", &["source"], ok("run-p")),
            job(
                "source",
                &[],
                vec![ran("run-s", "2026-01-02T06:00:00Z", 5, Failed)],
            ),
        ];
        annotate_at(
            &mut statuses,
            &BTreeMap::new(),
            &BTreeMap::new(),
            "2026-01-02T06:30:00Z",
        );
        assert_eq!(kinds(&statuses[0]), vec!["blocked"]);
        assert_eq!(kinds(&statuses[1]), vec!["blocked"]);
    }

    #[test]
    fn a_log_read_for_an_older_run_is_not_this_failures_cause() {
        let mut statuses = vec![job(
            "source",
            &[],
            vec![ran(
                "run-new",
                "2026-01-02T06:00:00Z",
                5,
                ScheduledJobRunOutcome::Failed,
            )],
        )];
        let failures = BTreeMap::from([(
            "synthetic_source".to_owned(),
            FailedRunLog {
                run_urn: "run-old".to_owned(),
                scheduler_run_id: None,
                cause: FailureCause::TimedOut,
                cause_line: None,
                waiting: None,
            },
        )]);
        annotate_at(
            &mut statuses,
            &BTreeMap::new(),
            &failures,
            "2026-01-02T06:30:00Z",
        );
        assert_eq!(
            statuses[0].problems[0],
            ScheduledJobProblem::Failed {
                cause: None,
                cause_line: None,
                scheduler_run_id: None
            }
        );
    }

    #[test]
    fn late_is_a_scheduled_start_past_its_time_limit_with_no_success_since() {
        let ok = |start: &str| ran("run", start, 5, ScheduledJobRunOutcome::Succeeded);
        let late_kind = |runs: Vec<ScheduledJobRun>, now: &str| {
            let mut statuses = vec![job("daily", &[], runs)];
            annotate_at(&mut statuses, &BTreeMap::new(), &BTreeMap::new(), now);
            kinds(&statuses[0])
        };
        // Due at 06:00 with an hour's limit: not late before 07:00.
        assert!(late_kind(vec![ok("2026-01-01T06:00:00Z")], "2026-01-02T06:59:00Z").is_empty());
        assert_eq!(
            late_kind(vec![ok("2026-01-01T06:00:00Z")], "2026-01-02T07:01:00Z"),
            vec!["late"]
        );
        assert!(late_kind(vec![ok("2026-01-02T06:00:00Z")], "2026-01-02T07:01:00Z").is_empty());
        assert_eq!(
            late_kind(Vec::new(), "2026-01-02T07:01:00Z"),
            vec!["late"],
            "never succeeded"
        );

        let mut off = vec![job("daily", &[], Vec::new())];
        off[0].enabled = false;
        annotate_at(
            &mut off,
            &BTreeMap::new(),
            &BTreeMap::new(),
            "2026-01-02T07:01:00Z",
        );
        assert!(
            off[0].problems.is_empty(),
            "a job that is off is never late"
        );
    }

    #[test]
    fn slow_is_half_again_the_median_and_ten_minutes_more() {
        use ScheduledJobRunOutcome::{Running, Succeeded};
        let check = |runs: Vec<ScheduledJobRun>, now: &str| {
            // Yesterday's run succeeded, so none of these is late as well.
            let mut runs = runs;
            runs.push(ran(
                "yesterday",
                "2026-01-01T06:00:00Z",
                30,
                ScheduledJobRunOutcome::Succeeded,
            ));
            let mut statuses = vec![job("daily", &[], runs)];
            statuses[0].typical_duration_seconds = Some(1800);
            statuses[0].typical_duration_runs = 5;
            annotate_at(&mut statuses, &BTreeMap::new(), &BTreeMap::new(), now);
            statuses[0].problems.clone()
        };
        assert!(check(
            vec![ran("r", "2026-01-02T06:00:00Z", 44, Succeeded)],
            "2026-01-02T07:00:00Z"
        )
        .is_empty());
        assert_eq!(
            check(
                vec![ran("r", "2026-01-02T06:00:00Z", 46, Succeeded)],
                "2026-01-02T07:00:00Z"
            ),
            vec![ScheduledJobProblem::Slow {
                seconds: 2760,
                typical_seconds: 1800,
                running: false
            }]
        );
        assert_eq!(
            check(
                vec![ran("r", "2026-01-02T06:00:00Z", 0, Running)],
                "2026-01-02T06:50:00Z"
            ),
            vec![ScheduledJobProblem::Slow {
                seconds: 3000,
                typical_seconds: 1800,
                running: true
            }],
            "a run still going counts from its start to now"
        );
    }

    #[test]
    fn volume_compares_the_latest_rows_with_the_median_of_the_runs_before_it() {
        use ScheduledJobRunOutcome::Succeeded;
        let runs: Vec<ScheduledJobRun> = (0..5)
            .rev()
            .map(|day| {
                ran(
                    &format!("run-{day}"),
                    &format!("2026-01-0{}T06:00:00Z", day + 1),
                    30,
                    Succeeded,
                )
            })
            .collect();
        let tables = |latest: u64| {
            BTreeMap::from([(
                "silver.daily".to_owned(),
                vec![
                    snapshot("2026-01-05T06:10:00Z", latest),
                    snapshot("2026-01-04T06:10:00Z", 1000),
                    snapshot("2026-01-03T06:10:00Z", 900),
                    snapshot("2026-01-02T06:10:00Z", 1100),
                    // Outside every run's window: no run wrote it.
                    snapshot("2026-01-01T08:00:00Z", 5),
                ],
            )])
        };
        let check = |latest: u64| {
            let mut statuses = vec![job("daily", &[], runs.clone())];
            statuses[0].schedule = "0 6 * * *".to_owned();
            annotate_at(
                &mut statuses,
                &tables(latest),
                &BTreeMap::new(),
                "2026-01-05T07:00:00Z",
            );
            statuses.remove(0)
        };
        let usual = check(980);
        assert!(usual.problems.is_empty(), "{:?}", usual.problems);
        assert_eq!(usual.recent_runs[0].rows_written, Some(980));
        assert_eq!(
            usual.last_run.as_ref().and_then(|run| run.rows_written),
            Some(980)
        );
        assert_eq!(
            usual.recent_runs[4].rows_written,
            Some(0),
            "the snapshot after the run is not its rows"
        );
        assert_eq!(
            check(500).problems,
            vec![ScheduledJobProblem::Volume {
                node_id: "node-daily".to_owned(),
                rows: 500,
                typical_rows: 1000
            }],
            "half the usual batch"
        );
        assert_eq!(kinds(&check(2000)), vec!["volume"], "twice the usual batch");
    }

    #[test]
    fn a_run_queued_or_running_in_the_scheduler_is_not_late() {
        // Due at 06:00 with an hour's limit, no success since, and at 13:00 the run is still
        // waiting for its pool's slots: a fold may wait that long (`job_specs.py`).
        let mut statuses = vec![job(
            "fold",
            &[],
            vec![ran(
                "yesterday",
                "2026-01-01T06:00:00Z",
                5,
                ScheduledJobRunOutcome::Succeeded,
            )],
        )];
        let timeouts = BTreeMap::from([("fold".to_owned(), 60)]);
        let judge = |statuses: &mut [ScheduledJobStatus],
                     in_progress: &BTreeMap<String, DateTime<Utc>>| {
            annotate(
                statuses,
                &HealthFacts {
                    timeouts: &timeouts,
                    enabled_since: &BTreeMap::new(),
                    tables: &BTreeMap::new(),
                    failures: &BTreeMap::new(),
                    in_progress,
                    runs_readable: true,
                    now: at("2026-01-02T13:00:00Z"),
                },
            );
        };
        judge(&mut statuses, &BTreeMap::new());
        assert_eq!(kinds(&statuses[0]), vec!["late"], "no run in the scheduler");
        let queued = BTreeMap::from([("synthetic_fold".to_owned(), at("2026-01-02T06:00:00Z"))]);
        judge(&mut statuses, &queued);
        assert!(
            statuses[0].problems.is_empty(),
            "{:?}",
            statuses[0].problems
        );
    }

    /// The commits of a real Gold panel (2026-10-07 to 10-12): four whole rewrites of 5,945,767
    /// rows, an empty append between them, then an incremental merge of 47,153 rows. Iceberg calls
    /// the rewrites and the merge `overwrite` alike.
    fn panel_commits() -> Vec<IcebergSnapshotSummary> {
        vec![
            commit("2026-10-12T08:38:22Z", Some(47_153), 5_945_767),
            commit("2026-10-11T10:37:07Z", Some(5_945_767), 5_945_767),
            commit("2026-10-10T21:41:16Z", None, 5_945_767),
            commit("2026-10-10T14:59:28Z", Some(5_945_767), 5_945_767),
            commit("2026-10-09T10:41:22Z", Some(5_945_767), 5_945_767),
            commit("2026-10-07T15:56:57Z", Some(5_945_767), 5_945_767),
        ]
    }

    /// One succeeded run around each commit, newest first, judged the day after the last.
    fn volume_of(commits: Vec<IcebergSnapshotSummary>) -> Vec<ScheduledJobProblem> {
        let runs = commits
            .iter()
            .enumerate()
            .filter_map(|(index, commit)| {
                let committed = DateTime::from_timestamp_millis(commit.timestamp_ms?)?;
                Some(ran(
                    &format!("run-{index}"),
                    &(committed - Duration::minutes(10)).to_rfc3339(),
                    20,
                    ScheduledJobRunOutcome::Succeeded,
                ))
            })
            .collect();
        let mut statuses = vec![job("panel", &[], runs)];
        statuses[0].enabled = false;
        let tables = BTreeMap::from([("silver.panel".to_owned(), commits)]);
        annotate_at(
            &mut statuses,
            &tables,
            &BTreeMap::new(),
            "2026-10-13T00:00:00Z",
        );
        statuses.remove(0).problems
    }

    #[test]
    fn an_incremental_run_after_full_rewrites_is_no_change_in_volume_nor_the_reverse() {
        assert!(volume_of(panel_commits()).is_empty());

        // A rewrite after three incremental merges.
        let mut commits = vec![
            commit("2026-10-14T08:00:00Z", Some(5_945_767), 5_945_767),
            commit("2026-10-13T08:00:00Z", Some(40_000), 5_945_767),
        ];
        commits.extend(panel_commits());
        assert!(volume_of(commits).is_empty());
    }

    #[test]
    fn runs_of_the_same_kind_are_still_compared() {
        // Three incremental merges near 47 thousand rows, then one of 900 thousand.
        let mut commits = vec![
            commit("2026-10-15T08:00:00Z", Some(900_000), 5_945_767),
            commit("2026-10-14T08:00:00Z", Some(52_000), 5_945_767),
            commit("2026-10-13T08:00:00Z", Some(40_000), 5_945_767),
        ];
        commits.extend(panel_commits());
        assert_eq!(
            volume_of(commits),
            vec![ScheduledJobProblem::Volume {
                node_id: "node-panel".to_owned(),
                rows: 900_000,
                typical_rows: 47_153
            }]
        );

        // A rewrite that lost half the table, against the rewrites before it.
        let mut commits = vec![commit("2026-10-13T08:00:00Z", Some(2_900_000), 2_900_000)];
        commits.extend(panel_commits());
        assert_eq!(
            volume_of(commits),
            vec![ScheduledJobProblem::Volume {
                node_id: "node-panel".to_owned(),
                rows: 2_900_000,
                typical_rows: 5_945_767
            }]
        );
    }

    #[test]
    fn a_job_is_not_late_for_a_slot_from_before_it_was_switched_on() {
        // 2026-10-14: five Silver lanes switched on at 02:51 were called late for the 07:00 slot
        // of the day before, which no run could have taken (root ADR-0239).
        let late_since = |since: &str, now: &str| {
            let mut statuses = vec![job("daily", &[], Vec::new())];
            let timeouts = BTreeMap::from([("daily".to_owned(), 60)]);
            let enabled_since = BTreeMap::from([("daily".to_owned(), at(since))]);
            annotate(
                &mut statuses,
                &HealthFacts {
                    timeouts: &timeouts,
                    enabled_since: &enabled_since,
                    tables: &BTreeMap::new(),
                    failures: &BTreeMap::new(),
                    in_progress: &BTreeMap::new(),
                    runs_readable: true,
                    now: at(now),
                },
            );
            kinds(&statuses[0])
        };
        // Due 06:00 yesterday and today; switched on in between: today's 06:00 is the first it owes.
        assert!(late_since("2026-01-01T09:00:00Z", "2026-01-02T05:00:00Z").is_empty());
        assert_eq!(
            late_since("2026-01-01T09:00:00Z", "2026-01-02T07:01:00Z"),
            vec!["late"],
            "the first slot after it was switched on is owed"
        );
        assert_eq!(
            late_since("2026-01-01T05:00:00Z", "2026-01-02T05:00:00Z"),
            vec!["late"],
            "switched on before yesterday's slot: that slot was owed"
        );
    }

    #[test]
    fn nothing_is_claimed_late_when_the_runs_were_not_read() {
        let mut statuses = vec![job("daily", &[], Vec::new())];
        let timeouts = BTreeMap::from([("daily".to_owned(), 60)]);
        annotate(
            &mut statuses,
            &HealthFacts {
                timeouts: &timeouts,
                enabled_since: &BTreeMap::new(),
                tables: &BTreeMap::new(),
                failures: &BTreeMap::new(),
                in_progress: &BTreeMap::new(),
                runs_readable: false,
                now: at("2026-01-02T08:00:00Z"),
            },
        );
        assert!(statuses[0].problems.is_empty());
    }
}
