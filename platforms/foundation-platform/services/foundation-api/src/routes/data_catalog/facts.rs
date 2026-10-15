//! The run facts the operations map reads, kept for a short while (root ADR-0195).
//!
//! Three stores answer: the data catalog holds the runs (`OpenLineage`, root ADR-0165), the
//! lakehouse catalog holds each table's snapshots with the rows each added, and the scheduler
//! (Airflow) holds each run's log, the only place a failure says why. Asking all three for every
//! screen refresh would make each answer as slow as the slowest of them, so one read is kept and
//! served for `FRESH_FOR`; a request after that gets the kept read at once and starts the next in
//! the background (stale-while-revalidate). A table is read again only when a job that writes it
//! finished since it was read, or after `TABLE_FRESH_FOR`; a failed run's log is read once, and
//! one the scheduler did not give is asked for again only after `SCHEDULER_RETRY_AFTER`. The
//! scheduler is asked which runs it holds queued or running only while some job is past its
//! scheduled start's time limit, with one sign-in per read.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};

use chrono::{DateTime, Duration, Utc};
use foundation_contracts::data_catalog::{
    FailureCause, ScheduledJobRunOutcome, ScheduledJobStatus,
};
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotSummary, LakehouseCatalogConfig,
};
use regex::Regex;
use serde_json::{json, Value as JsonValue};
use tokio::sync::RwLock;

use super::health::{iceberg_table, late, FailedRunLog};
use super::operations::{
    catalog_flows, job_statuses, unread_runs_reason, JobDependencies, JobList,
};

/// How long one read of the three stores is served before the next is started.
const FRESH_FOR: Duration = Duration::seconds(30);
/// How long a table's snapshots are kept when no job that writes it has finished since.
const TABLE_FRESH_FOR: Duration = Duration::minutes(30);
/// The scheduler's address inside the runtime, its sign-in and its web address for staff.
const SCHEDULER_API_URL_ENV: &str = "FOUNDATION_PLATFORM_AIRFLOW_API_URL";
const SCHEDULER_USERNAME_ENV: &str = "FOUNDATION_PLATFORM_AIRFLOW_API_USERNAME";
const SCHEDULER_PASSWORD_ENV: &str = "FOUNDATION_PLATFORM_AIRFLOW_API_PASSWORD";
/// The task of every job's DAG that runs it (`orchestration/dags/foundation_jobs.py`).
const RUN_TASK: &str = "run";
/// How long the scheduler is left after it did not answer (a log it did not give, a refused
/// sign-in) before it is asked again: a failed job stays failed for hours, and every read would
/// otherwise sign in and ask three times for it.
const SCHEDULER_RETRY_AFTER: Duration = Duration::minutes(10);
/// The most queued and running runs one ask reads; the scheduler holds far fewer.
const IN_PROGRESS_LIMIT: &str = "100";
/// A cause line is cut to this many characters.
const CAUSE_LINE_CHARS: usize = 240;

/// One read of the three stores.
pub(super) struct Facts {
    pub read_at: DateTime<Utc>,
    /// The data catalog's flows, or why they could not be read.
    pub flows: Result<Vec<JsonValue>, String>,
    /// Every Iceberg table a job reads or writes that could be read, its `main` snapshots newest
    /// first. A table that could not be read keeps its earlier read, or is absent.
    pub tables: BTreeMap<String, Vec<IcebergSnapshotSummary>>,
    /// When each table in `tables` was read.
    table_read_at: BTreeMap<String, DateTime<Utc>>,
    /// Why some table could not be read, when one could not.
    pub lakehouse_error: Option<String>,
    /// The logs of the failed latest runs, by DAG id.
    pub failures: BTreeMap<String, FailedRunLog>,
    /// The failed latest runs whose log the scheduler did not give, by DAG id.
    log_misses: BTreeMap<String, SchedulerMiss>,
    /// By DAG id, when the newest run the scheduler holds queued or running was due.
    pub in_progress: BTreeMap<String, DateTime<Utc>>,
    /// The scheduler refused the sign-in: it is not asked again before the miss's `retry_at`.
    sign_in_miss: Option<SchedulerMiss>,
    /// Why the scheduler could not answer something, when it could not.
    pub scheduler_error: Option<String>,
}

/// Something the scheduler did not answer, for which run, why, and when to ask again.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SchedulerMiss {
    run_urn: String,
    error: String,
    retry_at: DateTime<Utc>,
}

/// What to do for a failed latest run's log on this read.
#[derive(Debug, Eq, PartialEq)]
enum LogRead {
    /// It was read for this run before.
    Known(FailedRunLog),
    /// The scheduler did not give it for this run a short while ago.
    Wait(SchedulerMiss),
    /// Ask the scheduler for it.
    Ask,
}

fn log_read(
    failures: &BTreeMap<String, FailedRunLog>,
    misses: &BTreeMap<String, SchedulerMiss>,
    dag_id: &str,
    run_urn: &str,
    now: DateTime<Utc>,
) -> LogRead {
    if let Some(log) = failures.get(dag_id).filter(|log| log.run_urn == run_urn) {
        return LogRead::Known(log.clone());
    }
    misses
        .get(dag_id)
        .filter(|miss| miss.run_urn == run_urn && now < miss.retry_at)
        .map_or(LogRead::Ask, |miss| LogRead::Wait(miss.clone()))
}

fn kept() -> &'static RwLock<Option<Arc<Facts>>> {
    static KEPT: OnceLock<RwLock<Option<Arc<Facts>>>> = OnceLock::new();
    KEPT.get_or_init(|| RwLock::new(None))
}

static READING: AtomicBool = AtomicBool::new(false);

/// The kept read, starting the next one when it is older than `FRESH_FOR`; the first call waits.
pub(super) async fn current(
    list: Arc<JobList>,
    dependencies: Arc<Vec<JobDependencies>>,
) -> Arc<Facts> {
    let previous = kept().read().await.clone();
    match previous {
        Some(facts) if Utc::now() - facts.read_at < FRESH_FOR => facts,
        Some(facts) => {
            if !READING.swap(true, Ordering::AcqRel) {
                let earlier = Arc::clone(&facts);
                tokio::spawn(async move {
                    let next = Arc::new(read(Some(&earlier), &list, &dependencies).await);
                    *kept().write().await = Some(next);
                    READING.store(false, Ordering::Release);
                });
            }
            facts
        }
        None => {
            let next = Arc::new(read(None, &list, &dependencies).await);
            *kept().write().await = Some(Arc::clone(&next));
            next
        }
    }
}

async fn read(previous: Option<&Facts>, list: &JobList, dependencies: &[JobDependencies]) -> Facts {
    let read_at = Utc::now();
    let flows = catalog_flows().await.map_err(unread_runs_reason);
    let statuses = job_statuses(
        list,
        dependencies,
        flows.as_deref().unwrap_or_default(),
        read_at,
    );
    // When a job that writes each table last finished: a table is read again after that.
    let mut written_at: BTreeMap<&str, DateTime<Utc>> = BTreeMap::new();
    let mut tables: Vec<&str> = Vec::new();
    for status in &statuses {
        let finished = status
            .recent_runs
            .iter()
            .filter_map(|run| run.finished_at)
            .max();
        for dataset in &status.writes {
            if let (Some(table), Some(at)) = (iceberg_table(&dataset.dataset), finished) {
                let entry = written_at.entry(table).or_insert(at);
                *entry = (*entry).max(at);
            }
        }
        for dataset in status.writes.iter().chain(&status.reads) {
            if let Some(table) = iceberg_table(&dataset.dataset) {
                if !tables.contains(&table) {
                    tables.push(table);
                }
            }
        }
    }
    let (tables, table_read_at, lakehouse_error) =
        read_tables(&tables, previous, &written_at, read_at).await;

    let (mut scheduler, ask) = known_logs(previous, &statuses, read_at);
    // Only a job past its scheduled start's time limit can be told apart by a queued or running
    // run, so the scheduler is asked for those only while there is one.
    let timeouts = list.timeouts();
    let enabled_since = list.enabled_since();
    let overdue = statuses.iter().any(|status| {
        let timeout = timeouts.get(&status.job_id).copied().unwrap_or(0);
        late(
            status,
            timeout,
            enabled_since.get(&status.job_id).copied(),
            read_at,
        )
        .is_some()
    });
    if !ask.is_empty() || overdue {
        ask_scheduler(previous, &mut scheduler, &ask, overdue, read_at).await;
    }
    let mut errors = scheduler.errors;
    errors.sort();
    errors.dedup();
    Facts {
        read_at,
        flows,
        tables,
        table_read_at,
        lakehouse_error,
        failures: scheduler.failures,
        log_misses: scheduler.log_misses,
        in_progress: scheduler.in_progress,
        sign_in_miss: scheduler.sign_in_miss,
        scheduler_error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
}

/// What one read learns from the scheduler.
#[derive(Default)]
struct SchedulerFacts {
    failures: BTreeMap<String, FailedRunLog>,
    log_misses: BTreeMap<String, SchedulerMiss>,
    in_progress: BTreeMap<String, DateTime<Utc>>,
    sign_in_miss: Option<SchedulerMiss>,
    errors: Vec<String>,
}

/// The failed latest runs' logs already read or not to be asked for yet, and the (DAG id, run)
/// pairs to ask the scheduler for.
fn known_logs<'a>(
    previous: Option<&Facts>,
    statuses: &'a [ScheduledJobStatus],
    read_at: DateTime<Utc>,
) -> (SchedulerFacts, Vec<(&'a str, &'a str)>) {
    let no_failures = BTreeMap::new();
    let no_misses = BTreeMap::new();
    let (known, misses) = previous.map_or((&no_failures, &no_misses), |facts| {
        (&facts.failures, &facts.log_misses)
    });
    let mut facts = SchedulerFacts {
        sign_in_miss: previous
            .and_then(|facts| facts.sign_in_miss.clone())
            .filter(|miss| read_at < miss.retry_at),
        ..SchedulerFacts::default()
    };
    let mut ask = Vec::new();
    for status in statuses {
        let Some(run) = status.last_run.as_ref().filter(|run| {
            matches!(
                run.outcome,
                ScheduledJobRunOutcome::Failed | ScheduledJobRunOutcome::UpForRetry
            )
        }) else {
            continue;
        };
        let dag_id = status.dag_id.as_str();
        match log_read(known, misses, dag_id, &run.run_urn, read_at) {
            LogRead::Known(log) => {
                facts.failures.insert(dag_id.to_owned(), log);
            }
            LogRead::Wait(miss) => {
                facts.errors.push(miss.error.clone());
                facts.log_misses.insert(dag_id.to_owned(), miss);
            }
            LogRead::Ask => ask.push((dag_id, run.run_urn.as_str())),
        }
    }
    (facts, ask)
}

/// Signs in once and asks for each log in `ask`, and for the queued and running runs when
/// `overdue`. A read that cannot ask keeps the last answer about queued and running runs.
async fn ask_scheduler(
    previous: Option<&Facts>,
    facts: &mut SchedulerFacts,
    ask: &[(&str, &str)],
    overdue: bool,
    read_at: DateTime<Utc>,
) {
    let kept_in_progress = || {
        previous
            .map(|facts| facts.in_progress.clone())
            .unwrap_or_default()
    };
    let Some(scheduler) = Scheduler::from_env() else {
        facts
            .errors
            .push("the scheduler's API is not configured".to_owned());
        return;
    };
    if let Some(miss) = &facts.sign_in_miss {
        facts.errors.push(miss.error.clone());
        facts.in_progress = kept_in_progress();
        return;
    }
    let token = match scheduler.sign_in().await {
        Ok(token) => token,
        Err(error) => {
            tracing::warn!(%error, "the scheduler refused Foundation's sign-in");
            facts.errors.push(error.clone());
            facts.in_progress = kept_in_progress();
            facts.sign_in_miss = Some(SchedulerMiss {
                run_urn: String::new(),
                error,
                retry_at: read_at + SCHEDULER_RETRY_AFTER,
            });
            return;
        }
    };
    for (dag_id, run_urn) in ask {
        match scheduler.latest_failure(&token, dag_id, run_urn).await {
            Ok(log) => {
                facts.failures.insert((*dag_id).to_owned(), log);
            }
            Err(error) => {
                tracing::warn!(%dag_id, %error, "a failed run's log could not be read");
                facts.errors.push(error.clone());
                facts.log_misses.insert(
                    (*dag_id).to_owned(),
                    SchedulerMiss {
                        run_urn: (*run_urn).to_owned(),
                        error,
                        retry_at: read_at + SCHEDULER_RETRY_AFTER,
                    },
                );
            }
        }
    }
    if overdue {
        match scheduler.in_progress(&token).await {
            Ok(runs) => facts.in_progress = runs,
            Err(error) => {
                tracing::warn!(%error, "the scheduler's queued and running runs could not be read");
                facts.errors.push(error);
                facts.in_progress = kept_in_progress();
            }
        }
    }
}

type Tables = BTreeMap<String, Vec<IcebergSnapshotSummary>>;
type ReadAt = BTreeMap<String, DateTime<Utc>>;

/// Every table in `wanted`: kept when no writer finished since it was read and it is younger than
/// `TABLE_FRESH_FOR`, read again otherwise, all at once. A table that cannot be read keeps its
/// earlier read, and the answer says which could not be read.
async fn read_tables(
    wanted: &[&str],
    previous: Option<&Facts>,
    written_at: &BTreeMap<&str, DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (Tables, ReadAt, Option<String>) {
    let catalog = LakehouseCatalogConfig::from_env()
        .map_err(|_| "the lakehouse catalog is not configured".to_owned())
        .and_then(|config| IcebergRestCatalog::new(config).map_err(|error| error.to_string()));
    let catalog = match catalog {
        Ok(catalog) => catalog,
        Err(error) => return (Tables::new(), ReadAt::new(), Some(error)),
    };
    let mut tables = Tables::new();
    let mut read_at = ReadAt::new();
    let mut stale = Vec::new();
    for table in wanted {
        let kept = previous.and_then(|facts| {
            let at = *facts.table_read_at.get(*table)?;
            let fresh = now - at < TABLE_FRESH_FOR
                && written_at.get(table).is_none_or(|written| *written < at);
            Some((at, facts.tables.get(*table)?, fresh))
        });
        match kept {
            Some((at, snapshots, true)) => {
                tables.insert((*table).to_owned(), snapshots.clone());
                read_at.insert((*table).to_owned(), at);
            }
            _ => stale.push(*table),
        }
    }
    let reads = futures_util::future::join_all(stale.iter().map(|table| {
        let catalog = &catalog;
        async move { (*table, catalog.load_main_snapshot_history(table).await) }
    }))
    .await;
    let mut unread = Vec::new();
    for (table, answer) in reads {
        match answer {
            Ok(snapshots) => {
                tables.insert(table.to_owned(), snapshots.unwrap_or_default());
                read_at.insert(table.to_owned(), now);
            }
            Err(error) => {
                tracing::warn!(%table, %error, "a table's snapshots could not be read");
                unread.push(table);
                let earlier = previous.and_then(|facts| {
                    Some((facts.tables.get(table)?, *facts.table_read_at.get(table)?))
                });
                if let Some((snapshots, at)) = earlier {
                    tables.insert(table.to_owned(), snapshots.clone());
                    read_at.insert(table.to_owned(), at);
                }
            }
        }
    }
    let error = (!unread.is_empty()).then(|| {
        format!(
            "the lakehouse catalog did not answer for {}",
            unread.join(", ")
        )
    });
    (tables, read_at, error)
}

/// The scheduler's API, signed in as the read-only account the runtime gives Foundation.
struct Scheduler {
    api: String,
    username: String,
    password: String,
}

impl Scheduler {
    fn from_env() -> Option<Self> {
        let value = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        Some(Self {
            api: value(SCHEDULER_API_URL_ENV)?
                .trim_end_matches('/')
                .to_owned(),
            username: value(SCHEDULER_USERNAME_ENV)?,
            password: value(SCHEDULER_PASSWORD_ENV)?,
        })
    }

    /// `GET` the API path made of `segments` (each percent-encoded, as run ids hold `+` and `:`)
    /// with `query`.
    async fn get(
        &self,
        token: &str,
        segments: &[&str],
        query: &[(&str, &str)],
    ) -> Result<JsonValue, String> {
        let mut url = reqwest::Url::parse(&self.api)
            .map_err(|_| format!("{SCHEDULER_API_URL_ENV} is not a URL"))?;
        url.path_segments_mut()
            .map_err(|()| format!("{SCHEDULER_API_URL_ENV} is not a URL"))?
            .pop_if_empty()
            .extend(segments);
        url.query_pairs_mut().extend_pairs(query);
        let what = segments.join("/");
        let response = super::http()
            .get(url)
            .bearer_auth(token)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|_| "the scheduler is not reachable".to_owned())?;
        if !response.status().is_success() {
            return Err(format!(
                "the scheduler answered {} for {what}",
                response.status()
            ));
        }
        response
            .json()
            .await
            .map_err(|_| format!("the scheduler's answer for {what} is not JSON"))
    }

    /// A token for the read-only account.
    async fn sign_in(&self) -> Result<String, String> {
        let signed_in: JsonValue = super::http()
            .post(format!("{}/auth/token", self.api))
            .json(&json!({ "username": self.username, "password": self.password }))
            .send()
            .await
            .map_err(|_| "the scheduler is not reachable".to_owned())?
            .json()
            .await
            .map_err(|_| "the scheduler refused Foundation's sign-in".to_owned())?;
        signed_in["access_token"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "the scheduler refused Foundation's sign-in".to_owned())
    }

    /// By DAG id, when the newest run the scheduler holds queued (waiting for its turn or for its
    /// pool's slots) or running was due.
    async fn in_progress(&self, token: &str) -> Result<BTreeMap<String, DateTime<Utc>>, String> {
        let runs = self
            .get(
                token,
                &["api", "v2", "dags", "~", "dagRuns"],
                &[
                    ("state", "queued"),
                    ("state", "running"),
                    ("order_by", "-run_after"),
                    ("limit", IN_PROGRESS_LIMIT),
                ],
            )
            .await?;
        Ok(in_progress_runs(&runs))
    }

    /// The cause of `dag_id`'s latest failed (or retrying) run, read for the data catalog's
    /// failed run `run_urn`.
    async fn latest_failure(
        &self,
        token: &str,
        dag_id: &str,
        run_urn: &str,
    ) -> Result<FailedRunLog, String> {
        // A run whose task waits for its retry is still running as a DAG run.
        let runs = self
            .get(
                token,
                &["api", "v2", "dags", dag_id, "dagRuns"],
                &[
                    ("order_by", "-run_after"),
                    ("limit", "1"),
                    ("state", "failed"),
                    ("state", "running"),
                ],
            )
            .await?;
        let run_id = runs["dag_runs"][0]["dag_run_id"]
            .as_str()
            .ok_or_else(|| format!("the scheduler holds no failed run of {dag_id}"))?
            .to_owned();
        let task_path = [
            "api",
            "v2",
            "dags",
            dag_id,
            "dagRuns",
            &run_id,
            "taskInstances",
            RUN_TASK,
        ];
        let task = self.get(token, &task_path, &[]).await?;
        // A task trying again is on its next try; the failure is the one before.
        let tried = task["try_number"].as_u64().unwrap_or(1);
        let attempt = match task["state"].as_str() {
            Some("running" | "queued" | "scheduled") => tried.saturating_sub(1),
            _ => tried,
        }
        .max(1)
        .to_string();
        let mut log_path = task_path.to_vec();
        log_path.extend(["logs", &attempt]);
        let log = self
            .get(token, &log_path, &[("full_content", "true")])
            .await?;
        let (cause, cause_line) = classify(&log_lines(&log["content"]));
        Ok(FailedRunLog {
            run_urn: run_urn.to_owned(),
            scheduler_run_id: Some(run_id),
            cause,
            cause_line,
        })
    }
}

/// By DAG id, the latest `run_after` of the queued and running runs in a `dagRuns` answer.
fn in_progress_runs(answer: &JsonValue) -> BTreeMap<String, DateTime<Utc>> {
    let mut runs: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
    for run in answer["dag_runs"].as_array().into_iter().flatten() {
        if !matches!(run["state"].as_str(), Some("queued" | "running")) {
            continue;
        }
        let due = run["run_after"]
            .as_str()
            .and_then(|at| DateTime::parse_from_rfc3339(at).ok());
        let (Some(dag_id), Some(due)) = (run["dag_id"].as_str(), due) else {
            continue;
        };
        let due = due.with_timezone(&Utc);
        let entry = runs.entry(dag_id.to_owned()).or_insert(due);
        *entry = (*entry).max(due);
    }
    runs
}

/// The lines of a task log as the scheduler's API returns it: structured events, or plain text.
fn log_lines(content: &JsonValue) -> Vec<String> {
    match content {
        JsonValue::String(text) => text.lines().map(str::to_owned).collect(),
        JsonValue::Array(events) => events
            .iter()
            .flat_map(|event| match event {
                JsonValue::String(text) => text.lines().map(str::to_owned).collect::<Vec<_>>(),
                other => other["event"]
                    .as_str()
                    .map(|text| text.lines().map(str::to_owned).collect())
                    .unwrap_or_default(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// What a failed run's log says broke it, and the line that says it.
///
/// The host's forced command frames the job's own output: `start-scheduled-job: <service>
/// invocation=...` before it and `start-scheduled-job: <service> result=<systemd Result>` after
/// it (`scripts/ops/start-scheduled-job.sh`); what the scheduler logs after that (its own
/// exception, its lineage events) is not the job's cause, so only the framed lines are read when
/// the frame is there. A `result=` of `oom-kill` or `timeout` names the cause outright; otherwise
/// the job's own last `FAILED`/`refused` line is the cause line (our jobs say what failed in those
/// words), else its last error line, else the result line. Spark's lost shuffle files ("No such
/// file" under `blockmgr-`) are a symptom of the executor, never a missing source.
pub(super) fn classify(lines: &[String]) -> (FailureCause, Option<String>) {
    const OUT_OF_MEMORY: [&str; 5] = [
        "result=oom-kill",
        "outofmemoryerror",
        "out of memory",
        "java heap space",
        "cannot allocate memory",
    ];
    const TIMED_OUT: [&str; 4] = ["result=timeout", "timed out", "timeouterror", "tasktimeout"];
    const SOURCE_MISSING: [&str; 6] = [
        "no such file",
        "filenotfounderror",
        "nosuchkey",
        "nosuchtable",
        "table not found",
        "does not exist",
    ];
    const OWN_WORDS: [&str; 2] = ["failed", "refused"];
    const ERRORS: [&str; 4] = ["error", "exception", "fatal", "result="];
    const FRAME: &str = "start-scheduled-job: ";
    let lowered: Vec<String> = lines.iter().map(|line| line.to_lowercase()).collect();
    let begin = lowered
        .iter()
        .position(|line| line.starts_with(FRAME) && line.contains(" invocation="))
        .unwrap_or(0);
    let end = lowered
        .iter()
        .rposition(|line| line.starts_with(FRAME) && line.contains(" result="))
        .map_or(lowered.len(), |at| at + 1);
    let framed = begin..end.max(begin);
    let last = |needles: &[&str], skip: &dyn Fn(&str) -> bool| {
        framed.clone().rev().find(|&at| {
            let line = &lowered[at];
            !skip(line) && needles.iter().any(|needle| line.contains(needle))
        })
    };
    let never = |_: &str| false;
    let not_frame = |line: &str| line.starts_with(FRAME);
    let spark_scratch = |line: &str| line.contains("blockmgr-") || line.contains("shuffle_");
    let (cause, at) = if let Some(at) = last(&OUT_OF_MEMORY, &never) {
        (FailureCause::OutOfMemory, at)
    } else if let Some(at) = last(&TIMED_OUT, &never) {
        (FailureCause::TimedOut, at)
    } else {
        let own = last(&OWN_WORDS, &not_frame).or_else(|| last(&ERRORS, &not_frame));
        let missing = last(&SOURCE_MISSING, &spark_scratch);
        match (missing, own) {
            (Some(at), _) => (FailureCause::SourceMissing, own.unwrap_or(at)),
            (None, Some(at)) => (FailureCause::Other, at),
            (None, None) => match last(&ERRORS, &never) {
                Some(at) => (FailureCause::Other, at),
                None => return (FailureCause::Other, None),
            },
        }
    };
    (cause, lines.get(at).map(|line| mask(line.trim())))
}

/// The masks a relayed log line goes through, in order, each a pattern and what the match becomes.
/// The host's journal relay masks the same kinds (`scripts/ops/job-journal.sh`); these also catch
/// the spaced, quoted and command-line forms a failing tool prints.
static MASKS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    // A name that holds a credential: `*SECRET*`, `*PASSWORD*`, `*PASSWD*`, `*TOKEN*`,
    // `*ACCESS_KEY*`, `*API_KEY*`, `*CREDENTIAL*`, in any case and with `_`, `-` or `.` inside.
    const NAME: &str = r"[A-Za-z0-9_.-]*(?i:secret|password|passwd|token|access[_-]?key|api[_-]?key|credential)[A-Za-z0-9_.-]*";
    [
        // A URL's password: `scheme://user:password@host`.
        (r"(://[^/\s:@]+:)[^@\s]+@".to_owned(), "${1}***@"),
        // An `Authorization` header's credentials, with or without a scheme word.
        (
            r#"(?i)(authorization["']?\s*[:=]\s*["']?(?:(?:basic|bearer|digest|token|negotiate)\s+)?)[^\s"',]+"#
                .to_owned(),
            "${1}***",
        ),
        (r#"(?i)(\bbearer\s+)[^\s"',]+"#.to_owned(), "${1}***"),
        // `name = "value"`, `"name": "value"`, `name: 'value'`: the whole quoted value.
        (format!(r#"(\b{NAME}"?\s*[=:]\s*")[^"]*""#), "${1}***\""),
        (format!(r"(\b{NAME}'?\s*[=:]\s*')[^']*'"), "${1}***'"),
        // `name=value`, `name: value`, `name = value`.
        (format!(r#"(\b{NAME}["']?\s*[=:]\s*)[^\s"',;&}}]+"#), "${1}***"),
        // A command-line option and its value: `--password value`.
        (format!(r"(--{NAME}\s+)[^\s-]\S*"), "${1}***"),
    ]
    .into_iter()
    .filter_map(|(pattern, replacement)| Some((Regex::new(&pattern).ok()?, replacement)))
    .collect()
});

/// A log line with credentials hidden and its length cut (`MASKS`).
pub(super) fn mask(line: &str) -> String {
    let masked = MASKS
        .iter()
        .fold(line.to_owned(), |line, (pattern, replacement)| {
            pattern.replace_all(&line, *replacement).into_owned()
        });
    if masked.chars().count() > CAUSE_LINE_CHARS {
        format!(
            "{}...",
            masked.chars().take(CAUSE_LINE_CHARS).collect::<String>()
        )
    } else {
        masked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(text: &str) -> Vec<String> {
        text.lines().map(str::to_owned).collect()
    }

    /// The shape of a real failed run's log (2026-10-12, a Gold rebuild), names made up.
    const FRAMED: &str = "Running command: synthetic_job triggered
start-scheduled-job: foundation-synthetic.service invocation=0
synthetic: rebuilding gold.synthetic
26/10/12 07:11:15 ERROR Executor: Exception in task 0.0 in stage 366.0: /scratch/blockmgr-0/08/shuffle_108_2973_0.data (No such file or directory)
synthetic: FAILED: synthetic_silver_to_gold refused or crashed; nothing was committed
foundation-job-outcome unchanged
start-scheduled-job: foundation-synthetic.service result=exit-code
Task failed with exception
Successfully emitted OpenLineage `FAIL` event";

    #[test]
    fn the_jobs_own_failed_line_is_the_cause_and_spark_scratch_is_no_missing_source() {
        let (cause, line) = classify(&lines(FRAMED));
        assert_eq!(cause, FailureCause::Other);
        assert_eq!(
            line.as_deref(),
            Some("synthetic: FAILED: synthetic_silver_to_gold refused or crashed; nothing was committed")
        );
    }

    #[test]
    fn the_systemd_result_names_memory_and_time_outright() {
        let oom = FRAMED.replace("result=exit-code", "result=oom-kill");
        let (cause, line) = classify(&lines(&oom));
        assert_eq!(cause, FailureCause::OutOfMemory);
        assert_eq!(
            line.as_deref(),
            Some("start-scheduled-job: foundation-synthetic.service result=oom-kill")
        );
        let timeout = FRAMED.replace("result=exit-code", "result=timeout");
        assert_eq!(classify(&lines(&timeout)).0, FailureCause::TimedOut);
    }

    #[test]
    fn a_missing_source_is_named_with_the_jobs_own_line() {
        let missing = FRAMED.replace(
            "synthetic: rebuilding gold.synthetic",
            "FileNotFoundError: [Errno 2] No such file or directory: '/synthetic/source.zip'",
        );
        let (cause, line) = classify(&lines(&missing));
        assert_eq!(cause, FailureCause::SourceMissing);
        assert!(line.unwrap_or_default().starts_with("synthetic: FAILED"));
    }

    #[test]
    fn without_its_own_words_the_last_error_line_then_the_result_line_say_it() {
        let (cause, line) = classify(&lines(
            "start-scheduled-job: x.service invocation=1\nValueError: the handoff names no rows\nstart-scheduled-job: x.service result=exit-code\nTask failed with exception",
        ));
        assert_eq!(cause, FailureCause::Other);
        assert_eq!(
            line.as_deref(),
            Some("ValueError: the handoff names no rows")
        );
        let (_, line) = classify(&lines(
            "start-scheduled-job: x.service invocation=1\nstart-scheduled-job: x.service result=exit-code",
        ));
        assert_eq!(
            line.as_deref(),
            Some("start-scheduled-job: x.service result=exit-code")
        );
        assert_eq!(classify(&[]), (FailureCause::Other, None));
    }

    #[test]
    fn a_relayed_line_never_shows_a_credential() {
        let masked = mask(
            "GET https://reader:hunter2@synthetic.example/x Authorization: Bearer abc.def SYNTHETIC_API_TOKEN=s3cr3t password:pw",
        );
        for secret in ["hunter2", "abc.def", "s3cr3t", ":pw"] {
            assert!(!masked.contains(secret), "{secret} in {masked}");
        }
        assert!(masked.contains("https://reader:***@synthetic.example/x"));
        assert_eq!(mask(&"x".repeat(500)).chars().count(), CAUSE_LINE_CHARS + 3);
    }

    #[test]
    fn every_mask_compiles() {
        // A pattern that does not compile is dropped, and its secrets would show.
        assert_eq!(MASKS.len(), 7);
    }

    #[test]
    fn spaced_quoted_and_command_line_credentials_are_hidden_too() {
        for (line, expected) in [
            (
                r#"config {"user": "reader", "password": "two words"}"#,
                r#"config {"user": "reader", "password": "***"}"#,
            ),
            (
                r#"{"db_password":"pw1","host":"h"}"#,
                r#"{"db_password":"***","host":"h"}"#,
            ),
            (
                "spark-submit --password pw2 --name job",
                "spark-submit --password *** --name job",
            ),
            ("aws_secret_access_key = pw3", "aws_secret_access_key = ***"),
            ("AWS_SECRET_ACCESS_KEY: pw4", "AWS_SECRET_ACCESS_KEY: ***"),
            (
                "Authorization: Basic cmVhZGVyOnB3NQ==",
                "Authorization: Basic ***",
            ),
            (
                "headers={'Authorization': 'Token pw6'}",
                "headers={'Authorization': 'Token ***'}",
            ),
            (
                "client_secret='pw7' scope=read",
                "client_secret='***' scope=read",
            ),
            ("--api-key=pw8", "--api-key=***"),
        ] {
            assert_eq!(mask(line), expected, "{line}");
        }
        assert_eq!(
            mask("synthetic: FAILED: synthetic_silver_to_gold refused; 3 of 9 files"),
            "synthetic: FAILED: synthetic_silver_to_gold refused; 3 of 9 files",
            "a line without a credential is left as it is"
        );
    }

    #[test]
    fn a_log_the_scheduler_did_not_give_is_asked_for_again_only_later() {
        let now = DateTime::parse_from_rfc3339("2026-01-02T08:00:00Z")
            .map(|at| at.with_timezone(&Utc))
            .unwrap_or_default();
        let log = FailedRunLog {
            run_urn: "run-1".to_owned(),
            scheduler_run_id: None,
            cause: FailureCause::Other,
            cause_line: None,
        };
        let failures = BTreeMap::from([("dag".to_owned(), log.clone())]);
        let miss = SchedulerMiss {
            run_urn: "run-2".to_owned(),
            error: "the scheduler answered 404".to_owned(),
            retry_at: now + Duration::minutes(5),
        };
        let misses = BTreeMap::from([("dag".to_owned(), miss.clone())]);
        let (no_failures, no_misses) = (BTreeMap::new(), BTreeMap::new());
        assert_eq!(
            log_read(&failures, &no_misses, "dag", "run-1", now),
            LogRead::Known(log)
        );
        assert_eq!(
            log_read(&no_failures, &misses, "dag", "run-2", now),
            LogRead::Wait(miss)
        );
        assert_eq!(
            log_read(
                &no_failures,
                &misses,
                "dag",
                "run-2",
                now + Duration::minutes(5)
            ),
            LogRead::Ask,
            "after the wait"
        );
        assert_eq!(
            log_read(&failures, &misses, "dag", "run-3", now),
            LogRead::Ask,
            "a newer failed run"
        );
    }

    #[test]
    fn queued_and_running_runs_are_read_by_dag_newest_first() {
        // The shape of Airflow 3's `GET /api/v2/dags/~/dagRuns`, ids made up.
        let answer = json!({
            "dag_runs": [
                { "dag_id": "fold", "dag_run_id": "scheduled__b", "state": "queued", "run_after": "2026-01-02T07:00:00Z" },
                { "dag_id": "fold", "dag_run_id": "scheduled__a", "state": "running", "run_after": "2026-01-02T06:00:00Z" },
                { "dag_id": "bake", "dag_run_id": "manual__c", "state": "running", "run_after": "2026-01-02T05:30:00+00:00" },
                { "dag_id": "done", "dag_run_id": "scheduled__d", "state": "success", "run_after": "2026-01-02T05:00:00Z" },
                { "dag_id": "odd", "state": "queued" }
            ],
            "total_entries": 5
        });
        let runs = in_progress_runs(&answer);
        assert_eq!(
            runs.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["bake", "fold"]
        );
        assert_eq!(
            runs.get("fold").map(DateTime::to_rfc3339),
            Some("2026-01-02T07:00:00+00:00".to_owned())
        );
    }

    #[test]
    fn a_log_reads_as_structured_events_or_as_text() {
        assert_eq!(
            log_lines(&json!([{ "event": "one\ntwo", "timestamp": "t" }, "three", { "other": 1 }])),
            vec!["one", "two", "three"]
        );
        assert_eq!(log_lines(&json!("a\nb")), vec!["a", "b"]);
    }
}
