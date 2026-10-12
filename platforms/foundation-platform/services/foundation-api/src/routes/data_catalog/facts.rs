//! The run facts the operations map reads, kept for a short while (root ADR-0195).
//!
//! Three stores answer: the data catalog holds the runs (`OpenLineage`, root ADR-0165), the
//! lakehouse catalog holds each table's snapshots with the rows each added, and the scheduler
//! (Airflow) holds each run's log, the only place a failure says why. Asking all three for every
//! screen refresh would make each answer as slow as the slowest of them, so one read is kept and
//! served for `FRESH_FOR`; a request after that gets the kept read at once and starts the next in
//! the background (stale-while-revalidate). A table is read again only when a job that writes it
//! finished since it was read, or after `TABLE_FRESH_FOR`; a failed run's log is read once.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Duration, Utc};
use foundation_contracts::data_catalog::{FailureCause, ScheduledJobRunOutcome};
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotSummary, LakehouseCatalogConfig,
};
use serde_json::{json, Value as JsonValue};
use tokio::sync::RwLock;

use super::health::{iceberg_table, FailedRunLog};
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
    /// Why a failed run's log could not be read, when one could not.
    pub scheduler_error: Option<String>,
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

    let mut failures = BTreeMap::new();
    let mut scheduler_error = None;
    let failed: Vec<(&str, &str)> = statuses
        .iter()
        .filter_map(|status| {
            let run = status.last_run.as_ref()?;
            matches!(
                run.outcome,
                ScheduledJobRunOutcome::Failed | ScheduledJobRunOutcome::UpForRetry
            )
            .then_some((status.dag_id.as_str(), run.run_urn.as_str()))
        })
        .collect();
    if !failed.is_empty() {
        match Scheduler::from_env() {
            None => scheduler_error = Some("the scheduler's API is not configured".to_owned()),
            Some(scheduler) => {
                for (dag_id, run_urn) in failed {
                    let known = previous
                        .and_then(|facts| facts.failures.get(dag_id))
                        .filter(|log| log.run_urn == run_urn);
                    if let Some(log) = known {
                        failures.insert(dag_id.to_owned(), log.clone());
                        continue;
                    }
                    match scheduler.latest_failure(dag_id, run_urn).await {
                        Ok(log) => {
                            failures.insert(dag_id.to_owned(), log);
                        }
                        Err(error) => {
                            tracing::warn!(%dag_id, %error, "a failed run's log could not be read");
                            scheduler_error = Some(error);
                        }
                    }
                }
            }
        }
    }
    Facts {
        read_at,
        flows,
        tables,
        table_read_at,
        lakehouse_error,
        failures,
        scheduler_error,
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

    /// The cause of `dag_id`'s latest failed (or retrying) run, read for the data catalog's
    /// failed run `run_urn`.
    async fn latest_failure(&self, dag_id: &str, run_urn: &str) -> Result<FailedRunLog, String> {
        let signed_in: JsonValue = super::http()
            .post(format!("{}/auth/token", self.api))
            .json(&json!({ "username": self.username, "password": self.password }))
            .send()
            .await
            .map_err(|_| "the scheduler is not reachable".to_owned())?
            .json()
            .await
            .map_err(|_| "the scheduler refused Foundation's sign-in".to_owned())?;
        let token = signed_in["access_token"]
            .as_str()
            .ok_or_else(|| "the scheduler refused Foundation's sign-in".to_owned())?;
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

/// A log line with credentials hidden and its length cut, the way the host's journal relay masks
/// what it relays (`scripts/ops/job-journal.sh`): a URL's password, a bearer token, and the value
/// of any `*SECRET*`, `*PASSWORD*`, `*TOKEN*`, `*ACCESS_KEY*` or `*API_KEY*` assignment.
pub(super) fn mask(line: &str) -> String {
    const SECRET_NAMES: [&str; 5] = ["SECRET", "PASSWORD", "TOKEN", "ACCESS_KEY", "API_KEY"];
    let mut words: Vec<String> = Vec::new();
    let mut bearer = false;
    for word in line.split(' ') {
        let mut word = word.to_owned();
        if bearer && !word.is_empty() {
            "***".clone_into(&mut word);
        }
        bearer = word.eq_ignore_ascii_case("bearer");
        if let Some(scheme) = word.find("://") {
            let rest = &word[scheme + 3..];
            if let Some(at) = rest.find('@') {
                if let Some(colon) = rest[..at].find(':') {
                    word = format!(
                        "{}{}***{}",
                        &word[..scheme + 3],
                        &rest[..=colon],
                        &rest[at..]
                    );
                }
            }
        }
        if let Some(split) = word.find(['=', ':']) {
            let name = word[..split].to_ascii_uppercase();
            if SECRET_NAMES.iter().any(|secret| name.contains(secret)) && split + 1 < word.len() {
                word = format!("{}***", &word[..=split]);
            }
        }
        words.push(word);
    }
    let masked = words.join(" ");
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
    fn a_log_reads_as_structured_events_or_as_text() {
        assert_eq!(
            log_lines(&json!([{ "event": "one\ntwo", "timestamp": "t" }, "three", { "other": 1 }])),
            vec!["one", "two", "three"]
        );
        assert_eq!(log_lines(&json!("a\nb")), vec!["a", "b"]);
    }
}
