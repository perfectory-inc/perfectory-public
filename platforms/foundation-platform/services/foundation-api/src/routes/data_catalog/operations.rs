//! How the scheduled jobs ran and what the data contracts' checks found (root ADR-0165).
//!
//! Both facts already live in the data catalog: every Airflow run reports to it by `OpenLineage`
//! (root ADR-0118 §2), and the quality job reports each contract check's result on its assertion
//! (root ADR-0117 §6, ADR-0123). Foundation reads them back with fixed questions, as it does for
//! search and lineage (root ADR-0119), and adds only what the catalog does not hold: the job list
//! itself (`orchestration/jobs.v1.json`) and each job's next start, computed from its schedule
//! there, and how long each job usually takes, from the durations of its recent succeeded runs.
//!
//! What each job reads, writes and waits on is not derived here: `orchestration/dags/job_specs.py`
//! derives it for the scheduler and renders it to `orchestration/job-dependencies.v1.json`, which a
//! test keeps equal to that derivation; this reads the rendered file (root ADR-0188).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use axum::{extract::State, Json};
use chrono::{DateTime, TimeZone, Utc};
use croner::Cron;
use foundation_contracts::data_catalog::{
    DataContractQuality, DataQualityCheck, DataQualityDetail, DataQualityResult, DataQualityStatus,
    OperationsLinks, ScheduledJobDataset, ScheduledJobRun, ScheduledJobRunOutcome,
    ScheduledJobStart, ScheduledJobStatus, ScheduledJobsStatus,
};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use super::{ask, facts, health, text};
use crate::routes::api_error::ApiError;
use crate::state::AppState;

const JOBS_PATH_ENV: &str = "FOUNDATION_PLATFORM_SCHEDULED_JOBS_PATH";
const DEFAULT_JOBS_PATH: &str = "orchestration/jobs.v1.json";
const DEPENDENCIES_PATH_ENV: &str = "FOUNDATION_PLATFORM_JOB_DEPENDENCIES_PATH";
const DEFAULT_DEPENDENCIES_PATH: &str = "orchestration/job-dependencies.v1.json";
const DEPENDENCIES_SCHEMA: &str = "foundation-platform.orchestration_job_dependencies.v1";
const SCHEDULER_UI_URL_ENV: &str = "FOUNDATION_PLATFORM_AIRFLOW_UI_URL";
const CATALOG_UI_URL_ENV: &str = "FOUNDATION_PLATFORM_DATAHUB_UI_URL";
/// Search results per catalog page.
const PAGE: u64 = 100;
/// Pages read at most. A catalog with more flows or contracts than this is reported, never cut
/// short silently: a job past the cut would otherwise look as if it had never run.
const MAX_PAGES: u64 = 50;
/// Child jobs of one flow (an Airflow DAG reports itself and its one task) and runs per child read.
/// The runs read are also the ones a job's typical duration is the median of, and the recent runs
/// each job shows.
const CHILD_JOBS: u64 = 10;
const RUNS_PER_JOB: u64 = 10;

/// The part of the job list this screen reads.
#[derive(Debug, Deserialize)]
pub(super) struct JobList {
    dag_id_prefix: String,
    jobs: Vec<ListedJob>,
}

impl JobList {
    /// Each job's limit for one start in minutes, by job id: its run's time limit plus its stop
    /// grace (root ADR-0270 §1; `job_specs.scheduler_timeout_minutes` adds the same two).
    pub(super) fn timeouts(&self) -> BTreeMap<String, u32> {
        self.jobs
            .iter()
            .map(|job| {
                (
                    job.id.clone(),
                    job.time_limit_minutes
                        .saturating_add(job.stop_grace_minutes),
                )
            })
            .collect()
    }

    /// When each enabled job was switched on, by job id (root ADR-0239).
    pub(super) fn enabled_since(&self) -> BTreeMap<String, DateTime<Utc>> {
        self.jobs
            .iter()
            .filter_map(|job| Some((job.id.clone(), job.enabled_since?)))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct ListedJob {
    id: String,
    description: String,
    schedule: String,
    /// How long a run may run (root ADR-0270 §1). With `stop_grace_minutes`, a scheduled start
    /// this long ago with no success since is late (root ADR-0195). Both are required: a job list
    /// without them is refused, not read as zero.
    time_limit_minutes: u32,
    /// How much longer than its run the scheduler gives one start.
    stop_grace_minutes: u32,
    enabled: bool,
    /// When the job was switched on (root ADR-0239): a slot before it is not one it missed.
    #[serde(default)]
    enabled_since: Option<DateTime<Utc>>,
    #[serde(default)]
    disabled_reason: Option<String>,
    started_by: ScheduledJobStart,
    pipeline_graph_edges: Vec<String>,
}

/// `orchestration/job-dependencies.v1.json`, rendered by `job_specs.py`.
#[derive(Debug, Deserialize)]
struct DependencyDocument {
    schema_version: String,
    /// The task of every job DAG that runs the job (`job_specs.RUN_TASK_ID`).
    run_task_id: String,
    jobs: Vec<JobDependencies>,
}

#[derive(Debug, Deserialize)]
pub(super) struct JobDependencies {
    id: String,
    producers: Vec<String>,
    pub(super) reads: Vec<ScheduledJobDataset>,
    pub(super) writes: Vec<ScheduledJobDataset>,
    /// The document's `run_task_id`, carried to each job.
    #[serde(skip)]
    run_task_id: String,
}

/// Every scheduled job, its next start and its latest run.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/scheduled-jobs",
    operation_id = "listScheduledJobs",
    responses(
        (status = 200, body = ScheduledJobsStatus),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
        (status = 500, description = "The job list could not be read", body = InternalApiErrorResponse),
    )
)]
pub(super) async fn scheduled_jobs(
    State(_state): State<Arc<AppState>>,
) -> Result<Json<ScheduledJobsStatus>, ApiError> {
    Ok(Json(jobs_status().await?))
}

/// Every scheduled job with its runs and problems, from the kept read of the three stores.
pub(super) async fn jobs_status() -> Result<ScheduledJobsStatus, ApiError> {
    let (list, dependencies) = read_registry()?;
    let facts = facts::current(Arc::clone(&list), Arc::clone(&dependencies)).await;
    let now = Utc::now();
    let (flows, run_history_error) = match &facts.flows {
        Ok(flows) => (flows.as_slice(), None),
        Err(error) => (&[][..], Some(error.clone())),
    };
    let mut jobs = job_statuses(&list, &dependencies, flows, now);
    let timeouts = list.timeouts();
    let enabled_since = list.enabled_since();
    health::annotate(
        &mut jobs,
        &health::HealthFacts {
            timeouts: &timeouts,
            enabled_since: &enabled_since,
            tables: &facts.tables,
            failures: &facts.failures,
            in_progress: &facts.in_progress,
            runs_readable: run_history_error.is_none(),
            now,
        },
    );
    Ok(ScheduledJobsStatus {
        read_at: now,
        jobs,
        run_history_error,
        lakehouse_error: facts.lakehouse_error.clone(),
        scheduler_error: facts.scheduler_error.clone(),
        links: links(),
    })
}

/// The job list and its rendered dependencies, shared with the run facts' background read.
pub(super) fn read_registry() -> Result<(Arc<JobList>, Arc<Vec<JobDependencies>>), ApiError> {
    let list = read_job_list(&jobs_path())?;
    let dependencies = read_dependencies(&dependencies_path(), &list)?;
    Ok((Arc::new(list), Arc::new(dependencies)))
}

/// Where staff open the scheduler's and the data catalog's own screens: the runtime names them
/// (`compose.orchestration-bridge.yml`, `compose.metadata-bridge.yml`), nothing here does.
fn links() -> OperationsLinks {
    let value = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    OperationsLinks {
        scheduler_url: value(SCHEDULER_UI_URL_ENV),
        catalog_url: value(CATALOG_UI_URL_ENV),
    }
}

/// Every data contract the catalog holds and the latest result of each of its checks.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/quality-checks",
    operation_id = "listDataQualityChecks",
    responses(
        (status = 200, body = DataQualityStatus),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
        (status = 503, description = "The data catalog is not configured or not reachable", body = ApiErrorResponse),
    )
)]
pub(super) async fn quality_checks(
    State(_state): State<Arc<AppState>>,
) -> Result<Json<DataQualityStatus>, ApiError> {
    let datasets = search_all(CONTRACTS_QUERY, json!({})).await?;
    let mut contracts: Vec<DataContractQuality> =
        datasets.iter().filter_map(contract_quality).collect();
    contracts.sort_by(|a, b| a.contract_id.cmp(&b.contract_id));
    Ok(Json(DataQualityStatus {
        read_at: Utc::now(),
        contracts,
    }))
}

/// What the screen is told when the runs could not be read. An internal failure's detail goes to
/// the log, as `ApiError::Internal` does for a whole response.
pub(super) fn unread_runs_reason(error: ApiError) -> String {
    match error {
        ApiError::Unavailable(message) => message,
        other => {
            tracing::warn!(error = ?other, "scheduled job runs could not be read from the data catalog");
            "the data catalog answered, but its runs could not be read".to_owned()
        }
    }
}

fn jobs_path() -> PathBuf {
    // Resolved exactly as the pipeline graph is: the runtime image names its copy by the
    // environment variable, a build tree reads its own.
    crate::routes::pipeline_graph::artifact_path_from(
        std::env::var(JOBS_PATH_ENV).ok().as_deref(),
        DEFAULT_JOBS_PATH,
    )
}

fn dependencies_path() -> PathBuf {
    crate::routes::pipeline_graph::artifact_path_from(
        std::env::var(DEPENDENCIES_PATH_ENV).ok().as_deref(),
        DEFAULT_DEPENDENCIES_PATH,
    )
}

/// Each listed job's dependencies, in job-list order; refuses a file rendered from another list.
fn read_dependencies(path: &Path, list: &JobList) -> Result<Vec<JobDependencies>, ApiError> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        ApiError::Internal(format!(
            "cannot read the job dependencies {}: {error}",
            path.display()
        ))
    })?;
    let document: DependencyDocument = serde_json::from_str(&raw).map_err(|error| {
        ApiError::Internal(format!(
            "the job dependencies {} are invalid: {error}",
            path.display()
        ))
    })?;
    if document.schema_version != DEPENDENCIES_SCHEMA {
        return Err(ApiError::Internal(format!(
            "the job dependencies {} are not {DEPENDENCIES_SCHEMA}",
            path.display()
        )));
    }
    let rendered: Vec<&str> = document.jobs.iter().map(|job| job.id.as_str()).collect();
    let listed: Vec<&str> = list.jobs.iter().map(|job| job.id.as_str()).collect();
    if rendered != listed {
        return Err(ApiError::Internal(format!(
            "the job dependencies {} were rendered from another job list; run job_specs.py --write-dependencies",
            path.display()
        )));
    }
    let run_task_id = document.run_task_id;
    Ok(document
        .jobs
        .into_iter()
        .map(|job| JobDependencies {
            run_task_id: run_task_id.clone(),
            ..job
        })
        .collect())
}

fn read_job_list(path: &Path) -> Result<JobList, ApiError> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        ApiError::Internal(format!(
            "cannot read the job list {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        ApiError::Internal(format!(
            "the job list {} is invalid: {error}",
            path.display()
        ))
    })
}

/// Each listed job with its next start, its dependencies (`dependencies` in job-list order) and
/// the latest runs among the catalog flows named for it.
pub(super) fn job_statuses(
    list: &JobList,
    dependencies: &[JobDependencies],
    flows: &[JsonValue],
    now: DateTime<Utc>,
) -> Vec<ScheduledJobStatus> {
    list.jobs
        .iter()
        .zip(dependencies)
        .map(|(job, depends)| {
            let dag_id = format!("{}{}", list.dag_id_prefix, job.id);
            let runs: Vec<(String, ScheduledJobRun, i64)> = flows
                .iter()
                .filter(|flow| flow["flowId"].as_str() == Some(dag_id.as_str()))
                .flat_map(child_runs)
                .collect();
            let runs = with_run_task_starts(runs, &dag_id, &depends.run_task_id);
            let latest = runs
                .iter()
                .max_by_key(|(_, run, last_event)| (*last_event, run.run_urn.clone()));
            let catalog_job_urn = latest.map(|(job_urn, _, _)| job_urn.clone());
            let last_run = latest.map(|(_, run, _)| run.clone());
            // The same catalog job's runs only: the DAG and its task each report every run, so
            // taking both would count one run twice.
            let mut own: Vec<&(String, ScheduledJobRun, i64)> = runs
                .iter()
                .filter(|(job_urn, _, _)| Some(job_urn) == catalog_job_urn.as_ref())
                .collect();
            let typical = typical_duration(own.iter().map(|(_, run, _)| run));
            own.sort_by(|a, b| (b.2, &b.1.run_urn).cmp(&(a.2, &a.1.run_urn)));
            let recent_runs = own
                .iter()
                .take(usize::try_from(RUNS_PER_JOB).unwrap_or(usize::MAX))
                .map(|(_, run, _)| run.clone())
                .collect();
            ScheduledJobStatus {
                job_id: job.id.clone(),
                dag_id,
                description: job.description.clone(),
                schedule: job.schedule.clone(),
                enabled: job.enabled,
                disabled_reason: job.disabled_reason.clone(),
                next_run_at: job
                    .enabled
                    .then(|| next_start(&job.schedule, now))
                    .flatten(),
                catalog_job_urn,
                last_run,
                typical_duration_seconds: typical.map(|(seconds, _)| seconds),
                typical_duration_runs: typical.map_or(0, |(_, count)| count),
                recent_runs,
                started_by: job.started_by,
                pipeline_graph_edges: job.pipeline_graph_edges.clone(),
                upstream_job_ids: depends.producers.clone(),
                reads: depends.reads.clone(),
                writes: depends.writes.clone(),
                problems: Vec::new(),
            }
        })
        .collect()
}

/// The median duration of the succeeded runs among `runs` that recorded both a start and a
/// finish, in whole seconds, and how many runs it is the median of; `None` when there is none.
///
/// The median, not the mean: one run that waited out a long retry must not move what "usually"
/// means. An even count takes the mean of the two middle durations, rounded down.
fn typical_duration<'a>(runs: impl Iterator<Item = &'a ScheduledJobRun>) -> Option<(u64, u32)> {
    let mut seconds: Vec<u64> = runs
        .filter(|run| run.outcome == ScheduledJobRunOutcome::Succeeded)
        .filter_map(|run| {
            let took = run.finished_at?.signed_duration_since(run.started_at?);
            u64::try_from(took.num_seconds()).ok()
        })
        .collect();
    seconds.sort_unstable();
    let count = seconds.len();
    let upper = *seconds.get(count / 2)?;
    let median = if count.is_multiple_of(2) {
        let lower = *seconds.get(count / 2 - 1)?;
        lower + (upper - lower) / 2
    } else {
        upper
    };
    Some((median, u32::try_from(count).unwrap_or(u32::MAX)))
}

/// The DAG's own runs with their waiting told from their running.
///
/// A job's DAG run starts first and its run task waits there for a slot of the job's pool: the DAG
/// run's start is when the run was queued (`queued_at`), and the first start of a run task inside
/// the DAG run is when its work started (`started_at`, absent while it still waits). Measured from
/// the DAG run's start, a run that waited three hours for the pool and ran for a minute read as a
/// three-hour run and was called slow (2026-10-18). Runs of the task job itself are left as they are.
fn with_run_task_starts(
    runs: Vec<(String, ScheduledJobRun, i64)>,
    dag_id: &str,
    run_task_id: &str,
) -> Vec<(String, ScheduledJobRun, i64)> {
    let dag_job = format!(",{dag_id})");
    let task_job = format!(",{dag_id}.{run_task_id})");
    let task_starts: Vec<DateTime<Utc>> = runs
        .iter()
        .filter(|(job_urn, _, _)| job_urn.ends_with(&task_job))
        .filter_map(|(_, run, _)| run.started_at)
        .collect();
    runs.into_iter()
        .map(|(job_urn, mut run, last_event)| {
            if job_urn.ends_with(&dag_job) {
                if let Some(queued) = run.started_at {
                    let until = run.finished_at.unwrap_or(DateTime::<Utc>::MAX_UTC);
                    run.queued_at = Some(queued);
                    run.started_at = task_starts
                        .iter()
                        .copied()
                        .filter(|start| queued <= *start && *start <= until)
                        .min();
                }
            }
            (job_urn, run, last_event)
        })
        .collect()
}

/// The next time a five-field cron schedule starts a job after `now`, in UTC as Airflow reads it.
fn next_start(schedule: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let cron = Cron::from_str(schedule).ok()?;
    cron.find_next_occurrence(&now, false).ok()
}

/// Every run of a flow's child jobs: the child job's urn, the run, and its latest event time.
fn child_runs(flow: &JsonValue) -> Vec<(String, ScheduledJobRun, i64)> {
    let mut found = Vec::new();
    for relationship in flow["jobs"]["relationships"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let job = &relationship["entity"];
        let Some(job_urn) = job["urn"].as_str() else {
            continue;
        };
        for run in job["runs"]["runs"].as_array().into_iter().flatten() {
            if let Some((run, last_event)) = run_from(run) {
                found.push((job_urn.to_owned(), run, last_event));
            }
        }
    }
    found
}

/// One run from its recorded state changes; `None` when it has no state change at all.
///
/// The latest change decides: a start after a completion is a retry that is running again.
fn run_from(run: &JsonValue) -> Option<(ScheduledJobRun, i64)> {
    let run_urn = run["urn"].as_str()?.to_owned();
    let events: Vec<&JsonValue> = run["state"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|event| event["timestampMillis"].as_i64().is_some())
        .collect();
    let latest = *events
        .iter()
        .max_by_key(|event| event["timestampMillis"].as_i64())?;
    let last_event = latest["timestampMillis"].as_i64()?;
    let started_at = events
        .iter()
        .filter(|event| event["status"].as_str() == Some("STARTED"))
        .filter_map(|event| event["timestampMillis"].as_i64())
        .min()
        .and_then(millis);
    let complete = latest["status"].as_str() == Some("COMPLETE");
    let result = &latest["result"];
    let outcome = if complete {
        match result["resultType"].as_str() {
            Some("SUCCESS") => ScheduledJobRunOutcome::Succeeded,
            Some("FAILURE") => ScheduledJobRunOutcome::Failed,
            Some("SKIPPED") => ScheduledJobRunOutcome::Skipped,
            Some("UP_FOR_RETRY") => ScheduledJobRunOutcome::UpForRetry,
            _ => ScheduledJobRunOutcome::Unknown,
        }
    } else {
        ScheduledJobRunOutcome::Running
    };
    Some((
        ScheduledJobRun {
            run_urn,
            queued_at: None,
            started_at,
            finished_at: complete.then(|| millis(last_event)).flatten(),
            outcome,
            native_result: complete
                .then(|| text(&result["nativeResultType"]))
                .flatten(),
            rows_written: None,
        },
        last_event,
    ))
}

fn millis(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
}

/// Every Airflow-style flow in the catalog with its child jobs and their latest runs.
pub(super) async fn catalog_flows() -> Result<Vec<JsonValue>, ApiError> {
    search_all(
        FLOWS_QUERY,
        json!({ "children": CHILD_JOBS, "runs": RUNS_PER_JOB }),
    )
    .await
}

const FLOWS_QUERY: &str = "query($start: Int!, $count: Int!, $children: Int!, $runs: Int!) {
  searchAcrossEntities(input: { types: [DATA_FLOW], query: \"*\", start: $start, count: $count }) {
    total searchResults { entity { ... on DataFlow { urn flowId
      jobs: relationships(input: { types: [\"IsPartOf\"], direction: INCOMING, start: 0, count: $children }) {
        relationships { entity { ... on DataJob { urn
          runs(start: 0, count: $runs) { runs { urn
            state(limit: 20) { status timestampMillis result { resultType nativeResultType } } } } } } } } } } } } }";

const CONTRACTS_QUERY: &str = "query($start: Int!, $count: Int!) {
  searchAcrossEntities(input: { types: [DATASET], query: \"*\", start: $start, count: $count,
    orFilters: [{ and: [{ field: \"platform\", values: [\"urn:li:dataPlatform:odcs\"] }] }] }) {
    total searchResults { entity { ... on Dataset { urn name platform { name } properties { name }
      assertions(start: 0, count: 200) { assertions { urn
        info { type fieldAssertion { fieldValuesAssertion { field { path } } } }
        runEvents(limit: 1) { runEvents { timestampMillis result { type nativeResults { key value } } } } } } } } } } }";

/// Every search result of `document`, page by page; refuses a catalog larger than `MAX_PAGES`.
///
/// `document` takes `$start` and `$count`, and whatever else `variables` names.
async fn search_all(document: &str, variables: JsonValue) -> Result<Vec<JsonValue>, ApiError> {
    let mut entities = Vec::new();
    let mut start: u64 = 0;
    for _ in 0..MAX_PAGES {
        let mut page_variables = variables.clone();
        if let Some(object) = page_variables.as_object_mut() {
            object.insert("start".to_owned(), json!(start));
            object.insert("count".to_owned(), json!(PAGE));
        }
        let data = ask(document, page_variables).await?;
        let found = &data["searchAcrossEntities"];
        let page: Vec<JsonValue> = found["searchResults"]
            .as_array()
            .map(|rows| rows.iter().map(|row| row["entity"].clone()).collect())
            .unwrap_or_default();
        let total = found["total"].as_u64().unwrap_or(0);
        let read = u64::try_from(page.len()).unwrap_or(u64::MAX);
        entities.extend(page);
        start += read;
        if read == 0 || start >= total {
            return Ok(entities);
        }
    }
    Err(ApiError::Internal(format!(
        "the data catalog holds more than {} results; not all were read",
        PAGE * MAX_PAGES
    )))
}

/// One contract's checks with their latest results; `None` for anything but a contract dataset.
fn contract_quality(dataset: &JsonValue) -> Option<DataContractQuality> {
    if dataset["platform"]["name"].as_str() != Some("odcs") {
        return None;
    }
    let dataset_urn = dataset["urn"].as_str()?.to_owned();
    let contract_id = text(&dataset["properties"]["name"]).or_else(|| text(&dataset["name"]))?;
    let mut checks: Vec<DataQualityCheck> = dataset["assertions"]["assertions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(quality_check)
        .collect();
    checks.sort_by(|a, b| {
        (a.kind.as_str(), a.field.as_deref()).cmp(&(b.kind.as_str(), b.field.as_deref()))
    });
    Some(DataContractQuality {
        contract_id,
        dataset_urn,
        checks,
    })
}

fn quality_check(assertion: &JsonValue) -> Option<DataQualityCheck> {
    let assertion_urn = assertion["urn"].as_str()?.to_owned();
    let info = &assertion["info"];
    let latest = assertion["runEvents"]["runEvents"]
        .as_array()
        .and_then(|events| {
            events
                .iter()
                .max_by_key(|event| event["timestampMillis"].as_i64())
        });
    let result = latest.map_or(DataQualityResult::NotRun, |event| {
        match event["result"]["type"].as_str() {
            Some("SUCCESS") => DataQualityResult::Success,
            Some("FAILURE") => DataQualityResult::Failure,
            Some("ERROR") => DataQualityResult::Error,
            _ => DataQualityResult::NotRun,
        }
    });
    let details = latest
        .and_then(|event| event["result"]["nativeResults"].as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    Some(DataQualityDetail {
                        key: entry["key"].as_str()?.to_owned(),
                        value: entry["value"].as_str()?.to_owned(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Some(DataQualityCheck {
        assertion_urn,
        kind: text(&info["type"]).unwrap_or_else(|| "UNKNOWN".to_owned()),
        field: text(&info["fieldAssertion"]["fieldValuesAssertion"]["field"]["path"]),
        result,
        checked_at: latest
            .and_then(|event| event["timestampMillis"].as_i64())
            .and_then(millis),
        details,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(iso: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(iso)
            .map(|time| time.with_timezone(&Utc))
            .unwrap_or_default()
    }

    fn ms(iso: &str) -> i64 {
        at(iso).timestamp_millis()
    }

    #[test]
    fn the_real_job_list_gives_each_start_more_than_its_run() -> Result<(), String> {
        // Read at run time (cargo sets it for the test): no compile-time file dependency.
        let manifest = std::env::var("CARGO_MANIFEST_DIR").map_err(|error| error.to_string())?;
        let path = Path::new(&manifest).join("../..").join(DEFAULT_JOBS_PATH);
        let real = read_job_list(&path).map_err(|error| format!("{error:?}"))?;
        let timeouts = real.timeouts();
        assert_eq!(timeouts.len(), real.jobs.len());
        for job in &real.jobs {
            assert!(job.time_limit_minutes > 0, "{}", job.id);
            assert!(
                timeouts.get(&job.id) > Some(&job.time_limit_minutes),
                "{}",
                job.id
            );
        }
        Ok(())
    }

    fn list() -> JobList {
        JobList {
            dag_id_prefix: "synthetic_".to_owned(),
            jobs: vec![
                ListedJob {
                    id: "nightly".to_owned(),
                    description: "a nightly job".to_owned(),
                    schedule: "20 7 * * *".to_owned(),
                    time_limit_minutes: 55,
                    stop_grace_minutes: 5,
                    enabled: true,
                    enabled_since: None,
                    disabled_reason: None,
                    started_by: ScheduledJobStart::Schedule,
                    pipeline_graph_edges: vec!["synthetic-edge".to_owned()],
                },
                ListedJob {
                    id: "half_hourly".to_owned(),
                    description: "a half-hourly job".to_owned(),
                    schedule: "0,30 * * * *".to_owned(),
                    time_limit_minutes: 55,
                    stop_grace_minutes: 5,
                    enabled: true,
                    enabled_since: None,
                    disabled_reason: None,
                    started_by: ScheduledJobStart::Schedule,
                    pipeline_graph_edges: vec!["synthetic-edge".to_owned()],
                },
                ListedJob {
                    id: "off".to_owned(),
                    description: "a job that is off".to_owned(),
                    schedule: "40 4 * * *".to_owned(),
                    time_limit_minutes: 55,
                    stop_grace_minutes: 5,
                    enabled: false,
                    enabled_since: None,
                    disabled_reason: Some("waiting for a supervised run".to_owned()),
                    started_by: ScheduledJobStart::Inputs,
                    pipeline_graph_edges: Vec::new(),
                },
            ],
        }
    }

    /// Dependencies for `list` as `job_specs.py` renders them: the half-hourly job feeds the others.
    fn dependencies(list: &JobList) -> Vec<JobDependencies> {
        list.jobs
            .iter()
            .map(|job| JobDependencies {
                id: job.id.clone(),
                producers: if job.id == "half_hourly" {
                    Vec::new()
                } else {
                    vec!["half_hourly".to_owned()]
                },
                reads: Vec::new(),
                writes: Vec::new(),
                run_task_id: "run".to_owned(),
            })
            .collect()
    }

    fn event(status: &str, iso: &str, result: Option<&str>) -> JsonValue {
        json!({
            "status": status,
            "timestampMillis": ms(iso),
            "result": result.map(|r| json!({ "resultType": r, "nativeResultType": r.to_lowercase() })),
        })
    }

    fn flow(flow_id: &str, jobs: &[(&str, Vec<JsonValue>)]) -> JsonValue {
        json!({
            "urn": format!("urn:li:dataFlow:(synthetic,{flow_id},test)"),
            "flowId": flow_id,
            "jobs": { "relationships": jobs.iter().map(|(job, runs)| json!({ "entity": {
                "urn": format!("urn:li:dataJob:(urn:li:dataFlow:(synthetic,{flow_id},test),{job})"),
                "runs": { "runs": runs },
            }})).collect::<Vec<_>>() },
        })
    }

    fn run(urn: &str, events: &[JsonValue]) -> JsonValue {
        json!({ "urn": urn, "state": events })
    }

    /// One day's DAG run: the DAG starts at 07:20, its run task at `task_start` (absent while it
    /// still waits) and completes `run_minutes` after it, and the DAG a minute later (its
    /// `publish_outputs` task runs last).
    fn day(day: u32, task_start: Option<&str>, run_minutes: i64) -> (JsonValue, Option<JsonValue>) {
        let dag_start = format!("2026-01-{day:02}T07:20:00Z");
        let Some(task_start) = task_start.map(|time| format!("2026-01-{day:02}T{time}:00Z")) else {
            return (
                run(
                    &format!("urn:li:dataProcessInstance:dag-{day}"),
                    &[event("STARTED", &dag_start, None)],
                ),
                None,
            );
        };
        let end = (at(&task_start) + chrono::Duration::minutes(run_minutes)).to_rfc3339();
        let dag_end = (at(&task_start) + chrono::Duration::minutes(run_minutes + 1)).to_rfc3339();
        (
            run(
                &format!("urn:li:dataProcessInstance:dag-{day}"),
                &[
                    event("STARTED", &dag_start, None),
                    event("COMPLETE", &dag_end, Some("SUCCESS")),
                ],
            ),
            Some(run(
                &format!("urn:li:dataProcessInstance:task-{day}"),
                &[
                    event("STARTED", &task_start, None),
                    event("COMPLETE", &end, Some("SUCCESS")),
                ],
            )),
        )
    }

    /// 2026-10-18: `silver_refresh_*` waited about 205 minutes for the spark pool and ran for none, and
    /// the screen called the run slow, because a run was measured from its DAG run's start.
    #[test]
    fn a_run_that_waited_for_its_pool_is_timed_from_its_run_task_and_is_not_slow() {
        let now = at("2026-01-06T12:00:00Z");
        let mut dag_runs = Vec::new();
        let mut task_runs = Vec::new();
        // Five ordinary days: a minute's wait and ten minutes' work. Then a run that waited three
        // hours for the pool and worked for ten minutes.
        for (number, start) in [
            (1, "07:21"),
            (2, "07:21"),
            (3, "07:21"),
            (4, "07:21"),
            (5, "07:21"),
            (6, "10:20"),
        ] {
            let (dag, task) = day(number, Some(start), 10);
            dag_runs.push(dag);
            task_runs.extend(task);
        }
        let flows = vec![flow(
            "synthetic_nightly",
            &[
                ("synthetic_nightly", dag_runs),
                ("synthetic_nightly.run", task_runs),
            ],
        )];
        let mut statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
        let nightly = &statuses[0];
        let last = nightly.last_run.as_ref();
        assert_eq!(
            last.and_then(|run| run.queued_at),
            Some(at("2026-01-06T07:20:00Z")),
            "the run was queued when the DAG started"
        );
        assert_eq!(
            last.and_then(|run| run.started_at),
            Some(at("2026-01-06T10:20:00Z")),
            "its work started when the run task did"
        );
        assert_eq!(
            nightly.typical_duration_seconds,
            Some(660),
            "a typical run is its work, not its wait"
        );

        let (timeouts, enabled_since, tables, failures, in_progress) = (
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
        );
        health::annotate(
            &mut statuses,
            &health::HealthFacts {
                timeouts: &timeouts,
                enabled_since: &enabled_since,
                tables: &tables,
                failures: &failures,
                in_progress: &in_progress,
                runs_readable: true,
                now,
            },
        );
        assert!(
            !statuses[0].problems.iter().any(|problem| matches!(
                problem,
                foundation_contracts::data_catalog::ScheduledJobProblem::Slow { .. }
            )),
            "a ten-minute run after a three-hour wait is not slow: {:?}",
            statuses[0].problems
        );
    }

    #[test]
    fn a_run_still_waiting_for_its_pool_has_no_start_yet() {
        let now = at("2026-01-01T09:00:00Z");
        let (dag, _) = day(1, None, 0);
        let flows = vec![flow(
            "synthetic_nightly",
            &[("synthetic_nightly", vec![dag])],
        )];
        let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
        let last = statuses[0].last_run.as_ref();
        assert_eq!(
            last.and_then(|run| run.queued_at),
            Some(at("2026-01-01T07:20:00Z"))
        );
        assert_eq!(
            last.and_then(|run| run.started_at),
            None,
            "its run task has not started"
        );
        assert_eq!(
            last.map(|run| run.outcome),
            Some(ScheduledJobRunOutcome::Running)
        );
    }

    #[test]
    fn each_job_takes_the_latest_run_of_its_own_flow_from_any_child_job() {
        let now = at("2026-01-02T08:00:00Z");
        let flows = vec![
            // The DAG reports itself and its task; the task's later run is the latest.
            flow(
                "synthetic_nightly",
                &[
                    (
                        "synthetic_nightly",
                        vec![run(
                            "urn:li:dataProcessInstance:dag-1",
                            &[
                                event("STARTED", "2026-01-01T07:20:00Z", None),
                                event("COMPLETE", "2026-01-01T07:30:00Z", Some("SUCCESS")),
                            ],
                        )],
                    ),
                    (
                        "synthetic_nightly.run",
                        vec![run(
                            "urn:li:dataProcessInstance:task-2",
                            &[
                                event("STARTED", "2026-01-02T07:20:00Z", None),
                                event("COMPLETE", "2026-01-02T07:25:00Z", Some("FAILURE")),
                            ],
                        )],
                    ),
                ],
            ),
            // Another system's flow with a name that only contains the job id is not this job.
            flow(
                "other_synthetic_nightly",
                &[(
                    "x",
                    vec![run(
                        "urn:li:dataProcessInstance:other",
                        &[event("COMPLETE", "2026-01-02T07:59:00Z", Some("SUCCESS"))],
                    )],
                )],
            ),
        ];
        let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
        let nightly = &statuses[0];
        assert_eq!(nightly.dag_id, "synthetic_nightly");
        let last = nightly.last_run.as_ref();
        assert_eq!(
            last.map(|r| (r.run_urn.as_str(), r.outcome)),
            Some((
                "urn:li:dataProcessInstance:task-2",
                ScheduledJobRunOutcome::Failed
            ))
        );
        assert_eq!(
            last.and_then(|r| r.started_at),
            Some(at("2026-01-02T07:20:00Z"))
        );
        assert_eq!(
            last.and_then(|r| r.finished_at),
            Some(at("2026-01-02T07:25:00Z"))
        );
        assert_eq!(
            last.and_then(|r| r.native_result.clone()),
            Some("failure".to_owned())
        );
        assert_eq!(
            nightly.catalog_job_urn.as_deref(),
            Some("urn:li:dataJob:(urn:li:dataFlow:(synthetic,synthetic_nightly,test),synthetic_nightly.run)")
        );
        assert_eq!(nightly.next_run_at, Some(at("2026-01-03T07:20:00Z")));

        let half_hourly = &statuses[1];
        assert_eq!(half_hourly.last_run, None, "no flow of its own: never ran");
        assert_eq!(
            half_hourly.next_run_at,
            Some(at("2026-01-02T08:30:00Z")),
            "the start at `now` itself is not next"
        );

        let off = &statuses[2];
        assert_eq!(off.next_run_at, None, "a job that is off has no next start");
        assert_eq!(
            off.disabled_reason.as_deref(),
            Some("waiting for a supervised run")
        );
    }

    #[test]
    fn a_start_after_a_completion_is_a_retry_running_again() {
        let (retrying, _) = run_from(&run(
            "urn:li:dataProcessInstance:retry",
            &[
                event("STARTED", "2026-01-02T07:20:00Z", None),
                event("COMPLETE", "2026-01-02T07:25:00Z", Some("UP_FOR_RETRY")),
                event("STARTED", "2026-01-02T07:30:00Z", None),
            ],
        ))
        .unwrap_or_else(|| unreachable!("the run has state changes"));
        assert_eq!(retrying.outcome, ScheduledJobRunOutcome::Running);
        assert_eq!(retrying.finished_at, None);
        assert_eq!(
            retrying.started_at,
            Some(at("2026-01-02T07:20:00Z")),
            "the first start"
        );

        assert_eq!(
            run_from(&run("urn:li:dataProcessInstance:empty", &[])),
            None,
            "a run with no state change is not a run to show"
        );
        let (strange, _) = run_from(&run(
            "urn:li:dataProcessInstance:odd",
            &[event(
                "COMPLETE",
                "2026-01-02T07:25:00Z",
                Some("SOMETHING_NEW"),
            )],
        ))
        .unwrap_or_else(|| unreachable!("the run has a state change"));
        assert_eq!(strange.outcome, ScheduledJobRunOutcome::Unknown);
    }

    /// A run of `minutes`, started at a fixed synthetic time, ending with `result`.
    fn took(urn: &str, minutes: i64, result: &str) -> JsonValue {
        let start = at("2026-01-01T07:20:00Z");
        let end = start + chrono::Duration::minutes(minutes);
        run(
            urn,
            &[
                event("STARTED", &start.to_rfc3339(), None),
                event("COMPLETE", &end.to_rfc3339(), Some(result)),
            ],
        )
    }

    fn typical_of(runs: &[JsonValue]) -> Option<(u64, u32)> {
        let parsed: Vec<ScheduledJobRun> =
            runs.iter().filter_map(run_from).map(|(r, _)| r).collect();
        typical_duration(parsed.iter())
    }

    #[test]
    fn the_typical_duration_is_the_median_of_succeeded_runs_with_both_times() {
        assert_eq!(
            typical_of(&[
                took("a", 10, "SUCCESS"),
                took("b", 30, "SUCCESS"),
                took("c", 20, "SUCCESS")
            ]),
            Some((20 * 60, 3)),
            "odd count: the middle one"
        );
        assert_eq!(
            typical_of(&[
                took("a", 10, "SUCCESS"),
                took("b", 40, "SUCCESS"),
                took("c", 20, "SUCCESS"),
                took("d", 25, "SUCCESS"),
            ]),
            Some((22 * 60 + 30, 4)),
            "even count: the mean of the two middle ones"
        );
        assert_eq!(
            typical_of(&[
                took("ok", 10, "SUCCESS"),
                took("failed", 90, "FAILURE"),
                took("retry", 90, "UP_FOR_RETRY"),
                took("skipped", 0, "SKIPPED"),
                run("running", &[event("STARTED", "2026-01-01T07:20:00Z", None)]),
            ]),
            Some((10 * 60, 1)),
            "failed, retried, skipped and running runs are not durations of the job"
        );
        assert_eq!(
            typical_of(&[
                run(
                    "no-start",
                    &[event("COMPLETE", "2026-01-01T07:30:00Z", Some("SUCCESS"))]
                ),
                took("ok", 5, "SUCCESS"),
            ]),
            Some((5 * 60, 1)),
            "a run without a recorded start has no duration"
        );
        assert_eq!(
            typical_of(&[took("failed", 10, "FAILURE")]),
            None,
            "no succeeded run: no typical duration"
        );
        assert_eq!(typical_of(&[]), None);
    }

    #[test]
    fn a_running_job_carries_the_typical_duration_of_its_own_catalog_job() {
        let now = at("2026-01-02T08:00:00Z");
        let flows = vec![flow(
            "synthetic_nightly",
            &[
                // The DAG reports the same runs as its task; they must not count twice.
                ("synthetic_nightly", vec![took("dag-1", 99, "SUCCESS")]),
                (
                    "synthetic_nightly.run",
                    vec![
                        run(
                            "urn:li:dataProcessInstance:now",
                            &[event("STARTED", "2026-01-02T07:20:00Z", None)],
                        ),
                        took("task-1", 30, "SUCCESS"),
                        took("task-2", 40, "SUCCESS"),
                        took("task-3", 50, "SUCCESS"),
                    ],
                ),
            ],
        )];
        let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
        let nightly = &statuses[0];
        assert_eq!(
            nightly.last_run.as_ref().map(|r| r.outcome),
            Some(ScheduledJobRunOutcome::Running)
        );
        assert_eq!(nightly.typical_duration_seconds, Some(40 * 60));
        assert_eq!(nightly.typical_duration_runs, 3);
        let never = &statuses[1];
        assert_eq!(
            (never.typical_duration_seconds, never.typical_duration_runs),
            (None, 0)
        );
    }

    #[test]
    fn a_contract_shows_each_check_with_its_latest_result_and_skips_other_datasets() {
        let dataset = json!({
            "urn": "urn:li:dataset:(urn:li:dataPlatform:odcs,silver.synthetic,PROD)",
            "name": "silver.synthetic",
            "platform": { "name": "odcs" },
            "properties": null,
            "assertions": { "assertions": [
                { "urn": "urn:li:assertion:schema", "info": { "type": "DATA_SCHEMA", "fieldAssertion": null },
                  "runEvents": { "runEvents": [
                    { "timestampMillis": ms("2026-01-02T07:21:00Z"), "result": { "type": "FAILURE",
                      "nativeResults": [{ "key": "missing_columns", "value": "row_digest" }] } } ] } },
                { "urn": "urn:li:assertion:kind", "info": { "type": "FIELD",
                    "fieldAssertion": { "fieldValuesAssertion": { "field": { "path": "kind" } } } },
                  "runEvents": { "runEvents": [
                    { "timestampMillis": ms("2026-01-02T07:20:00Z"), "result": { "type": "SUCCESS", "nativeResults": [] } } ] } },
                { "urn": "urn:li:assertion:never", "info": { "type": "FIELD",
                    "fieldAssertion": { "fieldValuesAssertion": { "field": { "path": "stage" } } } },
                  "runEvents": { "runEvents": [] } }
            ] }
        });
        let quality = contract_quality(&dataset).unwrap_or_else(|| unreachable!("a contract"));
        assert_eq!(quality.contract_id, "silver.synthetic");
        let by_urn = |urn: &str| quality.checks.iter().find(|c| c.assertion_urn == urn);
        let schema = by_urn("urn:li:assertion:schema");
        assert_eq!(schema.map(|c| c.result), Some(DataQualityResult::Failure));
        assert_eq!(
            schema.map(|c| c.details.clone()),
            Some(vec![DataQualityDetail {
                key: "missing_columns".to_owned(),
                value: "row_digest".to_owned()
            }])
        );
        assert_eq!(
            by_urn("urn:li:assertion:kind").map(|c| (c.result, c.field.clone())),
            Some((DataQualityResult::Success, Some("kind".to_owned())))
        );
        let never = by_urn("urn:li:assertion:never");
        assert_eq!(
            never.map(|c| (c.result, c.checked_at)),
            Some((DataQualityResult::NotRun, None))
        );

        let table =
            json!({ "urn": "urn:li:dataset:x", "name": "x", "platform": { "name": "iceberg" } });
        assert_eq!(contract_quality(&table), None, "only contract datasets");
    }

    #[test]
    fn the_real_job_list_reads_and_every_enabled_schedule_has_a_next_start() {
        let list = read_job_list(&jobs_path()).unwrap_or_else(|error| unreachable!("{error:?}"));
        assert!(!list.jobs.is_empty());
        let now = at("2026-01-02T08:00:00Z");
        let depends = dependencies(&list);
        for status in job_statuses(&list, &depends, &[], now) {
            assert!(status.dag_id.starts_with(&list.dag_id_prefix));
            if status.enabled {
                assert!(
                    status.next_run_at.is_some_and(|next| next > now),
                    "{}: schedule {} has no next start",
                    status.job_id,
                    status.schedule
                );
            }
        }
    }

    #[test]
    fn recent_runs_are_the_latest_jobs_own_runs_newest_first_and_bounded() {
        let now = at("2026-01-02T08:00:00Z");
        let start = at("2026-01-01T00:00:00Z");
        // Twelve hourly runs of the task, more than are shown, and one later run of the DAG
        // itself, which reports the same runs and must not be shown beside them.
        let task_runs: Vec<JsonValue> = (0..12)
            .map(|hour| {
                let began = start + chrono::Duration::hours(hour);
                run(
                    &format!("urn:li:dataProcessInstance:task-{hour:02}"),
                    &[
                        event("STARTED", &began.to_rfc3339(), None),
                        event(
                            "COMPLETE",
                            &(began + chrono::Duration::minutes(5)).to_rfc3339(),
                            Some("SUCCESS"),
                        ),
                    ],
                )
            })
            .collect();
        let flows = vec![flow(
            "synthetic_nightly",
            &[
                (
                    "synthetic_nightly",
                    vec![run(
                        "urn:li:dataProcessInstance:dag",
                        &[event("STARTED", "2026-01-01T09:00:00Z", None)],
                    )],
                ),
                ("synthetic_nightly.run", task_runs),
            ],
        )];
        let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
        let nightly = &statuses[0];
        assert_eq!(
            nightly.last_run.as_ref().map(|r| r.run_urn.as_str()),
            Some("urn:li:dataProcessInstance:task-11")
        );
        let shown: Vec<&str> = nightly
            .recent_runs
            .iter()
            .map(|r| r.run_urn.as_str())
            .collect();
        let expected: Vec<String> = (2..12)
            .rev()
            .map(|hour| format!("urn:li:dataProcessInstance:task-{hour:02}"))
            .collect();
        assert_eq!(shown, expected, "newest first, at most RUNS_PER_JOB");
        assert_eq!(
            nightly.recent_runs.first(),
            nightly.last_run.as_ref(),
            "the latest run leads"
        );
        assert!(statuses[1].recent_runs.is_empty(), "never ran: no runs");
    }

    #[test]
    fn each_job_carries_its_rendered_dependencies_and_how_it_starts() {
        let now = at("2026-01-02T08:00:00Z");
        let statuses = job_statuses(&list(), &dependencies(&list()), &[], now);
        assert_eq!(statuses[0].upstream_job_ids, vec!["half_hourly".to_owned()]);
        assert!(statuses[1].upstream_job_ids.is_empty());
        assert_eq!(statuses[2].started_by, ScheduledJobStart::Inputs);
    }

    #[test]
    fn the_real_dependency_file_is_rendered_from_the_real_job_list() {
        let list = read_job_list(&jobs_path()).unwrap_or_else(|error| unreachable!("{error:?}"));
        let depends = read_dependencies(&dependencies_path(), &list)
            .unwrap_or_else(|error| unreachable!("{error:?}"));
        let listed: Vec<&str> = list.jobs.iter().map(|job| job.id.as_str()).collect();
        for job in &depends {
            for producer in &job.producers {
                assert!(
                    listed.contains(&producer.as_str()) && producer != &job.id,
                    "{}: producer {producer} is another listed job",
                    job.id
                );
            }
            assert!(
                job.reads
                    .iter()
                    .chain(&job.writes)
                    .all(|d| !d.node_id.is_empty()),
                "{}: every dataset names its pipeline-graph node",
                job.id
            );
        }
        assert!(
            depends.iter().any(|job| !job.producers.is_empty()),
            "the real jobs feed each other"
        );
    }

    #[test]
    fn a_dependency_file_rendered_from_another_job_list_is_refused() {
        let directory = std::env::temp_dir().join(format!(
            "foundation-job-dependencies-{}",
            std::process::id()
        ));
        let path = directory.join("job-dependencies.v1.json");
        let written = std::fs::create_dir_all(&directory).and_then(|()| {
            std::fs::write(
                &path,
                json!({
                    "schema_version": DEPENDENCIES_SCHEMA,
                    "run_task_id": "run",
                    "jobs": [{ "id": "nightly", "producers": [], "reads": [], "writes": [] }],
                })
                .to_string(),
            )
        });
        assert!(written.is_ok(), "{written:?}");
        let refused = read_dependencies(&path, &list());
        let _ = std::fs::remove_dir_all(&directory);
        assert!(
            matches!(refused, Err(ApiError::Internal(ref message)) if message.contains("another job list")),
            "{refused:?}"
        );
    }
}
