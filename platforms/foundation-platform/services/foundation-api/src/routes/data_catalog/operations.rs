//! How the scheduled jobs ran and what the data contracts' checks found (root ADR-0165).
//!
//! The runs live in the orchestrator that ran them (root ADR-0609 §1): a job Dagster runs
//! (`dagster_stage` `proxied` or `dagster`) has the runs of its Dagster job, read from Dagster's
//! read-only webserver, and a job Airflow still runs has its DAG's runs, until Airflow goes
//! (`facts.rs`). The contract checks' results live in Dagster too: the quality job reports each rule
//! as an asset check the code location declares from the contract files (root ADR-0609 §2), and
//! this reads their newest evaluations. Foundation reads both with fixed questions (root ADR-0119),
//! and adds only what they do not hold: the job list itself (`orchestration/jobs.v1.json`) and each
//! job's next start, computed from its schedule there, and how long each job usually takes, from
//! the durations of its recent succeeded runs.
//!
//! What each job reads, writes and waits on is not derived here: `orchestration/dags/job_specs.py`
//! derives it for the scheduler and renders it to `orchestration/job-dependencies.v1.json`, which a
//! test keeps equal to that derivation; this reads the rendered file (root ADR-0188).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use croner::Cron;
use foundation_contracts::data_catalog::{
    DataContractQuality, DataQualityCheck, DataQualityDetail, DataQualityResult, DataQualityStatus,
    OperationsLinks, ScheduledJobDataset, ScheduledJobRun, ScheduledJobRunOutcome,
    ScheduledJobScheduler, ScheduledJobStart, ScheduledJobStatus, ScheduledJobsStatus,
};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::{facts, health};
use crate::routes::api_error::ApiError;
use crate::state::AppState;

const JOBS_PATH_ENV: &str = "FOUNDATION_PLATFORM_SCHEDULED_JOBS_PATH";
const DEFAULT_JOBS_PATH: &str = "orchestration/jobs.v1.json";
const DEPENDENCIES_PATH_ENV: &str = "FOUNDATION_PLATFORM_JOB_DEPENDENCIES_PATH";
const DEFAULT_DEPENDENCIES_PATH: &str = "orchestration/job-dependencies.v1.json";
const DEPENDENCIES_SCHEMA: &str = "foundation-platform.orchestration_job_dependencies.v1";
const SCHEDULER_UI_URL_ENV: &str = "FOUNDATION_PLATFORM_AIRFLOW_UI_URL";
const CATALOG_UI_URL_ENV: &str = "FOUNDATION_PLATFORM_DATAHUB_UI_URL";
const DAGSTER_UI_URL_ENV: &str = "FOUNDATION_PLATFORM_DAGSTER_UI_URL";
/// The runs read of each job: the ones its typical duration is the median of, and the recent runs
/// it shows.
pub(super) const RUNS_PER_JOB: usize = 10;

/// The part of the job list this screen reads.
#[derive(Debug, Deserialize)]
pub(super) struct JobList {
    dag_id_prefix: String,
    jobs: Vec<ListedJob>,
}

impl JobList {
    /// Each job's limit for one scheduled start in minutes, by job id: how long it may wait for
    /// the host, its run's time limit and its stop grace (root ADR-0270 §1, §2b;
    /// `job_specs.scheduler_timeout_minutes` adds the same three).
    pub(super) fn timeouts(&self) -> BTreeMap<String, u32> {
        self.jobs
            .iter()
            .map(|job| {
                (
                    job.id.clone(),
                    job.wait_limit_minutes
                        .saturating_add(job.time_limit_minutes)
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

    /// The jobs Dagster runs, by id, and the jobs Airflow runs, each with its DAG id and whether it
    /// is switched on: where each job's runs are read.
    pub(super) fn by_runner(&self) -> (Vec<&str>, Vec<(&str, String, bool)>) {
        let (dagster, airflow): (Vec<&ListedJob>, Vec<&ListedJob>) = self
            .jobs
            .iter()
            .partition(|job| job.decided_by() == ScheduledJobScheduler::Dagster);
        (
            dagster.iter().map(|job| job.id.as_str()).collect(),
            airflow
                .iter()
                .map(|job| {
                    let dag_id = format!("{}{}", self.dag_id_prefix, job.id);
                    (job.id.as_str(), dag_id, job.enabled)
                })
                .collect(),
        )
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
    /// How long one scheduled start may wait for the host before its run begins (root ADR-0270
    /// §2b). Required like the two above.
    wait_limit_minutes: u32,
    enabled: bool,
    /// When the job was switched on (root ADR-0239): a slot before it is not one it missed.
    #[serde(default)]
    enabled_since: Option<DateTime<Utc>>,
    #[serde(default)]
    disabled_reason: Option<String>,
    started_by: ScheduledJobStart,
    pipeline_graph_edges: Vec<String>,
    /// Which orchestrator decides and runs it (root ADR-0431 §5, ADR-0565): absent is `airflow`.
    /// From `proxied` on Dagster runs it, so its runs and their logs are Dagster's (root ADR-0609).
    #[serde(default)]
    dagster_stage: DagsterStage,
}

/// A job's `dagster_stage` in the job list.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum DagsterStage {
    #[default]
    Airflow,
    Shadow,
    Proxied,
    Dagster,
}

impl ListedJob {
    /// The orchestrator that runs it: a proxied job's DAG still starts it, but its run is Dagster's.
    const fn decided_by(&self) -> ScheduledJobScheduler {
        if matches!(
            self.dagster_stage,
            DagsterStage::Proxied | DagsterStage::Dagster
        ) {
            ScheduledJobScheduler::Dagster
        } else {
            ScheduledJobScheduler::Airflow
        }
    }
}

/// `orchestration/job-dependencies.v1.json`, rendered by `job_specs.py`.
#[derive(Debug, Deserialize)]
struct DependencyDocument {
    schema_version: String,
    jobs: Vec<JobDependencies>,
}

#[derive(Debug, Deserialize)]
pub(super) struct JobDependencies {
    id: String,
    producers: Vec<String>,
    pub(super) reads: Vec<ScheduledJobDataset>,
    pub(super) writes: Vec<ScheduledJobDataset>,
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

/// Every scheduled job with its runs and problems, from the kept read of the stores.
pub(super) async fn jobs_status() -> Result<ScheduledJobsStatus, ApiError> {
    let (list, dependencies) = read_registry()?;
    let facts = facts::current(Arc::clone(&list), Arc::clone(&dependencies)).await;
    let now = Utc::now();
    let mut jobs = job_statuses(&list, &dependencies, &facts.runs.by_job, now);
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
            runs_unread: &facts.runs.unread,
            now,
        },
    );
    Ok(ScheduledJobsStatus {
        read_at: now,
        jobs,
        run_history_error: facts.runs.error.clone(),
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
        dagster_url: value(DAGSTER_UI_URL_ENV),
    }
}

/// Every data contract's checks and the latest result of each, as Dagster holds them.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/quality-checks",
    operation_id = "listDataQualityChecks",
    responses(
        (status = 200, body = DataQualityStatus),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the data catalog"),
        (status = 503, description = "Dagster is not configured or not reachable", body = ApiErrorResponse),
    )
)]
pub(super) async fn quality_checks(
    State(_state): State<Arc<AppState>>,
) -> Result<Json<DataQualityStatus>, ApiError> {
    let dagster = facts::dagster::Dagster::from_env()
        .ok_or_else(|| ApiError::Unavailable("Dagster is not configured".to_owned()))?;
    let checks = dagster
        .contract_checks()
        .await
        .map_err(ApiError::Unavailable)?;
    Ok(Json(DataQualityStatus {
        read_at: Utc::now(),
        contracts: contract_quality(&checks),
    }))
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
    Ok(document.jobs)
}

/// Whether `job_id` is a job the release's job list names (enabled or not).
///
/// # Errors
/// Returns [`ApiError::Internal`] when the job list cannot be read.
pub(super) fn is_listed_job(job_id: &str) -> Result<bool, ApiError> {
    Ok(read_job_list(&jobs_path())?
        .jobs
        .iter()
        .any(|job| job.id == job_id))
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
/// its recent runs (`runs` by job id, newest first, as the orchestrator that runs it holds them).
pub(super) fn job_statuses(
    list: &JobList,
    dependencies: &[JobDependencies],
    runs: &BTreeMap<String, Vec<ScheduledJobRun>>,
    now: DateTime<Utc>,
) -> Vec<ScheduledJobStatus> {
    list.jobs
        .iter()
        .zip(dependencies)
        .map(|(job, depends)| {
            let dag_id = format!("{}{}", list.dag_id_prefix, job.id);
            let recent_runs: Vec<ScheduledJobRun> = runs
                .get(&job.id)
                .map(|runs| runs.iter().take(RUNS_PER_JOB).cloned().collect())
                .unwrap_or_default();
            let typical = typical_duration(recent_runs.iter());
            let decided_by = job.decided_by();
            // The orchestrator's job that holds the runs, once one ran.
            let catalog_job_urn = (!recent_runs.is_empty()).then(|| match decided_by {
                ScheduledJobScheduler::Dagster => {
                    format!("{}{}", facts::dagster::JOB_PREFIX, job.id)
                }
                ScheduledJobScheduler::Airflow => dag_id.clone(),
            });
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
                last_run: recent_runs.first().cloned(),
                typical_duration_seconds: typical.map(|(seconds, _)| seconds),
                typical_duration_runs: typical.map_or(0, |(_, count)| count),
                recent_runs,
                started_by: job.started_by,
                decided_by,
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

/// The next time a five-field cron schedule starts a job after `now`, in UTC as Airflow reads it.
fn next_start(schedule: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let cron = Cron::from_str(schedule).ok()?;
    cron.find_next_occurrence(&now, false).ok()
}

/// The contract checks grouped by contract, by id, each with its newest result. A contract is its
/// table: the asset key after its platform (`iceberg/gold/unit_price` is `gold.unit_price`).
fn contract_quality(checks: &[facts::dagster::ContractCheck]) -> Vec<DataContractQuality> {
    let mut contracts: BTreeMap<String, DataContractQuality> = BTreeMap::new();
    for check in checks {
        let contract_id = check.asset_key.get(1..).unwrap_or_default().join(".");
        contracts
            .entry(contract_id.clone())
            .or_insert_with(|| DataContractQuality {
                contract_id,
                asset_key: check.asset_key.join("/"),
                checks: Vec::new(),
            })
            .checks
            .push(quality_check(check));
    }
    contracts
        .into_values()
        .map(|mut contract| {
            contract
                .checks
                .sort_by(|a, b| a.check_name.cmp(&b.check_name));
            contract
        })
        .collect()
}

/// A contract check's allowed-values rule names its column after this.
const ALLOWED_VALUES_CHECK: &str = "contract_allowed_values";
/// The metadata of a check run on its own in Dagster: a re-statement of the newest report, empty when
/// the quality job has reported none yet (`orchestration/dagster/takeover_checks.py` `RESTATED_FROM`).
const RESTATED_FROM: &str = "restated_from_run";

/// A check's result, from its newest executions (newest first): the newest one with an evaluation; a
/// newest execution that failed to run is `Error`; a re-statement with nothing reported yet, or no
/// evaluation at all, is `NotRun`.
fn check_result(executions: &[JsonValue]) -> (DataQualityResult, Option<&JsonValue>) {
    let judged = executions
        .iter()
        .find(|execution| execution["evaluation"].is_object());
    if executions
        .first()
        .is_some_and(|newest| newest["status"].as_str() == Some("EXECUTION_FAILED"))
    {
        return (DataQualityResult::Error, judged);
    }
    let Some(execution) = judged else {
        return (DataQualityResult::NotRun, None);
    };
    let evaluation = &execution["evaluation"];
    let nothing_reported = evaluation["metadataEntries"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|entry| {
            entry["label"].as_str() == Some(RESTATED_FROM) && entry["text"].as_str() == Some("")
        });
    if nothing_reported {
        return (DataQualityResult::NotRun, None);
    }
    let result = if evaluation["success"].as_bool() == Some(true) {
        DataQualityResult::Success
    } else {
        DataQualityResult::Failure
    };
    (result, Some(execution))
}

fn quality_check(check: &facts::dagster::ContractCheck) -> DataQualityCheck {
    let executions = check
        .executions
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let (result, judged) = check_result(executions);
    let evaluation = judged.map_or(&JsonValue::Null, |execution| &execution["evaluation"]);
    let field = check
        .check_name
        .strip_prefix(ALLOWED_VALUES_CHECK)
        .and_then(|rest| rest.strip_prefix('_'))
        .map(str::to_owned);
    let kind = if field.is_some() {
        ALLOWED_VALUES_CHECK.to_owned()
    } else {
        check.check_name.clone()
    };
    // The severity first (WARN while the contract only reports, ERROR once enforced), then what
    // the quality job reported, as it wrote it.
    let details = evaluation["severity"]
        .as_str()
        .map(|severity| DataQualityDetail {
            key: "severity".to_owned(),
            value: severity.to_owned(),
        })
        .into_iter()
        .chain(
            evaluation["metadataEntries"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    let value = entry["text"]
                        .as_str()
                        .or_else(|| entry["intRepr"].as_str())?;
                    Some(DataQualityDetail {
                        key: entry["label"].as_str()?.to_owned(),
                        value: value.to_owned(),
                    })
                }),
        )
        .collect();
    DataQualityCheck {
        check_name: check.check_name.clone(),
        kind,
        field,
        result,
        checked_at: judged.and_then(facts::dagster::execution_time),
        details,
    }
}

#[cfg(test)]
mod tests;
