//! How the scheduled jobs ran and what the data contracts' checks found (root ADR-0165).
//!
//! Both facts already live in the data catalog: every Airflow run reports to it by `OpenLineage`
//! (root ADR-0118 §2), and the quality job reports each contract check's result on its assertion
//! (root ADR-0117 §6, ADR-0123). Foundation reads them back with fixed questions, as it does for
//! search and lineage (root ADR-0119), and adds only what the catalog does not hold: the job list
//! itself (`orchestration/jobs.v1.json`) and each job's next start, computed from its schedule
//! there.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use axum::{extract::State, Json};
use chrono::{DateTime, TimeZone, Utc};
use croner::Cron;
use foundation_contracts::data_catalog::{
    DataContractQuality, DataQualityCheck, DataQualityDetail, DataQualityResult, DataQualityStatus,
    ScheduledJobRun, ScheduledJobRunOutcome, ScheduledJobStatus, ScheduledJobsStatus,
};
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use super::{ask, text};
use crate::routes::api_error::ApiError;
use crate::state::AppState;

const JOBS_PATH_ENV: &str = "FOUNDATION_PLATFORM_SCHEDULED_JOBS_PATH";
const DEFAULT_JOBS_PATH: &str = "orchestration/jobs.v1.json";
/// Search results per catalog page.
const PAGE: u64 = 100;
/// Pages read at most. A catalog with more flows or contracts than this is reported, never cut
/// short silently: a job past the cut would otherwise look as if it had never run.
const MAX_PAGES: u64 = 50;
/// Child jobs of one flow (an Airflow DAG reports itself and its one task) and runs per child read.
const CHILD_JOBS: u64 = 10;
const RUNS_PER_JOB: u64 = 5;

/// The part of the job list this screen reads.
#[derive(Debug, Deserialize)]
struct JobList {
    dag_id_prefix: String,
    jobs: Vec<ListedJob>,
}

#[derive(Debug, Deserialize)]
struct ListedJob {
    id: String,
    description: String,
    schedule: String,
    enabled: bool,
    #[serde(default)]
    disabled_reason: Option<String>,
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
    let list = read_job_list(&jobs_path())?;
    let now = Utc::now();
    let (flows, run_history_error) = match catalog_flows().await {
        Ok(flows) => (flows, None),
        Err(error) => (Vec::new(), Some(unread_runs_reason(error))),
    };
    Ok(Json(ScheduledJobsStatus {
        read_at: now,
        jobs: job_statuses(&list, &flows, now),
        run_history_error,
    }))
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
fn unread_runs_reason(error: ApiError) -> String {
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

/// Each listed job with its next start and the latest run among the catalog flows named for it.
fn job_statuses(
    list: &JobList,
    flows: &[JsonValue],
    now: DateTime<Utc>,
) -> Vec<ScheduledJobStatus> {
    list.jobs
        .iter()
        .map(|job| {
            let dag_id = format!("{}{}", list.dag_id_prefix, job.id);
            let latest = flows
                .iter()
                .filter(|flow| flow["flowId"].as_str() == Some(dag_id.as_str()))
                .flat_map(child_runs)
                .max_by_key(|(_, run, last_event)| (*last_event, run.run_urn.clone()));
            let (catalog_job_urn, last_run) =
                latest.map_or((None, None), |(job_urn, run, _)| (Some(job_urn), Some(run)));
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
            }
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
            started_at,
            finished_at: complete.then(|| millis(last_event)).flatten(),
            outcome,
            native_result: complete
                .then(|| text(&result["nativeResultType"]))
                .flatten(),
        },
        last_event,
    ))
}

fn millis(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
}

/// Every Airflow-style flow in the catalog with its child jobs and their latest runs.
async fn catalog_flows() -> Result<Vec<JsonValue>, ApiError> {
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
            state(limit: 20) { status timestampMillis result { resultType nativeResultType } } } } } } } } } } } }";

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

    fn list() -> JobList {
        JobList {
            dag_id_prefix: "synthetic_".to_owned(),
            jobs: vec![
                ListedJob {
                    id: "nightly".to_owned(),
                    description: "a nightly job".to_owned(),
                    schedule: "20 7 * * *".to_owned(),
                    enabled: true,
                    disabled_reason: None,
                },
                ListedJob {
                    id: "half_hourly".to_owned(),
                    description: "a half-hourly job".to_owned(),
                    schedule: "0,30 * * * *".to_owned(),
                    enabled: true,
                    disabled_reason: None,
                },
                ListedJob {
                    id: "off".to_owned(),
                    description: "a job that is off".to_owned(),
                    schedule: "40 4 * * *".to_owned(),
                    enabled: false,
                    disabled_reason: Some("waiting for a supervised run".to_owned()),
                },
            ],
        }
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
        let statuses = job_statuses(&list(), &flows, now);
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
        for status in job_statuses(&list, &[], now) {
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
}
