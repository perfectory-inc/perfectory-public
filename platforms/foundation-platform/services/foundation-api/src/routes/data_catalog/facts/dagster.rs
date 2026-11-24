//! The run facts of the jobs Dagster runs (root ADR-0565 §8, ADR-0609 §1), read from Dagster's
//! read-only webserver (`dagster-webserver --read-only`, `compose.orchestration.yml`): it answers the
//! same GraphQL as the operating one and holds no mutation, so the API can ask it and change nothing.
//!
//! A job at `proxied` or `dagster` runs as its Dagster job (`foundation_job__<id>`): its runs are that
//! job's runs, whether its sensor or (while `proxied`) its DAG's `run` task launched them. Its run op
//! logs every line the host's forced command streams (`orchestration/dagster/systemd_pipes.py`), so a
//! failed run's lines are that run's message events, framed by `start-scheduled-job` as an Airflow
//! task log was, and the same `classify` and `mask` read them.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::{DateTime, TimeZone, Utc};
use foundation_contracts::data_catalog::{ScheduledJobRun, ScheduledJobRunOutcome};
use serde_json::{json, Value as JsonValue};

use super::super::health::FailedRunLog;
use super::super::operations::RUNS_PER_JOB;
use super::{classify, waiting_reason};

/// The read-only webserver's address inside the runtime.
const API_URL_ENV: &str = "FOUNDATION_PLATFORM_DAGSTER_API_URL";
/// A job's Dagster job (`orchestration/dags/job_specs.py` `DAGSTER_JOB_PREFIX`).
pub(in crate::routes::data_catalog) const JOB_PREFIX: &str = "foundation_job__";
/// The tag a run's outcome is on (`job_specs.OUTCOME_TAG`): a run that ended waiting, or skipped,
/// succeeded (`run_jobs.py`), as Airflow skipped the run task of one (root ADR-0260, ADR-0282).
const OUTCOME_TAG: &str = "foundation/job-outcome";
/// Message events per page of a run's log, and the most pages read: a run's journal is thousands of
/// lines, never this many.
const LOG_PAGE: u32 = 2_000;
const LOG_PAGES: u32 = 25;
const UNFINISHED: &str = "[QUEUED, NOT_STARTED, STARTING, STARTED, CANCELING]";

const RUNS_QUERY: &str = "query($filter: RunsFilter!, $limit: Int!) {
  runsOrError(filter: $filter, limit: $limit) {
    __typename ... on Runs { results { runId jobName creationTime } } } }";

const LOG_QUERY: &str = "query($runId: ID!, $after: String, $limit: Int!) {
  logsForRun(runId: $runId, afterCursor: $after, limit: $limit) {
    __typename ... on EventConnection { cursor hasMore events { __typename ... on MessageEvent { message } } } } }";

/// The data contracts' asset checks (`orchestration/dagster/contract_checks.py`, root ADR-0609 §2)
/// are the checks whose names start so.
const CONTRACT_CHECK_PREFIX: &str = "contract_";

/// Every asset and the names of its checks.
const ASSET_CHECKS_QUERY: &str = "{ assetNodes { assetKey { path }
  assetChecksOrError { __typename ... on AssetChecks { checks { name } } } } }";

/// The newest executions of a check read: the newest one with an evaluation is its result, and a
/// planned or running one ahead of it does not hide it.
const EXECUTIONS: u32 = 5;

/// One contract check and its newest executions, as Dagster holds them.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::routes::data_catalog) struct ContractCheck {
    /// The checked asset's key, platform first (`iceberg`, `gold`, `unit_price`).
    pub(in crate::routes::data_catalog) asset_key: Vec<String>,
    pub(in crate::routes::data_catalog) check_name: String,
    /// Its newest executions, newest first (`status`, `timestamp`, `evaluation`); none when it never ran.
    pub(in crate::routes::data_catalog) executions: JsonValue,
}

pub(in crate::routes::data_catalog) struct Dagster {
    api: String,
}

impl Dagster {
    pub(in crate::routes::data_catalog) fn from_env() -> Option<Self> {
        std::env::var(API_URL_ENV)
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_owned())
            .filter(|value| !value.is_empty())
            .map(|api| Self { api })
    }

    async fn ask(&self, query: &str, variables: JsonValue) -> Result<JsonValue, String> {
        let response = super::super::http()
            .post(format!("{}/graphql", self.api))
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .map_err(|_| "Dagster is not reachable".to_owned())?;
        if !response.status().is_success() {
            return Err(format!("Dagster answered {}", response.status()));
        }
        let answer: JsonValue = response
            .json()
            .await
            .map_err(|_| "Dagster's answer is not JSON".to_owned())?;
        if let Some(error) = answer["errors"][0]["message"].as_str() {
            return Err(format!("Dagster refused the query: {error}"));
        }
        Ok(answer["data"].clone())
    }

    /// By job id, when its newest unfinished Dagster run was made.
    pub(super) async fn in_progress(&self) -> Result<BTreeMap<String, DateTime<Utc>>, String> {
        let data = self
            .ask(
                RUNS_QUERY,
                json!({ "filter": { "statuses": unfinished() }, "limit": 200 }),
            )
            .await?;
        Ok(in_progress_runs(&data))
    }

    /// By job id, the newest runs of each job's Dagster job, newest first: one question for all.
    pub(super) async fn job_runs(
        &self,
        job_ids: &[&str],
    ) -> Result<BTreeMap<String, Vec<ScheduledJobRun>>, String> {
        if job_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let data = self.ask(&runs_query(job_ids), json!({})).await?;
        runs_of(job_ids, &data)
    }

    /// The log of the Dagster run `run_id`, which failed or ended waiting.
    pub(super) async fn log_of(&self, run_id: &str) -> Result<FailedRunLog, String> {
        let mut lines = Vec::new();
        let mut after = JsonValue::Null;
        for _ in 0..LOG_PAGES {
            let page = self
                .ask(
                    LOG_QUERY,
                    json!({ "runId": run_id, "after": after, "limit": LOG_PAGE }),
                )
                .await?;
            let connection = &page["logsForRun"];
            if connection["__typename"].as_str() != Some("EventConnection") {
                return Err(format!("Dagster holds no log of run {run_id}"));
            }
            lines.extend(message_lines(connection));
            if connection["hasMore"].as_bool() != Some(true) {
                break;
            }
            after = connection["cursor"].clone();
        }
        let (cause, cause_line) = classify(&lines);
        Ok(FailedRunLog {
            run_id: run_id.to_owned(),
            cause,
            cause_line,
            waiting: waiting_reason(&lines),
        })
    }

    /// Every contract check and its newest executions: the checks Dagster declares, then one question
    /// for all their executions. No contract check at all is the code location not loaded (its
    /// assets are then not Dagster's), never "no contracts".
    pub(in crate::routes::data_catalog) async fn contract_checks(
        &self,
    ) -> Result<Vec<ContractCheck>, String> {
        let nodes = self.ask(ASSET_CHECKS_QUERY, json!({})).await?;
        let mut checks = contract_checks_of(&nodes)?;
        if checks.is_empty() {
            return Err(
                "Dagster declares no contract check: its code location is not loaded".to_owned(),
            );
        }
        let data = self.ask(&executions_query(&checks), json!({})).await?;
        for (index, check) in checks.iter_mut().enumerate() {
            check.executions = data[format!("c{index}")].clone();
        }
        Ok(checks)
    }
}

/// The contract checks of an `assetNodes` answer, by asset key and name, none run yet; an asset whose
/// checks Dagster could not list is refused, so a check is never missing from the answer unseen.
fn contract_checks_of(data: &JsonValue) -> Result<Vec<ContractCheck>, String> {
    let mut checks = Vec::new();
    for node in data["assetNodes"].as_array().into_iter().flatten() {
        let listed = node["assetChecksOrError"]["__typename"].as_str();
        if listed != Some("AssetChecks") {
            return Err(format!(
                "Dagster could not list the checks of an asset: {}",
                listed.unwrap_or("no answer")
            ));
        }
        let asset_key: Vec<String> = node["assetKey"]["path"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|part| part.as_str().map(str::to_owned))
            .collect();
        if asset_key.is_empty() {
            continue;
        }
        for check in node["assetChecksOrError"]["checks"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let Some(name) = check["name"]
                .as_str()
                .filter(|name| name.starts_with(CONTRACT_CHECK_PREFIX))
            {
                checks.push(ContractCheck {
                    asset_key: asset_key.clone(),
                    check_name: name.to_owned(),
                    executions: JsonValue::Null,
                });
            }
        }
    }
    checks.sort_by(|a, b| (&a.asset_key, &a.check_name).cmp(&(&b.asset_key, &b.check_name)));
    Ok(checks)
}

/// One query for the newest executions of every check, each under the alias `c<index>`.
fn executions_query(checks: &[ContractCheck]) -> String {
    let mut query = String::from("{");
    for (index, check) in checks.iter().enumerate() {
        let _ = write!(
            query,
            " c{index}: assetCheckExecutions(assetKey: {{path: {}}}, checkName: {}, limit: {EXECUTIONS}) \
             {{ status timestamp evaluation {{ success severity metadataEntries {{ label \
             ... on TextMetadataEntry {{ text }} ... on IntMetadataEntry {{ intRepr }} }} }} }}",
            json!(check.asset_key),
            json!(check.check_name)
        );
    }
    query.push_str(" }");
    query
}

/// One query for the newest runs of every job's Dagster job, each under the alias `j<index>`.
fn runs_query(job_ids: &[&str]) -> String {
    let mut query = String::from("{");
    for (index, job_id) in job_ids.iter().enumerate() {
        let _ = write!(
            query,
            " j{index}: runsOrError(filter: {{pipelineName: {}}}, limit: {RUNS_PER_JOB}) \
             {{ __typename ... on Runs {{ results {{ runId status creationTime startTime endTime \
             tags {{ key value }} }} }} }}",
            json!(format!("{JOB_PREFIX}{job_id}"))
        );
    }
    query.push_str(" }");
    query
}

/// Each job's runs in a `runs_query` answer; a job whose runs Dagster could not list refuses the
/// whole answer, so no job reads as never run because of it.
fn runs_of(
    job_ids: &[&str],
    data: &JsonValue,
) -> Result<BTreeMap<String, Vec<ScheduledJobRun>>, String> {
    job_ids
        .iter()
        .enumerate()
        .map(|(index, job_id)| {
            let answer = &data[format!("j{index}")];
            let listed = answer["__typename"].as_str();
            if listed != Some("Runs") {
                return Err(format!(
                    "Dagster could not list the runs of {JOB_PREFIX}{job_id}: {}",
                    listed.unwrap_or("no answer")
                ));
            }
            let runs = answer["results"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(run_of)
                .collect();
            Ok(((*job_id).to_owned(), runs))
        })
        .collect()
}

/// One Dagster run: made (`creationTime`) when it was queued, started when its op could start (after
/// its pool's wait), ended when its last try did. An op tries again inside the same run, so a run
/// waiting for its next try is still running. A run that succeeded saying it waits, or that it was
/// skipped, is a success here as the data catalog showed it (its word stays in `native_result`): on
/// 2026-11-23 counting those as no success would have called 19 more jobs late (15 to 34), the Gold
/// builds that wait for their inputs every day among them.
fn run_of(run: &JsonValue) -> Option<ScheduledJobRun> {
    let status = run["status"].as_str()?;
    let outcome_tag = run["tags"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|tag| tag["key"].as_str() == Some(OUTCOME_TAG))
        .and_then(|tag| tag["value"].as_str())
        .filter(|outcome| !outcome.is_empty());
    let outcome = match status {
        "SUCCESS" => ScheduledJobRunOutcome::Succeeded,
        // Cancelled: stopped before its work ended (its time limit, or the DAG that launched it was
        // stopped), which is no success.
        "FAILURE" | "CANCELED" => ScheduledJobRunOutcome::Failed,
        "QUEUED" | "NOT_STARTED" | "STARTING" | "STARTED" | "CANCELING" => {
            ScheduledJobRunOutcome::Running
        }
        _ => ScheduledJobRunOutcome::Unknown,
    };
    let finished = matches!(status, "SUCCESS" | "FAILURE" | "CANCELED");
    Some(ScheduledJobRun {
        run_urn: run["runId"].as_str()?.to_owned(),
        queued_at: epoch(&run["creationTime"]),
        started_at: epoch(&run["startTime"]),
        finished_at: finished.then(|| epoch(&run["endTime"])).flatten(),
        outcome,
        native_result: finished.then(|| {
            outcome_tag.map_or_else(|| status.to_owned(), |said| format!("{status} {said}"))
        }),
        rows_written: None,
    })
}

/// When an execution started, as Dagster gives it.
pub(in crate::routes::data_catalog) fn execution_time(
    execution: &JsonValue,
) -> Option<DateTime<Utc>> {
    epoch(&execution["timestamp"])
}

fn unfinished() -> JsonValue {
    UNFINISHED
        .trim_matches(['[', ']'])
        .split(", ")
        .map(|status| JsonValue::String(status.to_owned()))
        .collect()
}

/// By job id, the newest `creationTime` of the jobs' own unfinished runs in a `runsOrError` answer.
fn in_progress_runs(data: &JsonValue) -> BTreeMap<String, DateTime<Utc>> {
    let mut runs: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
    for run in data["runsOrError"]["results"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let job = run["jobName"]
            .as_str()
            .and_then(|name| name.strip_prefix(JOB_PREFIX));
        let made = epoch(&run["creationTime"]);
        let (Some(job), Some(made)) = (job, made) else {
            continue;
        };
        let entry = runs.entry(job.to_owned()).or_insert(made);
        *entry = (*entry).max(made);
    }
    runs
}

/// A time Dagster gives as seconds since the epoch with a fraction (`1800000600.5`), read from the
/// number's text so no float is cast.
fn epoch(value: &JsonValue) -> Option<DateTime<Utc>> {
    let text = value.as_number()?.to_string();
    let (seconds, fraction) = text.split_once('.').unwrap_or((&text, ""));
    let digits: String = fraction
        .chars()
        .chain(std::iter::repeat('0'))
        .take(9)
        .collect();
    Utc.timestamp_opt(seconds.parse().ok()?, digits.parse().ok()?)
        .single()
}

/// The lines of one page of a run's message events, in order.
fn message_lines(connection: &JsonValue) -> Vec<String> {
    connection["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| event["message"].as_str())
        .flat_map(|message| message.lines().map(str::to_owned).collect::<Vec<_>>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        contract_checks_of, executions_query, in_progress_runs, message_lines, runs_of, runs_query,
        unfinished, ContractCheck,
    };
    use foundation_contracts::data_catalog::ScheduledJobRunOutcome;
    use serde_json::{json, Value as JsonValue};

    #[test]
    fn every_jobs_newest_runs_are_asked_in_one_query() {
        let query = runs_query(&["synthetic_load", "synthetic_bake"]);
        assert!(query.starts_with("{ j0: runsOrError(filter: {pipelineName: \"foundation_job__synthetic_load\"}, limit: 10) { __typename ... on Runs { results { runId status creationTime startTime endTime tags { key value } } } }"));
        assert!(query.contains(" j1: runsOrError(filter: {pipelineName: \"foundation_job__synthetic_bake\"}, limit: 10)"));
        assert!(query.ends_with(" }"));
    }

    /// The shape of Dagster's `runsOrError` (1.13), ids made up.
    fn synthetic_runs() -> JsonValue {
        let tag = |outcome: &str| json!([{ "key": "foundation/run", "value": "job" }, { "key": "foundation/job-outcome", "value": outcome }]);
        json!({ "j0": { "__typename": "Runs", "results": [
            // Waiting for its pool: made, not started.
            { "runId": "r-queued", "status": "QUEUED", "creationTime": 1_800_007_200.0, "startTime": null, "endTime": null, "tags": [] },
            // Its op failed a try and waits for the next: the run is still going, from its first start.
            { "runId": "r-retrying", "status": "STARTED", "creationTime": 1_800_003_600.0, "startTime": 1_800_003_660.5, "endTime": null, "tags": tag("failed") },
            { "runId": "r-waited", "status": "SUCCESS", "creationTime": 1_800_000_000.0, "startTime": 1_800_000_010.0, "endTime": 1_800_000_020.0, "tags": tag("waiting") },
            { "runId": "r-changed", "status": "SUCCESS", "creationTime": 1_799_996_400.0, "startTime": 1_799_996_401.0, "endTime": 1_799_997_000.0, "tags": tag("changed") },
            { "runId": "r-failed", "status": "FAILURE", "creationTime": 1_799_992_800.0, "startTime": 1_799_992_801.0, "endTime": 1_799_993_000.0, "tags": tag("failed") },
            { "runId": "r-cancelled", "status": "CANCELED", "creationTime": 1_799_989_200.0, "startTime": 1_799_989_201.0, "endTime": 1_799_989_300.0, "tags": [] },
            { "runId": "r-odd", "status": "SOMETHING_NEW", "creationTime": 1_799_985_600.0, "tags": [] },
        ] } })
    }

    #[test]
    fn a_dagster_run_is_queued_when_made_works_from_its_start_and_says_how_it_ended() {
        use ScheduledJobRunOutcome::{Failed, Running, Succeeded, Unknown};
        let runs = runs_of(&["synthetic_load"], &synthetic_runs())
            .unwrap_or_else(|error| unreachable!("{error}"));
        let runs = &runs["synthetic_load"];
        let outcomes: Vec<(&str, ScheduledJobRunOutcome, Option<&str>)> = runs
            .iter()
            .map(|run| {
                (
                    run.run_urn.as_str(),
                    run.outcome,
                    run.native_result.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![
                ("r-queued", Running, None),
                ("r-retrying", Running, None),
                ("r-waited", Succeeded, Some("SUCCESS waiting")),
                ("r-changed", Succeeded, Some("SUCCESS changed")),
                ("r-failed", Failed, Some("FAILURE failed")),
                ("r-cancelled", Failed, Some("CANCELED")),
                ("r-odd", Unknown, None),
            ],
            "newest first, as Dagster gave them"
        );
        let millis = |run: usize| {
            let run = &runs[run];
            [run.queued_at, run.started_at, run.finished_at]
                .map(|at| at.map(|at| at.timestamp_millis()))
        };
        assert_eq!(
            millis(0),
            [Some(1_800_007_200_000), None, None],
            "still waits for its pool"
        );
        assert_eq!(
            millis(1),
            [Some(1_800_003_600_000), Some(1_800_003_660_500), None],
            "a retry runs inside the run, which keeps its first start and has no end yet"
        );
        assert_eq!(
            millis(3),
            [
                Some(1_799_996_400_000),
                Some(1_799_996_401_000),
                Some(1_799_997_000_000)
            ]
        );
    }

    #[test]
    fn a_job_whose_runs_dagster_could_not_list_refuses_the_whole_answer() {
        let mut data = synthetic_runs();
        data["j1"] = json!({ "__typename": "PythonError" });
        assert_eq!(
            runs_of(&["synthetic_load", "synthetic_bake"], &data),
            Err(
                "Dagster could not list the runs of foundation_job__synthetic_bake: PythonError"
                    .to_owned()
            )
        );
        let never = runs_of(
            &["synthetic_new"],
            &json!({ "j0": { "__typename": "Runs", "results": [] } }),
        );
        assert_eq!(
            never.map(|runs| runs.get("synthetic_new").map(Vec::len)),
            Ok(Some(0)),
            "a job with no run yet is listed with none"
        );
    }

    #[test]
    fn the_contract_checks_are_the_assets_checks_named_so_by_asset_and_name() {
        let data = json!({ "assetNodes": [
            { "assetKey": { "path": ["iceberg", "silver", "synthetic_b"] }, "assetChecksOrError": {
                "__typename": "AssetChecks", "checks": [
                    { "name": "contract_columns_present" }, { "name": "takeover_equal" }] } },
            { "assetKey": { "path": ["iceberg", "silver", "synthetic_a"] }, "assetChecksOrError": {
                "__typename": "AssetChecks", "checks": [
                    { "name": "contract_columns_present" }, { "name": "contract_allowed_values_kind" }] } },
            { "assetKey": { "path": ["perfectory", "synthetic-surface"] }, "assetChecksOrError": {
                "__typename": "AssetChecks", "checks": [] } },
        ] });
        let checks = contract_checks_of(&data).unwrap_or_else(|error| unreachable!("{error}"));
        let named: Vec<(String, &str)> = checks
            .iter()
            .map(|check| (check.asset_key.join("/"), check.check_name.as_str()))
            .collect();
        assert_eq!(
            named,
            vec![
                (
                    "iceberg/silver/synthetic_a".to_owned(),
                    "contract_allowed_values_kind"
                ),
                (
                    "iceberg/silver/synthetic_a".to_owned(),
                    "contract_columns_present"
                ),
                (
                    "iceberg/silver/synthetic_b".to_owned(),
                    "contract_columns_present"
                ),
            ]
        );
        assert!(checks
            .iter()
            .all(|check| check.executions == JsonValue::Null));
    }

    #[test]
    fn an_asset_whose_checks_dagster_could_not_list_is_an_error_not_an_empty_answer() {
        let data = json!({ "assetNodes": [
            { "assetKey": { "path": ["iceberg", "silver", "synthetic"] }, "assetChecksOrError": {
                "__typename": "AssetCheckNeedsMigrationError" } },
        ] });
        assert_eq!(
            contract_checks_of(&data),
            Err(
                "Dagster could not list the checks of an asset: AssetCheckNeedsMigrationError"
                    .to_owned()
            )
        );
    }

    #[test]
    fn every_checks_newest_execution_is_asked_in_one_query() {
        let checks = vec![
            ContractCheck {
                asset_key: vec!["iceberg".into(), "silver".into(), "synthetic".into()],
                check_name: "contract_columns_present".into(),
                executions: JsonValue::Null,
            },
            ContractCheck {
                asset_key: vec!["perfectory".into(), "intelligence".into(), "fact".into()],
                check_name: "contract_allowed_values_kind".into(),
                executions: JsonValue::Null,
            },
        ];
        let query = executions_query(&checks);
        assert!(query.starts_with("{ c0: assetCheckExecutions(assetKey: {path: [\"iceberg\",\"silver\",\"synthetic\"]}, checkName: \"contract_columns_present\", limit: 5) { status timestamp evaluation"));
        assert!(query.contains(" c1: assetCheckExecutions(assetKey: {path: [\"perfectory\",\"intelligence\",\"fact\"]}, checkName: \"contract_allowed_values_kind\", limit: 5)"));
        assert!(query.ends_with(" }"));
    }

    #[test]
    fn the_jobs_own_unfinished_runs_are_read_by_job_newest_first() {
        let data = json!({ "runsOrError": { "results": [
            { "runId": "a", "jobName": "foundation_job__synthetic_load", "creationTime": 1_800_000_000.0 },
            { "runId": "b", "jobName": "foundation_job__synthetic_load", "creationTime": 1_800_000_600.5 },
            // An asset check's run is no job's.
            { "runId": "c", "jobName": "__ASSET_JOB", "creationTime": 1_800_000_900.0 },
        ] } });
        let runs = in_progress_runs(&data);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs["synthetic_load"].timestamp_millis(), 1_800_000_600_500);
        assert_eq!(
            unfinished(),
            json!(["QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING"])
        );
    }

    #[test]
    fn a_runs_lines_are_its_message_events_in_order() {
        let page = json!({ "events": [
            { "__typename": "MessageEvent", "message": "start-scheduled-job: synthetic.service invocation=1" },
            { "__typename": "StepStartEvent" },
            { "__typename": "MessageEvent", "message": "synthetic FAILED: no source\nsecond line" },
        ] });
        assert_eq!(
            message_lines(&page),
            vec![
                "start-scheduled-job: synthetic.service invocation=1",
                "synthetic FAILED: no source",
                "second line"
            ]
        );
    }
}
