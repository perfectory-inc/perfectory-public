//! The run facts of the jobs Dagster decides (root ADR-0565 §8), read from Dagster's read-only
//! webserver (`dagster-webserver --read-only`, `compose.orchestration.yml`): it answers the same
//! GraphQL as the operating one and holds no mutation, so the API can ask it and change nothing.
//!
//! A job at `dagster` has no DAG: its runs are its Dagster job's (`foundation_job__<id>`). Its run op
//! logs every line the host's forced command streams (`orchestration/dagster/systemd_pipes.py`), so a
//! failed run's lines are that run's message events, framed by `start-scheduled-job` as an Airflow
//! task log was, and the same `classify` and `mask` read them.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value as JsonValue};

use super::super::health::FailedRunLog;
use super::{classify, waiting_reason};

/// The read-only webserver's address inside the runtime.
const API_URL_ENV: &str = "FOUNDATION_PLATFORM_DAGSTER_API_URL";
/// A job's Dagster job (`orchestration/dags/job_specs.py` `DAGSTER_JOB_PREFIX`).
pub(super) const JOB_PREFIX: &str = "foundation_job__";
/// The tag a run's outcome is on (`job_specs.OUTCOME_TAG`): a run that ended waiting succeeded.
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

    /// The log of `job_id`'s latest failed run, or with `waited` of its latest run that ended
    /// waiting, read for the data catalog's run `run_urn`.
    pub(super) async fn latest_log(
        &self,
        job_id: &str,
        run_urn: &str,
        waited: bool,
    ) -> Result<FailedRunLog, String> {
        let mut filter = json!({ "pipelineName": format!("{JOB_PREFIX}{job_id}") });
        if waited {
            filter["statuses"] = json!(["SUCCESS"]);
            filter["tags"] = json!([{ "key": OUTCOME_TAG, "value": "waiting" }]);
        } else {
            filter["statuses"] = json!(["FAILURE"]);
        }
        let runs = self
            .ask(RUNS_QUERY, json!({ "filter": filter, "limit": 1 }))
            .await?;
        let run_id = runs["runsOrError"]["results"][0]["runId"]
            .as_str()
            .ok_or_else(|| format!("Dagster holds no such run of {job_id}"))?
            .to_owned();
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
            run_urn: run_urn.to_owned(),
            scheduler_run_id: Some(run_id),
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
        contract_checks_of, executions_query, in_progress_runs, message_lines, unfinished,
        ContractCheck,
    };
    use serde_json::{json, Value as JsonValue};

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
