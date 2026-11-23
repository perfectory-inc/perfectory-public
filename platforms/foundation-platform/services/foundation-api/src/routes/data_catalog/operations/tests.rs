use super::*;
use serde_json::json;

fn at(iso: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(iso)
        .map(|time| time.with_timezone(&Utc))
        .unwrap_or_default()
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

#[test]
fn a_start_s_limit_is_its_wait_its_run_and_its_grace() {
    // root ADR-0270 §2b: a start that may wait for the host is late only after all three.
    let mut jobs = list();
    jobs.jobs[0].wait_limit_minutes = 45;
    let timeouts = jobs.timeouts();
    assert_eq!(timeouts.get("nightly"), Some(&(45 + 55 + 5)));
    assert_eq!(timeouts.get("half_hourly"), Some(&(55 + 5)));
}

#[test]
fn a_job_list_without_a_wait_limit_is_refused() {
    let listed = r#"{"dag_id_prefix": "synthetic_", "jobs": [{"id": "nightly", "description": "d",
        "schedule": "20 7 * * *", "time_limit_minutes": 55, "stop_grace_minutes": 5,
        WAIT"enabled": true, "started_by": "schedule", "pipeline_graph_edges": []}]}"#;
    assert!(serde_json::from_str::<JobList>(&listed.replace("WAIT", "")).is_err());
    // The same list with the field is read: the refusal is the missing field's, nothing else's.
    assert!(serde_json::from_str::<JobList>(
        &listed.replace("WAIT", r#""wait_limit_minutes": 0, "#)
    )
    .is_ok());
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
                wait_limit_minutes: 0,
                enabled: true,
                enabled_since: None,
                disabled_reason: None,
                started_by: ScheduledJobStart::Schedule,
                pipeline_graph_edges: vec!["synthetic-edge".to_owned()],
                dagster_stage: DagsterStage::Airflow,
            },
            ListedJob {
                id: "half_hourly".to_owned(),
                description: "a half-hourly job".to_owned(),
                schedule: "0,30 * * * *".to_owned(),
                time_limit_minutes: 55,
                stop_grace_minutes: 5,
                wait_limit_minutes: 0,
                enabled: true,
                enabled_since: None,
                disabled_reason: None,
                started_by: ScheduledJobStart::Schedule,
                pipeline_graph_edges: vec!["synthetic-edge".to_owned()],
                dagster_stage: DagsterStage::Airflow,
            },
            ListedJob {
                id: "off".to_owned(),
                description: "a job that is off".to_owned(),
                schedule: "40 4 * * *".to_owned(),
                time_limit_minutes: 55,
                stop_grace_minutes: 5,
                wait_limit_minutes: 0,
                enabled: false,
                enabled_since: None,
                disabled_reason: Some("waiting for a supervised run".to_owned()),
                started_by: ScheduledJobStart::Inputs,
                pipeline_graph_edges: Vec::new(),
                dagster_stage: DagsterStage::Dagster,
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
        })
        .collect()
}

/// A run queued at `queued` whose work started at `started` (absent while it waits) and took
/// `minutes`, ending with `outcome`.
fn ran(
    id: &str,
    queued: &str,
    started: Option<&str>,
    minutes: i64,
    outcome: ScheduledJobRunOutcome,
) -> ScheduledJobRun {
    let started_at = started.map(at);
    ScheduledJobRun {
        run_urn: id.to_owned(),
        queued_at: Some(at(queued)),
        started_at,
        finished_at: (outcome != ScheduledJobRunOutcome::Running)
            .then(|| started_at.map(|start| start + chrono::Duration::minutes(minutes)))
            .flatten(),
        outcome,
        native_result: None,
        rows_written: None,
    }
}

/// The nightly job's runs, newest first, as its orchestrator gave them.
fn nightly(runs: Vec<ScheduledJobRun>) -> BTreeMap<String, Vec<ScheduledJobRun>> {
    BTreeMap::from([("nightly".to_owned(), runs)])
}

/// 2026-10-18: `silver_refresh_*` waited about 205 minutes for the spark pool and ran for none, and
/// the screen called the run slow, because a run was measured from when it was queued.
#[test]
fn a_run_that_waited_for_its_pool_is_timed_from_its_start_and_is_not_slow() {
    use ScheduledJobRunOutcome::Succeeded;
    let now = at("2026-01-06T12:00:00Z");
    // Five ordinary days: a minute's wait and ten minutes' work. Then a run that waited three
    // hours for the pool and worked for ten minutes.
    let runs = (1..=6)
        .rev()
        .map(|day| {
            let start = format!(
                "2026-01-{day:02}T{}:00Z",
                if day == 6 { "10:20" } else { "07:21" }
            );
            let queued = format!("2026-01-{day:02}T07:20:00Z");
            ran(&format!("run-{day}"), &queued, Some(&start), 10, Succeeded)
        })
        .collect();
    let mut statuses = job_statuses(&list(), &dependencies(&list()), &nightly(runs), now);
    let last = statuses[0].last_run.as_ref();
    assert_eq!(
        (
            last.and_then(|run| run.queued_at),
            last.and_then(|run| run.started_at)
        ),
        (
            Some(at("2026-01-06T07:20:00Z")),
            Some(at("2026-01-06T10:20:00Z"))
        ),
        "queued when it was made, started when its work did"
    );
    assert_eq!(
        statuses[0].typical_duration_seconds,
        Some(600),
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
            runs_unread: &std::collections::BTreeSet::new(),
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
fn each_job_takes_its_newest_run_names_the_job_holding_it_and_its_next_start() {
    use ScheduledJobRunOutcome::{Failed, Succeeded};
    let now = at("2026-01-02T08:00:00Z");
    let mut failed = ran(
        "scheduled__2026-01-02",
        "2026-01-02T07:20:00Z",
        Some("2026-01-02T07:20:05Z"),
        5,
        Failed,
    );
    failed.native_result = Some("failed".to_owned());
    let runs = BTreeMap::from([
        (
            "nightly".to_owned(),
            vec![
                failed,
                ran(
                    "scheduled__2026-01-01",
                    "2026-01-01T07:20:00Z",
                    Some("2026-01-01T07:20:05Z"),
                    5,
                    Succeeded,
                ),
            ],
        ),
        (
            "off".to_owned(),
            vec![ran(
                "7a1e2b3c-0000-4000-8000-000000000001",
                "2026-01-01T04:40:00Z",
                Some("2026-01-01T04:41:00Z"),
                5,
                Succeeded,
            )],
        ),
        // A job the list no longer names is no job's.
        (
            "gone".to_owned(),
            vec![ran("x", "2026-01-02T07:00:00Z", None, 0, Succeeded)],
        ),
    ]);
    let statuses = job_statuses(&list(), &dependencies(&list()), &runs, now);
    let nightly = &statuses[0];
    assert_eq!(nightly.dag_id, "synthetic_nightly");
    let last = nightly.last_run.as_ref();
    assert_eq!(
        last.map(|r| (r.run_urn.as_str(), r.outcome, r.native_result.as_deref())),
        Some(("scheduled__2026-01-02", Failed, Some("failed")))
    );
    assert_eq!(
        nightly.catalog_job_urn.as_deref(),
        Some("synthetic_nightly"),
        "Airflow holds its runs under its DAG"
    );
    assert_eq!(nightly.next_run_at, Some(at("2026-01-03T07:20:00Z")));

    let half_hourly = &statuses[1];
    assert_eq!(
        (
            half_hourly.last_run.as_ref(),
            half_hourly.catalog_job_urn.as_deref()
        ),
        (None, None),
        "no run: never ran"
    );
    assert_eq!(
        half_hourly.next_run_at,
        Some(at("2026-01-02T08:30:00Z")),
        "the start at `now` itself is not next"
    );

    let off = &statuses[2];
    assert_eq!(
        (off.catalog_job_urn.as_deref(), off.decided_by),
        (Some("foundation_job__off"), ScheduledJobScheduler::Dagster),
        "Dagster holds its runs under its Dagster job"
    );
    assert_eq!(off.next_run_at, None, "a job that is off has no next start");
    assert_eq!(
        off.disabled_reason.as_deref(),
        Some("waiting for a supervised run")
    );
}

#[test]
fn a_job_dagster_runs_has_its_runs_read_there_from_proxied_on() {
    // Root ADR-0609 §1: a proxied job's DAG still starts it, but its run is its Dagster job's.
    let mut jobs = list();
    jobs.jobs[0].dagster_stage = DagsterStage::Shadow;
    jobs.jobs[1].dagster_stage = DagsterStage::Proxied;
    let (dagster, airflow) = jobs.by_runner();
    assert_eq!(dagster, vec!["half_hourly", "off"]);
    assert_eq!(
        airflow,
        vec![("nightly", "synthetic_nightly".to_owned(), true)]
    );
    let statuses = job_statuses(
        &jobs,
        &dependencies(&jobs),
        &BTreeMap::new(),
        at("2026-01-02T08:00:00Z"),
    );
    assert_eq!(
        statuses
            .iter()
            .map(|status| status.decided_by)
            .collect::<Vec<_>>(),
        vec![
            ScheduledJobScheduler::Airflow,
            ScheduledJobScheduler::Dagster,
            ScheduledJobScheduler::Dagster
        ]
    );
}

/// A run of `minutes` from a fixed synthetic start, ending with `outcome`.
fn took(id: &str, minutes: i64, outcome: ScheduledJobRunOutcome) -> ScheduledJobRun {
    ran(
        id,
        "2026-01-01T07:20:00Z",
        Some("2026-01-01T07:20:00Z"),
        minutes,
        outcome,
    )
}

#[test]
fn the_typical_duration_is_the_median_of_succeeded_runs_with_both_times() {
    use ScheduledJobRunOutcome::{Failed, Running, Skipped, Succeeded, UpForRetry};
    let typical_of = |runs: &[ScheduledJobRun]| typical_duration(runs.iter());
    assert_eq!(
        typical_of(&[
            took("a", 10, Succeeded),
            took("b", 30, Succeeded),
            took("c", 20, Succeeded)
        ]),
        Some((20 * 60, 3)),
        "odd count: the middle one"
    );
    assert_eq!(
        typical_of(&[
            took("a", 10, Succeeded),
            took("b", 40, Succeeded),
            took("c", 20, Succeeded),
            took("d", 25, Succeeded),
        ]),
        Some((22 * 60 + 30, 4)),
        "even count: the mean of the two middle ones"
    );
    assert_eq!(
        typical_of(&[
            took("ok", 10, Succeeded),
            took("failed", 90, Failed),
            took("retry", 90, UpForRetry),
            took("skipped", 0, Skipped),
            took("running", 0, Running),
        ]),
        Some((10 * 60, 1)),
        "failed, retried, skipped and running runs are not durations of the job"
    );
    assert_eq!(
        typical_of(&[
            ran("no-start", "2026-01-01T07:20:00Z", None, 10, Succeeded),
            took("ok", 5, Succeeded),
        ]),
        Some((5 * 60, 1)),
        "a run without a recorded start has no duration"
    );
    assert_eq!(
        typical_of(&[took("failed", 10, Failed)]),
        None,
        "no succeeded run: no typical duration"
    );
    assert_eq!(typical_of(&[]), None);
}

#[test]
fn a_running_job_carries_the_typical_duration_of_its_succeeded_runs() {
    use ScheduledJobRunOutcome::{Running, Succeeded};
    let now = at("2026-01-02T08:00:00Z");
    let runs = vec![
        ran(
            "now",
            "2026-01-02T07:20:00Z",
            Some("2026-01-02T07:20:00Z"),
            0,
            Running,
        ),
        took("run-1", 30, Succeeded),
        took("run-2", 40, Succeeded),
        took("run-3", 50, Succeeded),
    ];
    let statuses = job_statuses(&list(), &dependencies(&list()), &nightly(runs), now);
    let nightly = &statuses[0];
    assert_eq!(nightly.last_run.as_ref().map(|r| r.outcome), Some(Running));
    assert_eq!(nightly.typical_duration_seconds, Some(40 * 60));
    assert_eq!(nightly.typical_duration_runs, 3);
    let never = &statuses[1];
    assert_eq!(
        (never.typical_duration_seconds, never.typical_duration_runs),
        (None, 0)
    );
}

/// Two synthetic contracts' checks, each with the executions Dagster gave.
fn synthetic_contract_checks() -> Vec<super::facts::dagster::ContractCheck> {
    let check =
        |key: &[&str], name: &str, executions: JsonValue| super::facts::dagster::ContractCheck {
            asset_key: key.iter().map(|part| (*part).to_owned()).collect(),
            check_name: name.to_owned(),
            executions,
        };
    let synthetic = ["iceberg", "silver", "synthetic"];
    vec![
        check(
            &synthetic,
            "contract_columns_present",
            json!([
            // A check run planned after the report does not hide it.
            { "status": "IN_PROGRESS", "timestamp": 1_800_000_900.0, "evaluation": null },
            { "status": "FAILED", "timestamp": 1_800_000_000.5,
              "evaluation": { "success": false, "severity": "ERROR", "metadataEntries": [
                { "label": "missing_columns", "text": "row_digest" }] } }]),
        ),
        check(
            &synthetic,
            "contract_allowed_values_kind",
            json!([
            { "status": "SUCCEEDED", "timestamp": 1_800_000_100.0,
              "evaluation": { "success": true, "severity": "WARN", "metadataEntries": [
                { "label": "invalid_rows", "intRepr": "0" }] } }]),
        ),
        check(&synthetic, "contract_allowed_values_stage", json!([])),
        check(
            &synthetic,
            "contract_allowed_values_zone",
            json!([
            { "status": "EXECUTION_FAILED", "timestamp": 1_800_000_200.0, "evaluation": null }]),
        ),
        // The check run alone before the quality job ever reported: a re-statement of nothing.
        check(
            &["perfectory", "intelligence", "synthetic_fact"],
            "contract_columns_present",
            json!([
            { "status": "FAILED", "timestamp": 1_800_000_300.0,
              "evaluation": { "success": false, "severity": "WARN", "metadataEntries": [
                { "label": "restated_from_run", "text": "" }] } }]),
        ),
    ]
}

#[test]
fn a_contract_shows_each_check_with_its_newest_evaluation_and_severity() {
    let contracts = contract_quality(&synthetic_contract_checks());
    let ids: Vec<(&str, &str)> = contracts
        .iter()
        .map(|c| (c.contract_id.as_str(), c.asset_key.as_str()))
        .collect();
    assert_eq!(
        ids,
        vec![
            (
                "intelligence.synthetic_fact",
                "perfectory/intelligence/synthetic_fact"
            ),
            ("silver.synthetic", "iceberg/silver/synthetic"),
        ]
    );
    assert_eq!(
        contracts[0]
            .checks
            .iter()
            .map(|c| (c.result, c.checked_at))
            .collect::<Vec<_>>(),
        vec![(DataQualityResult::NotRun, None)]
    );
    let quality = &contracts[1];
    let by_name = |name: &str| quality.checks.iter().find(|c| c.check_name == name);
    let schema = by_name("contract_columns_present");
    assert_eq!(
        schema.map(|c| (c.result, c.kind.as_str(), c.field.clone())),
        Some((DataQualityResult::Failure, "contract_columns_present", None))
    );
    assert_eq!(
        schema.map(|c| c.details.clone()),
        Some(vec![
            DataQualityDetail {
                key: "severity".to_owned(),
                value: "ERROR".to_owned()
            },
            DataQualityDetail {
                key: "missing_columns".to_owned(),
                value: "row_digest".to_owned()
            },
        ])
    );
    assert_eq!(
        schema
            .and_then(|c| c.checked_at)
            .map(|at| at.timestamp_millis()),
        Some(1_800_000_000_500)
    );
    assert_eq!(
        by_name("contract_allowed_values_kind").map(|c| (
            c.result,
            c.kind.as_str(),
            c.field.clone()
        )),
        Some((
            DataQualityResult::Success,
            "contract_allowed_values",
            Some("kind".to_owned())
        ))
    );
    assert_eq!(
        by_name("contract_allowed_values_stage").map(|c| (c.result, c.checked_at, c.details.len())),
        Some((DataQualityResult::NotRun, None, 0)),
        "a check the quality job has not reported yet"
    );
    assert_eq!(
        by_name("contract_allowed_values_zone").map(|c| c.result),
        Some(DataQualityResult::Error),
        "a newest execution that failed to run"
    );
    assert_eq!(
        quality
            .checks
            .iter()
            .map(|c| c.check_name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "contract_allowed_values_kind",
            "contract_allowed_values_stage",
            "contract_allowed_values_zone",
            "contract_columns_present"
        ]
    );
}

#[test]
fn the_real_job_list_reads_and_every_enabled_schedule_has_a_next_start() {
    let list = read_job_list(&jobs_path()).unwrap_or_else(|error| unreachable!("{error:?}"));
    assert!(!list.jobs.is_empty());
    let now = at("2026-01-02T08:00:00Z");
    let depends = dependencies(&list);
    for status in job_statuses(&list, &depends, &BTreeMap::new(), now) {
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
fn recent_runs_are_the_newest_runs_and_bounded() {
    let now = at("2026-01-02T08:00:00Z");
    let start = at("2026-01-01T00:00:00Z");
    // Twelve hourly runs, newest first, more than are shown.
    let runs = (0..12)
        .rev()
        .map(|hour| {
            let began = (start + chrono::Duration::hours(hour)).to_rfc3339();
            ran(
                &format!("run-{hour:02}"),
                &began,
                Some(&began),
                5,
                ScheduledJobRunOutcome::Succeeded,
            )
        })
        .collect();
    let statuses = job_statuses(&list(), &dependencies(&list()), &nightly(runs), now);
    let nightly = &statuses[0];
    let shown: Vec<&str> = nightly
        .recent_runs
        .iter()
        .map(|r| r.run_urn.as_str())
        .collect();
    let expected: Vec<String> = (2..12).rev().map(|hour| format!("run-{hour:02}")).collect();
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
    let statuses = job_statuses(&list(), &dependencies(&list()), &BTreeMap::new(), now);
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
