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
    flow_of("synthetic", flow_id, jobs)
}

/// A flow as `DataHub` names it from the engine that reported it (`processing_engine`).
fn flow_of(engine: &str, flow_id: &str, jobs: &[(&str, Vec<JsonValue>)]) -> JsonValue {
    json!({
        "urn": format!("urn:li:dataFlow:({engine},{flow_id},test)"),
        "flowId": flow_id,
        "jobs": { "relationships": jobs.iter().map(|(job, runs)| json!({ "entity": {
            "urn": format!("urn:li:dataJob:(urn:li:dataFlow:({engine},{flow_id},test),{job})"),
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

/// 2026-10-18, `building_register_floor`: queued 08:20:00, its run task 08:20:01 to 08:33:46,
/// then a deploy paused every DAG and the publish task after it ran at 08:54, so the DAG run
/// ended 08:54:18. The screen said 34 minutes; the work took 13 minutes 45 seconds.
#[test]
fn a_run_ends_when_its_run_task_does_not_when_a_paused_dag_run_does() {
    let now = at("2026-01-02T12:00:00Z");
    let dag = run(
        "urn:li:dataProcessInstance:dag-floor",
        &[
            event("STARTED", "2026-01-02T08:20:00Z", None),
            event("COMPLETE", "2026-01-02T08:54:18Z", Some("SUCCESS")),
        ],
    );
    let task = run(
        "urn:li:dataProcessInstance:task-floor",
        &[
            event("STARTED", "2026-01-02T08:20:01Z", None),
            event("COMPLETE", "2026-01-02T08:33:46Z", Some("SUCCESS")),
        ],
    );
    let flows = vec![flow(
        "synthetic_nightly",
        &[
            ("synthetic_nightly", vec![dag]),
            ("synthetic_nightly.run", vec![task]),
        ],
    )];
    let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
    let last = statuses[0].last_run.as_ref();
    assert_eq!(
        last.and_then(|run| run.started_at),
        Some(at("2026-01-02T08:20:01Z"))
    );
    assert_eq!(
        last.and_then(|run| run.finished_at),
        Some(at("2026-01-02T08:33:46Z")),
        "the work ended with the run task, not with the paused DAG run"
    );
    assert_eq!(statuses[0].typical_duration_seconds, Some(825));
}

/// A retried run task: its work spans the first try's start to the last try's end.
#[test]
fn a_retried_run_spans_its_first_try_to_its_last() {
    let now = at("2026-01-02T12:00:00Z");
    let dag = run(
        "urn:li:dataProcessInstance:dag-retry",
        &[
            event("STARTED", "2026-01-02T08:00:00Z", None),
            event("COMPLETE", "2026-01-02T08:40:00Z", Some("SUCCESS")),
        ],
    );
    let tries = vec![
        run(
            "urn:li:dataProcessInstance:task-retry-1",
            &[
                event("STARTED", "2026-01-02T08:05:00Z", None),
                event("COMPLETE", "2026-01-02T08:10:00Z", Some("FAILURE")),
            ],
        ),
        run(
            "urn:li:dataProcessInstance:task-retry-2",
            &[
                event("STARTED", "2026-01-02T08:15:00Z", None),
                event("COMPLETE", "2026-01-02T08:30:00Z", Some("SUCCESS")),
            ],
        ),
    ];
    let flows = vec![flow(
        "synthetic_nightly",
        &[
            ("synthetic_nightly", vec![dag]),
            ("synthetic_nightly.run", tries),
        ],
    )];
    let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
    let last = statuses[0].last_run.as_ref();
    assert_eq!(
        (
            last.and_then(|run| run.started_at),
            last.and_then(|run| run.finished_at)
        ),
        (
            Some(at("2026-01-02T08:05:00Z")),
            Some(at("2026-01-02T08:30:00Z"))
        )
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

/// Root ADR-0565 §7: a job Dagster decides reports the same flow id and job names as its DAG did,
/// under its own engine's flow; its history goes on from the runs Airflow reported.
#[test]
fn a_job_moved_to_dagster_keeps_one_run_history_across_the_two_engines_flows() {
    let now = at("2026-01-06T12:00:00Z");
    let mut engines: Vec<(Vec<JsonValue>, Vec<JsonValue>)> = vec![(Vec::new(), Vec::new()); 2];
    for number in 1..=5 {
        let (dag, task) = day(number, Some("07:21"), 10);
        let (dags, tasks) = &mut engines[usize::from(number > 3)];
        dags.push(dag);
        tasks.extend(task);
    }
    let flows: Vec<JsonValue> = ["airflow", "dagster"]
        .iter()
        .zip(engines)
        .map(|(engine, (dags, tasks))| {
            flow_of(
                engine,
                "synthetic_nightly",
                &[
                    ("synthetic_nightly", dags),
                    ("synthetic_nightly.run", tasks),
                ],
            )
        })
        .collect();
    let statuses = job_statuses(&list(), &dependencies(&list()), &flows, now);
    let nightly = &statuses[0];
    assert_eq!(
        nightly.catalog_job_urn.as_deref(),
        Some("urn:li:dataJob:(urn:li:dataFlow:(dagster,synthetic_nightly,test),synthetic_nightly)"),
        "the latest run is Dagster's"
    );
    assert_eq!(
        nightly
            .recent_runs
            .iter()
            .map(|run| run.run_urn.as_str())
            .collect::<Vec<_>>(),
        (1..=5)
            .rev()
            .map(|number| format!("urn:li:dataProcessInstance:dag-{number}"))
            .collect::<Vec<_>>(),
        "the runs either engine reported, newest first, each once"
    );
    assert_eq!(nightly.typical_duration_runs, 5);
    assert_eq!(nightly.typical_duration_seconds, Some(600));
}

#[test]
fn job_name_is_the_part_after_its_flow() {
    assert_eq!(
        job_name(
            "urn:li:dataJob:(urn:li:dataFlow:(dagster,foundation_x,perfectory),foundation_x.run)"
        ),
        "foundation_x.run"
    );
    assert_eq!(job_name("not a job urn"), "not a job urn");
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
    let parsed: Vec<ScheduledJobRun> = runs.iter().filter_map(run_from).map(|(r, _)| r).collect();
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
fn a_contract_shows_each_check_with_its_newest_evaluation_and_severity() {
    use super::facts::dagster::ContractCheck;

    let check = |key: &[&str], name: &str, execution: JsonValue| ContractCheck {
        asset_key: key.iter().map(|part| (*part).to_owned()).collect(),
        check_name: name.to_owned(),
        execution,
    };
    let synthetic = ["iceberg", "silver", "synthetic"];
    let checks = vec![
        check(
            &synthetic,
            "contract_columns_present",
            json!({
            "timestamp": 1_800_000_000.5,
            "evaluation": { "success": false, "severity": "ERROR", "metadataEntries": [
                { "label": "missing_columns", "text": "row_digest" }] } }),
        ),
        check(
            &synthetic,
            "contract_allowed_values_kind",
            json!({
            "timestamp": 1_800_000_100.0,
            "evaluation": { "success": true, "severity": "WARN", "metadataEntries": [
                { "label": "invalid_rows", "intRepr": "0" }] } }),
        ),
        check(&synthetic, "contract_allowed_values_stage", JsonValue::Null),
        check(
            &["perfectory", "intelligence", "synthetic_fact"],
            "contract_columns_present",
            JsonValue::Null,
        ),
    ];
    let contracts = contract_quality(&checks);
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
    let kind = by_name("contract_allowed_values_kind");
    assert_eq!(
        kind.map(|c| (c.result, c.kind.as_str(), c.field.clone())),
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
        quality
            .checks
            .iter()
            .map(|c| c.check_name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "contract_allowed_values_kind",
            "contract_allowed_values_stage",
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
