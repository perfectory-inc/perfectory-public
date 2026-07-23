use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_coverage_ledger.v1";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_plan.v1";
const LEDGER_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_entry.v1";
const EXECUTION_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_execution.v1";
const EVENT_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_ledger_event.v1";
const DEFAULT_PLAN_PATH: &str = "target/audit/national-data-collection-plan.json";
const DEFAULT_EVIDENCE_GLOB: &str =
    "target/audit/national-data-collection-ledger-execution-*-evidence.json";
const DEFAULT_REPORT_PATH: &str = "target/audit/national-data-collection-coverage-ledger.json";
const FORBIDDEN_TOKENS: &[&str] = &[
    "DATA_GO_KR_SERVICE_KEY",
    "VWORLD_API_KEY",
    "serviceKey",
    "raw_payload",
    "unit-test-key",
    "fake-vworld-key",
];

pub fn run() -> anyhow::Result<()> {
    let config = CheckConfig::from_env()?;
    let report = if config.plan_path.is_file() {
        check_coverage(&config)?
    } else {
        CheckReport::skipped(
            &config.root,
            &config.plan_path,
            "national data collection plan has not been compiled",
        )
    };

    write_json_file(&config.report_path, &report)?;

    if report.status == "skipped" {
        println!(
            "national-data-collection-coverage-ledger-ok status=skipped report={}",
            repo_relative_path(&config.root, &config.report_path)
        );
        return Ok(());
    }

    if !report.blockers.is_empty() {
        println!(
            "national-data-collection-coverage-ledger-blocked status=blocked blockers={} report={}",
            report.blockers.len(),
            repo_relative_path(&config.root, &config.report_path)
        );
        for blocker in &report.blockers {
            println!("blocker={blocker}");
        }
        bail!("national data collection coverage ledger blocked");
    }

    println!(
        "national-data-collection-coverage-ledger-ok status=ready planned={} succeeded={} failed={} missing={} extra={} report={}",
        report.coverage.planned_job_count,
        report.coverage.succeeded_job_count,
        report.coverage.failed_job_count,
        report.coverage.missing_job_count,
        report.coverage.extra_job_count,
        repo_relative_path(&config.root, &config.report_path)
    );
    Ok(())
}

struct CheckConfig {
    root: PathBuf,
    plan_path: PathBuf,
    evidence_paths: Vec<PathBuf>,
    explicit_evidence_paths: bool,
    evidence_glob: PathBuf,
    report_path: PathBuf,
}

impl CheckConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        let evidence_paths_raw =
            env::var("FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_COVERAGE_EVIDENCE_PATHS").ok();
        let explicit_evidence_paths = evidence_paths_raw
            .as_deref()
            .is_some_and(|raw| raw.split(';').any(|part| !part.trim().is_empty()));
        let evidence_paths = evidence_paths_raw
            .map(|raw| {
                raw.split(';')
                    .filter_map(|part| {
                        let trimmed = part.trim();
                        (!trimmed.is_empty()).then(|| PathBuf::from(trimmed))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(Self {
            plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_COVERAGE_PLAN_PATH",
                    DEFAULT_PLAN_PATH,
                )?,
                "PlanPath",
            )?,
            evidence_paths,
            explicit_evidence_paths,
            evidence_glob: env_path(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_COVERAGE_EVIDENCE_GLOB",
                DEFAULT_EVIDENCE_GLOB,
            )?,
            report_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_COVERAGE_REPORT_PATH",
                    DEFAULT_REPORT_PATH,
                )?,
                "ReportPath",
            )?,
            root,
        })
    }
}

fn check_coverage(config: &CheckConfig) -> anyhow::Result<CheckReport> {
    let mut blockers = Vec::new();
    let plan = read_json(&config.plan_path, "national data collection plan")?;
    add_forbidden_token_blockers(&config.plan_path, "plan", &mut blockers);

    let plan_hash = string_property(&plan, "compiler_input_hash_sha256");
    validate_plan(&plan, &plan_hash, &mut blockers);

    let execution_ledger = plan.get("execution_ledger").unwrap_or(&JsonValue::Null);
    let ledger_path_value = string_property(execution_ledger, "path");
    let ledger_entry_count = i64_property(execution_ledger, "entry_count", 0);
    let planned_count = i64_property(execution_ledger, "planned_count", 0);
    let resolved_ledger_path = if ledger_path_value.trim().is_empty() {
        blockers.push("plan execution_ledger.path is required".to_owned());
        None
    } else {
        Some(resolve_repo_path(
            &config.root,
            &PathBuf::from(&ledger_path_value),
            "execution_ledger.path",
        )?)
    };

    if let Some(path) = &resolved_ledger_path {
        if !path.is_file() {
            blockers.push("execution ledger file missing".to_owned());
        }
    }

    let ledger_rows = read_ledger_rows(resolved_ledger_path.as_deref(), &mut blockers)?;
    add_if(
        &mut blockers,
        ledger_entry_count != i64::try_from(ledger_rows.len()).unwrap_or(i64::MAX),
        "execution ledger entry_count must match rows",
    );
    add_if(
        &mut blockers,
        planned_count != ledger_entry_count,
        "execution ledger planned_count must match entry_count",
    );

    let mut provider_buckets = BTreeMap::new();
    let mut planned_jobs = BTreeMap::new();
    let mut planned_scopes = BTreeSet::new();
    validate_ledger_rows(
        &ledger_rows,
        &plan_hash,
        &mut provider_buckets,
        &mut planned_jobs,
        &mut planned_scopes,
        &mut blockers,
    );

    let resolved_evidence_paths = resolve_evidence_paths(config)?;
    if resolved_evidence_paths.is_empty() {
        blockers.push(
            "national data collection ledger execution evidence has not been produced".to_owned(),
        );
    }

    let mut state = CoverageState::default();
    for evidence_path in &resolved_evidence_paths {
        inspect_execution_evidence(
            config,
            evidence_path,
            &plan_hash,
            &planned_jobs,
            &mut provider_buckets,
            &mut state,
            &mut blockers,
        )?;
    }

    let mut missing_jobs = Vec::new();
    for job_id in planned_jobs.keys() {
        if !state.succeeded_jobs.contains(job_id) {
            missing_jobs.push(job_id.to_owned());
            blockers.push(format!("missing planned job: {job_id}"));
        }
    }

    add_if(
        &mut blockers,
        state.succeeded_jobs.len() != planned_jobs.len(),
        "succeeded job count must match planned job count",
    );
    add_if(
        &mut blockers,
        !state.failed_jobs.is_empty() || state.failed_job_count != 0,
        "failed job count must be zero",
    );
    add_if(
        &mut blockers,
        state.request_count_total < i64::try_from(state.succeeded_jobs.len()).unwrap_or(i64::MAX)
            && !state.succeeded_jobs.is_empty(),
        "request_count_total must cover succeeded jobs",
    );

    let provider_rows = provider_buckets.into_values().collect::<Vec<_>>();
    Ok(CheckReport {
        schema_version: SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status: if blockers.is_empty() {
            "ready"
        } else {
            "blocked"
        },
        plan: PlanReport {
            path: repo_relative_path(&config.root, &config.plan_path),
            compiler_input_hash_sha256: plan_hash,
            ledger_path: resolved_ledger_path
                .as_deref()
                .map(|path| repo_relative_path(&config.root, path))
                .unwrap_or_default(),
        },
        evidence: EvidenceReport {
            file_count: resolved_evidence_paths.len(),
            stale_file_count: state.stale_evidence_paths.len(),
            paths: state.evidence_paths,
            stale_paths: state.stale_evidence_paths,
        },
        coverage: CoverageReport {
            planned_job_count: planned_jobs.len(),
            planned_scope_count: planned_scopes.len(),
            selected_job_count: state.selected_job_count,
            started_job_count: state.started_jobs.len(),
            succeeded_job_count: state.succeeded_jobs.len(),
            failed_job_count: state.failed_jobs.len(),
            missing_job_count: missing_jobs.len(),
            extra_job_count: state.extra_jobs.len(),
            duplicate_succeeded_job_count: state.duplicate_succeeded_jobs.len(),
            empty_job_count: state.empty_job_count,
            request_count_total: state.request_count_total,
            source_record_count: state.source_record_count,
            raw_response_preserved: state.raw_response_preserved_all,
        },
        providers: provider_rows,
        missing_jobs,
        extra_jobs: state.extra_jobs,
        duplicate_succeeded_jobs: state.duplicate_succeeded_jobs,
        blockers,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        evidence_limitations: vec![
            "coverage_ledger_only".to_owned(),
            "does_not_promote_silver_gold_national_tables".to_owned(),
            "does_not_approve_production_cutover".to_owned(),
            "does_not_mark_national_rollout_complete".to_owned(),
        ],
        next_gates: vec!["silver-gold-national-promotion".to_owned()],
    })
}

fn validate_plan(plan: &JsonValue, plan_hash: &str, blockers: &mut Vec<String>) {
    add_if(
        blockers,
        string_property(plan, "schema_version") != PLAN_SCHEMA_VERSION,
        "plan schema mismatch",
    );
    add_if(
        blockers,
        string_property(plan, "status") != "ready",
        "plan status must be ready",
    );
    add_if(
        blockers,
        string_property(plan, "run_mode") != "national",
        "plan run_mode must be national",
    );
    add_if(
        blockers,
        bool_property(plan, "completion_claim_allowed", true),
        "plan completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(plan, "production_cutover_allowed", true),
        "plan production_cutover_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(plan, "national_rollout_allowed", true),
        "plan national_rollout_allowed must be false",
    );
    add_if(
        blockers,
        !is_lowercase_sha256(plan_hash),
        "plan compiler_input_hash_sha256 must be sha256",
    );
}

fn read_ledger_rows(
    resolved_ledger_path: Option<&Path>,
    blockers: &mut Vec<String>,
) -> anyhow::Result<Vec<JsonValue>> {
    let Some(path) = resolved_ledger_path else {
        return Ok(Vec::new());
    };
    if !path.is_file() {
        return Ok(Vec::new());
    }
    add_forbidden_token_blockers(path, "execution ledger", blockers);
    read_jsonl_file(path, "execution ledger", blockers)
}

fn validate_ledger_rows(
    ledger_rows: &[JsonValue],
    plan_hash: &str,
    provider_buckets: &mut BTreeMap<String, ProviderBucket>,
    planned_jobs: &mut BTreeMap<String, JsonValue>,
    planned_scopes: &mut BTreeSet<String>,
    blockers: &mut Vec<String>,
) {
    for row in ledger_rows {
        let job_id = string_property(row, "job_id");
        let scope_unit_id = string_property(row, "scope_unit_id");
        let provider = string_property(row, "provider");
        let endpoint = string_property(row, "endpoint");
        add_if(
            blockers,
            string_property(row, "schema_version") != LEDGER_ENTRY_SCHEMA_VERSION,
            "ledger entry schema mismatch",
        );
        add_if(
            blockers,
            string_property(row, "compiler_input_hash_sha256") != plan_hash,
            &format!("ledger entry compiler hash mismatch: {job_id}"),
        );
        add_if(
            blockers,
            string_property(row, "status") != "planned",
            &format!("ledger entry status must be planned: {job_id}"),
        );
        add_if(
            blockers,
            job_id.trim().is_empty(),
            "ledger entry job_id is required",
        );
        add_if(
            blockers,
            scope_unit_id.trim().is_empty(),
            &format!("ledger entry scope_unit_id is required: {job_id}"),
        );
        add_if(
            blockers,
            provider.trim().is_empty(),
            &format!("ledger entry provider is required: {job_id}"),
        );
        add_if(
            blockers,
            endpoint.trim().is_empty(),
            &format!("ledger entry endpoint is required: {job_id}"),
        );
        if !job_id.trim().is_empty() && planned_jobs.insert(job_id.clone(), row.clone()).is_some() {
            blockers.push(format!("duplicate planned job: {job_id}"));
        }
        if !scope_unit_id.trim().is_empty() {
            planned_scopes.insert(scope_unit_id);
        }
        get_or_create_provider_bucket(provider_buckets, &provider, &endpoint).planned_job_count +=
            1;
    }
}

fn resolve_evidence_paths(config: &CheckConfig) -> anyhow::Result<Vec<PathBuf>> {
    if !config.evidence_paths.is_empty() {
        let mut paths = Vec::new();
        for path in &config.evidence_paths {
            paths.push(resolve_repo_path(&config.root, path, "EvidencePaths")?);
        }
        paths.sort();
        return Ok(paths);
    }

    let glob_text = config.evidence_glob.to_string_lossy().replace('\\', "/");
    let (parent_text, file_pattern) = glob_text
        .rsplit_once('/')
        .map_or((".", glob_text.as_str()), |(parent, file)| (parent, file));
    let parent = resolve_repo_path(&config.root, &PathBuf::from(parent_text), "EvidenceGlob")?;
    if !parent.is_dir() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in
        fs::read_dir(&parent).with_context(|| format!("failed to read {}", parent.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", parent.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if file_name.contains("coverage-ledger") {
            continue;
        }
        if wildcard_matches(file_pattern, &file_name) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

#[allow(clippy::too_many_arguments)]
fn inspect_execution_evidence(
    config: &CheckConfig,
    evidence_path: &Path,
    plan_hash: &str,
    planned_jobs: &BTreeMap<String, JsonValue>,
    provider_buckets: &mut BTreeMap<String, ProviderBucket>,
    state: &mut CoverageState,
    blockers: &mut Vec<String>,
) -> anyhow::Result<()> {
    if !evidence_path.is_file() {
        blockers.push(format!(
            "execution evidence file missing: {}",
            repo_relative_path(&config.root, evidence_path)
        ));
        return Ok(());
    }

    let evidence_relative_path = repo_relative_path(&config.root, evidence_path);
    add_forbidden_token_blockers(evidence_path, "execution evidence", blockers);
    let evidence = read_json(
        evidence_path,
        "national data collection ledger execution evidence",
    )?;

    if !config.explicit_evidence_paths
        && !evidence_references_current_plan(config, &evidence, plan_hash)?
    {
        state.stale_evidence_paths.push(evidence_relative_path);
        return Ok(());
    }

    state.evidence_paths.push(evidence_relative_path.clone());
    validate_execution_evidence_header(&evidence, &evidence_relative_path, blockers);

    if !bool_property(&evidence, "raw_response_preserved", false) {
        state.raw_response_preserved_all = false;
        blockers.push(format!(
            "execution evidence raw_response_preserved must be true: {evidence_relative_path}"
        ));
    }

    let evidence_plan = evidence.get("plan").unwrap_or(&JsonValue::Null);
    let evidence_plan_path = string_property(evidence_plan, "path");
    let evidence_plan_hash = string_property(evidence_plan, "compiler_input_hash_sha256");
    add_if(
        blockers,
        evidence_plan_hash != plan_hash,
        &format!("execution evidence plan compiler hash mismatch: {evidence_relative_path}"),
    );
    if evidence_plan_path.trim().is_empty() {
        blockers.push(format!(
            "execution evidence plan.path is required: {evidence_relative_path}"
        ));
    } else {
        let actual_plan_path = normalized_repo_path(&config.root, &evidence_plan_path)?;
        let expected_plan_path = repo_relative_path(&config.root, &config.plan_path);
        add_if(
            blockers,
            actual_plan_path != expected_plan_path,
            &format!("execution evidence plan path mismatch: {evidence_relative_path}"),
        );
    }

    let evidence_selected = i64_property(&evidence, "selected_job_count", 0);
    let evidence_succeeded = i64_property(&evidence, "succeeded_job_count", 0);
    let evidence_failed = i64_property(&evidence, "failed_job_count", 0);
    let evidence_empty = i64_property(&evidence, "empty_job_count", 0);
    let evidence_request_total = i64_property(&evidence, "request_count_total", 0);
    let evidence_source_record_total = i64_property(&evidence, "source_record_count", 0);
    state.selected_job_count += evidence_selected;
    state.failed_job_count += evidence_failed;
    state.empty_job_count += evidence_empty;
    state.request_count_total += evidence_request_total;
    state.source_record_count += evidence_source_record_total;

    let event_log = evidence.get("event_log").unwrap_or(&JsonValue::Null);
    let event_log_path = string_property(event_log, "path");
    let expected_event_count = i64_property(event_log, "entry_count", 0);
    if event_log_path.trim().is_empty() {
        blockers.push(format!(
            "event log path is required: {evidence_relative_path}"
        ));
        return Ok(());
    }
    let resolved_event_log_path = resolve_repo_path(
        &config.root,
        &PathBuf::from(&event_log_path),
        "event_log.path",
    )?;
    if !resolved_event_log_path.is_file() {
        blockers.push(format!("event log file missing: {event_log_path}"));
        return Ok(());
    }

    add_forbidden_token_blockers(&resolved_event_log_path, "event log", blockers);
    let events = read_jsonl_file(&resolved_event_log_path, "event log", blockers)?;
    add_if(
        blockers,
        expected_event_count != i64::try_from(events.len()).unwrap_or(i64::MAX),
        &format!("event log entry_count must match rows: {event_log_path}"),
    );

    let event_summary = inspect_events(
        &events,
        plan_hash,
        planned_jobs,
        provider_buckets,
        state,
        blockers,
    );
    add_if(
        blockers,
        event_summary.succeeded_seen != evidence_succeeded,
        &format!("succeeded event count must match evidence: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        event_summary.failed_seen != evidence_failed,
        &format!("failed event count must match evidence: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        event_summary.succeeded_seen + event_summary.failed_seen != evidence_selected,
        &format!("selected event count must match evidence: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        event_summary.request_seen != evidence_request_total,
        &format!("event request sum must match evidence: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        event_summary.source_record_seen != evidence_source_record_total,
        &format!("event source_record sum must match evidence: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        event_summary.empty_seen != evidence_empty,
        &format!("event empty job count must match evidence: {evidence_relative_path}"),
    );

    Ok(())
}

fn evidence_references_current_plan(
    config: &CheckConfig,
    evidence: &JsonValue,
    plan_hash: &str,
) -> anyhow::Result<bool> {
    let evidence_plan = evidence.get("plan").unwrap_or(&JsonValue::Null);
    let evidence_plan_path = string_property(evidence_plan, "path");
    let evidence_plan_hash = string_property(evidence_plan, "compiler_input_hash_sha256");
    if evidence_plan_hash != plan_hash || evidence_plan_path.trim().is_empty() {
        return Ok(false);
    }

    let actual_plan_path = normalized_repo_path(&config.root, &evidence_plan_path)?;
    let expected_plan_path = repo_relative_path(&config.root, &config.plan_path);
    Ok(actual_plan_path == expected_plan_path)
}

fn validate_execution_evidence_header(
    evidence: &JsonValue,
    evidence_relative_path: &str,
    blockers: &mut Vec<String>,
) {
    add_if(
        blockers,
        string_property(evidence, "schema_version") != EXECUTION_EVIDENCE_SCHEMA_VERSION,
        &format!("execution evidence schema mismatch: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        string_property(evidence, "status") != "ready",
        &format!("execution evidence status must be ready: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        !bool_property(evidence, "executed", false),
        &format!("execution evidence must be executed: {evidence_relative_path}"),
    );
    add_if(
        blockers,
        bool_property(evidence, "completion_claim_allowed", true),
        "execution evidence completion_claim_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(evidence, "production_cutover_allowed", true),
        "execution evidence production_cutover_allowed must be false",
    );
    add_if(
        blockers,
        bool_property(evidence, "national_rollout_allowed", true),
        "execution evidence national_rollout_allowed must be false",
    );
}

fn inspect_events(
    events: &[JsonValue],
    plan_hash: &str,
    planned_jobs: &BTreeMap<String, JsonValue>,
    provider_buckets: &mut BTreeMap<String, ProviderBucket>,
    state: &mut CoverageState,
    blockers: &mut Vec<String>,
) -> EventSummary {
    let mut summary = EventSummary::default();
    for event in events {
        let job_id = string_property(event, "job_id");
        let scope_unit_id = string_property(event, "scope_unit_id");
        let status = string_property(event, "status");

        add_if(
            blockers,
            string_property(event, "schema_version") != EVENT_SCHEMA_VERSION,
            "event schema mismatch",
        );
        add_if(
            blockers,
            string_property(event, "compiler_input_hash_sha256") != plan_hash,
            &format!("event compiler hash mismatch: {job_id}"),
        );
        add_if(
            blockers,
            job_id.trim().is_empty(),
            "event job_id is required",
        );
        add_if(
            blockers,
            scope_unit_id.trim().is_empty(),
            &format!("event scope_unit_id is required: {job_id}"),
        );
        add_if(
            blockers,
            !matches!(status.as_str(), "running" | "succeeded" | "failed"),
            &format!("event status invalid: {job_id}"),
        );

        let planned_row = planned_jobs.get(&job_id);
        if let Some(row) = planned_row {
            let planned_scope_unit_id = string_property(row, "scope_unit_id");
            add_if(
                blockers,
                scope_unit_id != planned_scope_unit_id,
                &format!("event scope_unit_id mismatch: {job_id}"),
            );
        } else if status == "succeeded" {
            add_unique_string(&mut state.extra_job_set, &mut state.extra_jobs, &job_id);
            blockers.push(format!("succeeded event not in plan: {job_id}"));
        }

        if status == "running" {
            state.started_jobs.insert(job_id.clone());
            if let Some(row) = planned_row {
                let bucket = bucket_for_planned(provider_buckets, row);
                bucket.started_job_count += 1;
            }
        }

        if status == "succeeded" {
            inspect_succeeded_event(event, planned_row, provider_buckets, state, blockers);
            summary.succeeded_seen += 1;
            let event_request_count = i64_property(event, "request_count", -1);
            let event_source_record_count = i64_property(event, "source_record_count", -1);
            if event_request_count > 0 {
                summary.request_seen += event_request_count;
            }
            if event_source_record_count >= 0 {
                summary.source_record_seen += event_source_record_count;
                if event_source_record_count == 0 {
                    summary.empty_seen += 1;
                }
            }
        }

        if status == "failed" {
            summary.failed_seen += 1;
            state.failed_jobs.insert(job_id.clone());
            blockers.push(format!("failed job: {job_id}"));
            if let Some(row) = planned_row {
                let bucket = bucket_for_planned(provider_buckets, row);
                bucket.failed_job_count += 1;
                bucket.selected_job_count += 1;
            }
        }
    }
    summary
}

fn inspect_succeeded_event(
    event: &JsonValue,
    planned_row: Option<&JsonValue>,
    provider_buckets: &mut BTreeMap<String, ProviderBucket>,
    state: &mut CoverageState,
    blockers: &mut Vec<String>,
) {
    let job_id = string_property(event, "job_id");
    if !state.succeeded_jobs.insert(job_id.clone()) {
        add_unique_string(
            &mut state.duplicate_succeeded_set,
            &mut state.duplicate_succeeded_jobs,
            &job_id,
        );
        blockers.push(format!("duplicate succeeded event: {job_id}"));
    }

    let event_request_count = i64_property(event, "request_count", -1);
    let event_source_record_count = i64_property(event, "source_record_count", -1);
    let bronze_object_key = string_property(event, "bronze_object_key");
    add_if(
        blockers,
        event_request_count < 1,
        &format!("succeeded event request_count must be positive: {job_id}"),
    );
    add_if(
        blockers,
        event_source_record_count < 0,
        &format!("succeeded event source_record_count must be non-negative: {job_id}"),
    );
    add_if(
        blockers,
        bronze_object_key.trim().is_empty(),
        "succeeded event must include bronze_object_key",
    );
    if bronze_object_key.trim().is_empty() {
        state.raw_response_preserved_all = false;
    }
    add_if(
        blockers,
        string_property(event, "storage_driver").trim().is_empty(),
        &format!("succeeded event storage_driver is required: {job_id}"),
    );

    if let Some(row) = planned_row {
        let bucket = bucket_for_planned(provider_buckets, row);
        bucket.succeeded_job_count += 1;
        bucket.request_count_total += event_request_count;
        bucket.source_record_count += event_source_record_count;
        if event_source_record_count == 0 {
            bucket.empty_job_count += 1;
        }
        bucket.selected_job_count += 1;
    }
}

fn read_jsonl_file(
    path: &Path,
    label: &str,
    blockers: &mut Vec<String>,
) -> anyhow::Result<Vec<JsonValue>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line_number = index + 1;
        let line = if line_number == 1 {
            raw_line.trim_start_matches('\u{feff}')
        } else {
            raw_line
        };
        if line.trim().is_empty() {
            blockers.push(format!("{label} line {line_number} must not be blank"));
            continue;
        }
        match serde_json::from_str::<JsonValue>(line) {
            Ok(row) => rows.push(row),
            Err(_) => blockers.push(format!("{label} line {line_number} is not valid JSON")),
        }
    }
    Ok(rows)
}

fn add_forbidden_token_blockers(path: &Path, label: &str, blockers: &mut Vec<String>) {
    if !path.is_file() {
        return;
    }
    if let Ok(content) = fs::read_to_string(path) {
        for token in FORBIDDEN_TOKENS {
            if content.contains(token) {
                blockers.push(format!("{label} must not contain forbidden token: {token}"));
            }
        }
    }
}

fn normalized_repo_path(root: &Path, value: &str) -> anyhow::Result<String> {
    let resolved = resolve_repo_path(root, &PathBuf::from(value), "path")?;
    Ok(repo_relative_path(root, &resolved))
}

fn get_or_create_provider_bucket<'a>(
    buckets: &'a mut BTreeMap<String, ProviderBucket>,
    provider: &str,
    endpoint: &str,
) -> &'a mut ProviderBucket {
    let key = format!("{provider}\t{endpoint}");
    buckets
        .entry(key)
        .or_insert_with(|| ProviderBucket::new(provider, endpoint))
}

fn bucket_for_planned<'a>(
    buckets: &'a mut BTreeMap<String, ProviderBucket>,
    planned_row: &JsonValue,
) -> &'a mut ProviderBucket {
    let provider = string_property(planned_row, "provider");
    let endpoint = string_property(planned_row, "endpoint");
    get_or_create_provider_bucket(buckets, &provider, &endpoint)
}

fn add_unique_string(set: &mut BTreeSet<String>, list: &mut Vec<String>, value: &str) {
    if set.insert(value.to_owned()) {
        list.push(value.to_owned());
    }
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
    }
}

fn string_property(value: &JsonValue, name: &str) -> String {
    value
        .get(name)
        .map(|property| match property {
            JsonValue::String(text) => text.clone(),
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(number) => number.to_string(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn i64_property(value: &JsonValue, name: &str, default: i64) -> i64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_i64().or_else(|| {
                number
                    .as_u64()
                    .and_then(|number| i64::try_from(number).ok())
            }),
            JsonValue::String(text) => text.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn bool_property(value: &JsonValue, name: &str, default: bool) -> bool {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Bool(flag) => Some(*flag),
            JsonValue::String(text) => text.trim().parse::<bool>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }
    let parts = pattern.split('*').collect::<Vec<_>>();
    let mut remaining = value;
    if let Some(first) = parts.first().copied() {
        if !first.is_empty() {
            let Some(stripped) = remaining.strip_prefix(first) else {
                return false;
            };
            remaining = stripped;
        }
    }
    for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if part.is_empty() {
            continue;
        }
        let Some(position) = remaining.find(part) else {
            return false;
        };
        remaining = &remaining[position + part.len()..];
    }
    if let Some(last) = parts.last().copied() {
        if !last.is_empty() {
            return remaining.ends_with(last);
        }
    }
    true
}

#[derive(Default)]
struct CoverageState {
    started_jobs: BTreeSet<String>,
    succeeded_jobs: BTreeSet<String>,
    failed_jobs: BTreeSet<String>,
    extra_job_set: BTreeSet<String>,
    extra_jobs: Vec<String>,
    duplicate_succeeded_set: BTreeSet<String>,
    duplicate_succeeded_jobs: Vec<String>,
    selected_job_count: i64,
    failed_job_count: i64,
    empty_job_count: i64,
    request_count_total: i64,
    source_record_count: i64,
    raw_response_preserved_all: bool,
    evidence_paths: Vec<String>,
    stale_evidence_paths: Vec<String>,
}

impl CoverageState {
    fn default() -> Self {
        Self {
            raw_response_preserved_all: true,
            ..Default::default()
        }
    }
}

#[derive(Default)]
struct EventSummary {
    succeeded_seen: i64,
    failed_seen: i64,
    request_seen: i64,
    source_record_seen: i64,
    empty_seen: i64,
}

#[derive(Serialize)]
struct CheckReport {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    plan: PlanReport,
    evidence: EvidenceReport,
    coverage: CoverageReport,
    providers: Vec<ProviderBucket>,
    missing_jobs: Vec<String>,
    extra_jobs: Vec<String>,
    duplicate_succeeded_jobs: Vec<String>,
    blockers: Vec<String>,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    evidence_limitations: Vec<String>,
    next_gates: Vec<String>,
}

impl CheckReport {
    fn skipped(root: &Path, plan_path: &Path, reason: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(root),
            status: "skipped",
            plan: PlanReport {
                path: repo_relative_path(root, plan_path),
                compiler_input_hash_sha256: String::new(),
                ledger_path: String::new(),
            },
            evidence: EvidenceReport {
                file_count: 0,
                stale_file_count: 0,
                paths: Vec::new(),
                stale_paths: Vec::new(),
            },
            coverage: CoverageReport {
                planned_job_count: 0,
                planned_scope_count: 0,
                selected_job_count: 0,
                started_job_count: 0,
                succeeded_job_count: 0,
                failed_job_count: 0,
                missing_job_count: 0,
                extra_job_count: 0,
                duplicate_succeeded_job_count: 0,
                empty_job_count: 0,
                request_count_total: 0,
                source_record_count: 0,
                raw_response_preserved: false,
            },
            providers: Vec::new(),
            missing_jobs: Vec::new(),
            extra_jobs: Vec::new(),
            duplicate_succeeded_jobs: Vec::new(),
            blockers: vec![reason.to_owned()],
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            evidence_limitations: vec!["coverage_ledger_not_evaluated".to_owned()],
            next_gates: vec!["national-data-collection-coverage-ledger".to_owned()],
        }
    }
}

#[derive(Serialize)]
struct PlanReport {
    path: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    compiler_input_hash_sha256: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    ledger_path: String,
}

#[derive(Serialize)]
struct EvidenceReport {
    file_count: usize,
    stale_file_count: usize,
    paths: Vec<String>,
    stale_paths: Vec<String>,
}

#[derive(Serialize)]
struct CoverageReport {
    planned_job_count: usize,
    planned_scope_count: usize,
    selected_job_count: i64,
    started_job_count: usize,
    succeeded_job_count: usize,
    failed_job_count: usize,
    missing_job_count: usize,
    extra_job_count: usize,
    duplicate_succeeded_job_count: usize,
    empty_job_count: i64,
    request_count_total: i64,
    source_record_count: i64,
    raw_response_preserved: bool,
}

#[derive(Clone, Serialize)]
struct ProviderBucket {
    provider: String,
    endpoint: String,
    planned_job_count: i64,
    selected_job_count: i64,
    started_job_count: i64,
    succeeded_job_count: i64,
    failed_job_count: i64,
    empty_job_count: i64,
    request_count_total: i64,
    source_record_count: i64,
}

impl ProviderBucket {
    fn new(provider: &str, endpoint: &str) -> Self {
        Self {
            provider: provider.to_owned(),
            endpoint: endpoint.to_owned(),
            planned_job_count: 0,
            selected_job_count: 0,
            started_job_count: 0,
            succeeded_job_count: 0,
            failed_job_count: 0,
            empty_job_count: 0,
            request_count_total: 0,
            source_record_count: 0,
        }
    }
}
