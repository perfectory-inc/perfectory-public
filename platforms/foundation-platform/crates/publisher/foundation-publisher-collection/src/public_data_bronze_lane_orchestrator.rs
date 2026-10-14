use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, Write},
    path::PathBuf,
    time::Instant,
};

use anyhow::{bail, Context};
use futures_util::{stream, StreamExt};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tokio::process::Command as TokioCommand;

use crate::public_data_bronze_lane_registry::{
    validate_lane_control_contract, LaneDefinition, LaneRegistryDocument,
    LANE_REGISTRY_SCHEMA_VERSION,
};
use crate::public_data_control_support::{
    env_path, git_head, optional_env_value, read_json, repo_relative_path, resolve_repo_path,
    utc_now, write_json_file,
};

const ORCHESTRATION_SCHEMA_VERSION: &str =
    "foundation-platform.public_data_bronze_lane_orchestration.v1";
const DEFAULT_OUTPUT_PATH: &str =
    "target/audit/public-data-bronze-lane-orchestration-evidence.json";
const DEFAULT_LANE_REGISTRY_PATH: &str = "docs/catalog/public-data-bronze-lane-registry.v1.json";

pub async fn run() -> anyhow::Result<()> {
    let config = OrchestratorConfig::from_env()?;
    let registry_value: JsonValue = read_json(&config.lane_registry_path, "lane registry")?;
    let registry: LaneRegistryDocument = serde_json::from_value(registry_value)
        .context("failed to parse public data Bronze lane registry")?;

    let selected_lanes = select_lanes(&registry, &config.include_lane_ids, &config.skip_lane_ids);
    let effective_max_concurrent_lanes =
        effective_max_concurrent_lanes(config.max_concurrent_lanes, selected_lanes.len());
    let blockers = execution_blockers(&registry, &config, selected_lanes.len());
    if !blockers.is_empty() {
        return write_blocked_plan(
            &config,
            &selected_lanes,
            effective_max_concurrent_lanes,
            &blockers,
        );
    }

    if !config.execute {
        return write_planned(&config, &selected_lanes, effective_max_concurrent_lanes);
    }

    execute_and_report(config, selected_lanes, effective_max_concurrent_lanes).await
}

fn execution_blockers(
    registry: &LaneRegistryDocument,
    config: &OrchestratorConfig,
    selected_lane_count: usize,
) -> Vec<String> {
    let mut blockers = validate_registry(registry);
    if selected_lane_count == 0 {
        blockers.push("at least one public data Bronze lane must be selected".to_owned());
    }
    if effective_max_concurrent_lanes(config.max_concurrent_lanes, selected_lane_count) == 0 {
        blockers.push("MaxConcurrentLanes must be zero for auto or greater than zero".to_owned());
    }
    if config.execute && !config.confirm_execution {
        blockers.push("Execute requires ConfirmPublicDataBronzeLaneExecution".to_owned());
    }
    blockers
}

const fn effective_max_concurrent_lanes(configured: usize, selected_lane_count: usize) -> usize {
    if configured == 0 {
        selected_lane_count
    } else {
        configured
    }
}

fn planned_lane_reports(selected_lanes: &[SelectedLane]) -> Vec<LaneReport> {
    selected_lanes
        .iter()
        .map(LaneReport::planned)
        .collect::<Vec<_>>()
}

fn write_blocked_plan(
    config: &OrchestratorConfig,
    selected_lanes: &[SelectedLane],
    effective_max_concurrent_lanes: usize,
    blockers: &[String],
) -> anyhow::Result<()> {
    write_report(
        config,
        "blocked",
        false,
        effective_max_concurrent_lanes,
        planned_lane_reports(selected_lanes),
        blockers.to_owned(),
    )?;
    bail!(
        "public data Bronze lane orchestration blocked: {}",
        blockers[0]
    );
}

fn write_planned(
    config: &OrchestratorConfig,
    selected_lanes: &[SelectedLane],
    effective_max_concurrent_lanes: usize,
) -> anyhow::Result<()> {
    write_report(
        config,
        "planned",
        false,
        effective_max_concurrent_lanes,
        planned_lane_reports(selected_lanes),
        Vec::new(),
    )?;
    writeln!(
        io::stdout().lock(),
        "public-data-bronze-lanes-planned lanes={} output={}",
        selected_lanes.len(),
        repo_relative_path(&config.root, &config.output_path)
    )?;
    Ok(())
}

async fn execute_and_report(
    config: OrchestratorConfig,
    selected_lanes: Vec<SelectedLane>,
    effective_max_concurrent_lanes: usize,
) -> anyhow::Result<()> {
    let lane_executor_exe = resolve_lane_executor_exe(config.lane_executor_exe.clone())?;
    let lane_reports = execute_lanes(
        selected_lanes,
        effective_max_concurrent_lanes,
        lane_executor_exe,
        config.root.clone(),
    )
    .await;
    let failed_lanes = lane_reports
        .iter()
        .filter(|lane| lane.status != "ready")
        .map(|lane| format!("lane failed: {}", lane.lane_id))
        .collect::<Vec<_>>();
    if !failed_lanes.is_empty() {
        write_report(
            &config,
            "blocked",
            true,
            effective_max_concurrent_lanes,
            lane_reports,
            failed_lanes.clone(),
        )?;
        bail!(
            "public data Bronze lane orchestration blocked failed_lanes={} output={}",
            failed_lanes.len(),
            repo_relative_path(&config.root, &config.output_path)
        );
    }

    let lane_count = lane_reports.len();
    write_report(
        &config,
        "ready",
        true,
        effective_max_concurrent_lanes,
        lane_reports,
        Vec::new(),
    )?;
    writeln!(
        io::stdout().lock(),
        "public-data-bronze-lanes-ok lanes={} output={}",
        lane_count,
        repo_relative_path(&config.root, &config.output_path)
    )?;
    Ok(())
}

#[derive(Clone, Debug)]
struct OrchestratorConfig {
    root: PathBuf,
    output_path: PathBuf,
    lane_registry_path: PathBuf,
    lane_executor_exe: Option<PathBuf>,
    max_concurrent_lanes: usize,
    execute: bool,
    confirm_execution: bool,
    include_lane_ids: Vec<String>,
    skip_lane_ids: Vec<String>,
}

impl OrchestratorConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        let lane_executor_exe =
            optional_env_value("FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTOR_EXE")?
                .map(PathBuf::from);
        Ok(Self {
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_ORCHESTRATION_OUTPUT_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "output path",
            )?,
            lane_registry_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_REGISTRY_PATH",
                    DEFAULT_LANE_REGISTRY_PATH,
                )?,
                "lane registry path",
            )?,
            lane_executor_exe,
            max_concurrent_lanes: parse_usize_env(
                "FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_MAX_CONCURRENT_LANES",
                0,
            )?,
            execute: env_flag("FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_LANE_EXECUTE")?,
            confirm_execution: env_flag(
                "FOUNDATION_PLATFORM_CONFIRM_PUBLIC_DATA_BRONZE_LANE_EXECUTION",
            )?,
            include_lane_ids: parse_id_list_env(
                "FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_INCLUDE_LANE_IDS",
            )?,
            skip_lane_ids: parse_id_list_env(
                "FOUNDATION_PLATFORM_PUBLIC_DATA_BRONZE_SKIP_LANE_IDS",
            )?,
            root,
        })
    }
}

#[derive(Clone, Debug)]
struct SelectedLane {
    sequence: usize,
    lane: LaneDefinition,
}

#[derive(Debug, Serialize)]
struct OrchestrationEvidence {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    executed: bool,
    max_concurrent_lanes: usize,
    lane_registry_path: String,
    selected_lane_count: usize,
    succeeded_lane_count: usize,
    failed_lane_count: usize,
    output_path: String,
    lanes: Vec<LaneReport>,
    blockers: Vec<String>,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
}

#[derive(Clone, Debug, Serialize)]
struct LaneReport {
    sequence: usize,
    lane_id: String,
    provider: String,
    source_acquisition_lanes: Vec<String>,
    endpoint_groups: Vec<String>,
    command: String,
    command_args: Vec<String>,
    inner_parallel_env: String,
    environment_keys: Vec<String>,
    status: String,
    exit_code: Option<i32>,
    started_at_utc: Option<String>,
    finished_at_utc: Option<String>,
    duration_ms: Option<u64>,
    output: String,
}

impl LaneReport {
    fn planned(selected: &SelectedLane) -> Self {
        Self {
            sequence: selected.sequence,
            lane_id: selected.lane.lane_id.clone(),
            provider: selected.lane.provider.clone(),
            source_acquisition_lanes: selected.lane.source_acquisition_lanes.clone(),
            endpoint_groups: selected.lane.endpoint_groups.clone(),
            command: selected.lane.command.clone(),
            command_args: selected.lane.command_args_or_default(),
            inner_parallel_env: selected.lane.inner_parallel_env.clone(),
            environment_keys: environment_keys(&selected.lane.environment),
            status: "planned".to_owned(),
            exit_code: None,
            started_at_utc: None,
            finished_at_utc: None,
            duration_ms: None,
            output: String::new(),
        }
    }
}

fn validate_registry(registry: &LaneRegistryDocument) -> Vec<String> {
    let mut blockers = Vec::new();
    if registry.schema_version != LANE_REGISTRY_SCHEMA_VERSION {
        blockers.push("lane registry schema_version mismatch".to_owned());
    }
    if registry.status != "ready" {
        blockers.push("lane registry status must be ready".to_owned());
    }
    blockers.extend(validate_lane_control_contract(registry));
    blockers
}

fn select_lanes(
    registry: &LaneRegistryDocument,
    include_lane_ids: &[String],
    skip_lane_ids: &[String],
) -> Vec<SelectedLane> {
    registry
        .lanes
        .iter()
        .enumerate()
        .filter(|(_, lane)| lane.status == "enabled")
        .filter(|(_, lane)| lane.include_by_default || !include_lane_ids.is_empty())
        .filter(|(_, lane)| include_lane_ids.is_empty() || include_lane_ids.contains(&lane.lane_id))
        .filter(|(_, lane)| !skip_lane_ids.contains(&lane.lane_id))
        .map(|(index, lane)| SelectedLane {
            sequence: index + 1,
            lane: lane.clone(),
        })
        .collect()
}

async fn execute_lanes(
    selected_lanes: Vec<SelectedLane>,
    max_concurrent_lanes: usize,
    lane_executor_exe: PathBuf,
    root: PathBuf,
) -> Vec<LaneReport> {
    let mut reports = stream::iter(selected_lanes)
        .map(|lane| {
            let lane_executor_exe = lane_executor_exe.clone();
            let root = root.clone();
            async move { execute_lane(lane, lane_executor_exe, root).await }
        })
        .buffer_unordered(max_concurrent_lanes)
        .collect::<Vec<_>>()
        .await;
    reports.sort_by_key(|report| report.sequence);
    reports
}

async fn execute_lane(
    selected: SelectedLane,
    lane_executor_exe: PathBuf,
    root: PathBuf,
) -> LaneReport {
    let started_at_utc = utc_now();
    let started = Instant::now();
    let command_args = selected.lane.command_args_or_default();
    let environment = selected.lane.environment.clone();
    let environment_keys = environment_keys(&environment);
    let output = run_lane_process(&lane_executor_exe, &command_args, &root, &environment).await;
    let finished_at_utc = utc_now();
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match output {
        Ok(process_output) => LaneReport {
            sequence: selected.sequence,
            lane_id: selected.lane.lane_id,
            provider: selected.lane.provider,
            source_acquisition_lanes: selected.lane.source_acquisition_lanes,
            endpoint_groups: selected.lane.endpoint_groups,
            command: selected.lane.command,
            command_args,
            inner_parallel_env: selected.lane.inner_parallel_env,
            environment_keys,
            status: if process_output.exit_code == 0 {
                "ready".to_owned()
            } else {
                "blocked".to_owned()
            },
            exit_code: Some(process_output.exit_code),
            started_at_utc: Some(started_at_utc),
            finished_at_utc: Some(finished_at_utc),
            duration_ms: Some(duration_ms),
            output: process_output.output,
        },
        Err(error) => LaneReport {
            sequence: selected.sequence,
            lane_id: selected.lane.lane_id,
            provider: selected.lane.provider,
            source_acquisition_lanes: selected.lane.source_acquisition_lanes,
            endpoint_groups: selected.lane.endpoint_groups,
            command: selected.lane.command,
            command_args,
            inner_parallel_env: selected.lane.inner_parallel_env,
            environment_keys,
            status: "blocked".to_owned(),
            exit_code: Some(1),
            started_at_utc: Some(started_at_utc),
            finished_at_utc: Some(finished_at_utc),
            duration_ms: Some(duration_ms),
            output: error.to_string(),
        },
    }
}

struct LaneProcessOutput {
    exit_code: i32,
    output: String,
}

async fn run_lane_process(
    lane_executor_exe: &PathBuf,
    command_args: &[String],
    root: &PathBuf,
    environment: &BTreeMap<String, String>,
) -> anyhow::Result<LaneProcessOutput> {
    let mut command = TokioCommand::new(lane_executor_exe);
    command.args(command_args);
    let output = command
        .current_dir(root)
        .envs(environment)
        .output()
        .await
        .with_context(|| format!("failed to run lane command {}", lane_executor_exe.display()))?;
    let mut combined = String::new();
    combined.push_str(&String::from_utf8_lossy(&output.stdout));
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok(LaneProcessOutput {
        exit_code: output.status.code().unwrap_or(1),
        output: combined.trim().to_owned(),
    })
}

fn environment_keys(environment: &BTreeMap<String, String>) -> Vec<String> {
    environment.keys().cloned().collect()
}

fn resolve_lane_executor_exe(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        if !path.as_os_str().is_empty() {
            return Ok(path);
        }
    }
    if let Some(path) = optional_env_value("FOUNDATION_PLATFORM_OUTBOX_PUBLISHER_EXE")? {
        return Ok(PathBuf::from(path));
    }
    env::current_exe().context("failed to resolve current outbox publisher executable")
}

fn write_report(
    config: &OrchestratorConfig,
    status: &'static str,
    executed: bool,
    max_concurrent_lanes: usize,
    lane_reports: Vec<LaneReport>,
    blockers: Vec<String>,
) -> anyhow::Result<()> {
    let succeeded_lane_count = lane_reports
        .iter()
        .filter(|lane| lane.status == "ready")
        .count();
    let failed_lane_count = lane_reports
        .iter()
        .filter(|lane| lane.status == "blocked")
        .count();
    let evidence = OrchestrationEvidence {
        schema_version: ORCHESTRATION_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status,
        executed,
        max_concurrent_lanes,
        lane_registry_path: repo_relative_path(&config.root, &config.lane_registry_path),
        selected_lane_count: lane_reports.len(),
        succeeded_lane_count,
        failed_lane_count,
        output_path: repo_relative_path(&config.root, &config.output_path),
        lanes: lane_reports,
        blockers,
        completion_claim_allowed: false,
        national_rollout_allowed: false,
    };
    write_json_file(&config.output_path, &evidence)
}

fn parse_usize_env(name: &str, default: usize) -> anyhow::Result<usize> {
    optional_env_value(name)?
        .map(|raw| {
            raw.parse::<usize>()
                .with_context(|| format!("{name} must be zero for auto or a positive integer"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn env_flag(name: &str) -> anyhow::Result<bool> {
    Ok(optional_env_value(name)?.as_deref() == Some("1"))
}

fn parse_id_list_env(name: &str) -> anyhow::Result<Vec<String>> {
    Ok(optional_env_value(name)?
        .map(|raw| {
            raw.split([';', ','])
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{select_lanes, LaneRegistryDocument, LaneReport};

    fn lane(lane_id: &str, status: &str, include_by_default: bool) -> serde_json::Value {
        json!({
            "lane_id": lane_id,
            "status": status,
            "include_by_default": include_by_default,
            "provider": "fixture",
            "source_acquisition_lanes": ["open_api_only"],
            "endpoint_groups": ["fixture_group"],
            "command": lane_id,
            "command_args": [lane_id],
            "inner_parallel_env": "FOUNDATION_PLATFORM_FIXTURE_MAX_IN_FLIGHT",
            "execution_gate": "ConfirmPublicDataBronzeLaneExecution",
            "full_download_gate_env": "FOUNDATION_PLATFORM_FIXTURE_CONFIRM_FULL_DOWNLOAD",
            "live_write_gate_env": "FOUNDATION_PLATFORM_FIXTURE_LIVE_WRITE",
            "planned_blocker": "fixture planned lane must be explicitly selected",
            "completion_claim_allowed": false,
            "national_rollout_allowed": false
        })
    }

    fn registry() -> anyhow::Result<LaneRegistryDocument> {
        Ok(serde_json::from_value(json!({
            "schema_version": "foundation-platform.public_data_bronze_lane_registry.v1",
            "status": "ready",
            "owner": "foundation-platform",
            "lanes": [
                lane("lane-a", "enabled", true),
                lane("lane-b", "enabled", true),
                lane("lane-c", "enabled", false),
                lane("lane-d", "planned", false)
            ]
        }))?)
    }

    #[test]
    fn lane_environment_is_registry_owned_and_reported_by_key_only() -> anyhow::Result<()> {
        let registry: LaneRegistryDocument = serde_json::from_value(json!({
            "schema_version": "foundation-platform.public_data_bronze_lane_registry.v1",
            "status": "ready",
            "owner": "foundation-platform",
            "lanes": [{
                "lane_id": "data-go-kr-api",
                "status": "enabled",
                "include_by_default": true,
                "provider": "data.go.kr",
                "source_acquisition_lanes": ["open_api_only"],
                "endpoint_groups": ["real_transaction_open_api"],
                "command": "execute-national-data-collection-async",
                "command_args": ["execute-national-data-collection-async"],
                "inner_parallel_env": "FOUNDATION_PLATFORM_NATIONAL_ASYNC_MAX_IN_FLIGHT",
                "execution_gate": "ConfirmPublicDataBronzeLaneExecution",
                "live_write_gate_env": "FOUNDATION_PLATFORM_NATIONAL_ASYNC_LIVE_WRITE",
                "completion_claim_allowed": false,
                "national_rollout_allowed": false,
                "environment": {
                    "FOUNDATION_PLATFORM_NATIONAL_ASYNC_PLAN_PATH": "target/audit/real-transaction-plan.json",
                    "FOUNDATION_PLATFORM_NATIONAL_ASYNC_EXECUTE": "1"
                }
            }]
        }))?;

        let selected = select_lanes(&registry, &[], &[]);
        let report = LaneReport::planned(&selected[0]);

        assert_eq!(
            selected[0]
                .lane
                .environment
                .get("FOUNDATION_PLATFORM_NATIONAL_ASYNC_PLAN_PATH")
                .map(String::as_str),
            Some("target/audit/real-transaction-plan.json")
        );
        assert_eq!(
            report.environment_keys,
            vec![
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_EXECUTE".to_owned(),
                "FOUNDATION_PLATFORM_NATIONAL_ASYNC_PLAN_PATH".to_owned()
            ]
        );
        Ok(())
    }

    #[test]
    fn selection_defaults_to_enabled_include_by_default_lanes() -> anyhow::Result<()> {
        let registry = registry()?;
        let selected = select_lanes(&registry, &[], &[]);

        assert_eq!(
            selected
                .iter()
                .map(|lane| lane.lane.lane_id.as_str())
                .collect::<Vec<_>>(),
            vec!["lane-a", "lane-b"]
        );
        Ok(())
    }

    #[test]
    fn explicit_include_can_select_enabled_non_default_lane() -> anyhow::Result<()> {
        let registry = registry()?;
        let selected = select_lanes(&registry, &["lane-c".to_owned()], &[]);

        assert_eq!(
            selected
                .iter()
                .map(|lane| lane.lane.lane_id.as_str())
                .collect::<Vec<_>>(),
            vec!["lane-c"]
        );
        Ok(())
    }

    #[test]
    fn skip_removes_selected_default_lane() -> anyhow::Result<()> {
        let registry = registry()?;
        let selected = select_lanes(&registry, &[], &["lane-b".to_owned()]);

        assert_eq!(
            selected
                .iter()
                .map(|lane| lane.lane.lane_id.as_str())
                .collect::<Vec<_>>(),
            vec!["lane-a"]
        );
        Ok(())
    }

    #[test]
    fn orchestrator_blocks_lane_registry_missing_rust_owned_execution_gate() -> anyhow::Result<()> {
        let mut registry = registry()?;
        registry.lanes[0].execution_gate.clear();

        let blockers = super::validate_registry(&registry);

        assert!(blockers
            .iter()
            .any(|blocker| blocker.contains("enabled lane execution_gate is required")));
        Ok(())
    }

    #[test]
    fn orchestrator_blocks_lane_registry_that_allows_completion_or_rollout() -> anyhow::Result<()> {
        let mut registry = registry()?;
        registry.lanes[0].completion_claim_allowed = Some(true);
        registry.lanes[1].national_rollout_allowed = None;

        let blockers = super::validate_registry(&registry);

        assert!(blockers
            .iter()
            .any(|blocker| blocker
                .contains("lane completion_claim_allowed must be explicitly false")));
        assert!(blockers
            .iter()
            .any(|blocker| blocker
                .contains("lane national_rollout_allowed must be explicitly false")));
        Ok(())
    }
}
