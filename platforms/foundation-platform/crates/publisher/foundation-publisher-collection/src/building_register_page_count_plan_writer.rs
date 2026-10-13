use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::public_api_metric_writer;
use crate::public_data_control_support::{
    env_path, git_head, repo_relative_path, resolve_repo_path, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.building_register_page_count_plan.v1";
const SCOPE_EVIDENCE_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope.v1";
const SCOPE_ROW_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope_row.v1";
const SOURCE: &str = "data-go-kr-building-register-page-count-probe";
const PROVIDER: &str = "data.go.kr";
const ENDPOINT_SLUG: &str = "data-go-kr-building-register-getBrTitleInfo";
const OPERATION: &str = "getBrTitleInfo";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    Writer::new(config)?.run()
}

struct Config {
    root: PathBuf,
    env_file: PathBuf,
    scope_jsonl_path: PathBuf,
    scope_evidence_path: PathBuf,
    output_path: PathBuf,
    probe_output_root: PathBuf,
    quota_metrics_path: Option<PathBuf>,
    cargo_exe: Option<PathBuf>,
    runner_exe: Option<PathBuf>,
    max_jobs: Option<usize>,
    skip_jobs: usize,
    request_cap: usize,
    building_num_of_rows: u32,
    max_in_flight: usize,
    execute: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let max_jobs = env_i64("FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_MAX_JOBS", 0)?;
        let skip_jobs = env_i64("FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_SKIP_JOBS", 0)?;
        let request_cap = env_i64(
            "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_REQUEST_CAP",
            1,
        )?;
        let building_num_of_rows = env_i64(
            "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_BUILDING_NUM_OF_ROWS",
            100,
        )?;
        let max_in_flight = env_i64(
            "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_MAX_IN_FLIGHT",
            1,
        )?;
        if skip_jobs < 0 {
            bail!("SkipJobs must be non-negative");
        }
        if max_jobs < 0 {
            bail!("MaxJobs must be non-negative");
        }
        if request_cap < 1 {
            bail!("RequestCap must be positive");
        }
        if !(1..=100).contains(&building_num_of_rows) {
            bail!("BuildingNumOfRows must be between 1 and 100");
        }
        if !(1..=16).contains(&max_in_flight) {
            bail!("MaxInFlight must be between 1 and 16");
        }
        let execute = env_bool(
            "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_EXECUTE",
            false,
        )?;
        let confirm_public_api_quota_impact = env_bool(
            "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_CONFIRM_PUBLIC_API_QUOTA_IMPACT",
            false,
        )?;
        let confirm_building_page_count_probe = env_bool(
            "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_CONFIRM_BUILDING_PAGE_COUNT_PROBE",
            false,
        )?;
        if execute && (!confirm_public_api_quota_impact || !confirm_building_page_count_probe) {
            bail!("ConfirmPublicApiQuotaImpact and ConfirmBuildingPageCountProbe are required with Execute");
        }
        Ok(Self {
            scope_jsonl_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_SCOPE_JSONL_PATH",
                    "target/audit/national-data-collection-scope.jsonl",
                )?,
                "ScopeJsonlPath",
            )?,
            scope_evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_SCOPE_EVIDENCE_PATH",
                    "target/audit/national-data-collection-scope-evidence.json",
                )?,
                "ScopeEvidencePath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_OUTPUT_PATH",
                    "target/audit/building-register-page-count-plan.json",
                )?,
                "OutputPath",
            )?,
            probe_output_root: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_PROBE_OUTPUT_ROOT",
                    "target/audit/building-register-page-count-probes",
                )?,
                "ProbeOutputRoot",
            )?,
            quota_metrics_path: optional_env_path(
                &root,
                "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_QUOTA_METRICS_PATH",
                "QuotaMetricsPath",
            )?,
            env_file: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_ENV_FILE",
                    ".env.local",
                )?,
                "EnvFile",
            )?,
            cargo_exe: optional_external_path(
                "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_CARGO_EXE",
            )?,
            runner_exe: optional_env_path(
                &root,
                "FOUNDATION_PLATFORM_BUILDING_PAGE_COUNT_PLAN_RUNNER_EXE",
                "RunnerExe",
            )?,
            root,
            max_jobs: if max_jobs == 0 {
                None
            } else {
                Some(usize::try_from(max_jobs).context("MaxJobs overflow")?)
            },
            skip_jobs: usize::try_from(skip_jobs).context("SkipJobs overflow")?,
            request_cap: usize::try_from(request_cap).context("RequestCap overflow")?,
            building_num_of_rows: u32::try_from(building_num_of_rows)
                .context("BuildingNumOfRows overflow")?,
            max_in_flight: usize::try_from(max_in_flight).context("MaxInFlight overflow")?,
            execute,
        })
    }
}

struct Writer {
    config: Config,
    scope_rows: Vec<ScopeRow>,
    scope_sha256: String,
    selected_rows: Vec<ScopeRow>,
}

impl Writer {
    fn new(config: Config) -> anyhow::Result<Self> {
        if config.output_path.is_file() {
            bail!(
                "building-register page count plan already exists: {}",
                repo_relative_path(&config.root, &config.output_path)
            );
        }
        let scope_rows = read_scope_rows(&config.scope_jsonl_path)?;
        let scope_sha256 = file_sha256(&config.scope_jsonl_path)?;
        validate_scope_evidence(&config, &scope_sha256, scope_rows.len())?;
        let selected_rows = select_scope_rows(
            &scope_rows,
            config.skip_jobs,
            config.max_jobs,
            config.request_cap,
        )?;
        Ok(Self {
            config,
            scope_rows,
            scope_sha256,
            selected_rows,
        })
    }

    fn run(&self) -> anyhow::Result<()> {
        if !self.config.execute {
            return self.write_planned();
        }
        if self.config.runner_exe.is_none() {
            return self.run_native_batch();
        }
        self.run_direct_probe_runner()
    }

    fn write_planned(&self) -> anyhow::Result<()> {
        let plan = self.plan_payload("planned", false, 0, Vec::new(), Vec::new());
        write_json_file(&self.config.output_path, &plan)?;
        self.print_written(
            "planned",
            self.selected_rows.len(),
            self.selected_rows.len(),
        );
        Ok(())
    }

    fn run_native_batch(&self) -> anyhow::Result<()> {
        let dotenv = import_dotenv(&self.config.env_file)?;
        require_env(&dotenv, "DATA_GO_KR_SERVICE_KEY")?;
        let cargo = resolve_cargo(self.config.cargo_exe.as_ref())?;
        let mut child_env = dotenv;
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS".to_owned(),
            self.config.building_num_of_rows.to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_SCOPE_JSONL_PATH".to_owned(),
            self.config.scope_jsonl_path.to_string_lossy().to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_SCOPE_EVIDENCE_PATH".to_owned(),
            self.config
                .scope_evidence_path
                .to_string_lossy()
                .to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_PLAN_OUTPUT_PATH".to_owned(),
            self.config.output_path.to_string_lossy().to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_PROBE_OUTPUT_ROOT".to_owned(),
            self.config.probe_output_root.to_string_lossy().to_string(),
        );
        if let Some(max_jobs) = self.config.max_jobs {
            child_env.insert(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_MAX_JOBS".to_owned(),
                max_jobs.to_string(),
            );
        } else {
            child_env.remove("FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_MAX_JOBS");
        }
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_SKIP_JOBS".to_owned(),
            self.config.skip_jobs.to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_REQUEST_CAP".to_owned(),
            self.config.request_cap.to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_MAX_IN_FLIGHT".to_owned(),
            self.config.max_in_flight.to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_CONFIRM_PUBLIC_API_QUOTA_IMPACT"
                .to_owned(),
            "1".to_owned(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_CONFIRM_PROBE".to_owned(),
            "1".to_owned(),
        );
        let output = outbox_subcommand(&cargo, "probe-building-register-page-count-batch")
            .current_dir(&self.config.root)
            .envs(&child_env)
            .output()
            .context("failed to run building-register page-count batch")?;
        if !output.status.success() {
            bail!(
                "building-register page-count batch runner failed exit_code={} output={}",
                output.status.code().unwrap_or(1),
                safe_output(&lines_from_output(&output.stdout, &output.stderr))
            );
        }
        let plan = read_json(
            &self.config.output_path,
            "building-register page count plan",
        )?;
        let status = string_prop(&plan, "status");
        let jobs = plan
            .get("jobs")
            .and_then(JsonValue::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        let requests = plan
            .get("request_plan")
            .map(|request_plan| u64_prop(request_plan, "request_count_estimate", jobs as u64))
            .unwrap_or(jobs as u64);
        self.print_written(
            &status,
            jobs,
            usize::try_from(requests).unwrap_or(usize::MAX),
        );
        Ok(())
    }

    fn run_direct_probe_runner(&self) -> anyhow::Result<()> {
        let runner = self
            .config
            .runner_exe
            .as_ref()
            .context("RunnerExe is required")?;
        if !runner.is_file() {
            bail!("RunnerExe does not exist: {}", runner.display());
        }
        let dotenv = import_dotenv(&self.config.env_file)?;
        let mut jobs = Vec::new();
        let mut failed_jobs = Vec::new();
        for row in &self.selected_rows {
            let job_id = row.job_id();
            let probe_path = self.config.probe_output_root.join(format!("{job_id}.json"));
            match self.run_single_probe(row, &job_id, &probe_path, runner, &dotenv) {
                Ok(result) => jobs.push(result),
                Err(error) => {
                    failed_jobs.push(FailedPageCountJob {
                        job_id,
                        scope_unit_id: row.scope_unit_id.clone(),
                        sigungu_cd: row.sigungu_cd.clone(),
                        bjdong_cd: row.bjdong_cd.clone(),
                        error_kind: "probe_failed".to_owned(),
                        error_message: safe_output(&[error.to_string()]),
                    });
                    break;
                }
            }
        }
        let status = if failed_jobs.is_empty() {
            "ready"
        } else {
            "blocked"
        };
        let attempted = jobs.len() + failed_jobs.len();
        let plan = self.plan_payload(status, true, attempted, jobs, failed_jobs);
        write_json_file(&self.config.output_path, &plan)?;
        self.print_written(status, plan.jobs.len(), self.selected_rows.len());
        if status == "blocked" {
            bail!("building-register page count plan blocked");
        }
        Ok(())
    }

    fn run_single_probe(
        &self,
        row: &ScopeRow,
        job_id: &str,
        probe_path: &Path,
        runner: &Path,
        dotenv: &BTreeMap<String, String>,
    ) -> anyhow::Result<PageCountJob> {
        let mut child_env = dotenv.clone();
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION".to_owned(),
            OPERATION.to_owned(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD".to_owned(),
            row.sigungu_cd.clone(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD".to_owned(),
            row.bjdong_cd.clone(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_NO".to_owned(),
            "1".to_owned(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS".to_owned(),
            self.config.building_num_of_rows.to_string(),
        );
        child_env.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_PROBE_OUTPUT_PATH".to_owned(),
            probe_path.to_string_lossy().to_string(),
        );
        let started = Instant::now();
        let output = run_runner(runner, &self.config.root, &child_env)?;
        let duration = started.elapsed();
        if !output.success {
            bail!(
                "probe runner failed exit_code={} output={}",
                output.exit_code,
                safe_output(&output.lines)
            );
        }
        if !probe_path.is_file() {
            bail!("probe runner did not write expected output file");
        }
        let probe = read_json(probe_path, "building-register page count probe")?;
        validate_probe(&probe, row, self.config.building_num_of_rows)?;
        let effective_page_size = u64_prop(&probe, "effective_page_size", 0);
        let provider_total_count = u64_prop(&probe, "provider_total_count", u64::MAX);
        let required_pages = u64_prop(&probe, "required_pages", 0);
        if required_pages != required_pages_for(provider_total_count, effective_page_size) {
            bail!("probe output required_pages mismatch");
        }
        self.write_probe_metrics(duration, "succeeded")?;
        Ok(PageCountJob {
            job_id: job_id.to_owned(),
            scope_unit_id: row.scope_unit_id.clone(),
            endpoint_slug: ENDPOINT_SLUG.to_owned(),
            operation: OPERATION.to_owned(),
            sigungu_cd: row.sigungu_cd.clone(),
            bjdong_cd: row.bjdong_cd.clone(),
            requested_page_size: self.config.building_num_of_rows,
            effective_page_size,
            provider_total_count,
            required_pages,
            probe_output_path: repo_relative_path(&self.config.root, probe_path),
        })
    }

    fn plan_payload(
        &self,
        status: &str,
        execute: bool,
        attempted_request_count: usize,
        jobs: Vec<PageCountJob>,
        failed_jobs: Vec<FailedPageCountJob>,
    ) -> PageCountPlan {
        PageCountPlan {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: chrono::Utc::now().to_rfc3339(),
            git_head: git_head(&self.config.root),
            status: status.to_owned(),
            source: SOURCE,
            scope_source: ScopeSource {
                path: repo_relative_path(&self.config.root, &self.config.scope_jsonl_path),
                evidence_path: repo_relative_path(
                    &self.config.root,
                    &self.config.scope_evidence_path,
                ),
                row_count: self.scope_rows.len(),
                selected_rows: self.selected_rows.len(),
                skip_jobs: self.config.skip_jobs,
                sha256: self.scope_sha256.clone(),
            },
            request_plan: RequestPlan {
                request_cap: self.config.request_cap,
                request_count_estimate: self.selected_rows.len(),
                selected_job_count: self.selected_rows.len(),
                requested_page_size: self.config.building_num_of_rows,
                provider: PROVIDER,
                endpoint_slug: ENDPOINT_SLUG,
                operation: OPERATION,
                max_in_flight: self.config.max_in_flight,
            },
            probe_output_root: repo_relative_path(
                &self.config.root,
                &self.config.probe_output_root,
            ),
            execute,
            attempted_request_count,
            jobs,
            failed_jobs,
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            evidence_limitations: if execute {
                vec![
                    "page_count_probe_only",
                    "does_not_collect_bronze_payloads",
                    "does_not_approve_national_rollout",
                ]
            } else {
                vec![
                    "planned_only",
                    "does_not_execute_public_api_requests",
                    "does_not_collect_bronze_payloads",
                    "does_not_approve_national_rollout",
                ]
            },
        }
    }

    fn write_probe_metrics(&self, duration: Duration, outcome: &str) -> anyhow::Result<()> {
        let Some(path) = &self.config.quota_metrics_path else {
            return Ok(());
        };
        public_api_metric_writer::write_quota_metric(
            path,
            PROVIDER,
            OPERATION,
            1,
            outcome,
            "page_count_probe",
        )?;
        public_api_metric_writer::write_dependency_metric_duration(
            path,
            PROVIDER,
            OPERATION,
            duration,
            outcome,
            "page_count_probe",
            None,
        )
    }

    fn print_written(&self, status: &str, jobs: usize, requests: usize) {
        println!(
            "building-register-page-count-plan-written status={} jobs={} requests={} path={}",
            status,
            jobs,
            requests,
            repo_relative_path(&self.config.root, &self.config.output_path)
        );
    }
}

#[derive(Debug, Serialize)]
struct PageCountPlan {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: String,
    source: &'static str,
    scope_source: ScopeSource,
    request_plan: RequestPlan,
    probe_output_root: String,
    execute: bool,
    attempted_request_count: usize,
    jobs: Vec<PageCountJob>,
    failed_jobs: Vec<FailedPageCountJob>,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    evidence_limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct ScopeSource {
    path: String,
    evidence_path: String,
    row_count: usize,
    selected_rows: usize,
    skip_jobs: usize,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct RequestPlan {
    request_cap: usize,
    request_count_estimate: usize,
    selected_job_count: usize,
    requested_page_size: u32,
    provider: &'static str,
    endpoint_slug: &'static str,
    operation: &'static str,
    max_in_flight: usize,
}

#[derive(Clone, Debug, Serialize)]
struct PageCountJob {
    job_id: String,
    scope_unit_id: String,
    endpoint_slug: String,
    operation: String,
    sigungu_cd: String,
    bjdong_cd: String,
    requested_page_size: u32,
    effective_page_size: u64,
    provider_total_count: u64,
    required_pages: u64,
    probe_output_path: String,
}

#[derive(Clone, Debug, Serialize)]
struct FailedPageCountJob {
    job_id: String,
    scope_unit_id: String,
    sigungu_cd: String,
    bjdong_cd: String,
    error_kind: String,
    error_message: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ScopeRow {
    schema_version: String,
    scope_unit_id: String,
    scope_kind: String,
    canonical_code: String,
    scope_key: String,
    bjdong_code: String,
    sigungu_cd: String,
    bjdong_cd: String,
    geometry_srid: u32,
    bbox: JsonValue,
}

impl ScopeRow {
    fn validate(&self, line_number: usize) -> anyhow::Result<()> {
        if self.schema_version != SCOPE_ROW_SCHEMA_VERSION {
            bail!("scope row {line_number} schema mismatch");
        }
        if self.scope_kind != "legal_dong" {
            bail!("scope row {line_number} scope_kind must be legal_dong");
        }
        if !five_digits(&self.sigungu_cd) || !five_digits(&self.bjdong_cd) {
            bail!("scope row {line_number} must use five-digit sigungu_cd and bjdong_cd");
        }
        let expected = format!("{}{}", self.sigungu_cd, self.bjdong_cd);
        if self.bjdong_code != expected || self.canonical_code != expected {
            bail!("scope row {line_number} bjdong identity mismatch");
        }
        if self.scope_key != format!("{}:{}", self.sigungu_cd, self.bjdong_cd) {
            bail!("scope row {line_number} scope_key mismatch");
        }
        if self.scope_unit_id.trim().is_empty() {
            bail!("scope row {line_number} scope_unit_id is required");
        }
        if self.geometry_srid != 4326 {
            bail!("scope row {line_number} geometry_srid must be EPSG 4326");
        }
        if self.bbox.is_null() {
            bail!("scope row {line_number} missing bbox");
        }
        Ok(())
    }

    fn job_id(&self) -> String {
        format!("building-register-{}-{}", self.sigungu_cd, self.bjdong_cd)
    }
}

#[derive(Debug, Deserialize)]
struct ScopeEvidence {
    schema_version: String,
    status: String,
    output_path: String,
    scope_row_count: usize,
    scope_sha256: Option<String>,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
}

fn read_scope_rows(path: &Path) -> anyhow::Result<Vec<ScopeRow>> {
    let bytes = fs::read(path)?;
    let raw = String::from_utf8_lossy(strip_utf8_bom(&bytes));
    let mut rows = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            bail!("scope JSONL line {} must not be blank", index + 1);
        }
        let row: ScopeRow = serde_json::from_str(line.trim_start_matches('\u{feff}'))
            .with_context(|| format!("scope JSONL line {} is not valid JSON", index + 1))?;
        row.validate(index + 1)?;
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("scope JSONL must contain at least one row");
    }
    Ok(rows)
}

fn validate_scope_evidence(
    config: &Config,
    scope_sha256: &str,
    scope_row_count: usize,
) -> anyhow::Result<()> {
    if !config.scope_evidence_path.is_file() {
        bail!(
            "scope evidence missing: {}",
            repo_relative_path(&config.root, &config.scope_evidence_path)
        );
    }
    let evidence: ScopeEvidence =
        serde_json::from_slice(strip_utf8_bom(&fs::read(&config.scope_evidence_path)?))?;
    if evidence.schema_version != SCOPE_EVIDENCE_SCHEMA_VERSION {
        bail!("scope evidence schema mismatch");
    }
    if evidence.status != "ready" {
        bail!("scope evidence status must be ready");
    }
    if evidence.output_path != repo_relative_path(&config.root, &config.scope_jsonl_path) {
        bail!("scope evidence output_path must match ScopeJsonlPath");
    }
    if evidence.scope_row_count != scope_row_count {
        bail!("scope evidence row count must match scope JSONL");
    }
    if evidence.completion_claim_allowed || evidence.national_rollout_allowed {
        bail!("scope evidence must not allow completion claim or national rollout");
    }
    if let Some(evidence_sha256) = evidence.scope_sha256 {
        if !evidence_sha256.is_empty() && evidence_sha256 != scope_sha256 {
            bail!("scope evidence scope_sha256 must match ScopeJsonlPath");
        }
    }
    Ok(())
}

fn select_scope_rows(
    rows: &[ScopeRow],
    skip_jobs: usize,
    max_jobs: Option<usize>,
    request_cap: usize,
) -> anyhow::Result<Vec<ScopeRow>> {
    let selected = rows
        .iter()
        .skip(skip_jobs)
        .take(max_jobs.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!("selected scope must contain at least one row");
    }
    if request_cap < selected.len() {
        bail!("RequestCap must be at least selected job count because page-count probe uses one request per legal dong");
    }
    Ok(selected)
}

fn validate_probe(
    probe: &JsonValue,
    row: &ScopeRow,
    expected_page_size: u32,
) -> anyhow::Result<()> {
    if string_prop(probe, "operation") != OPERATION {
        bail!("probe output operation mismatch");
    }
    if string_prop(probe, "sigungu_cd") != row.sigungu_cd
        || string_prop(probe, "bjdong_cd") != row.bjdong_cd
    {
        bail!("probe output legal-dong mismatch");
    }
    if u64_prop(probe, "requested_page_size", 0) != u64::from(expected_page_size) {
        bail!("probe output requested_page_size mismatch");
    }
    if u64_prop(probe, "effective_page_size", 0) < 1 {
        bail!("probe output effective_page_size must be positive");
    }
    if u64_prop(probe, "provider_total_count", u64::MAX) == u64::MAX {
        bail!("probe output provider_total_count must be non-negative");
    }
    Ok(())
}

fn required_pages_for(total_count: u64, effective_page_size: u64) -> u64 {
    if effective_page_size == 0 {
        return 0;
    }
    if total_count == 0 {
        return 1;
    }
    total_count.div_ceil(effective_page_size)
}

struct RunnerOutput {
    success: bool,
    exit_code: i32,
    lines: Vec<String>,
}

/// Builds the command that runs an outbox-publisher `command_name` subcommand.
///
/// Prefers re-invoking the current binary directly so production never needs the Cargo toolchain;
/// falls back to `cargo run -p foundation-outbox-publisher -- <command_name>` (using the resolved
/// `cargo` path) only when the current executable cannot be resolved.
fn outbox_subcommand(cargo: &Path, command_name: &str) -> Command {
    match std::env::current_exe() {
        Ok(exe) => {
            let mut command = Command::new(exe);
            command.arg(command_name);
            command
        }
        Err(_) => {
            let mut command = Command::new(cargo);
            command.args([
                "run",
                "-p",
                "foundation-outbox-publisher",
                "--",
                command_name,
            ]);
            command
        }
    }
}

fn run_runner(
    runner: &Path,
    root: &Path,
    child_env: &BTreeMap<String, String>,
) -> anyhow::Result<RunnerOutput> {
    let mut command = Command::new(runner);
    let output = command.current_dir(root).envs(child_env).output()?;
    let lines = lines_from_output(&output.stdout, &output.stderr);
    Ok(RunnerOutput {
        success: output.status.success(),
        exit_code: output.status.code().unwrap_or(1),
        lines,
    })
}

fn import_dotenv(path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    let mut values = env::vars().collect::<BTreeMap<_, _>>();
    if !path.is_file() {
        return Ok(values);
    }
    let bytes = fs::read(path)?;
    let raw = String::from_utf8_lossy(strip_utf8_bom(&bytes));
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        values.insert(
            key.trim().to_owned(),
            value.trim().trim_matches('"').trim_matches('\'').to_owned(),
        );
    }
    Ok(values)
}

fn require_env(values: &BTreeMap<String, String>, name: &str) -> anyhow::Result<()> {
    if values
        .get(name)
        .is_some_and(|value| !value.trim().is_empty())
    {
        Ok(())
    } else {
        bail!("{name} is required for live building-register page count probe")
    }
}

fn resolve_cargo(explicit: Option<&PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.clone());
    }
    for profile_root in [env::var_os("USERPROFILE"), env::var_os("HOME")]
        .into_iter()
        .flatten()
    {
        let candidate = PathBuf::from(profile_root).join(".cargo/bin/cargo.exe");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Ok(PathBuf::from("cargo"))
}

fn optional_env_path(root: &Path, name: &str, field: &str) -> anyhow::Result<Option<PathBuf>> {
    let value = env_string(name, "")?;
    if value.is_empty() {
        return Ok(None);
    }
    resolve_repo_path(root, &PathBuf::from(value), field).map(Some)
}

fn optional_external_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    let value = env_string(name, "")?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(PathBuf::from(value)))
    }
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        // Present-but-empty behaves like unset: PowerShell wrappers cannot delete env vars
        // portably ($env:X = "" removes on Windows but sets empty on Linux).
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            _ => bail!("invalid {name} environment variable"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    env_string(name, &default.to_string())?
        .parse::<i64>()
        .with_context(|| format!("invalid {name} environment variable"))
}

fn read_json(path: &Path, label: &str) -> anyhow::Result<JsonValue> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))?;
    serde_json::from_slice(strip_utf8_bom(&bytes))
        .with_context(|| format!("failed to parse {label} {}", path.display()))
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    Ok(sha256_hex(&fs::read(path)?))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn five_digits(value: &str) -> bool {
    value.len() == 5 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn string_prop(value: &JsonValue, name: &str) -> String {
    value.get(name).map(value_to_string).unwrap_or_default()
}

fn u64_prop(value: &JsonValue, name: &str, default: u64) -> u64 {
    value
        .get(name)
        .and_then(|raw| {
            raw.as_u64()
                .or_else(|| raw.as_i64().and_then(|number| u64::try_from(number).ok()))
                .or_else(|| raw.as_str().and_then(|text| text.parse::<u64>().ok()))
        })
        .unwrap_or(default)
}

fn value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(raw) => raw.clone(),
        JsonValue::Number(raw) => raw.to_string(),
        JsonValue::Bool(raw) => raw.to_string(),
        JsonValue::Null => String::new(),
        JsonValue::Array(_) => "[array]".to_owned(),
        JsonValue::Object(_) => "[object]".to_owned(),
    }
}

fn safe_output(lines: &[String]) -> String {
    let mut message = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ");
    if message.is_empty() {
        message = "runner exited without output".to_owned();
    }
    for token in ["serviceKey", "service_key", "DATA_GO_KR_SERVICE_KEY"] {
        message = message.replace(token, "[redacted]");
    }
    if message.len() > 1000 {
        message.truncate(1000);
    }
    message
}

fn lines_from_output(stdout: &[u8], stderr: &[u8]) -> Vec<String> {
    let mut lines = String::from_utf8_lossy(stdout)
        .lines()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    lines.extend(
        String::from_utf8_lossy(stderr)
            .lines()
            .map(ToOwned::to_owned),
    );
    lines
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let value = path.to_string_lossy();
        if let Some(rest) = value.strip_prefix("\\\\?\\UNC\\") {
            return PathBuf::from(format!("\\\\{rest}"));
        }
        if let Some(rest) = value.strip_prefix("\\\\?\\") {
            return PathBuf::from(rest);
        }
    }
    path
}
