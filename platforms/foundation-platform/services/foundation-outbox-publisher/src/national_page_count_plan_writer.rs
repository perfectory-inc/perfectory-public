use std::{
    collections::{BTreeSet, HashSet},
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{
    page_count_plan_contract::{
        building_register_endpoint_slug, building_register_job_id, is_valid_building_job_id,
        is_valid_vworld_job_id, required_pages_for,
    },
    public_data_control_support::{
        env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now,
        write_json_file,
    },
};

const SCHEMA_VERSION: &str = "foundation-platform.national_page_count_plan.v1";
const BUILDING_PLAN_SCHEMA_VERSION: &str =
    "foundation-platform.building_register_page_count_plan.v1";
const VWORLD_PLAN_SCHEMA_VERSION: &str = "foundation-platform.vworld_page_count_plan.v1";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    Writer::new(config)?.run()
}

struct Config {
    root: PathBuf,
    building_page_count_plan_path: PathBuf,
    vworld_page_count_plan_path: PathBuf,
    output_path: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if !env_bool(
            "FOUNDATION_PLATFORM_NATIONAL_PAGE_COUNT_PLAN_CONFIRM",
            false,
        )? {
            bail!(
                "ConfirmNationalPageCountPlan is required before writing national page-count plan"
            );
        }

        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );

        Ok(Self {
            building_page_count_plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_PAGE_COUNT_PLAN_BUILDING_PATH",
                    "target/audit/building-register-page-count-plan.json",
                )?,
                "BuildingPageCountPlanPath",
            )?,
            vworld_page_count_plan_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_PAGE_COUNT_PLAN_VWORLD_PATH",
                    "target/audit/vworld-page-count-plan.json",
                )?,
                "VWorldPageCountPlanPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_PAGE_COUNT_PLAN_OUTPUT_PATH",
                    "target/audit/national-page-count-plan.json",
                )?,
                "OutputPath",
            )?,
            root,
        })
    }
}

struct Writer {
    config: Config,
}

impl Writer {
    fn new(config: Config) -> anyhow::Result<Self> {
        if config.output_path.is_file() {
            bail!(
                "national page-count plan already exists: {}",
                repo_relative_path(&config.root, &config.output_path)
            );
        }
        if !config.building_page_count_plan_path.is_file() {
            bail!(
                "Building register page-count plan missing: {}",
                repo_relative_path(&config.root, &config.building_page_count_plan_path)
            );
        }
        if !config.vworld_page_count_plan_path.is_file() {
            bail!(
                "VWorld page-count plan missing: {}",
                repo_relative_path(&config.root, &config.vworld_page_count_plan_path)
            );
        }
        Ok(Self { config })
    }

    fn run(&self) -> anyhow::Result<()> {
        let building_plan = read_json(
            &self.config.building_page_count_plan_path,
            "building-register page-count plan",
        )?;
        let vworld_plan = read_json(
            &self.config.vworld_page_count_plan_path,
            "VWorld page-count plan",
        )?;
        validate_source_plans(&building_plan, &vworld_plan)?;

        let building_jobs = json_array(&building_plan, "jobs");
        let vworld_jobs = json_array(&vworld_plan, "jobs");
        let mut seen_job_ids = HashSet::new();
        let mut jobs = Vec::with_capacity(building_jobs.len() + vworld_jobs.len());

        for source_job in building_jobs {
            let job = new_national_building_job(source_job)?;
            insert_unique_job_id(&mut seen_job_ids, &job.job_id)?;
            jobs.push(job);
        }
        for source_job in vworld_jobs {
            let job = new_national_vworld_job(source_job)?;
            insert_unique_job_id(&mut seen_job_ids, &job.job_id)?;
            jobs.push(job);
        }
        if jobs.is_empty() {
            bail!("national page-count plan jobs must not be empty");
        }
        assert_national_page_count_coverage(&jobs)?;

        let request_count_estimate = jobs
            .iter()
            .try_fold(0_u64, |total, job| total.checked_add(job.required_pages))
            .context("request_count_estimate overflow")?;

        let plan = NationalPageCountPlan {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&self.config.root),
            status: "ready",
            source: "provider-page-count-probes",
            source_plans: SourcePlans {
                building_register: SourcePlanRef {
                    path: repo_relative_path(
                        &self.config.root,
                        &self.config.building_page_count_plan_path,
                    ),
                    sha256: file_sha256(&self.config.building_page_count_plan_path)?,
                    job_count: u64::try_from(json_array(&building_plan, "jobs").len())
                        .context("building source job_count overflow")?,
                },
                vworld: SourcePlanRef {
                    path: repo_relative_path(
                        &self.config.root,
                        &self.config.vworld_page_count_plan_path,
                    ),
                    sha256: file_sha256(&self.config.vworld_page_count_plan_path)?,
                    job_count: u64::try_from(json_array(&vworld_plan, "jobs").len())
                        .context("VWorld source job_count overflow")?,
                },
            },
            request_plan: RequestPlan {
                page_count_source: "national_page_count_plan",
                selected_job_count: u64::try_from(jobs.len())
                    .context("selected_job_count overflow")?,
                request_count_estimate,
            },
            jobs,
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            evidence_limitations: vec![
                "page_count_plan_only",
                "does_not_collect_bronze_payloads",
                "does_not_approve_national_rollout",
            ],
            next_gates: vec![
                "national-data-collection-shard-manifest",
                "national-data-collection-shard-execution",
            ],
        };

        write_json_file(&self.config.output_path, &plan)?;
        println!(
            "national-page-count-plan-written status=ready jobs={} requests={} path={}",
            plan.jobs.len(),
            plan.request_plan.request_count_estimate,
            repo_relative_path(&self.config.root, &self.config.output_path)
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct NationalPageCountPlan {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    source: &'static str,
    source_plans: SourcePlans,
    request_plan: RequestPlan,
    jobs: Vec<NationalJob>,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    evidence_limitations: Vec<&'static str>,
    next_gates: Vec<&'static str>,
}

#[derive(Serialize)]
struct SourcePlans {
    building_register: SourcePlanRef,
    vworld: SourcePlanRef,
}

#[derive(Serialize)]
struct SourcePlanRef {
    path: String,
    sha256: String,
    job_count: u64,
}

#[derive(Serialize)]
struct RequestPlan {
    page_count_source: &'static str,
    selected_job_count: u64,
    request_count_estimate: u64,
}

#[derive(Clone, Serialize)]
struct NationalJob {
    job_id: String,
    provider: &'static str,
    endpoint_slug: String,
    scope_unit_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation: Option<String>,
    sigungu_cd: String,
    bjdong_cd: String,
    requested_page_size: u64,
    effective_page_size: u64,
    provider_total_count: u64,
    required_pages: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_empty_reason: Option<String>,
}

fn validate_source_plans(building_plan: &JsonValue, vworld_plan: &JsonValue) -> anyhow::Result<()> {
    if json_string(building_plan, "schema_version") != BUILDING_PLAN_SCHEMA_VERSION {
        bail!("building-register page-count plan schema mismatch");
    }
    if json_string(vworld_plan, "schema_version") != VWORLD_PLAN_SCHEMA_VERSION {
        bail!("VWorld page-count plan schema mismatch");
    }
    if json_string(building_plan, "status") != "ready"
        || json_string(vworld_plan, "status") != "ready"
    {
        bail!("source page-count plans must be ready");
    }
    Ok(())
}

fn new_national_building_job(job: &JsonValue) -> anyhow::Result<NationalJob> {
    let job_id = json_string(job, "job_id");
    if !is_valid_building_job_id(&job_id) {
        bail!("building-register page count job_id invalid: {job_id}");
    }
    let operation = json_string(job, "operation");
    let expected_endpoint_slug = building_register_endpoint_slug(&operation)?;
    let endpoint_slug = match json_string(job, "endpoint_slug") {
        raw if raw.trim().is_empty() => expected_endpoint_slug.clone(),
        raw => raw,
    };
    if endpoint_slug != expected_endpoint_slug {
        bail!("building-register page count endpoint_slug must match operation: {job_id}");
    }
    let page_numbers = page_count_numbers(job, &job_id)?;
    let sigungu = json_string(job, "sigungu_cd");
    let bjdong = json_string(job, "bjdong_cd");
    let expected_job_id = building_register_job_id(&operation, &sigungu, &bjdong);
    if job_id != expected_job_id {
        bail!("building-register page count job_id must match operation and legal-dong: {job_id}");
    }

    Ok(NationalJob {
        job_id,
        provider: "data.go.kr",
        endpoint_slug,
        scope_unit_id: json_string(job, "scope_unit_id"),
        operation: Some(operation),
        sigungu_cd: sigungu,
        bjdong_cd: bjdong,
        requested_page_size: page_numbers.requested_page_size,
        effective_page_size: page_numbers.effective_page_size,
        provider_total_count: page_numbers.provider_total_count,
        required_pages: page_numbers.required_pages,
        dataset: None,
        provider_empty_reason: None,
    })
}

fn new_national_vworld_job(job: &JsonValue) -> anyhow::Result<NationalJob> {
    let job_id = json_string(job, "job_id");
    if !is_valid_vworld_job_id(&job_id) {
        bail!("VWorld page count job_id invalid: {job_id}");
    }
    let page_numbers = page_count_numbers(job, &job_id)?;
    let endpoint_slug = json_string(job, "endpoint_slug");
    let dataset = json_string(job, "dataset");
    let operation = json_string(job, "operation");
    let is_cadastral = job_id.starts_with("vworld-cadastral-");
    let is_land_register = job_id.starts_with("vworld-land-register-");

    if is_cadastral && (endpoint_slug != "vworld-dataset-parcel" || dataset != "LP_PA_CBND_BUBUN") {
        bail!("VWorld cadastral page count contract mismatch: {job_id}");
    }
    if is_land_register
        && (endpoint_slug != "vworld-dataset-land_register" || operation != "ladfrlList")
    {
        bail!("VWorld land-register page count contract mismatch: {job_id}");
    }

    Ok(NationalJob {
        job_id,
        provider: "VWorld",
        endpoint_slug,
        scope_unit_id: json_string(job, "scope_unit_id"),
        operation: is_land_register.then_some(operation),
        sigungu_cd: json_string(job, "sigungu_cd"),
        bjdong_cd: json_string(job, "bjdong_cd"),
        requested_page_size: page_numbers.requested_page_size,
        effective_page_size: page_numbers.effective_page_size,
        provider_total_count: page_numbers.provider_total_count,
        required_pages: page_numbers.required_pages,
        dataset: is_cadastral.then_some(dataset),
        provider_empty_reason: non_empty_json_string(job, "provider_empty_reason"),
    })
}

fn insert_unique_job_id(seen_job_ids: &mut HashSet<String>, job_id: &str) -> anyhow::Result<()> {
    if !seen_job_ids.insert(job_id.to_owned()) {
        bail!("duplicate page-count job: {job_id}");
    }
    Ok(())
}

fn assert_national_page_count_coverage(jobs: &[NationalJob]) -> anyhow::Result<()> {
    let mut job_ids = HashSet::new();
    let mut scope_keys = BTreeSet::new();
    let mut building_operations = BTreeSet::new();

    for job in jobs {
        job_ids.insert(job.job_id.clone());
        scope_keys.insert(format!("{}-{}", job.sigungu_cd, job.bjdong_cd));
        if job.provider == "data.go.kr" {
            let operation = job
                .operation
                .as_deref()
                .context("building-register job operation is required")?;
            building_operations.insert(operation.to_owned());
        }
    }

    for scope_key in scope_keys {
        let (sigungu, bjdong) = scope_key
            .split_once('-')
            .context("scope key must use sigungu-bjdong format")?;
        for operation in &building_operations {
            let required_job_id = building_register_job_id(operation, sigungu, bjdong);
            if !job_ids.contains(&required_job_id) {
                bail!("missing required page-count job: {required_job_id}");
            }
        }
        for prefix in ["vworld-cadastral", "vworld-land-register"] {
            let required_job_id = format!("{prefix}-{scope_key}");
            if !job_ids.contains(&required_job_id) {
                bail!("missing required page-count job: {required_job_id}");
            }
        }
    }
    Ok(())
}

struct PageNumbers {
    requested_page_size: u64,
    effective_page_size: u64,
    provider_total_count: u64,
    required_pages: u64,
}

fn page_count_numbers(job: &JsonValue, job_id: &str) -> anyhow::Result<PageNumbers> {
    let requested_page_size = json_u64(job, "requested_page_size", 0)?;
    let effective_page_size = json_u64(job, "effective_page_size", 0)?;
    let provider_total_count = json_u64(job, "provider_total_count", u64::MAX)?;
    let required_pages = json_u64(job, "required_pages", 0)?;
    if requested_page_size < 1 || effective_page_size < 1 {
        bail!("page-count plan page sizes must be positive: {job_id}");
    }
    if provider_total_count == u64::MAX {
        bail!("page-count plan provider_total_count must be non-negative: {job_id}");
    }
    if required_pages != required_pages_for(provider_total_count, effective_page_size)? {
        bail!("page-count plan required_pages must match provider_total_count and effective_page_size: {job_id}");
    }
    Ok(PageNumbers {
        requested_page_size,
        effective_page_size,
        provider_total_count,
        required_pages,
    })
}

fn json_array<'a>(value: &'a JsonValue, field: &str) -> Vec<&'a JsonValue> {
    value
        .get(field)
        .and_then(JsonValue::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}

fn json_string(value: &JsonValue, field: &str) -> String {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn non_empty_json_string(value: &JsonValue, field: &str) -> Option<String> {
    let value = json_string(value, field);
    (!value.trim().is_empty()).then_some(value)
}

fn json_u64(value: &JsonValue, field: &str, default: u64) -> anyhow::Result<u64> {
    let Some(raw) = value.get(field) else {
        return Ok(default);
    };
    if let Some(value) = raw.as_u64() {
        return Ok(value);
    }
    if let Some(value) = raw.as_i64() {
        return u64::try_from(value).map_err(|_| anyhow::anyhow!("{field} must be non-negative"));
    }
    bail!("{field} must be an integer")
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read source plan for hash {}", path.display()))?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("{digest:x}"))
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => bail!("{name} must be a boolean"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    const VERBATIM_PREFIX: &str = r"\\?\";
    let raw = path.to_string_lossy();
    if let Some(stripped) = raw.strip_prefix(VERBATIM_PREFIX) {
        PathBuf::from(stripped)
    } else {
        path
    }
}
