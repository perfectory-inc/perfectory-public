//! Batch page-count probe for `VWorld` national planning.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, Context};
use collection_infrastructure::{
    VWorldDataApiClient, VWorldDataApiConfig, VWorldDataFeatureRequest, VWorldNedAttributeClient,
    VWorldNedAttributeConfig, VWorldRequestPolicy,
};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::public_data_control_support::{
    optional_duration_millis_env, optional_duration_seconds_env, optional_env_value,
    optional_positive_u32_env, optional_usize_env, required_env_value,
};

const PLAN_SCHEMA_VERSION: &str = "foundation-platform.vworld_page_count_plan.v1";
const SCOPE_ROW_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope_row.v1";
const SCOPE_EVIDENCE_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope.v1";
const SOURCE: &str = "vworld-page-count-probe";
const PROVIDER: &str = "VWorld";
const CADASTRAL_ENDPOINT_SLUG: &str = "vworld-dataset-parcel";
const CADASTRAL_DATASET: &str = "LP_PA_CBND_BUBUN";
const LAND_REGISTER_ENDPOINT_SLUG: &str = "vworld-dataset-land_register";
const LAND_REGISTER_OPERATION: &str = "ladfrlList";
const DEFAULT_DATA_BASE_URI: &str = "https://api.vworld.kr";
const DEFAULT_NED_BASE_URI: &str = "https://api.vworld.kr/ned/data";
const DEFAULT_USER_AGENT: &str = "foundation-outbox-publisher/0.1";
const DEFAULT_CADASTRAL_PAGE_SIZE: u32 = 1000;

fn default_cadastral_page_size() -> u32 {
    DEFAULT_CADASTRAL_PAGE_SIZE
}

/// Runs a one-process batch probe that writes the VWorld page-count plan.
pub async fn run() -> anyhow::Result<()> {
    let config = VWorldPageCountBatchConfig::from_env()?;
    let scope_rows = read_scope_rows(&config.scope_jsonl_path)?;
    validate_scope_evidence(
        &config.scope_evidence_path,
        &config.scope_jsonl_path,
        scope_rows.len(),
    )?;
    let selected_rows = select_scope_rows(
        &scope_rows,
        config.skip_scope_rows,
        config.max_scope_rows,
        config.request_cap,
    )?;

    if config.output_path.exists() {
        bail!(
            "VWorld page count plan already exists: {}",
            config.output_path.display()
        );
    }

    let data_client = VWorldDataApiClient::new_with_policy(
        &VWorldDataApiConfig {
            base_uri: config.data_base_uri.clone(),
            api_key: config.api_key.clone(),
            domain: config.domain.clone(),
            user_agent: config.user_agent.clone(),
        },
        config.request_policy,
    )?;
    let ned_client = VWorldNedAttributeClient::new_with_policy(
        &VWorldNedAttributeConfig {
            base_uri: config.ned_base_uri.clone(),
            api_key: config.api_key.clone(),
            domain: config.domain.clone(),
            user_agent: config.user_agent.clone(),
        },
        config.request_policy,
    )?;
    let repo_root = std::env::current_dir().context("failed to resolve current directory")?;
    let probe_jobs = selected_rows
        .iter()
        .flat_map(|row| {
            [
                VWorldProbeJob::Cadastral(row.clone()),
                VWorldProbeJob::LandRegister(row.clone()),
            ]
        })
        .collect::<Vec<_>>();

    let results =
        stream::iter(probe_jobs.into_iter().map(|probe_job| {
            let data_client = data_client.clone();
            let ned_client = ned_client.clone();
            let config = config.clone();
            let repo_root = repo_root.clone();
            async move {
                probe_vworld_job(&data_client, &ned_client, &config, &probe_job, &repo_root).await
            }
        }))
        .buffer_unordered(config.max_in_flight)
        .collect::<Vec<_>>()
        .await;

    let mut jobs = Vec::new();
    let mut failed_jobs = Vec::new();
    for result in results {
        match result {
            Ok(job) => jobs.push(job),
            Err(failed_job) => failed_jobs.push(failed_job),
        }
    }
    jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    failed_jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));

    let attempted_request_count = results_len(&jobs, &failed_jobs);
    let status = if failed_jobs.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let plan = VWorldPageCountPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        generated_at_utc: chrono::Utc::now().to_rfc3339(),
        git_head: git_head(),
        status,
        source: SOURCE,
        scope_source: ScopeSource {
            path: repo_relative_path(&config.scope_jsonl_path, &repo_root),
            evidence_path: repo_relative_path(&config.scope_evidence_path, &repo_root),
            row_count: scope_rows.len(),
            selected_rows: selected_rows.len(),
            skip_scope_rows: config.skip_scope_rows,
            sha256: sha256_file_hex(&config.scope_jsonl_path)?,
        },
        request_plan: PageCountRequestPlan {
            request_cap: config.request_cap,
            request_count_estimate: attempted_request_count,
            selected_job_count: attempted_request_count,
            selected_scope_count: selected_rows.len(),
            provider: PROVIDER,
            endpoints: vec![CADASTRAL_ENDPOINT_SLUG, LAND_REGISTER_ENDPOINT_SLUG],
            cadastral_page_size: config.cadastral_size,
            land_register_num_of_rows: config.land_register_num_of_rows,
            max_in_flight: config.max_in_flight,
        },
        probe_output_root: repo_relative_path(&config.probe_output_root, &repo_root),
        execute: true,
        attempted_request_count,
        jobs,
        failed_jobs,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        evidence_limitations: vec![
            "page_count_probe_only",
            "does_not_collect_bronze_payloads",
            "does_not_approve_national_rollout",
        ],
    };

    write_json(&config.output_path, &plan)?;
    if plan.status == "blocked" {
        bail!(
            "VWorld page-count batch blocked failed_jobs={}",
            plan.failed_jobs.len()
        );
    }

    tracing::info!(
        jobs = plan.jobs.len(),
        requests = plan.request_plan.request_count_estimate,
        output_path = %config.output_path.display(),
        "VWorld page-count batch succeeded"
    );
    Ok(())
}

async fn probe_vworld_job(
    data_client: &VWorldDataApiClient,
    ned_client: &VWorldNedAttributeClient,
    config: &VWorldPageCountBatchConfig,
    probe_job: &VWorldProbeJob,
    repo_root: &Path,
) -> Result<PageCountJob, FailedPageCountJob> {
    let started = Instant::now();
    match probe_job {
        VWorldProbeJob::Cadastral(row) => {
            probe_cadastral_job(data_client, config, row, repo_root, started).await
        }
        VWorldProbeJob::LandRegister(row) => {
            probe_land_register_job(ned_client, config, row, repo_root, started).await
        }
    }
}

async fn probe_cadastral_job(
    client: &VWorldDataApiClient,
    config: &VWorldPageCountBatchConfig,
    row: &ScopeRow,
    repo_root: &Path,
    started: Instant,
) -> Result<PageCountJob, FailedPageCountJob> {
    let job_id = row.cadastral_job_id();
    let attr_filter = row.cadastral_attr_filter();
    let probe_output_path = config.probe_output_root.join(format!("{job_id}.json"));
    if probe_output_path.exists() {
        return read_existing_probe_job(
            &probe_output_path,
            row,
            &job_id,
            CADASTRAL_ENDPOINT_SLUG,
            config.cadastral_size,
            Some(CADASTRAL_DATASET.to_owned()),
            None,
            repo_root,
        )
        .map_err(|error| {
            failed_job(
                row,
                job_id.clone(),
                "vworld_cadastral_probe_reuse_failed",
                error,
                started,
            )
        });
    }
    let result = async {
        let request = VWorldDataFeatureRequest {
            dataset: CADASTRAL_DATASET.to_owned(),
            attr_filter: Some(attr_filter.clone()),
            columns: vec!["pnu".to_owned()],
            geometry: false,
            attribute: true,
            crs: Some("EPSG:4326".to_owned()),
            page: 1,
            size: config.cadastral_size,
        };
        let fetched_page = client
            .fetch_feature_page(&request)
            .await
            .context("failed to fetch VWorld cadastral page-count probe")?;
        let provider_total_count =
            required_json_u64_pointer(&fetched_page.payload, "/response/record/total")?;
        let probe = VWorldPageCountProbe {
            job_id: job_id.clone(),
            provider: PROVIDER.to_owned(),
            endpoint_slug: CADASTRAL_ENDPOINT_SLUG.to_owned(),
            scope_unit_id: row.scope_unit_id.clone(),
            sigungu_cd: row.sigungu_cd.clone(),
            bjdong_cd: row.bjdong_cd.clone(),
            requested_page_size: config.cadastral_size,
            effective_page_size: config.cadastral_size,
            provider_total_count,
            required_pages: required_pages_for_total_count(
                provider_total_count,
                config.cadastral_size,
            ),
            selector: attr_filter.clone(),
            provider_empty_reason: zero_count_reason(provider_total_count),
        };
        write_json(&probe_output_path, &probe)?;
        Ok::<_, anyhow::Error>(PageCountJob {
            job_id: job_id.clone(),
            scope_unit_id: row.scope_unit_id.clone(),
            provider: PROVIDER,
            endpoint_slug: CADASTRAL_ENDPOINT_SLUG,
            dataset: Some(CADASTRAL_DATASET.to_owned()),
            operation: None,
            sigungu_cd: row.sigungu_cd.clone(),
            bjdong_cd: row.bjdong_cd.clone(),
            requested_page_size: probe.requested_page_size,
            effective_page_size: probe.effective_page_size,
            provider_total_count,
            required_pages: probe.required_pages,
            provider_empty_reason: zero_count_reason(provider_total_count),
            probe_output_path: repo_relative_path(&probe_output_path, repo_root),
        })
    }
    .await;

    match result {
        Ok(job) => Ok(job),
        Err(error) if is_vworld_invalid_range_error(&error) => write_cadastral_zero_count_job(
            config,
            row,
            job_id,
            attr_filter,
            &probe_output_path,
            repo_root,
        ),
        Err(error) => Err(failed_job(
            row,
            job_id,
            "vworld_cadastral_probe_failed",
            error,
            started,
        )),
    }
}

fn write_cadastral_zero_count_job(
    config: &VWorldPageCountBatchConfig,
    row: &ScopeRow,
    job_id: String,
    attr_filter: String,
    probe_output_path: &Path,
    repo_root: &Path,
) -> Result<PageCountJob, FailedPageCountJob> {
    let provider_empty_reason = Some("vworld_invalid_emd_code".to_owned());
    let probe = VWorldPageCountProbe {
        job_id: job_id.clone(),
        provider: PROVIDER.to_owned(),
        endpoint_slug: CADASTRAL_ENDPOINT_SLUG.to_owned(),
        scope_unit_id: row.scope_unit_id.clone(),
        sigungu_cd: row.sigungu_cd.clone(),
        bjdong_cd: row.bjdong_cd.clone(),
        requested_page_size: config.cadastral_size,
        effective_page_size: config.cadastral_size,
        provider_total_count: 0,
        required_pages: 1,
        selector: attr_filter,
        provider_empty_reason: provider_empty_reason.clone(),
    };
    write_json(probe_output_path, &probe).map_err(|error| {
        failed_job(
            row,
            job_id.clone(),
            "vworld_cadastral_zero_count_probe_write_failed",
            error,
            Instant::now(),
        )
    })?;
    Ok(PageCountJob {
        job_id,
        scope_unit_id: row.scope_unit_id.clone(),
        provider: PROVIDER,
        endpoint_slug: CADASTRAL_ENDPOINT_SLUG,
        dataset: Some(CADASTRAL_DATASET.to_owned()),
        operation: None,
        sigungu_cd: row.sigungu_cd.clone(),
        bjdong_cd: row.bjdong_cd.clone(),
        requested_page_size: config.cadastral_size,
        effective_page_size: config.cadastral_size,
        provider_total_count: 0,
        required_pages: 1,
        provider_empty_reason,
        probe_output_path: repo_relative_path(probe_output_path, repo_root),
    })
}

async fn probe_land_register_job(
    client: &VWorldNedAttributeClient,
    config: &VWorldPageCountBatchConfig,
    row: &ScopeRow,
    repo_root: &Path,
    started: Instant,
) -> Result<PageCountJob, FailedPageCountJob> {
    let job_id = row.land_register_job_id();
    let pnu_prefix = row.legal_dong_code();
    let probe_output_path = config.probe_output_root.join(format!("{job_id}.json"));
    if probe_output_path.exists() {
        return read_existing_probe_job(
            &probe_output_path,
            row,
            &job_id,
            LAND_REGISTER_ENDPOINT_SLUG,
            config.land_register_num_of_rows,
            None,
            Some(LAND_REGISTER_OPERATION.to_owned()),
            repo_root,
        )
        .map_err(|error| {
            failed_job(
                row,
                job_id.clone(),
                "vworld_land_register_probe_reuse_failed",
                error,
                started,
            )
        });
    }
    let result = async {
        let fetched_page = client
            .fetch_json_page(
                LAND_REGISTER_OPERATION,
                &BTreeMap::from([("pnu".to_owned(), pnu_prefix.clone())]),
                1,
                config.land_register_num_of_rows,
            )
            .await
            .context("failed to fetch VWorld land-register page-count probe")?;
        let provider_total_count = land_register_total_count(&fetched_page.payload)?;
        let required_pages =
            required_pages_for_total_count(provider_total_count, config.land_register_num_of_rows);
        let probe = VWorldPageCountProbe {
            job_id: job_id.clone(),
            provider: PROVIDER.to_owned(),
            endpoint_slug: LAND_REGISTER_ENDPOINT_SLUG.to_owned(),
            scope_unit_id: row.scope_unit_id.clone(),
            sigungu_cd: row.sigungu_cd.clone(),
            bjdong_cd: row.bjdong_cd.clone(),
            requested_page_size: config.land_register_num_of_rows,
            effective_page_size: config.land_register_num_of_rows,
            provider_total_count,
            required_pages,
            selector: pnu_prefix.clone(),
            provider_empty_reason: zero_count_reason(provider_total_count),
        };
        write_json(&probe_output_path, &probe)?;
        Ok::<_, anyhow::Error>(PageCountJob {
            job_id: job_id.clone(),
            scope_unit_id: row.scope_unit_id.clone(),
            provider: PROVIDER,
            endpoint_slug: LAND_REGISTER_ENDPOINT_SLUG,
            dataset: None,
            operation: Some(LAND_REGISTER_OPERATION.to_owned()),
            sigungu_cd: row.sigungu_cd.clone(),
            bjdong_cd: row.bjdong_cd.clone(),
            requested_page_size: probe.requested_page_size,
            effective_page_size: probe.effective_page_size,
            provider_total_count,
            required_pages,
            provider_empty_reason: zero_count_reason(provider_total_count),
            probe_output_path: repo_relative_path(&probe_output_path, repo_root),
        })
    }
    .await;

    result.map_err(|error| {
        failed_job(
            row,
            job_id,
            "vworld_land_register_probe_failed",
            error,
            started,
        )
    })
}

fn read_existing_probe_job(
    probe_output_path: &Path,
    row: &ScopeRow,
    job_id: &str,
    endpoint_slug: &'static str,
    requested_page_size: u32,
    dataset: Option<String>,
    operation: Option<String>,
    repo_root: &Path,
) -> anyhow::Result<PageCountJob> {
    let content = fs::read_to_string(probe_output_path).with_context(|| {
        format!(
            "failed to read existing VWorld probe {}",
            probe_output_path.display()
        )
    })?;
    let probe: VWorldPageCountProbe = serde_json::from_str(content.trim_start_matches('\u{feff}'))
        .context("existing VWorld probe is not valid JSON")?;
    if probe.job_id != job_id {
        bail!(
            "existing VWorld probe job_id mismatch: expected={job_id} actual={}",
            probe.job_id
        );
    }
    if probe.endpoint_slug != endpoint_slug {
        bail!("existing VWorld probe endpoint_slug mismatch: {job_id}");
    }
    if probe.scope_unit_id != row.scope_unit_id
        || probe.sigungu_cd != row.sigungu_cd
        || probe.bjdong_cd != row.bjdong_cd
    {
        bail!("existing VWorld probe legal-dong scope mismatch: {job_id}");
    }
    if probe.requested_page_size != requested_page_size {
        bail!("existing VWorld probe requested_page_size mismatch: {job_id}");
    }
    if probe.required_pages
        != required_pages_for_total_count(probe.provider_total_count, probe.effective_page_size)
    {
        bail!("existing VWorld probe required_pages mismatch: {job_id}");
    }
    Ok(PageCountJob {
        job_id: probe.job_id,
        scope_unit_id: probe.scope_unit_id,
        provider: PROVIDER,
        endpoint_slug,
        dataset,
        operation,
        sigungu_cd: probe.sigungu_cd,
        bjdong_cd: probe.bjdong_cd,
        requested_page_size: probe.requested_page_size,
        effective_page_size: probe.effective_page_size,
        provider_total_count: probe.provider_total_count,
        required_pages: probe.required_pages,
        provider_empty_reason: probe.provider_empty_reason,
        probe_output_path: repo_relative_path(probe_output_path, repo_root),
    })
}

fn failed_job(
    row: &ScopeRow,
    job_id: String,
    error_kind: &'static str,
    error: anyhow::Error,
    started: Instant,
) -> FailedPageCountJob {
    FailedPageCountJob {
        job_id,
        scope_unit_id: row.scope_unit_id.clone(),
        sigungu_cd: row.sigungu_cd.clone(),
        bjdong_cd: row.bjdong_cd.clone(),
        error_kind,
        error_message: safe_error_message(&error.to_string()),
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

fn results_len(jobs: &[PageCountJob], failed_jobs: &[FailedPageCountJob]) -> usize {
    jobs.len() + failed_jobs.len()
}

fn required_json_u64_pointer(payload: &JsonValue, pointer: &str) -> anyhow::Result<u64> {
    let Some(value) = payload.pointer(pointer) else {
        bail!("VWorld JSON field {pointer} is required for page-count probe");
    };
    json_u64_value(value, pointer)
}

fn json_u64_value(value: &JsonValue, pointer: &str) -> anyhow::Result<u64> {
    match value {
        JsonValue::Number(number) => number
            .as_u64()
            .with_context(|| format!("VWorld JSON field {pointer} must be an unsigned integer")),
        JsonValue::String(raw) => raw
            .trim()
            .parse::<u64>()
            .with_context(|| format!("VWorld JSON field {pointer} must be an unsigned integer")),
        _ => bail!("VWorld JSON field {pointer} must be an unsigned integer"),
    }
}

fn land_register_total_count(payload: &JsonValue) -> anyhow::Result<u64> {
    if payload.pointer("/ladfrlVOList/totalCount").is_some() {
        return required_json_u64_pointer(payload, "/ladfrlVOList/totalCount");
    }
    required_json_u64_pointer(payload, "/response/totalCount")
}

fn zero_count_reason(provider_total_count: u64) -> Option<String> {
    if provider_total_count == 0 {
        Some("provider_total_count_zero".to_owned())
    } else {
        None
    }
}

fn is_vworld_invalid_range_error(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("code=INVALID_RANGE")
}

fn required_pages_for_total_count(provider_total_count: u64, effective_page_size: u32) -> u32 {
    if provider_total_count == 0 {
        return 1;
    }
    provider_total_count.div_ceil(u64::from(effective_page_size)) as u32
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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
}

impl ScopeRow {
    fn validate(&self, line_number: usize) -> anyhow::Result<()> {
        if self.schema_version != SCOPE_ROW_SCHEMA_VERSION {
            bail!("scope line {line_number} schema mismatch");
        }
        if self.scope_kind != "legal_dong" {
            bail!("scope line {line_number} scope_kind must be legal_dong");
        }
        if !is_five_digit_code(&self.sigungu_cd) || !is_five_digit_code(&self.bjdong_cd) {
            bail!("scope line {line_number} sigungu_cd and bjdong_cd must be five digits");
        }
        let expected_code = self.legal_dong_code();
        if self.bjdong_code != expected_code || self.canonical_code != expected_code {
            bail!("scope line {line_number} bjdong identity mismatch");
        }
        if self.scope_key != format!("{}:{}", self.sigungu_cd, self.bjdong_cd) {
            bail!("scope line {line_number} scope_key mismatch");
        }
        if self.scope_unit_id != format!("scope:legal-dong:{expected_code}") {
            bail!("scope line {line_number} scope_unit_id must match legal dong code");
        }
        if self.geometry_srid != 4326 {
            bail!("scope line {line_number} geometry_srid must be EPSG 4326");
        }
        Ok(())
    }

    fn legal_dong_code(&self) -> String {
        format!("{}{}", self.sigungu_cd, self.bjdong_cd)
    }

    fn provider_emd_cd(&self) -> String {
        self.legal_dong_code()[..8].to_owned()
    }

    fn cadastral_job_id(&self) -> String {
        format!("vworld-cadastral-{}-{}", self.sigungu_cd, self.bjdong_cd)
    }

    fn land_register_job_id(&self) -> String {
        format!(
            "vworld-land-register-{}-{}",
            self.sigungu_cd, self.bjdong_cd
        )
    }

    fn cadastral_attr_filter(&self) -> String {
        format!("emdCd:=:{}", self.provider_emd_cd())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum VWorldProbeJob {
    Cadastral(ScopeRow),
    LandRegister(ScopeRow),
}

fn is_five_digit_code(value: &str) -> bool {
    value.len() == 5 && value.as_bytes().iter().all(u8::is_ascii_digit)
}

fn read_scope_rows(path: &Path) -> anyhow::Result<Vec<ScopeRow>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read scope JSONL {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = if index == 0 {
            line.trim_start_matches('\u{feff}')
        } else {
            line
        };
        if line.trim().is_empty() {
            bail!("scope JSONL line {} must not be blank", index + 1);
        }
        let row: ScopeRow = serde_json::from_str(line)
            .with_context(|| format!("scope JSONL line {} is not valid JSON", index + 1))?;
        row.validate(index + 1)?;
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("scope JSONL must contain at least one row");
    }
    Ok(rows)
}

fn select_scope_rows(
    rows: &[ScopeRow],
    skip_scope_rows: usize,
    max_scope_rows: Option<usize>,
    request_cap: usize,
) -> anyhow::Result<Vec<ScopeRow>> {
    let selected = rows
        .iter()
        .skip(skip_scope_rows)
        .take(max_scope_rows.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!("selected VWorld page-count scope must not be empty");
    }
    let selected_probe_requests = selected
        .len()
        .checked_mul(2)
        .context("selected VWorld page-count request count exceeds usize")?;
    if request_cap < selected_probe_requests {
        bail!("request_cap must be at least selected VWorld probe request count");
    }
    Ok(selected)
}

fn validate_scope_evidence(
    evidence_path: &Path,
    scope_jsonl_path: &Path,
    scope_row_count: usize,
) -> anyhow::Result<()> {
    let content = fs::read_to_string(evidence_path)
        .with_context(|| format!("failed to read scope evidence {}", evidence_path.display()))?;
    let evidence: ScopeEvidence = serde_json::from_str(content.trim_start_matches('\u{feff}'))
        .context("scope evidence is not valid JSON")?;
    if evidence.schema_version != SCOPE_EVIDENCE_SCHEMA_VERSION {
        bail!("scope evidence schema mismatch");
    }
    if evidence.status != "ready" {
        bail!("scope evidence status must be ready");
    }
    let expected_repo_relative = repo_relative_path(scope_jsonl_path, &current_dir_lossy());
    let expected_input_relative = scope_jsonl_path.to_string_lossy().replace('\\', "/");
    if evidence.output_path != expected_repo_relative
        && evidence.output_path != expected_input_relative
    {
        bail!("scope evidence output_path must match scope JSONL path");
    }
    if evidence.scope_row_count != scope_row_count {
        bail!("scope evidence row count must match scope JSONL");
    }
    if evidence.completion_claim_allowed || evidence.national_rollout_allowed {
        bail!("scope evidence must not allow completion claim or national rollout");
    }
    Ok(())
}

fn current_dir_lossy() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

#[derive(Debug, Deserialize)]
struct ScopeEvidence {
    schema_version: String,
    status: String,
    output_path: String,
    scope_row_count: usize,
    completion_claim_allowed: bool,
    national_rollout_allowed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldPageCountBatchConfig {
    scope_jsonl_path: PathBuf,
    scope_evidence_path: PathBuf,
    output_path: PathBuf,
    probe_output_root: PathBuf,
    max_scope_rows: Option<usize>,
    skip_scope_rows: usize,
    request_cap: usize,
    max_in_flight: usize,
    data_base_uri: String,
    ned_base_uri: String,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    request_policy: VWorldRequestPolicy,
    cadastral_size: u32,
    land_register_num_of_rows: u32,
}

impl VWorldPageCountBatchConfig {
    fn from_env() -> anyhow::Result<Self> {
        require_confirm("FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_CONFIRM_PUBLIC_API_QUOTA_IMPACT")?;
        require_confirm("FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_CONFIRM_PROBE")?;
        let default_policy = VWorldRequestPolicy::default();
        let max_attempts = optional_positive_u32_env("FOUNDATION_PLATFORM_VWORLD_MAX_ATTEMPTS")?
            .unwrap_or_else(|| default_policy.max_attempts());
        let request_timeout =
            optional_duration_seconds_env("FOUNDATION_PLATFORM_VWORLD_REQUEST_TIMEOUT_SECONDS")?
                .unwrap_or_else(|| default_policy.request_timeout());
        let initial_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_INITIAL_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.initial_backoff());
        let max_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_MAX_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.max_backoff());

        Ok(Self {
            scope_jsonl_path: required_path_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_SCOPE_JSONL_PATH",
            )?,
            scope_evidence_path: optional_path_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_SCOPE_EVIDENCE_PATH",
                "target/audit/national-data-collection-scope-evidence.json",
            )?,
            output_path: optional_path_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_PLAN_OUTPUT_PATH",
                "target/audit/vworld-page-count-plan.json",
            )?,
            probe_output_root: optional_path_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_PROBE_OUTPUT_ROOT",
                "target/audit/vworld-page-count-probes",
            )?,
            max_scope_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_MAX_SCOPE_ROWS",
            )?,
            skip_scope_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_SKIP_SCOPE_ROWS",
            )?
            .unwrap_or(0),
            request_cap: optional_usize_env("FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_REQUEST_CAP")?
                .unwrap_or(2),
            max_in_flight: optional_usize_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_MAX_IN_FLIGHT",
            )?
            .unwrap_or(1)
            .clamp(1, 16),
            data_base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATA_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_DATA_BASE_URI.to_owned()),
            ned_base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_NED_BASE_URI.to_owned()),
            api_key: required_env_value("VWORLD_API_KEY")?,
            domain: optional_env_value("VWORLD_DOMAIN")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request_policy: VWorldRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
            cadastral_size: optional_positive_u32_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_CADASTRAL_SIZE",
            )?
            .unwrap_or_else(default_cadastral_page_size),
            land_register_num_of_rows: optional_positive_u32_env(
                "FOUNDATION_PLATFORM_VWORLD_PAGE_COUNT_LAND_REGISTER_NUM_OF_ROWS",
            )?
            .unwrap_or(1000),
        })
    }
}

fn require_confirm(name: &str) -> anyhow::Result<()> {
    if optional_env_value(name)?.as_deref() != Some("1") {
        bail!("{name}=1 is required for VWorld page-count batch")
    }
    Ok(())
}

fn required_path_env(name: &str) -> anyhow::Result<PathBuf> {
    optional_env_value(name)?
        .map(PathBuf::from)
        .with_context(|| format!("{name} is required"))
}

fn optional_path_env(name: &str, default: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from(
        optional_env_value(name)?.unwrap_or_else(|| default.to_owned()),
    ))
}

#[derive(Debug, Serialize)]
struct VWorldPageCountPlan {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    source: &'static str,
    scope_source: ScopeSource,
    request_plan: PageCountRequestPlan,
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
    skip_scope_rows: usize,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct PageCountRequestPlan {
    request_cap: usize,
    request_count_estimate: usize,
    selected_job_count: usize,
    selected_scope_count: usize,
    provider: &'static str,
    endpoints: Vec<&'static str>,
    cadastral_page_size: u32,
    land_register_num_of_rows: u32,
    max_in_flight: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PageCountJob {
    job_id: String,
    scope_unit_id: String,
    provider: &'static str,
    endpoint_slug: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation: Option<String>,
    sigungu_cd: String,
    bjdong_cd: String,
    requested_page_size: u32,
    effective_page_size: u32,
    provider_total_count: u64,
    required_pages: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_empty_reason: Option<String>,
    probe_output_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct FailedPageCountJob {
    job_id: String,
    scope_unit_id: String,
    sigungu_cd: String,
    bjdong_cd: String,
    error_kind: &'static str,
    error_message: String,
    duration_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct VWorldPageCountProbe {
    job_id: String,
    provider: String,
    endpoint_slug: String,
    scope_unit_id: String,
    sigungu_cd: String,
    bjdong_cd: String,
    requested_page_size: u32,
    effective_page_size: u32,
    provider_total_count: u64,
    required_pages: u32,
    selector: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_empty_reason: Option<String>,
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(value).context("failed to serialize JSON")?;
    fs::write(path, payload).with_context(|| format!("failed to write {}", path.display()))
}

fn sha256_file_hex(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(hex_lower(&Sha256::digest(bytes)))
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn repo_relative_path(path: &Path, repo_root: &Path) -> String {
    let repo_root = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    let normalized = path.canonicalize().unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            repo_root.join(path)
        }
    });
    let relative = normalized
        .strip_prefix(&repo_root)
        .map_or(normalized.as_path(), |stripped| stripped);
    relative.to_string_lossy().replace('\\', "/")
}

fn safe_error_message(message: &str) -> String {
    message
        .split_whitespace()
        .map(|part| {
            if part.starts_with("key=") || part.starts_with("api_key=") {
                "key=<redacted>"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn git_head() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        is_vworld_invalid_range_error, land_register_total_count, required_json_u64_pointer,
        required_pages_for_total_count, select_scope_rows, ScopeRow,
    };

    fn scope_row(sigungu_cd: &str, bjdong_cd: &str) -> ScopeRow {
        let bjdong_code = format!("{sigungu_cd}{bjdong_cd}");
        ScopeRow {
            schema_version: "foundation-platform.national_data_collection_scope_row.v1".to_owned(),
            scope_unit_id: format!("scope:legal-dong:{bjdong_code}"),
            scope_kind: "legal_dong".to_owned(),
            canonical_code: bjdong_code.clone(),
            scope_key: format!("{sigungu_cd}:{bjdong_cd}"),
            bjdong_code,
            sigungu_cd: sigungu_cd.to_owned(),
            bjdong_cd: bjdong_cd.to_owned(),
            geometry_srid: 4326,
        }
    }

    #[test]
    fn vworld_scope_row_derives_provider_selectors() {
        let row = scope_row("11680", "10300");

        assert_eq!(row.cadastral_job_id(), "vworld-cadastral-11680-10300");
        assert_eq!(
            row.land_register_job_id(),
            "vworld-land-register-11680-10300"
        );
        assert_eq!(row.provider_emd_cd(), "11680103");
        assert_eq!(row.cadastral_attr_filter(), "emdCd:=:11680103");
        assert_eq!(row.legal_dong_code(), "1168010300");
    }

    #[test]
    fn required_pages_uses_minimum_one_page() {
        assert_eq!(required_pages_for_total_count(0, 100), 1);
        assert_eq!(required_pages_for_total_count(1, 100), 1);
        assert_eq!(required_pages_for_total_count(100, 100), 1);
        assert_eq!(required_pages_for_total_count(101, 100), 2);
        assert_eq!(required_pages_for_total_count(817, 100), 9);
    }

    #[test]
    fn cadastral_page_count_default_uses_provider_efficient_window() {
        assert_eq!(super::default_cadastral_page_size(), 1000);
    }

    #[test]
    fn parses_vworld_total_count_metadata() -> anyhow::Result<()> {
        let cadastral = json!({
            "response": {
                "record": {
                    "total": "817"
                }
            }
        });
        assert_eq!(
            required_json_u64_pointer(&cadastral, "/response/record/total")?,
            817
        );

        let land = json!({
            "ladfrlVOList": {
                "totalCount": "819"
            }
        });
        assert_eq!(
            required_json_u64_pointer(&land, "/ladfrlVOList/totalCount")?,
            819
        );
        let empty_land = json!({
            "response": {
                "totalCount": "0"
            }
        });
        assert_eq!(land_register_total_count(&empty_land)?, 0);
        Ok(())
    }

    #[test]
    fn detects_vworld_invalid_range_as_provider_empty_scope() {
        let error = anyhow::anyhow!(
            "VWorld Data API request failed with status=ERROR code=INVALID_RANGE text=attrFilter invalid"
        );

        assert!(is_vworld_invalid_range_error(&error));
    }

    #[test]
    fn select_scope_rows_counts_two_vworld_probe_requests_per_scope() -> anyhow::Result<()> {
        let rows = vec![scope_row("11680", "10300"), scope_row("28200", "11000")];

        assert!(select_scope_rows(&rows, 0, None, 3).is_err());
        let selected = select_scope_rows(&rows, 0, Some(1), 2)?;

        assert_eq!(selected, vec![scope_row("11680", "10300")]);
        Ok(())
    }
}
