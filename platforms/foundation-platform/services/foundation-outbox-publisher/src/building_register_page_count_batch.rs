//! Batch page-count probe for data.go.kr building-register national planning.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, Context};
use collection_application::BuildingRegisterPageRequest;
use collection_infrastructure::{DataGoKrBuildingRegisterClient, DataGoKrBuildingRegisterConfig};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::building_register_ingest::{
    page_count_probe_from_response_metadata, write_page_count_probe_output,
    BuildingRegisterIngestConfig, BuildingRegisterPageCountProbe, DEFAULT_OPERATION,
};
use crate::public_data_control_support::{optional_env_value, optional_usize_env};

const PLAN_SCHEMA_VERSION: &str = "foundation-platform.building_register_page_count_plan.v1";
const SCOPE_ROW_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope_row.v1";
const SCOPE_EVIDENCE_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope.v1";
const SOURCE: &str = "data-go-kr-building-register-page-count-probe";
const PROVIDER: &str = "data.go.kr";

/// Runs a one-process batch probe that writes the manifest-ready page-count plan.
pub async fn run() -> anyhow::Result<()> {
    let ingest_config = BuildingRegisterIngestConfig::from_env()?;
    let batch_config = BuildingRegisterPageCountBatchConfig::from_env()?;
    let operations = parse_operations(
        optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_OPERATIONS")?
            .as_deref(),
        &ingest_config.request.operation,
    )?;
    let endpoint_slugs = operations
        .iter()
        .map(|operation| endpoint_slug_for_operation(operation).map(str::to_owned))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let scope_rows = read_scope_rows(&batch_config.scope_jsonl_path)?;
    validate_scope_evidence(
        &batch_config.scope_evidence_path,
        &batch_config.scope_jsonl_path,
        scope_rows.len(),
    )?;
    let selected_rows = select_scope_rows(
        &scope_rows,
        batch_config.skip_jobs,
        batch_config.max_jobs,
        batch_config.request_cap,
        operations.len(),
    )?;

    if batch_config.output_path.exists() {
        bail!(
            "building-register page count plan already exists: {}",
            batch_config.output_path.display()
        );
    }

    let client = DataGoKrBuildingRegisterClient::new_with_policy(
        &DataGoKrBuildingRegisterConfig {
            base_uri: ingest_config.base_uri.clone(),
            service_key: ingest_config.service_key.clone(),
        },
        ingest_config.request_policy,
    )?;
    let requested_page_size = ingest_config.request.num_of_rows;
    let probe_root = batch_config.probe_output_root.clone();
    let repo_root = std::env::current_dir().context("failed to resolve current directory")?;
    let probe_inputs = selected_rows
        .iter()
        .cloned()
        .flat_map(|row| {
            operations
                .iter()
                .cloned()
                .map(move |operation| (row.clone(), operation))
        })
        .collect::<Vec<_>>();

    let results = stream::iter(probe_inputs.into_iter().map(|(row, operation)| {
        let client = client.clone();
        let probe_root = probe_root.clone();
        let repo_root = repo_root.clone();
        async move {
            probe_scope_row(
                &client,
                &row,
                &operation,
                requested_page_size,
                &probe_root,
                &repo_root,
            )
            .await
        }
    }))
    .buffer_unordered(batch_config.max_in_flight)
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

    let status = if failed_jobs.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let plan = BuildingRegisterPageCountPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        generated_at_utc: chrono::Utc::now().to_rfc3339(),
        git_head: git_head(),
        status,
        source: SOURCE,
        scope_source: ScopeSource {
            path: repo_relative_path(&batch_config.scope_jsonl_path, &repo_root),
            evidence_path: repo_relative_path(&batch_config.scope_evidence_path, &repo_root),
            row_count: scope_rows.len(),
            selected_rows: selected_rows.len(),
            skip_jobs: batch_config.skip_jobs,
            sha256: sha256_file_hex(&batch_config.scope_jsonl_path)?,
        },
        request_plan: PageCountRequestPlan {
            request_cap: batch_config.request_cap,
            request_count_estimate: selected_rows.len() * operations.len(),
            selected_job_count: selected_rows.len() * operations.len(),
            requested_page_size,
            provider: PROVIDER,
            endpoint_slug: endpoint_slugs[0].clone(),
            operation: operations[0].clone(),
            endpoint_slugs,
            operations,
            max_in_flight: batch_config.max_in_flight,
        },
        probe_output_root: repo_relative_path(&batch_config.probe_output_root, &repo_root),
        execute: true,
        attempted_request_count: results_len(&jobs, &failed_jobs),
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

    write_json(&batch_config.output_path, &plan)?;
    if plan.status == "blocked" {
        bail!(
            "building-register page-count batch blocked failed_jobs={}",
            plan.failed_jobs.len()
        );
    }

    tracing::info!(
        jobs = plan.jobs.len(),
        requests = plan.request_plan.request_count_estimate,
        output_path = %batch_config.output_path.display(),
        "building-register page-count batch succeeded"
    );
    Ok(())
}

fn results_len(jobs: &[PageCountJob], failed_jobs: &[FailedPageCountJob]) -> usize {
    jobs.len() + failed_jobs.len()
}

async fn probe_scope_row(
    client: &DataGoKrBuildingRegisterClient,
    row: &ScopeRow,
    operation: &str,
    requested_page_size: u32,
    probe_output_root: &Path,
    repo_root: &Path,
) -> Result<PageCountJob, FailedPageCountJob> {
    let job_id = row.job_id_for_operation(operation);
    let probe_output_path = probe_output_root.join(format!("{job_id}.json"));
    let request = BuildingRegisterPageRequest {
        operation: operation.to_owned(),
        sigungu_cd: row.sigungu_cd.clone(),
        bjdong_cd: row.bjdong_cd.clone(),
        page_no: 1,
        num_of_rows: requested_page_size,
    };
    let started = Instant::now();
    let result = async {
        let fetched_page = client
            .fetch_page(&request)
            .await
            .context("failed to fetch data.go.kr building-register page-count probe")?;
        let probe = page_count_probe_from_response_metadata(&request, &fetched_page.payload)?;
        write_page_count_probe_output(&probe_output_path, &probe)?;
        page_count_job(row, &probe, &probe_output_path, repo_root)
    }
    .await;

    match result {
        Ok(job) => Ok(job),
        Err(error) => Err(FailedPageCountJob {
            job_id,
            scope_unit_id: row.scope_unit_id.clone(),
            sigungu_cd: row.sigungu_cd.clone(),
            bjdong_cd: row.bjdong_cd.clone(),
            error_kind: "probe_failed",
            error_message: safe_error_message(&error.to_string()),
            duration_ms: started.elapsed().as_millis() as u64,
        }),
    }
}

fn page_count_job(
    row: &ScopeRow,
    probe: &BuildingRegisterPageCountProbe,
    probe_output_path: &Path,
    repo_root: &Path,
) -> anyhow::Result<PageCountJob> {
    Ok(PageCountJob {
        job_id: row.job_id_for_operation(&probe.operation),
        scope_unit_id: row.scope_unit_id.clone(),
        endpoint_slug: endpoint_slug_for_operation(&probe.operation)?.to_owned(),
        operation: probe.operation.clone(),
        sigungu_cd: probe.sigungu_cd.clone(),
        bjdong_cd: probe.bjdong_cd.clone(),
        requested_page_size: probe.requested_page_size,
        effective_page_size: probe.effective_page_size,
        provider_total_count: probe.provider_total_count,
        required_pages: probe.required_pages,
        probe_output_path: repo_relative_path(probe_output_path, repo_root),
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
        let expected_code = format!("{}{}", self.sigungu_cd, self.bjdong_cd);
        if self.bjdong_code != expected_code || self.canonical_code != expected_code {
            bail!("scope line {line_number} bjdong identity mismatch");
        }
        if self.scope_key != format!("{}:{}", self.sigungu_cd, self.bjdong_cd) {
            bail!("scope line {line_number} scope_key mismatch");
        }
        if self.scope_unit_id.trim().is_empty() {
            bail!("scope line {line_number} scope_unit_id is required");
        }
        if self.geometry_srid != 4326 {
            bail!("scope line {line_number} geometry_srid must be EPSG 4326");
        }
        Ok(())
    }

    fn job_id_for_operation(&self, operation: &str) -> String {
        if operation == DEFAULT_OPERATION {
            format!("building-register-{}-{}", self.sigungu_cd, self.bjdong_cd)
        } else {
            format!(
                "building-register-{}-{}-{}",
                operation, self.sigungu_cd, self.bjdong_cd
            )
        }
    }
}

// endpoint_slug intentionally retains the legacy kebab form (ADR 0014 D6); NOT a Bronze source_slug.
fn endpoint_slug_for_operation(operation: &str) -> anyhow::Result<&'static str> {
    match operation {
        "getBrTitleInfo" => Ok("data-go-kr-building-register-getBrTitleInfo"),
        "getBrBasisOulnInfo" => Ok("data-go-kr-building-register-getBrBasisOulnInfo"),
        "getBrFlrOulnInfo" => Ok("data-go-kr-building-register-getBrFlrOulnInfo"),
        "getBrExposPubuseAreaInfo" => Ok("data-go-kr-building-register-getBrExposPubuseAreaInfo"),
        "getBrHsprcInfo" => Ok("data-go-kr-building-register-getBrHsprcInfo"),
        "getBrExposInfo" => Ok("data-go-kr-building-register-getBrExposInfo"),
        "getBrWclfInfo" => Ok("data-go-kr-building-register-getBrWclfInfo"),
        "getBrRecapTitleInfo" => Ok("data-go-kr-building-register-getBrRecapTitleInfo"),
        "getBrAtchJibunInfo" => Ok("data-go-kr-building-register-getBrAtchJibunInfo"),
        "getBrJijiguInfo" => Ok("data-go-kr-building-register-getBrJijiguInfo"),
        _ => bail!("unsupported building-register operation: {operation}"),
    }
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
    skip_jobs: usize,
    max_jobs: Option<usize>,
    request_cap: usize,
    requests_per_row: usize,
) -> anyhow::Result<Vec<ScopeRow>> {
    if requests_per_row < 1 {
        bail!("requests_per_row must be positive");
    }
    let selected = rows
        .iter()
        .skip(skip_jobs)
        .take(max_jobs.unwrap_or(usize::MAX))
        .cloned()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!("selected page-count scope must not be empty");
    }
    if request_cap < selected.len() * requests_per_row {
        bail!("request_cap must be at least selected job count");
    }
    Ok(selected)
}

fn parse_operations(raw: Option<&str>, fallback_operation: &str) -> anyhow::Result<Vec<String>> {
    let candidates = raw
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|operation| !operation.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![fallback_operation.to_owned()]);
    if candidates.is_empty() {
        bail!("building-register page-count operations must not be empty");
    }

    let mut seen = HashSet::new();
    let mut operations = Vec::new();
    for operation in candidates {
        endpoint_slug_for_operation(&operation)?;
        if seen.insert(operation.clone()) {
            operations.push(operation);
        }
    }
    Ok(operations)
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
struct BuildingRegisterPageCountBatchConfig {
    scope_jsonl_path: PathBuf,
    scope_evidence_path: PathBuf,
    output_path: PathBuf,
    probe_output_root: PathBuf,
    max_jobs: Option<usize>,
    skip_jobs: usize,
    request_cap: usize,
    max_in_flight: usize,
}

impl BuildingRegisterPageCountBatchConfig {
    fn from_env() -> anyhow::Result<Self> {
        require_confirm(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_CONFIRM_PUBLIC_API_QUOTA_IMPACT",
        )?;
        require_confirm("FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_CONFIRM_PROBE")?;
        let scope_jsonl_path =
            required_path_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_SCOPE_JSONL_PATH")?;
        Ok(Self {
            scope_jsonl_path,
            scope_evidence_path: optional_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_SCOPE_EVIDENCE_PATH",
                "target/audit/national-data-collection-scope-evidence.json",
            )?,
            output_path: optional_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_PLAN_OUTPUT_PATH",
                "target/audit/building-register-page-count-plan.json",
            )?,
            probe_output_root: optional_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_PROBE_OUTPUT_ROOT",
                "target/audit/building-register-page-count-probes",
            )?,
            max_jobs: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_MAX_JOBS",
            )?,
            skip_jobs: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_SKIP_JOBS",
            )?
            .unwrap_or(0),
            request_cap: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_REQUEST_CAP",
            )?
            .unwrap_or(1),
            max_in_flight: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_MAX_IN_FLIGHT",
            )?
            .unwrap_or(1)
            .clamp(1, 16),
        })
    }
}

fn require_confirm(name: &str) -> anyhow::Result<()> {
    if optional_env_value(name)?.as_deref() != Some("1") {
        bail!("{name}=1 is required for building-register page-count batch")
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
struct BuildingRegisterPageCountPlan {
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
    skip_jobs: usize,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct PageCountRequestPlan {
    request_cap: usize,
    request_count_estimate: usize,
    selected_job_count: usize,
    requested_page_size: u32,
    provider: &'static str,
    endpoint_slug: String,
    operation: String,
    endpoint_slugs: Vec<String>,
    operations: Vec<String>,
    max_in_flight: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PageCountJob {
    job_id: String,
    scope_unit_id: String,
    endpoint_slug: String,
    operation: String,
    sigungu_cd: String,
    bjdong_cd: String,
    requested_page_size: u32,
    effective_page_size: u32,
    provider_total_count: u64,
    required_pages: u32,
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
            if part.starts_with("serviceKey=") || part.starts_with("service_key=") {
                "serviceKey=<redacted>"
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
    use std::path::Path;

    use collection_application::BuildingRegisterPageRequest;
    use serde_json::json;

    use super::{page_count_job, read_scope_rows, select_scope_rows};
    use crate::building_register_ingest::page_count_probe_from_response_metadata;

    #[test]
    fn selects_scope_rows_with_operation_multiplier_budget() -> anyhow::Result<()> {
        let rows = vec![scope_row("11110", "10100"), scope_row("11110", "10200")];

        let selected = select_scope_rows(&rows, 1, Some(1), 2, 2)?;

        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected[0].job_id_for_operation(super::DEFAULT_OPERATION),
            "building-register-11110-10200"
        );
        assert!(select_scope_rows(&rows, 0, Some(2), 3, 2)
            .unwrap_err()
            .to_string()
            .contains("request_cap"));
        Ok(())
    }

    #[test]
    fn parses_distinct_operation_list_for_multi_endpoint_probe() -> anyhow::Result<()> {
        let operations = super::parse_operations(
            Some(" getBrTitleInfo, getBrFlrOulnInfo, getBrFlrOulnInfo "),
            "getBrTitleInfo",
        )?;

        assert_eq!(operations, vec!["getBrTitleInfo", "getBrFlrOulnInfo"]);
        assert!(
            super::parse_operations(Some("getBrUnknownInfo"), "getBrTitleInfo")
                .unwrap_err()
                .to_string()
                .contains("unsupported building-register operation")
        );
        Ok(())
    }

    #[test]
    fn parses_scope_jsonl_and_rejects_identity_mismatch() -> anyhow::Result<()> {
        let dir = Path::new("target/building-register-page-count-batch-tests");
        std::fs::create_dir_all(dir)?;
        let valid_path = dir.join("scope-valid.jsonl");
        std::fs::write(
            &valid_path,
            serde_json::to_string(&scope_row("11110", "10100"))? + "\n",
        )?;
        assert_eq!(read_scope_rows(&valid_path)?.len(), 1);

        let invalid_path = dir.join("scope-invalid.jsonl");
        let mut invalid = scope_row("11110", "10100");
        invalid.bjdong_code = "1111010200".to_owned();
        std::fs::write(&invalid_path, serde_json::to_string(&invalid)? + "\n")?;
        assert!(read_scope_rows(&invalid_path)
            .unwrap_err()
            .to_string()
            .contains("bjdong identity mismatch"));

        let bom_path = dir.join("scope-bom.jsonl");
        let bom_prefixed = format!(
            "\u{feff}{}\n",
            serde_json::to_string(&scope_row("11110", "10300"))?
        );
        std::fs::write(&bom_path, bom_prefixed)?;
        assert_eq!(read_scope_rows(&bom_path)?.len(), 1);
        Ok(())
    }

    #[test]
    fn page_count_job_preserves_probe_metadata_and_repo_relative_output() -> anyhow::Result<()> {
        let row = scope_row("11110", "10100");
        let request = BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11110".to_owned(),
            bjdong_cd: "10100".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        };
        let probe = page_count_probe_from_response_metadata(
            &request,
            &json!({
                "response": {
                    "body": {
                        "numOfRows": "100",
                        "totalCount": "358"
                    }
                }
            }),
        )?;

        let job = page_count_job(
            &row,
            &probe,
            Path::new("target/audit/probes/building-register-11110-10100.json"),
            Path::new("."),
        )?;

        assert_eq!(job.job_id, "building-register-11110-10100");
        assert_eq!(job.provider_total_count, 358);
        assert_eq!(job.required_pages, 4);
        assert_eq!(
            job.probe_output_path,
            "target/audit/probes/building-register-11110-10100.json"
        );
        Ok(())
    }

    #[test]
    fn page_count_job_uses_operation_specific_identity_for_non_title_operations(
    ) -> anyhow::Result<()> {
        let row = scope_row("11110", "10100");
        let request = BuildingRegisterPageRequest {
            operation: "getBrFlrOulnInfo".to_owned(),
            sigungu_cd: "11110".to_owned(),
            bjdong_cd: "10100".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        };
        let probe = page_count_probe_from_response_metadata(
            &request,
            &json!({
                "response": {
                    "body": {
                        "numOfRows": "100",
                        "totalCount": "201"
                    }
                }
            }),
        )?;

        let job = page_count_job(
            &row,
            &probe,
            Path::new("target/audit/probes/building-register-getBrFlrOulnInfo-11110-10100.json"),
            Path::new("."),
        )?;

        assert_eq!(job.job_id, "building-register-getBrFlrOulnInfo-11110-10100");
        assert_eq!(
            job.endpoint_slug,
            "data-go-kr-building-register-getBrFlrOulnInfo"
        );
        assert_eq!(job.required_pages, 3);
        Ok(())
    }

    #[test]
    fn repo_relative_path_strips_canonical_windows_prefixes() -> anyhow::Result<()> {
        let repo_root = std::env::current_dir()?;
        let path = repo_root.join("target/building-register-page-count-batch-tests/existing.json");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, "{}")?;

        assert_eq!(
            super::repo_relative_path(&path, &repo_root),
            "target/building-register-page-count-batch-tests/existing.json"
        );
        Ok(())
    }

    #[test]
    fn validates_scope_evidence_with_utf8_bom() -> anyhow::Result<()> {
        let dir = Path::new("target/building-register-page-count-batch-tests");
        std::fs::create_dir_all(dir)?;
        let scope_path = dir.join("scope-evidence-scope.jsonl");
        std::fs::write(
            &scope_path,
            serde_json::to_string(&scope_row("11110", "10100"))? + "\n",
        )?;
        let evidence_path = dir.join("scope-evidence.json");
        let evidence = json!({
            "schema_version": "foundation-platform.national_data_collection_scope.v1",
            "status": "ready",
            "output_path": super::repo_relative_path(&scope_path, &std::env::current_dir()?),
            "scope_row_count": 1,
            "completion_claim_allowed": false,
            "national_rollout_allowed": false
        });
        std::fs::write(
            &evidence_path,
            format!("\u{feff}{}", serde_json::to_string(&evidence)?),
        )?;

        super::validate_scope_evidence(&evidence_path, &scope_path, 1)?;
        Ok(())
    }

    fn scope_row(sigungu_cd: &str, bjdong_cd: &str) -> super::ScopeRow {
        super::ScopeRow {
            schema_version: super::SCOPE_ROW_SCHEMA_VERSION.to_owned(),
            scope_unit_id: format!("scope:legal-dong:{sigungu_cd}{bjdong_cd}"),
            scope_kind: "legal_dong".to_owned(),
            canonical_code: format!("{sigungu_cd}{bjdong_cd}"),
            scope_key: format!("{sigungu_cd}:{bjdong_cd}"),
            bjdong_code: format!("{sigungu_cd}{bjdong_cd}"),
            sigungu_cd: sigungu_cd.to_owned(),
            bjdong_cd: bjdong_cd.to_owned(),
            geometry_srid: 4326,
        }
    }
}
