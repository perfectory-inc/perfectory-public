use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use anyhow::{bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use collection_infrastructure::{
    VWorldDatasetFileClient, VWorldDatasetFileConfig, VWorldDatasetFileInventoryItem,
    VWorldDatasetFileInventorySelector, VWorldDatasetFileKind,
};
use serde::{Deserialize, Serialize};

use crate::public_data_control_support::optional_env_value;

const REPORT_SCHEMA_VERSION: &str = "foundation-platform.vworld_dataset_file_inventory.v1";
const DEFAULT_COLLECTION_PLAN_PATH: &str = "target/audit/vworld-dataset-collection-plan.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/vworld-dataset-file-inventory.json";
const DEFAULT_USER_AGENT: &str = "foundation-platform-vworld-dataset-file-inventory/1.0";
const DEFAULT_PAGE_SIZE: u64 = 100;

pub async fn run() -> Result<()> {
    let config = VWorldDatasetFileInventoryConfig::from_env()?;
    let plan_json = fs::read_to_string(&config.plan_path).with_context(|| {
        format!(
            "failed to read VWorld dataset collection plan: {}",
            config.plan_path.display()
        )
    })?;
    let plan = parse_collection_plan(&plan_json)?;
    let selected_jobs = select_jobs(&plan.jobs, config.max_jobs)?;
    let mut files_by_job = Vec::with_capacity(selected_jobs.len());

    for job in selected_jobs {
        let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
            base_uri: config
                .base_uri
                .clone()
                .unwrap_or_else(|| job.base_uri.clone()),
            user_agent: config.user_agent.clone(),
            page_size: config.page_size,
            cookie_header: config.cookie_header.clone(),
        })?;
        let files = client
            .fetch_dataset_file_inventory(&VWorldDatasetFileInventorySelector {
                svc_cde: job.svc_cde.clone(),
                ds_id: job.ds_id.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "failed to fetch VWorld dataset file inventory for {} svc_cde={} ds_id={}",
                    job.endpoint_slug, job.svc_cde, job.ds_id
                )
            })?;
        files_by_job.push((job.endpoint_slug.clone(), files));
    }

    let selected_endpoint_slugs = selected_jobs
        .iter()
        .map(|job| job.endpoint_slug.clone())
        .collect::<Vec<_>>();
    let report = compile_vworld_dataset_file_inventory_report(
        &plan_json,
        &config.plan_path.to_string_lossy().replace('\\', "/"),
        &selected_endpoint_slugs,
        files_by_job,
    )?;
    write_report(&config.output_path, &report)?;
    if report.status == "blocked" {
        bail!(
            "VWorld dataset file inventory blocked blockers={} report={}",
            report.blockers.len(),
            config.output_path.display()
        );
    }
    tracing::info!(
        jobs = report.inventory_job_count,
        files = report.discovered_file_count,
        selection_archives = report.selection_archive_file_count,
        report = %config.output_path.display(),
        "VWorld dataset file inventory written"
    );
    Ok(())
}

fn compile_vworld_dataset_file_inventory_report(
    plan_json: &str,
    plan_path: &str,
    selected_endpoint_slugs: &[String],
    files_by_job: Vec<(String, Vec<VWorldDatasetFileInventoryItem>)>,
) -> Result<VWorldDatasetFileInventoryReport> {
    let plan = parse_collection_plan(plan_json)?;
    let selected_endpoint_slug_set = selected_endpoint_slugs
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
    let mut count_drift = Vec::new();
    if plan.status != "ready" {
        blockers.push(format!(
            "VWorld dataset collection plan status must be ready, got {}",
            plan.status
        ));
    }
    if plan.job_count != plan.jobs.len() as u64 {
        blockers.push(format!(
            "VWorld dataset collection plan job_count {} did not match jobs {}",
            plan.job_count,
            plan.jobs.len()
        ));
    }
    if selected_endpoint_slug_set.is_empty() {
        blockers.push("at least one selected VWorld dataset endpoint is required".to_owned());
    }

    let plan_endpoint_slug_set = plan
        .jobs
        .iter()
        .map(|job| job.endpoint_slug.clone())
        .collect::<BTreeSet<_>>();
    for selected_endpoint_slug in &selected_endpoint_slug_set {
        if !plan_endpoint_slug_set.contains(selected_endpoint_slug) {
            blockers.push(format!(
                "selected VWorld dataset endpoint {selected_endpoint_slug} does not exist in plan"
            ));
        }
    }

    let mut files_by_endpoint = files_by_job.into_iter().collect::<BTreeMap<_, _>>();
    let mut job_reports = Vec::with_capacity(plan.jobs.len());
    let mut expected_file_count = 0_u64;
    let mut discovered_file_count = 0_u64;
    let mut single_resource_file_count = 0_u64;
    let mut selection_archive_file_count = 0_u64;

    for job in plan
        .jobs
        .iter()
        .filter(|job| selected_endpoint_slug_set.contains(&job.endpoint_slug))
    {
        expected_file_count += job.file_count;
        let files = files_by_endpoint
            .remove(&job.endpoint_slug)
            .unwrap_or_default();
        let job_discovered_file_count = files.len() as u64;
        discovered_file_count += job_discovered_file_count;
        let job_single_count = files
            .iter()
            .filter(|file| file.download_kind == VWorldDatasetFileKind::SingleResourceFile)
            .count() as u64;
        let job_selection_count = files
            .iter()
            .filter(|file| file.download_kind == VWorldDatasetFileKind::SelectionArchive)
            .count() as u64;
        single_resource_file_count += job_single_count;
        selection_archive_file_count += job_selection_count;

        let file_count_drifted = job_discovered_file_count != job.file_count;
        let selection_archive_count_drifted = job_selection_count != job.large_file_count;
        if file_count_drifted {
            warnings.push(format!(
                "endpoint {} expected {} files, discovered {}",
                job.endpoint_slug, job.file_count, job_discovered_file_count
            ));
        }
        if selection_archive_count_drifted {
            warnings.push(format!(
                "endpoint {} expected {} large files, discovered {} selection archive files",
                job.endpoint_slug, job.large_file_count, job_selection_count
            ));
        }
        if file_count_drifted || selection_archive_count_drifted {
            count_drift.push(VWorldDatasetFileInventoryCountDrift {
                endpoint_slug: job.endpoint_slug.clone(),
                expected_file_count: job.file_count,
                discovered_file_count: job_discovered_file_count,
                expected_selection_archive_count: job.large_file_count,
                selection_archive_count: job_selection_count,
            });
        }
        for file in &files {
            if file.svc_cde != job.svc_cde || file.ds_id != job.ds_id {
                blockers.push(format!(
                    "endpoint {} file {} selector mismatch: expected {}/{}, got {}/{}",
                    job.endpoint_slug,
                    file.file_no,
                    job.svc_cde,
                    job.ds_id,
                    file.svc_cde,
                    file.ds_id
                ));
            }
        }

        job_reports.push(VWorldDatasetFileInventoryJobReport {
            endpoint_slug: job.endpoint_slug.clone(),
            source_slug: job.source_slug.clone(),
            source_name: job.source_name.clone(),
            dataset_name: job.dataset_name.clone(),
            base_uri: job.base_uri.clone(),
            terms_url: job.terms_url.clone(),
            operation: job.operation.clone(),
            provider_module: job.provider_module.clone(),
            svc_cde: job.svc_cde.clone(),
            ds_id: job.ds_id.clone(),
            expected_file_count: job.file_count,
            discovered_file_count: job_discovered_file_count,
            single_resource_file_count: job_single_count,
            selection_archive_file_count: job_selection_count,
            files,
        });
    }
    for endpoint_slug in files_by_endpoint.keys() {
        blockers.push(format!(
            "VWorld dataset file inventory included unknown endpoint {endpoint_slug}"
        ));
    }

    Ok(VWorldDatasetFileInventoryReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        status: if blockers.is_empty() {
            "ready".to_owned()
        } else {
            "blocked".to_owned()
        },
        plan_path: plan_path.to_owned(),
        plan_job_count: plan.jobs.len() as u64,
        inventory_job_count: job_reports.len() as u64,
        expected_file_count,
        discovered_file_count,
        single_resource_file_count,
        selection_archive_file_count,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        blockers,
        warnings,
        count_drift,
        jobs: job_reports,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldDatasetFileInventoryConfig {
    plan_path: PathBuf,
    output_path: PathBuf,
    base_uri: Option<String>,
    user_agent: String,
    page_size: u64,
    cookie_header: Option<String>,
    max_jobs: Option<usize>,
}

impl VWorldDatasetFileInventoryConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            plan_path: optional_env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH",
            )?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_COLLECTION_PLAN_PATH)),
            output_path: optional_env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH",
            )?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH)),
            base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_BASE_URI")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            page_size: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE")?
                .map(|value| {
                    parse_positive_u64("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE", &value)
                })
                .transpose()?
                .unwrap_or(DEFAULT_PAGE_SIZE),
            cookie_header: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER")?,
            max_jobs: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS")?
                .map(|value| {
                    parse_positive_usize("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS", &value)
                })
                .transpose()?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct VWorldDatasetCollectionPlanFile {
    status: String,
    job_count: u64,
    jobs: Vec<VWorldDatasetCollectionPlanJob>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct VWorldDatasetCollectionPlanJob {
    endpoint_slug: String,
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    terms_url: Option<String>,
    operation: String,
    provider_module: String,
    svc_cde: String,
    ds_id: String,
    file_count: u64,
    large_file_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct VWorldDatasetFileInventoryReport {
    schema_version: String,
    generated_at_utc: String,
    status: String,
    plan_path: String,
    plan_job_count: u64,
    inventory_job_count: u64,
    expected_file_count: u64,
    discovered_file_count: u64,
    single_resource_file_count: u64,
    selection_archive_file_count: u64,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    blockers: Vec<String>,
    warnings: Vec<String>,
    count_drift: Vec<VWorldDatasetFileInventoryCountDrift>,
    jobs: Vec<VWorldDatasetFileInventoryJobReport>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct VWorldDatasetFileInventoryCountDrift {
    endpoint_slug: String,
    expected_file_count: u64,
    discovered_file_count: u64,
    expected_selection_archive_count: u64,
    selection_archive_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct VWorldDatasetFileInventoryJobReport {
    endpoint_slug: String,
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    terms_url: Option<String>,
    operation: String,
    provider_module: String,
    svc_cde: String,
    ds_id: String,
    expected_file_count: u64,
    discovered_file_count: u64,
    single_resource_file_count: u64,
    selection_archive_file_count: u64,
    files: Vec<VWorldDatasetFileInventoryItem>,
}

fn parse_collection_plan(plan_json: &str) -> Result<VWorldDatasetCollectionPlanFile> {
    serde_json::from_str(plan_json).context("failed to parse VWorld dataset collection plan")
}

fn select_jobs(
    jobs: &[VWorldDatasetCollectionPlanJob],
    max_jobs: Option<usize>,
) -> Result<&[VWorldDatasetCollectionPlanJob]> {
    if jobs.is_empty() {
        bail!("VWorld dataset collection plan contains no jobs");
    }
    let end = max_jobs.unwrap_or(jobs.len()).min(jobs.len());
    Ok(&jobs[..end])
}

fn write_report(path: &PathBuf, report: &VWorldDatasetFileInventoryReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create VWorld dataset file inventory report directory: {}",
                parent.display()
            )
        })?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?).with_context(|| {
        format!(
            "failed to write VWorld dataset file inventory report: {}",
            path.display()
        )
    })
}

fn parse_positive_u64(name: &str, value: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests;
