#[cfg(test)]
mod tests;

use anyhow::{bail, Context, Result};
use collection_application::{plan_vworld_raon_acquisition, ProviderBlockedVWorldFile};
use collection_domain::ProviderAcquisitionResource;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::public_data_control_support::optional_env_value;

/// v2 (root ADR-0170): each job carries what the RAON batch and the importer need — operation,
/// dataset name, the provider's period and update date, the listed size — so the plan is the
/// batch's selection as written. The plan is limited and priced against a byte budget.
pub(crate) const SCHEMA_VERSION: &str = "foundation-platform.provider_acquisition_plan.v2";
const DEFAULT_INPUT_PATH: &str =
    "target/audit/vworld-dataset-file-failed-reclassification-evidence-after-empty-raon-fix.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/provider-acquisition-plan.json";
/// A file the download path met and the provider refused to plain HTTP.
const BLOCKED_STATUS: &str = "provider_acquisition_blocked";
/// A RAON selection archive the daily sweep excluded and Bronze does not hold (root ADR-0170).
const DEFERRED_SELECTION_ARCHIVE_STATUS: &str = "deferred_selection_archive";
const READY_STATUS: &str = "ready";
const NEW_BYTES_BUDGET_EXCEEDED_STATUS: &str = "blocked_new_bytes_budget";
const NEW_BYTES_BUDGET_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_NEW_BYTES_BUDGET";
const MAX_FILES_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_MAX_FILES";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderBlockedFileRow {
    pub(crate) source_slug: String,
    pub(crate) download_ds_id: String,
    pub(crate) file_no: String,
    pub(crate) provider_file_name: String,
    pub(crate) operation: String,
    pub(crate) source_name: String,
    pub(crate) dataset_name: String,
    pub(crate) base_ym: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) listed_bytes: u64,
}

/// How much one run may take: the first `max_files` candidates, and refused whole when their
/// listed bytes exceed `new_bytes_budget` (root ADR-0170; the regular VWorld lane has had no such
/// budget since ADR-0172).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProviderAcquisitionPlanLimits {
    pub(crate) max_files: Option<usize>,
    pub(crate) new_bytes_budget: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderAcquisitionPlanReport {
    pub(crate) schema_version: &'static str,
    pub(crate) status: &'static str,
    /// Every blocked or deferred file the evidence lists, before the limit.
    pub(crate) candidate_count: usize,
    pub(crate) max_files: Option<usize>,
    pub(crate) new_bytes_budget: Option<u64>,
    /// The provider's listed size of the planned jobs, in bytes.
    pub(crate) listed_bytes_total: u64,
    pub(crate) job_count: usize,
    pub(crate) jobs: Vec<ProviderAcquisitionPlanJobReport>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderAcquisitionPlanJobReport {
    pub(crate) source_slug: String,
    pub(crate) provider: String,
    pub(crate) acquisition_method: String,
    pub(crate) operation: String,
    pub(crate) source_name: String,
    pub(crate) dataset_name: String,
    pub(crate) download_ds_id: String,
    pub(crate) file_no: String,
    pub(crate) provider_file_name: String,
    pub(crate) provider_file_id: String,
    pub(crate) source_identity_key: String,
    pub(crate) provider_resource_id: String,
    pub(crate) base_ym: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) listed_bytes: u64,
}

pub(crate) fn compile_provider_acquisition_plan_report(
    rows: &[ProviderBlockedFileRow],
    limits: ProviderAcquisitionPlanLimits,
) -> Result<ProviderAcquisitionPlanReport> {
    let candidate_count = rows.len();
    let rows = &rows[..limits.max_files.unwrap_or(rows.len()).min(rows.len())];
    let blocked = rows
        .iter()
        .map(|row| ProviderBlockedVWorldFile {
            source_slug: row.source_slug.clone(),
            download_ds_id: row.download_ds_id.clone(),
            file_no: row.file_no.clone(),
            provider_file_name: row.provider_file_name.clone(),
        })
        .collect::<Vec<_>>();

    let plan = plan_vworld_raon_acquisition(&blocked)?;
    let jobs = plan
        .jobs
        .iter()
        .zip(rows)
        .map(|(job, row)| {
            let ProviderAcquisitionResource::VWorldDatasetFile {
                download_ds_id,
                file_no,
            } = job.resource();
            let provider_file_id = provider_file_id(download_ds_id, file_no);
            ProviderAcquisitionPlanJobReport {
                source_slug: job.source_slug().to_owned(),
                provider: job.provider().to_owned(),
                acquisition_method: "raon_kupload_browser".to_owned(),
                operation: row.operation.clone(),
                source_name: row.source_name.clone(),
                dataset_name: row.dataset_name.clone(),
                download_ds_id: download_ds_id.to_owned(),
                file_no: file_no.to_owned(),
                provider_file_name: job.expected_file_name().to_owned(),
                provider_file_id: provider_file_id.clone(),
                source_identity_key: format!("provider_file_id={provider_file_id}"),
                provider_resource_id: provider_resource_id(job.resource()),
                base_ym: row.base_ym.clone(),
                updated_at: row.updated_at.clone(),
                listed_bytes: row.listed_bytes,
            }
        })
        .collect::<Vec<_>>();
    let listed_bytes_total = jobs
        .iter()
        .map(|job| job.listed_bytes)
        .fold(0_u64, u64::saturating_add);
    let status = match limits.new_bytes_budget {
        Some(budget) if listed_bytes_total > budget => NEW_BYTES_BUDGET_EXCEEDED_STATUS,
        _ => READY_STATUS,
    };

    Ok(ProviderAcquisitionPlanReport {
        schema_version: SCHEMA_VERSION,
        status,
        candidate_count,
        max_files: limits.max_files,
        new_bytes_budget: limits.new_bytes_budget,
        listed_bytes_total,
        job_count: jobs.len(),
        jobs,
    })
}

pub(crate) async fn run() -> Result<()> {
    let config = ProviderAcquisitionPlanConfig::from_env()?;
    let rows = read_blocked_file_rows(&config.input_path)?;
    let report = compile_provider_acquisition_plan_report(&rows, config.limits)?;
    write_report(&config.output_path, &report)?;
    if report.status != READY_STATUS {
        bail!(
            "provider acquisition plan refused before any download: {} files list {} bytes, above the new-bytes budget of {} (root ADR-0170) report={}",
            report.job_count,
            report.listed_bytes_total,
            report.new_bytes_budget.unwrap_or_default(),
            config.output_path.display()
        );
    }
    tracing::info!(
        input_path = %config.input_path.display(),
        output_path = %config.output_path.display(),
        job_count = report.job_count,
        listed_bytes_total = report.listed_bytes_total,
        "provider acquisition plan written"
    );
    Ok(())
}

fn provider_file_id(download_ds_id: &str, file_no: &str) -> String {
    format!("{download_ds_id}-{file_no}")
}

fn provider_resource_id(resource: &ProviderAcquisitionResource) -> String {
    match resource {
        ProviderAcquisitionResource::VWorldDatasetFile {
            download_ds_id,
            file_no,
        } => format!("vworld_dataset_file:{download_ds_id}:{file_no}"),
    }
}

struct ProviderAcquisitionPlanConfig {
    input_path: PathBuf,
    output_path: PathBuf,
    limits: ProviderAcquisitionPlanLimits,
}

impl ProviderAcquisitionPlanConfig {
    fn from_env() -> Result<Self> {
        let input_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BLOCKED_EVIDENCE_PATH")?
                .map_or_else(|| PathBuf::from(DEFAULT_INPUT_PATH), PathBuf::from);
        let output_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PLAN_OUTPUT_PATH")?
                .map_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH), PathBuf::from);
        let limits = ProviderAcquisitionPlanLimits {
            max_files: optional_env_value(MAX_FILES_ENV)?
                .map(|value| parse_max_files(&value))
                .transpose()?,
            new_bytes_budget: optional_env_value(NEW_BYTES_BUDGET_ENV)?
                .map(|value| parse_new_bytes_budget(&value))
                .transpose()?,
        };

        Ok(Self {
            input_path,
            output_path,
            limits,
        })
    }
}

/// A budget is a whole number of bytes; zero is allowed and means "download nothing".
pub(crate) fn parse_new_bytes_budget(raw: &str) -> Result<u64> {
    raw.trim()
        .parse::<u64>()
        .with_context(|| format!("{NEW_BYTES_BUDGET_ENV} must be a whole number of bytes"))
}

pub(crate) fn parse_max_files(raw: &str) -> Result<usize> {
    match raw.trim().parse::<usize>() {
        Ok(0) | Err(_) => bail!("{MAX_FILES_ENV} must be a positive whole number"),
        Ok(value) => Ok(value),
    }
}

#[derive(Debug, Deserialize)]
struct VWorldDatasetFileIngestEvidence {
    file_inventory_path: Option<String>,
    files: Vec<VWorldDatasetFileEvidenceRow>,
    #[serde(default)]
    selection_archives: Vec<VWorldDatasetFileEvidenceRow>,
}

#[derive(Debug, Deserialize)]
struct VWorldDatasetFileEvidenceRow {
    source_slug: String,
    download_ds_id: String,
    file_no: String,
    provider_file_name: String,
    status: String,
}

/// The listing the ingest read (`file_inventory_path` in its evidence): what each job is and what
/// the provider says of each file.
#[derive(Debug, Deserialize)]
struct VWorldDatasetFileInventory {
    jobs: Vec<VWorldDatasetFileInventoryJob>,
}

#[derive(Debug, Deserialize)]
struct VWorldDatasetFileInventoryJob {
    source_slug: String,
    source_name: String,
    dataset_name: String,
    operation: String,
    files: Vec<VWorldDatasetFileInventoryFile>,
}

#[derive(Debug, Deserialize)]
struct VWorldDatasetFileInventoryFile {
    download_ds_id: String,
    file_no: String,
    #[serde(default)]
    base_ym: String,
    #[serde(default)]
    updated_at: String,
    size_kib: u64,
}

pub(crate) fn read_blocked_file_rows(path: &Path) -> Result<Vec<ProviderBlockedFileRow>> {
    let evidence: VWorldDatasetFileIngestEvidence =
        read_json(path, "provider acquisition evidence")?;
    let candidates = evidence
        .files
        .into_iter()
        .filter(|file| file.status == BLOCKED_STATUS)
        .chain(
            evidence
                .selection_archives
                .into_iter()
                .filter(|file| file.status == DEFERRED_SELECTION_ARCHIVE_STATUS),
        )
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let inventory_path = evidence
        .file_inventory_path
        .map(PathBuf::from)
        .context("the evidence names no file_inventory_path to read the listed files from")?;
    let inventory: VWorldDatasetFileInventory =
        read_json(&inventory_path, "VWorld dataset file inventory")?;
    let listed = inventory
        .jobs
        .iter()
        .flat_map(|job| {
            job.files.iter().map(move |file| {
                (
                    (
                        job.source_slug.as_str(),
                        file.download_ds_id.as_str(),
                        file.file_no.as_str(),
                    ),
                    (job, file),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();

    candidates
        .into_iter()
        .map(|row| {
            let (job, file) = listed
                .get(&(
                    row.source_slug.as_str(),
                    row.download_ds_id.as_str(),
                    row.file_no.as_str(),
                ))
                .with_context(|| {
                    format!(
                        "{}:{}-{} is in the evidence but not in the inventory {}",
                        row.source_slug,
                        row.download_ds_id,
                        row.file_no,
                        inventory_path.display()
                    )
                })?;
            Ok(ProviderBlockedFileRow {
                operation: job.operation.clone(),
                source_name: job.source_name.clone(),
                dataset_name: job.dataset_name.clone(),
                base_ym: listed_value(&file.base_ym),
                updated_at: listed_value(&file.updated_at),
                listed_bytes: file.size_kib.saturating_mul(1024),
                source_slug: row.source_slug,
                download_ds_id: row.download_ds_id,
                file_no: row.file_no,
                provider_file_name: row.provider_file_name,
            })
        })
        .collect()
}

/// The listing writes `-` or nothing where it has no value.
fn listed_value(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value != "-").then(|| value.to_owned())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path, what: &str) -> Result<T> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {what} {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {what} {}", path.display()))
}

fn write_report(path: &PathBuf, report: &ProviderAcquisitionPlanReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create provider acquisition plan directory {}",
                parent.display()
            )
        })?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?).with_context(|| {
        format!(
            "failed to write provider acquisition plan {}",
            path.display()
        )
    })
}
