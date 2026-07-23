#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use collection_application::{plan_vworld_raon_acquisition, ProviderBlockedVWorldFile};
use collection_domain::ProviderAcquisitionResource;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

use crate::public_data_control_support::optional_env_value;

pub(crate) const SCHEMA_VERSION: &str = "foundation-platform.provider_acquisition_plan.v1";
const DEFAULT_INPUT_PATH: &str =
    "target/audit/vworld-dataset-file-failed-reclassification-evidence-after-empty-raon-fix.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/provider-acquisition-plan.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderBlockedFileRow {
    pub(crate) source_slug: String,
    pub(crate) download_ds_id: String,
    pub(crate) file_no: String,
    pub(crate) provider_file_name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderAcquisitionPlanReport {
    pub(crate) schema_version: &'static str,
    pub(crate) job_count: usize,
    pub(crate) jobs: Vec<ProviderAcquisitionPlanJobReport>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderAcquisitionPlanJobReport {
    pub(crate) source_slug: String,
    pub(crate) provider: String,
    pub(crate) acquisition_method: String,
    pub(crate) download_ds_id: String,
    pub(crate) file_no: String,
    pub(crate) provider_file_name: String,
    pub(crate) provider_file_id: String,
    pub(crate) source_identity_key: String,
    pub(crate) provider_resource_id: String,
}

pub(crate) fn compile_provider_acquisition_plan_report(
    rows: &[ProviderBlockedFileRow],
) -> Result<ProviderAcquisitionPlanReport> {
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
        .map(|job| {
            let ProviderAcquisitionResource::VWorldDatasetFile {
                download_ds_id,
                file_no,
            } = job.resource();
            let provider_file_id = provider_file_id(download_ds_id, file_no);
            ProviderAcquisitionPlanJobReport {
                source_slug: job.source_slug().to_owned(),
                provider: job.provider().to_owned(),
                acquisition_method: "raon_kupload_browser".to_owned(),
                download_ds_id: download_ds_id.to_owned(),
                file_no: file_no.to_owned(),
                provider_file_name: job.expected_file_name().to_owned(),
                provider_file_id: provider_file_id.clone(),
                source_identity_key: format!("provider_file_id={provider_file_id}"),
                provider_resource_id: provider_resource_id(job.resource()),
            }
        })
        .collect::<Vec<_>>();

    Ok(ProviderAcquisitionPlanReport {
        schema_version: SCHEMA_VERSION,
        job_count: jobs.len(),
        jobs,
    })
}

pub(crate) async fn run() -> Result<()> {
    let config = ProviderAcquisitionPlanConfig::from_env()?;
    let rows = read_blocked_file_rows(&config.input_path)?;
    let report = compile_provider_acquisition_plan_report(&rows)?;
    write_report(&config.output_path, &report)?;
    tracing::info!(
        input_path = %config.input_path.display(),
        output_path = %config.output_path.display(),
        job_count = report.job_count,
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
}

impl ProviderAcquisitionPlanConfig {
    fn from_env() -> Result<Self> {
        let input_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BLOCKED_EVIDENCE_PATH")?
                .map_or_else(|| PathBuf::from(DEFAULT_INPUT_PATH), PathBuf::from);
        let output_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PLAN_OUTPUT_PATH")?
                .map_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH), PathBuf::from);

        Ok(Self {
            input_path,
            output_path,
        })
    }
}

#[derive(Debug, Deserialize)]
struct VWorldDatasetFileIngestEvidence {
    files: Vec<VWorldDatasetFileEvidenceRow>,
}

#[derive(Debug, Deserialize)]
struct VWorldDatasetFileEvidenceRow {
    source_slug: String,
    download_ds_id: String,
    file_no: String,
    provider_file_name: String,
    status: String,
}

fn read_blocked_file_rows(path: &PathBuf) -> Result<Vec<ProviderBlockedFileRow>> {
    let bytes = fs::read(path).with_context(|| {
        format!(
            "failed to read provider acquisition evidence {}",
            path.display()
        )
    })?;
    let evidence: VWorldDatasetFileIngestEvidence =
        serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "failed to parse provider acquisition evidence {}",
                path.display()
            )
        })?;

    Ok(evidence
        .files
        .into_iter()
        .filter(|file| file.status == "provider_acquisition_blocked")
        .map(|file| ProviderBlockedFileRow {
            source_slug: file.source_slug,
            download_ds_id: file.download_ds_id,
            file_no: file.file_no,
            provider_file_name: file.provider_file_name,
        })
        .collect())
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
