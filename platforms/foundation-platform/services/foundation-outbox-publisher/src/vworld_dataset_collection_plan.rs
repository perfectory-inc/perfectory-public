use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use collection_application::{
    plan_vworld_dataset_collection, VWorldDatasetCollectionEndpoint, VWorldDatasetInventoryDataset,
    VWorldDatasetInventorySelector,
};
use serde::{Deserialize, Serialize};

const REPORT_SCHEMA_VERSION: &str = "foundation-platform.vworld_dataset_collection_plan.v1";
const DEFAULT_ENDPOINT_CATALOG_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
const DEFAULT_INVENTORY_SUMMARY_PATH: &str = "target/vworld-core-live-inventory-summary.csv";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/vworld-dataset-collection-plan.json";
const DEFAULT_BASE_URI: &str = "https://www.vworld.kr";
const DEFAULT_TERMS_URL: &str = "https://www.vworld.kr/dtmk/dtmk_ntads_s001.do";
const VWORLD_DATASET_GROUP: &str = "vworld_dataset";

pub async fn run() -> Result<()> {
    let config = VWorldDatasetCollectionPlanConfig::from_env();
    let catalog_json = fs::read_to_string(&config.endpoint_catalog_path).with_context(|| {
        format!(
            "failed to read endpoint catalog: {}",
            config.endpoint_catalog_path.display()
        )
    })?;
    let inventory_csv = fs::read_to_string(&config.inventory_summary_path).with_context(|| {
        format!(
            "failed to read VWorld inventory summary: {}",
            config.inventory_summary_path.display()
        )
    })?;
    let report = compile_vworld_dataset_collection_plan(
        &catalog_json,
        &inventory_csv,
        &config.base_uri,
        config.terms_url.as_deref(),
    )?;

    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create VWorld dataset plan output directory: {}",
                parent.display()
            )
        })?;
    }
    fs::write(&config.output_path, serde_json::to_vec_pretty(&report)?).with_context(|| {
        format!(
            "failed to write VWorld dataset collection plan: {}",
            config.output_path.display()
        )
    })?;
    tracing::info!(
        status = report.status,
        jobs = report.job_count,
        blockers = report.blockers.len(),
        report = %config.output_path.display(),
        "VWorld dataset collection plan written"
    );
    Ok(())
}

fn compile_vworld_dataset_collection_plan(
    catalog_json: &str,
    inventory_csv: &str,
    base_uri: &str,
    terms_url: Option<&str>,
) -> Result<VWorldDatasetCollectionPlanReport> {
    let catalog =
        serde_json::from_str::<EndpointCatalog>(catalog_json.trim_start_matches('\u{feff}'))
            .context("failed to parse endpoint catalog")?;
    let inventory = parse_inventory_summary_csv(inventory_csv)?;
    let mut blockers = Vec::new();
    let mut endpoint_count = 0_u64;
    let mut jobs = Vec::new();

    for endpoint in catalog
        .endpoints
        .into_iter()
        .filter(|endpoint| endpoint.group == VWORLD_DATASET_GROUP)
    {
        endpoint_count += 1;
        let Some(selector) = endpoint.provider_dataset_selector.clone() else {
            blockers.push(format!(
                "endpoint {} provider_dataset_selector is required",
                endpoint.endpoint_slug
            ));
            continue;
        };
        let plan_endpoint = endpoint.into_plan_endpoint(selector, base_uri, terms_url);
        match plan_vworld_dataset_collection(&[plan_endpoint], &inventory) {
            Ok(plan) => jobs.extend(plan.jobs.into_iter().map(job_report)),
            Err(error) => blockers.push(error.to_string()),
        }
    }

    jobs.sort_by(|left, right| {
        left.source_slug
            .cmp(&right.source_slug)
            .then_with(|| left.svc_cde.cmp(&right.svc_cde))
            .then_with(|| left.ds_id.cmp(&right.ds_id))
    });
    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };

    Ok(VWorldDatasetCollectionPlanReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        status: status.to_owned(),
        endpoint_count,
        inventory_dataset_count: inventory.len() as u64,
        job_count: jobs.len() as u64,
        listed_gib_total: listed_gib_total(&jobs),
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        blockers,
        jobs,
    })
}

#[derive(Debug)]
struct VWorldDatasetCollectionPlanConfig {
    endpoint_catalog_path: PathBuf,
    inventory_summary_path: PathBuf,
    output_path: PathBuf,
    base_uri: String,
    terms_url: Option<String>,
}

impl VWorldDatasetCollectionPlanConfig {
    fn from_env() -> Self {
        Self {
            endpoint_catalog_path: env_path(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_ENDPOINT_CATALOG_PATH",
                DEFAULT_ENDPOINT_CATALOG_PATH,
            ),
            inventory_summary_path: env_path(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH",
                DEFAULT_INVENTORY_SUMMARY_PATH,
            ),
            output_path: env_path(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH",
                DEFAULT_OUTPUT_PATH,
            ),
            base_uri: env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_BASE_URI",
                DEFAULT_BASE_URI,
            ),
            terms_url: Some(env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_TERMS_URL",
                DEFAULT_TERMS_URL,
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct VWorldDatasetCollectionPlanReport {
    pub schema_version: String,
    pub generated_at_utc: String,
    pub status: String,
    pub endpoint_count: u64,
    pub inventory_dataset_count: u64,
    pub job_count: u64,
    pub listed_gib_total: String,
    pub completion_claim_allowed: bool,
    pub production_cutover_allowed: bool,
    pub national_rollout_allowed: bool,
    pub blockers: Vec<String>,
    pub jobs: Vec<VWorldDatasetCollectionJobReport>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct VWorldDatasetCollectionJobReport {
    pub endpoint_slug: String,
    pub source_slug: String,
    pub source_name: String,
    pub dataset_name: String,
    pub base_uri: String,
    pub terms_url: Option<String>,
    pub operation: String,
    pub provider_module: String,
    pub svc_cde: String,
    pub ds_id: String,
    pub file_pages: u64,
    pub file_count: u64,
    pub large_file_count: u64,
    pub listed_gib: String,
}

#[derive(Debug, Deserialize)]
struct EndpointCatalog {
    endpoints: Vec<EndpointCatalogEntry>,
}

#[derive(Debug, Deserialize)]
struct EndpointCatalogEntry {
    endpoint_slug: String,
    group: String,
    display_name_ko: String,
    operation: String,
    source_acquisition_lane: String,
    national_collection_allowed: bool,
    provider_dataset_selector: Option<EndpointCatalogDatasetSelector>,
    bronze: EndpointCatalogBronze,
}

impl EndpointCatalogEntry {
    fn into_plan_endpoint(
        self,
        selector: EndpointCatalogDatasetSelector,
        base_uri: &str,
        terms_url: Option<&str>,
    ) -> VWorldDatasetCollectionEndpoint {
        VWorldDatasetCollectionEndpoint {
            endpoint_slug: self.endpoint_slug,
            source_slug: self.bronze.source_slug,
            source_name: self.display_name_ko,
            dataset_name: self.operation.clone(),
            base_uri: base_uri.to_owned(),
            terms_url: terms_url.map(str::to_owned),
            operation: self.operation,
            source_acquisition_lane: self.source_acquisition_lane,
            national_collection_allowed: self.national_collection_allowed,
            selector: VWorldDatasetInventorySelector {
                svc_cde: selector.svc_cde,
                ds_id: selector.ds_id,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct EndpointCatalogDatasetSelector {
    svc_cde: String,
    ds_id: String,
}

#[derive(Debug, Deserialize)]
struct EndpointCatalogBronze {
    source_slug: String,
}

fn job_report(
    job: collection_application::VWorldDatasetCollectionJob,
) -> VWorldDatasetCollectionJobReport {
    VWorldDatasetCollectionJobReport {
        endpoint_slug: job.endpoint_slug,
        source_slug: job.source_slug,
        source_name: job.source_name,
        dataset_name: job.dataset_name,
        base_uri: job.base_uri,
        terms_url: job.terms_url,
        operation: job.operation,
        provider_module: job.provider_module,
        svc_cde: job.svc_cde,
        ds_id: job.ds_id,
        file_pages: job.file_pages,
        file_count: job.file_count,
        large_file_count: job.large_file_count,
        listed_gib: job.listed_gib,
    }
}

fn parse_inventory_summary_csv(csv: &str) -> Result<Vec<VWorldDatasetInventoryDataset>> {
    let mut lines = csv.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .context("VWorld inventory summary CSV header is required")?;
    let expected_header = [
        "module",
        "svc_cde",
        "ds_id",
        "file_pages",
        "file_count",
        "large_file_count",
        "listed_gib",
    ];
    if parse_csv_line(header) != expected_header {
        anyhow::bail!("VWorld inventory summary CSV header mismatch");
    }

    let mut inventory = Vec::new();
    for (index, line) in lines.enumerate() {
        let values = parse_csv_line(line);
        if values.len() != expected_header.len() {
            anyhow::bail!(
                "VWorld inventory summary CSV row {} expected {} columns, got {}",
                index + 2,
                expected_header.len(),
                values.len()
            );
        }
        inventory.push(VWorldDatasetInventoryDataset {
            module: values[0].clone(),
            svc_cde: values[1].clone(),
            ds_id: values[2].clone(),
            file_pages: parse_u64(&values[3], "file_pages", index + 2)?,
            file_count: parse_u64(&values[4], "file_count", index + 2)?,
            large_file_count: parse_u64(&values[5], "large_file_count", index + 2)?,
            listed_gib: values[6].clone(),
        });
    }
    Ok(inventory)
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                current.push('"');
                let _ = chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => values.push(std::mem::take(&mut current)),
            _ => current.push(ch),
        }
    }
    values.push(current);
    if let Some(first) = values.first_mut() {
        *first = first.trim_start_matches('\u{feff}').to_owned();
    }
    values
}

fn parse_u64(value: &str, field: &str, row: usize) -> Result<u64> {
    value.parse::<u64>().with_context(|| {
        format!("VWorld inventory summary CSV row {row} field {field} must be u64")
    })
}

fn listed_gib_total(jobs: &[VWorldDatasetCollectionJobReport]) -> String {
    let total = jobs
        .iter()
        .filter_map(|job| job.listed_gib.parse::<f64>().ok())
        .sum::<f64>();
    format!("{total:.2}")
}

fn env_path(name: &str, default_value: &str) -> PathBuf {
    env::var(name)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(default_value))
}

fn env_value(name: &str, default_value: &str) -> String {
    env::var(name).unwrap_or_else(|_| default_value.to_owned())
}

#[cfg(test)]
mod tests;
