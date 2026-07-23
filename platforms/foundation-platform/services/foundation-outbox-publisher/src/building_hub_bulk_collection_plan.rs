use std::{collections::BTreeSet, env, fs, path::PathBuf};

use anyhow::{bail, Context};
use chrono::{SecondsFormat, Utc};
use collection_application::{
    plan_building_hub_bulk_collection, BuildingHubBulkEndpoint, BuildingHubBulkInventoryFile,
    BuildingHubBulkInventorySelector,
};
use collection_infrastructure::{
    BuildingHubBulkClient, BuildingHubBulkConfig, BuildingHubBulkInventoryItem,
};
use serde::{Deserialize, Serialize};

const REPORT_SCHEMA_VERSION: &str = "foundation-platform.building_hub_bulk_collection_plan.v1";
const DEFAULT_ENDPOINT_CATALOG_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/building-hub-bulk-collection-plan.json";
const DEFAULT_BASE_URI: &str = "https://www.hub.go.kr";
const DEFAULT_TERMS_URL: &str = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do";
const DEFAULT_USER_AGENT: &str = "foundation-platform-building-hub-bulk-planner/1.0";
const BUILDING_HUB_BULK_GROUP: &str = "building_hub_bulk";

pub async fn run() -> anyhow::Result<()> {
    let config = BuildingHubBulkCollectionPlanConfig::from_env();
    let catalog_json = fs::read_to_string(&config.endpoint_catalog_path).with_context(|| {
        format!(
            "failed to read endpoint catalog: {}",
            config.endpoint_catalog_path.display()
        )
    })?;
    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent.clone(),
    })?;
    let inventory = client.fetch_inventory().await?;
    let report = compile_building_hub_bulk_collection_plan(
        &catalog_json,
        &inventory,
        &config.base_uri,
        config.terms_url.as_deref(),
    )?;

    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create building hub bulk plan output directory: {}",
                parent.display()
            )
        })?;
    }
    fs::write(&config.output_path, serde_json::to_vec_pretty(&report)?).with_context(|| {
        format!(
            "failed to write building hub bulk collection plan: {}",
            config.output_path.display()
        )
    })?;
    tracing::info!(
        jobs = report.job_count,
        inventory_files = report.inventory_file_count,
        report = %config.output_path.display(),
        "building hub bulk collection plan ready"
    );
    Ok(())
}

fn compile_building_hub_bulk_collection_plan(
    catalog_json: &str,
    inventory: &[BuildingHubBulkInventoryItem],
    base_uri: &str,
    terms_url: Option<&str>,
) -> anyhow::Result<BuildingHubBulkCollectionPlanReport> {
    let catalog =
        serde_json::from_str::<EndpointCatalog>(catalog_json.trim_start_matches('\u{feff}'))
            .context("failed to parse endpoint catalog")?;
    let endpoints = catalog
        .endpoints
        .into_iter()
        .filter(|endpoint| endpoint.group == BUILDING_HUB_BULK_GROUP)
        .map(|endpoint| endpoint.into_plan_endpoint(base_uri, terms_url))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if endpoints.is_empty() {
        bail!("endpoint catalog contains no building_hub_bulk endpoints");
    }
    let inventory_files = inventory
        .iter()
        .map(inventory_file_from_item)
        .collect::<Vec<_>>();
    let plan = plan_building_hub_bulk_collection(&endpoints, &inventory_files)?;
    let covered_inventory_keys = plan
        .jobs
        .iter()
        .map(|job| inventory_key(&job.task_group_code, &job.task_code, &job.provider_file_id))
        .collect::<BTreeSet<_>>();
    let cataloged_job_count = plan.jobs.len() as u64;
    let mut jobs = plan
        .jobs
        .into_iter()
        .map(cataloged_job_report)
        .collect::<Vec<_>>();
    // Fail closed (ADR 0014 §6/§7, owner-confirmed): a building_hub_bulk inventory item that no
    // cataloged endpoint covers has no canonical dataset_slug, so we refuse to emit the old opaque
    // `hub-go-kr-public-bulk-task-*` slug. Registered tasks never reach here — they go through
    // `cataloged_job_report` with the catalog's generator-derived `bronze.source_slug`.
    if let Some(item) = inventory.iter().find(|item| {
        !covered_inventory_keys.contains(&inventory_key(
            &item.task_group_code,
            &item.task_code,
            &item.file_id,
        ))
    }) {
        bail!(
            "hub bulk task {}-{} has no registered dataset_slug; register a canonical dataset_slug \
             in public-source-endpoint-catalog.v1.json before collecting it",
            item.task_group_code,
            item.task_code
        );
    }
    jobs.sort_by(|left, right| {
        left.source_slug
            .cmp(&right.source_slug)
            .then_with(|| left.provider_file_period.cmp(&right.provider_file_period))
            .then_with(|| left.provider_file_id.cmp(&right.provider_file_id))
    });
    let provider_inventory_only_job_count = jobs.len() as u64 - cataloged_job_count;

    Ok(BuildingHubBulkCollectionPlanReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        status: "ready".to_owned(),
        endpoint_count: endpoints.len() as u64,
        inventory_file_count: inventory.len() as u64,
        job_count: jobs.len() as u64,
        cataloged_job_count,
        provider_inventory_only_job_count,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        jobs,
    })
}

#[derive(Debug)]
struct BuildingHubBulkCollectionPlanConfig {
    endpoint_catalog_path: PathBuf,
    output_path: PathBuf,
    base_uri: String,
    terms_url: Option<String>,
    user_agent: String,
}

impl BuildingHubBulkCollectionPlanConfig {
    fn from_env() -> Self {
        Self {
            endpoint_catalog_path: env_path(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_ENDPOINT_CATALOG_PATH",
                DEFAULT_ENDPOINT_CATALOG_PATH,
            ),
            output_path: env_path(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH",
                DEFAULT_OUTPUT_PATH,
            ),
            base_uri: env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_BASE_URI",
                DEFAULT_BASE_URI,
            ),
            terms_url: Some(env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_TERMS_URL",
                DEFAULT_TERMS_URL,
            )),
            user_agent: env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_USER_AGENT",
                DEFAULT_USER_AGENT,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BuildingHubBulkCollectionPlanReport {
    pub schema_version: String,
    pub generated_at_utc: String,
    pub status: String,
    pub endpoint_count: u64,
    pub inventory_file_count: u64,
    pub job_count: u64,
    pub cataloged_job_count: u64,
    pub provider_inventory_only_job_count: u64,
    pub completion_claim_allowed: bool,
    pub production_cutover_allowed: bool,
    pub national_rollout_allowed: bool,
    pub jobs: Vec<BuildingHubBulkCollectionJobReport>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BuildingHubBulkCollectionJobReport {
    pub catalog_binding_status: String,
    pub endpoint_slug: String,
    pub source_slug: String,
    pub source_name: String,
    pub dataset_name: String,
    pub base_uri: String,
    pub terms_url: Option<String>,
    pub operation: String,
    pub provider_file_period: String,
    pub provider_file_id: String,
    pub category_name: String,
    pub service_name: String,
    pub service_period_label: String,
    pub task_group_code: String,
    pub task_code: String,
}

fn cataloged_job_report(
    job: collection_application::BuildingHubBulkCollectionJob,
) -> BuildingHubBulkCollectionJobReport {
    BuildingHubBulkCollectionJobReport {
        catalog_binding_status: "cataloged_endpoint".to_owned(),
        endpoint_slug: job.endpoint_slug,
        source_slug: job.source_slug,
        source_name: job.source_name,
        dataset_name: job.dataset_name,
        base_uri: job.base_uri,
        terms_url: job.terms_url,
        operation: job.operation,
        provider_file_period: job.provider_file_period,
        provider_file_id: job.provider_file_id,
        category_name: job.category_name,
        service_name: job.service_name,
        service_period_label: job.service_period_label,
        task_group_code: job.task_group_code,
        task_code: job.task_code,
    }
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
    provider_inventory_selector: Option<EndpointCatalogInventorySelector>,
    bronze: EndpointCatalogBronze,
}

impl EndpointCatalogEntry {
    fn into_plan_endpoint(
        self,
        base_uri: &str,
        terms_url: Option<&str>,
    ) -> anyhow::Result<BuildingHubBulkEndpoint> {
        let selector = self.provider_inventory_selector.with_context(|| {
            format!(
                "endpoint {} provider_inventory_selector is required",
                self.endpoint_slug
            )
        })?;
        Ok(BuildingHubBulkEndpoint {
            endpoint_slug: self.endpoint_slug,
            source_slug: self.bronze.source_slug,
            source_name: self.display_name_ko,
            dataset_name: self.operation.clone(),
            base_uri: base_uri.to_owned(),
            terms_url: terms_url.map(str::to_owned),
            operation: self.operation,
            source_acquisition_lane: self.source_acquisition_lane,
            national_collection_allowed: self.national_collection_allowed,
            selector: BuildingHubBulkInventorySelector {
                task_group_code: selector.task_group_code,
                task_code: selector.task_code,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
struct EndpointCatalogInventorySelector {
    task_group_code: String,
    task_code: String,
}

#[derive(Debug, Deserialize)]
struct EndpointCatalogBronze {
    source_slug: String,
}

fn inventory_file_from_item(item: &BuildingHubBulkInventoryItem) -> BuildingHubBulkInventoryFile {
    BuildingHubBulkInventoryFile {
        category_name: item.category_name.clone(),
        service_name: item.service_name.clone(),
        service_period_label: item.service_period_label.clone(),
        provider_file_period: item.provider_file_period.clone(),
        task_group_code: item.task_group_code.clone(),
        task_code: item.task_code.clone(),
        provider_file_id: item.file_id.clone(),
    }
}

fn inventory_key(task_group_code: &str, task_code: &str, provider_file_id: &str) -> String {
    format!("{task_group_code}:{task_code}:{provider_file_id}")
}

fn env_value(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn env_path(name: &str, default: &str) -> PathBuf {
    PathBuf::from(env_value(name, default))
}

#[cfg(test)]
mod tests;
