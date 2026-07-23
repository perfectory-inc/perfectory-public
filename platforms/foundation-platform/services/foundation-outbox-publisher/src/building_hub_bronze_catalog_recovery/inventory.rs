use std::{collections::BTreeMap, fs, path::PathBuf};

use anyhow::{bail, Context as _, Result};
use chrono::{SecondsFormat, Utc};
use collection_infrastructure::{
    BuildingHubBulkClient, BuildingHubBulkConfig, BuildingHubBulkInventoryItem,
};

use super::{
    artifact, EndpointCatalogDocument, HubEndpoint, HubRecoveryInventoryBlocker,
    HubRecoveryInventoryDocument, HubRecoveryInventoryFile, HubRecoveryInventoryJob,
    ENDPOINT_CATALOG_SCHEMA_VERSION, HUB_RECOVERY_INVENTORY_SCHEMA_VERSION,
};
use crate::r2_command_support::{env_path, optional_env, write_json_file};

const DEFAULT_ENDPOINT_CATALOG_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
const DEFAULT_OUTPUT_PATH: &str =
    "target/audit/building-hub-bronze-catalog-recovery-inventory.json";
const DEFAULT_BASE_URI: &str = "https://www.hub.go.kr";
const DEFAULT_TERMS_URL: &str = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do";
const DEFAULT_USER_AGENT: &str =
    "foundation-platform-building-hub-bronze-catalog-recovery-inventory/1.0";

struct Config {
    endpoint_catalog_path: PathBuf,
    output_path: PathBuf,
    requested_source_slugs: Vec<String>,
    base_uri: String,
    terms_url: Option<String>,
    user_agent: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        let requested = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_SOURCE_SLUGS",
        )?
        .context(
            "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_SOURCE_SLUGS is required",
        )?;
        Ok(Self {
            endpoint_catalog_path: env_path(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_ENDPOINT_CATALOG_PATH",
                DEFAULT_ENDPOINT_CATALOG_PATH,
            )?,
            output_path: env_path(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_INVENTORY_PATH",
                DEFAULT_OUTPUT_PATH,
            )?,
            requested_source_slugs: parse_requested_source_slugs(&requested)?,
            base_uri: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_BASE_URI",
            )?
            .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            terms_url: Some(
                optional_env("FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_TERMS_URL")?
                    .unwrap_or_else(|| DEFAULT_TERMS_URL.to_owned()),
            ),
            user_agent: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_USER_AGENT",
            )?
            .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
        })
    }
}

pub(super) async fn run() -> Result<()> {
    let config = Config::from_env()?;
    let endpoint_catalog_json =
        fs::read_to_string(&config.endpoint_catalog_path).with_context(|| {
            format!(
                "failed to read endpoint catalog {}",
                config.endpoint_catalog_path.display()
            )
        })?;
    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent,
    })?;
    let provider_inventory = client.fetch_inventory().await?;
    let document = compile_recovery_inventory(
        &endpoint_catalog_json,
        &config.requested_source_slugs,
        &provider_inventory,
        &config.base_uri,
        config.terms_url.as_deref(),
        &Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    )?;
    write_json_file(&config.output_path, &document)?;
    if document.status == "blocked" {
        bail!(
            "Hub recovery provider inventory is blocked; blockers={}; report={}",
            document.blockers.len(),
            config.output_path.display()
        );
    }
    tracing::info!(
        source_count = document.jobs.len(),
        file_count = document.jobs.iter().map(|job| job.files.len()).sum::<usize>(),
        report = %config.output_path.display(),
        "Hub Bronze Catalog recovery provider inventory written"
    );
    Ok(())
}

fn compile_recovery_inventory(
    endpoint_catalog_json: &str,
    requested_source_slugs: &[String],
    provider_inventory: &[BuildingHubBulkInventoryItem],
    base_uri: &str,
    terms_url: Option<&str>,
    generated_at_utc: &str,
) -> Result<HubRecoveryInventoryDocument> {
    let catalog: EndpointCatalogDocument =
        serde_json::from_str(endpoint_catalog_json.trim_start_matches('\u{feff}'))
            .context("failed to parse endpoint catalog for Hub recovery inventory")?;
    if catalog.schema_version != ENDPOINT_CATALOG_SCHEMA_VERSION || catalog.status != "ready" {
        bail!("endpoint catalog must be the supported ready document");
    }
    let requested_sources = requested_source_slugs
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if requested_sources.is_empty() {
        bail!("at least one Hub recovery source slug is required");
    }
    if requested_sources.len() != requested_source_slugs.len() {
        bail!("duplicate Hub recovery source slug");
    }

    let mut endpoints_by_source = BTreeMap::new();
    for endpoint in catalog.endpoints {
        if endpoint.provider != "hub.go.kr" || endpoint.group != "building_hub_bulk" {
            continue;
        }
        validate_endpoint_contract(&endpoint)?;
        let source_slug = endpoint.bronze.source_slug.clone();
        if endpoints_by_source
            .insert(source_slug.clone(), endpoint)
            .is_some()
        {
            bail!("endpoint catalog contains duplicate Hub source slug {source_slug}");
        }
    }

    let mut jobs = Vec::with_capacity(requested_source_slugs.len());
    let mut blockers = Vec::new();
    for source_slug in requested_source_slugs {
        let endpoint = endpoints_by_source
            .remove(source_slug)
            .with_context(|| format!("Hub recovery source {source_slug} is not cataloged"))?;
        let selector = endpoint
            .provider_inventory_selector
            .as_ref()
            .context("validated Hub endpoint lost provider selector")?;
        let mut files = provider_inventory
            .iter()
            .filter(|item| {
                item.task_group_code == selector.task_group_code
                    && item.task_code == selector.task_code
            })
            .map(|item| HubRecoveryInventoryFile {
                category_name: item.category_name.clone(),
                service_name: item.service_name.clone(),
                service_period_label: item.service_period_label.clone(),
                provider_file_period: item.provider_file_period.clone(),
                provider_file_id: item.file_id.clone(),
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| {
            left.provider_file_period
                .cmp(&right.provider_file_period)
                .then_with(|| left.provider_file_id.cmp(&right.provider_file_id))
        });
        if files.is_empty() {
            blockers.push(HubRecoveryInventoryBlocker {
                endpoint_slug: endpoint.endpoint_slug.clone(),
                source_slug: source_slug.clone(),
                reason: "missing_provider_inventory_match".to_owned(),
            });
        }
        jobs.push(HubRecoveryInventoryJob {
            endpoint_slug: endpoint.endpoint_slug,
            source_slug: source_slug.clone(),
            source_name: if endpoint.display_name_ko.trim().is_empty() {
                endpoint.dataset_slug.clone()
            } else {
                endpoint.display_name_ko
            },
            dataset_name: endpoint.dataset_slug,
            base_uri: base_uri.to_owned(),
            terms_url: terms_url.map(str::to_owned),
            operation: endpoint.operation,
            provider_module: "building_hub_bulk".to_owned(),
            task_group_code: selector.task_group_code.clone(),
            task_code: selector.task_code.clone(),
            files,
        });
    }

    Ok(HubRecoveryInventoryDocument {
        schema_version: HUB_RECOVERY_INVENTORY_SCHEMA_VERSION.to_owned(),
        generated_at_utc: generated_at_utc.to_owned(),
        status: if blockers.is_empty() {
            "ready".to_owned()
        } else {
            "blocked".to_owned()
        },
        endpoint_catalog: artifact(
            "docs/catalog/public-source-endpoint-catalog.v1.json",
            endpoint_catalog_json,
        ),
        requested_source_slugs: requested_source_slugs.to_vec(),
        blockers,
        jobs,
    })
}

fn validate_endpoint_contract(endpoint: &HubEndpoint) -> Result<()> {
    if endpoint.source_acquisition_lane != "bulk_file"
        || !endpoint.national_collection_allowed
        || endpoint.dataset_slug.trim().is_empty()
        || endpoint.dataset_slug != endpoint.operation
        || endpoint.auth_kind != "provider_managed_credential"
        || endpoint.provider_inventory_selector.is_none()
    {
        bail!(
            "Hub endpoint {} has an invalid recovery inventory contract",
            endpoint.endpoint_slug
        );
    }
    Ok(())
}

fn parse_requested_source_slugs(raw: &str) -> Result<Vec<String>> {
    let requested = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let unique = requested
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if requested.is_empty() {
        bail!("at least one Hub recovery source slug is required");
    }
    if unique.len() != requested.len() {
        bail!("duplicate Hub recovery source slug");
    }
    Ok(requested)
}

#[cfg(test)]
mod tests;
