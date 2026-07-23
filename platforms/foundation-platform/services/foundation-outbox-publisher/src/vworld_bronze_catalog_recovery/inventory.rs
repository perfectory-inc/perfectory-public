use std::{collections::BTreeMap, fs};

use anyhow::{bail, Context as _, Result};
use chrono::{SecondsFormat, Utc};
use collection_infrastructure::{
    VWorldDatasetFileClient, VWorldDatasetFileConfig, VWorldDatasetFileInventorySelector,
};
use serde::Serialize;

use super::{
    artifact, EndpointCatalogDocument, EndpointCatalogEntry, VWorldInventoryJob,
    DEFAULT_ENDPOINT_CATALOG_PATH, DEFAULT_PROVIDER_INVENTORY_PATH,
    ENDPOINT_CATALOG_SCHEMA_VERSION, VWORLD_RECOVERY_INVENTORY_SCHEMA_VERSION,
};
use crate::{
    bronze_catalog_recovery_manifest::RecoveryEvidenceArtifact,
    r2_command_support::{canonical_path, env_path, optional_env, write_json_file},
};

const DEFAULT_BASE_URI: &str = "https://www.vworld.kr";
const DEFAULT_TERMS_URL: &str = "https://www.vworld.kr/dtmk/dtmk_ntads_s001.do";
const DEFAULT_USER_AGENT: &str = "foundation-platform-vworld-bronze-catalog-recovery-inventory/1.0";
const DEFAULT_PAGE_SIZE: u64 = 100;

#[derive(Debug)]
struct Config {
    endpoint_catalog_path: std::path::PathBuf,
    output_path: std::path::PathBuf,
    requested_source_slugs: Vec<String>,
    base_uri: String,
    terms_url: String,
    user_agent: String,
    page_size: u64,
    cookie_header: Option<String>,
}

impl Config {
    fn from_env() -> Result<Self> {
        let raw_source_slugs =
            optional_env("FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_SOURCE_SLUGS")?
                .context(
                    "FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_SOURCE_SLUGS is required",
                )?;
        let page_size = optional_env("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE")?
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE must be an integer")?
            .unwrap_or(DEFAULT_PAGE_SIZE);
        if page_size == 0 {
            bail!("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE must be positive");
        }

        Ok(Self {
            endpoint_catalog_path: env_path(
                "FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_ENDPOINT_CATALOG_PATH",
                DEFAULT_ENDPOINT_CATALOG_PATH,
            )?,
            output_path: env_path(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH",
                DEFAULT_PROVIDER_INVENTORY_PATH,
            )?,
            requested_source_slugs: parse_requested_source_slugs(&raw_source_slugs)?,
            base_uri: optional_env("FOUNDATION_PLATFORM_VWORLD_DATASET_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            terms_url: optional_env("FOUNDATION_PLATFORM_VWORLD_DATASET_TERMS_URL")?
                .unwrap_or_else(|| DEFAULT_TERMS_URL.to_owned()),
            user_agent: optional_env("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            page_size,
            cookie_header: optional_env("FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER")?,
        })
    }
}

#[derive(Debug, Serialize)]
struct RecoveryInventoryDocument {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    endpoint_catalog: RecoveryEvidenceArtifact,
    requested_source_slugs: Vec<String>,
    blockers: Vec<String>,
    jobs: Vec<VWorldInventoryJob>,
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
    let catalog: EndpointCatalogDocument =
        serde_json::from_str(endpoint_catalog_json.trim_start_matches('\u{feff}'))
            .context("failed to parse endpoint catalog for VWorld recovery inventory")?;
    let endpoints = select_endpoints(catalog, &config.requested_source_slugs)?;
    let mut jobs = Vec::with_capacity(endpoints.len());
    let mut blockers = Vec::new();

    for endpoint in endpoints {
        let selector = endpoint
            .provider_dataset_selector
            .as_ref()
            .context("selected VWorld endpoint is missing provider_dataset_selector")?;
        let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
            base_uri: config.base_uri.clone(),
            user_agent: config.user_agent.clone(),
            page_size: config.page_size,
            cookie_header: config.cookie_header.clone(),
        })?;
        let files = client
            .fetch_dataset_file_inventory(&VWorldDatasetFileInventorySelector {
                svc_cde: selector.svc_cde.clone(),
                ds_id: selector.ds_id.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "failed to fetch VWorld recovery inventory for {}",
                    endpoint.bronze.source_slug
                )
            })?;
        if files.is_empty() {
            blockers.push(format!(
                "VWorld provider inventory returned no files for {}",
                endpoint.bronze.source_slug
            ));
        }
        jobs.push(VWorldInventoryJob {
            endpoint_slug: endpoint.endpoint_slug,
            source_slug: endpoint.bronze.source_slug,
            source_name: if endpoint.display_name_ko.trim().is_empty() {
                endpoint.dataset_slug.clone()
            } else {
                endpoint.display_name_ko
            },
            dataset_name: endpoint.dataset_slug,
            base_uri: config.base_uri.clone(),
            terms_url: Some(config.terms_url.clone()),
            operation: endpoint.operation,
            provider_module: "vworld_dataset_file".to_owned(),
            svc_cde: selector.svc_cde.clone(),
            ds_id: selector.ds_id.clone(),
            files,
        });
    }

    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let document = RecoveryInventoryDocument {
        schema_version: VWORLD_RECOVERY_INVENTORY_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        status,
        endpoint_catalog: artifact(
            &canonical_path(&config.endpoint_catalog_path),
            &endpoint_catalog_json,
        ),
        requested_source_slugs: config.requested_source_slugs,
        blockers,
        jobs,
    };
    write_json_file(&config.output_path, &document)?;
    if status == "blocked" {
        bail!(
            "VWorld recovery provider inventory is blocked; report={}",
            config.output_path.display()
        );
    }
    tracing::info!(
        source_count = document.jobs.len(),
        file_count = document.jobs.iter().map(|job| job.files.len()).sum::<usize>(),
        report = %config.output_path.display(),
        "VWorld Bronze Catalog recovery provider inventory written"
    );
    Ok(())
}

fn parse_requested_source_slugs(raw: &str) -> Result<Vec<String>> {
    let mut unique = std::collections::BTreeSet::new();
    for source_slug in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if !unique.insert(source_slug.to_owned()) {
            bail!("duplicate VWorld recovery source slug {source_slug}");
        }
    }
    if unique.is_empty() {
        bail!("at least one VWorld recovery source slug is required");
    }
    Ok(unique.into_iter().collect())
}

fn select_endpoints(
    catalog: EndpointCatalogDocument,
    requested_source_slugs: &[String],
) -> Result<Vec<EndpointCatalogEntry>> {
    if catalog.schema_version != ENDPOINT_CATALOG_SCHEMA_VERSION || catalog.status != "ready" {
        bail!("endpoint catalog must be the supported ready document");
    }
    let mut endpoints_by_source = BTreeMap::new();
    for endpoint in catalog.endpoints {
        if endpoint.provider != "VWorld"
            || endpoint.source_acquisition_lane != "provider_dataset_file"
        {
            continue;
        }
        if endpoint.dataset_slug.trim().is_empty()
            || endpoint.dataset_slug != endpoint.operation
            || endpoint.auth_kind != "provider_managed_credential"
            || endpoint.provider_dataset_selector.is_none()
        {
            bail!(
                "VWorld endpoint {} has an invalid recovery inventory contract",
                endpoint.endpoint_slug
            );
        }
        let source_slug = endpoint.bronze.source_slug.clone();
        if endpoints_by_source
            .insert(source_slug.clone(), endpoint)
            .is_some()
        {
            bail!("endpoint catalog contains duplicate VWorld source slug {source_slug}");
        }
    }

    requested_source_slugs
        .iter()
        .map(|source_slug| {
            endpoints_by_source
                .remove(source_slug)
                .with_context(|| format!("VWorld recovery source {source_slug} is not cataloged"))
        })
        .collect()
}

#[cfg(test)]
mod tests;
