use std::{
    collections::{BTreeSet, HashMap},
    fs,
};

use anyhow::{bail, Context as _, Result};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest as _, Sha256};

use crate::{
    bronze_catalog_recovery_manifest::{
        BronzeCatalogRecoveryManifest, BronzeCatalogRecoveryManifestStatus,
        RecoveryEvidenceArtifact, RecoverySourceSnapshot,
        BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION,
    },
    provider_file_bronze_catalog_recovery::{
        compile_provider_file_recovery, ProviderFileEvidence, ProviderFileR2AuditDocument,
    },
    r2_command_support::{canonical_path, env_path, write_json_file},
};

const ENDPOINT_CATALOG_SCHEMA_VERSION: &str =
    "foundation-platform.public_source_endpoint_catalog.v1";
const HUB_RECOVERY_INVENTORY_SCHEMA_VERSION: &str =
    "foundation-platform.building_hub_bronze_catalog_recovery_inventory.v1";
const R2_AUDIT_SCHEMA_VERSION: &str = "foundation-platform.r2_inventory_audit.v1";
const DEFAULT_ENDPOINT_CATALOG_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
const DEFAULT_PROVIDER_INVENTORY_PATH: &str =
    "target/audit/building-hub-bronze-catalog-recovery-inventory.json";
const DEFAULT_R2_AUDIT_PATH: &str = "target/r2-inventory-audit/r2-inventory-audit.json";
const DEFAULT_MANIFEST_PATH: &str =
    "target/audit/building-hub-bronze-catalog-recovery-manifest.json";
const DEFAULT_EXECUTABLE_MANIFEST_DIRECTORY: &str =
    "target/audit/building-hub-bronze-catalog-recovery-executable-sources";

mod inventory;

pub(crate) async fn collect_inventory() -> Result<()> {
    inventory::run().await
}

pub(crate) async fn run() -> Result<()> {
    let endpoint_catalog_path = env_path(
        "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_ENDPOINT_CATALOG_PATH",
        DEFAULT_ENDPOINT_CATALOG_PATH,
    )?;
    let provider_inventory_path = env_path(
        "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_INVENTORY_PATH",
        DEFAULT_PROVIDER_INVENTORY_PATH,
    )?;
    let r2_inventory_path = env_path(
        "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_R2_AUDIT_PATH",
        DEFAULT_R2_AUDIT_PATH,
    )?;
    let output_path = env_path(
        "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_MANIFEST_PATH",
        DEFAULT_MANIFEST_PATH,
    )?;
    let executable_manifest_directory = env_path(
        "FOUNDATION_PLATFORM_BUILDING_HUB_BRONZE_CATALOG_RECOVERY_EXECUTABLE_MANIFEST_DIRECTORY",
        DEFAULT_EXECUTABLE_MANIFEST_DIRECTORY,
    )?;
    let endpoint_catalog_json = read_text(&endpoint_catalog_path)?;
    let provider_inventory_json = read_text(&provider_inventory_path)?;
    let r2_inventory_json = read_text(&r2_inventory_path)?;
    let manifest = compile_building_hub_bronze_catalog_recovery_manifest(
        &endpoint_catalog_json,
        &canonical_path(&endpoint_catalog_path),
        &provider_inventory_json,
        &canonical_path(&provider_inventory_path),
        &r2_inventory_json,
        &canonical_path(&r2_inventory_path),
        Utc::now(),
    )?;
    write_json_file(&output_path, &manifest)?;
    let executable_manifest_paths =
        manifest.write_executable_source_projections(&executable_manifest_directory)?;
    tracing::info!(
        status = ?manifest.status,
        sources = manifest.sources.len(),
        candidates = manifest.sources.iter().map(|source| source.candidates.len()).sum::<usize>(),
        unresolved = manifest.unresolved.len(),
        executable_source_manifests = executable_manifest_paths.len(),
        output = %output_path.display(),
        "Hub Bronze Catalog recovery manifest compiled"
    );
    if manifest.status == BronzeCatalogRecoveryManifestStatus::Blocked {
        bail!(
            "Hub Bronze Catalog recovery manifest is blocked; unresolved={}; output={}",
            manifest.unresolved.len(),
            output_path.display()
        );
    }
    Ok(())
}

fn read_text(path: &std::path::Path) -> Result<String> {
    fs::read_to_string(path)
        .with_context(|| format!("failed to read recovery evidence {}", path.display()))
}

#[allow(clippy::too_many_arguments)]
fn compile_building_hub_bronze_catalog_recovery_manifest(
    endpoint_catalog_json: &str,
    endpoint_catalog_uri: &str,
    provider_inventory_json: &str,
    provider_inventory_uri: &str,
    r2_inventory_json: &str,
    r2_inventory_uri: &str,
    generated_at: DateTime<Utc>,
) -> Result<BronzeCatalogRecoveryManifest> {
    let endpoint_catalog: EndpointCatalogDocument =
        serde_json::from_str(endpoint_catalog_json.trim_start_matches('\u{feff}'))
            .context("failed to parse public source endpoint catalog")?;
    let provider_inventory: HubRecoveryInventoryDocument =
        serde_json::from_str(provider_inventory_json)
            .context("failed to parse Hub recovery provider inventory")?;
    let r2_inventory: ProviderFileR2AuditDocument =
        serde_json::from_str(r2_inventory_json).context("failed to parse R2 inventory audit")?;
    validate_document_headers(&endpoint_catalog, &provider_inventory, &r2_inventory)?;

    let mut endpoints = HashMap::with_capacity(endpoint_catalog.endpoints.len());
    for endpoint in endpoint_catalog.endpoints {
        let endpoint_slug = endpoint.endpoint_slug.clone();
        if endpoints.insert(endpoint_slug.clone(), endpoint).is_some() {
            bail!("endpoint catalog contains duplicate endpoint_slug {endpoint_slug}");
        }
    }

    let selected_sources = provider_inventory
        .requested_source_slugs
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if selected_sources.is_empty()
        || selected_sources.len() != provider_inventory.requested_source_slugs.len()
    {
        bail!("Hub recovery inventory must select unique non-empty source slugs");
    }
    validate_blockers(&provider_inventory, &selected_sources)?;

    let mut seen_job_sources = BTreeSet::new();
    let mut provider_evidence = Vec::new();
    for job in &provider_inventory.jobs {
        if !selected_sources.contains(job.source_slug.as_str()) {
            bail!(
                "Hub recovery job source {} is outside requested scope",
                job.source_slug
            );
        }
        if !seen_job_sources.insert(job.source_slug.as_str()) {
            bail!(
                "Hub recovery inventory contains duplicate source job {}",
                job.source_slug
            );
        }
        let endpoint = endpoints.get(&job.endpoint_slug).with_context(|| {
            format!(
                "Hub recovery endpoint {} is absent from endpoint catalog",
                job.endpoint_slug
            )
        })?;
        validate_job_against_endpoint(job, endpoint)?;
        for file in &job.files {
            provider_evidence.push(provider_evidence_from_inventory(job, endpoint, file)?);
        }
    }
    if seen_job_sources != selected_sources {
        bail!("Hub recovery inventory must contain exactly one job per requested source");
    }

    let compilation = compile_provider_file_recovery(
        &provider_inventory.requested_source_slugs,
        provider_evidence,
        r2_inventory.objects,
        generated_at.date_naive(),
    )?;
    let status = if provider_inventory.status == "ready" && compilation.unresolved.is_empty() {
        BronzeCatalogRecoveryManifestStatus::Ready
    } else {
        BronzeCatalogRecoveryManifestStatus::Blocked
    };

    Ok(BronzeCatalogRecoveryManifest {
        schema_version: BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION.to_owned(),
        generated_at_utc: generated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        status,
        endpoint_catalog: artifact(endpoint_catalog_uri, endpoint_catalog_json),
        provider_inventory: artifact(provider_inventory_uri, provider_inventory_json),
        r2_inventory: artifact(r2_inventory_uri, r2_inventory_json),
        sources: compilation.sources,
        unresolved: compilation.unresolved,
    })
}

fn validate_document_headers(
    endpoint_catalog: &EndpointCatalogDocument,
    provider_inventory: &HubRecoveryInventoryDocument,
    r2_inventory: &ProviderFileR2AuditDocument,
) -> Result<()> {
    if endpoint_catalog.schema_version != ENDPOINT_CATALOG_SCHEMA_VERSION
        || endpoint_catalog.status != "ready"
    {
        bail!("endpoint catalog must be the supported ready document");
    }
    if provider_inventory.schema_version != HUB_RECOVERY_INVENTORY_SCHEMA_VERSION
        || !matches!(provider_inventory.status.as_str(), "ready" | "blocked")
    {
        bail!("unsupported Hub recovery provider inventory document");
    }
    if r2_inventory.schema_version != R2_AUDIT_SCHEMA_VERSION {
        bail!("unsupported R2 audit schema version");
    }
    DateTime::parse_from_rfc3339(&provider_inventory.generated_at_utc)
        .context("invalid Hub recovery inventory generated_at_utc")?;
    if provider_inventory.endpoint_catalog.uri.trim().is_empty()
        || !is_sha256(&provider_inventory.endpoint_catalog.sha256)
    {
        bail!("Hub recovery inventory endpoint catalog evidence is invalid");
    }
    Ok(())
}

fn validate_blockers<'a>(
    inventory: &'a HubRecoveryInventoryDocument,
    selected_sources: &BTreeSet<&'a str>,
) -> Result<()> {
    if (inventory.status == "ready") != inventory.blockers.is_empty() {
        bail!("Hub recovery inventory status and blockers contradict each other");
    }
    for blocker in &inventory.blockers {
        if blocker.endpoint_slug.trim().is_empty()
            || blocker.reason.trim().is_empty()
            || !selected_sources.contains(blocker.source_slug.as_str())
        {
            bail!("Hub recovery inventory contains an invalid blocker");
        }
    }
    Ok(())
}

fn validate_job_against_endpoint(
    job: &HubRecoveryInventoryJob,
    endpoint: &HubEndpoint,
) -> Result<()> {
    let selector = endpoint
        .provider_inventory_selector
        .as_ref()
        .context("Hub endpoint is missing provider_inventory_selector")?;
    if endpoint.provider != "hub.go.kr"
        || endpoint.group != "building_hub_bulk"
        || endpoint.source_acquisition_lane != "bulk_file"
        || !endpoint.national_collection_allowed
        || endpoint.dataset_slug != endpoint.operation
        || endpoint.auth_kind != "provider_managed_credential"
        || endpoint.bronze.source_slug != job.source_slug
        || endpoint.operation != job.operation
        || selector.task_group_code != job.task_group_code
        || selector.task_code != job.task_code
        || job.provider_module != "building_hub_bulk"
    {
        bail!(
            "Hub recovery job {} contradicts endpoint catalog",
            job.endpoint_slug
        );
    }
    Ok(())
}

fn provider_evidence_from_inventory(
    job: &HubRecoveryInventoryJob,
    endpoint: &HubEndpoint,
    file: &HubRecoveryInventoryFile,
) -> Result<ProviderFileEvidence> {
    if file.provider_file_id.trim().is_empty() {
        bail!("Hub recovery provider_file_id must not be empty");
    }
    let provider_period_date =
        NaiveDate::parse_from_str(&format!("{}-01", file.provider_file_period), "%Y-%m-%d")
            .context("Hub recovery provider_file_period must be YYYY-MM")?;
    if provider_period_date.format("%Y-%m").to_string() != file.provider_file_period {
        bail!("Hub recovery provider_file_period must be canonical YYYY-MM");
    }
    let mut request_params_extra = JsonMap::new();
    for (key, value) in [
        ("sourceAcquisitionLane", "bulk_file"),
        ("endpointSlug", job.endpoint_slug.as_str()),
        ("taskGroupCode", job.task_group_code.as_str()),
        ("taskCode", job.task_code.as_str()),
        ("categoryName", file.category_name.as_str()),
        ("serviceName", file.service_name.as_str()),
        ("servicePeriodLabel", file.service_period_label.as_str()),
    ] {
        request_params_extra.insert(key.to_owned(), JsonValue::String(value.to_owned()));
    }

    Ok(ProviderFileEvidence {
        source: RecoverySourceSnapshot {
            endpoint_slug: job.endpoint_slug.clone(),
            slug: job.source_slug.clone(),
            name: job.source_name.clone(),
            provider: endpoint.provider.clone(),
            dataset_name: job.dataset_name.clone(),
            base_url: Some(job.base_uri.clone()),
            auth_kind: "manual".to_owned(),
            payload_format: "unknown".to_owned(),
            terms_url: job.terms_url.clone(),
        },
        operation: job.operation.clone(),
        provider_file_period: Some(file.provider_file_period.clone()),
        provider_snapshot_date: None,
        provider_file_id: file.provider_file_id.clone(),
        provider_file_name_label: file.service_name.clone(),
        provider_updated_at: None,
        request_params_extra,
    })
}

fn artifact(uri: &str, content: &str) -> RecoveryEvidenceArtifact {
    RecoveryEvidenceArtifact {
        uri: uri.to_owned(),
        sha256: sha256_hex(content.as_bytes()),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _ = write!(&mut output, "{byte:02x}");
            output
        })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Deserialize)]
struct EndpointCatalogDocument {
    schema_version: String,
    status: String,
    endpoints: Vec<HubEndpoint>,
}

#[derive(Debug, Deserialize)]
struct HubEndpoint {
    endpoint_slug: String,
    provider: String,
    group: String,
    #[serde(default)]
    display_name_ko: String,
    #[serde(default)]
    dataset_slug: String,
    operation: String,
    source_acquisition_lane: String,
    national_collection_allowed: bool,
    provider_inventory_selector: Option<HubInventorySelector>,
    auth_kind: String,
    bronze: HubBronzeContract,
}

#[derive(Debug, Deserialize)]
struct HubInventorySelector {
    task_group_code: String,
    task_code: String,
}

#[derive(Debug, Deserialize)]
struct HubBronzeContract {
    source_slug: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct HubRecoveryInventoryDocument {
    schema_version: String,
    generated_at_utc: String,
    status: String,
    endpoint_catalog: RecoveryEvidenceArtifact,
    requested_source_slugs: Vec<String>,
    blockers: Vec<HubRecoveryInventoryBlocker>,
    jobs: Vec<HubRecoveryInventoryJob>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HubRecoveryInventoryBlocker {
    endpoint_slug: String,
    source_slug: String,
    reason: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct HubRecoveryInventoryJob {
    endpoint_slug: String,
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    terms_url: Option<String>,
    operation: String,
    provider_module: String,
    task_group_code: String,
    task_code: String,
    files: Vec<HubRecoveryInventoryFile>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HubRecoveryInventoryFile {
    category_name: String,
    service_name: String,
    service_period_label: String,
    provider_file_period: String,
    provider_file_id: String,
}

#[cfg(test)]
mod tests;
