use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context as _, Result};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde_json::{Map as JsonMap, Value as JsonValue};

mod evidence;
mod inventory;

use evidence::{
    artifact, EndpointCatalogDocument, EndpointCatalogEntry, VWorldInventoryDocument,
    VWorldInventoryJob,
};

use crate::bronze_catalog_recovery_manifest::{
    BronzeCatalogRecoveryManifest, BronzeCatalogRecoveryManifestStatus, RecoverySourceSnapshot,
    BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION,
};
use crate::provider_file_bronze_catalog_recovery::{
    compile_provider_file_recovery, ProviderFileEvidence, ProviderFileR2AuditDocument,
};

pub(crate) async fn collect_inventory() -> Result<()> {
    inventory::run().await
}

use crate::public_data_control_support::optional_env_value;

const ENDPOINT_CATALOG_SCHEMA_VERSION: &str =
    "foundation-platform.public_source_endpoint_catalog.v1";
const VWORLD_INVENTORY_SCHEMA_VERSION: &str =
    "foundation-platform.vworld_dataset_file_inventory.v1";
const VWORLD_RECOVERY_INVENTORY_SCHEMA_VERSION: &str =
    "foundation-platform.vworld_bronze_catalog_recovery_inventory.v1";
const R2_AUDIT_SCHEMA_VERSION: &str = "foundation-platform.r2_inventory_audit.v1";
const DEFAULT_ENDPOINT_CATALOG_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
const DEFAULT_PROVIDER_INVENTORY_PATH: &str = "target/audit/vworld-dataset-file-inventory.json";
const DEFAULT_R2_AUDIT_PATH: &str = "target/r2-inventory-audit/r2-inventory-audit.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/vworld-bronze-catalog-recovery-manifest.json";
const DEFAULT_EXECUTABLE_MANIFEST_DIRECTORY: &str =
    "target/audit/vworld-bronze-catalog-recovery-executable-sources";

pub(crate) async fn run() -> Result<()> {
    let endpoint_catalog_path = env_path(
        "FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_ENDPOINT_CATALOG_PATH",
        DEFAULT_ENDPOINT_CATALOG_PATH,
    )?;
    let provider_inventory_path = env_path(
        "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH",
        DEFAULT_PROVIDER_INVENTORY_PATH,
    )?;
    let r2_inventory_path = env_path(
        "FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_R2_AUDIT_PATH",
        DEFAULT_R2_AUDIT_PATH,
    )?;
    let output_path = env_path(
        "FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_MANIFEST_PATH",
        DEFAULT_OUTPUT_PATH,
    )?;
    let executable_manifest_directory = env_path(
        "FOUNDATION_PLATFORM_VWORLD_BRONZE_CATALOG_RECOVERY_EXECUTABLE_MANIFEST_DIRECTORY",
        DEFAULT_EXECUTABLE_MANIFEST_DIRECTORY,
    )?;
    let endpoint_catalog_json = read_text(&endpoint_catalog_path)?;
    let provider_inventory_json = read_text(&provider_inventory_path)?;
    let r2_inventory_json = read_text(&r2_inventory_path)?;
    let manifest = compile_vworld_bronze_catalog_recovery_manifest(
        &endpoint_catalog_json,
        &normalized_path(&endpoint_catalog_path),
        &provider_inventory_json,
        &normalized_path(&provider_inventory_path),
        &r2_inventory_json,
        &normalized_path(&r2_inventory_path),
        Utc::now(),
    )?;
    write_manifest(&output_path, &manifest)?;
    let executable_manifest_paths =
        manifest.write_executable_source_projections(&executable_manifest_directory)?;
    tracing::info!(
        status = ?manifest.status,
        sources = manifest.sources.len(),
        candidates = manifest
            .sources
            .iter()
            .map(|source| source.candidates.len())
            .sum::<usize>(),
        unresolved = manifest.unresolved.len(),
        executable_source_manifests = executable_manifest_paths.len(),
        output = %output_path.display(),
        "VWorld Bronze Catalog recovery manifest compiled"
    );
    if manifest.status == BronzeCatalogRecoveryManifestStatus::Blocked {
        bail!(
            "VWorld Bronze Catalog recovery manifest is blocked by {} unresolved object(s): {}",
            manifest.unresolved.len(),
            output_path.display()
        );
    }
    Ok(())
}

fn env_path(name: &str, default: &str) -> Result<PathBuf> {
    Ok(optional_env_value(name)?
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default)))
}

fn read_text(path: &Path) -> Result<String> {
    fs::read_to_string(path)
        .with_context(|| format!("failed to read recovery evidence {}", path.display()))
}

fn write_manifest(path: &Path, manifest: &BronzeCatalogRecoveryManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create recovery manifest directory {}",
                parent.display()
            )
        })?;
    }
    let bytes = serde_json::to_vec_pretty(manifest)?;
    fs::write(path, bytes)
        .with_context(|| format!("failed to write recovery manifest {}", path.display()))
}

fn normalized_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[allow(clippy::too_many_arguments)]
fn compile_vworld_bronze_catalog_recovery_manifest(
    endpoint_catalog_json: &str,
    endpoint_catalog_uri: &str,
    provider_inventory_json: &str,
    provider_inventory_uri: &str,
    r2_inventory_json: &str,
    r2_inventory_uri: &str,
    generated_at: DateTime<Utc>,
) -> Result<BronzeCatalogRecoveryManifest> {
    let endpoint_catalog: EndpointCatalogDocument = serde_json::from_str(endpoint_catalog_json)
        .context("failed to parse public source endpoint catalog")?;
    let provider_inventory: VWorldInventoryDocument = serde_json::from_str(provider_inventory_json)
        .context("failed to parse VWorld dataset file inventory")?;
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
    let mut selected_source_slugs = Vec::with_capacity(provider_inventory.jobs.len());
    let mut provider_evidence = Vec::new();
    for job in &provider_inventory.jobs {
        let endpoint = endpoints.get(&job.endpoint_slug).with_context(|| {
            format!(
                "VWorld inventory endpoint {} is absent from endpoint catalog",
                job.endpoint_slug
            )
        })?;
        validate_job_against_endpoint(job, endpoint)?;
        selected_source_slugs.push(job.source_slug.clone());
        for file in &job.files {
            validate_inventory_file(job, file)?;
            provider_evidence.push(provider_evidence_from_inventory(job, endpoint, file)?);
        }
    }
    let compilation = compile_provider_file_recovery(
        &selected_source_slugs,
        provider_evidence,
        r2_inventory.objects,
        generated_at.date_naive(),
    )?;

    Ok(BronzeCatalogRecoveryManifest {
        schema_version: BRONZE_CATALOG_RECOVERY_MANIFEST_SCHEMA_VERSION.to_owned(),
        generated_at_utc: generated_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        status: if compilation.unresolved.is_empty() {
            BronzeCatalogRecoveryManifestStatus::Ready
        } else {
            BronzeCatalogRecoveryManifestStatus::Blocked
        },
        endpoint_catalog: artifact(endpoint_catalog_uri, endpoint_catalog_json),
        provider_inventory: artifact(provider_inventory_uri, provider_inventory_json),
        r2_inventory: artifact(r2_inventory_uri, r2_inventory_json),
        sources: compilation.sources,
        unresolved: compilation.unresolved,
    })
}

fn validate_document_headers(
    endpoint_catalog: &EndpointCatalogDocument,
    provider_inventory: &VWorldInventoryDocument,
    r2_inventory: &ProviderFileR2AuditDocument,
) -> Result<()> {
    if endpoint_catalog.schema_version != ENDPOINT_CATALOG_SCHEMA_VERSION {
        bail!("unsupported endpoint catalog schema version");
    }
    if endpoint_catalog.status != "ready" {
        bail!("endpoint catalog must be ready");
    }
    if provider_inventory.schema_version != VWORLD_INVENTORY_SCHEMA_VERSION
        && provider_inventory.schema_version != VWORLD_RECOVERY_INVENTORY_SCHEMA_VERSION
    {
        bail!("unsupported VWorld inventory schema version");
    }
    if provider_inventory.status != "ready" {
        bail!("VWorld inventory must be ready");
    }
    if r2_inventory.schema_version != R2_AUDIT_SCHEMA_VERSION {
        bail!("unsupported R2 audit schema version");
    }
    Ok(())
}

fn validate_job_against_endpoint(
    job: &VWorldInventoryJob,
    endpoint: &EndpointCatalogEntry,
) -> Result<()> {
    let selector = endpoint
        .provider_dataset_selector
        .as_ref()
        .context("VWorld endpoint is missing provider_dataset_selector")?;
    if endpoint.provider != "VWorld"
        || endpoint.source_acquisition_lane != "provider_dataset_file"
        || endpoint.bronze.source_slug != job.source_slug
        || endpoint.operation != job.operation
        || endpoint.dataset_slug != job.operation
        || endpoint.auth_kind != "provider_managed_credential"
        || selector.svc_cde != job.svc_cde
        || selector.ds_id != job.ds_id
    {
        bail!(
            "VWorld inventory job {} contradicts endpoint catalog",
            job.endpoint_slug
        );
    }
    if job.provider_module != "vworld_dataset_file" {
        bail!(
            "VWorld inventory job {} uses unsupported provider module {}",
            job.endpoint_slug,
            job.provider_module
        );
    }
    Ok(())
}

fn provider_evidence_from_inventory(
    job: &VWorldInventoryJob,
    endpoint: &EndpointCatalogEntry,
    file: &collection_infrastructure::VWorldDatasetFileInventoryItem,
) -> Result<ProviderFileEvidence> {
    validate_inventory_file(job, file)?;
    let provider_file_id = format!("{}-{}", file.download_ds_id, file.file_no);
    let (provider_file_period, provider_snapshot_date) = provider_base_temporal(&file.base_ym)?;
    let provider_updated_at = optional_provider_date("updated_at", &file.updated_at)?;
    let mut request_params_extra = JsonMap::new();
    request_params_extra.insert(
        "provider_file_format".to_owned(),
        JsonValue::String(file.file_format.clone()),
    );
    request_params_extra.insert(
        "provider_file_kind".to_owned(),
        JsonValue::String(file.provider_file_kind.clone()),
    );
    request_params_extra.insert(
        "sourceAcquisitionLane".to_owned(),
        JsonValue::String("provider_dataset_file".to_owned()),
    );
    request_params_extra.insert(
        "downloadKind".to_owned(),
        serde_json::to_value(&file.download_kind)?,
    );
    for (key, value) in [
        ("endpointSlug", job.endpoint_slug.as_str()),
        ("svcCde", job.svc_cde.as_str()),
        ("dsId", job.ds_id.as_str()),
        ("downloadDsId", file.download_ds_id.as_str()),
        ("fileNo", file.file_no.as_str()),
    ] {
        request_params_extra.insert(key.to_owned(), JsonValue::String(value.to_owned()));
    }

    Ok(ProviderFileEvidence {
        source: RecoverySourceSnapshot {
            endpoint_slug: job.endpoint_slug.clone(),
            slug: endpoint.bronze.source_slug.clone(),
            name: job.source_name.clone(),
            provider: endpoint.provider.clone(),
            dataset_name: job.dataset_name.clone(),
            base_url: Some(job.base_uri.clone()),
            auth_kind: "manual".to_owned(),
            payload_format: "unknown".to_owned(),
            terms_url: job.terms_url.clone(),
        },
        operation: job.operation.clone(),
        provider_file_period,
        provider_snapshot_date,
        provider_file_id,
        provider_file_name_label: file.provider_file_name.clone(),
        provider_updated_at,
        request_params_extra,
    })
}

fn validate_inventory_file(
    job: &VWorldInventoryJob,
    file: &collection_infrastructure::VWorldDatasetFileInventoryItem,
) -> Result<()> {
    if file.svc_cde != job.svc_cde || file.ds_id != job.ds_id {
        bail!(
            "VWorld inventory file {} selector contradicts job {}",
            file.file_no,
            job.endpoint_slug
        );
    }
    if file.download_ds_id.trim().is_empty() || file.file_no.trim().is_empty() {
        bail!("VWorld inventory file identity must not be empty");
    }
    Ok(())
}

fn provider_base_temporal(value: &str) -> Result<(Option<String>, Option<NaiveDate>)> {
    let value = value.trim();
    if value.is_empty() || value == "-" {
        return Ok((None, None));
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok((None, Some(date)));
    }
    if value.len() == 7
        && value.as_bytes().get(4) == Some(&b'-')
        && NaiveDate::parse_from_str(&format!("{value}-01"), "%Y-%m-%d").is_ok()
    {
        return Ok((Some(value.to_owned()), None));
    }
    bail!("VWorld provider base_ym must be '-', YYYY-MM, or YYYY-MM-DD, got {value:?}")
}

fn optional_provider_date(field: &str, value: &str) -> Result<Option<NaiveDate>> {
    let value = value.trim();
    if value.is_empty() || value == "-" {
        return Ok(None);
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(Some)
        .with_context(|| format!("VWorld provider {field} must be YYYY-MM-DD, got {value:?}"))
}

#[cfg(test)]
mod tests;
