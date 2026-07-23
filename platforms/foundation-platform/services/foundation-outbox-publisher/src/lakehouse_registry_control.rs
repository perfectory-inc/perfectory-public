//! Lakehouse Registry seed and verification commands.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use foundation_shared_kernel::ids::LakehouseStorageNamespaceId;
use lakehouse_application::ports::{LakehouseRegistryRepository, LakehouseRegistryUnitOfWork};
use lakehouse_application::RegisterLakehouseObjectArtifactCommand;
use lakehouse_domain::{
    LakehouseArtifactFormat, LakehouseAssetKind, LakehouseCatalogProvider, LakehouseEnvironment,
    LakehouseNamespaceStatus, LakehouseOwnerService, LakehouseRegistryLayer,
    LakehouseStorageNamespace, LakehouseStorageProvider,
};
use lakehouse_infrastructure::{PgLakehouseRegistryRepository, PgLakehouseRegistryUnitOfWork};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::public_data_control_support::{env_path, optional_env_value, write_json_file};

/// Seeds the required service-owned production lakehouse namespaces.
pub async fn seed() -> anyhow::Result<()> {
    let pool = connect_pool().await?;
    let repository = PgLakehouseRegistryRepository::new(pool);
    let mut namespaces = Vec::new();

    for seed in namespace_seeds()? {
        let namespace = LakehouseStorageNamespace::new(
            seed.id,
            LakehouseStorageProvider::R2,
            LakehouseEnvironment::Production,
            seed.owner_service,
            seed.owner_service.production_r2_bucket_name().to_owned(),
            None,
            LakehouseCatalogProvider::R2DataCatalog,
            LakehouseNamespaceStatus::Active,
        )?;
        let stored = repository.upsert_storage_namespace(&namespace).await?;
        namespaces.push(namespace_report(&stored));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&RegistrySeedReport {
            schema_version: "foundation-platform.lakehouse_registry_seed.v1",
            status: "seeded",
            namespaces,
        })?
    );
    Ok(())
}

/// Verifies the required service-owned production lakehouse namespaces.
pub async fn verify() -> anyhow::Result<()> {
    // The fmt/guardrail CI job runs without a database. An absent DATABASE_URL means this
    // environment cannot inspect the registry at all, so report skipped instead of failing,
    // matching the other environment-dependent checkers (e.g. administrative spatial scope).
    if optional_env_value("DATABASE_URL")?.is_none() {
        let report = RegistryVerifyReport {
            schema_version: "foundation-platform.lakehouse_registry_verify.v1",
            status: "skipped",
            namespaces: Vec::new(),
            blockers: vec!["DATABASE_URL is not configured in this environment".to_owned()],
        };
        write_registry_verify_report(&report)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let pool = connect_pool().await?;
    let repository = PgLakehouseRegistryRepository::new(pool);
    let mut namespaces = Vec::new();
    let mut blockers = Vec::new();

    for seed in namespace_seeds()? {
        match repository
            .find_storage_namespace(seed.owner_service, LakehouseEnvironment::Production)
            .await?
        {
            Some(namespace) => {
                let expected_bucket = seed.owner_service.production_r2_bucket_name();
                if namespace.bucket_name != expected_bucket {
                    blockers.push(format!(
                        "{} production bucket mismatch: expected {}, got {}",
                        seed.owner_service.wire_name(),
                        expected_bucket,
                        namespace.bucket_name
                    ));
                }
                if namespace.status != LakehouseNamespaceStatus::Active {
                    blockers.push(format!(
                        "{} production namespace is not active",
                        seed.owner_service.wire_name()
                    ));
                }
                namespaces.push(namespace_report(&namespace));
            }
            None => blockers.push(format!(
                "{} production namespace is missing",
                seed.owner_service.wire_name()
            )),
        }
    }

    verify_foundation_platform_r2_bucket_env(&mut blockers);
    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let report = RegistryVerifyReport {
        schema_version: "foundation-platform.lakehouse_registry_verify.v1",
        status,
        namespaces,
        blockers,
    };
    write_registry_verify_report(&report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if status != "ready" {
        bail!("lakehouse registry verification blocked");
    }
    Ok(())
}

/// Records Bronze object artifacts from a national data collection evidence file.
pub async fn record_bronze_run_evidence() -> anyhow::Result<()> {
    let evidence_path = bronze_run_evidence_path()?;
    let evidence = read_bronze_run_evidence(&evidence_path)?;
    evidence.validate()?;

    let pool = connect_pool().await?;
    let repository = PgLakehouseRegistryRepository::new(pool.clone());
    let unit_of_work = PgLakehouseRegistryUnitOfWork::new(pool);
    let namespace = repository
        .find_storage_namespace(
            LakehouseOwnerService::FoundationPlatform,
            LakehouseEnvironment::Production,
        )
        .await?
        .context("foundation-platform production lakehouse namespace is missing")?;
    if namespace.status != LakehouseNamespaceStatus::Active {
        bail!("foundation-platform production lakehouse namespace is not active");
    }

    let mut assets = Vec::new();
    let mut artifact_count = 0_u64;
    for (provider_key, provider) in &evidence.providers {
        let asset_token = source_slug_to_asset_token(&provider.source_slug)?;
        let qualified_name = format!("foundation_platform.bronze.{asset_token}");
        let mut provider_artifact_count = 0_u64;
        for object in &provider.bronze.objects {
            let command = bronze_artifact_command(
                &qualified_name,
                &evidence.schema_version,
                &provider.ingestion_run_id,
                object,
            )
            .with_context(|| format!("invalid Bronze Registry command for {provider_key}"))?;
            unit_of_work.register_object_artifact(command).await?;
            provider_artifact_count += 1;
            artifact_count += 1;
        }

        assets.push(BronzeEvidenceRegistryAssetReport {
            provider_key: provider_key.clone(),
            source_slug: provider.source_slug.clone(),
            qualified_name,
            version: provider.ingestion_run_id.clone(),
            object_count: provider_artifact_count,
        });
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&BronzeEvidenceRegistryRecordReport {
            schema_version: "foundation-platform.lakehouse_bronze_evidence_registry_record.v1",
            status: "ready",
            evidence_path: evidence_path.to_string_lossy().to_string(),
            provider_count: evidence.providers.len(),
            artifact_count,
            assets,
        })?
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct NamespaceSeed {
    id: LakehouseStorageNamespaceId,
    owner_service: LakehouseOwnerService,
}

#[derive(Serialize)]
struct RegistrySeedReport {
    schema_version: &'static str,
    status: &'static str,
    namespaces: Vec<NamespaceReport>,
}

#[derive(Serialize)]
struct RegistryVerifyReport {
    schema_version: &'static str,
    status: &'static str,
    namespaces: Vec<NamespaceReport>,
    blockers: Vec<String>,
}

#[derive(Deserialize)]
struct NationalDataCollectionRunEvidence {
    schema_version: String,
    status: String,
    raw_response_preserved: bool,
    providers: BTreeMap<String, ProviderEvidence>,
}

impl NationalDataCollectionRunEvidence {
    fn validate(&self) -> anyhow::Result<()> {
        if self.schema_version != "foundation-platform.national_data_collection_run_evidence.v1" {
            bail!("national data collection run evidence schema mismatch");
        }
        if self.status != "ready" {
            bail!("national data collection run evidence status must be ready");
        }
        if !self.raw_response_preserved {
            bail!("national data collection run evidence must preserve raw responses");
        }
        if self.providers.is_empty() {
            bail!("national data collection run evidence must include providers");
        }
        for (provider_key, provider) in &self.providers {
            if provider.source_slug.trim().is_empty() {
                bail!("{provider_key}.source_slug is required");
            }
            if provider.ingestion_run_id.trim().is_empty() {
                bail!("{provider_key}.ingestion_run_id is required");
            }
            if provider.bronze.storage_driver.trim().is_empty() {
                bail!("{provider_key}.bronze.storage_driver is required");
            }
            if provider.bronze.objects.is_empty() {
                bail!("{provider_key}.bronze.objects must not be empty");
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct ProviderEvidence {
    source_slug: String,
    ingestion_run_id: String,
    bronze: BronzeEvidence,
}

#[derive(Deserialize)]
struct BronzeEvidence {
    storage_driver: String,
    objects: Vec<BronzeObjectEvidence>,
}

#[derive(Deserialize)]
struct BronzeObjectEvidence {
    object_key: String,
    checksum_sha256: String,
    size_bytes: u64,
    logical_record_count: u64,
}

#[derive(Serialize)]
struct BronzeEvidenceRegistryRecordReport {
    schema_version: &'static str,
    status: &'static str,
    evidence_path: String,
    provider_count: usize,
    artifact_count: u64,
    assets: Vec<BronzeEvidenceRegistryAssetReport>,
}

#[derive(Serialize)]
struct BronzeEvidenceRegistryAssetReport {
    provider_key: String,
    source_slug: String,
    qualified_name: String,
    version: String,
    object_count: u64,
}

#[derive(Serialize)]
struct NamespaceReport {
    owner_service: &'static str,
    environment: &'static str,
    provider: &'static str,
    bucket_name: String,
    catalog_provider: &'static str,
    status: &'static str,
}

async fn connect_pool() -> anyhow::Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")
}

fn bronze_run_evidence_path() -> anyhow::Result<PathBuf> {
    Ok(
        std::env::var("FOUNDATION_PLATFORM_LAKEHOUSE_BRONZE_RUN_EVIDENCE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from("target/audit/national-data-collection-run-evidence.json")
            }),
    )
}

fn write_registry_verify_report(report: &RegistryVerifyReport) -> anyhow::Result<()> {
    let Some(raw_path) =
        optional_env_value("FOUNDATION_PLATFORM_LAKEHOUSE_REGISTRY_VERIFY_OUTPUT_PATH")?
    else {
        return Ok(());
    };
    let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
    let root = fs::canonicalize(&root)
        .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
    let output_path = resolve_output_path(&root, Path::new(&raw_path), "registry verify output")?;
    write_json_file(&output_path, report)
}

fn resolve_output_path(root: &Path, path: &Path, label: &str) -> anyhow::Result<PathBuf> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("{label} must not contain parent directory segments");
    }
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let parent = resolved
        .parent()
        .with_context(|| format!("{label} must have a parent directory"))?;
    let file_name = resolved
        .file_name()
        .with_context(|| format!("{label} must have a file name"))?;
    let canonical_parent = if parent.exists() {
        fs::canonicalize(parent)
            .with_context(|| format!("failed to resolve {label} parent {}", parent.display()))?
    } else {
        parent.to_path_buf()
    };
    if parent.exists() && !canonical_parent.starts_with(root) {
        bail!("{label} must stay within repo root");
    }
    Ok(canonical_parent.join(file_name))
}

fn read_bronze_run_evidence(path: &PathBuf) -> anyhow::Result<NationalDataCollectionRunEvidence> {
    let payload = fs::read(path)
        .with_context(|| format!("failed to read Bronze run evidence {}", path.display()))?;
    serde_json::from_slice(strip_utf8_bom(&payload))
        .with_context(|| format!("failed to parse Bronze run evidence {}", path.display()))
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes)
}

fn namespace_seeds() -> anyhow::Result<[NamespaceSeed; 3]> {
    Ok([
        NamespaceSeed {
            id: LakehouseStorageNamespaceId::new(Uuid::parse_str(
                "018f0000-0000-7000-8000-000000000901",
            )?),
            owner_service: LakehouseOwnerService::FoundationPlatform,
        },
        NamespaceSeed {
            id: LakehouseStorageNamespaceId::new(Uuid::parse_str(
                "018f0000-0000-7000-8000-000000000902",
            )?),
            owner_service: LakehouseOwnerService::Gongzzang,
        },
        NamespaceSeed {
            id: LakehouseStorageNamespaceId::new(Uuid::parse_str(
                "018f0000-0000-7000-8000-000000000903",
            )?),
            owner_service: LakehouseOwnerService::Dawneer,
        },
    ])
}

fn source_slug_to_asset_token(source_slug: &str) -> anyhow::Result<String> {
    if source_slug.is_empty()
        || source_slug.starts_with('-')
        || source_slug.ends_with('-')
        || source_slug.starts_with('_')
        || source_slug.ends_with('_')
        || source_slug.contains("--")
        || !source_slug.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
    {
        bail!("source_slug must be lowercase letters, digits, '-', or '_'; must not start or end with '-' or '_'; must not contain '--'");
    }
    Ok(source_slug.replace('-', "_"))
}

fn content_type_for_object_key(object_key: &str) -> &'static str {
    if object_key.ends_with(".jsonl") {
        "application/x-ndjson"
    } else if object_key.ends_with(".json") {
        "application/json"
    } else {
        "application/octet-stream"
    }
}

fn bronze_artifact_command(
    qualified_name: &str,
    schema_contract_ref: &str,
    ingestion_run_id: &str,
    object: &BronzeObjectEvidence,
) -> anyhow::Result<RegisterLakehouseObjectArtifactCommand> {
    Uuid::parse_str(ingestion_run_id).context("ingestion_run_id must be a UUID")?;
    Ok(RegisterLakehouseObjectArtifactCommand {
        qualified_name: qualified_name.to_owned(),
        owner_service: LakehouseOwnerService::FoundationPlatform,
        environment: LakehouseEnvironment::Production,
        layer: LakehouseRegistryLayer::Bronze,
        asset_kind: LakehouseAssetKind::RawObjectSet,
        schema_contract_ref: schema_contract_ref.to_owned(),
        dataset_version: ingestion_run_id.to_owned(),
        schema_version: "foundation-platform.bronze.raw-object-set.v1".to_owned(),
        artifact_format: LakehouseArtifactFormat::Json,
        created_by_ingestion_run_id: None,
        object_key: object.object_key.clone(),
        content_type: content_type_for_object_key(&object.object_key).to_owned(),
        checksum_sha256: object.checksum_sha256.clone(),
        size_bytes: object.size_bytes,
        logical_record_count: Some(object.logical_record_count),
    })
}

fn namespace_report(namespace: &LakehouseStorageNamespace) -> NamespaceReport {
    NamespaceReport {
        owner_service: namespace.owner_service.wire_name(),
        environment: namespace.environment.wire_name(),
        provider: namespace.provider.wire_name(),
        bucket_name: namespace.bucket_name.clone(),
        catalog_provider: namespace.catalog_provider.wire_name(),
        status: namespace.status.wire_name(),
    }
}

fn verify_foundation_platform_r2_bucket_env(blockers: &mut Vec<String>) {
    match std::env::var("R2_BUCKET_NAME") {
        Ok(value)
            if value.trim()
                == LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name() => {}
        Ok(value) => blockers.push(format!(
            "R2_BUCKET_NAME must be {}, got {}",
            LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name(),
            value.trim()
        )),
        Err(std::env::VarError::NotPresent) => {
            blockers.push("R2_BUCKET_NAME is missing".to_owned())
        }
        Err(error) => blockers.push(format!("R2_BUCKET_NAME is invalid: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        bronze_artifact_command, content_type_for_object_key, read_bronze_run_evidence,
        source_slug_to_asset_token, BronzeObjectEvidence,
    };

    #[test]
    fn source_slug_token_is_registry_qualified_name_safe() -> anyhow::Result<()> {
        // Hyphen slugs (legacy format): hyphens become underscores.
        assert_eq!(
            source_slug_to_asset_token("molit-building-register-national-pilot")?,
            "molit_building_register_national_pilot"
        );
        // New provider__dataset format: underscores pass through unchanged.
        assert_eq!(
            source_slug_to_asset_token("datagokr__building_register_main")?,
            "datagokr__building_register_main"
        );
        assert_eq!(
            source_slug_to_asset_token("hubgokr__building_register_basis_outline")?,
            "hubgokr__building_register_basis_outline"
        );
        // Uppercase rejected.
        assert!(source_slug_to_asset_token("MOLIT").is_err());
        // Double hyphen (malformed hyphen-slug) rejected.
        assert!(source_slug_to_asset_token("molit--building").is_err());
        // Leading underscore rejected.
        assert!(source_slug_to_asset_token("_datagokr__building").is_err());
        // Trailing underscore rejected.
        assert!(source_slug_to_asset_token("datagokr__building_").is_err());
        Ok(())
    }

    #[test]
    fn content_type_follows_object_key_extension() {
        assert_eq!(
            content_type_for_object_key("bronze/source=x/part-000001.json"),
            "application/json"
        );
        assert_eq!(
            content_type_for_object_key("bronze/source=x/part-000001.jsonl"),
            "application/x-ndjson"
        );
    }

    #[test]
    fn bronze_run_evidence_reader_accepts_utf8_bom() -> anyhow::Result<()> {
        let path = PathBuf::from(
            "target/outbox-publisher-main-tests/lakehouse-registry-control-evidence-bom.json",
        );
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &path,
            concat!(
                "\u{feff}",
                r#"{"schema_version":"foundation-platform.national_data_collection_run_evidence.v1","#,
                r#""status":"ready","raw_response_preserved":true,"providers":{}}"#
            ),
        )?;

        let evidence = read_bronze_run_evidence(&path)?;
        assert_eq!(
            evidence.schema_version,
            "foundation-platform.national_data_collection_run_evidence.v1"
        );
        Ok(())
    }

    #[test]
    fn bronze_evidence_builds_one_atomic_registry_command() -> anyhow::Result<()> {
        let command = bronze_artifact_command(
            "foundation_platform.bronze.datagokr__building_register_main",
            "foundation-platform.national_data_collection_run_evidence.v1",
            "018f0000-0000-7000-8000-000000000911",
            &BronzeObjectEvidence {
                object_key: "bronze/source=datagokr__building_register_main/page-000001.json"
                    .to_owned(),
                checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                size_bytes: 1024,
                logical_record_count: 100,
            },
        )?;

        assert_eq!(
            command.dataset_version,
            "018f0000-0000-7000-8000-000000000911"
        );
        assert_eq!(
            command.object_key,
            "bronze/source=datagokr__building_register_main/page-000001.json"
        );
        assert_eq!(command.logical_record_count, Some(100));
        assert!(command.created_by_ingestion_run_id.is_none());
        Ok(())
    }
}
