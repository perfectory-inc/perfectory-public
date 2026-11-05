//! Lakehouse Registry seed and verification commands.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use foundation_shared_kernel::ids::LakehouseStorageNamespaceId;
use lakehouse_application::ports::LakehouseRegistryRepository;
use lakehouse_domain::{
    LakehouseCatalogProvider, LakehouseEnvironment, LakehouseNamespaceStatus,
    LakehouseOwnerService, LakehouseStorageNamespace, LakehouseStorageProvider,
};
use lakehouse_infrastructure::PgLakehouseRegistryRepository;
use serde::Serialize;
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
    match std::env::var("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET") {
        Ok(value)
            if value.trim()
                == LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name() => {}
        Ok(value) => blockers.push(format!(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET must be {}, got {}",
            LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name(),
            value.trim()
        )),
        Err(std::env::VarError::NotPresent) => {
            blockers.push("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET is missing".to_owned())
        }
        Err(error) => blockers.push(format!(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET is invalid: {error}"
        )),
    }
}
