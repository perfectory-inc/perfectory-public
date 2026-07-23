//! Transactional reconciliation of resolved workload principals.

use crate::{compiled_policy, parse_bindings, resolve_manifest, ManifestError, ValidatedManifest};
use authorization_domain::Permission;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

const DATABASE_URL_ENV: &str = "IDENTITY_PROVISIONER_DATABASE_URL";
const BINDINGS_PATH_ENV: &str = "IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS";

/// Environment-derived one-shot provisioning configuration.
pub struct ProvisionConfig {
    database_url: String,
    bindings_path: PathBuf,
}

impl fmt::Debug for ProvisionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProvisionConfig")
            .field("database_url", &"[REDACTED]")
            .field("bindings_path", &"[REDACTED]")
            .finish()
    }
}

impl ProvisionConfig {
    /// Reads the required database URL and environment binding path.
    ///
    /// # Errors
    /// Returns [`ProvisionError::Configuration`] when either value is missing or blank.
    pub fn from_env() -> Result<Self, ProvisionError> {
        let database_url = required_env(DATABASE_URL_ENV)?;
        let bindings_path = PathBuf::from(required_env(BINDINGS_PATH_ENV)?);
        Ok(Self {
            database_url,
            bindings_path,
        })
    }
}

fn required_env(name: &str) -> Result<String, ProvisionError> {
    let value = env::var(name).map_err(|_| ProvisionError::Configuration)?;
    if value.trim().is_empty() {
        return Err(ProvisionError::Configuration);
    }
    Ok(value)
}

/// Safe provisioning result containing counts only.
#[derive(Debug, Serialize)]
pub struct ProvisionReport {
    /// Fixed successful result marker.
    pub status: &'static str,
    /// Number of principals synchronized.
    pub principal_count: usize,
    /// Number of exact capability grants in policy.
    pub capability_count: usize,
}

/// Stable error category suitable for secret-safe command output.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionErrorCategory {
    /// Required runtime configuration was absent.
    Configuration,
    /// The bindings document could not be read.
    BindingsRead,
    /// Compiled policy or bindings failed validation.
    ManifestValidation,
    /// A database operation failed.
    Database,
}

/// One-shot provisioning failure.
#[derive(Debug, Error)]
pub enum ProvisionError {
    /// Required configuration was missing.
    #[error("required provisioning configuration is missing")]
    Configuration,
    /// The configured subject bindings could not be read.
    #[error("workload principal bindings could not be read")]
    BindingsRead,
    /// Workload policy or subject bindings failed validation.
    #[error("workload principal policy resolution failed")]
    ManifestValidation(#[from] ManifestError),
    /// A database operation failed.
    #[error("service-principal database operation failed")]
    Database(#[from] sqlx::Error),
}

impl ProvisionError {
    /// Returns a stable error category without sensitive details.
    #[must_use]
    pub const fn category(&self) -> ProvisionErrorCategory {
        match self {
            Self::Configuration => ProvisionErrorCategory::Configuration,
            Self::BindingsRead => ProvisionErrorCategory::BindingsRead,
            Self::ManifestValidation(_) => ProvisionErrorCategory::ManifestValidation,
            Self::Database(_) => ProvisionErrorCategory::Database,
        }
    }
}

/// Resolves compiled policy with environment bindings and provisions it through a bounded pool.
///
/// # Errors
/// Returns [`ProvisionError`] for binding I/O, validation, connection, or transaction failures.
pub async fn provision_with_config(
    config: &ProvisionConfig,
) -> Result<ProvisionReport, ProvisionError> {
    let bindings_raw =
        fs::read_to_string(&config.bindings_path).map_err(|_| ProvisionError::BindingsRead)?;
    let manifest = resolve_manifest(compiled_policy()?, parse_bindings(&bindings_raw)?)?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&config.database_url)
        .await?;
    provision(&pool, &manifest).await
}

/// Transactionally upserts principals and exactly synchronizes each listed principal's grants.
///
/// Principals absent from policy are intentionally not deleted; revocation remains an explicit
/// operational action. Capabilities for every policy principal are reconciled to the exact set.
///
/// # Errors
/// Returns [`ProvisionError::Database`] and rolls back the complete policy on any database error.
pub async fn provision(
    pool: &PgPool,
    manifest: &ValidatedManifest,
) -> Result<ProvisionReport, ProvisionError> {
    let mut transaction = pool.begin().await?;
    let mut capability_count = 0;

    for principal in &manifest.principals {
        sqlx::query(
            "INSERT INTO identity.service_principal (id, zitadel_subject, display_name) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (id) DO UPDATE \
             SET zitadel_subject = EXCLUDED.zitadel_subject, \
                 display_name = EXCLUDED.display_name, \
                 updated_at = now() \
             WHERE identity.service_principal.zitadel_subject \
                       IS DISTINCT FROM EXCLUDED.zitadel_subject \
                OR identity.service_principal.display_name \
                       IS DISTINCT FROM EXCLUDED.display_name",
        )
        .bind(principal.principal_id.as_uuid())
        .bind(&principal.zitadel_subject)
        .bind(&principal.display_name)
        .execute(&mut *transaction)
        .await?;

        let capability_values: Vec<&str> = principal
            .capabilities
            .iter()
            .map(Permission::as_str)
            .collect();
        sqlx::query(
            "DELETE FROM identity.service_capability_grant \
             WHERE service_principal_id = $1 \
               AND NOT (capability = ANY($2::text[]))",
        )
        .bind(principal.principal_id.as_uuid())
        .bind(&capability_values)
        .execute(&mut *transaction)
        .await?;

        for capability in &principal.capabilities {
            sqlx::query(
                "INSERT INTO identity.service_capability_grant \
                     (service_principal_id, capability) \
                 VALUES ($1, $2) \
                 ON CONFLICT (service_principal_id, capability) DO NOTHING",
            )
            .bind(principal.principal_id.as_uuid())
            .bind(capability.as_str())
            .execute(&mut *transaction)
            .await?;
        }
        capability_count += principal.capabilities.len();
    }

    transaction.commit().await?;
    Ok(ProvisionReport {
        status: "ok",
        principal_count: manifest.principals.len(),
        capability_count,
    })
}
