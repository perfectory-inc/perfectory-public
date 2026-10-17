//! Transactional reconciliation of resolved workload principals.

use crate::{compiled_policy, parse_bindings, resolve_manifest, ManifestError, ValidatedManifest};
use authorization_domain::Permission;
use chrono::{DateTime, Utc};
use identity_contracts::{IdentityEventV1, ServiceCapabilityChangedV1};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

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
    /// Number of `granted` records this run appended.
    pub granted_count: usize,
    /// Number of `revoked` records this run appended.
    pub revoked_count: usize,
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
    /// A grant-change event could not be encoded.
    EventEncoding,
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
    /// A grant-change event could not be encoded for the outbox.
    #[error("service capability event could not be encoded")]
    EventEncoding,
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
            Self::EventEncoding => ProvisionErrorCategory::EventEncoding,
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

/// Transactionally upserts principals and appends one grant record per capability change.
///
/// Grants are an append-only ledger (root ADR-0212): a capability the policy adds is appended as
/// `granted`, one it drops as `revoked`, and each change writes its outbox event in the same
/// transaction. Nothing is deleted or updated, so an unchanged policy appends nothing. Principals
/// absent from policy are intentionally left alone; removing a principal is an explicit
/// operational action.
///
/// # Errors
/// Returns [`ProvisionError::Database`] and rolls back the complete policy on any database error.
pub async fn provision(
    pool: &PgPool,
    manifest: &ValidatedManifest,
) -> Result<ProvisionReport, ProvisionError> {
    let mut transaction = pool.begin().await?;
    // Two provisioners at once would read the same current grants and append the same change
    // twice; one lock makes record order equal commit order.
    sqlx::query(PROVISION_LOCK_SQL)
        .execute(&mut *transaction)
        .await?;
    let mut capability_count = 0;
    let mut granted_count = 0;
    let mut revoked_count = 0;

    for principal in &manifest.principals {
        // The upsert returns a row only when it inserted or changed one; only then is there a
        // new value to record (root ADR-0264).
        let changed: Option<Uuid> = sqlx::query_scalar(UPSERT_PRINCIPAL_SQL)
            .bind(principal.principal_id.as_uuid())
            .bind(&principal.zitadel_subject)
            .bind(&principal.display_name)
            .fetch_optional(&mut *transaction)
            .await?;
        if changed.is_some() {
            sqlx::query(APPEND_PRINCIPAL_RECORD_SQL)
                .bind(principal.principal_id.as_uuid())
                .bind(&principal.zitadel_subject)
                .bind(&principal.display_name)
                .bind(&manifest.policy_sha256)
                .execute(&mut *transaction)
                .await?;
        }

        let current: Vec<String> = sqlx::query_scalar(CURRENT_GRANTS_SQL)
            .bind(principal.principal_id.as_uuid())
            .fetch_all(&mut *transaction)
            .await?;
        let desired: Vec<&str> = principal
            .capabilities
            .iter()
            .map(Permission::as_str)
            .collect();
        for (change, capability) in plan_capability_changes(&current, &desired) {
            let changed_at: DateTime<Utc> = sqlx::query_scalar(APPEND_GRANT_RECORD_SQL)
                .bind(principal.principal_id.as_uuid())
                .bind(capability)
                .bind(change.as_str())
                .bind(&manifest.policy_sha256)
                .fetch_one(&mut *transaction)
                .await?;
            let payload = ServiceCapabilityChangedV1 {
                schema_version: 1,
                service_principal_id: principal.principal_id,
                capability: capability.to_owned(),
                changed_at,
                policy_sha256: manifest.policy_sha256.clone(),
            };
            let event = match change {
                CapabilityChange::Granted => {
                    granted_count += 1;
                    IdentityEventV1::ServiceCapabilityGranted(payload)
                }
                CapabilityChange::Revoked => {
                    revoked_count += 1;
                    IdentityEventV1::ServiceCapabilityRevoked(payload)
                }
            };
            insert_outbox_event(&mut transaction, &event, changed_at).await?;
        }
        capability_count += principal.capabilities.len();
    }

    transaction.commit().await?;
    Ok(ProvisionReport {
        status: "ok",
        principal_count: manifest.principals.len(),
        capability_count,
        granted_count,
        revoked_count,
    })
}

const PROVISION_LOCK_SQL: &str =
    "SELECT pg_advisory_xact_lock(hashtext('identity.service_capability_grant.provision'))";
const UPSERT_PRINCIPAL_SQL: &str = "INSERT INTO identity.service_principal \
         (id, zitadel_subject, display_name) \
     VALUES ($1, $2, $3) \
     ON CONFLICT (id) DO UPDATE \
     SET zitadel_subject = EXCLUDED.zitadel_subject, \
         display_name = EXCLUDED.display_name, \
         updated_at = now() \
     WHERE identity.service_principal.zitadel_subject \
               IS DISTINCT FROM EXCLUDED.zitadel_subject \
        OR identity.service_principal.display_name \
               IS DISTINCT FROM EXCLUDED.display_name \n     RETURNING id";
const APPEND_PRINCIPAL_RECORD_SQL: &str = "INSERT INTO identity.service_principal_record \n         (service_principal_id, zitadel_subject, display_name, source, policy_sha256) \n     VALUES ($1, $2, $3, 'workload_policy', $4)";
const CURRENT_GRANTS_SQL: &str = "SELECT capability FROM identity.service_capability_grant \
     WHERE service_principal_id = $1 ORDER BY capability";
const APPEND_GRANT_RECORD_SQL: &str = "INSERT INTO identity.service_capability_grant_record \
         (service_principal_id, capability, change, source, policy_sha256) \
     VALUES ($1, $2, $3, 'workload_policy', $4) \
     RETURNING recorded_at";
const INSERT_OUTBOX_SQL: &str = "INSERT INTO identity.outbox_event \
         (event_id, type, payload, occurred_at) \
     VALUES ($1, $2, $3, $4)";

/// One appended change to a principal's capability set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapabilityChange {
    Granted,
    Revoked,
}

impl CapabilityChange {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Revoked => "revoked",
        }
    }
}

/// Changes that turn `current` into `desired`: revokes first, then grants, each sorted.
fn plan_capability_changes<'a>(
    current: &'a [String],
    desired: &[&'a str],
) -> Vec<(CapabilityChange, &'a str)> {
    let current_set: BTreeSet<&str> = current.iter().map(String::as_str).collect();
    let desired_set: BTreeSet<&str> = desired.iter().copied().collect();
    current_set
        .difference(&desired_set)
        .map(|capability| (CapabilityChange::Revoked, *capability))
        .chain(
            desired_set
                .difference(&current_set)
                .map(|capability| (CapabilityChange::Granted, *capability)),
        )
        .collect()
}

async fn insert_outbox_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &IdentityEventV1,
    occurred_at: DateTime<Utc>,
) -> Result<(), ProvisionError> {
    let payload = serde_json::to_value(event).map_err(|_| ProvisionError::EventEncoding)?;
    let event_type = payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(ProvisionError::EventEncoding)?
        .to_owned();
    sqlx::query(INSERT_OUTBOX_SQL)
        .bind(Uuid::now_v7())
        .bind(event_type)
        .bind(payload)
        .bind(occurred_at)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{plan_capability_changes, CapabilityChange};

    #[test]
    fn an_unchanged_policy_appends_nothing() {
        let current = vec!["a:read".to_owned(), "b:write".to_owned()];
        assert!(plan_capability_changes(&current, &["b:write", "a:read"]).is_empty());
    }

    #[test]
    fn a_dropped_capability_is_revoked_and_an_added_one_granted() {
        let current = vec!["a:read".to_owned(), "b:write".to_owned()];
        assert_eq!(
            plan_capability_changes(&current, &["b:write", "c:propose"]),
            vec![
                (CapabilityChange::Revoked, "a:read"),
                (CapabilityChange::Granted, "c:propose"),
            ]
        );
    }

    #[test]
    fn an_empty_policy_revokes_everything() {
        let current = vec!["a:read".to_owned()];
        assert_eq!(
            plan_capability_changes(&current, &[]),
            vec![(CapabilityChange::Revoked, "a:read")]
        );
    }
}
