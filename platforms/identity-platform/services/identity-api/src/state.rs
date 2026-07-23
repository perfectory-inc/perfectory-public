//! Identity API application state and production adapter composition.

use std::env;
use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use authorization_application::{
    AssignStaffRole, BootstrapMasterAdmin, BootstrapMasterAdminInput, BootstrapMasterAdminOutcome,
    EvaluateAccess,
};
use authorization_infrastructure::{
    PgEffectiveRoleReader, PgIdentityBootstrapUnitOfWork, PgRoleGrantUnitOfWork,
};
use service_identity_application::ports::ServiceCredentialVerifier;
use service_identity_application::AuthorizeServiceCall;
use service_identity_infrastructure::{
    PgServicePrincipalCapabilityReader, TracingIdentityAuditSink, ZitadelMachineTokenVerifier,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use staff_identity_application::{RevokeStaffSession, VerifyStaffSession};
use staff_identity_infrastructure::{
    PgStaffRepository, PgStaffSessionUnitOfWork, ZitadelOidcVerifier,
};

/// Readiness boundary used by production and explicit test states.
#[async_trait]
pub trait ReadinessProbe: Send + Sync {
    /// Returns whether the Identity database can answer a minimal query.
    async fn database_ready(&self) -> bool;
}

/// Immutable application composition shared by all Identity routes.
pub struct AppState {
    pub(crate) verify_staff_session: Arc<VerifyStaffSession>,
    pub(crate) revoke_staff_session: Arc<RevokeStaffSession>,
    pub(crate) assign_staff_role: Arc<AssignStaffRole>,
    pub(crate) evaluate_access: EvaluateAccess,
    pub(crate) service_credential_verifier: Arc<dyn ServiceCredentialVerifier>,
    pub(crate) authorize_service_call: Arc<AuthorizeServiceCall>,
    readiness_probe: Arc<dyn ReadinessProbe>,
    verifier_configuration_valid: bool,
}

impl AppState {
    /// Creates state from explicit use cases and ports.
    #[must_use]
    pub fn new(
        verify_staff_session: Arc<VerifyStaffSession>,
        revoke_staff_session: Arc<RevokeStaffSession>,
        assign_staff_role: Arc<AssignStaffRole>,
        evaluate_access: EvaluateAccess,
        service_credential_verifier: Arc<dyn ServiceCredentialVerifier>,
        authorize_service_call: Arc<AuthorizeServiceCall>,
        readiness_probe: Arc<dyn ReadinessProbe>,
    ) -> Self {
        Self {
            verify_staff_session,
            revoke_staff_session,
            assign_staff_role,
            evaluate_access,
            service_credential_verifier,
            authorize_service_call,
            readiness_probe,
            verifier_configuration_valid: true,
        }
    }

    /// Composes final Task 3 use cases with Task 4A production adapters.
    ///
    /// # Errors
    /// Returns an error when the database URL is invalid or configured administrator bootstrap
    /// cannot complete.
    pub async fn production(config: ProductionConfig) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect_lazy(&config.database_url)
            .context("IDENTITY_DATABASE_URL is invalid")?;

        if let Some(bootstrap) = config.bootstrap_admin {
            let use_case = BootstrapMasterAdmin::new(Arc::new(PgIdentityBootstrapUnitOfWork::new(
                pool.clone(),
            )));
            match use_case
                .execute(BootstrapMasterAdminInput {
                    zitadel_subject: bootstrap.zitadel_subject,
                    email: bootstrap.email,
                    display_name: bootstrap.display_name,
                })
                .await
                .context("Identity administrator bootstrap failed")?
            {
                BootstrapMasterAdminOutcome::AlreadyPresent => {
                    tracing::info!("Identity administrator bootstrap already satisfied");
                }
                BootstrapMasterAdminOutcome::Created { .. } => {
                    tracing::info!("Identity administrator bootstrap completed");
                }
            }
        }

        let session_uow = Arc::new(PgStaffSessionUnitOfWork::new(pool.clone()));
        let verify_staff_session = Arc::new(VerifyStaffSession::new(
            Arc::new(PgStaffRepository::new(pool.clone())),
            session_uow.clone(),
            Arc::new(PgEffectiveRoleReader::new(pool.clone())),
            Arc::new(ZitadelOidcVerifier::new(
                &config.zitadel_issuer_url,
                &config.audience,
            )),
        ));
        let revoke_staff_session = Arc::new(RevokeStaffSession::new(session_uow));
        let assign_staff_role = Arc::new(AssignStaffRole::new(Arc::new(
            PgRoleGrantUnitOfWork::new(pool.clone()),
        )));
        let service_reader = Arc::new(PgServicePrincipalCapabilityReader::new(pool.clone()));
        let service_credential_verifier = Arc::new(ZitadelMachineTokenVerifier::new(
            &config.zitadel_issuer_url,
            &config.audience,
            service_reader,
        ));
        let authorize_service_call = Arc::new(AuthorizeServiceCall::new(Arc::new(
            TracingIdentityAuditSink,
        )));

        Ok(Self::new(
            verify_staff_session,
            revoke_staff_session,
            assign_staff_role,
            EvaluateAccess::new(),
            service_credential_verifier,
            authorize_service_call,
            Arc::new(PgReadinessProbe { pool }),
        ))
    }

    pub(crate) async fn readiness(&self) -> Readiness {
        let database = self.readiness_probe.database_ready().await;
        Readiness {
            database,
            verifier_configuration_valid: self.verifier_configuration_valid,
        }
    }
}

pub(crate) struct Readiness {
    pub(crate) database: bool,
    pub(crate) verifier_configuration_valid: bool,
}

impl Readiness {
    pub(crate) const fn is_ready(&self) -> bool {
        self.database && self.verifier_configuration_valid
    }
}

struct PgReadinessProbe {
    pool: PgPool,
}

#[async_trait]
impl ReadinessProbe for PgReadinessProbe {
    async fn database_ready(&self) -> bool {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }
}

/// Validated production configuration for Identity API wiring.
pub struct ProductionConfig {
    database_url: String,
    zitadel_issuer_url: String,
    audience: String,
    bootstrap_admin: Option<BootstrapAdminConfig>,
}

impl ProductionConfig {
    /// Loads required production wiring and optional all-or-none administrator bootstrap data.
    ///
    /// # Errors
    /// Returns an error when required values are absent or the bootstrap tuple is incomplete.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let database_url =
            required_value("IDENTITY_DATABASE_URL", lookup("IDENTITY_DATABASE_URL"))?;
        let zitadel_issuer_url = issuer_url(&required_value(
            "IDENTITY_ZITADEL_ISSUER_URL",
            lookup("IDENTITY_ZITADEL_ISSUER_URL"),
        )?)?;
        let audience = required_value("IDENTITY_API_AUDIENCE", lookup("IDENTITY_API_AUDIENCE"))?;
        let bootstrap_admin = bootstrap_from_lookup(&lookup)?;
        Ok(Self {
            database_url,
            zitadel_issuer_url,
            audience,
            bootstrap_admin,
        })
    }
}

struct BootstrapAdminConfig {
    zitadel_subject: String,
    email: String,
    display_name: String,
}

fn required_value(name: &'static str, value: Option<String>) -> anyhow::Result<String> {
    let value = value.with_context(|| format!("{name} is required"))?;
    if value.trim().is_empty() {
        bail!("{name} must not be blank");
    }
    Ok(value)
}

fn issuer_url(value: &str) -> anyhow::Result<String> {
    let parsed = reqwest::Url::parse(value).context("IDENTITY_ZITADEL_ISSUER_URL is invalid")?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.has_host()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("IDENTITY_ZITADEL_ISSUER_URL must use HTTP or HTTPS");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn bootstrap_from_lookup(
    lookup: &impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Option<BootstrapAdminConfig>> {
    let subject = lookup("IDENTITY_BOOTSTRAP_ADMIN_ZITADEL_SUBJECT");
    let email = lookup("IDENTITY_BOOTSTRAP_ADMIN_EMAIL");
    let display_name = lookup("IDENTITY_BOOTSTRAP_ADMIN_DISPLAY_NAME");
    match (subject, email, display_name) {
        (None, None, None) => Ok(None),
        (Some(zitadel_subject), Some(email), Some(display_name))
            if !zitadel_subject.trim().is_empty()
                && !email.trim().is_empty()
                && !display_name.trim().is_empty() =>
        {
            Ok(Some(BootstrapAdminConfig {
                zitadel_subject,
                email,
                display_name,
            }))
        }
        _ => bail!(
            "Identity bootstrap requires subject, email, and display name together with non-blank values"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::ProductionConfig;

    #[test]
    fn production_config_requires_final_identity_wiring_names() {
        let values = HashMap::from([
            ("IDENTITY_DATABASE_URL", "postgres://identity-secret"),
            (
                "IDENTITY_ZITADEL_ISSUER_URL",
                "https://identity.example.test",
            ),
        ]);

        let error = ProductionConfig::from_lookup(|name| values.get(name).map(ToString::to_string))
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();

        assert!(error.contains("IDENTITY_API_AUDIENCE"));
        assert!(!error.contains("identity-secret"));
    }

    #[test]
    fn production_config_accepts_absent_or_complete_bootstrap_but_rejects_partial() {
        let base = HashMap::from([
            ("IDENTITY_DATABASE_URL", "postgres://identity-secret"),
            (
                "IDENTITY_ZITADEL_ISSUER_URL",
                "https://identity.example.test",
            ),
            ("IDENTITY_API_AUDIENCE", "identity-api"),
        ]);
        let without_bootstrap =
            ProductionConfig::from_lookup(|name| base.get(name).map(ToString::to_string));
        assert!(without_bootstrap.is_ok());

        let mut partial = base.clone();
        partial.insert("IDENTITY_BOOTSTRAP_ADMIN_ZITADEL_SUBJECT", "admin-subject");
        assert!(ProductionConfig::from_lookup(|name| {
            partial.get(name).map(ToString::to_string)
        })
        .is_err());

        let mut complete = partial;
        complete.insert("IDENTITY_BOOTSTRAP_ADMIN_EMAIL", "admin@example.test");
        complete.insert("IDENTITY_BOOTSTRAP_ADMIN_DISPLAY_NAME", "Admin");
        let configured =
            ProductionConfig::from_lookup(|name| complete.get(name).map(ToString::to_string));
        assert!(configured.is_ok());
    }

    #[test]
    fn production_config_accepts_only_well_formed_http_or_https_issuer_urls() {
        for issuer in [
            "not a URL",
            "ftp://identity.example.test",
            "file:///tmp/issuer",
            "https://",
            "https://identity.example.test/?tenant=a",
            "https://identity.example.test/#fragment",
        ] {
            let values = HashMap::from([
                ("IDENTITY_DATABASE_URL", "postgres://identity-secret"),
                ("IDENTITY_ZITADEL_ISSUER_URL", issuer),
                ("IDENTITY_API_AUDIENCE", "identity-api"),
            ]);

            assert!(
                ProductionConfig::from_lookup(|name| { values.get(name).map(ToString::to_string) })
                    .is_err(),
                "accepted issuer {issuer}"
            );
        }

        for issuer in [
            "http://identity.example.test",
            "https://identity.example.test/tenant/",
        ] {
            let values = HashMap::from([
                ("IDENTITY_DATABASE_URL", "postgres://identity-secret"),
                ("IDENTITY_ZITADEL_ISSUER_URL", issuer),
                ("IDENTITY_API_AUDIENCE", "identity-api"),
            ]);

            assert!(
                ProductionConfig::from_lookup(|name| { values.get(name).map(ToString::to_string) })
                    .is_ok(),
                "rejected issuer {issuer}"
            );
        }
    }
}
