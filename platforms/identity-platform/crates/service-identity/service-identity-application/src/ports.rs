//! Credential verification and audit ports for service identity.

use async_trait::async_trait;
use authorization_domain::PolicyDecision;
use chrono::{DateTime, Utc};
use identity_contracts::PrincipalId;
use service_identity_domain::{ServiceIdentityError, ValidatedServicePrincipal};

/// Audit record emitted for a validated service principal's authorization decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceAuthorizationAudit {
    /// Validated service principal.
    pub principal_id: PrincipalId,
    /// Capability or resource namespace evaluated.
    pub resource: String,
    /// Requested action.
    pub action: String,
    /// Optional resource instance identifier.
    pub resource_id: Option<String>,
    /// Result of the pure service capability policy.
    pub decision: PolicyDecision,
    /// Correlation identifier supplied with the call.
    pub trace_id: String,
    /// UTC timestamp when Identity evaluated the call.
    pub evaluated_at: DateTime<Utc>,
}

/// Verifies service credentials and returns validated principals.
#[async_trait]
pub trait ServiceCredentialVerifier: Send + Sync {
    /// Validates a bearer credential without persisting or exposing it.
    ///
    /// # Errors
    /// Returns [`ServiceIdentityError`] when credential verification fails.
    async fn verify_credential(
        &self,
        bearer_token: &str,
    ) -> Result<ValidatedServicePrincipal, ServiceIdentityError>;
}

/// Records Identity authorization decisions without coupling policy to storage.
#[async_trait]
pub trait IdentityAuditSink: Send + Sync {
    /// Persists or publishes a service authorization audit record.
    ///
    /// # Errors
    /// Returns [`ServiceIdentityError`] when audit recording fails.
    async fn record_service_authorization(
        &self,
        audit: &ServiceAuthorizationAudit,
    ) -> Result<(), ServiceIdentityError>;
}
