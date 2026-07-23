//! Focused outbound ports for staff session verification.

use async_trait::async_trait;
use authorization_domain::RoleCode;
use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;
use staff_identity_domain::{Staff, StaffIdentityError, StaffSession};

/// Trusted claims returned after OIDC bearer verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedOidcClaims {
    /// Identity-provider subject mapped to an Identity staff account.
    pub subject: String,
    /// JWT ID used for durable revocation checks.
    pub jti: String,
    /// UTC timestamp when the bearer was issued.
    pub issued_at: DateTime<Utc>,
    /// UTC timestamp when the bearer expires.
    pub expires_at: DateTime<Utc>,
}

/// Reads staff identity state required during session verification.
#[async_trait]
pub trait StaffRepository: Send + Sync {
    /// Finds a staff account by its verified identity-provider subject.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when the read fails.
    async fn find_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<Staff>, StaffIdentityError>;

    /// Returns whether Identity has revoked the supplied JWT ID.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when the revoke-state read fails.
    async fn is_jti_revoked(&self, jti: &str) -> Result<bool, StaffIdentityError>;
}

/// Atomically persists staff session state after successful verification.
#[async_trait]
pub trait StaffSessionUnitOfWork: Send + Sync {
    /// Inserts or refreshes the verified session.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when session persistence fails.
    async fn persist_verified_session(
        &self,
        session: &StaffSession,
    ) -> Result<(), StaffIdentityError>;

    /// Revokes a persisted session and records the corresponding Identity event atomically.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError::SessionNotFound`] when the JTI is unknown, or an
    /// infrastructure error when the revoke and outbox transaction cannot be committed.
    async fn revoke_jti(
        &self,
        jti: &str,
        reason: &str,
        revoked_at: DateTime<Utc>,
    ) -> Result<(), StaffIdentityError>;
}

/// Reads effective Identity role grants for a trusted staff identifier.
#[async_trait]
pub trait EffectiveRoleReader: Send + Sync {
    /// Returns all effective roles for `staff_id`.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when role resolution fails.
    async fn read_effective_roles(
        &self,
        staff_id: StaffId,
    ) -> Result<Vec<RoleCode>, StaffIdentityError>;
}

/// Verifies OIDC bearer credentials without exposing adapter details.
#[async_trait]
pub trait OidcVerifier: Send + Sync {
    /// Validates a raw bearer and returns trusted claims.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when signature, issuer, audience, or claims validation fails.
    async fn verify_bearer(
        &self,
        bearer_token: &str,
    ) -> Result<VerifiedOidcClaims, StaffIdentityError>;
}
