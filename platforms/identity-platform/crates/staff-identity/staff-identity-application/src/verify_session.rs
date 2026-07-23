//! Staff bearer verification and session persistence use case.

use std::sync::Arc;

use authorization_domain::RoleCode;
use chrono::Utc;
use identity_contracts::PrincipalId;
use identity_shared_kernel::{SessionId, StaffId};
use staff_identity_domain::{StaffIdentityError, StaffSession};

use crate::ports::{EffectiveRoleReader, OidcVerifier, StaffRepository, StaffSessionUnitOfWork};

/// Raw bearer input accepted only by staff session verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyStaffSessionInput {
    /// OIDC bearer credential supplied by the caller.
    pub bearer_token: String,
}

/// Trusted staff identity and effective roles returned after verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedStaffContext {
    /// Stable public principal identifier derived from Identity-owned staff state.
    pub principal_id: PrincipalId,
    /// Internal staff identifier used by Identity mutations.
    pub staff_id: StaffId,
    /// Effective Identity roles read from the role source of truth.
    pub roles: Vec<RoleCode>,
}

/// Complete result returned after a staff bearer has been verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyStaffSessionOutput {
    /// Trusted staff authorization context.
    pub context: VerifiedStaffContext,
    /// Staff email address from the loaded Identity profile.
    pub email: String,
    /// Staff display name from the loaded Identity profile.
    pub display_name: String,
    /// UTC timestamp from the verified bearer claims when the credential expires.
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// JWT ID of the verified bearer, usable by the self-session revoke command.
    pub jti: String,
}

/// Verifies a staff bearer and returns the only trusted staff authorization context.
pub struct VerifyStaffSession {
    staff_repository: Arc<dyn StaffRepository>,
    session_uow: Arc<dyn StaffSessionUnitOfWork>,
    effective_role_reader: Arc<dyn EffectiveRoleReader>,
    oidc_verifier: Arc<dyn OidcVerifier>,
}

impl VerifyStaffSession {
    /// Creates the use case from its four focused ports.
    #[must_use]
    pub fn new(
        staff_repository: Arc<dyn StaffRepository>,
        session_uow: Arc<dyn StaffSessionUnitOfWork>,
        effective_role_reader: Arc<dyn EffectiveRoleReader>,
        oidc_verifier: Arc<dyn OidcVerifier>,
    ) -> Self {
        Self {
            staff_repository,
            session_uow,
            effective_role_reader,
            oidc_verifier,
        }
    }

    /// Verifies OIDC claims, enforces expiry and revocation, persists the session, and reads roles.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] for invalid, expired, revoked, unknown, or unpersistable
    /// sessions and for effective-role read failures.
    pub async fn execute(
        &self,
        input: VerifyStaffSessionInput,
    ) -> Result<VerifyStaffSessionOutput, StaffIdentityError> {
        let claims = self
            .oidc_verifier
            .verify_bearer(&input.bearer_token)
            .await?;
        if claims.expires_at <= Utc::now() {
            return Err(StaffIdentityError::SessionExpired);
        }
        if self.staff_repository.is_jti_revoked(&claims.jti).await? {
            return Err(StaffIdentityError::JtiRevoked(claims.jti));
        }

        let staff = self
            .staff_repository
            .find_by_zitadel_subject(&claims.subject)
            .await?
            .ok_or_else(|| StaffIdentityError::InvalidClaims("unknown subject".to_owned()))?;
        let mut roles = self
            .effective_role_reader
            .read_effective_roles(staff.id)
            .await?;
        roles.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        roles.dedup();

        let jti = claims.jti.clone();
        let session = StaffSession {
            session_id: SessionId::new(uuid::Uuid::now_v7()),
            staff_id: staff.id,
            jti: jti.clone(),
            issued_at: claims.issued_at,
            expires_at: claims.expires_at,
        };
        self.session_uow.persist_verified_session(&session).await?;

        Ok(VerifyStaffSessionOutput {
            context: VerifiedStaffContext {
                principal_id: PrincipalId::new(staff.id.as_uuid()),
                staff_id: staff.id,
                roles,
            },
            email: staff.email,
            display_name: staff.display_name,
            expires_at: claims.expires_at,
            jti,
        })
    }
}
