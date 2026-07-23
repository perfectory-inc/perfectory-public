//! Staff-session revocation use case.

use std::sync::Arc;

use chrono::Utc;
use staff_identity_domain::StaffIdentityError;

use crate::ports::StaffSessionUnitOfWork;

/// Revokes one already-persisted staff session.
pub struct RevokeStaffSession {
    session_uow: Arc<dyn StaffSessionUnitOfWork>,
}

impl RevokeStaffSession {
    /// Creates a revocation use case backed by the Identity session unit of work.
    #[must_use]
    pub fn new(session_uow: Arc<dyn StaffSessionUnitOfWork>) -> Self {
        Self { session_uow }
    }

    /// Revokes a session by its verified JWT ID.
    ///
    /// # Errors
    ///
    /// Returns [`StaffIdentityError::SessionNotFound`] when the JTI is not
    /// backed by a persisted staff session, or
    /// [`StaffIdentityError::Infrastructure`] when persistence fails.
    pub async fn execute(
        &self,
        jti: impl AsRef<str>,
        reason: impl AsRef<str>,
    ) -> Result<(), StaffIdentityError> {
        let jti = jti.as_ref().trim();
        let reason = reason.as_ref().trim();
        if jti.is_empty() || reason.is_empty() {
            return Err(StaffIdentityError::InvalidClaims(
                "session revocation requires a JTI and reason".to_owned(),
            ));
        }
        self.session_uow.revoke_jti(jti, reason, Utc::now()).await
    }
}
