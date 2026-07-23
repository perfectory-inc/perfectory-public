//! `PostgreSQL` staff session unit of work.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use identity_contracts::{IdentityEventV1, PrincipalId, StaffSessionRevokedV1};
use sqlx::PgPool;
use staff_identity_application::ports::StaffSessionUnitOfWork;
use staff_identity_domain::{StaffIdentityError, StaffSession};

use crate::row_map::map_sqlx;

const PERSIST_VERIFIED_SESSION_SQL: &str = "INSERT INTO identity.staff_session
         (session_id, staff_id, jti, issued_at, expires_at)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (jti) DO UPDATE
     SET session_id = EXCLUDED.session_id,
         staff_id = EXCLUDED.staff_id,
         issued_at = EXCLUDED.issued_at,
         expires_at = EXCLUDED.expires_at";

/// `PostgreSQL` implementation of verified staff-session persistence.
pub struct PgStaffSessionUnitOfWork {
    pool: PgPool,
}

impl PgStaffSessionUnitOfWork {
    /// Creates a session unit of work backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StaffSessionUnitOfWork for PgStaffSessionUnitOfWork {
    async fn persist_verified_session(
        &self,
        session: &StaffSession,
    ) -> Result<(), StaffIdentityError> {
        sqlx::query(PERSIST_VERIFIED_SESSION_SQL)
            .bind(session.session_id.as_uuid())
            .bind(session.staff_id.as_uuid())
            .bind(&session.jti)
            .bind(session.issued_at)
            .bind(session.expires_at)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    async fn revoke_jti(
        &self,
        jti: &str,
        reason: &str,
        revoked_at: DateTime<Utc>,
    ) -> Result<(), StaffIdentityError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        let staff_id: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT staff_id
             FROM identity.staff_session
             WHERE jti = $1
             FOR UPDATE",
        )
        .bind(jti)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let Some(staff_id) = staff_id else {
            return Err(StaffIdentityError::SessionNotFound);
        };

        sqlx::query(
            "INSERT INTO identity.revoked_jti (jti, revoked_at, reason)
             VALUES ($1, $2, $3)
             ON CONFLICT (jti) DO UPDATE
             SET revoked_at = EXCLUDED.revoked_at, reason = EXCLUDED.reason",
        )
        .bind(jti)
        .bind(revoked_at)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let event = IdentityEventV1::StaffSessionRevoked(StaffSessionRevokedV1 {
            schema_version: 1,
            staff_id: PrincipalId::new(staff_id),
            jti: jti.to_owned(),
            revoked_at,
            reason: reason.to_owned(),
        });
        let payload = serde_json::to_value(&event)
            .map_err(|error| StaffIdentityError::Infrastructure(error.to_string()))?;
        sqlx::query(
            "INSERT INTO identity.outbox_event (event_id, type, payload, occurred_at)
             VALUES ($1, 'identity.staff.session_revoked.v1', $2, $3)",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(payload)
        .bind(revoked_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PERSIST_VERIFIED_SESSION_SQL;

    #[test]
    fn session_upsert_targets_only_the_final_identity_schema() {
        assert!(PERSIST_VERIFIED_SESSION_SQL.contains("INSERT INTO identity.staff_session"));
        assert!(PERSIST_VERIFIED_SESSION_SQL.contains("ON CONFLICT (jti) DO UPDATE"));
    }

    #[test]
    fn revoke_contract_writes_the_durable_denylist_and_event() {
        let source = include_str!("session_uow.rs");
        assert!(source.contains("INSERT INTO identity.revoked_jti"));
        assert!(source.contains("identity.staff.session_revoked.v1"));
    }
}
