//! Atomic staff role-grant persistence.

use async_trait::async_trait;
use authorization_application::ports::{RoleGrantPersistenceError, RoleGrantUnitOfWork};
use authorization_domain::{RoleCode, RoleGrant};
use chrono::{DateTime, Utc};
use identity_contracts::{IdentityEventV1, PrincipalId, StaffRoleAssignedV1};
use identity_shared_kernel::StaffId;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use super::{is_foreign_key_constraint_violation, is_unique_constraint_violation};

const ROLE_GRANT_CONSTRAINT: &str = "identity_staff_role_pkey";
const TARGET_STAFF_CONSTRAINT: &str = "identity_staff_role_staff_id_fkey";
const INSERT_ROLE_GRANT_SQL: &str =
    "INSERT INTO identity.staff_role (staff_id, role_code, granted_at, granted_by)
     VALUES ($1, $2, now(), $3)
     RETURNING staff_id, role_code, granted_at, granted_by";
const INSERT_OUTBOX_SQL: &str =
    "INSERT INTO identity.outbox_event (event_id, type, payload, occurred_at)
     VALUES ($1, $2, $3, $4)";

/// `PostgreSQL` unit of work for role grants and their outbox events.
pub struct PgRoleGrantUnitOfWork {
    pool: PgPool,
}

impl PgRoleGrantUnitOfWork {
    /// Creates a role-grant unit of work backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

struct RoleGrantRow {
    staff_id: Uuid,
    role_code: String,
    granted_at: DateTime<Utc>,
    granted_by: Uuid,
}

#[async_trait]
impl RoleGrantUnitOfWork for PgRoleGrantUnitOfWork {
    async fn assign_role(
        &self,
        staff_id: StaffId,
        role_code: &RoleCode,
        granted_by: StaffId,
    ) -> Result<RoleGrant, RoleGrantPersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        let row = sqlx::query(INSERT_ROLE_GRANT_SQL)
            .bind(staff_id.as_uuid())
            .bind(role_code.as_str())
            .bind(granted_by.as_uuid())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| map_role_grant_insert_error(error, staff_id))?;
        let grant = row_to_role_grant(&row)?;
        let principal_id = PrincipalId::new(grant.staff_id.as_uuid());
        let event = IdentityEventV1::StaffRoleAssigned(StaffRoleAssignedV1 {
            schema_version: 1,
            staff_id: principal_id,
            role_code: grant.role_code.as_str().to_owned(),
            assigned_at: grant.granted_at,
            assigned_by: PrincipalId::new(grant.granted_by.as_uuid()),
        });
        insert_outbox_event(&mut transaction, &event, grant.granted_at).await?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(grant)
    }
}

fn row_to_role_grant(row: &PgRow) -> Result<RoleGrant, RoleGrantPersistenceError> {
    let values = RoleGrantRow {
        staff_id: row.try_get("staff_id").map_err(map_sqlx)?,
        role_code: row.try_get("role_code").map_err(map_sqlx)?,
        granted_at: row.try_get("granted_at").map_err(map_sqlx)?,
        granted_by: row.try_get("granted_by").map_err(map_sqlx)?,
    };
    map_role_grant_row(values)
}

fn map_role_grant_row(row: RoleGrantRow) -> Result<RoleGrant, RoleGrantPersistenceError> {
    Ok(RoleGrant {
        staff_id: StaffId::new(row.staff_id),
        role_code: RoleCode::parse(row.role_code)
            .map_err(|error| RoleGrantPersistenceError::Infrastructure(error.to_string()))?,
        granted_at: row.granted_at,
        granted_by: StaffId::new(row.granted_by),
    })
}

async fn insert_outbox_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &IdentityEventV1,
    occurred_at: DateTime<Utc>,
) -> Result<(), RoleGrantPersistenceError> {
    let payload = serde_json::to_value(event)
        .map_err(|error| RoleGrantPersistenceError::Infrastructure(error.to_string()))?;
    let event_type = payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            RoleGrantPersistenceError::Infrastructure("event type is missing".to_owned())
        })?
        .to_owned();
    sqlx::query(INSERT_OUTBOX_SQL)
        .bind(Uuid::now_v7())
        .bind(event_type)
        .bind(payload)
        .bind(occurred_at)
        .execute(&mut **transaction)
        .await
        .map_err(map_sqlx)?;
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn map_role_grant_insert_error(error: sqlx::Error, staff_id: StaffId) -> RoleGrantPersistenceError {
    if is_unique_constraint_violation(&error, ROLE_GRANT_CONSTRAINT) {
        return RoleGrantPersistenceError::DuplicateRole;
    }
    if is_foreign_key_constraint_violation(&error, TARGET_STAFF_CONSTRAINT) {
        return RoleGrantPersistenceError::StaffNotFound(staff_id.as_uuid().to_string());
    }
    map_sqlx(error)
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> RoleGrantPersistenceError {
    RoleGrantPersistenceError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{map_role_grant_insert_error, map_role_grant_row, RoleGrantRow};
    use authorization_application::ports::RoleGrantPersistenceError;
    use chrono::{TimeZone, Utc};
    use identity_shared_kernel::StaffId;
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn maps_role_grant_row_values_into_the_domain_model() -> Result<(), Box<dyn Error>> {
        let staff_id = Uuid::parse_str("018f30c0-7b5a-7cc0-8c9d-1f3d12f85350")?;
        let granted_by = Uuid::parse_str("018f30c0-7b5a-7cc0-8c9d-1f3d12f85351")?;
        let granted_at = Utc.timestamp_opt(1_700_000_000, 0).single().ok_or("time")?;

        let grant = map_role_grant_row(RoleGrantRow {
            staff_id,
            role_code: "CATALOG_ADMIN".to_owned(),
            granted_at,
            granted_by,
        })?;

        assert_eq!(grant.staff_id, StaffId::new(staff_id));
        assert_eq!(grant.role_code.as_str(), "CATALOG_ADMIN");
        assert_eq!(grant.granted_at, granted_at);
        assert_eq!(grant.granted_by, StaffId::new(granted_by));
        Ok(())
    }

    #[test]
    fn maps_only_the_named_role_grant_constraint_to_duplicate_role() {
        let staff_id = StaffId::new(Uuid::from_u128(1));
        let duplicate = map_role_grant_insert_error(
            super::super::test_database_error("23505", "identity_staff_role_pkey"),
            staff_id,
        );
        let other_unique = map_role_grant_insert_error(
            super::super::test_database_error("23505", "identity_staff_role_other_key"),
            staff_id,
        );
        let wrong_sqlstate = map_role_grant_insert_error(
            super::super::test_database_error("23503", "identity_staff_role_pkey"),
            staff_id,
        );

        assert!(matches!(
            duplicate,
            RoleGrantPersistenceError::DuplicateRole
        ));
        assert!(matches!(
            other_unique,
            RoleGrantPersistenceError::Infrastructure(_)
        ));
        assert!(matches!(
            wrong_sqlstate,
            RoleGrantPersistenceError::Infrastructure(_)
        ));
    }

    #[test]
    fn maps_only_the_named_target_staff_foreign_key_to_staff_not_found() {
        let staff_id = StaffId::new(Uuid::from_u128(2));
        let missing_staff = map_role_grant_insert_error(
            super::super::test_database_error("23503", "identity_staff_role_staff_id_fkey"),
            staff_id,
        );
        let missing_grantor = map_role_grant_insert_error(
            super::super::test_database_error("23503", "identity_staff_role_granted_by_fkey"),
            staff_id,
        );

        assert!(matches!(
            missing_staff,
            RoleGrantPersistenceError::StaffNotFound(id) if id == staff_id.as_uuid().to_string()
        ));
        assert!(matches!(
            missing_grantor,
            RoleGrantPersistenceError::Infrastructure(_)
        ));
    }
}
