//! Atomic first-master-administrator bootstrap persistence.

use async_trait::async_trait;
use authorization_application::ports::IdentityBootstrapUnitOfWork;
use authorization_domain::RoleGrant;
use identity_contracts::{IdentityEventV1, PrincipalId, StaffRoleAssignedV1};
use serde_json::Value;
use sqlx::PgPool;
use staff_identity_domain::{Staff, StaffIdentityError};
use uuid::Uuid;

use super::is_unique_constraint_violation;

const MASTER_ADMIN: &str = "MASTER_ADMIN";
const ZITADEL_SUBJECT_CONSTRAINT: &str = "identity_staff_zitadel_subject_key";
const BOOTSTRAP_LOCK_SQL: &str =
    "SELECT pg_advisory_xact_lock(hashtext('identity.master_admin.bootstrap'))";
const MASTER_ADMIN_EXISTS_SQL: &str = "SELECT EXISTS (
         SELECT 1 FROM identity.staff WHERE primary_role_code = 'MASTER_ADMIN'
         UNION ALL
         SELECT 1 FROM identity.staff_role WHERE role_code = 'MASTER_ADMIN'
     )";
const INSERT_STAFF_SQL: &str = "INSERT INTO identity.staff
         (id, zitadel_subject, email, display_name, primary_role_code,
          created_at, updated_at, version)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
const INSERT_ROLE_SQL: &str =
    "INSERT INTO identity.staff_role (staff_id, role_code, granted_at, granted_by)
     VALUES ($1, $2, $3, $4)";
const INSERT_OUTBOX_SQL: &str =
    "INSERT INTO identity.outbox_event (event_id, type, payload, occurred_at)
     VALUES ($1, $2, $3, $4)";

/// `PostgreSQL` unit of work for creating the first Identity master administrator.
pub struct PgIdentityBootstrapUnitOfWork {
    pool: PgPool,
}

impl PgIdentityBootstrapUnitOfWork {
    /// Creates a bootstrap unit of work backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IdentityBootstrapUnitOfWork for PgIdentityBootstrapUnitOfWork {
    async fn master_admin_exists(&self) -> Result<bool, StaffIdentityError> {
        sqlx::query_scalar::<_, bool>(MASTER_ADMIN_EXISTS_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)
    }

    async fn create_first_master_admin(
        &self,
        staff: &Staff,
        role_grant: &RoleGrant,
    ) -> Result<(), StaffIdentityError> {
        validate_bootstrap(staff, role_grant)?;
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        sqlx::query(BOOTSTRAP_LOCK_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        if sqlx::query_scalar::<_, bool>(MASTER_ADMIN_EXISTS_SQL)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_sqlx)?
        {
            return Err(StaffIdentityError::Infrastructure(
                "MASTER_ADMIN already exists".to_owned(),
            ));
        }

        sqlx::query(INSERT_STAFF_SQL)
            .bind(staff.id.as_uuid())
            .bind(&staff.zitadel_subject)
            .bind(&staff.email)
            .bind(&staff.display_name)
            .bind(&staff.primary_role_code)
            .bind(staff.created_at)
            .bind(staff.updated_at)
            .bind(staff.version)
            .execute(&mut *transaction)
            .await
            .map_err(map_staff_insert_error)?;
        sqlx::query(INSERT_ROLE_SQL)
            .bind(role_grant.staff_id.as_uuid())
            .bind(role_grant.role_code.as_str())
            .bind(role_grant.granted_at)
            .bind(role_grant.granted_by.as_uuid())
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;

        let principal_id = PrincipalId::new(staff.id.as_uuid());
        let event = IdentityEventV1::StaffRoleAssigned(StaffRoleAssignedV1 {
            schema_version: 1,
            staff_id: principal_id,
            role_code: role_grant.role_code.as_str().to_owned(),
            assigned_at: role_grant.granted_at,
            assigned_by: principal_id,
        });
        let payload = serde_json::to_value(&event)
            .map_err(|error| StaffIdentityError::Infrastructure(error.to_string()))?;
        let event_type = event_type_tag_from_value(&payload)?.to_owned();
        sqlx::query(INSERT_OUTBOX_SQL)
            .bind(Uuid::now_v7())
            .bind(event_type)
            .bind(payload)
            .bind(role_grant.granted_at)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}

fn validate_bootstrap(staff: &Staff, role_grant: &RoleGrant) -> Result<(), StaffIdentityError> {
    if staff.primary_role_code != MASTER_ADMIN
        || role_grant.role_code.as_str() != MASTER_ADMIN
        || role_grant.staff_id != staff.id
        || role_grant.granted_by != staff.id
    {
        return Err(StaffIdentityError::Infrastructure(
            "invalid first MASTER_ADMIN self-grant".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
const fn bootstrap_sql() -> [&'static str; 3] {
    [INSERT_STAFF_SQL, INSERT_ROLE_SQL, INSERT_OUTBOX_SQL]
}

#[cfg(test)]
fn event_type_tag(event: &IdentityEventV1) -> Result<String, StaffIdentityError> {
    let payload = serde_json::to_value(event)
        .map_err(|error| StaffIdentityError::Infrastructure(error.to_string()))?;
    event_type_tag_from_value(&payload).map(ToOwned::to_owned)
}

fn event_type_tag_from_value(payload: &Value) -> Result<&str, StaffIdentityError> {
    payload
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| StaffIdentityError::Infrastructure("event type is missing".to_owned()))
}

fn map_staff_insert_error(error: sqlx::Error) -> StaffIdentityError {
    if is_unique_constraint_violation(&error, ZITADEL_SUBJECT_CONSTRAINT) {
        return StaffIdentityError::DuplicateZitadelSubject;
    }
    map_sqlx(error)
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> StaffIdentityError {
    StaffIdentityError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{bootstrap_sql, event_type_tag, map_staff_insert_error, validate_bootstrap};
    use authorization_domain::{RoleCode, RoleGrant};
    use chrono::{TimeZone, Utc};
    use identity_contracts::{IdentityEventV1, PrincipalId, StaffRoleAssignedV1};
    use identity_shared_kernel::StaffId;
    use staff_identity_domain::Staff;
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn bootstrap_plan_targets_staff_role_and_outbox_in_identity_schema() {
        let statements = bootstrap_sql();

        assert_eq!(statements.len(), 3);
        assert!(statements[0].contains("INSERT INTO identity.staff"));
        assert!(statements[1].contains("INSERT INTO identity.staff_role"));
        assert!(statements[2].contains("INSERT INTO identity.outbox_event"));
    }

    #[test]
    fn bootstrap_outbox_type_uses_identity_v1_contract() -> Result<(), Box<dyn Error>> {
        let principal_id = PrincipalId::new(Uuid::nil());
        let assigned_at = Utc.timestamp_opt(1_700_000_000, 0).single().ok_or("time")?;
        let event = IdentityEventV1::StaffRoleAssigned(StaffRoleAssignedV1 {
            schema_version: 1,
            staff_id: principal_id,
            role_code: "MASTER_ADMIN".to_owned(),
            assigned_at,
            assigned_by: principal_id,
        });

        assert_eq!(event_type_tag(&event)?, "identity.staff.role_assigned.v1");
        Ok(())
    }

    #[test]
    fn bootstrap_requires_a_self_granted_master_admin() -> Result<(), Box<dyn Error>> {
        let staff_id = StaffId::new(Uuid::nil());
        let other_id = StaffId::new(Uuid::from_u128(1));
        let now = Utc.timestamp_opt(1_700_000_000, 0).single().ok_or("time")?;
        let staff = Staff {
            id: staff_id,
            zitadel_subject: "subject".to_owned(),
            email: "staff@example.test".to_owned(),
            display_name: "Staff".to_owned(),
            primary_role_code: "MASTER_ADMIN".to_owned(),
            created_at: now,
            updated_at: now,
            version: 1,
        };
        let valid = RoleGrant {
            staff_id,
            role_code: RoleCode::parse("MASTER_ADMIN")?,
            granted_at: now,
            granted_by: staff_id,
        };
        assert!(validate_bootstrap(&staff, &valid).is_ok());

        let invalid = RoleGrant {
            granted_by: other_id,
            ..valid
        };
        assert!(validate_bootstrap(&staff, &invalid).is_err());
        Ok(())
    }

    #[test]
    fn maps_only_the_named_zitadel_subject_constraint_to_subject_conflict() {
        let duplicate_subject = map_staff_insert_error(super::super::test_database_error(
            "23505",
            "identity_staff_zitadel_subject_key",
        ));
        let other_unique = map_staff_insert_error(super::super::test_database_error(
            "23505",
            "identity_staff_email_key",
        ));
        let wrong_sqlstate = map_staff_insert_error(super::super::test_database_error(
            "23503",
            "identity_staff_zitadel_subject_key",
        ));

        assert!(matches!(
            duplicate_subject,
            staff_identity_domain::StaffIdentityError::DuplicateZitadelSubject
        ));
        assert!(matches!(
            other_unique,
            staff_identity_domain::StaffIdentityError::Infrastructure(_)
        ));
        assert!(matches!(
            wrong_sqlstate,
            staff_identity_domain::StaffIdentityError::Infrastructure(_)
        ));
    }
}
