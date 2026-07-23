//! Effective staff-role reader.

use async_trait::async_trait;
use authorization_domain::RoleCode;
use identity_shared_kernel::StaffId;
use sqlx::PgPool;
use staff_identity_application::ports::EffectiveRoleReader;
use staff_identity_domain::StaffIdentityError;

const READ_EFFECTIVE_ROLES_SQL: &str = "SELECT role_code
    FROM (
        SELECT primary_role_code AS role_code
        FROM identity.staff
        WHERE id = $1
        UNION ALL
        SELECT role_code
        FROM identity.staff_role
        WHERE staff_id = $1
    ) AS effective_roles
    ORDER BY role_code";

/// `PostgreSQL` reader for a staff account's effective Identity roles.
pub struct PgEffectiveRoleReader {
    pool: PgPool,
}

impl PgEffectiveRoleReader {
    /// Creates a role reader backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EffectiveRoleReader for PgEffectiveRoleReader {
    async fn read_effective_roles(
        &self,
        staff_id: StaffId,
    ) -> Result<Vec<RoleCode>, StaffIdentityError> {
        let role_codes = sqlx::query_scalar::<_, String>(READ_EFFECTIVE_ROLES_SQL)
            .bind(staff_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        map_role_codes(role_codes)
    }
}

fn map_role_codes(role_codes: Vec<String>) -> Result<Vec<RoleCode>, StaffIdentityError> {
    let mut roles = role_codes
        .into_iter()
        .map(|role_code| {
            RoleCode::parse(role_code)
                .map_err(|error| StaffIdentityError::Infrastructure(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    roles.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    roles.dedup_by(|left, right| left.as_str() == right.as_str());
    Ok(roles)
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> StaffIdentityError {
    StaffIdentityError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{map_role_codes, READ_EFFECTIVE_ROLES_SQL};
    use staff_identity_domain::StaffIdentityError;

    #[test]
    fn effective_role_sql_reads_primary_and_granted_roles() {
        assert!(READ_EFFECTIVE_ROLES_SQL.contains("identity.staff"));
        assert!(READ_EFFECTIVE_ROLES_SQL.contains("primary_role_code"));
        assert!(READ_EFFECTIVE_ROLES_SQL.contains("identity.staff_role"));
        assert!(READ_EFFECTIVE_ROLES_SQL.contains("UNION ALL"));
    }

    #[test]
    fn maps_a_primary_only_role() -> Result<(), StaffIdentityError> {
        let roles = map_role_codes(vec!["MASTER_ADMIN".to_owned()])?;

        assert_eq!(role_names(&roles), vec!["MASTER_ADMIN"]);
        Ok(())
    }

    #[test]
    fn maps_a_grant_only_role() -> Result<(), StaffIdentityError> {
        let roles = map_role_codes(vec!["CATALOG_ADMIN".to_owned()])?;

        assert_eq!(role_names(&roles), vec!["CATALOG_ADMIN"]);
        Ok(())
    }

    #[test]
    fn validates_sorts_and_deduplicates_primary_and_granted_roles() -> Result<(), StaffIdentityError>
    {
        let roles = map_role_codes(vec![
            "MASTER_ADMIN".to_owned(),
            "CATALOG_ADMIN".to_owned(),
            "MASTER_ADMIN".to_owned(),
        ])?;

        assert_eq!(role_names(&roles), vec!["CATALOG_ADMIN", "MASTER_ADMIN"]);
        Ok(())
    }

    #[test]
    fn rejects_invalid_role_codes() {
        let invalid = map_role_codes(vec!["lowercase".to_owned()]);

        assert!(matches!(
            invalid,
            Err(StaffIdentityError::Infrastructure(_))
        ));
    }

    fn role_names(roles: &[authorization_domain::RoleCode]) -> Vec<&str> {
        roles
            .iter()
            .map(authorization_domain::RoleCode::as_str)
            .collect()
    }
}
