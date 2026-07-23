//! `PostgreSQL` staff repository.

use async_trait::async_trait;
use sqlx::PgPool;
use staff_identity_application::ports::StaffRepository;
use staff_identity_domain::{Staff, StaffIdentityError};

use crate::row_map::{map_sqlx, row_to_staff};

const FIND_BY_SUBJECT_SQL: &str =
    "SELECT id, zitadel_subject, email, display_name, primary_role_code,
            created_at, updated_at, version
     FROM identity.staff
     WHERE zitadel_subject = $1";
const IS_JTI_REVOKED_SQL: &str =
    "SELECT EXISTS (SELECT 1 FROM identity.revoked_jti WHERE jti = $1)";

/// `PostgreSQL` implementation of staff identity reads.
pub struct PgStaffRepository {
    pool: PgPool,
}

impl PgStaffRepository {
    /// Creates a repository backed by the Identity database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StaffRepository for PgStaffRepository {
    async fn find_by_zitadel_subject(
        &self,
        subject: &str,
    ) -> Result<Option<Staff>, StaffIdentityError> {
        let row = sqlx::query(FIND_BY_SUBJECT_SQL)
            .bind(subject)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.as_ref().map(row_to_staff).transpose().map_err(map_sqlx)
    }

    async fn is_jti_revoked(&self, jti: &str) -> Result<bool, StaffIdentityError> {
        sqlx::query_scalar::<_, bool>(IS_JTI_REVOKED_SQL)
            .bind(jti)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)
    }
}

#[cfg(test)]
mod tests {
    use super::{FIND_BY_SUBJECT_SQL, IS_JTI_REVOKED_SQL};

    #[test]
    fn repository_queries_target_only_the_final_identity_schema() {
        assert!(FIND_BY_SUBJECT_SQL.contains("FROM identity.staff"));
        assert!(IS_JTI_REVOKED_SQL.contains("FROM identity.revoked_jti"));
    }
}
