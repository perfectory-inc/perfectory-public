//! Row mapping for staff identity persistence.

use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;
use sqlx::postgres::PgRow;
use sqlx::Row;
use staff_identity_domain::Staff;
use uuid::Uuid;

pub struct StaffRow {
    pub id: Uuid,
    pub zitadel_subject: String,
    pub email: String,
    pub display_name: String,
    pub primary_role_code: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i64,
}

pub fn map_staff_row(row: StaffRow) -> Staff {
    Staff {
        id: StaffId::new(row.id),
        zitadel_subject: row.zitadel_subject,
        email: row.email,
        display_name: row.display_name,
        primary_role_code: row.primary_role_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
        version: row.version,
    }
}

pub fn row_to_staff(row: &PgRow) -> Result<Staff, sqlx::Error> {
    Ok(map_staff_row(StaffRow {
        id: row.try_get("id")?,
        zitadel_subject: row.try_get("zitadel_subject")?,
        email: row.try_get("email")?,
        display_name: row.try_get("display_name")?,
        primary_role_code: row.try_get("primary_role_code")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        version: row.try_get("version")?,
    }))
}

#[allow(clippy::needless_pass_by_value)]
pub fn map_sqlx(error: sqlx::Error) -> staff_identity_domain::StaffIdentityError {
    staff_identity_domain::StaffIdentityError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{map_staff_row, StaffRow};
    use chrono::{TimeZone, Utc};
    use identity_shared_kernel::StaffId;
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn maps_staff_row_values_into_the_domain_model() -> Result<(), Box<dyn Error>> {
        let created_at = Utc.timestamp_opt(1_700_000_000, 0).single().ok_or("time")?;
        let updated_at = Utc.timestamp_opt(1_700_000_100, 0).single().ok_or("time")?;
        let id = Uuid::parse_str("018f30c0-7b5a-7cc0-8c9d-1f3d12f85350")?;

        let staff = map_staff_row(StaffRow {
            id,
            zitadel_subject: "staff-subject".to_owned(),
            email: "staff@example.test".to_owned(),
            display_name: "Staff".to_owned(),
            primary_role_code: "MASTER_ADMIN".to_owned(),
            created_at,
            updated_at,
            version: 7,
        });

        assert_eq!(staff.id, StaffId::new(id));
        assert_eq!(staff.zitadel_subject, "staff-subject");
        assert_eq!(staff.email, "staff@example.test");
        assert_eq!(staff.display_name, "Staff");
        assert_eq!(staff.primary_role_code, "MASTER_ADMIN");
        assert_eq!(staff.created_at, created_at);
        assert_eq!(staff.updated_at, updated_at);
        assert_eq!(staff.version, 7);
        Ok(())
    }
}
