//! One-time first `MASTER_ADMIN` bootstrap use case.

use std::sync::Arc;

use authorization_domain::{RoleCode, RoleGrant};
use chrono::Utc;
use identity_shared_kernel::StaffId;
use staff_identity_domain::{Staff, StaffIdentityError};

use crate::ports::IdentityBootstrapUnitOfWork;

const MASTER_ADMIN: &str = "MASTER_ADMIN";

/// Configuration-owned identity data for the first administrator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapMasterAdminInput {
    /// Verified identity-provider subject for the administrator.
    pub zitadel_subject: String,
    /// Administrator email address.
    pub email: String,
    /// Administrator display name.
    pub display_name: String,
}

/// Outcome of an idempotent first-administrator bootstrap attempt.
#[derive(Debug)]
pub enum BootstrapMasterAdminOutcome {
    /// A `MASTER_ADMIN` already exists and no mutation was attempted.
    AlreadyPresent,
    /// The first staff account and its self-granted role were atomically created.
    Created {
        /// Created staff account.
        staff: Box<Staff>,
        /// Created self-granted `MASTER_ADMIN` role.
        role_grant: RoleGrant,
    },
}

/// Idempotently creates the first Identity `MASTER_ADMIN`.
pub struct BootstrapMasterAdmin {
    bootstrap_uow: Arc<dyn IdentityBootstrapUnitOfWork>,
}

impl BootstrapMasterAdmin {
    /// Creates the bootstrap use case from its dedicated transaction port.
    #[must_use]
    pub fn new(bootstrap_uow: Arc<dyn IdentityBootstrapUnitOfWork>) -> Self {
        Self { bootstrap_uow }
    }

    /// Creates the first staff account, self-grant, and outbox event in one adapter transaction.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when the existence check, role construction, or atomic
    /// transaction fails.
    pub async fn execute(
        &self,
        input: BootstrapMasterAdminInput,
    ) -> Result<BootstrapMasterAdminOutcome, StaffIdentityError> {
        if self.bootstrap_uow.master_admin_exists().await? {
            return Ok(BootstrapMasterAdminOutcome::AlreadyPresent);
        }

        let now = Utc::now();
        let staff_id = StaffId::new(uuid::Uuid::now_v7());
        let role_code = RoleCode::parse(MASTER_ADMIN)
            .map_err(|error| StaffIdentityError::Infrastructure(error.to_string()))?;
        let staff = Staff {
            id: staff_id,
            zitadel_subject: input.zitadel_subject,
            email: input.email,
            display_name: input.display_name,
            primary_role_code: MASTER_ADMIN.to_owned(),
            created_at: now,
            updated_at: now,
            version: 1,
        };
        let role_grant = RoleGrant {
            staff_id,
            role_code,
            granted_at: now,
            granted_by: staff_id,
        };
        self.bootstrap_uow
            .create_first_master_admin(&staff, &role_grant)
            .await?;

        Ok(BootstrapMasterAdminOutcome::Created {
            staff: Box::new(staff),
            role_grant,
        })
    }
}
