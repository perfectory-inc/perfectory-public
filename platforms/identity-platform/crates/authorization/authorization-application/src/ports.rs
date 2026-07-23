//! Focused mutation ports for authorization use cases.

use std::error::Error;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use authorization_domain::{RoleCode, RoleGrant};
use identity_shared_kernel::StaffId;
use staff_identity_domain::{Staff, StaffIdentityError};

/// Persistence failures specific to assigning a staff role.
#[derive(Debug)]
pub enum RoleGrantPersistenceError {
    /// The target staff account does not exist.
    StaffNotFound(String),
    /// The staff account already has the requested role.
    DuplicateRole,
    /// The role grant or its outbox event could not be persisted.
    Infrastructure(String),
}

impl Display for RoleGrantPersistenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaffNotFound(staff_id) => write!(formatter, "staff not found (id={staff_id})"),
            Self::DuplicateRole => formatter.write_str("role already assigned"),
            Self::Infrastructure(message) => {
                write!(formatter, "role grant infrastructure error: {message}")
            }
        }
    }
}

impl Error for RoleGrantPersistenceError {}

/// Atomically records an ordinary role grant and its outbox event.
#[async_trait]
pub trait RoleGrantUnitOfWork: Send + Sync {
    /// Assigns `role_code` to `staff_id` using the trusted actor as grantor.
    ///
    /// # Errors
    /// Returns [`RoleGrantPersistenceError`] when the target is absent, the grant conflicts, or
    /// the atomic role/outbox transaction fails.
    async fn assign_role(
        &self,
        staff_id: StaffId,
        role_code: &RoleCode,
        granted_by: StaffId,
    ) -> Result<RoleGrant, RoleGrantPersistenceError>;
}

/// Dedicated transaction boundary for creating the first Identity administrator.
#[async_trait]
pub trait IdentityBootstrapUnitOfWork: Send + Sync {
    /// Returns whether any staff account already holds `MASTER_ADMIN`.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when the existence check fails.
    async fn master_admin_exists(&self) -> Result<bool, StaffIdentityError>;

    /// Atomically creates the first staff account, self-grant, and matching outbox event.
    ///
    /// # Errors
    /// Returns [`StaffIdentityError`] when another administrator wins the race or any write in
    /// the atomic transaction fails.
    async fn create_first_master_admin(
        &self,
        staff: &Staff,
        role_grant: &RoleGrant,
    ) -> Result<(), StaffIdentityError>;
}
