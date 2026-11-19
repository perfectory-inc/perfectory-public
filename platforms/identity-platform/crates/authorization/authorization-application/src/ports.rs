//! Focused mutation ports for authorization use cases.

use std::error::Error;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use authorization_domain::{RoleCode, RoleGrant};
use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;
use staff_identity_domain::{ExternalAccount, ExternalAccountLink, Staff, StaffIdentityError};

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

/// Persistence failures specific to linking a source's account to a staff member.
#[derive(Debug)]
pub enum ExternalAccountLinkPersistenceError {
    /// The target staff account does not exist.
    StaffNotFound(String),
    /// The account already has a link that holds now (to this or another staff member).
    AlreadyLinked,
    /// The staff member has no link to the account that holds now.
    NotLinked,
    /// The link record or its outbox event could not be persisted.
    Infrastructure(String),
}

impl Display for ExternalAccountLinkPersistenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaffNotFound(staff_id) => write!(formatter, "staff not found (id={staff_id})"),
            Self::AlreadyLinked => formatter.write_str("external account is already linked"),
            Self::NotLinked => formatter.write_str("external account is not linked to this staff"),
            Self::Infrastructure(message) => {
                write!(
                    formatter,
                    "external account link infrastructure error: {message}"
                )
            }
        }
    }
}

impl Error for ExternalAccountLinkPersistenceError {}

/// Atomically appends an external-account link record and its outbox event (root ADR-0569).
#[async_trait]
pub trait ExternalAccountLinkUnitOfWork: Send + Sync {
    /// Appends a link of `account` to `staff_id` that holds from `valid_from`.
    ///
    /// # Errors
    /// Returns [`ExternalAccountLinkPersistenceError::AlreadyLinked`] when the account has a link
    /// that holds now, or another variant when the target is absent or the write fails.
    async fn link(
        &self,
        staff_id: StaffId,
        account: &ExternalAccount,
        valid_from: DateTime<Utc>,
        linked_by: StaffId,
    ) -> Result<ExternalAccountLink, ExternalAccountLinkPersistenceError>;

    /// Appends the end, now, of the link of `account` to `staff_id` that holds now.
    ///
    /// # Errors
    /// Returns [`ExternalAccountLinkPersistenceError::NotLinked`] when no such link holds, or
    /// another variant when the write fails.
    async fn unlink(
        &self,
        staff_id: StaffId,
        account: &ExternalAccount,
        unlinked_by: StaffId,
    ) -> Result<ExternalAccountLink, ExternalAccountLinkPersistenceError>;
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
