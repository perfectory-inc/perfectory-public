//! Staff role assignment from already verified actor context.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use authorization_domain::{evaluate_policy, PolicyInput, RoleCode, RoleGrant};
use identity_contracts::PrincipalId;
use identity_shared_kernel::StaffId;

use crate::ports::{RoleGrantPersistenceError, RoleGrantUnitOfWork};

/// Input required to assign a role using trusted actor context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignStaffRoleInput {
    /// Trusted public identifier of the actor returned by session verification.
    pub actor_principal_id: PrincipalId,
    /// Trusted internal staff identifier used as `granted_by`.
    pub actor_staff_id: StaffId,
    /// Trusted effective roles used to authorize the grant.
    pub actor_roles: Vec<RoleCode>,
    /// Staff account that should receive the role.
    pub target_staff_id: StaffId,
    /// Validated role code to grant.
    pub role_code: RoleCode,
    /// Correlation identifier for the mutation.
    pub trace_id: String,
}

/// Successful role assignment and its trusted actor metadata.
#[derive(Debug)]
pub struct AssignStaffRoleOutput {
    /// Trusted public actor identifier.
    pub actor_principal_id: PrincipalId,
    /// Persisted role grant.
    pub grant: RoleGrant,
    /// Correlation identifier from the command.
    pub trace_id: String,
}

/// Error returned while assigning a staff role.
#[derive(Debug)]
pub enum AssignStaffRoleError {
    /// The verified actor lacks the role-grant capability.
    PermissionDenied(&'static str),
    /// The target staff account already has the requested role.
    DuplicateRole,
    /// Role or outbox persistence failed.
    Persistence(RoleGrantPersistenceError),
}

impl Display for AssignStaffRoleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PermissionDenied(reason_code) => {
                write!(formatter, "permission denied: {reason_code}")
            }
            Self::DuplicateRole => formatter.write_str("role already assigned"),
            Self::Persistence(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for AssignStaffRoleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PermissionDenied(_) | Self::DuplicateRole => None,
            Self::Persistence(error) => Some(error),
        }
    }
}

impl From<RoleGrantPersistenceError> for AssignStaffRoleError {
    fn from(error: RoleGrantPersistenceError) -> Self {
        match error {
            RoleGrantPersistenceError::DuplicateRole => Self::DuplicateRole,
            other => Self::Persistence(other),
        }
    }
}

/// Authorizes and records an ordinary staff role assignment.
pub struct AssignStaffRole {
    role_grant_uow: Arc<dyn RoleGrantUnitOfWork>,
}

impl AssignStaffRole {
    /// Creates a role-assignment use case backed only by its mutation port.
    #[must_use]
    pub fn new(role_grant_uow: Arc<dyn RoleGrantUnitOfWork>) -> Self {
        Self { role_grant_uow }
    }

    /// Applies the role-grant policy to trusted roles and records the authorized grant.
    ///
    /// # Errors
    /// Returns [`AssignStaffRoleError::PermissionDenied`] when the trusted actor lacks
    /// `MASTER_ADMIN`, [`AssignStaffRoleError::DuplicateRole`] when the role already exists, or
    /// [`AssignStaffRoleError::Persistence`] when another atomic write fails.
    pub async fn execute(
        &self,
        input: AssignStaffRoleInput,
    ) -> Result<AssignStaffRoleOutput, AssignStaffRoleError> {
        let decision = evaluate_policy(&PolicyInput::staff_role_grant(input.actor_roles));
        if !decision.is_allowed() {
            return Err(AssignStaffRoleError::PermissionDenied(
                decision.reason_code(),
            ));
        }

        let grant = self
            .role_grant_uow
            .assign_role(
                input.target_staff_id,
                &input.role_code,
                input.actor_staff_id,
            )
            .await?;
        Ok(AssignStaffRoleOutput {
            actor_principal_id: input.actor_principal_id,
            grant,
            trace_id: input.trace_id,
        })
    }
}
