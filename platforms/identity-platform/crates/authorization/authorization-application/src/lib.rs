//! Authorization application use cases and their focused outbound ports.

#![deny(missing_docs)]

/// Use case for assigning staff roles from verified actor context.
pub mod assign_role;
/// One-time first `MASTER_ADMIN` bootstrap use case.
pub mod bootstrap_master_admin;
/// Use case for evaluating resource access from verified roles.
pub mod evaluate_access;
/// Use case for linking a source's account to a staff member.
pub mod link_external_account;
/// Focused mutation ports for authorization use cases.
pub mod ports;

pub use assign_role::{
    AssignStaffRole, AssignStaffRoleError, AssignStaffRoleInput, AssignStaffRoleOutput,
};
pub use bootstrap_master_admin::{
    BootstrapMasterAdmin, BootstrapMasterAdminInput, BootstrapMasterAdminOutcome,
};
pub use evaluate_access::{EvaluateAccess, EvaluateAccessInput, EvaluateAccessOutput};
pub use link_external_account::{
    ExternalAccountLinkChange, LinkStaffExternalAccount, LinkStaffExternalAccountError,
    LinkStaffExternalAccountInput,
};
