//! Versioned HTTP and event contracts published by Identity Platform.
//!
//! These types define the stable v1 boundary between Identity Platform and its consumers. They do
//! not depend on Identity implementation crates or legacy staff-identity code.

#![deny(missing_docs)]

/// Versioned Identity event delivery envelopes.
pub mod delivery;
/// Versioned Identity event contracts.
pub mod events;
/// Versioned Identity HTTP routes and DTOs.
pub mod http;

pub use delivery::IdentityEventDeliveryV1;
pub use events::{IdentityEventV1, StaffInvitedV1, StaffRoleAssignedV1, StaffSessionRevokedV1};
pub use http::{
    AssignStaffRoleRequest, PolicyDecisionRequest, PolicyDecisionResponse, PrincipalId,
    ResourceAction, StaffRoleResponse, VerifyStaffSessionResponse, ASSIGN_STAFF_ROLE_ROUTE,
    POLICY_DECISION_ROUTE, REVOKE_STAFF_SESSION_ROUTE, VERIFY_STAFF_SESSION_ROUTE,
};
