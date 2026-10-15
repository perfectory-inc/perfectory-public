//! Published Identity v1 event payloads.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::PrincipalId;

/// Union of Identity events published through the transactional outbox.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum IdentityEventV1 {
    /// A staff account was invited.
    #[serde(rename = "identity.staff.invited.v1")]
    StaffInvited(StaffInvitedV1),
    /// A role was assigned to a staff account.
    #[serde(rename = "identity.staff.role_assigned.v1")]
    StaffRoleAssigned(StaffRoleAssignedV1),
    /// A staff session was revoked.
    #[serde(rename = "identity.staff.session_revoked.v1")]
    StaffSessionRevoked(StaffSessionRevokedV1),
    /// A capability was granted to a service principal by the workload policy.
    #[serde(rename = "identity.service_principal.capability_granted.v1")]
    ServiceCapabilityGranted(ServiceCapabilityChangedV1),
    /// A capability was revoked from a service principal by the workload policy.
    #[serde(rename = "identity.service_principal.capability_revoked.v1")]
    ServiceCapabilityRevoked(ServiceCapabilityChangedV1),
}

/// Event emitted when a staff account is invited.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StaffInvitedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Staff principal that was invited.
    pub staff_id: PrincipalId,
    /// Invited email address.
    pub email: String,
    /// UTC timestamp when the invitation was created.
    pub invited_at: DateTime<Utc>,
    /// Principal that created the invitation.
    pub invited_by: PrincipalId,
}

/// Event emitted when a staff role is assigned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StaffRoleAssignedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Staff principal that received the role.
    pub staff_id: PrincipalId,
    /// Assigned role code.
    pub role_code: String,
    /// UTC timestamp when the role was assigned.
    pub assigned_at: DateTime<Utc>,
    /// Principal that assigned the role.
    pub assigned_by: PrincipalId,
}

/// Event emitted when a staff session is revoked.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StaffSessionRevokedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Staff principal that owned the session.
    pub staff_id: PrincipalId,
    /// JWT ID used by consumers for denylist matching.
    pub jti: String,
    /// UTC timestamp when the session was revoked.
    pub revoked_at: DateTime<Utc>,
    /// Revoke reason. Wire values are `logout`, `admin_revoke`, `role_changed`, or `security`.
    pub reason: String,
}

/// Event emitted when the workload policy grants or revokes one service capability.
///
/// The event type says which; the payload is the same appended grant record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ServiceCapabilityChangedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Service principal whose capability changed.
    pub service_principal_id: PrincipalId,
    /// Exact `resource:action` capability.
    pub capability: String,
    /// UTC timestamp when the change was recorded.
    pub changed_at: DateTime<Utc>,
    /// Lowercase hex SHA-256 of the compiled workload policy that caused the change.
    pub policy_sha256: String,
}
