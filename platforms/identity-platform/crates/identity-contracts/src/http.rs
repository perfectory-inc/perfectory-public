//! HTTP routes and DTOs for the published Identity v1 API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Route for verifying the staff principal represented by the bearer credential.
pub const VERIFY_STAFF_SESSION_ROUTE: &str = "/identity/v1/staff/sessions/verify";
/// Route for revoking the current staff session represented by the bearer credential.
pub const REVOKE_STAFF_SESSION_ROUTE: &str = "/identity/v1/staff/sessions/revoke";
/// Route for assigning a role to a staff principal.
pub const ASSIGN_STAFF_ROLE_ROUTE: &str = "/identity/v1/staff/{staff_id}/roles";
/// Route for linking or unlinking a source's account to a staff principal (root ADR-0569).
pub const STAFF_EXTERNAL_ACCOUNTS_ROUTE: &str = "/identity/v1/staff/{staff_id}/external-accounts";
/// Route for requesting an authorization policy decision.
pub const POLICY_DECISION_ROUTE: &str = "/identity/v1/policy/decisions";

/// Stable public identifier for an authenticated Identity principal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct PrincipalId(Uuid);

impl PrincipalId {
    /// Wraps a UUID in the public Identity principal identifier.
    #[must_use]
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the UUID represented by this principal identifier.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Authorization decision returned by Identity Platform.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceAction {
    /// The requested action is permitted.
    Allow,
    /// The requested action is not permitted.
    Deny,
}

/// Response returned after Identity verifies the bearer credential for a staff principal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VerifyStaffSessionResponse {
    /// Verified Identity principal.
    pub principal_id: PrincipalId,
    /// Staff email address supplied by the identity provider.
    pub email: String,
    /// Staff display name supplied by the identity provider.
    pub display_name: String,
    /// Effective role codes for the verified principal.
    pub roles: Vec<String>,
    /// UTC timestamp when the verified credential expires.
    pub expires_at: DateTime<Utc>,
}

/// Request body for assigning a role to a staff principal.
///
/// The actor credential is supplied only in the `Authorization: Bearer` header.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AssignStaffRoleRequest {
    /// Role code to grant to the staff principal named in the route.
    pub role_code: String,
}

/// Response describing a role assignment made by Identity Platform.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StaffRoleResponse {
    /// Staff principal that received the role.
    pub principal_id: PrincipalId,
    /// Granted role code.
    pub role_code: String,
    /// UTC timestamp when Identity recorded the grant.
    pub granted_at: DateTime<Utc>,
    /// Authenticated principal that granted the role.
    pub granted_by: PrincipalId,
}

/// Whether a request ties an account to the staff principal or ends the tie.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAccountChange {
    /// Append a link that holds from `valid_from` (or now).
    Link,
    /// Append the end of the link that holds now.
    Unlink,
}

/// Request body for linking or unlinking a source's account to the staff principal in the route.
///
/// The actor credential is supplied only in the `Authorization: Bearer` header. Records are only
/// appended; an unlink appends the same link with `valid_to` filled.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StaffExternalAccountRequest {
    /// `link` or `unlink`.
    pub change: ExternalAccountChange,
    /// Source of the account: `github` or `unix`.
    pub provider: String,
    /// The source's immutable id: a numeric GitHub user id, or `<uid>@<host>`. Never a login name.
    pub external_id: String,
    /// For `link`: when the link starts to hold, at most now. Defaults to now. Ignored by `unlink`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<DateTime<Utc>>,
}

/// The appended link record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StaffExternalAccountResponse {
    /// Staff principal the account belongs to.
    pub principal_id: PrincipalId,
    /// Source of the account.
    pub provider: String,
    /// The source's immutable id.
    pub external_id: String,
    /// UTC timestamp from which the link holds.
    pub valid_from: DateTime<Utc>,
    /// UTC timestamp at which the link stopped holding; absent while it holds.
    pub valid_to: Option<DateTime<Utc>>,
    /// Authenticated principal that recorded this record.
    pub linked_by: PrincipalId,
}

/// Request body for evaluating a resource action against the bearer principal.
///
/// The credential is supplied only in the `Authorization: Bearer` header.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PolicyDecisionRequest {
    /// Capability or resource namespace being evaluated.
    pub resource: String,
    /// Requested action within the resource namespace.
    pub action: String,
    /// Optional resource instance identifier for instance-scoped decisions.
    pub resource_id: Option<String>,
    /// Caller-provided correlation identifier for traceability.
    pub trace_id: String,
}

/// Authorization decision evaluated for the bearer principal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PolicyDecisionResponse {
    /// Principal whose authorization was evaluated.
    pub principal_id: PrincipalId,
    /// Allow or deny decision.
    pub decision: ResourceAction,
    /// Stable machine-readable reason for the decision.
    pub reason_code: String,
    /// UTC timestamp when Identity evaluated the request.
    pub evaluated_at: DateTime<Utc>,
}
