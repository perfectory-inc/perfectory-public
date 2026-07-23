//! Pure role-based policy evaluation.

use identity_contracts::ResourceAction;

use crate::RoleCode;

const STAFF_ROLE_RESOURCE: &str = "identity.staff_role";
const STAFF_ROLE_GRANT_ACTION: &str = "grant";
const MASTER_ADMIN: &str = "MASTER_ADMIN";

/// Input required to evaluate an authorization policy decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyInput {
    roles: Vec<RoleCode>,
    resource: String,
    action: String,
}

impl PolicyInput {
    /// Builds input for evaluating whether the effective roles may grant staff roles.
    #[must_use]
    pub fn staff_role_grant(roles: Vec<RoleCode>) -> Self {
        Self::resource_action(roles, STAFF_ROLE_RESOURCE, STAFF_ROLE_GRANT_ACTION)
    }

    /// Builds input for evaluating a resource action.
    #[must_use]
    pub fn resource_action(
        roles: Vec<RoleCode>,
        resource: impl Into<String>,
        action: impl Into<String>,
    ) -> Self {
        Self {
            roles,
            resource: resource.into(),
            action: action.into(),
        }
    }
}

/// Authorization result with a stable machine-readable reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyDecision {
    decision: ResourceAction,
    reason_code: &'static str,
}

impl PolicyDecision {
    /// Builds an allowed policy decision.
    #[must_use]
    pub const fn allow(reason_code: &'static str) -> Self {
        Self {
            decision: ResourceAction::Allow,
            reason_code,
        }
    }

    /// Builds a denied policy decision.
    #[must_use]
    pub const fn deny(reason_code: &'static str) -> Self {
        Self {
            decision: ResourceAction::Deny,
            reason_code,
        }
    }

    /// Returns the published allow or deny decision.
    #[must_use]
    pub const fn decision(&self) -> &ResourceAction {
        &self.decision
    }

    /// Returns the stable machine-readable reason code.
    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        self.reason_code
    }

    /// Returns whether the requested resource action is allowed.
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        self.decision == ResourceAction::Allow
    }
}

/// Evaluates the supplied staff roles against the requested action.
#[must_use]
pub fn evaluate_policy(input: &PolicyInput) -> PolicyDecision {
    if input.resource == STAFF_ROLE_RESOURCE && input.action == STAFF_ROLE_GRANT_ACTION {
        return if has_role(&input.roles, MASTER_ADMIN) {
            PolicyDecision::allow("role_grant")
        } else {
            PolicyDecision::deny("missing_master_admin")
        };
    }

    let allowed = input.roles.iter().any(|role| {
        matches!(
            (
                role.as_str(),
                input.resource.as_str(),
                input.action.as_str()
            ),
            (MASTER_ADMIN, _, _)
                | ("CATALOG_ADMIN", "foundation.catalog", "write")
                | (
                    "CATALOG_ADMIN" | "LAKEHOUSE_ADMIN",
                    "foundation.lakehouse",
                    "batch_audit"
                )
                | (
                    "VECTOR_TILE_ADMIN",
                    "foundation.spatial",
                    "manifest_admin" | "anchor_rebuild"
                )
        )
    });

    if allowed {
        PolicyDecision::allow("role_grant")
    } else {
        PolicyDecision::deny("missing_capability")
    }
}

fn has_role(roles: &[RoleCode], expected: &str) -> bool {
    roles.iter().any(|role| role.as_str() == expected)
}
