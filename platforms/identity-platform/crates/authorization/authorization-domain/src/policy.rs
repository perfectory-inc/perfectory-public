//! Pure role-based policy evaluation.

use identity_contracts::ResourceAction;

use crate::RoleCode;

const STAFF_ROLE_RESOURCE: &str = "identity.staff_role";
const STAFF_ROLE_GRANT_ACTION: &str = "grant";
const MASTER_ADMIN: &str = "MASTER_ADMIN";

/// One resource action a staff role is granted. `MASTER_ADMIN` holds every action and is not listed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaffRoleGrant {
    /// Role code holding the grant.
    pub role: &'static str,
    /// Resource namespace, e.g. `foundation.catalog`.
    pub resource: &'static str,
    /// Action within the resource.
    pub action: &'static str,
}

const fn grant(role: &'static str, resource: &'static str, action: &'static str) -> StaffRoleGrant {
    StaffRoleGrant {
        role,
        resource,
        action,
    }
}

/// The staff role matrix: the one list of which role may do which resource action.
///
/// Every `foundation.*` entry must be a staff policy a Foundation route asks for, and every such
/// policy must be granted here — `tests/route_policy_contract.rs` checks this list against
/// Foundation's `config/identity-route-policies.contract.json` (root ADR-0214).
pub const STAFF_ROLE_GRANTS: &[StaffRoleGrant] = &[
    grant("CATALOG_ADMIN", "foundation.catalog", "write"),
    grant("CATALOG_ADMIN", "foundation.lakehouse", "batch_audit"),
    grant("LAKEHOUSE_ADMIN", "foundation.lakehouse", "batch_audit"),
    grant("VECTOR_TILE_ADMIN", "foundation.spatial", "manifest_admin"),
    grant("VECTOR_TILE_ADMIN", "foundation.spatial", "anchor_rebuild"),
    grant("VECTOR_TILE_ADMIN", "foundation.spatial", "map_edit"),
    // Parcel-lineage stewardship (Foundation root ADR-0115): a steward decides review items; an
    // adjudicator also rules on decisions that need a second person.
    grant("LINEAGE_STEWARD", "foundation.lineage", "review"),
    grant("LINEAGE_ADJUDICATOR", "foundation.lineage", "review"),
    grant("LINEAGE_ADJUDICATOR", "foundation.lineage", "adjudicate"),
    // The data catalog (root ADR-0117, ADR-0119): finding data and reading its description,
    // lineage and runs. Every data role reads it; a reader-only role exists for staff who do
    // nothing else with data.
    grant("DATA_CATALOG_READER", "foundation.metadata", "read"),
    grant("CATALOG_ADMIN", "foundation.metadata", "read"),
    grant("LAKEHOUSE_ADMIN", "foundation.metadata", "read"),
    grant("VECTOR_TILE_ADMIN", "foundation.metadata", "read"),
    grant("LINEAGE_STEWARD", "foundation.metadata", "read"),
    grant("LINEAGE_ADJUDICATOR", "foundation.metadata", "read"),
];

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
        role.as_str() == MASTER_ADMIN
            || STAFF_ROLE_GRANTS.iter().any(|grant| {
                grant.role == role.as_str()
                    && grant.resource == input.resource
                    && grant.action == input.action
            })
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
