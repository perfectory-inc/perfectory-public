//! Pure service capability evaluation.

use authorization_domain::{Permission, PolicyDecision};

use crate::ValidatedServicePrincipal;

/// Metadata for a service call whose principal has already been validated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCallMetadata {
    /// Capability or resource namespace being called.
    pub resource: String,
    /// Requested action within the resource namespace.
    pub action: String,
    /// Optional resource instance identifier.
    pub resource_id: Option<String>,
    /// Correlation identifier for audit and telemetry.
    pub trace_id: String,
}

/// Evaluates a validated service principal's exact capability for a call.
#[must_use]
pub fn evaluate_service_call(
    principal: &ValidatedServicePrincipal,
    call: &ServiceCallMetadata,
) -> PolicyDecision {
    let required = format!("{}:{}", call.resource, call.action);
    if principal
        .capabilities
        .iter()
        .map(Permission::as_str)
        .any(|capability| capability == required)
    {
        PolicyDecision::allow("service_capability")
    } else {
        PolicyDecision::deny("missing_service_capability")
    }
}
