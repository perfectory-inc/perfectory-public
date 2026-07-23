//! Resource access evaluation from already verified staff roles.

use authorization_domain::{evaluate_policy, PolicyDecision, PolicyInput, RoleCode};
use chrono::{DateTime, Utc};
use identity_contracts::PrincipalId;
use staff_identity_domain::StaffIdentityError;

/// Input required to evaluate access for a verified staff principal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvaluateAccessInput {
    /// Trusted principal identifier returned by staff session verification.
    pub principal_id: PrincipalId,
    /// Trusted effective roles returned by staff session verification.
    pub roles: Vec<RoleCode>,
    /// Capability namespace being evaluated.
    pub resource: String,
    /// Requested action within the capability namespace.
    pub action: String,
    /// Optional resource instance identifier for instance-scoped decisions.
    pub resource_id: Option<String>,
    /// Correlation identifier carried through audit and telemetry boundaries.
    pub trace_id: String,
}

/// Outcome of an authorization policy evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvaluateAccessOutput {
    /// Trusted principal whose access was evaluated.
    pub principal_id: PrincipalId,
    /// Pure authorization-domain decision.
    pub decision: PolicyDecision,
    /// Optional resource instance identifier from the request.
    pub resource_id: Option<String>,
    /// Correlation identifier from the request.
    pub trace_id: String,
    /// UTC timestamp when Identity evaluated the policy.
    pub evaluated_at: DateTime<Utc>,
}

/// Evaluates resource access without authentication or persistence dependencies.
#[derive(Clone, Copy, Debug, Default)]
pub struct EvaluateAccess;

impl EvaluateAccess {
    /// Creates a stateless access evaluator.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Evaluates the supplied trusted roles against the requested resource action.
    ///
    /// # Errors
    /// This pure use case currently has no failure path; the result shape leaves composition
    /// roots free to use a uniform asynchronous command interface.
    #[allow(
        clippy::unused_async,
        reason = "the application command interface is asynchronous by contract"
    )]
    pub async fn execute(
        &self,
        input: EvaluateAccessInput,
    ) -> Result<EvaluateAccessOutput, StaffIdentityError> {
        let decision = evaluate_policy(&PolicyInput::resource_action(
            input.roles,
            input.resource,
            input.action,
        ));

        Ok(EvaluateAccessOutput {
            principal_id: input.principal_id,
            decision,
            resource_id: input.resource_id,
            trace_id: input.trace_id,
            evaluated_at: Utc::now(),
        })
    }
}
