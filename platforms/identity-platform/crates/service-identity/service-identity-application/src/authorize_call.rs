//! Validated service-call authorization use case.

use std::sync::Arc;

use authorization_domain::PolicyDecision;
use chrono::{DateTime, Utc};
use identity_contracts::PrincipalId;
use service_identity_domain::{
    evaluate_service_call, ServiceCallMetadata, ServiceIdentityError, ValidatedServicePrincipal,
};

use crate::ports::{IdentityAuditSink, ServiceAuthorizationAudit};

/// A validated service principal and the call metadata to authorize.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizeServiceCallInput {
    /// Principal already validated through [`crate::ports::ServiceCredentialVerifier`].
    pub principal: ValidatedServicePrincipal,
    /// Call metadata derived by the composition root.
    pub call: ServiceCallMetadata,
}

/// Authorization result for a validated service principal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizeServiceCallOutput {
    /// Validated service principal whose call was evaluated.
    pub principal_id: PrincipalId,
    /// Pure service capability decision.
    pub decision: PolicyDecision,
    /// Optional resource instance identifier.
    pub resource_id: Option<String>,
    /// Correlation identifier supplied with the call.
    pub trace_id: String,
    /// UTC timestamp when Identity evaluated the call.
    pub evaluated_at: DateTime<Utc>,
}

/// Evaluates and audits a call made by an already validated service principal.
pub struct AuthorizeServiceCall {
    audit_sink: Arc<dyn IdentityAuditSink>,
}

impl AuthorizeServiceCall {
    /// Creates the use case from its audit sink.
    #[must_use]
    pub fn new(audit_sink: Arc<dyn IdentityAuditSink>) -> Self {
        Self { audit_sink }
    }

    /// Evaluates the service capability and records the resulting audit decision.
    ///
    /// # Errors
    /// Returns [`ServiceIdentityError`] when the audit sink cannot record the decision.
    pub async fn execute(
        &self,
        input: AuthorizeServiceCallInput,
    ) -> Result<AuthorizeServiceCallOutput, ServiceIdentityError> {
        let decision = evaluate_service_call(&input.principal, &input.call);
        let evaluated_at = Utc::now();
        let audit = ServiceAuthorizationAudit {
            principal_id: input.principal.principal_id,
            resource: input.call.resource,
            action: input.call.action,
            resource_id: input.call.resource_id.clone(),
            decision: decision.clone(),
            trace_id: input.call.trace_id.clone(),
            evaluated_at,
        };
        self.audit_sink.record_service_authorization(&audit).await?;

        Ok(AuthorizeServiceCallOutput {
            principal_id: input.principal.principal_id,
            decision,
            resource_id: input.call.resource_id,
            trace_id: input.call.trace_id,
            evaluated_at,
        })
    }
}
