//! Zitadel and `PostgreSQL` adapters for service identity.

/// PostgreSQL service-principal and capability reader.
pub mod postgres;
/// Zitadel machine-token verifier.
pub mod zitadel_service_token_verifier;

pub use postgres::{PgServicePrincipalCapabilityReader, ServicePrincipalReader};
pub use zitadel_service_token_verifier::ZitadelMachineTokenVerifier;

use async_trait::async_trait;
use service_identity_application::ports::{IdentityAuditSink, ServiceAuthorizationAudit};
use service_identity_domain::ServiceIdentityError;

/// Credential-free tracing sink for service authorization audit decisions.
pub struct TracingIdentityAuditSink;

#[async_trait]
impl IdentityAuditSink for TracingIdentityAuditSink {
    async fn record_service_authorization(
        &self,
        audit: &ServiceAuthorizationAudit,
    ) -> Result<(), ServiceIdentityError> {
        tracing::info!(
            principal_id = %audit.principal_id.as_uuid(),
            resource = audit.resource,
            action = audit.action,
            resource_id = audit.resource_id,
            decision_allowed = audit.decision.is_allowed(),
            reason_code = audit.decision.reason_code(),
            trace_id = audit.trace_id,
            evaluated_at = %audit.evaluated_at,
            "service authorization evaluated"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TracingIdentityAuditSink;
    use authorization_domain::PolicyDecision;
    use chrono::{TimeZone, Utc};
    use identity_contracts::PrincipalId;
    use service_identity_application::ports::{IdentityAuditSink, ServiceAuthorizationAudit};
    use std::error::Error;
    use uuid::Uuid;

    #[tokio::test]
    async fn tracing_audit_sink_accepts_credential_free_decision_metadata(
    ) -> Result<(), Box<dyn Error>> {
        let sink = TracingIdentityAuditSink;
        let evaluated_at = Utc.timestamp_opt(1_700_000_000, 0).single().ok_or("time")?;
        sink.record_service_authorization(&ServiceAuthorizationAudit {
            principal_id: PrincipalId::new(Uuid::nil()),
            resource: "catalog".to_owned(),
            action: "read".to_owned(),
            resource_id: None,
            decision: PolicyDecision::allow("service_capability"),
            trace_id: "trace-1".to_owned(),
            evaluated_at,
        })
        .await?;
        Ok(())
    }
}
