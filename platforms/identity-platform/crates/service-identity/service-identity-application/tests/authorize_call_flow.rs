//! Service call authorization accepts only validated principals.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use authorization_domain::Permission;
use identity_contracts::PrincipalId;
use service_identity_application::ports::{
    IdentityAuditSink, ServiceAuthorizationAudit, ServiceCredentialVerifier,
};
use service_identity_application::{AuthorizeServiceCall, AuthorizeServiceCallInput};
use service_identity_domain::{
    ServiceCallMetadata, ServiceIdentityError, ValidatedServicePrincipal,
};
use uuid::Uuid;

#[tokio::test]
async fn authorize_service_call_allows_matching_capability_and_audits_decision(
) -> Result<(), Box<dyn std::error::Error>> {
    let audit = Arc::new(RecordingAuditSink::default());
    let use_case = AuthorizeServiceCall::new(audit.clone());
    let principal_id = PrincipalId::new(Uuid::from_u128(7));

    let output = use_case
        .execute(AuthorizeServiceCallInput {
            principal: ValidatedServicePrincipal {
                principal_id,
                capabilities: vec![Permission::parse("foundation.catalog:write")?],
            },
            call: ServiceCallMetadata {
                resource: "foundation.catalog".to_owned(),
                action: "write".to_owned(),
                resource_id: Some("parcel-1".to_owned()),
                trace_id: "trace-service-1".to_owned(),
            },
        })
        .await?;

    assert!(output.decision.is_allowed());
    assert_eq!(output.principal_id, principal_id);
    let (recorded_principal_id, recorded_allowed, recorded_trace_id) = {
        let recorded_guard = audit
            .recorded
            .lock()
            .map_err(|_| "audit recording mutex poisoned")?;
        let Some(recorded) = recorded_guard.as_ref() else {
            return Err("authorization audit was not recorded".into());
        };
        let snapshot = (
            recorded.principal_id,
            recorded.decision.is_allowed(),
            recorded.trace_id.clone(),
        );
        drop(recorded_guard);
        snapshot
    };
    assert_eq!(recorded_principal_id, principal_id);
    assert!(recorded_allowed);
    assert_eq!(recorded_trace_id, "trace-service-1");
    Ok(())
}

#[tokio::test]
async fn authorize_service_call_denies_missing_capability() -> Result<(), Box<dyn std::error::Error>>
{
    let use_case = AuthorizeServiceCall::new(Arc::new(RecordingAuditSink::default()));

    let output = use_case
        .execute(AuthorizeServiceCallInput {
            principal: ValidatedServicePrincipal {
                principal_id: PrincipalId::new(Uuid::from_u128(8)),
                capabilities: vec![Permission::parse("foundation.catalog:read")?],
            },
            call: ServiceCallMetadata {
                resource: "foundation.catalog".to_owned(),
                action: "write".to_owned(),
                resource_id: None,
                trace_id: "trace-service-2".to_owned(),
            },
        })
        .await?;

    assert!(!output.decision.is_allowed());
    assert_eq!(output.decision.reason_code(), "missing_service_capability");
    Ok(())
}

const fn credential_verifier_is_a_separate_port<T: ServiceCredentialVerifier>() {}

#[test]
fn service_credential_verifier_port_returns_validated_principals() {
    credential_verifier_is_a_separate_port::<FakeCredentialVerifier>();
}

#[derive(Default)]
struct RecordingAuditSink {
    recorded: Mutex<Option<ServiceAuthorizationAudit>>,
}

#[async_trait]
impl IdentityAuditSink for RecordingAuditSink {
    async fn record_service_authorization(
        &self,
        audit: &ServiceAuthorizationAudit,
    ) -> Result<(), ServiceIdentityError> {
        let mut recorded = self.recorded.lock().map_err(|_| {
            ServiceIdentityError::Infrastructure("recording mutex poisoned".to_owned())
        })?;
        *recorded = Some(audit.clone());
        drop(recorded);
        Ok(())
    }
}

struct FakeCredentialVerifier;

#[async_trait]
impl ServiceCredentialVerifier for FakeCredentialVerifier {
    async fn verify_credential(
        &self,
        _bearer_token: &str,
    ) -> Result<ValidatedServicePrincipal, ServiceIdentityError> {
        Ok(ValidatedServicePrincipal {
            principal_id: PrincipalId::new(Uuid::nil()),
            capabilities: Vec::new(),
        })
    }
}
