use super::*;
use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationApplicationRecord, NormalizationProposalRecord,
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationRollbackCommand, NormalizationRollbackRecord, NormalizationUnitOfWork,
};
use foundation_normalization_domain::{
    NormalizationError, NormalizationProposalStatus, NormalizationReviewDecision,
    NormalizationTargetKind,
};

mod authorization;
mod error_contract;
mod openapi_contract;
mod submission;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NormalizationOperation {
    Submit,
    Review,
    Apply,
    Rollback,
}

struct ConfiguredNormalizationError {
    operation: NormalizationOperation,
    error: NormalizationError,
}

#[derive(Default)]
struct RecordingNormalizationUnitOfWork {
    commands: Mutex<Vec<NormalizationProposalSubmissionCommand>>,
    review_commands: Mutex<Vec<NormalizationProposalReviewCommand>>,
    apply_commands: Mutex<Vec<NormalizationApplicationCommand>>,
    rollback_commands: Mutex<Vec<NormalizationRollbackCommand>>,
    error: Mutex<Option<ConfiguredNormalizationError>>,
}

impl RecordingNormalizationUnitOfWork {
    fn failing(operation: NormalizationOperation, error: NormalizationError) -> Self {
        Self {
            commands: Mutex::default(),
            review_commands: Mutex::default(),
            apply_commands: Mutex::default(),
            rollback_commands: Mutex::default(),
            error: Mutex::new(Some(ConfiguredNormalizationError { operation, error })),
        }
    }

    async fn take_error(&self, operation: NormalizationOperation) -> Option<NormalizationError> {
        let mut configured = self.error.lock().await;
        if let Some(expected) = configured.as_ref() {
            assert_eq!(
                expected.operation, operation,
                "configured normalization error was consumed by the wrong operation"
            );
        }
        configured.take().map(|configured| configured.error)
    }

    async fn assert_only_review_invocation(
        &self,
        proposal_id: uuid::Uuid,
        decision: NormalizationReviewDecision,
        reason: &str,
    ) {
        let commands = self.review_commands.lock().await;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].proposal_id, proposal_id);
        assert_eq!(commands[0].decision, decision);
        assert_eq!(commands[0].reason, reason);
        drop(commands);
        assert!(self.apply_commands.lock().await.is_empty());
        assert!(self.rollback_commands.lock().await.is_empty());
        assert!(self.error.lock().await.is_none());
    }

    async fn assert_only_apply_invocation(&self, proposal_id: uuid::Uuid, expected_version: i64) {
        let commands = self.apply_commands.lock().await;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].proposal_id, proposal_id);
        assert_eq!(commands[0].expected_version, expected_version);
        drop(commands);
        assert!(self.review_commands.lock().await.is_empty());
        assert!(self.rollback_commands.lock().await.is_empty());
        assert!(self.error.lock().await.is_none());
    }

    async fn assert_only_rollback_invocation(
        &self,
        application_id: uuid::Uuid,
        expected_current_version: i64,
        reason: &str,
    ) {
        let commands = self.rollback_commands.lock().await;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].application_id, application_id);
        assert_eq!(
            commands[0].expected_current_version,
            expected_current_version
        );
        assert_eq!(commands[0].reason, reason);
        drop(commands);
        assert!(self.review_commands.lock().await.is_empty());
        assert!(self.apply_commands.lock().await.is_empty());
        assert!(self.error.lock().await.is_none());
    }

    async fn assert_no_admin_invocation(&self) {
        assert!(self.review_commands.lock().await.is_empty());
        assert!(self.apply_commands.lock().await.is_empty());
        assert!(self.rollback_commands.lock().await.is_empty());
    }
}

#[async_trait]
impl NormalizationUnitOfWork for RecordingNormalizationUnitOfWork {
    async fn submit_normalization_proposal(
        &self,
        command: NormalizationProposalSubmissionCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        self.commands.lock().await.push(command.clone());
        if let Some(error) = self.take_error(NormalizationOperation::Submit).await {
            return Err(error);
        }
        let record = NormalizationProposalRecord {
            id: command.id,
            proposal_key: command.proposal_key.clone(),
            status: command.status,
            created: true,
        };
        Ok(record)
    }

    async fn review_normalization_proposal(
        &self,
        command: NormalizationProposalReviewCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        self.review_commands.lock().await.push(command);
        if let Some(error) = self.take_error(NormalizationOperation::Review).await {
            return Err(error);
        }
        Err(NormalizationError::Persistence(
            "review is not used by normalization intake route tests".to_owned(),
        ))
    }

    async fn apply_normalization_proposal(
        &self,
        command: NormalizationApplicationCommand,
    ) -> Result<NormalizationApplicationRecord, NormalizationError> {
        self.apply_commands.lock().await.push(command.clone());
        if let Some(error) = self.take_error(NormalizationOperation::Apply).await {
            return Err(error);
        }
        Ok(NormalizationApplicationRecord {
            id: command.id,
            proposal_id: command.proposal_id,
            target_kind: NormalizationTargetKind::IndustrialComplex,
            target_id: Some(uuid::Uuid::now_v7()),
        })
    }

    async fn rollback_normalization_application(
        &self,
        command: NormalizationRollbackCommand,
    ) -> Result<NormalizationRollbackRecord, NormalizationError> {
        self.rollback_commands.lock().await.push(command.clone());
        if let Some(error) = self.take_error(NormalizationOperation::Rollback).await {
            return Err(error);
        }
        Ok(NormalizationRollbackRecord {
            id: command.id,
            rollback_of: command.application_id,
            target_kind: NormalizationTargetKind::IndustrialComplex,
            target_id: Some(uuid::Uuid::now_v7()),
        })
    }
}

struct StaffIdentityAuthorization;

#[async_trait]
impl IdentityAuthorization for StaffIdentityAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        _resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        if required_principal_kind == RequiredPrincipalKind::Staff
            && (resource, action) == ("foundation.catalog", "write")
        {
            return Ok(AuthorizedPrincipal {
                principal_id: uuid::Uuid::now_v7(),
                trace_id: trace_id.to_owned(),
            });
        }
        Err(IdentityAuthorizationError::Forbidden)
    }
}

fn staff_identity_authorization() -> Arc<dyn IdentityAuthorization> {
    Arc::new(StaffIdentityAuthorization)
}

fn normalization_review_request_body(reason: &str) -> serde_json::Value {
    serde_json::json!({"reason": reason})
}

fn normalization_apply_request_body(expected_version: i64) -> serde_json::Value {
    serde_json::json!({"expected_version": expected_version})
}

fn normalization_rollback_request_body(
    expected_current_version: i64,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "expected_current_version": expected_current_version,
        "reason": reason
    })
}

async fn authorized_admin_response(
    uri: &str,
    body: &serde_json::Value,
    normalization_uow: Arc<RecordingNormalizationUnitOfWork>,
) -> Result<axum::response::Response, Box<dyn Error>> {
    let state = Arc::new(
        AppState::bootstrap_for_test_with_normalization_uow_and_identity_authorization(
            normalization_uow,
            staff_identity_authorization(),
        )?,
    );
    let response = router(state)
        .oneshot(normalization_admin_request(uri, body)?)
        .await?;
    Ok(response)
}

async fn assert_exact_json_response(
    response: axum::response::Response,
    expected_status: StatusCode,
    expected_body: serde_json::Value,
) -> Result<String, Box<dyn Error>> {
    assert_eq!(response.status(), expected_status);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let expected = serde_json::to_vec(&expected_body)?;
    assert_eq!(body.as_ref(), expected.as_slice());
    Ok(String::from_utf8(body.to_vec())?)
}

async fn assert_opaque_internal_error_response(
    response: axum::response::Response,
    sensitive_detail: &str,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let body = String::from_utf8(body.to_vec())?;
    assert!(!body.contains(sensitive_detail));

    let payload: serde_json::Value = serde_json::from_str(&body)?;
    let object = payload
        .as_object()
        .ok_or("internal error response must be a JSON object")?;
    assert_eq!(object.len(), 2);
    assert_eq!(payload["error"], "internal server error");
    let correlation_id = payload["correlation_id"]
        .as_str()
        .ok_or("correlation_id must be a string")?;
    uuid::Uuid::parse_str(correlation_id)?;
    Ok(())
}
