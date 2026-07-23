//! Staff and service policy-decision composition.

use std::sync::Arc;

use authorization_application::EvaluateAccessInput;
use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use identity_contracts::{PolicyDecisionRequest, PolicyDecisionResponse};
use serde::Deserialize;
use service_identity_application::AuthorizeServiceCallInput;
use service_identity_domain::{ServiceCallMetadata, ServiceIdentityError};
use staff_identity_application::{VerifyStaffSessionInput, VerifyStaffSessionOutput};

use crate::error::{ApiError, ApiErrorResponse};
use crate::state::AppState;

use super::staff::{bearer_credential, map_staff_auth_error};

#[utoipa::path(
    post,
    path = "/identity/v1/policy/decisions",
    request_body = PolicyDecisionRequest,
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Typed allow or deny decision", body = PolicyDecisionResponse),
        (status = 400, description = "Malformed or unsupported JSON request", body = ApiErrorResponse),
        (status = 401, description = "Credential is invalid, expired, or revoked", body = ApiErrorResponse),
        (status = 500, description = "Opaque internal failure", body = ApiErrorResponse)
    )
)]
pub async fn decide(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Result<Json<PolicyDecisionRequest>, JsonRejection>,
) -> Result<Json<PolicyDecisionResponse>, ApiError> {
    let bearer = bearer_credential(&headers)?;
    let Json(request) = request.map_err(|_| ApiError::BadRequest)?;
    match untrusted_principal_kind(bearer.as_str())? {
        PrincipalKind::Staff => {
            let staff = state
                .verify_staff_session
                .execute(VerifyStaffSessionInput {
                    bearer_token: bearer.into_inner(),
                })
                .await
                .map_err(|error| map_staff_auth_error(&error))?;
            decide_for_staff(&state, staff, request).await
        }
        PrincipalKind::Service => decide_for_service(&state, bearer.as_str(), request).await,
    }
}

enum PrincipalKind {
    Staff,
    Service,
}

#[derive(Deserialize)]
struct UntrustedDispatchClaims {
    principal_kind: String,
}

fn untrusted_principal_kind(token: &str) -> Result<PrincipalKind, ApiError> {
    let mut segments = token.split('.');
    let header = segments.next().filter(|segment| !segment.is_empty());
    let payload = segments.next().filter(|segment| !segment.is_empty());
    let signature = segments.next().filter(|segment| !segment.is_empty());
    if header.is_none() || signature.is_none() || segments.next().is_some() {
        return Err(ApiError::Unauthorized);
    }
    let payload = URL_SAFE_NO_PAD
        .decode(payload.ok_or(ApiError::Unauthorized)?)
        .map_err(|_| ApiError::Unauthorized)?;
    let claims: UntrustedDispatchClaims =
        serde_json::from_slice(&payload).map_err(|_| ApiError::Unauthorized)?;
    match claims.principal_kind.as_str() {
        "staff" => Ok(PrincipalKind::Staff),
        "service" => Ok(PrincipalKind::Service),
        _ => Err(ApiError::Unauthorized),
    }
}

async fn decide_for_staff(
    state: &AppState,
    staff: VerifyStaffSessionOutput,
    request: PolicyDecisionRequest,
) -> Result<Json<PolicyDecisionResponse>, ApiError> {
    let output = state
        .evaluate_access
        .execute(EvaluateAccessInput {
            principal_id: staff.context.principal_id,
            roles: staff.context.roles,
            resource: request.resource,
            action: request.action,
            resource_id: request.resource_id,
            trace_id: request.trace_id,
        })
        .await
        .map_err(|_| ApiError::internal("evaluate_staff_access"))?;
    Ok(Json(PolicyDecisionResponse {
        principal_id: output.principal_id,
        decision: output.decision.decision().clone(),
        reason_code: output.decision.reason_code().to_owned(),
        evaluated_at: output.evaluated_at,
    }))
}

async fn decide_for_service(
    state: &AppState,
    bearer: &str,
    request: PolicyDecisionRequest,
) -> Result<Json<PolicyDecisionResponse>, ApiError> {
    let principal = state
        .service_credential_verifier
        .verify_credential(bearer)
        .await
        .map_err(|error| map_service_error(&error))?;
    let output = state
        .authorize_service_call
        .execute(AuthorizeServiceCallInput {
            principal,
            call: ServiceCallMetadata {
                resource: request.resource,
                action: request.action,
                resource_id: request.resource_id,
                trace_id: request.trace_id,
            },
        })
        .await
        .map_err(|error| map_service_error(&error))?;
    Ok(Json(PolicyDecisionResponse {
        principal_id: output.principal_id,
        decision: output.decision.decision().clone(),
        reason_code: output.decision.reason_code().to_owned(),
        evaluated_at: output.evaluated_at,
    }))
}

fn map_service_error(error: &ServiceIdentityError) -> ApiError {
    match error {
        ServiceIdentityError::InvalidCredential => ApiError::Unauthorized,
        ServiceIdentityError::Infrastructure(_) => ApiError::internal("authorize_service_call"),
    }
}
