//! Staff session verification and role assignment routes.

use std::sync::Arc;

use authorization_application::{AssignStaffRoleError, AssignStaffRoleInput};
use authorization_domain::RoleCode;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use identity_contracts::{
    AssignStaffRoleRequest, PrincipalId, StaffRoleResponse, VerifyStaffSessionResponse,
};
use identity_shared_kernel::StaffId;
use staff_identity_application::{VerifyStaffSessionInput, VerifyStaffSessionOutput};
use staff_identity_domain::StaffIdentityError;
use uuid::Uuid;

use crate::error::{ApiError, ApiErrorResponse};
use crate::state::AppState;

#[utoipa::path(
    post,
    path = "/identity/v1/staff/sessions/verify",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Verified staff session", body = VerifyStaffSessionResponse),
        (status = 401, description = "Credential is invalid, expired, or revoked", body = ApiErrorResponse),
        (status = 500, description = "Opaque internal failure", body = ApiErrorResponse)
    )
)]
pub async fn verify_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<VerifyStaffSessionResponse>, ApiError> {
    let bearer = bearer_credential(&headers)?;
    let output = state
        .verify_staff_session
        .execute(VerifyStaffSessionInput {
            bearer_token: bearer.into_inner(),
        })
        .await
        .map_err(|error| map_staff_auth_error(&error))?;
    Ok(Json(map_verify_response(output)))
}

#[utoipa::path(
    post,
    path = "/identity/v1/staff/sessions/revoke",
    security(("bearerAuth" = [])),
    responses(
        (status = 204, description = "Current staff session revoked"),
        (status = 401, description = "Credential is invalid, expired, or revoked", body = ApiErrorResponse),
        (status = 404, description = "Verified session was not found", body = ApiErrorResponse),
        (status = 500, description = "Opaque internal failure", body = ApiErrorResponse)
    )
)]
pub async fn revoke_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<axum::http::StatusCode, ApiError> {
    let bearer = bearer_credential(&headers)?;
    let verified = state
        .verify_staff_session
        .execute(VerifyStaffSessionInput {
            bearer_token: bearer.into_inner(),
        })
        .await
        .map_err(|error| map_staff_auth_error(&error))?;
    state
        .revoke_staff_session
        .execute(verified.jti, "logout")
        .await
        .map_err(|error| match error {
            StaffIdentityError::SessionNotFound => ApiError::NotFound,
            other => map_staff_auth_error(&other),
        })?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/identity/v1/staff/{staff_id}/roles",
    params(("staff_id" = Uuid, Path, description = "Target staff identifier")),
    request_body = AssignStaffRoleRequest,
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Role assigned", body = StaffRoleResponse),
        (status = 400, description = "Invalid staff identifier or role code", body = ApiErrorResponse),
        (status = 401, description = "Credential is invalid, expired, or revoked", body = ApiErrorResponse),
        (status = 403, description = "Verified staff principal cannot grant roles", body = ApiErrorResponse),
        (status = 404, description = "Target staff principal does not exist", body = ApiErrorResponse),
        (status = 409, description = "Role is already assigned", body = ApiErrorResponse),
        (status = 500, description = "Opaque internal failure", body = ApiErrorResponse)
    )
)]
pub async fn assign_role(
    State(state): State<Arc<AppState>>,
    Path(staff_id): Path<String>,
    headers: HeaderMap,
    request: Result<Json<AssignStaffRoleRequest>, JsonRejection>,
) -> Result<Json<StaffRoleResponse>, ApiError> {
    let target_staff_id = Uuid::parse_str(&staff_id)
        .map(StaffId::new)
        .map_err(|_| ApiError::BadRequest)?;
    let bearer = bearer_credential(&headers)?;
    let Json(request) = request.map_err(|_| ApiError::BadRequest)?;
    let role_code = RoleCode::parse(request.role_code).map_err(|_| ApiError::BadRequest)?;
    let verified = state
        .verify_staff_session
        .execute(VerifyStaffSessionInput {
            bearer_token: bearer.into_inner(),
        })
        .await
        .map_err(|error| map_staff_auth_error(&error))?;
    let output = state
        .assign_staff_role
        .execute(AssignStaffRoleInput {
            actor_principal_id: verified.context.principal_id,
            actor_staff_id: verified.context.staff_id,
            actor_roles: verified.context.roles,
            target_staff_id,
            role_code,
            trace_id: Uuid::now_v7().to_string(),
        })
        .await
        .map_err(|error| map_assign_role_error(&error))?;
    Ok(Json(StaffRoleResponse {
        principal_id: PrincipalId::new(output.grant.staff_id.as_uuid()),
        role_code: output.grant.role_code.as_str().to_owned(),
        granted_at: output.grant.granted_at,
        granted_by: PrincipalId::new(output.grant.granted_by.as_uuid()),
    }))
}

pub struct BearerCredential(String);

impl BearerCredential {
    pub(crate) fn into_inner(self) -> String {
        self.0
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn bearer_credential(headers: &HeaderMap) -> Result<BearerCredential, ApiError> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_whitespace))
        .ok_or(ApiError::Unauthorized)?;
    Ok(BearerCredential(token.to_owned()))
}

pub fn map_verify_response(output: VerifyStaffSessionOutput) -> VerifyStaffSessionResponse {
    VerifyStaffSessionResponse {
        principal_id: output.context.principal_id,
        email: output.email,
        display_name: output.display_name,
        roles: output.context.roles.into_iter().map(String::from).collect(),
        expires_at: output.expires_at,
    }
}

pub fn map_staff_auth_error(error: &StaffIdentityError) -> ApiError {
    match error {
        StaffIdentityError::StaffNotFound(_)
        | StaffIdentityError::SessionNotFound
        | StaffIdentityError::SessionExpired
        | StaffIdentityError::JtiRevoked(_)
        | StaffIdentityError::InvalidClaims(_) => ApiError::Unauthorized,
        StaffIdentityError::DuplicateZitadelSubject | StaffIdentityError::Infrastructure(_) => {
            ApiError::internal("verify_staff_session")
        }
    }
}

fn map_assign_role_error(error: &AssignStaffRoleError) -> ApiError {
    match error {
        AssignStaffRoleError::PermissionDenied(_) => ApiError::Forbidden,
        AssignStaffRoleError::DuplicateRole
        | AssignStaffRoleError::Persistence(
            authorization_application::ports::RoleGrantPersistenceError::DuplicateRole,
        ) => ApiError::Conflict,
        AssignStaffRoleError::Persistence(
            authorization_application::ports::RoleGrantPersistenceError::StaffNotFound(_),
        ) => ApiError::NotFound,
        AssignStaffRoleError::Persistence(
            authorization_application::ports::RoleGrantPersistenceError::Infrastructure(_),
        ) => ApiError::internal("assign_staff_role"),
    }
}
