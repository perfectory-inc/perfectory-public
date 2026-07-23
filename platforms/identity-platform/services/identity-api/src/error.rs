//! Opaque HTTP error mapping.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

/// Published error envelope for Identity HTTP failures.
#[derive(Serialize, ToSchema)]
pub struct ApiErrorResponse {
    /// Stable machine-readable error code.
    code: &'static str,
    /// Safe client-facing summary.
    message: &'static str,
    /// Opaque identifier available for internal failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<Uuid>,
}

pub enum ApiError {
    Unauthorized,
    Forbidden,
    Conflict,
    NotFound,
    BadRequest,
    Internal { correlation_id: Uuid },
}

impl ApiError {
    pub(crate) fn internal(operation: &'static str) -> Self {
        let correlation_id = Uuid::now_v7();
        tracing::error!(%correlation_id, operation, "identity request failed");
        Self::Internal { correlation_id }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, correlation_id) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "A valid bearer credential is required.",
                None,
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "The verified principal is not allowed to perform this action.",
                None,
            ),
            Self::Conflict => (
                StatusCode::CONFLICT,
                "role_already_assigned",
                "The requested role is already assigned.",
                None,
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "staff_not_found",
                "The requested staff principal was not found.",
                None,
            ),
            Self::BadRequest => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "The request is invalid.",
                None,
            ),
            Self::Internal { correlation_id } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "The request could not be completed.",
                Some(correlation_id),
            ),
        };
        (
            status,
            Json(ApiErrorResponse {
                code,
                message,
                correlation_id,
            }),
        )
            .into_response()
    }
}
