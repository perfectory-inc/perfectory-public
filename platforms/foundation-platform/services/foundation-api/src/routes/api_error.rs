//! Shared Foundation API command error response.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use foundation_contracts::error::{ApiErrorResponse, InternalApiErrorResponse};
use uuid::Uuid;

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Forbidden(String),
    Conflict(String),
    NotFound(String),
    /// Internal failures that must be logged and redacted from the HTTP response.
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Forbidden(message) => (StatusCode::FORBIDDEN, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Internal(detail) => {
                let correlation_id = Uuid::new_v4();
                tracing::error!(
                    %correlation_id,
                    error = %detail,
                    "internal server error"
                );
                let payload = InternalApiErrorResponse {
                    error: "internal server error".to_owned(),
                    correlation_id,
                };
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(payload)).into_response();
            }
        };
        (status, Json(ApiErrorResponse { error: body })).into_response()
    }
}
