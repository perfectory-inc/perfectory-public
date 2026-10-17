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
    /// An opaque internal failure. The cause is required and goes to the log only, never to the
    /// caller: an error logged without its cause cost the 2026-10-16 staff sign-in its diagnosis.
    pub(crate) fn internal(operation: &'static str, cause: &dyn std::fmt::Display) -> Self {
        let correlation_id = Uuid::now_v7();
        tracing::error!(%correlation_id, operation, %cause, "identity request failed");
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::ApiError;

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Ok(mut buffer) = self.0.lock() {
                buffer.extend_from_slice(bytes);
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn an_internal_error_logs_its_cause_and_answers_without_it() {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .finish();
        let response = tracing::subscriber::with_default(subscriber, || {
            axum::response::IntoResponse::into_response(ApiError::internal(
                "verify_staff_session",
                &"error sending request: dns error: try again",
            ))
        });
        let logged = captured
            .0
            .lock()
            .map(|buffer| String::from_utf8_lossy(&buffer).into_owned())
            .unwrap_or_default();
        assert!(
            logged.contains("operation=\"verify_staff_session\""),
            "{logged}"
        );
        assert!(
            logged.contains("cause=error sending request: dns error: try again"),
            "{logged}"
        );
        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
