//! Identity HTTP route registration and health endpoints.

pub mod policy;
pub mod staff;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/identity/v1/staff/sessions/verify",
            post(staff::verify_session),
        )
        .route(
            "/identity/v1/staff/sessions/revoke",
            post(staff::revoke_session),
        )
        .route(
            "/identity/v1/staff/{staff_id}/roles",
            post(staff::assign_role),
        )
        .route("/identity/v1/policy/decisions", post(policy::decide))
        .route("/healthz", get(live))
        .route("/readyz", get(ready))
        .with_state(state)
}

/// Safe health response without configuration details.
#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    status: &'static str,
}

/// Safe readiness response without connection or verifier secrets.
#[derive(Serialize, ToSchema)]
pub struct ReadinessResponse {
    status: &'static str,
    database: &'static str,
    verifier_configuration: &'static str,
}

#[utoipa::path(
    get,
    path = "/healthz",
    responses((status = 200, description = "Process is live", body = HealthResponse))
)]
pub async fn live() -> Json<HealthResponse> {
    Json(HealthResponse { status: "live" })
}

#[utoipa::path(
    get,
    path = "/readyz",
    responses(
        (status = 200, description = "Identity dependencies are ready", body = ReadinessResponse),
        (status = 503, description = "Identity dependencies are unavailable", body = ReadinessResponse)
    )
)]
pub async fn ready(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> (StatusCode, Json<ReadinessResponse>) {
    let readiness = state.readiness().await;
    let status = if readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadinessResponse {
            status: if readiness.is_ready() {
                "ready"
            } else {
                "not_ready"
            },
            database: if readiness.database {
                "ok"
            } else {
                "unavailable"
            },
            verifier_configuration: if readiness.verifier_configuration_valid {
                "valid"
            } else {
                "invalid"
            },
        }),
    )
}
