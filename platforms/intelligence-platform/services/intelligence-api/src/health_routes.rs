use std::time::Duration;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tokio::time::timeout;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
}

// ---------------------------------------------------------------------------
// Liveness — always 200 when the process is running
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "intelligence-platform",
    })
}

// ---------------------------------------------------------------------------
// Readiness — config-based, no live dependency calls
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ReadinessResponse {
    status: &'static str,
    checks: Vec<ReadinessCheck>,
}

#[derive(Debug, Serialize)]
struct ReadinessCheck {
    name: &'static str,
    ok: bool,
}

async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<ReadinessResponse>) {
    let checks = vec![
        ReadinessCheck {
            name: "model_gateway",
            ok: state.model_gateway.is_some(),
        },
        ReadinessCheck {
            name: "foundation_submitter",
            ok: state.foundation_submitter.is_some(),
        },
    ];

    let all_ok = checks.iter().all(|c| c.ok);
    let status_code = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status = if all_ok { "ready" } else { "degraded" };

    (status_code, Json(ReadinessResponse { status, checks }))
}

// ---------------------------------------------------------------------------
// Metrics — Prometheus text format
// ---------------------------------------------------------------------------

const METRICS_UNAVAILABLE_FALLBACK: &str = concat!(
    "# HELP intelligence_metrics_unavailable metrics recorder unavailable\n",
    "# TYPE intelligence_metrics_unavailable gauge\n",
    "intelligence_metrics_unavailable 1\n",
);
const RECONCILE_METRICS_TIMEOUT: Duration = Duration::from_millis(100);

async fn metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    // /metrics is bearer-gated when auth is required so that metric data is not
    // publicly readable.  /healthz and /readyz remain open because liveness and
    // readiness probes must not require credentials.
    if let Some(config) = state.inbound_auth.as_ref().filter(|c| c.required) {
        if !crate::auth::bearer_token_matches(&headers, config) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(crate::auth::AuthErrorResponse {
                    code: "authentication_failed",
                    message: "authentication failed",
                }),
            )
                .into_response();
        }
    }
    refresh_reconcile_metrics(&state).await;
    MetricsResponse(
        state
            .metrics
            .as_ref()
            .map(PrometheusHandle::render)
            .unwrap_or_else(|| METRICS_UNAVAILABLE_FALLBACK.to_owned()),
    )
    .into_response()
}

async fn refresh_reconcile_metrics(state: &AppState) {
    let stats = timeout(RECONCILE_METRICS_TIMEOUT, state.reconcile_queue.stats()).await;
    match stats {
        Ok(Ok(stats)) => {
            metrics::gauge!("outbox_reconcile_required_depth").set(stats.depth as f64);
            metrics::gauge!("outbox_reconcile_oldest_age_seconds").set(stats.oldest_age_seconds);
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "failed to refresh reconcile metrics");
        }
        Err(_) => {
            tracing::warn!("timed out refreshing reconcile metrics");
        }
    }
}

struct MetricsResponse(String);

impl IntoResponse for MetricsResponse {
    fn into_response(self) -> axum::response::Response {
        (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            self.0,
        )
            .into_response()
    }
}
