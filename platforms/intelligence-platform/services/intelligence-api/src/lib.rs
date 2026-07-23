pub mod admission;
pub mod auth;
pub mod health_routes;
pub mod observability;
pub mod rate_limit_middleware;
pub mod routes;
pub mod state;

use std::time::Duration;

use admission::{apply_admission_layers, AdmissionConfig};
use auth::{auth_middleware, InboundAuthConfig};
use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    http::{header, Method, StatusCode},
    middleware, BoxError, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use state::AppState;
use tower::ServiceBuilder;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};

fn cors_layer_for_config(config: &InboundAuthConfig) -> CorsLayer {
    let base = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    if config.allowed_origins.is_empty() {
        return base; // no allow_origin -> cross-origin browser calls are refused
    }
    let allowed = config.allowed_origins.clone();
    base.allow_origin(AllowOrigin::predicate(move |origin, _| {
        allowed
            .iter()
            .any(|allowed_origin| allowed_origin.as_bytes() == origin.as_bytes())
    }))
}

pub fn app(state: AppState) -> Router {
    app_full(state, AdmissionConfig::default())
}

pub fn app_with_metrics(state: AppState, metrics: Option<PrometheusHandle>) -> Router {
    app_full(state.with_metrics(metrics), AdmissionConfig::default())
}

/// Compose the full router with explicit admission-control parameters.
///
/// Admission layers wrap only the *protected* router so that health endpoints
/// are never shed or concurrency-blocked — K8s liveness probes must answer
/// even when the service is saturated.  Health routes still get a short
/// per-request timeout (2 s) and a tiny body cap (1 KiB) for safety.
pub fn app_with_admission(state: AppState, admission: AdmissionConfig) -> Router {
    app_full(state, admission)
}

fn app_full(state: AppState, admission: AdmissionConfig) -> Router {
    let cors = cors_layer_for_config(
        state
            .inbound_auth
            .as_ref()
            .unwrap_or(&InboundAuthConfig::default()),
    );

    // Protected routes: auth + CORS + admission (shed / concurrency / timeout / body).
    let protected = routes::protected_router()
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware::rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state.clone())
        .layer(cors);
    let protected = apply_admission_layers(protected, &admission);

    // Health routes: OUTSIDE load-shed and concurrency limit so liveness probes
    // always answer.  Still get a 2-second hard deadline and a 1 KiB body cap.
    // /metrics is served here (outside the shed/concurrency stack) intentionally:
    // scrapes must work during saturation.  The C3 design moves /metrics to a
    // separate loopback listener; for now it lives on the main port and is
    // bearer-gated when auth is required.
    let health = health_routes::router()
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_error: BoxError| async {
                    StatusCode::GATEWAY_TIMEOUT
                }))
                .timeout(Duration::from_secs(2)),
        )
        .layer(DefaultBodyLimit::max(1024));

    // Merge: health is evaluated first (no shed stack), protected second.
    // track_metrics wraps the merged router outermost so shed 503s and
    // timeouts are counted.  Because admission layers run post-routing,
    // shed 503s/504s carry the real route template; only true 404 fallbacks
    // get path="unmatched".
    // TraceLayer adds per-request tracing spans (A4 requirement).
    health
        .merge(protected)
        .layer(middleware::from_fn(observability::track_metrics))
        .layer(TraceLayer::new_for_http())
}
