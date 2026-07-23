//! axum 라우터 조립.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::{Path, State};
use axum::http::{header, HeaderName, HeaderValue, Method, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, put, MethodRouter};
use axum::{Json, Router};
use serde::Serialize;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tracing::warn;

use crate::identity_authorization::{IdentityAuthorizationError, RequiredPrincipalKind};
use crate::state::{
    ApiDatabasePoolMetric, ApiHttpDurationMetric, ApiHttpRequestMetric, ApiOverloadRejectionMetric,
    AppState, IngestionRunMetric, LakehouseBatchRunMetric, OutboxQueueMetric,
};
use crate::traffic::{TrafficConfig, TrafficRuntime};

const FOUNDATION_PLATFORM_RUNTIME_ENV: &str = "FOUNDATION_PLATFORM_RUNTIME_ENV";
const DEFAULT_LOCAL_CORS_ALLOWED_ORIGINS: &str =
    "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3900,http://127.0.0.1:3900";
const PROMETHEUS_TEXT_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

mod api_error;
pub mod catalog;
mod catalog_openapi;
mod lakehouse_registry;
mod normalization;
pub mod pipeline_graph;

use api_error::ApiError;

pub use catalog_openapi::catalog_openapi_document;

#[cfg(test)]
pub fn router(state: Arc<AppState>) -> Router {
    router_with_traffic(state, TrafficConfig::default())
}

pub fn router_with_traffic(state: Arc<AppState>, traffic: TrafficConfig) -> Router {
    let metrics_state = state.clone();
    let concurrency_state = TrafficMiddlewareState {
        app_state: state.clone(),
        traffic: TrafficRuntime::new(traffic),
    };
    application_routes(&state)
        .with_state(state)
        .layer(DefaultBodyLimit::max(traffic.body_limit_bytes))
        .layer(middleware::from_fn_with_state(
            concurrency_state,
            enforce_concurrency_limit,
        ))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(traffic.request_timeout_ms),
        ))
        .layer(cors_layer_from_env())
        .layer(middleware::from_fn_with_state(
            metrics_state,
            record_http_metrics,
        ))
}

fn application_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .merge(system_routes())
        .merge(complex_catalog_routes(state))
        .merge(parcel_catalog_routes(state))
        .merge(lakehouse_catalog_routes(state))
        .merge(map_catalog_routes(state))
        .merge(internal_routes(state))
        .merge(normalization::routes(state))
}

fn system_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route(
            "/catalog/v1/pipeline-graph",
            get(pipeline_graph::get_pipeline_graph),
        )
}

fn complex_catalog_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/catalog/v1/complexes",
            get(catalog::list_complexes).merge(protected_route(
                post(catalog::register_complex),
                state,
                STAFF_CATALOG_WRITE,
                None,
            )),
        )
        .route(
            "/catalog/v1/complexes/{id}",
            get(catalog::get_complex).merge(protected_route(
                patch(catalog::update_complex),
                state,
                STAFF_CATALOG_WRITE,
                Some("id"),
            )),
        )
        .route(
            "/catalog/v1/complexes/{id}/archive",
            protected_route(
                post(catalog::archive_complex),
                state,
                STAFF_CATALOG_WRITE,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/anchor-summary",
            get(catalog::get_complex_anchor_summary),
        )
        .route(
            "/catalog/v1/complexes/{id}/parcels",
            protected_route(
                get(catalog::list_complex_parcels),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/buildings",
            protected_route(
                get(catalog::list_complex_buildings),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/manufacturers",
            get(catalog::list_complex_manufacturers),
        )
        .merge(complex_catalog_asset_routes(state))
        .route(
            "/catalog/v1/industry-groups",
            get(catalog::list_industry_groups),
        )
}

fn complex_catalog_asset_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/catalog/v1/complexes/{id}/notices",
            protected_route(
                get(catalog::list_complex_notices),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/attachments",
            protected_route(
                get(catalog::list_complex_attachments),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/blueprints",
            protected_route(
                get(catalog::list_complex_blueprints),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/spatial-layers",
            protected_route(
                get(catalog::list_complex_spatial_layers),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/complexes/{id}/digital-twin-assets",
            protected_route(
                get(catalog::list_complex_digital_twin_assets),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
}

fn parcel_catalog_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/catalog/v1/parcels/by-pnu/{pnu}/buildings",
            protected_route(
                get(catalog::list_parcel_buildings_by_pnu),
                state,
                SERVICE_CATALOG_READ,
                Some("pnu"),
            ),
        )
        .route(
            "/catalog/v1/parcels/by-pnu/{pnu}/units",
            protected_route(
                get(catalog::list_parcel_units_by_pnu),
                state,
                SERVICE_CATALOG_READ,
                Some("pnu"),
            ),
        )
        .route(
            "/catalog/v1/parcels/by-pnu/{pnu}",
            protected_route(
                get(catalog::get_parcel_by_pnu),
                state,
                SERVICE_CATALOG_READ,
                Some("pnu"),
            ),
        )
        .route(
            "/catalog/v1/parcels/{id}",
            protected_route(
                get(catalog::get_parcel),
                state,
                SERVICE_CATALOG_READ,
                Some("id"),
            ),
        )
        .route(
            "/catalog/v1/parcels/{id}/industry-assignments",
            get(catalog::list_parcel_industry_assignments),
        )
        .route(
            "/catalog/v1/parcels/{id}/kind",
            protected_route(
                patch(catalog::update_parcel_kind),
                state,
                STAFF_CATALOG_WRITE,
                Some("id"),
            ),
        )
}

fn lakehouse_catalog_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new().route(
        "/catalog/v1/lakehouse/batch-runs",
        protected_route(
            post(catalog::record_lakehouse_batch_run),
            state,
            STAFF_LAKEHOUSE_BATCH_AUDIT,
            None,
        ),
    )
}

fn map_catalog_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/catalog/v1/vector-tiles/manifest",
            get(catalog::get_vector_tile_manifest),
        )
        .route(
            "/map/v1/marker-tiles/contract",
            get(catalog::get_marker_tile_contract),
        )
        .route(
            "/map/v1/marker-tiles/{layer}/{z}/{x}/{y_pbf}",
            get(catalog::get_marker_tile),
        )
        // axum 0.8 (matchit 0.8) treats `:` as a literal, so the custom-verb
        // actions are registered as exact literal paths instead of the former
        // `manifest:action` suffix-parameter wildcard.
        .route(
            "/catalog/v1/vector-tiles/manifest:rollback",
            protected_route(
                post(catalog::rollback_vector_tile_manifest),
                state,
                STAFF_SPATIAL_MANIFEST_ADMIN,
                None,
            ),
        )
        .route(
            "/catalog/v1/vector-tiles/manifest:promote",
            protected_route(
                put(catalog::promote_vector_tile_manifest),
                state,
                STAFF_SPATIAL_MANIFEST_ADMIN,
                None,
            ),
        )
        .route(
            "/catalog/v1/parcel-marker-anchors:rebuild",
            protected_route(
                post(catalog::rebuild_parcel_marker_anchors),
                state,
                STAFF_SPATIAL_ANCHOR_REBUILD,
                None,
            ),
        )
}

fn internal_routes(state: &Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new().route(
        "/internal/lakehouse/artifacts",
        protected_route(
            post(lakehouse_registry::register_object_artifact),
            state,
            SERVICE_LAKEHOUSE_WRITE,
            None,
        ),
    )
}

#[derive(Clone)]
struct TrafficMiddlewareState {
    app_state: Arc<AppState>,
    traffic: TrafficRuntime,
}

#[derive(Clone, Copy)]
struct IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind,
    resource: &'static str,
    action: &'static str,
}

const STAFF_CATALOG_WRITE: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Staff,
    resource: "foundation.catalog",
    action: "write",
};
const SERVICE_CATALOG_READ: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Service,
    resource: "foundation.catalog",
    action: "read",
};
const STAFF_LAKEHOUSE_BATCH_AUDIT: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Staff,
    resource: "foundation.lakehouse",
    action: "batch_audit",
};
const STAFF_SPATIAL_MANIFEST_ADMIN: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Staff,
    resource: "foundation.spatial",
    action: "manifest_admin",
};
const STAFF_SPATIAL_ANCHOR_REBUILD: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Staff,
    resource: "foundation.spatial",
    action: "anchor_rebuild",
};
const SERVICE_LAKEHOUSE_WRITE: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Service,
    resource: "foundation.lakehouse",
    action: "write",
};
const SERVICE_NORMALIZATION_PROPOSE: IdentityRoutePolicy = IdentityRoutePolicy {
    required_principal_kind: RequiredPrincipalKind::Service,
    resource: "foundation.normalization",
    action: "propose",
};

#[derive(Clone)]
struct IdentityRouteAuthorizationState {
    app_state: Arc<AppState>,
    policy: IdentityRoutePolicy,
    resource_id_parameter: Option<&'static str>,
}

fn protected_route(
    route: MethodRouter<Arc<AppState>>,
    state: &Arc<AppState>,
    policy: IdentityRoutePolicy,
    resource_id_parameter: Option<&'static str>,
) -> MethodRouter<Arc<AppState>> {
    route.route_layer(middleware::from_fn_with_state(
        IdentityRouteAuthorizationState {
            app_state: state.clone(),
            policy,
            resource_id_parameter,
        },
        enforce_identity_authorization,
    ))
}

async fn enforce_identity_authorization(
    State(state): State<IdentityRouteAuthorizationState>,
    Path(parameters): Path<BTreeMap<String, String>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(bearer) = bearer_token(request.headers()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let trace_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(|| uuid::Uuid::now_v7().to_string(), ToOwned::to_owned);
    let resource_id = state
        .resource_id_parameter
        .and_then(|parameter| parameters.get(parameter))
        .map(String::as_str);
    match state
        .app_state
        .identity_authorization
        .authorize(
            bearer,
            state.policy.required_principal_kind,
            state.policy.resource,
            state.policy.action,
            resource_id,
            &trace_id,
        )
        .await
    {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => identity_error_status(error).into_response(),
    }
}

fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, token) = raw.split_once(char::is_whitespace)?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

const fn identity_error_status(error: IdentityAuthorizationError) -> StatusCode {
    match error {
        IdentityAuthorizationError::Unauthorized => StatusCode::UNAUTHORIZED,
        IdentityAuthorizationError::Forbidden => StatusCode::FORBIDDEN,
        IdentityAuthorizationError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn enforce_concurrency_limit(
    State(state): State<TrafficMiddlewareState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(_permit) = state.traffic.try_acquire_concurrency() else {
        state
            .app_state
            .record_overload_rejection("concurrency_limit");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    next.run(request).await
}

async fn record_http_metrics(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_owned();
    let route = canonical_route_label(request.uri().path());
    let started_at = Instant::now();
    let response = next.run(request).await;
    state.record_http_request(
        &method,
        &route,
        response.status().as_u16(),
        started_at.elapsed().as_secs_f64(),
    );
    response
}

#[derive(Serialize, utoipa::ToSchema)]
struct HealthResponse {
    service: &'static str,
    status: &'static str,
    version: &'static str,
}

#[derive(Serialize, utoipa::ToSchema)]
struct ReadinessResponse {
    service: &'static str,
    status: &'static str,
    version: &'static str,
    database: &'static str,
}

#[utoipa::path(
    get,
    path = "/healthz",
    operation_id = "health",
    responses((status = 200, description = "Service process is alive", body = HealthResponse))
)]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "foundation-api",
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[utoipa::path(
    get,
    path = "/readyz",
    operation_id = "ready",
    responses(
        (status = 200, description = "Service dependencies are ready", body = ReadinessResponse),
        (status = 503, description = "Service dependencies are unavailable", body = ReadinessResponse)
    )
)]
async fn ready(State(state): State<Arc<AppState>>) -> (StatusCode, Json<ReadinessResponse>) {
    let database_ready = state.database_ready().await;
    (
        readiness_status(database_ready),
        Json(ReadinessResponse {
            service: "foundation-api",
            status: if database_ready { "ready" } else { "not_ready" },
            version: env!("CARGO_PKG_VERSION"),
            database: if database_ready { "ok" } else { "unavailable" },
        }),
    )
}

const fn readiness_status(database_ready: bool) -> StatusCode {
    if database_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[utoipa::path(
    get,
    path = "/metrics",
    operation_id = "metrics",
    responses((status = 200, description = "Prometheus metrics", content_type = "text/plain", body = String))
)]
async fn metrics(State(state): State<Arc<AppState>>) -> ([(HeaderName, &'static str); 1], String) {
    let database_ready = state.database_ready().await;
    let database_pool = state.database_pool_metric();
    let lakehouse_batch_runs = if database_ready {
        state.latest_lakehouse_batch_run_metrics().await
    } else {
        Vec::new()
    };
    let ingestion_runs = if database_ready {
        state.latest_ingestion_run_metrics().await
    } else {
        Vec::new()
    };
    let outbox_queues = if database_ready {
        state.outbox_queue_metrics().await
    } else {
        Vec::new()
    };
    let http_requests = state.http_request_metrics();
    let http_durations = state.http_duration_metrics();
    let overload_rejections = state.overload_rejection_metrics();
    (
        [(header::CONTENT_TYPE, PROMETHEUS_TEXT_CONTENT_TYPE)],
        metrics_body(
            database_ready,
            &database_pool,
            MetricsBodyInput {
                http_requests: &http_requests,
                http_durations: &http_durations,
                overload_rejections: &overload_rejections,
                lakehouse_batch_runs: &lakehouse_batch_runs,
                ingestion_runs: &ingestion_runs,
                outbox_queues: &outbox_queues,
            },
        ),
    )
}

#[derive(Clone, Copy)]
struct MetricsBodyInput<'a> {
    http_requests: &'a [ApiHttpRequestMetric],
    http_durations: &'a [ApiHttpDurationMetric],
    overload_rejections: &'a [ApiOverloadRejectionMetric],
    lakehouse_batch_runs: &'a [LakehouseBatchRunMetric],
    ingestion_runs: &'a [IngestionRunMetric],
    outbox_queues: &'a [OutboxQueueMetric],
}

fn metrics_body(
    database_ready: bool,
    database_pool: &ApiDatabasePoolMetric,
    metrics: MetricsBodyInput<'_>,
) -> String {
    let database_ready_value = i32::from(database_ready);
    let mut body = format!(
        concat!(
            "# HELP foundation_api_up Whether the Foundation Platform API process is running.\n",
            "# TYPE foundation_api_up gauge\n",
            "foundation_api_up 1\n",
            "# HELP foundation_api_database_ready Whether the Foundation Platform API can query PostgreSQL.\n",
            "# TYPE foundation_api_database_ready gauge\n",
            "foundation_api_database_ready {}\n",
            "# HELP foundation_api_db_pool_size Current SQLx PostgreSQL pool size.\n",
            "# TYPE foundation_api_db_pool_size gauge\n",
            "foundation_api_db_pool_size {}\n",
            "# HELP foundation_api_db_pool_idle_connections Current idle SQLx PostgreSQL pool connections.\n",
            "# TYPE foundation_api_db_pool_idle_connections gauge\n",
            "foundation_api_db_pool_idle_connections {}\n",
            "# HELP foundation_api_db_pool_max_connections Configured maximum SQLx PostgreSQL pool connections.\n",
            "# TYPE foundation_api_db_pool_max_connections gauge\n",
            "foundation_api_db_pool_max_connections {}\n",
            "# HELP foundation_api_http_requests_total HTTP requests handled by method, canonical route, and status.\n",
            "# TYPE foundation_api_http_requests_total counter\n",
            "# HELP foundation_api_http_request_duration_seconds HTTP request duration by method, canonical route, status, and latency bucket.\n",
            "# TYPE foundation_api_http_request_duration_seconds histogram\n",
            "# HELP foundation_api_http_request_timeout_total HTTP requests timed out by canonical route.\n",
            "# TYPE foundation_api_http_request_timeout_total counter\n",
            "# HELP foundation_api_http_overload_rejected_total HTTP requests rejected before route handling by reason.\n",
            "# TYPE foundation_api_http_overload_rejected_total counter\n",
            "# HELP foundation_platform_lakehouse_batch_last_success_created_at_seconds Latest successful lakehouse batch run creation time by contract.\n",
            "# TYPE foundation_platform_lakehouse_batch_last_success_created_at_seconds gauge\n",
            "# HELP foundation_platform_lakehouse_batch_last_success_recorded_at_seconds Latest successful lakehouse batch run audit record time by contract.\n",
            "# TYPE foundation_platform_lakehouse_batch_last_success_recorded_at_seconds gauge\n",
            "# HELP foundation_platform_lakehouse_batch_last_success_row_count Latest successful lakehouse batch row count by contract.\n",
            "# TYPE foundation_platform_lakehouse_batch_last_success_row_count gauge\n",
            "# HELP foundation_platform_ingestion_run_last_finished_at_seconds Latest finished Bronze ingestion run time by source and status.\n",
            "# TYPE foundation_platform_ingestion_run_last_finished_at_seconds gauge\n",
            "# HELP foundation_platform_ingestion_run_last_duration_seconds Latest finished Bronze ingestion run duration by source and status.\n",
            "# TYPE foundation_platform_ingestion_run_last_duration_seconds gauge\n",
            "# HELP foundation_platform_ingestion_run_last_records_seen Latest finished Bronze ingestion records seen by source and status.\n",
            "# TYPE foundation_platform_ingestion_run_last_records_seen gauge\n",
            "# HELP foundation_platform_ingestion_run_last_objects_written Latest finished Bronze ingestion objects written by source and status.\n",
            "# TYPE foundation_platform_ingestion_run_last_objects_written gauge\n",
            "# HELP foundation_platform_ingestion_run_last_raw_response_size_bytes Latest finished Bronze ingestion raw response bytes archived by source and status.\n",
            "# TYPE foundation_platform_ingestion_run_last_raw_response_size_bytes gauge\n",
            "# HELP foundation_platform_outbox_pending_event_count Unpublished outbox events by scope.\n",
            "# TYPE foundation_platform_outbox_pending_event_count gauge\n",
            "# HELP foundation_platform_outbox_retry_event_count Unpublished outbox events with retry_count greater than zero by scope.\n",
            "# TYPE foundation_platform_outbox_retry_event_count gauge\n",
            "# HELP foundation_platform_outbox_oldest_pending_age_seconds Age in seconds of the oldest unpublished outbox event by scope.\n",
            "# TYPE foundation_platform_outbox_oldest_pending_age_seconds gauge\n",
            "# HELP foundation_api_build_info Foundation Platform API build metadata.\n",
            "# TYPE foundation_api_build_info gauge\n",
            "foundation_api_build_info{{service=\"foundation-api\",version=\"{}\"}} 1\n"
        ),
        database_ready_value,
        database_pool.pool_size,
        database_pool.idle_connections,
        database_pool.max_connections,
        env!("CARGO_PKG_VERSION")
    );

    append_http_request_metrics(&mut body, metrics.http_requests);
    append_http_duration_metrics(&mut body, metrics.http_durations);
    append_http_timeout_metrics(&mut body, metrics.http_requests);
    append_overload_rejection_metrics(&mut body, metrics.overload_rejections);
    append_lakehouse_batch_run_metrics(&mut body, metrics.lakehouse_batch_runs);
    append_ingestion_run_metrics(&mut body, metrics.ingestion_runs);
    append_outbox_queue_metrics(&mut body, metrics.outbox_queues);

    body
}

fn append_http_duration_metrics(body: &mut String, metrics: &[ApiHttpDurationMetric]) {
    for metric in metrics {
        let method = prometheus_label_value(&metric.method);
        let route = prometheus_label_value(&metric.route);
        let le = prometheus_label_value(&metric.le);
        let status = metric.status;
        let count = metric.count;
        let _ = writeln!(
            body,
            "foundation_api_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",status=\"{status}\",le=\"{le}\"}} {count}"
        );
    }
}

fn append_overload_rejection_metrics(body: &mut String, metrics: &[ApiOverloadRejectionMetric]) {
    for metric in metrics {
        let reason = prometheus_label_value(&metric.reason);
        let count = metric.count;
        let _ = writeln!(
            body,
            "foundation_api_http_overload_rejected_total{{reason=\"{reason}\"}} {count}"
        );
    }
}

fn append_http_timeout_metrics(body: &mut String, metrics: &[ApiHttpRequestMetric]) {
    let mut timeouts = BTreeMap::<String, u64>::new();
    let timeout_status = StatusCode::REQUEST_TIMEOUT.as_u16();
    for metric in metrics {
        if metric.status != timeout_status {
            continue;
        }
        let count = timeouts.entry(metric.route.clone()).or_insert(0);
        *count = count.saturating_add(metric.count);
    }
    for (route, count) in timeouts {
        let route = prometheus_label_value(&route);
        let _ = writeln!(
            body,
            "foundation_api_http_request_timeout_total{{route=\"{route}\"}} {count}"
        );
    }
}

fn append_http_request_metrics(body: &mut String, metrics: &[ApiHttpRequestMetric]) {
    for metric in metrics {
        let method = prometheus_label_value(&metric.method);
        let route = prometheus_label_value(&metric.route);
        let _ = writeln!(
            body,
            "foundation_api_http_requests_total{{method=\"{}\",route=\"{}\",status=\"{}\"}} {}",
            method, route, metric.status, metric.count
        );
    }
}

fn append_lakehouse_batch_run_metrics(body: &mut String, metrics: &[LakehouseBatchRunMetric]) {
    for metric in metrics {
        let contract = prometheus_label_value(&metric.contract);
        let _ = writeln!(
            body,
            "foundation_platform_lakehouse_batch_last_success_created_at_seconds{{contract=\"{}\"}} {}",
            contract, metric.created_at_unix_seconds
        );
        let _ = writeln!(
            body,
            "foundation_platform_lakehouse_batch_last_success_recorded_at_seconds{{contract=\"{}\"}} {}",
            contract, metric.recorded_at_unix_seconds
        );
        let _ = writeln!(
            body,
            "foundation_platform_lakehouse_batch_last_success_row_count{{contract=\"{}\"}} {}",
            contract, metric.row_count
        );
    }
}

fn append_ingestion_run_metrics(body: &mut String, metrics: &[IngestionRunMetric]) {
    for metric in metrics {
        let source = prometheus_label_value(&metric.source_slug);
        let status = prometheus_label_value(&metric.status);
        let _ = writeln!(
            body,
            "foundation_platform_ingestion_run_last_finished_at_seconds{{source=\"{}\",status=\"{}\"}} {}",
            source, status, metric.finished_at_unix_seconds
        );
        let _ = writeln!(
            body,
            "foundation_platform_ingestion_run_last_duration_seconds{{source=\"{}\",status=\"{}\"}} {}",
            source, status, metric.duration_seconds
        );
        let _ = writeln!(
            body,
            "foundation_platform_ingestion_run_last_records_seen{{source=\"{}\",status=\"{}\"}} {}",
            source, status, metric.logical_records_seen
        );
        let _ = writeln!(
            body,
            "foundation_platform_ingestion_run_last_objects_written{{source=\"{}\",status=\"{}\"}} {}",
            source, status, metric.objects_written
        );
        let _ = writeln!(
            body,
            "foundation_platform_ingestion_run_last_raw_response_size_bytes{{source=\"{}\",status=\"{}\"}} {}",
            source, status, metric.raw_response_size_bytes
        );
    }
}

fn append_outbox_queue_metrics(body: &mut String, metrics: &[OutboxQueueMetric]) {
    for metric in metrics {
        let scope = prometheus_label_value(&metric.scope);
        let _ = writeln!(
            body,
            "foundation_platform_outbox_pending_event_count{{scope=\"{}\"}} {}",
            scope, metric.pending_event_count
        );
        let _ = writeln!(
            body,
            "foundation_platform_outbox_retry_event_count{{scope=\"{}\"}} {}",
            scope, metric.retry_event_count
        );
        let _ = writeln!(
            body,
            "foundation_platform_outbox_oldest_pending_age_seconds{{scope=\"{}\"}} {}",
            scope, metric.oldest_pending_age_seconds
        );
    }
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

// Route labels use axum 0.8 `{param}` template syntax; they are plain strings, not format args.
#[allow(clippy::literal_string_with_formatting_args)]
fn canonical_route_label(path: &str) -> String {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    match segments.as_slice() {
        [] => "/".to_owned(),
        ["healthz"] => "/healthz".to_owned(),
        ["readyz"] => "/readyz".to_owned(),
        ["metrics"] => "/metrics".to_owned(),
        ["catalog", "v1", "pipeline-graph"] => "/catalog/v1/pipeline-graph".to_owned(),
        ["catalog", "v1", "complexes"] => "/catalog/v1/complexes".to_owned(),
        ["catalog", "v1", "complexes", _] => "/catalog/v1/complexes/{id}".to_owned(),
        ["catalog", "v1", "complexes", _, "archive"] => {
            "/catalog/v1/complexes/{id}/archive".to_owned()
        }
        ["catalog", "v1", "complexes", _, "anchor-summary"] => {
            "/catalog/v1/complexes/{id}/anchor-summary".to_owned()
        }
        ["catalog", "v1", "complexes", _, "parcels"] => {
            "/catalog/v1/complexes/{id}/parcels".to_owned()
        }
        ["catalog", "v1", "complexes", _, "buildings"] => {
            "/catalog/v1/complexes/{id}/buildings".to_owned()
        }
        ["catalog", "v1", "parcels", "by-pnu", _, "buildings"] => {
            "/catalog/v1/parcels/by-pnu/{pnu}/buildings".to_owned()
        }
        ["catalog", "v1", "parcels", "by-pnu", _] => "/catalog/v1/parcels/by-pnu/{pnu}".to_owned(),
        ["catalog", "v1", "complexes", _, "manufacturers"] => {
            "/catalog/v1/complexes/{id}/manufacturers".to_owned()
        }
        ["catalog", "v1", "complexes", _, "notices"] => {
            "/catalog/v1/complexes/{id}/notices".to_owned()
        }
        ["catalog", "v1", "complexes", _, "attachments"] => {
            "/catalog/v1/complexes/{id}/attachments".to_owned()
        }
        ["catalog", "v1", "complexes", _, "blueprints"] => {
            "/catalog/v1/complexes/{id}/blueprints".to_owned()
        }
        ["catalog", "v1", "complexes", _, "spatial-layers"] => {
            "/catalog/v1/complexes/{id}/spatial-layers".to_owned()
        }
        ["catalog", "v1", "complexes", _, "digital-twin-assets"] => {
            "/catalog/v1/complexes/{id}/digital-twin-assets".to_owned()
        }
        ["catalog", "v1", "industry-groups"] => "/catalog/v1/industry-groups".to_owned(),
        ["catalog", "v1", "lakehouse", "batch-runs"] => {
            "/catalog/v1/lakehouse/batch-runs".to_owned()
        }
        ["catalog", "v1", "vector-tiles", "manifest"] => {
            "/catalog/v1/vector-tiles/manifest".to_owned()
        }
        ["map", "v1", "marker-tiles", "contract"] => "/map/v1/marker-tiles/contract".to_owned(),
        ["map", "v1", "marker-tiles", _, _, _, y_pbf] if is_pbf_tile_segment(y_pbf) => {
            "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf".to_owned()
        }
        ["catalog", "v1", "vector-tiles", action] if action.starts_with("manifest:") => {
            "/catalog/v1/vector-tiles/manifest:action".to_owned()
        }
        ["catalog", "v1", action] if action == &"parcel-marker-anchors:rebuild" => {
            "/catalog/v1/parcel-marker-anchors:rebuild".to_owned()
        }
        ["catalog", "v1", "parcels", _] => "/catalog/v1/parcels/{id}".to_owned(),
        ["catalog", "v1", "parcels", _, "industry-assignments"] => {
            "/catalog/v1/parcels/{id}/industry-assignments".to_owned()
        }
        ["catalog", "v1", "parcels", _, "kind"] => "/catalog/v1/parcels/{id}/kind".to_owned(),
        ["internal", "normalization", "proposals"] => {
            "/internal/normalization/proposals".to_owned()
        }
        ["catalog", "v1", "normalization", "proposals", _, action]
            if matches!(*action, "approve" | "reject" | "apply") =>
        {
            "/catalog/v1/normalization/proposals/{id}/{action}".to_owned()
        }
        ["catalog", "v1", "normalization", "applications", _, action]
            if matches!(*action, "rollback") =>
        {
            "/catalog/v1/normalization/applications/{id}/{action}".to_owned()
        }
        _ => path.to_owned(),
    }
}

fn is_pbf_tile_segment(segment: &str) -> bool {
    FsPath::new(segment)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("pbf"))
}

fn cors_layer_from_env() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(cors_allowed_origins_from_env()))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::OPTIONS,
        ])
        .allow_headers([header::ACCEPT, header::AUTHORIZATION, header::CONTENT_TYPE])
}

fn cors_allowed_origins_from_env() -> Vec<HeaderValue> {
    let raw = std::env::var("FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS").ok();
    let runtime_env = std::env::var(FOUNDATION_PLATFORM_RUNTIME_ENV).ok();
    cors_allowed_origins_from(raw.as_deref(), runtime_env.as_deref())
}

fn cors_allowed_origins_from(raw: Option<&str>, runtime_env: Option<&str>) -> Vec<HeaderValue> {
    let production = is_production_runtime(runtime_env);
    let raw = match raw.and_then(non_empty_trimmed) {
        Some(raw) => raw,
        None if production => {
            warn!(
                env = FOUNDATION_PLATFORM_RUNTIME_ENV,
                "production CORS has no explicit FOUNDATION_PLATFORM_CORS_ALLOWED_ORIGINS; allowing no origins"
            );
            return Vec::new();
        }
        None => DEFAULT_LOCAL_CORS_ALLOWED_ORIGINS,
    };

    let origins = parse_cors_origins(raw);

    if origins.is_empty() {
        if production {
            warn!("production CORS parsed no valid origins; allowing no origins");
            Vec::new()
        } else {
            parse_cors_origins(DEFAULT_LOCAL_CORS_ALLOWED_ORIGINS)
        }
    } else {
        origins
    }
}

fn parse_cors_origins(raw: &str) -> Vec<HeaderValue> {
    raw.split(',')
        .filter_map(|origin| {
            let origin = origin.trim();
            if origin.is_empty() {
                return None;
            }
            match HeaderValue::from_str(origin) {
                Ok(value) => Some(value),
                Err(err) => {
                    warn!(%origin, error = %err, "ignoring invalid CORS origin");
                    None
                }
            }
        })
        .collect::<Vec<_>>()
}

fn non_empty_trimmed(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn is_production_runtime(runtime_env: Option<&str>) -> bool {
    runtime_env.map(str::trim).is_some_and(|value| {
        value.eq_ignore_ascii_case("production") || value.eq_ignore_ascii_case("prod")
    })
}

#[cfg(test)]
mod tests;
