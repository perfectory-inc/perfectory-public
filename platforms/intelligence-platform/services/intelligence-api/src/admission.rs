use std::time::Duration;

use axum::response::IntoResponse;
use axum::{
    error_handling::HandleErrorLayer, extract::DefaultBodyLimit, http::StatusCode, BoxError, Json,
    Router,
};
use serde::Serialize;
use tower::ServiceBuilder;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionConfig {
    pub max_body_bytes: usize,
    pub request_timeout_seconds: u64,
    pub max_concurrency: usize,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 1_048_576,
            request_timeout_seconds: 30,
            max_concurrency: 128,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AdmissionErrorBody {
    code: &'static str,
    message: &'static str,
}

/// Parse admission config from environment.
///
/// Reads three variables (with defaults):
/// - `INTELLIGENCE_MAX_BODY_BYTES` (default 1 MiB)
/// - `INTELLIGENCE_REQUEST_TIMEOUT_SECONDS` (default 30)
/// - `INTELLIGENCE_MAX_CONCURRENCY` (default 128)
///
/// Returns `Err(String)` if any variable is non-numeric or zero.
pub fn admission_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<AdmissionConfig, String> {
    let max_body_bytes =
        parse_positive_usize(&lookup, "INTELLIGENCE_MAX_BODY_BYTES", 1_048_576_usize)?;

    let request_timeout_seconds =
        parse_positive_u64(&lookup, "INTELLIGENCE_REQUEST_TIMEOUT_SECONDS", 30_u64)?;

    let max_concurrency = parse_positive_usize(&lookup, "INTELLIGENCE_MAX_CONCURRENCY", 128_usize)?;

    Ok(AdmissionConfig {
        max_body_bytes,
        request_timeout_seconds,
        max_concurrency,
    })
}

fn parse_positive_usize(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: usize,
) -> Result<usize, String> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value: usize = raw
        .trim()
        .parse()
        .map_err(|e| format!("{key} is invalid: {e}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

fn parse_positive_u64(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: u64,
) -> Result<u64, String> {
    let Some(raw) = lookup(key) else {
        return Ok(default);
    };
    let value: u64 = raw
        .trim()
        .parse()
        .map_err(|e| format!("{key} is invalid: {e}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

/// Wrap `router` with admission-control layers.
///
/// Layer order (outermost → innermost for incoming requests):
///   DefaultBodyLimit → HandleErrorLayer → load_shed → global concurrency limit → timeout → handler
///
/// - `DefaultBodyLimit` enforces the byte cap at the extractor level (413 on overflow).
/// - `load_shed` sheds immediately when the concurrency semaphore is exhausted (→ 503 via
///   HandleErrorLayer).
/// - `timeout` fires after `request_timeout_seconds` (→ 504 via HandleErrorLayer).
///
/// # Global concurrency cap
///
/// `GlobalConcurrencyLimitLayer` holds a single `Arc<Semaphore>` shared across every route it
/// wraps.  axum's `Router::layer` applies the layer once per route endpoint, but because all of
/// those applications clone the same `Arc<Semaphore>`, the permit pool is shared — giving a true
/// process-wide cap rather than a per-route cap.
///
/// # Eager route instantiation
///
/// `with_state(())` is called before the layers so that any `BoxedHandler` endpoints are
/// converted to concrete `Route` objects. Without this, axum reconstructs a fresh service
/// chain (and a fresh semaphore) per dispatch, which breaks load-shedding.
/// After `with_state`, each dispatch *clones* the shared `Route` instead, so all concurrent
/// requests see the same semaphore.
pub fn apply_admission_layers(router: Router, config: &AdmissionConfig) -> Router {
    let router: Router<()> = router.with_state(());
    router
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_admission_error))
                .load_shed()
                .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                    config.max_concurrency,
                ))
                .timeout(Duration::from_secs(config.request_timeout_seconds)),
        )
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
}

async fn handle_admission_error(error: BoxError) -> axum::response::Response {
    if error.is::<tower::timeout::error::Elapsed>() {
        tracing::warn!("request deadline exceeded; returning 504");
        (
            StatusCode::GATEWAY_TIMEOUT,
            Json(AdmissionErrorBody {
                code: "request_deadline_exceeded",
                message: "request deadline exceeded",
            }),
        )
            .into_response()
    } else if error.is::<tower::load_shed::error::Overloaded>() {
        tracing::warn!("service saturated; shedding request with 503");
        let mut response = (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(AdmissionErrorBody {
                code: "load_shed",
                message: "service is saturated, retry later",
            }),
        )
            .into_response();
        response.headers_mut().insert(
            axum::http::header::HeaderName::from_static("retry-after"),
            axum::http::HeaderValue::from_static("1"),
        );
        response
    } else {
        tracing::error!(%error, "unclassified admission error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AdmissionErrorBody {
                code: "admission_failed",
                message: "admission layer failed",
            }),
        )
            .into_response()
    }
}
