use std::sync::OnceLock;

use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

pub fn install_metrics_recorder() -> Result<PrometheusHandle, String> {
    static METRICS_HANDLE: OnceLock<Result<PrometheusHandle, String>> = OnceLock::new();

    METRICS_HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .set_buckets_for_metric(
                    // Singular name per Prometheus convention; 30 s / 60 s buckets
                    // cover LLM chat requests running up to the 30 s deadline so that
                    // p95/p99 do not collapse into +Inf.
                    Matcher::Full("http_request_duration_seconds".to_string()),
                    &[
                        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
                    ],
                )
                .map_err(|error| error.to_string())?
                .install_recorder()
                .map_err(|error| error.to_string())
        })
        .clone()
}

/// Track HTTP request counts and latency.
///
/// Applied as the outermost layer so it counts shed 503s and timeouts too.
/// Admission layers and this middleware run *post-routing*, so shed 503s/504s
/// carry the real route template (e.g. `path="/v1/chat/completions"`).
/// Only genuine router misses (404 fallback handler) produce `path="unmatched"`.
pub async fn track_metrics(request: Request<axum::body::Body>, next: Next) -> Response {
    let start = std::time::Instant::now();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let method = request.method().clone();

    let response = next.run(request).await;

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", response.status().as_u16().to_string()),
    ];
    metrics::counter!("http_requests_total", &labels).increment(1);
    metrics::histogram!("http_request_duration_seconds", &labels)
        .record(start.elapsed().as_secs_f64());

    response
}
