// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use intelligence_api::admission::{apply_admission_layers, AdmissionConfig};
use intelligence_api::state::AppState;
use tower::ServiceExt;

#[test]
fn admission_config_rejects_zero_concurrency() {
    let values = std::collections::BTreeMap::from([("INTELLIGENCE_MAX_CONCURRENCY", "0")]);

    let error = intelligence_api::admission::admission_config_from_lookup(|key| {
        values.get(key).map(|value| value.to_string())
    })
    .unwrap_err();

    assert!(error.contains("INTELLIGENCE_MAX_CONCURRENCY must be greater than zero"));
}

#[tokio::test]
async fn oversized_request_is_rejected_before_handler() {
    let state = AppState::default().with_required_inbound_auth("local-dev-token");
    let app = intelligence_api::app_with_admission(
        state,
        AdmissionConfig {
            max_body_bytes: 32,
            request_timeout_seconds: 30,
            max_concurrency: 128,
        },
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("authorization", "Bearer local-dev-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "service")
                .header("x-intelligence-tenant-id", "tenant-1")
                .header("x-intelligence-product-id", "foundation-platform")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"messages":[{"role":"user","content":"this body is intentionally too long"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn saturated_service_sheds_load_with_503() {
    let slow = Router::new().route(
        "/slow",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            "done"
        }),
    );
    let app = apply_admission_layers(
        slow,
        &AdmissionConfig {
            max_body_bytes: 1024,
            request_timeout_seconds: 30,
            max_concurrency: 1,
        },
    );

    let first = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = app
        .oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        second
            .headers()
            .get("retry-after")
            .unwrap()
            .to_str()
            .unwrap(),
        "1"
    );
    assert_eq!(first.await.unwrap().status(), StatusCode::OK);
}

/// Proves the semaphore is GLOBAL: saturating `/slow` must shed `/fast` (a different route).
///
/// With the old per-route `ConcurrencyLimitLayer` each route had its own semaphore, so `/fast`
/// would return 200 even while `/slow` held the only permit.  With `GlobalConcurrencyLimitLayer`
/// both routes share one `Arc<Semaphore>`, so `/fast` correctly receives 503.
#[tokio::test]
async fn saturation_on_one_route_sheds_other_routes() {
    let router = Router::new()
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                "done"
            }),
        )
        .route("/fast", get(|| async { "fast" }));
    let app = apply_admission_layers(
        router,
        &AdmissionConfig {
            max_body_bytes: 1024,
            request_timeout_seconds: 30,
            max_concurrency: 1,
        },
    );

    // Fire the slow request on a background task to hold the sole semaphore permit.
    let slow_handle = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
                .await
                .unwrap()
        }
    });

    // Wait long enough for the slow request to acquire the permit.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The fast route on a different path must be shed because the global semaphore is exhausted.
    let fast_response = app
        .oneshot(Request::builder().uri("/fast").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(fast_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(slow_handle.await.unwrap().status(), StatusCode::OK);
}

/// After the saturating request completes, the semaphore permit is released and
/// a subsequent request must succeed (proves the permit is returned on drop).
#[tokio::test]
async fn service_recovers_after_saturation() {
    let router = Router::new()
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                "done"
            }),
        )
        .route("/fast", get(|| async { "fast" }));
    let app = apply_admission_layers(
        router,
        &AdmissionConfig {
            max_body_bytes: 1024,
            request_timeout_seconds: 30,
            max_concurrency: 1,
        },
    );

    // First request: hold the permit.
    let slow_handle = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
                .await
                .unwrap()
        }
    });

    // Wait for slow request to acquire the permit.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second request (during saturation): should be shed.
    let shed = app
        .clone()
        .oneshot(Request::builder().uri("/fast").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(shed.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Wait for slow request to finish and release the permit.
    let slow_status = slow_handle.await.unwrap().status();
    assert_eq!(slow_status, StatusCode::OK);

    // Third request: permit is released, must succeed.
    let recovered = app
        .oneshot(Request::builder().uri("/fast").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
}

/// A small body (well within the limit) must NOT be rejected with 413.
#[tokio::test]
async fn body_under_limit_passes() {
    use axum::routing::post;

    let router = Router::new().route(
        "/echo",
        post(|body: axum::body::Bytes| async move { (StatusCode::OK, body) }),
    );
    let app = apply_admission_layers(
        router,
        &AdmissionConfig {
            max_body_bytes: 1024,
            request_timeout_seconds: 30,
            max_concurrency: 8,
        },
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("content-type", "application/octet-stream")
                .body(Body::from(b"hello".as_ref()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "small body must not be rejected"
    );
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn slow_handler_times_out_with_504() {
    let slow = Router::new().route(
        "/very-slow",
        get(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            "done"
        }),
    );
    let app = apply_admission_layers(
        slow,
        &AdmissionConfig {
            max_body_bytes: 1024,
            request_timeout_seconds: 1,
            max_concurrency: 8,
        },
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/very-slow")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(
        response.headers().get("retry-after").is_none(),
        "504 timeout responses must not include Retry-After"
    );
}
