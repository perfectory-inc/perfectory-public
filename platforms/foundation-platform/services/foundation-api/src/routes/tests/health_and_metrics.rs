use super::*;

#[tokio::test]
async fn health_returns_liveness_json_contract() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/json"))
    );

    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["service"], "foundation-api");
    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["version"], env!("CARGO_PKG_VERSION"));

    Ok(())
}

#[tokio::test]
async fn router_serves_pipeline_graph_registry() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/catalog/v1/pipeline-graph")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload["schema_version"],
        "foundation-platform.pipeline_graph.v1"
    );
    assert_eq!(
        payload["viewer_policy"]["canonical_store"],
        "foundation-platform"
    );
    assert!(payload["nodes"]
        .as_array()
        .is_some_and(|nodes| nodes.len() >= 32));

    Ok(())
}

#[tokio::test]
async fn register_complex_requires_bearer_token() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);
    let body = serde_json::json!({
        "official_complex_code": "1234567",
        "name": "테스트산단",
        "kind": "national",
        "primary_bjdong_code": "1111010100",
        "area_m2": 1000
    });

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/catalog/v1/complexes")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn update_parcel_kind_requires_bearer_token() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);
    let parcel_id = uuid::Uuid::now_v7();
    let body = serde_json::json!({ "new_kind": "factory", "if_match_version": 1 });

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/catalog/v1/parcels/{parcel_id}/kind"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[test]
fn readiness_status_maps_dependency_result_to_http_status() {
    assert_eq!(super::readiness_status(true), StatusCode::OK);
    assert_eq!(
        super::readiness_status(false),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn metrics_returns_prometheus_text_contract() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        app.oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())?,
        ),
    )
    .await??;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&HeaderValue::from_static(
            "text/plain; version=0.0.4; charset=utf-8"
        ))
    );

    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let payload = std::str::from_utf8(&body)?;
    assert!(payload.contains("# HELP foundation_api_up"));
    assert!(payload.contains("# TYPE foundation_api_up gauge"));
    assert!(payload.contains("foundation_api_up 1"));
    assert!(payload.contains("# HELP foundation_api_database_ready"));
    assert!(payload.contains("# TYPE foundation_api_database_ready gauge"));
    assert!(payload.contains("foundation_api_database_ready"));
    assert!(payload
        .contains("# HELP foundation_platform_lakehouse_batch_last_success_created_at_seconds"));
    assert!(
        payload.contains("# TYPE foundation_platform_lakehouse_batch_last_success_row_count gauge")
    );
    assert!(payload.contains("foundation_api_build_info"));
    assert!(payload.contains(env!("CARGO_PKG_VERSION")));

    Ok(())
}

#[tokio::test]
async fn router_records_http_request_metrics() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let health_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(health_response.status(), StatusCode::OK);

    let metrics_response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(metrics_response.status(), StatusCode::OK);
    let body = to_bytes(metrics_response.into_body(), usize::MAX).await?;
    let payload = std::str::from_utf8(&body)?;
    assert!(payload.contains(
        "foundation_api_http_requests_total{method=\"GET\",route=\"/healthz\",status=\"200\"} 1"
    ));
    assert!(payload.contains(
        "foundation_api_http_request_duration_seconds_bucket{method=\"GET\",route=\"/healthz\",status=\"200\",le=\"+Inf\"} 1"
    ));

    Ok(())
}

#[tokio::test]
async fn router_rejects_over_concurrency_and_records_overload_metric(
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let traffic = TrafficConfig {
        request_timeout_ms: 1_000,
        max_concurrency: 1,
        body_limit_bytes: 1_048_576,
    };
    let metrics_state = state.clone();
    let concurrency_state = TrafficMiddlewareState {
        app_state: state.clone(),
        traffic: TrafficRuntime::new(traffic),
    };
    let app = Router::new()
        .route("/slow-test", get(slow_test_handler))
        .route("/metrics", get(metrics))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            concurrency_state,
            enforce_concurrency_limit,
        ))
        .layer(middleware::from_fn_with_state(
            metrics_state,
            record_http_metrics,
        ));

    let first_app = app.clone();
    let first = tokio::spawn(async move {
        first_app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/slow-test")
                    .body(Body::empty())
                    .map_err(Box::<dyn Error + Send + Sync>::from)?,
            )
            .await
            .map_err(Box::<dyn Error + Send + Sync>::from)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let overloaded_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/slow-test")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(
        overloaded_response.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let _ = first
        .await
        .map_err(Box::<dyn Error + Send + Sync>::from)??;

    let metrics_response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())?,
        )
        .await?;
    let body = to_bytes(metrics_response.into_body(), usize::MAX).await?;
    let payload = std::str::from_utf8(&body)?;
    assert!(payload.contains("# HELP foundation_api_http_overload_rejected_total"));
    assert!(payload
        .contains("foundation_api_http_overload_rejected_total{reason=\"concurrency_limit\"} 1"));

    Ok(())
}

#[test]
fn metrics_body_renders_lakehouse_freshness_metrics() {
    let metrics = vec![LakehouseBatchRunMetric {
        contract: "silver.industrial_complexes".to_owned(),
        created_at_unix_seconds: 1_779_120_000,
        recorded_at_unix_seconds: 1_779_120_030,
        row_count: 42,
    }];

    let payload = super::metrics_body(
        true,
        &db_pool_metric(),
        super::MetricsBodyInput {
            lakehouse_batch_runs: &metrics,
            ..empty_metrics_body_input()
        },
    );

    assert!(payload.contains(
        "foundation_platform_lakehouse_batch_last_success_created_at_seconds{contract=\"silver.industrial_complexes\"} 1779120000"
    ));
    assert!(payload.contains(
        "foundation_platform_lakehouse_batch_last_success_recorded_at_seconds{contract=\"silver.industrial_complexes\"} 1779120030"
    ));
    assert!(payload.contains(
        "foundation_platform_lakehouse_batch_last_success_row_count{contract=\"silver.industrial_complexes\"} 42"
    ));
}

#[test]
fn metrics_body_renders_ingestion_run_metrics() {
    let metrics = vec![IngestionRunMetric {
        source_slug: "vworldkr__cadastral".to_owned(),
        status: "succeeded".to_owned(),
        finished_at_unix_seconds: 1_779_120_060,
        duration_seconds: 90,
        logical_records_seen: 500,
        objects_written: 5,
        raw_response_size_bytes: 65_536,
    }];

    let payload = super::metrics_body(
        true,
        &db_pool_metric(),
        super::MetricsBodyInput {
            ingestion_runs: &metrics,
            ..empty_metrics_body_input()
        },
    );

    assert!(payload.contains(
        "foundation_platform_ingestion_run_last_finished_at_seconds{source=\"vworldkr__cadastral\",status=\"succeeded\"} 1779120060"
    ));
    assert!(payload.contains(
        "foundation_platform_ingestion_run_last_duration_seconds{source=\"vworldkr__cadastral\",status=\"succeeded\"} 90"
    ));
    assert!(payload.contains(
        "foundation_platform_ingestion_run_last_records_seen{source=\"vworldkr__cadastral\",status=\"succeeded\"} 500"
    ));
    assert!(payload.contains(
        "foundation_platform_ingestion_run_last_objects_written{source=\"vworldkr__cadastral\",status=\"succeeded\"} 5"
    ));
    assert!(payload.contains(
        "foundation_platform_ingestion_run_last_raw_response_size_bytes{source=\"vworldkr__cadastral\",status=\"succeeded\"} 65536"
    ));
}

#[test]
fn metrics_body_renders_outbox_queue_metrics() {
    let metrics = vec![OutboxQueueMetric {
        scope: "catalog".to_owned(),
        pending_event_count: 3,
        retry_event_count: 2,
        oldest_pending_age_seconds: 600,
    }];

    let payload = super::metrics_body(
        true,
        &db_pool_metric(),
        super::MetricsBodyInput {
            outbox_queues: &metrics,
            ..empty_metrics_body_input()
        },
    );

    assert!(payload.contains("foundation_platform_outbox_pending_event_count{scope=\"catalog\"} 3"));
    assert!(payload.contains("foundation_platform_outbox_retry_event_count{scope=\"catalog\"} 2"));
    assert!(payload
        .contains("foundation_platform_outbox_oldest_pending_age_seconds{scope=\"catalog\"} 600"));
}

#[test]
fn slo_policy_only_targets_exported_catalog_outbox_scope() -> Result<(), Box<dyn Error>> {
    let policy: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../docs/observability/slo-policy.v1.example.json"
    ))?;
    let objectives = policy["objectives"]
        .as_array()
        .ok_or_else(|| std::io::Error::other("SLO policy objectives must be an array"))?;
    let outbox_objectives = objectives
        .iter()
        .filter(|objective| objective["scope_kind"] == "outbox_scope")
        .collect::<Vec<_>>();

    assert_eq!(outbox_objectives.len(), 1);
    let objective = outbox_objectives[0];
    assert_eq!(objective["id"], "outbox-catalog-fanout");
    assert_eq!(objective["scope"], "catalog");
    assert_eq!(
        objective["metric"],
        "foundation_platform_outbox_oldest_pending_age_seconds"
    );
    assert_eq!(objective["max_pending_age_seconds"], 600);
    assert_eq!(objective["severity"], "ticket");
    Ok(())
}

#[test]
fn metrics_body_renders_http_request_metrics() {
    let metrics = vec![
        ApiHttpRequestMetric {
            method: "GET".to_owned(),
            route: "/catalog/v1/pipeline-graph".to_owned(),
            status: 200,
            count: 7,
        },
        ApiHttpRequestMetric {
            method: "GET".to_owned(),
            route: "/catalog/v1/pipeline-graph".to_owned(),
            status: 408,
            count: 2,
        },
    ];

    let payload = super::metrics_body(
        true,
        &db_pool_metric(),
        super::MetricsBodyInput {
            http_requests: &metrics,
            ..empty_metrics_body_input()
        },
    );

    assert!(payload.contains("# HELP foundation_api_http_requests_total"));
    assert!(payload.contains("# TYPE foundation_api_http_requests_total counter"));
    assert!(payload.contains(
        "foundation_api_http_requests_total{method=\"GET\",route=\"/catalog/v1/pipeline-graph\",status=\"200\"} 7"
    ));
    assert!(payload.contains("# HELP foundation_api_http_request_timeout_total"));
    assert!(payload.contains("# TYPE foundation_api_http_request_timeout_total counter"));
    assert!(payload.contains(
        "foundation_api_http_request_timeout_total{route=\"/catalog/v1/pipeline-graph\"} 2"
    ));
}

#[test]
fn metrics_body_renders_database_pool_metrics() {
    let metric = ApiDatabasePoolMetric {
        pool_size: 8,
        idle_connections: 2,
        max_connections: 8,
    };

    let payload = super::metrics_body(true, &metric, empty_metrics_body_input());

    assert!(payload.contains("# HELP foundation_api_db_pool_size"));
    assert!(payload.contains("# TYPE foundation_api_db_pool_size gauge"));
    assert!(payload.contains("foundation_api_db_pool_size 8"));
    assert!(payload.contains("# HELP foundation_api_db_pool_idle_connections"));
    assert!(payload.contains("foundation_api_db_pool_idle_connections 2"));
    assert!(payload.contains("# HELP foundation_api_db_pool_max_connections"));
    assert!(payload.contains("foundation_api_db_pool_max_connections 8"));
}

#[test]
fn metrics_body_renders_http_duration_histogram_metrics() {
    let durations = vec![ApiHttpDurationMetric {
        method: "GET".to_owned(),
        route: "/healthz".to_owned(),
        status: 200,
        le: "0.1".to_owned(),
        count: 3,
    }];

    let payload = super::metrics_body(
        true,
        &db_pool_metric(),
        super::MetricsBodyInput {
            http_durations: &durations,
            ..empty_metrics_body_input()
        },
    );

    assert!(payload.contains("# HELP foundation_api_http_request_duration_seconds"));
    assert!(payload.contains("# TYPE foundation_api_http_request_duration_seconds histogram"));
    assert!(payload.contains(
        "foundation_api_http_request_duration_seconds_bucket{method=\"GET\",route=\"/healthz\",status=\"200\",le=\"0.1\"} 3"
    ));
}
