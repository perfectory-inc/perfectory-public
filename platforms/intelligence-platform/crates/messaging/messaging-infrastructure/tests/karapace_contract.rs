// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{OriginalUri, Path, State};
use axum::http::StatusCode;
use axum::routing::{post, put};
use axum::{Json, Router};
use intelligence_contracts::{schema_subject_for_topic, DEAD_LETTER_TOPIC};
use messaging_infrastructure::karapace::{KarapaceClient, KarapaceClientConfig, KarapaceError};
use serde_json::{json, Value};

#[tokio::test]
async fn karapace_client_sets_backward_transitive_compatibility_and_encodes_subject_path() {
    let captured = Arc::new(Mutex::new(None));
    let subject = format!("{} with space", schema_subject_for_topic(DEAD_LETTER_TOPIC));
    let base_url = spawn_server(
        Router::new()
            .route(
                "/config/{subject}",
                put(
                    |State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
                     Path(subject): Path<String>,
                     Json(body): Json<Value>| async move {
                        *captured.lock().unwrap() = Some(CapturedRequest { subject, body });
                        StatusCode::OK
                    },
                ),
            )
            .with_state(captured.clone()),
    )
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    client.set_backward_transitive(&subject).await.unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(captured.subject, subject);
    assert_eq!(captured.body["compatibility"], "BACKWARD_TRANSITIVE");
}

#[tokio::test]
async fn karapace_client_normalizes_zero_timeout_to_at_least_one_second() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_server(
        Router::new()
            .route(
                "/config/{subject}",
                put(
                    |State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
                     Path(subject): Path<String>,
                     Json(body): Json<Value>| async move {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        *captured.lock().unwrap() = Some(CapturedRequest { subject, body });
                        StatusCode::OK
                    },
                ),
            )
            .with_state(captured.clone()),
    )
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 0,
    })
    .unwrap();

    client
        .set_backward_transitive(&schema_subject_for_topic(DEAD_LETTER_TOPIC))
        .await
        .unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured.subject,
        schema_subject_for_topic(DEAD_LETTER_TOPIC)
    );
    assert_eq!(captured.body["compatibility"], "BACKWARD_TRANSITIVE");
}

#[tokio::test]
async fn karapace_client_accepts_empty_success_body_for_compatibility_update() {
    for status in [StatusCode::OK, StatusCode::CREATED] {
        let captured = Arc::new(Mutex::new(None));
        let base_url = spawn_empty_compatibility_server(captured.clone(), status).await;
        let client = KarapaceClient::new(KarapaceClientConfig {
            base_url,
            timeout_seconds: 5,
        })
        .unwrap();

        client
            .set_backward_transitive(&schema_subject_for_topic(DEAD_LETTER_TOPIC))
            .await
            .unwrap();

        let captured = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            captured.subject,
            schema_subject_for_topic(DEAD_LETTER_TOPIC)
        );
        assert_eq!(captured.body["compatibility"], "BACKWARD_TRANSITIVE");
    }
}

#[tokio::test]
async fn karapace_client_percent_encodes_reserved_subject_delimiters_in_request_uri() {
    let captured = Arc::new(Mutex::new(None));
    let subject = "team/alpha?stage#1".to_string();
    let base_url = spawn_encoded_subject_server(captured.clone()).await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    client.set_backward_transitive(&subject).await.unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(captured.subject, subject);
    assert_eq!(captured.body["compatibility"], "BACKWARD_TRANSITIVE");
    assert_eq!(captured.original_uri, "/config/team%2Falpha%3Fstage%231");
}

#[tokio::test]
async fn karapace_client_registers_avro_schema_and_returns_id_from_created_response() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_server(
        Router::new()
            .route(
                "/subjects/{subject}/versions",
                post(
                    |State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
                     Path(subject): Path<String>,
                     Json(body): Json<Value>| async move {
                        *captured.lock().unwrap() = Some(CapturedRequest { subject, body });
                        (StatusCode::CREATED, Json(json!({ "id": 17 })))
                    },
                ),
            )
            .with_state(captured.clone()),
    )
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();
    let schema_str = include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc");

    let schema_id = client
        .register_avro_schema(&schema_subject_for_topic(DEAD_LETTER_TOPIC), schema_str)
        .await
        .unwrap();

    assert_eq!(schema_id, 17);

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured.subject,
        schema_subject_for_topic(DEAD_LETTER_TOPIC)
    );
    assert_eq!(captured.body["schemaType"], "AVRO");
    assert!(captured.body["schema"]
        .as_str()
        .expect("schema must be a string")
        .contains("DeadLetterV1"));
}

#[tokio::test]
async fn karapace_client_rejects_non_success_status_with_body() {
    let base_url = spawn_server(Router::new().route(
        "/subjects/{subject}/versions",
        post(|| async move { (StatusCode::CONFLICT, "duplicate schema") }),
    ))
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    let error = client
        .register_avro_schema(
            &schema_subject_for_topic(DEAD_LETTER_TOPIC),
            include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc"),
        )
        .await
        .unwrap_err();

    match error {
        KarapaceError::Rejected { status, body } => {
            assert_eq!(status, 409);
            assert_eq!(body, "duplicate schema");
        }
        other => panic!("expected rejected error, got {other:?}"),
    }
}

#[tokio::test]
async fn karapace_client_rejects_invalid_json_response() {
    let base_url = spawn_server(Router::new().route(
        "/subjects/{subject}/versions",
        post(|| async move { (StatusCode::OK, "not-json") }),
    ))
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    let error = client
        .register_avro_schema(
            &schema_subject_for_topic(DEAD_LETTER_TOPIC),
            include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, KarapaceError::InvalidResponse { .. }));
}

#[tokio::test]
async fn karapace_client_rejects_missing_schema_id() {
    let base_url = spawn_server(Router::new().route(
        "/subjects/{subject}/versions",
        post(|| async move { (StatusCode::CREATED, Json(json!({}))) }),
    ))
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    let error = client
        .register_avro_schema(
            &schema_subject_for_topic(DEAD_LETTER_TOPIC),
            include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, KarapaceError::InvalidResponse { .. }));
}

#[tokio::test]
async fn karapace_client_rejects_non_integer_schema_id() {
    let base_url = spawn_server(Router::new().route(
        "/subjects/{subject}/versions",
        post(|| async move { (StatusCode::CREATED, Json(json!({ "id": "abc" }))) }),
    ))
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    let error = client
        .register_avro_schema(
            &schema_subject_for_topic(DEAD_LETTER_TOPIC),
            include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, KarapaceError::InvalidResponse { .. }));
}

#[tokio::test]
async fn karapace_client_rejects_out_of_range_schema_id() {
    let base_url = spawn_server(Router::new().route(
        "/subjects/{subject}/versions",
        post(|| async move {
            (
                StatusCode::CREATED,
                Json(json!({ "id": i64::from(i32::MAX) + 1 })),
            )
        }),
    ))
    .await;
    let client = KarapaceClient::new(KarapaceClientConfig {
        base_url,
        timeout_seconds: 5,
    })
    .unwrap();

    let error = client
        .register_avro_schema(
            &schema_subject_for_topic(DEAD_LETTER_TOPIC),
            include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, KarapaceError::InvalidResponse { .. }));
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    subject: String,
    body: Value,
}

#[derive(Clone, Debug)]
struct CapturedPathRequest {
    subject: String,
    body: Value,
    original_uri: String,
}

async fn spawn_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    format!("http://{address}")
}

async fn spawn_empty_compatibility_server(
    captured: Arc<Mutex<Option<CapturedRequest>>>,
    status: StatusCode,
) -> String {
    let router = Router::new()
        .route(
            "/config/{subject}",
            put(
                move |State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
                      Path(subject): Path<String>,
                      Json(body): Json<Value>| async move {
                    *captured.lock().unwrap() = Some(CapturedRequest { subject, body });
                    status
                },
            ),
        )
        .with_state(captured);

    spawn_server(router).await
}

async fn spawn_encoded_subject_server(captured: Arc<Mutex<Option<CapturedPathRequest>>>) -> String {
    let router = Router::new()
        .route(
            "/config/{subject}",
            put(
                |OriginalUri(original_uri): OriginalUri,
                 State(captured): State<Arc<Mutex<Option<CapturedPathRequest>>>>,
                 Path(subject): Path<String>,
                 Json(body): Json<Value>| async move {
                    *captured.lock().unwrap() = Some(CapturedPathRequest {
                        subject,
                        body,
                        original_uri: original_uri.to_string(),
                    });
                    StatusCode::OK
                },
            ),
        )
        .with_state(captured);

    spawn_server(router).await
}
