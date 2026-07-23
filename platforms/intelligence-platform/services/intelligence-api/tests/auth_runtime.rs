// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use intelligence_api::state::AppState;
use intelligence_normalization_application::PermissionAction;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn healthz_is_public_but_chat_requires_auth() {
    let app =
        intelligence_api::app(AppState::default().with_required_inbound_auth("local-dev-token"));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let chat = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(chat.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bearer_token_and_scope_headers_allow_protected_route() {
    let app =
        intelligence_api::app(AppState::default().with_required_inbound_auth("local-dev-token"));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer local-dev-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "service")
                .header("x-intelligence-tenant-id", "tenant-1")
                .header("x-intelligence-product-id", "foundation-platform")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn wrong_bearer_token_is_rejected() {
    let app =
        intelligence_api::app(AppState::default().with_required_inbound_auth("local-dev-token"));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer wrong-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "service")
                .header("x-intelligence-tenant-id", "tenant-1")
                .header("x-intelligence-product-id", "foundation-platform")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_not_required_passes_through_for_local_dev() {
    let app = intelligence_api::app(AppState::default());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_scope_headers_cannot_override_bound_principal() {
    let app =
        intelligence_api::app(AppState::default().with_required_inbound_auth("local-dev-token"));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer local-dev-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "service")
                .header("x-intelligence-product-id", "foundation-platform")
                // deliberately omitting x-intelligence-tenant-id
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_principal_kind_cannot_override_bound_service_identity() {
    let app =
        intelligence_api::app(AppState::default().with_required_inbound_auth("local-dev-token"));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer local-dev-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "robot")
                .header("x-intelligence-tenant-id", "tenant-1")
                .header("x-intelligence-product-id", "foundation-platform")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn service_token_is_denied_when_its_bound_actions_do_not_include_route() {
    let app = intelligence_api::app(AppState::default().with_required_inbound_auth_actions(
        "local-dev-token",
        vec![PermissionAction::SubmitNormalizationProposal],
    ));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer local-dev-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = response_body_json(response).await;
    assert_eq!(body["code"], "authorization_failed");
}

#[tokio::test]
async fn composite_generate_validate_submit_requires_generation_and_submit_actions() {
    let app = intelligence_api::app(AppState::default().with_required_inbound_auth_actions(
        "local-dev-token",
        vec![PermissionAction::SubmitNormalizationProposal],
    ));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/intelligence/v1/normalization/generate-validate-submit")
                .header("authorization", "Bearer local-dev-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn wrong_token_error_body_is_generic() {
    let configured_token = "local-dev-token";
    let app =
        intelligence_api::app(AppState::default().with_required_inbound_auth(configured_token));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/models")
                .header("authorization", "Bearer totally-wrong-token")
                .header("x-intelligence-subject-id", "service:test-client")
                .header("x-intelligence-principal-kind", "service")
                .header("x-intelligence-tenant-id", "tenant-1")
                .header("x-intelligence-product-id", "foundation-platform")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_body_json(response).await;
    assert_eq!(body["code"], "authentication_failed");
    let message = body["message"].as_str().unwrap_or("");
    assert!(
        !message.contains(configured_token),
        "error message must not leak the configured token: {message}"
    );
}

async fn response_body_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[test]
fn non_loopback_bind_allowed_with_shared_token() {
    let values = std::collections::BTreeMap::from([
        ("INTELLIGENCE_API_BIND", "0.0.0.0:8010"),
        ("INTELLIGENCE_INBOUND_AUTH_MODE", "shared-token"),
        ("INTELLIGENCE_INBOUND_SERVICE_TOKEN", "some-token"),
        (
            "INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID",
            "service:test-client",
        ),
        ("INTELLIGENCE_INBOUND_SERVICE_TENANT_ID", "tenant-1"),
        (
            "INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID",
            "intelligence-platform",
        ),
        (
            "INTELLIGENCE_INBOUND_SERVICE_ACTIONS",
            "chat_completions,submit_normalization_proposal",
        ),
    ]);

    let config = intelligence_api::state::api_runtime_config_from_lookup(|key| {
        values.get(key).map(|value| value.to_string())
    })
    .unwrap();

    assert!(config.inbound_auth.required);
    assert_eq!(config.bind_address.to_string(), "0.0.0.0:8010");
}

#[test]
fn non_loopback_bind_requires_inbound_auth() {
    let values = std::collections::BTreeMap::from([
        ("INTELLIGENCE_API_BIND", "0.0.0.0:8010"),
        ("INTELLIGENCE_INBOUND_AUTH_MODE", "disabled"),
    ]);

    let error = intelligence_api::state::api_runtime_config_from_lookup(|key| {
        values.get(key).map(|value| value.to_string())
    })
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("inbound authentication is required"));
}

#[test]
fn loopback_bind_allows_auth_disabled_for_local_dev() {
    let values = std::collections::BTreeMap::from([
        ("INTELLIGENCE_API_BIND", "127.0.0.1:8010"),
        ("INTELLIGENCE_INBOUND_AUTH_MODE", "disabled"),
    ]);

    let config = intelligence_api::state::api_runtime_config_from_lookup(|key| {
        values.get(key).map(|value| value.to_string())
    })
    .unwrap();

    assert_eq!(config.bind_address.to_string(), "127.0.0.1:8010");
    assert!(!config.inbound_auth.required);
}

#[test]
fn inbound_auth_config_reads_required_token() {
    let values = std::collections::BTreeMap::from([
        ("INTELLIGENCE_INBOUND_AUTH_MODE", "shared-token"),
        ("INTELLIGENCE_INBOUND_SERVICE_TOKEN", "local-dev-token"),
        (
            "INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID",
            "service:test-client",
        ),
        ("INTELLIGENCE_INBOUND_SERVICE_TENANT_ID", "tenant-1"),
        (
            "INTELLIGENCE_INBOUND_SERVICE_PRODUCT_ID",
            "intelligence-platform",
        ),
        (
            "INTELLIGENCE_INBOUND_SERVICE_ACTIONS",
            "chat_completions,submit_normalization_proposal",
        ),
        ("INTELLIGENCE_CORS_ALLOWED_ORIGINS", "http://localhost:3000"),
    ]);

    let config = intelligence_api::auth::inbound_auth_config_from_lookup(|key| {
        values.get(key).map(|value| value.to_string())
    })
    .unwrap();

    assert!(config.required);
    assert_eq!(config.shared_token.as_deref(), Some("local-dev-token"));
    assert_eq!(
        config
            .principal
            .as_ref()
            .map(|principal| principal.subject_id.as_str()),
        Some("service:test-client")
    );
    assert_eq!(config.allowed_origins, vec!["http://localhost:3000"]);
}

#[test]
fn shared_token_requires_a_bound_principal_and_explicit_actions() {
    let values = std::collections::BTreeMap::from([
        ("INTELLIGENCE_INBOUND_AUTH_MODE", "shared-token"),
        ("INTELLIGENCE_INBOUND_SERVICE_TOKEN", "local-dev-token"),
    ]);

    let error = intelligence_api::auth::inbound_auth_config_from_lookup(|key| {
        values.get(key).map(|value| value.to_string())
    })
    .expect_err("a bearer token alone must not create an arbitrary principal");

    assert!(error
        .to_string()
        .contains("INTELLIGENCE_INBOUND_SERVICE_SUBJECT_ID"));
}
