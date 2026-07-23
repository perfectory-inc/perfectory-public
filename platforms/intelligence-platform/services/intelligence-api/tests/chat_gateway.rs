// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use intelligence_api::{app, state::AppState};
use intelligence_normalization_application::{
    ModelGateway, ModelGatewayError, ModelGatewayRequest, ModelGatewayResponse, ModelMessageRole,
    ModelReasoningEffort,
};
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn chat_completions_return_501_without_configured_model_gateway() {
    let response = app(AppState::default())
        .oneshot(json_post(
            "/v1/chat/completions",
            json!({
                "model": "gemma2:9b",
                "messages": [{"role": "user", "content": "한국어로 답해줘"}]
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let body = response_json(response).await;
    assert_eq!(body["code"], "chat_model_gateway_not_configured");
}

#[tokio::test]
async fn models_endpoint_lists_configured_chat_models_for_open_webui() {
    let state = AppState::default().with_chat_model_id("gemma2:9b");

    let response = app(state)
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["object"], "list");
    assert_eq!(body["data"][0]["id"], "gemma2:9b");
    assert_eq!(body["data"][0]["object"], "model");
    assert_eq!(body["data"][0]["owned_by"], "intelligence-platform");
}

#[tokio::test]
async fn chat_gateway_allows_browser_cors_preflight_for_open_webui_direct_connections() {
    let state = AppState::default().with_inbound_auth(intelligence_api::auth::InboundAuthConfig {
        required: false,
        shared_token: None,
        principal: None,
        allowed_origins: vec!["http://localhost:3000".into()],
    });
    let response = app(state)
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/v1/chat/completions")
                .header("origin", "http://localhost:3000")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "http://localhost:3000"
    );
}

#[tokio::test]
async fn chat_gateway_refuses_cors_preflight_from_unlisted_origin() {
    let state = AppState::default().with_inbound_auth(intelligence_api::auth::InboundAuthConfig {
        required: false,
        shared_token: None,
        principal: None,
        allowed_origins: vec!["http://localhost:3000".into()],
    });
    let response = app(state)
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/v1/chat/completions")
                .header("origin", "http://evil.example")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // CORS layer must NOT echo back an allow-origin for an unlisted origin.
    assert!(
        response
            .headers()
            .get("access-control-allow-origin")
            .is_none(),
        "evil.example must not receive an access-control-allow-origin header"
    );
}

#[tokio::test]
async fn chat_completions_inject_korean_policy_and_return_openai_shape() {
    let gateway = Arc::new(FakeGateway::new(vec!["한국어로만 작성한 최종 답변입니다."]));
    let state = AppState::default().with_model_gateway(gateway.clone());

    let response = app(state)
        .oneshot(json_post(
            "/v1/chat/completions",
            json!({
                "model": "gemma2:9b",
                "messages": [{"role": "user", "content": "간단히 소개해줘"}],
                "temperature": 0.2,
                "max_tokens": 256,
                "reasoning_effort": "none"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "한국어로만 작성한 최종 답변입니다."
    );
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["model"], "gemma2:9b");
    assert_eq!(body["metadata"]["model_profile_id"], "korean-default");
    assert_eq!(
        body["metadata"]["language_policy_id"],
        "ko-KR-answer-policy"
    );
    assert_eq!(
        body["metadata"]["validator_policy_id"],
        "ko-KR-output-validator"
    );
    assert_eq!(body["metadata"]["language_policy_passed"], true);
    assert_eq!(body["metadata"]["repair_attempted"], false);

    let requests = gateway.requests();
    assert_eq!(requests.len(), 1);
    let first = &requests[0];
    assert_eq!(first.profile_id, "korean-default");
    assert_eq!(first.model_id.as_deref(), Some("gemma2:9b"));
    assert_eq!(first.reasoning_effort, Some(ModelReasoningEffort::None));
    assert_eq!(first.messages[0].role, ModelMessageRole::System);
    assert!(first.messages[0].content.contains("한국어"));
    assert!(first.messages[0].content.contains("섞지 않는다"));
}

#[tokio::test]
async fn chat_completions_repair_non_korean_model_output() {
    let gateway = Arc::new(FakeGateway::new(vec![
        "This answer is only written in English.",
        "영어 문장을 한국어로 다시 작성한 답변입니다.",
    ]));
    let state = AppState::default().with_model_gateway(gateway.clone());

    let response = app(state)
        .oneshot(json_post(
            "/v1/chat/completions",
            json!({
                "model": "gemma2:9b",
                "messages": [{"role": "user", "content": "언어 정책 테스트"}]
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "영어 문장을 한국어로 다시 작성한 답변입니다."
    );
    assert_eq!(body["metadata"]["language_policy_passed"], true);
    assert_eq!(body["metadata"]["repair_attempted"], true);

    let requests = gateway.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages[1].content.contains("다시 작성"));
    assert!(requests[1].messages[1]
        .content
        .contains("This answer is only written in English."));
}

struct FakeGateway {
    responses: Mutex<VecDeque<String>>,
    requests: Mutex<Vec<ModelGatewayRequest>>,
}

impl FakeGateway {
    fn new(responses: Vec<&str>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(str::to_string).collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ModelGatewayRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl ModelGateway for FakeGateway {
    async fn chat(
        &self,
        request: ModelGatewayRequest,
    ) -> Result<ModelGatewayResponse, ModelGatewayError> {
        self.requests.lock().unwrap().push(request.clone());
        let content = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("fake gateway response must be configured");

        Ok(ModelGatewayResponse {
            content,
            model_id: request
                .model_id
                .unwrap_or_else(|| "fake-model:latest".to_string()),
            usage: None,
            metadata: BTreeMap::new(),
        })
    }
}

fn json_post(uri: &str, payload: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}
