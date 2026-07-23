// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use intelligence_normalization_application::{
    ModelGateway, ModelGatewayMessage, ModelGatewayRequest, ModelMessageRole, ModelReasoningEffort,
    ModelUsage,
};
use intelligence_normalization_infrastructure::openai_compatible::{
    OpenAiCompatibleModelGateway, OpenAiCompatibleModelGatewayConfig,
};
use serde_json::{json, Value};

#[tokio::test]
async fn posts_openai_compatible_chat_request_and_parses_response() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let gateway = OpenAiCompatibleModelGateway::new(OpenAiCompatibleModelGatewayConfig {
        base_url,
        chat_path: "/v1/chat/completions".to_string(),
        api_key: Some("model-token".to_string()),
        default_model: "gemma-ko".to_string(),
        timeout_seconds: 5,
    })
    .unwrap();

    let response = gateway.chat(model_request(None)).await.unwrap();

    assert_eq!(response.model_id, "gemma-ko");
    assert_eq!(response.content, "{\"normalized_name\":\"Acme\"}");
    assert_eq!(
        response.usage,
        Some(ModelUsage {
            input_tokens: 11,
            output_tokens: 7,
            total_tokens: 18,
        })
    );

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured.authorization,
        Some("Bearer model-token".to_string())
    );
    assert_eq!(captured.body["model"], "gemma-ko");
    assert_eq!(captured.body["messages"][0]["role"], "system");
    assert_eq!(captured.body["messages"][1]["role"], "user");
    assert_eq!(captured.body["response_format"]["type"], "json_object");
    assert_eq!(captured.body["max_tokens"], 256);
}

#[tokio::test]
async fn request_model_id_overrides_default_model() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let gateway = OpenAiCompatibleModelGateway::new(OpenAiCompatibleModelGatewayConfig {
        base_url,
        chat_path: "/v1/chat/completions".to_string(),
        api_key: None,
        default_model: "default-model".to_string(),
        timeout_seconds: 5,
    })
    .unwrap();

    gateway
        .chat(model_request(Some("tenant-specific-model")))
        .await
        .unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(captured.body["model"], "tenant-specific-model");
}

#[tokio::test]
async fn sends_reasoning_effort_when_requested() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let gateway = OpenAiCompatibleModelGateway::new(OpenAiCompatibleModelGatewayConfig {
        base_url,
        chat_path: "/v1/chat/completions".to_string(),
        api_key: None,
        default_model: "qwen3.6".to_string(),
        timeout_seconds: 5,
    })
    .unwrap();
    let mut request = model_request(None);
    request.reasoning_effort = Some(ModelReasoningEffort::None);

    gateway.chat(request).await.unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(captured.body["reasoning_effort"], "none");
}

#[tokio::test]
async fn maps_non_success_status_to_transport_error() {
    let base_url = spawn_rejection_server().await;
    let gateway = OpenAiCompatibleModelGateway::new(OpenAiCompatibleModelGatewayConfig {
        base_url,
        chat_path: "/v1/chat/completions".to_string(),
        api_key: None,
        default_model: "gemma-ko".to_string(),
        timeout_seconds: 5,
    })
    .unwrap();

    let error = gateway.chat(model_request(None)).await.unwrap_err();

    assert_eq!(error.safe_message(), "model gateway transport error");
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    authorization: Option<String>,
    body: Value,
}

async fn spawn_success_server(captured: Arc<Mutex<Option<CapturedRequest>>>) -> String {
    async fn handler(
        State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        *captured.lock().unwrap() = Some(CapturedRequest {
            authorization: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned),
            body,
        });

        Json(json!({
            "model": "gemma-ko",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "{\"normalized_name\":\"Acme\"}"
                    }
                }
            ],
            "usage": {
                "prompt_tokens": 11,
                "completion_tokens": 7,
                "total_tokens": 18
            }
        }))
    }

    spawn_server(
        Router::new()
            .route("/v1/chat/completions", post(handler))
            .with_state(captured),
    )
    .await
}

async fn spawn_rejection_server() -> String {
    async fn handler() -> (StatusCode, &'static str) {
        (StatusCode::BAD_GATEWAY, "model unavailable")
    }

    spawn_server(Router::new().route("/v1/chat/completions", post(handler))).await
}

async fn spawn_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    format!("http://{address}")
}

fn model_request(model_id: Option<&str>) -> ModelGatewayRequest {
    ModelGatewayRequest {
        profile_id: "normalization-ko".to_string(),
        model_id: model_id.map(ToOwned::to_owned),
        messages: vec![
            ModelGatewayMessage {
                role: ModelMessageRole::System,
                content: "Always answer in Korean.".to_string(),
            },
            ModelGatewayMessage {
                role: ModelMessageRole::User,
                content: "Normalize this record.".to_string(),
            },
        ],
        temperature: Some(0.1),
        max_output_tokens: Some(256),
        response_format: Some(json!({"type": "json_object"})),
        reasoning_effort: None,
        metadata: Default::default(),
    }
}
