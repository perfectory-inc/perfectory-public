// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use intelligence_normalization_application::{
    ModelGateway, ModelGatewayMessage, ModelGatewayRequest, ModelMessageRole, ModelReasoningEffort,
    ModelUsage,
};
use intelligence_normalization_infrastructure::ollama_native::{
    OllamaNativeModelGateway, OllamaNativeModelGatewayConfig,
};
use serde_json::{json, Value};

#[tokio::test]
async fn posts_ollama_native_chat_request_and_parses_response() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let gateway = OllamaNativeModelGateway::new(OllamaNativeModelGatewayConfig {
        base_url,
        chat_path: "/api/chat".to_string(),
        api_key: None,
        default_model: "qwen3.6".to_string(),
        timeout_seconds: 5,
    })
    .unwrap();

    let response = gateway.chat(model_request()).await.unwrap();

    assert_eq!(response.model_id, "qwen3.6");
    assert_eq!(
        response.content,
        "{\"proposed_record\":{\"floor_display_ko\":\"지상 1층\"},\"confidence\":0.91,\"reasons\":[\"ok\"]}"
    );
    assert_eq!(
        response.usage,
        Some(ModelUsage {
            input_tokens: 13,
            output_tokens: 7,
            total_tokens: 20,
        })
    );

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(captured.body["model"], "qwen3.6");
    assert_eq!(captured.body["messages"][0]["role"], "system");
    assert_eq!(captured.body["messages"][1]["role"], "user");
    assert_eq!(captured.body["think"], false);
    assert_eq!(captured.body["format"], "json");
    let temperature = captured.body["options"]["temperature"].as_f64().unwrap();
    assert!((temperature - 0.1).abs() < 0.000001);
    assert_eq!(captured.body["options"]["num_predict"], 256);
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    body: Value,
}

async fn spawn_success_server(captured: Arc<Mutex<Option<CapturedRequest>>>) -> String {
    async fn handler(
        State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        *captured.lock().unwrap() = Some(CapturedRequest { body });

        Json(json!({
            "model": "qwen3.6",
            "message": {
                "role": "assistant",
                "content": "{\"proposed_record\":{\"floor_display_ko\":\"지상 1층\"},\"confidence\":0.91,\"reasons\":[\"ok\"]}"
            },
            "done": true,
            "prompt_eval_count": 13,
            "eval_count": 7
        }))
    }

    spawn_server(
        Router::new()
            .route("/api/chat", post(handler))
            .with_state(captured),
    )
    .await
}

async fn spawn_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    format!("http://{address}")
}

fn model_request() -> ModelGatewayRequest {
    ModelGatewayRequest {
        profile_id: "normalization-ko".to_string(),
        model_id: None,
        messages: vec![
            ModelGatewayMessage {
                role: ModelMessageRole::System,
                content: "Output only JSON.".to_string(),
            },
            ModelGatewayMessage {
                role: ModelMessageRole::User,
                content: "Normalize this record.".to_string(),
            },
        ],
        temperature: Some(0.1),
        max_output_tokens: Some(256),
        response_format: Some(json!({"type": "json_object"})),
        reasoning_effort: Some(ModelReasoningEffort::None),
        metadata: Default::default(),
    }
}
