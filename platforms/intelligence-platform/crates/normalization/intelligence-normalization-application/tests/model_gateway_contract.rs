// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_application::{
    ModelGatewayMessage, ModelGatewayRequest, ModelGatewayResponse, ModelMessageRole, ModelUsage,
};
use serde_json::json;

#[test]
fn serializes_model_gateway_request_with_provider_neutral_shape() {
    let request = ModelGatewayRequest {
        profile_id: "normalization-ko".to_string(),
        model_id: Some("gemma-ko".to_string()),
        messages: vec![
            ModelGatewayMessage {
                role: ModelMessageRole::System,
                content: "Answer in Korean.".to_string(),
            },
            ModelGatewayMessage {
                role: ModelMessageRole::User,
                content: "Normalize this record.".to_string(),
            },
        ],
        temperature: Some(0.1),
        max_output_tokens: Some(512),
        response_format: Some(json!({"type": "json_object"})),
        reasoning_effort: None,
        metadata: Default::default(),
    };

    let value = serde_json::to_value(request).unwrap();

    assert_eq!(value["profile_id"], "normalization-ko");
    assert_eq!(value["model_id"], "gemma-ko");
    assert_eq!(value["messages"][0]["role"], "system");
    assert_eq!(value["messages"][1]["role"], "user");
    assert_eq!(value["response_format"]["type"], "json_object");
}

#[test]
fn deserializes_model_gateway_response_usage_and_metadata() {
    let response: ModelGatewayResponse = serde_json::from_value(json!({
        "content": "{\"normalized_name\":\"Acme\"}",
        "model_id": "gemma-ko",
        "usage": {
            "input_tokens": 10,
            "output_tokens": 5,
            "total_tokens": 15
        },
        "metadata": {
            "provider": "local"
        }
    }))
    .unwrap();

    assert_eq!(response.model_id, "gemma-ko");
    assert_eq!(
        response.usage,
        Some(ModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            total_tokens: 15,
        })
    );
    assert_eq!(response.metadata["provider"], "local");
}
