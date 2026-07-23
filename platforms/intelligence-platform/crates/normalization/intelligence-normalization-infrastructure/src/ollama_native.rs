use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use intelligence_normalization_application::{
    ModelGateway, ModelGatewayError, ModelGatewayRequest, ModelGatewayResponse, ModelMessageRole,
    ModelReasoningEffort, ModelUsage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OllamaNativeModelGatewayConfig {
    pub base_url: String,
    pub chat_path: String,
    pub api_key: Option<String>,
    pub default_model: String,
    pub timeout_seconds: u64,
}

#[derive(Clone)]
pub struct OllamaNativeModelGateway {
    client: reqwest::Client,
    endpoint: String,
    api_key: Option<String>,
    default_model: String,
}

impl OllamaNativeModelGateway {
    pub fn new(config: OllamaNativeModelGatewayConfig) -> Result<Self, ModelGatewayError> {
        if config.base_url.trim().is_empty() {
            return Err(ModelGatewayError::InvalidResponse {
                message: "model gateway base_url is required".to_string(),
            });
        }
        if config.default_model.trim().is_empty() {
            return Err(ModelGatewayError::InvalidResponse {
                message: "model gateway default_model is required".to_string(),
            });
        }

        let timeout_seconds = config.timeout_seconds.max(1);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|error| ModelGatewayError::Transport {
                message: error.to_string(),
            })?;

        Ok(Self {
            client,
            endpoint: join_url(&config.base_url, &config.chat_path),
            api_key: config.api_key,
            default_model: config.default_model,
        })
    }
}

#[async_trait]
impl ModelGateway for OllamaNativeModelGateway {
    async fn chat(
        &self,
        request: ModelGatewayRequest,
    ) -> Result<ModelGatewayResponse, ModelGatewayError> {
        let model = request
            .model_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(&self.default_model)
            .to_string();
        let payload = OllamaChatRequest::from_gateway_request(model, request);
        let mut http_request = self.client.post(&self.endpoint).json(&payload);

        if let Some(api_key) = self.api_key.as_deref().filter(|value| !value.is_empty()) {
            http_request = http_request.bearer_auth(api_key);
        }

        let response = http_request
            .send()
            .await
            .map_err(|error| ModelGatewayError::Transport {
                message: error.to_string(),
            })?;
        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ModelGatewayError::Transport {
                message: format!(
                    "model gateway returned status {} with body {}",
                    status.as_u16(),
                    body
                ),
            });
        }

        let response: OllamaChatResponse =
            response
                .json()
                .await
                .map_err(|error| ModelGatewayError::InvalidResponse {
                    message: error.to_string(),
                })?;

        response.into_gateway_response()
    }
}

#[derive(Debug, Serialize)]
struct OllamaChatRequest {
    model: String,
    messages: Vec<OllamaChatMessage>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    think: Option<bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    options: BTreeMap<&'static str, Value>,
}

impl OllamaChatRequest {
    fn from_gateway_request(model: String, request: ModelGatewayRequest) -> Self {
        let mut options = BTreeMap::new();
        if let Some(temperature) = request.temperature {
            options.insert("temperature", Value::from(temperature));
        }
        if let Some(max_tokens) = request.max_output_tokens {
            options.insert("num_predict", Value::from(max_tokens));
        }

        Self {
            model,
            messages: request
                .messages
                .into_iter()
                .map(|message| OllamaChatMessage {
                    role: role_name(message.role),
                    content: message.content,
                })
                .collect(),
            stream: false,
            format: uses_json_object_response(&request.response_format).then_some("json"),
            think: matches!(request.reasoning_effort, Some(ModelReasoningEffort::None))
                .then_some(false),
            options,
        }
    }
}

#[derive(Debug, Serialize)]
struct OllamaChatMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Deserialize)]
struct OllamaChatResponse {
    model: Option<String>,
    message: OllamaChatResponseMessage,
    prompt_eval_count: Option<u32>,
    eval_count: Option<u32>,
}

impl OllamaChatResponse {
    fn into_gateway_response(self) -> Result<ModelGatewayResponse, ModelGatewayError> {
        let content = (!self.message.content.trim().is_empty())
            .then_some(self.message.content)
            .ok_or_else(|| ModelGatewayError::InvalidResponse {
                message: "chat response had no content".to_string(),
            })?;
        let usage = match (self.prompt_eval_count, self.eval_count) {
            (Some(input_tokens), Some(output_tokens)) => Some(ModelUsage {
                input_tokens,
                output_tokens,
                total_tokens: input_tokens + output_tokens,
            }),
            _ => None,
        };

        Ok(ModelGatewayResponse {
            content,
            model_id: self.model.unwrap_or_else(|| "unknown".to_string()),
            usage,
            metadata: BTreeMap::new(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct OllamaChatResponseMessage {
    content: String,
}

fn uses_json_object_response(response_format: &Option<Value>) -> bool {
    response_format
        .as_ref()
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        == Some("json_object")
}

fn role_name(role: ModelMessageRole) -> &'static str {
    match role {
        ModelMessageRole::System => "system",
        ModelMessageRole::User => "user",
        ModelMessageRole::Assistant => "assistant",
        ModelMessageRole::Tool => "tool",
    }
}

fn join_url(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}
