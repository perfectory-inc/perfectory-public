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
pub struct OpenAiCompatibleModelGatewayConfig {
    pub base_url: String,
    pub chat_path: String,
    pub api_key: Option<String>,
    pub default_model: String,
    pub timeout_seconds: u64,
}

#[derive(Clone)]
pub struct OpenAiCompatibleModelGateway {
    client: reqwest::Client,
    endpoint: String,
    api_key: Option<String>,
    default_model: String,
}

impl OpenAiCompatibleModelGateway {
    pub fn new(config: OpenAiCompatibleModelGatewayConfig) -> Result<Self, ModelGatewayError> {
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
impl ModelGateway for OpenAiCompatibleModelGateway {
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
        let payload = ChatCompletionRequest::from_gateway_request(model, request);
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

        let response: ChatCompletionResponse =
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
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatCompletionMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<ModelReasoningEffort>,
}

impl ChatCompletionRequest {
    fn from_gateway_request(model: String, request: ModelGatewayRequest) -> Self {
        Self {
            model,
            messages: request
                .messages
                .into_iter()
                .map(|message| ChatCompletionMessage {
                    role: role_name(message.role),
                    content: message.content,
                })
                .collect(),
            temperature: request.temperature,
            max_tokens: request.max_output_tokens,
            response_format: request.response_format,
            reasoning_effort: request.reasoning_effort,
        }
    }
}

#[derive(Debug, Serialize)]
struct ChatCompletionMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    model: Option<String>,
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

impl ChatCompletionResponse {
    fn into_gateway_response(self) -> Result<ModelGatewayResponse, ModelGatewayError> {
        let content = self
            .choices
            .first()
            .map(|choice| choice.message.content.clone())
            .filter(|content| !content.trim().is_empty())
            .ok_or_else(|| ModelGatewayError::InvalidResponse {
                message: "chat completion response had no content".to_string(),
            })?;

        Ok(ModelGatewayResponse {
            content,
            model_id: self.model.unwrap_or_else(|| "unknown".to_string()),
            usage: self.usage.map(Into::into),
            metadata: BTreeMap::new(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

#[derive(Debug, Deserialize)]
struct ChatMessageResponse {
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

impl From<ChatUsage> for ModelUsage {
    fn from(value: ChatUsage) -> Self {
        Self {
            input_tokens: value.prompt_tokens,
            output_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::join_url;

    #[test]
    fn joins_base_url_and_path_without_double_slashes() {
        assert_eq!(
            join_url("http://model-gateway/", "/v1/chat/completions"),
            "http://model-gateway/v1/chat/completions"
        );
    }
}
