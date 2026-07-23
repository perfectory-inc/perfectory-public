use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelMessageRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ModelGatewayMessage {
    pub role: ModelMessageRole,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelReasoningEffort {
    None,
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelGatewayRequest {
    pub profile_id: String,
    #[serde(default)]
    pub model_id: Option<String>,
    pub messages: Vec<ModelGatewayMessage>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub response_format: Option<Value>,
    #[serde(default)]
    pub reasoning_effort: Option<ModelReasoningEffort>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ModelUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelGatewayResponse {
    pub content: String,
    pub model_id: String,
    #[serde(default)]
    pub usage: Option<ModelUsage>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum ModelGatewayError {
    #[error("model gateway is not configured")]
    NotConfigured,
    #[error("model gateway transport error")]
    Transport { message: String },
    #[error("model gateway returned invalid response")]
    InvalidResponse { message: String },
}

impl ModelGatewayError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::NotConfigured => "model gateway is not configured",
            Self::Transport { .. } => "model gateway transport error",
            Self::InvalidResponse { .. } => "model gateway returned invalid response",
        }
    }
}

#[async_trait]
pub trait ModelGateway: Send + Sync {
    async fn chat(
        &self,
        request: ModelGatewayRequest,
    ) -> Result<ModelGatewayResponse, ModelGatewayError>;
}
