use std::time::Duration;

use reqwest::{Client, Url};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KarapaceClientConfig {
    pub base_url: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Error)]
pub enum KarapaceError {
    #[error("{message}")]
    InvalidConfig { message: String },
    #[error("{message}")]
    Request { message: String },
    #[error("rejected with status {status}: {body}")]
    Rejected { status: u16, body: String },
    #[error("{message}")]
    InvalidResponse { message: String },
}

impl KarapaceError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidConfig { .. } => "karapace client invalid config",
            Self::Request { .. } => "karapace client transport error",
            Self::Rejected { .. } => "karapace client rejected request",
            Self::InvalidResponse { .. } => "karapace client invalid response",
        }
    }
}

#[derive(Clone, Debug)]
pub struct KarapaceClient {
    client: Client,
    base_url: Url,
}

impl KarapaceClient {
    pub fn new(config: KarapaceClientConfig) -> Result<Self, KarapaceError> {
        let base_url = config.base_url.trim().trim_end_matches('/').to_string();
        if base_url.is_empty() {
            return Err(KarapaceError::InvalidConfig {
                message: "base_url must not be empty".to_string(),
            });
        }

        let base_url = Url::parse(&base_url).map_err(|error| KarapaceError::InvalidConfig {
            message: format!("base_url is invalid: {error}"),
        })?;

        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds.max(1)))
            .build()
            .map_err(|error| KarapaceError::InvalidConfig {
                message: format!("failed to build client: {error}"),
            })?;

        Ok(Self { client, base_url })
    }

    pub async fn set_backward_transitive(&self, subject: &str) -> Result<(), KarapaceError> {
        let url = self.endpoint_url(&["config", subject])?;
        let response = self
            .client
            .put(url)
            .json(&json!({ "compatibility": "BACKWARD_TRANSITIVE" }))
            .send()
            .await
            .map_err(|error| KarapaceError::Request {
                message: error.to_string(),
            })?;

        self.ensure_success(response).await.map(|_| ())
    }

    pub async fn register_avro_schema(
        &self,
        subject: &str,
        schema_str: &str,
    ) -> Result<i32, KarapaceError> {
        let url = self.endpoint_url(&["subjects", subject, "versions"])?;
        let response = self
            .client
            .post(url)
            .json(&json!({
                "schemaType": "AVRO",
                "schema": schema_str,
            }))
            .send()
            .await
            .map_err(|error| KarapaceError::Request {
                message: error.to_string(),
            })?;

        let body = self.ensure_success(response).await?;
        let id = body.get("id").and_then(Value::as_i64).ok_or_else(|| {
            KarapaceError::InvalidResponse {
                message: "response missing integer id".to_string(),
            }
        })?;

        i32::try_from(id).map_err(|_| KarapaceError::InvalidResponse {
            message: "response id is out of range for i32".to_string(),
        })
    }

    async fn ensure_success(&self, response: reqwest::Response) -> Result<Value, KarapaceError> {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| KarapaceError::Request {
                message: error.to_string(),
            })?;

        if !matches!(status.as_u16(), 200 | 201) {
            return Err(KarapaceError::Rejected {
                status: status.as_u16(),
                body,
            });
        }

        if body.trim().is_empty() {
            return Ok(json!({}));
        }

        serde_json::from_str(&body).map_err(|error| KarapaceError::InvalidResponse {
            message: format!("response body was not valid JSON: {error}"),
        })
    }

    fn endpoint_url(&self, path_segments: &[&str]) -> Result<Url, KarapaceError> {
        let mut url = self.base_url.clone();
        {
            let mut segments =
                url.path_segments_mut()
                    .map_err(|_| KarapaceError::InvalidConfig {
                        message: "base_url must be a valid HTTP URL with path segments".to_string(),
                    })?;
            segments.pop_if_empty();
            for segment in path_segments {
                segments.push(segment);
            }
        }

        Ok(url)
    }
}
