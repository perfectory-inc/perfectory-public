//! HTTP adapter for publishing validated Lakehouse lineage events.

use std::time::Duration;

use lakehouse_domain::validate_lakehouse_lineage_event;
use reqwest::Url;
use serde_json::Value;

use crate::errors::PublishError;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Publishes validated Lakehouse lineage events to an HTTP receiver.
#[derive(Clone, Debug)]
pub struct LakehouseLineagePublisher {
    client: reqwest::Client,
    endpoint: Url,
    auth_token: Option<String>,
}

impl LakehouseLineagePublisher {
    /// Creates a builder for [`LakehouseLineagePublisher`].
    #[must_use]
    pub const fn builder() -> LakehouseLineagePublisherBuilder {
        LakehouseLineagePublisherBuilder {
            endpoint: None,
            timeout: DEFAULT_TIMEOUT,
            auth_token: None,
        }
    }

    /// Validates and publishes one Lakehouse lineage event.
    ///
    /// # Errors
    /// Returns an infrastructure error when the lineage event contract is invalid
    /// or a broadcaster error when the receiver request fails or returns non-2xx.
    pub async fn publish(&self, event: &Value) -> Result<u16, PublishError> {
        Self::validate_event(event)?;

        let mut request = self.client.post(self.endpoint.clone()).json(event);
        if let Some(token) = &self.auth_token {
            request = request.bearer_auth(token);
        }

        let response = request.send().await.map_err(|error| {
            PublishError::Broadcaster(format!("lineage endpoint request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(PublishError::Broadcaster(format!(
                "lineage endpoint returned HTTP {}",
                status.as_u16()
            )));
        }

        tracing::info!(
            job_name = json_string(event, "job_name").unwrap_or("unknown"),
            run_id = json_string(event, "run_id").unwrap_or("unknown"),
            endpoint = %self.endpoint,
            "lakehouse lineage event published"
        );
        Ok(status.as_u16())
    }

    /// Validates one Lakehouse lineage event without performing network I/O.
    ///
    /// # Errors
    /// Returns an infrastructure error when the event violates the provider-neutral
    /// Lakehouse lineage contract.
    pub fn validate_event(event: &Value) -> Result<(), PublishError> {
        validate_lakehouse_lineage_event(event)
            .map_err(|error| PublishError::Infrastructure(error.to_string()))
    }
}

/// Builder for [`LakehouseLineagePublisher`].
#[derive(Clone, Debug)]
pub struct LakehouseLineagePublisherBuilder {
    endpoint: Option<Url>,
    timeout: Duration,
    auth_token: Option<String>,
}

impl LakehouseLineagePublisherBuilder {
    /// Sets the lineage receiver endpoint.
    ///
    /// Remote endpoints must use HTTPS. Plain HTTP is allowed only for loopback
    /// development endpoints.
    ///
    /// # Errors
    /// Returns an infrastructure error when the URL is invalid or insecure.
    pub fn endpoint(mut self, url: &str) -> Result<Self, PublishError> {
        let url = Url::parse(url.trim()).map_err(|error| {
            PublishError::Infrastructure(format!("invalid lineage endpoint URL: {error}"))
        })?;
        if !is_https_or_loopback_http(&url) {
            return Err(PublishError::Infrastructure(
                "lineage endpoint URL must use https unless it targets loopback development"
                    .to_owned(),
            ));
        }
        self.endpoint = Some(url);
        Ok(self)
    }

    /// Sets the per-request HTTP timeout.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets an optional bearer token for the lineage receiver.
    #[must_use]
    pub fn auth_token(mut self, token: &str) -> Self {
        let token = token.trim();
        if !token.is_empty() {
            self.auth_token = Some(token.to_owned());
        }
        self
    }

    /// Builds the publisher.
    ///
    /// # Errors
    /// Returns an infrastructure error if no endpoint is configured or the HTTP
    /// client cannot be constructed.
    pub fn build(self) -> Result<LakehouseLineagePublisher, PublishError> {
        let endpoint = self.endpoint.ok_or_else(|| {
            PublishError::Infrastructure("lineage endpoint is required".to_owned())
        })?;
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;

        Ok(LakehouseLineagePublisher {
            client,
            endpoint,
            auth_token: self.auth_token,
        })
    }
}

fn json_string<'a>(event: &'a Value, field: &str) -> Option<&'a str> {
    event.get(field).and_then(Value::as_str)
}

fn is_https_or_loopback_http(url: &Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => url
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1")),
        _ => false,
    }
}
