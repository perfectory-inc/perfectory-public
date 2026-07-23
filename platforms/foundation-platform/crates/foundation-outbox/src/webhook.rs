//! HTTP webhook broadcaster for consumer cache invalidation fan-out.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use reqwest::Url;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    broadcaster::{EventBroadcaster, EventEnvelope},
    errors::PublishError,
    worker::OutboxScope,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const HMAC_SHA256_BLOCK_BYTES: usize = 64;

#[derive(Clone, Debug)]
/// Publishes outbox events to one or more HTTP webhook endpoints.
pub struct WebhookBroadcaster {
    client: reqwest::Client,
    endpoints: Vec<WebhookEndpoint>,
    signature_secret: WebhookSignatureSecret,
}

impl WebhookBroadcaster {
    /// Creates a builder for an HTTP webhook broadcaster.
    #[must_use]
    pub const fn builder() -> WebhookBroadcasterBuilder {
        WebhookBroadcasterBuilder {
            endpoints: Vec::new(),
            timeout: DEFAULT_TIMEOUT,
            signature_secret: None,
        }
    }
}

#[async_trait]
impl EventBroadcaster for WebhookBroadcaster {
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        for endpoint in &self.endpoints {
            let body = json!({
                "event_id": event.event_id,
                "event_type": event.event_type,
                "occurred_at": event.occurred_at.to_rfc3339_opts(SecondsFormat::Secs, true),
                "scope": scope_name(event.scope),
                "payload": event.payload,
            });
            let body_text = serde_json::to_string(&body).map_err(|error| {
                PublishError::Infrastructure(format!("webhook body serialization failed: {error}"))
            })?;
            let timestamp = Utc::now().timestamp().to_string();
            let signature = self.signature_secret.signature(&timestamp, &body_text)?;
            let mut request = self
                .client
                .post(endpoint.url.clone())
                .header("x-foundation-platform-event-id", event.event_id.to_string())
                .header(
                    "x-foundation-platform-event-type",
                    event.event_type.as_str(),
                )
                .header(
                    "x-foundation-platform-outbox-scope",
                    scope_name(event.scope),
                );
            if let Some(signature) = signature {
                request = request
                    .header("x-foundation-platform-timestamp", timestamp)
                    .header("x-foundation-platform-signature", signature);
            }

            let response = request
                .header("content-type", "application/json")
                .body(body_text)
                .send()
                .await
                .map_err(|error| {
                    PublishError::Broadcaster(format!(
                        "webhook endpoint '{}' request failed: {error}",
                        endpoint.name
                    ))
                })?;

            let status = response.status();
            if !status.is_success() {
                return Err(PublishError::Broadcaster(format!(
                    "webhook endpoint '{}' returned HTTP {}",
                    endpoint.name,
                    status.as_u16()
                )));
            }

            tracing::info!(
                endpoint = endpoint.name,
                event_id = %event.event_id,
                event_type = %event.event_type,
                scope = scope_name(event.scope),
                "outbox event delivered to webhook endpoint"
            );
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// Builder for [`WebhookBroadcaster`].
pub struct WebhookBroadcasterBuilder {
    endpoints: Vec<WebhookEndpoint>,
    timeout: Duration,
    signature_secret: Option<WebhookSignatureSecret>,
}

impl WebhookBroadcasterBuilder {
    /// Adds one webhook endpoint.
    ///
    /// Remote endpoints must use HTTPS. Plain HTTP is allowed only for loopback
    /// development endpoints.
    ///
    /// # Errors
    ///
    /// Returns an infrastructure error if the endpoint name is empty, the URL is
    /// invalid, or the URL is plain HTTP for a non-loopback host.
    pub fn endpoint(mut self, name: &str, url: &str) -> Result<Self, PublishError> {
        self.endpoints.push(WebhookEndpoint::parse(name, url)?);
        Ok(self)
    }

    /// Sets the per-request HTTP timeout.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the HMAC secret used to sign webhook request bodies.
    ///
    /// # Errors
    ///
    /// Returns an infrastructure error if the secret is empty or padded.
    pub fn signature_secret(mut self, secret: &str) -> Result<Self, PublishError> {
        self.signature_secret = Some(WebhookSignatureSecret::parse(secret)?);
        Ok(self)
    }

    /// Builds the broadcaster.
    ///
    /// # Errors
    ///
    /// Returns an infrastructure error if no endpoint is configured or if the
    /// HTTP client cannot be constructed.
    pub fn build(self) -> Result<WebhookBroadcaster, PublishError> {
        if self.endpoints.is_empty() {
            return Err(PublishError::Infrastructure(
                "at least one webhook endpoint is required".to_owned(),
            ));
        }

        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;

        Ok(WebhookBroadcaster {
            client,
            endpoints: self.endpoints,
            signature_secret: WebhookSignatureSecret::optional(self.signature_secret),
        })
    }
}

#[derive(Clone, Debug)]
struct WebhookSignatureSecret(Option<String>);

impl WebhookSignatureSecret {
    fn optional(secret: Option<Self>) -> Self {
        secret.unwrap_or(Self(None))
    }

    fn parse(secret: &str) -> Result<Self, PublishError> {
        if secret.trim() != secret || secret.is_empty() {
            return Err(PublishError::Infrastructure(
                "webhook signature secret must not be empty or padded".to_owned(),
            ));
        }
        Ok(Self(Some(secret.to_owned())))
    }

    fn signature(&self, timestamp: &str, body: &str) -> Result<Option<String>, PublishError> {
        self.0.as_deref().map_or(Ok(None), |secret| {
            sign_webhook_body(secret, timestamp, body).map(|hex| Some(format!("v1={hex}")))
        })
    }
}

#[derive(Clone, Debug)]
struct WebhookEndpoint {
    name: String,
    url: Url,
}

impl WebhookEndpoint {
    fn parse(name: &str, url: &str) -> Result<Self, PublishError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(PublishError::Infrastructure(
                "webhook endpoint name must not be empty".to_owned(),
            ));
        }

        let url = Url::parse(url.trim()).map_err(|error| {
            PublishError::Infrastructure(format!("invalid webhook endpoint URL: {error}"))
        })?;
        if !is_https_or_loopback_http(&url) {
            return Err(PublishError::Infrastructure(
                "webhook endpoint URL must use https unless it targets loopback development"
                    .to_owned(),
            ));
        }

        Ok(Self {
            name: name.to_owned(),
            url,
        })
    }
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

/// Produces the HMAC-SHA256 hex digest for the signed webhook payload.
///
/// The signed message is `<unix_seconds>.<raw_json_body>`.
///
/// # Errors
///
/// Returns an infrastructure error if the secret or timestamp is blank.
pub fn sign_webhook_body(
    secret: &str,
    timestamp: &str,
    body: &str,
) -> Result<String, PublishError> {
    if secret.is_empty() || timestamp.trim().is_empty() {
        return Err(PublishError::Infrastructure(
            "webhook signature secret and timestamp must not be empty".to_owned(),
        ));
    }

    let key = normalized_hmac_key(secret.as_bytes());
    let mut inner_pad = [0x36_u8; HMAC_SHA256_BLOCK_BYTES];
    let mut outer_pad = [0x5c_u8; HMAC_SHA256_BLOCK_BYTES];
    for (index, byte) in key.iter().enumerate() {
        inner_pad[index] ^= byte;
        outer_pad[index] ^= byte;
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(timestamp.as_bytes());
    inner.update(b".");
    inner.update(body.as_bytes());
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    Ok(hex_lower(&outer.finalize()))
}

fn normalized_hmac_key(key: &[u8]) -> Vec<u8> {
    if key.len() > HMAC_SHA256_BLOCK_BYTES {
        Sha256::digest(key).to_vec()
    } else {
        key.to_vec()
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}

const fn scope_name(scope: OutboxScope) -> &'static str {
    match scope {
        OutboxScope::Catalog => "catalog",
    }
}
