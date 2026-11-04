//! HTTP webhook broadcaster for consumer cache invalidation fan-out.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use reqwest::Url;
use serde_json::json;
use sha2::Sha256;

use crate::{
    broadcaster::{EventBroadcaster, EventEnvelope},
    errors::PublishError,
    worker::OutboxScope,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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

    let digest = hmac_sha256(
        secret.as_bytes(),
        &[timestamp.as_bytes(), b".", body.as_bytes()],
    )
    .map_err(|error| PublishError::Infrastructure(format!("webhook signature key: {error}")))?;
    Ok(format!("{digest:x}"))
}

/// RFC 2104 HMAC-SHA256 over the concatenated `message` parts, from the `RustCrypto` `hmac` crate.
fn hmac_sha256(
    key: &[u8],
    message: &[&[u8]],
) -> Result<hmac::digest::Output<Sha256>, hmac::digest::InvalidLength> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)?;
    for part in message {
        mac.update(part);
    }
    Ok(mac.finalize().into_bytes())
}

const fn scope_name(scope: OutboxScope) -> &'static str {
    match scope {
        OutboxScope::Catalog => "catalog",
    }
}

#[cfg(test)]
mod tests {
    use super::hmac_sha256;

    type TestResult = Result<(), hmac::digest::InvalidLength>;

    fn hex(key: &[u8], message: &[u8]) -> Result<String, hmac::digest::InvalidLength> {
        Ok(format!("{:x}", hmac_sha256(key, &[message])?))
    }

    // RFC 4231 §4 HMAC-SHA-256 test cases (case 5 is a truncated output and is not used here).
    #[test]
    fn hmac_sha256_matches_rfc_4231_test_vectors() -> TestResult {
        assert_eq!(
            hex(&[0x0b; 20], b"Hi There")?,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hex(b"Jefe", b"what do ya want for nothing?")?,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(
            hex(&[0xaa; 20], &[0xdd; 50])?,
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
        let case_4_key: Vec<u8> = (0x01..=0x19).collect();
        assert_eq!(
            hex(&case_4_key, &[0xcd; 50])?,
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"
        );
        assert_eq!(
            hex(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )?,
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
        assert_eq!(
            hex(
                &[0xaa; 131],
                b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm."
            )?,
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2"
        );
        Ok(())
    }

    #[test]
    fn hmac_sha256_over_parts_equals_hmac_over_the_joined_message() -> TestResult {
        assert_eq!(
            hmac_sha256(b"Jefe", &[b"what do ya ", b"want for nothing?"])?,
            hmac_sha256(b"Jefe", &[b"what do ya want for nothing?"])?
        );
        Ok(())
    }
}
