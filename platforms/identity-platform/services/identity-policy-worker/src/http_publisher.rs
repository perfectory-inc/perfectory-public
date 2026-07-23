//! Exact-endpoint HTTP adapter for validated Identity events.

use std::time::Duration;

use async_trait::async_trait;
use identity_contracts::IdentityEventDeliveryV1;
use reqwest::Url;
use thiserror::Error;

use crate::worker::{EventPublisher, PublishError, ValidatedOutboxEvent};

/// Stable receiver header carrying the outbox `event_id` idempotency key.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// Validated exact HTTP endpoint with no credentials, query, or fragment.
#[derive(Clone, Debug)]
pub struct PublisherEndpoint(Url);

impl PublisherEndpoint {
    /// Parses and validates an exact event receiver endpoint.
    ///
    /// # Errors
    /// Returns a bounded configuration error for invalid or ambiguous URLs.
    pub fn parse(value: &str) -> Result<Self, PublisherEndpointError> {
        let url = Url::parse(value).map_err(|_| PublisherEndpointError)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(PublisherEndpointError);
        }
        Ok(Self(url))
    }
}

/// Bounded exact-endpoint validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("publisher endpoint must be an exact HTTP or HTTPS URL")]
pub struct PublisherEndpointError;

/// HTTP implementation of the worker-local event publisher port.
#[derive(Clone)]
pub struct HttpEventPublisher {
    client: reqwest::Client,
    endpoint: PublisherEndpoint,
}

impl HttpEventPublisher {
    /// Builds an HTTP publisher with a per-request timeout.
    ///
    /// # Errors
    /// Returns a bounded error when the HTTP client cannot be built.
    pub fn new(
        endpoint: PublisherEndpoint,
        timeout: Duration,
    ) -> Result<Self, HttpPublisherBuildError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|_| HttpPublisherBuildError)?;
        Ok(Self { client, endpoint })
    }
}

#[async_trait]
impl EventPublisher for HttpEventPublisher {
    async fn publish(&self, event: &ValidatedOutboxEvent) -> Result<(), PublishError> {
        let response = self
            .client
            .post(self.endpoint.0.clone())
            .header(IDEMPOTENCY_KEY_HEADER, event.event_id.to_string())
            .json(&IdentityEventDeliveryV1 {
                event_id: event.event_id,
                event_type: event.event_type.clone(),
                occurred_at: event.occurred_at,
                payload: event.payload.clone(),
            })
            .send()
            .await
            .map_err(|_| PublishError::Request)?;
        let status = response.status();
        if !status.is_success() {
            return Err(PublishError::NonSuccessStatus {
                status: status.as_u16(),
            });
        }
        Ok(())
    }
}

/// Bounded HTTP client construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("publisher client construction failed")]
pub struct HttpPublisherBuildError;
