//! Opt-in Kafka delivery for the Foundation Postgres outbox.
//!
//! The database outbox remains the source of truth. This adapter only publishes the typed
//! `catalog.collection.raw_written.v1` claim-check event after the outbox worker has leased it;
//! retry and quarantine remain owned by [`crate::OutboxWorker`].

use std::{sync::Arc, time::Duration};

use apache_avro::{to_avro_datum, types::Value as AvroValue, Schema};
use async_trait::async_trait;
use foundation_shared_kernel::events::catalog_v1::{CatalogEvent, CollectionRawWrittenV1};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use reqwest::{Client, Url};
use serde_json::{json, Value};

use crate::{
    broadcaster::{EventBroadcaster, EventEnvelope},
    errors::PublishError,
};

/// The only event type this adapter owns during the initial rollout.
pub const RAW_WRITTEN_EVENT_TYPE: &str = "catalog.collection.raw_written.v1";
/// Canonical topic for Foundation collection claim-check events.
pub const DEFAULT_RAW_WRITTEN_TOPIC: &str = "foundation-platform.catalog.collection-raw-written.v1";
/// Append-only Avro schema for the Foundation raw-written envelope.
pub const RAW_WRITTEN_AVRO_SCHEMA: &str =
    include_str!("../../../schemas/foundation-platform.catalog.collection-raw-written.v1.avsc");

/// Configuration for the opt-in Foundation Kafka broadcaster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaBroadcasterConfig {
    /// Kafka bootstrap server list.
    pub bootstrap_servers: String,
    /// Karapace/Schema Registry base URL.
    pub schema_registry_url: String,
    /// Topic receiving raw-written claim-check events.
    pub raw_written_topic: String,
    /// Stable producer client id.
    pub client_id: String,
    /// Kafka delivery timeout in milliseconds.
    pub message_timeout_ms: u64,
    /// Schema Registry request timeout in seconds.
    pub schema_registry_timeout_seconds: u64,
    /// Broker security protocol.
    pub security_protocol: String,
    /// SASL mechanism when a SASL protocol is selected.
    pub sasl_mechanism: Option<String>,
    /// SASL username when a SASL protocol is selected.
    pub sasl_username: Option<String>,
    /// SASL password when a SASL protocol is selected.
    pub sasl_password: Option<String>,
    /// Optional broker CA file.
    pub ssl_ca_location: Option<String>,
    /// Optional broker client certificate file.
    pub ssl_certificate_location: Option<String>,
    /// Optional broker client key file.
    pub ssl_key_location: Option<String>,
    /// Optional Schema Registry username.
    pub schema_registry_username: Option<String>,
    /// Optional Schema Registry password.
    pub schema_registry_password: Option<String>,
    /// During migration, also publish to the existing webhook/logging broadcaster.
    pub dual_publish_legacy: bool,
}

impl KafkaBroadcasterConfig {
    /// Load the opt-in configuration from the Foundation environment.
    ///
    /// An unset or explicit false enable flag returns `None`, preserving the existing broadcaster
    /// composition. Enabling Kafka without its endpoints or with an incomplete security setup is
    /// an error rather than a silent fallback.
    ///
    /// # Errors
    ///
    /// Returns an infrastructure error when an environment variable is invalid or when Kafka
    /// is enabled without the required endpoints or security settings.
    pub fn from_env() -> Result<Option<Self>, PublishError> {
        let enabled = match std::env::var("FOUNDATION_PLATFORM_KAFKA_ENABLED") {
            Ok(value) => parse_bool("FOUNDATION_PLATFORM_KAFKA_ENABLED", &value)?,
            Err(std::env::VarError::NotPresent) => false,
            Err(error) => {
                return Err(PublishError::Infrastructure(format!(
                    "failed to read FOUNDATION_PLATFORM_KAFKA_ENABLED: {error}"
                )))
            }
        };
        if !enabled {
            return Ok(None);
        }

        let security_protocol =
            env_or_default("FOUNDATION_PLATFORM_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")?;
        let security_protocol = security_protocol.to_ascii_uppercase();
        if !matches!(
            security_protocol.as_str(),
            "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
        ) {
            return Err(config_error(format!(
                "FOUNDATION_PLATFORM_KAFKA_SECURITY_PROTOCOL must be PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL; got {security_protocol}"
            )));
        }

        let sasl_mechanism = optional_env("FOUNDATION_PLATFORM_KAFKA_SASL_MECHANISM")?;
        let sasl_username = optional_env("FOUNDATION_PLATFORM_KAFKA_SASL_USERNAME")?;
        let sasl_password = optional_env("FOUNDATION_PLATFORM_KAFKA_SASL_PASSWORD")?;
        if security_protocol.starts_with("SASL_")
            && (sasl_mechanism.is_none() || sasl_username.is_none() || sasl_password.is_none())
        {
            return Err(config_error(
                "SASL Kafka security requires mechanism, username, and password".to_owned(),
            ));
        }

        let runtime_environment = required_env("FOUNDATION_PLATFORM_RUNTIME_ENV")?;
        let config = Self {
            bootstrap_servers: required_env("FOUNDATION_PLATFORM_KAFKA_BOOTSTRAP_SERVERS")?,
            schema_registry_url: required_env("FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_URL")?,
            raw_written_topic: env_or_default(
                "FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC",
                DEFAULT_RAW_WRITTEN_TOPIC,
            )?,
            client_id: env_or_default(
                "FOUNDATION_PLATFORM_KAFKA_CLIENT_ID",
                "foundation-outbox-publisher",
            )?,
            message_timeout_ms: positive_u64_env(
                "FOUNDATION_PLATFORM_KAFKA_MESSAGE_TIMEOUT_MS",
                30_000,
            )?,
            schema_registry_timeout_seconds: positive_u64_env(
                "FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_TIMEOUT_SECONDS",
                10,
            )?,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            sasl_password,
            ssl_ca_location: optional_env("FOUNDATION_PLATFORM_KAFKA_SSL_CA_LOCATION")?,
            ssl_certificate_location: optional_env(
                "FOUNDATION_PLATFORM_KAFKA_SSL_CERTIFICATE_LOCATION",
            )?,
            ssl_key_location: optional_env("FOUNDATION_PLATFORM_KAFKA_SSL_KEY_LOCATION")?,
            schema_registry_username: optional_env(
                "FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_USERNAME",
            )?,
            schema_registry_password: optional_env(
                "FOUNDATION_PLATFORM_KAFKA_SCHEMA_REGISTRY_PASSWORD",
            )?,
            dual_publish_legacy: parse_bool(
                "FOUNDATION_PLATFORM_KAFKA_DUAL_PUBLISH_LEGACY",
                &env_or_default("FOUNDATION_PLATFORM_KAFKA_DUAL_PUBLISH_LEGACY", "true")?,
            )?,
        };
        config.validate()?;
        validate_runtime_target(
            &runtime_environment,
            &config.security_protocol,
            &config.bootstrap_servers,
            &config.schema_registry_url,
        )?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<(), PublishError> {
        for (name, value) in [
            ("bootstrap_servers", self.bootstrap_servers.as_str()),
            ("schema_registry_url", self.schema_registry_url.as_str()),
            ("raw_written_topic", self.raw_written_topic.as_str()),
            ("client_id", self.client_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(config_error(format!("{name} must not be empty")));
            }
        }
        Url::parse(&self.schema_registry_url).map_err(|error| {
            config_error(format!("schema_registry_url is not a valid URL: {error}"))
        })?;
        if self.message_timeout_ms == 0 || self.schema_registry_timeout_seconds == 0 {
            return Err(config_error(
                "Kafka and Schema Registry timeouts must be greater than zero".to_owned(),
            ));
        }
        match (
            self.schema_registry_username.as_ref(),
            self.schema_registry_password.as_ref(),
        ) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(config_error(
                    "Schema Registry username and password must be provided together".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// Minimal producer seam used by the broadcaster and by contract tests.
#[async_trait]
pub trait KafkaPayloadPublisher: Send + Sync {
    /// Publish one already-encoded Kafka value.
    async fn publish(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), PublishError>;
}

/// Foundation outbox broadcaster that publishes only `raw_written` to Kafka.
pub struct KafkaEventBroadcaster<P> {
    publisher: P,
    topic: String,
    schema: Schema,
    schema_id: i32,
    fallback: Arc<dyn EventBroadcaster>,
    dual_publish_legacy: bool,
}

impl<P> KafkaEventBroadcaster<P>
where
    P: KafkaPayloadPublisher,
{
    /// Create a broadcaster from a registered schema and a Kafka publisher.
    ///
    /// # Errors
    ///
    /// Returns an infrastructure error when the configured topic or schema metadata is invalid.
    pub fn new(
        publisher: P,
        topic: impl Into<String>,
        schema: Schema,
        schema_id: i32,
        fallback: Arc<dyn EventBroadcaster>,
        dual_publish_legacy: bool,
    ) -> Result<Self, PublishError> {
        let topic = topic.into();
        if topic.trim().is_empty() {
            return Err(config_error("Kafka topic must not be empty".to_owned()));
        }
        if schema_id <= 0 {
            return Err(config_error("Kafka schema id must be positive".to_owned()));
        }
        Ok(Self {
            publisher,
            topic,
            schema,
            schema_id,
            fallback,
            dual_publish_legacy,
        })
    }
}

#[async_trait]
impl<P> EventBroadcaster for KafkaEventBroadcaster<P>
where
    P: KafkaPayloadPublisher,
{
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        if event.event_type != RAW_WRITTEN_EVENT_TYPE {
            return self.fallback.publish(event).await;
        }

        let (key, payload) = encode_raw_written_event(&self.schema, self.schema_id, event)?;
        self.publisher.publish(&self.topic, &key, &payload).await?;
        if self.dual_publish_legacy {
            self.fallback.publish(event).await?;
        }
        Ok(())
    }
}

/// Concrete librdkafka publisher used by the Foundation service.
pub struct RdkafkaKafkaPublisher {
    producer: FutureProducer,
    message_timeout: Duration,
}

impl RdkafkaKafkaPublisher {
    /// Create a librdkafka producer from an already validated configuration.
    ///
    /// Producer construction is local and does not perform schema registration; callers that
    /// need the production environment composition should use [`from_env`]. This narrow seam is
    /// also used by the live outage contract test to exercise the real delivery failure path.
    ///
    /// # Errors
    ///
    /// Returns an infrastructure error when librdkafka cannot construct the producer.
    pub fn from_config(config: &KafkaBroadcasterConfig) -> Result<Self, PublishError> {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", &config.client_id)
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .set("allow.auto.create.topics", "false")
            .set("security.protocol", &config.security_protocol);
        if let Some(mechanism) = &config.sasl_mechanism {
            client.set("sasl.mechanisms", mechanism);
        }
        if let Some(username) = &config.sasl_username {
            client.set("sasl.username", username);
        }
        if let Some(password) = &config.sasl_password {
            client.set("sasl.password", password);
        }
        if let Some(location) = &config.ssl_ca_location {
            client.set("ssl.ca.location", location);
        }
        if let Some(location) = &config.ssl_certificate_location {
            client.set("ssl.certificate.location", location);
        }
        if let Some(location) = &config.ssl_key_location {
            client.set("ssl.key.location", location);
        }
        let producer = client.create::<FutureProducer>().map_err(|error| {
            broadcaster_error(format!("failed to create Kafka producer: {error}"))
        })?;
        Ok(Self {
            producer,
            message_timeout: Duration::from_millis(config.message_timeout_ms),
        })
    }
}

#[async_trait]
impl KafkaPayloadPublisher for RdkafkaKafkaPublisher {
    async fn publish(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), PublishError> {
        self.producer
            .send(
                FutureRecord::to(topic).key(key).payload(payload),
                self.message_timeout,
            )
            .await
            .map(|_| ())
            .map_err(|(error, _)| broadcaster_error(format!("Kafka delivery failed: {error}")))
    }
}

/// Build the production broadcaster and register the append-only Avro schema in Karapace.
///
/// # Errors
///
/// Returns an infrastructure error when Kafka or Schema Registry configuration is invalid, or
/// when schema registration fails.
pub async fn from_env(
    fallback: Arc<dyn EventBroadcaster>,
) -> Result<Option<KafkaEventBroadcaster<RdkafkaKafkaPublisher>>, PublishError> {
    let Some(config) = KafkaBroadcasterConfig::from_env()? else {
        return Ok(None);
    };
    let schema = Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA)
        .map_err(|error| broadcaster_error(format!("invalid raw-written Avro schema: {error}")))?;
    let subject = format!("{}-value", config.raw_written_topic);
    let registry = SchemaRegistryClient::new(&config)?;
    registry.set_backward_transitive(&subject).await?;
    let schema_id = registry.register(&subject, RAW_WRITTEN_AVRO_SCHEMA).await?;
    let publisher = RdkafkaKafkaPublisher::from_config(&config)?;
    Ok(Some(KafkaEventBroadcaster::new(
        publisher,
        config.raw_written_topic,
        schema,
        schema_id,
        fallback,
        config.dual_publish_legacy,
    )?))
}

fn encode_raw_written_event(
    schema: &Schema,
    schema_id: i32,
    event: &EventEnvelope,
) -> Result<(String, Vec<u8>), PublishError> {
    let catalog_event: CatalogEvent =
        serde_json::from_value(event.payload.clone()).map_err(|error| {
            broadcaster_error(format!(
                "raw_written payload is not a CatalogEvent: {error}"
            ))
        })?;
    let CatalogEvent::CollectionRawWritten(raw_written) = catalog_event else {
        return Err(broadcaster_error(
            "raw_written event payload has a different CatalogEvent variant".to_owned(),
        ));
    };
    let key = raw_written.scope_unit_id.clone();
    let value = raw_written_to_avro_value(event, &raw_written)?;
    let datum = to_avro_datum(schema, value)
        .map_err(|error| broadcaster_error(format!("raw_written Avro encoding failed: {error}")))?;
    let mut wire = Vec::with_capacity(5 + datum.len());
    wire.push(0);
    wire.extend_from_slice(&schema_id.to_be_bytes());
    wire.extend_from_slice(&datum);
    Ok((key, wire))
}

fn raw_written_to_avro_value(
    envelope: &EventEnvelope,
    event: &CollectionRawWrittenV1,
) -> Result<AvroValue, PublishError> {
    let schema_version = i32::try_from(event.schema_version)
        .map_err(|_| broadcaster_error("schema_version exceeds Avro int range".to_owned()))?;
    Ok(AvroValue::Record(vec![
        (
            "event_id".to_owned(),
            AvroValue::String(envelope.event_id.to_string()),
        ),
        (
            "event_type".to_owned(),
            AvroValue::String(RAW_WRITTEN_EVENT_TYPE.to_owned()),
        ),
        (
            "specversion".to_owned(),
            AvroValue::String("1.0".to_owned()),
        ),
        (
            "source".to_owned(),
            AvroValue::String("/foundation-platform/collection".to_owned()),
        ),
        ("schema_version".to_owned(), AvroValue::Int(schema_version)),
        (
            "collection_snapshot_id".to_owned(),
            AvroValue::String(event.collection_snapshot_id.clone()),
        ),
        ("job_id".to_owned(), AvroValue::String(event.job_id.clone())),
        (
            "scope_unit_id".to_owned(),
            AvroValue::String(event.scope_unit_id.clone()),
        ),
        (
            "provider".to_owned(),
            AvroValue::String(event.provider.clone()),
        ),
        (
            "endpoint".to_owned(),
            AvroValue::String(event.endpoint.clone()),
        ),
        (
            "endpoint_slug".to_owned(),
            AvroValue::String(event.endpoint_slug.clone()),
        ),
        (
            "bronze_object_key".to_owned(),
            AvroValue::String(event.bronze_object_key.clone()),
        ),
        (
            "bronze_object_count".to_owned(),
            avro_long(event.bronze_object_count, "bronze_object_count")?,
        ),
        (
            "bronze_checksum_sha256".to_owned(),
            AvroValue::String(event.bronze_checksum_sha256.clone()),
        ),
        (
            "bronze_size_bytes".to_owned(),
            avro_long(event.bronze_size_bytes, "bronze_size_bytes")?,
        ),
        (
            "source_record_count".to_owned(),
            avro_long(event.source_record_count, "source_record_count")?,
        ),
        (
            "request_count".to_owned(),
            avro_long(event.request_count, "request_count")?,
        ),
        (
            "request_fingerprint_sha256".to_owned(),
            AvroValue::String(event.request_fingerprint_sha256.clone()),
        ),
        (
            "request_fingerprint_schema_version".to_owned(),
            AvroValue::String(event.request_fingerprint_schema_version.clone()),
        ),
        (
            "license".to_owned(),
            nullable_string(event.license.as_deref()),
        ),
        ("srid".to_owned(), nullable_string(event.srid.as_deref())),
        (
            "reused_bronze_object".to_owned(),
            AvroValue::Boolean(event.reused_bronze_object),
        ),
        (
            "fetched_at_utc".to_owned(),
            AvroValue::Long(event.fetched_at_utc.timestamp_millis()),
        ),
        (
            "event_occurred_at".to_owned(),
            AvroValue::Long(event.occurred_at.timestamp_millis()),
        ),
        (
            "outbox_occurred_at".to_owned(),
            AvroValue::Long(envelope.occurred_at.timestamp_millis()),
        ),
    ]))
}

fn avro_long(value: u64, field: &str) -> Result<AvroValue, PublishError> {
    i64::try_from(value)
        .map(AvroValue::Long)
        .map_err(|_| broadcaster_error(format!("{field} exceeds Avro long range")))
}

fn nullable_string(value: Option<&str>) -> AvroValue {
    value.map_or(AvroValue::Null, |value| AvroValue::String(value.to_owned()))
}

struct SchemaRegistryClient {
    client: Client,
    base_url: Url,
    username: Option<String>,
    password: Option<String>,
}

impl SchemaRegistryClient {
    fn new(config: &KafkaBroadcasterConfig) -> Result<Self, PublishError> {
        let base_url = Url::parse(config.schema_registry_url.trim().trim_end_matches('/'))
            .map_err(|error| config_error(format!("invalid Schema Registry URL: {error}")))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(config.schema_registry_timeout_seconds))
            .build()
            .map_err(|error| {
                broadcaster_error(format!("failed to build Schema Registry client: {error}"))
            })?;
        Ok(Self {
            client,
            base_url,
            username: config.schema_registry_username.clone(),
            password: config.schema_registry_password.clone(),
        })
    }

    async fn set_backward_transitive(&self, subject: &str) -> Result<(), PublishError> {
        let response = self
            .request(self.client.put(self.endpoint(&["config", subject])?))
            .json(&json!({"compatibility": "BACKWARD_TRANSITIVE"}))
            .send()
            .await
            .map_err(|error| {
                broadcaster_error(format!(
                    "Schema Registry compatibility request failed: {error}"
                ))
            })?;
        ensure_registry_success(response).await.map(|_| ())
    }

    async fn register(&self, subject: &str, schema: &str) -> Result<i32, PublishError> {
        let response = self
            .request(
                self.client
                    .post(self.endpoint(&["subjects", subject, "versions"])?),
            )
            .json(&json!({"schemaType": "AVRO", "schema": schema}))
            .send()
            .await
            .map_err(|error| {
                broadcaster_error(format!(
                    "Schema Registry registration request failed: {error}"
                ))
            })?;
        let body = ensure_registry_success(response).await?;
        let id = body.get("id").and_then(Value::as_i64).ok_or_else(|| {
            broadcaster_error("Schema Registry response has no integer id".to_owned())
        })?;
        i32::try_from(id)
            .map_err(|_| broadcaster_error("Schema Registry id exceeds i32 range".to_owned()))
    }

    fn request(&self, mut request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            request = request.basic_auth(username, Some(password));
        }
        request
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, PublishError> {
        let mut url = self.base_url.clone();
        let mut path = url.path_segments_mut().map_err(|()| {
            config_error("Schema Registry URL cannot be used for path segments".to_owned())
        })?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }
}

async fn ensure_registry_success(response: reqwest::Response) -> Result<Value, PublishError> {
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        broadcaster_error(format!("Schema Registry response read failed: {error}"))
    })?;
    if !status.is_success() {
        return Err(broadcaster_error(format!(
            "Schema Registry rejected request with status {}: {}",
            status.as_u16(),
            body
        )));
    }
    if body.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(&body).map_err(|error| {
        broadcaster_error(format!("Schema Registry response was not JSON: {error}"))
    })
}

fn required_env(name: &str) -> Result<String, PublishError> {
    let value = optional_env(name)?
        .ok_or_else(|| config_error(format!("{name} is required when Kafka is enabled")))?;
    if value.trim().is_empty() {
        return Err(config_error(format!("{name} must not be empty")));
    }
    Ok(value)
}

fn optional_env(name: &str) -> Result<Option<String>, PublishError> {
    std::env::var(name)
        .map(|value| (!value.trim().is_empty()).then_some(value))
        .or_else(|error| match error {
            std::env::VarError::NotPresent => Ok(None),
            std::env::VarError::NotUnicode(_) => Err(PublishError::Infrastructure(format!(
                "failed to read {name}: value is not valid Unicode"
            ))),
        })
}

fn env_or_default(name: &str, default: &str) -> Result<String, PublishError> {
    Ok(optional_env(name)?.unwrap_or_else(|| default.to_owned()))
}

fn positive_u64_env(name: &str, default: u64) -> Result<u64, PublishError> {
    let raw = env_or_default(name, &default.to_string())?;
    let value = raw
        .parse::<u64>()
        .map_err(|_| config_error(format!("{name} must be a positive integer")))?;
    if value == 0 {
        return Err(config_error(format!("{name} must be greater than zero")));
    }
    Ok(value)
}

fn parse_bool(name: &str, raw: &str) -> Result<bool, PublishError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(config_error(format!("{name} must be true/false or 1/0"))),
    }
}

fn validate_runtime_target(
    runtime_environment: &str,
    security_protocol: &str,
    bootstrap_servers: &str,
    schema_registry_url: &str,
) -> Result<(), PublishError> {
    match runtime_environment.trim() {
        "local" | "ci" => return Ok(()),
        "staging" | "production" => {}
        other => {
            return Err(config_error(format!(
                "FOUNDATION_PLATFORM_RUNTIME_ENV must be local, ci, staging, or production; got {other}"
            )))
        }
    }

    if !matches!(security_protocol, "SSL" | "SASL_SSL") {
        return Err(config_error(format!(
            "Kafka security protocol {security_protocol} is not allowed in {runtime_environment}; use SSL or SASL_SSL"
        )));
    }
    if bootstrap_servers
        .split(',')
        .any(|endpoint| is_loopback_host(endpoint.trim()))
    {
        return Err(config_error(format!(
            "Kafka bootstrap servers must not target localhost in {runtime_environment}"
        )));
    }
    let registry = Url::parse(schema_registry_url)
        .map_err(|error| config_error(format!("invalid Schema Registry URL: {error}")))?;
    if registry.scheme() != "https" || registry.host_str().is_some_and(is_loopback_host_name) {
        return Err(config_error(format!(
            "Schema Registry must use a non-loopback HTTPS URL in {runtime_environment}"
        )));
    }
    Ok(())
}

fn is_loopback_host(endpoint: &str) -> bool {
    let host = endpoint
        .rsplit_once(':')
        .map_or(endpoint, |(host, _)| host)
        .trim_matches(['[', ']']);
    is_loopback_host_name(host)
}

fn is_loopback_host_name(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host == "::1"
        || host == "0.0.0.0"
        || host == "127.0.0.1"
        || host.starts_with("127.")
}

fn config_error(message: impl Into<String>) -> PublishError {
    PublishError::Infrastructure(format!("Kafka configuration: {}", message.into()))
}

const fn broadcaster_error(message: String) -> PublishError {
    PublishError::Broadcaster(message)
}

#[cfg(test)]
mod tests {
    use super::validate_runtime_target;

    #[test]
    fn local_runtime_accepts_disposable_plaintext_endpoints() {
        assert!(validate_runtime_target(
            "local",
            "PLAINTEXT",
            "127.0.0.1:19092",
            "http://127.0.0.1:18081",
        )
        .is_ok());
    }

    #[test]
    fn production_rejects_plaintext_or_loopback_endpoints() {
        assert!(validate_runtime_target(
            "production",
            "PLAINTEXT",
            "127.0.0.1:19092",
            "http://127.0.0.1:18081",
        )
        .is_err());
        assert!(validate_runtime_target(
            "production",
            "SASL_SSL",
            "127.0.0.1:19092",
            "https://registry.example.com",
        )
        .is_err());
    }

    #[test]
    fn production_accepts_managed_tls_endpoints() {
        assert!(validate_runtime_target(
            "production",
            "SASL_SSL",
            "broker-1.example.com:9093,broker-2.example.com:9093",
            "https://registry.example.com",
        )
        .is_ok());
    }

    #[test]
    fn unknown_runtime_environment_is_rejected() {
        assert!(validate_runtime_target(
            "prod",
            "SASL_SSL",
            "broker.example.com:9093",
            "https://registry.example.com",
        )
        .is_err());
    }
}
