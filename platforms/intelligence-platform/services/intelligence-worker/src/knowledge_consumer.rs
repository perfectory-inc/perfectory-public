use std::future::Future;
use std::sync::Arc;

use apache_avro::Schema;
use intelligence_contracts::{
    schema_subject_for_topic, DeadLetterRecord, DeadLetterSourceMetadata, DEAD_LETTER_TOPIC,
    FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC,
};
use knowledge_application::{
    KnowledgeProjectionError, KnowledgeProjectionPort, UpsertKnowledgeSource,
};
use knowledge_domain::KnowledgeSourceUpserted;
use knowledge_infrastructure::{
    PostgresKnowledgeSourceRegistry, PostgresKnowledgeSourceRegistryConfig,
};
use messaging_infrastructure::{
    avro_codec::{ConfluentAvroCodec, EventCodecError},
    dead_letter_publisher::{dead_letter_to_avro_value, DeadLetterPublisher},
    foundation_knowledge_avro::{
        foundation_knowledge_source_upserted_fixture_schema_str, FoundationKnowledgeEventError,
        FoundationKnowledgeSourceUpsertedEvent,
    },
    kafka::{
        EventOffsetCommitter, EventPayloadPublisher, KafkaCommitError, KafkaConsumerConfig,
        KafkaEventConsumer, KafkaEventProducer, KafkaProducerConfig, KafkaPublishError,
        KafkaSourceMessage,
    },
    karapace::{KarapaceClient, KarapaceClientConfig},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoundationKnowledgeConsumerConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub client_id: String,
    pub source_topic: String,
    pub session_timeout_ms: u64,
}

pub struct FoundationKnowledgeConsumerSchemas {
    pub source_schema: Schema,
    pub source_schema_id: i32,
    pub dead_letter_schema: Schema,
    pub dead_letter_schema_id: i32,
}

pub fn foundation_knowledge_consumer_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<FoundationKnowledgeConsumerConfig>, String> {
    let Some(bootstrap_servers) = lookup("FOUNDATION_KNOWLEDGE_CONSUMER_BOOTSTRAP_SERVERS")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    let fixture_contract_enabled = parse_bool_env(
        lookup("FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT"),
        "FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT",
    )?
    .unwrap_or(false);

    if !fixture_contract_enabled {
        return Err(
            "FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT=true is required until Foundation Platform publishes the approved knowledge event contract"
                .to_string(),
        );
    }

    let group_id = lookup("FOUNDATION_KNOWLEDGE_CONSUMER_GROUP_ID")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "intelligence-foundation-knowledge".to_string());
    let client_id = lookup("FOUNDATION_KNOWLEDGE_CONSUMER_CLIENT_ID")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "intelligence-foundation-knowledge-worker".to_string());
    let source_topic = lookup("FOUNDATION_KNOWLEDGE_CONSUMER_SOURCE_TOPIC")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC.to_string());
    let session_timeout_ms = parse_positive_u64_env(
        lookup("FOUNDATION_KNOWLEDGE_CONSUMER_SESSION_TIMEOUT_MS"),
        "FOUNDATION_KNOWLEDGE_CONSUMER_SESSION_TIMEOUT_MS",
        45_000,
    )?;

    Ok(Some(FoundationKnowledgeConsumerConfig {
        bootstrap_servers,
        group_id,
        client_id,
        source_topic,
        session_timeout_ms,
    }))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoundationKnowledgeRuntimeConfig {
    pub consumer: FoundationKnowledgeConsumerConfig,
    pub karapace_url: String,
    pub karapace_timeout_seconds: u64,
    pub database_url: String,
    pub database_timeout_seconds: u64,
    pub database_max_connections: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KnowledgeConsumerRunStatus {
    Disabled,
    Stopped,
}

#[derive(Debug, Error)]
pub enum KnowledgeConsumerRuntimeError {
    #[error("knowledge consumer runtime configuration is invalid: {message}")]
    InvalidConfig { message: String },
    #[error("knowledge consumer postgres registry composition failed")]
    PostgresRegistry,
    #[error("knowledge consumer kafka composition failed")]
    Kafka,
    #[error("knowledge consumer karapace composition failed")]
    Karapace,
    #[error("knowledge consumer schema composition failed")]
    Schema,
}

pub fn foundation_knowledge_runtime_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<FoundationKnowledgeRuntimeConfig>, KnowledgeConsumerRuntimeError> {
    let Some(consumer) = foundation_knowledge_consumer_config_from_lookup(&lookup)
        .map_err(invalid_runtime_config)?
    else {
        return Ok(None);
    };
    let database_url = required_env(&lookup, "DATABASE_URL")?;
    let karapace_url = required_env(&lookup, "FOUNDATION_KNOWLEDGE_CONSUMER_KARAPACE_URL")?;
    let karapace_timeout_seconds = parse_positive_u64_env(
        lookup("FOUNDATION_KNOWLEDGE_CONSUMER_KARAPACE_TIMEOUT_SECONDS"),
        "FOUNDATION_KNOWLEDGE_CONSUMER_KARAPACE_TIMEOUT_SECONDS",
        10,
    )
    .map_err(invalid_runtime_config)?;
    let database_timeout_seconds = parse_positive_u64_env(
        lookup("DATABASE_TIMEOUT_SECONDS"),
        "DATABASE_TIMEOUT_SECONDS",
        10,
    )
    .map_err(invalid_runtime_config)?;
    let database_max_connections = parse_positive_u32_env(
        lookup("DATABASE_MAX_CONNECTIONS"),
        "DATABASE_MAX_CONNECTIONS",
        10,
    )
    .map_err(invalid_runtime_config)?;

    Ok(Some(FoundationKnowledgeRuntimeConfig {
        consumer,
        karapace_url,
        karapace_timeout_seconds,
        database_url,
        database_timeout_seconds,
        database_max_connections,
    }))
}

pub async fn run_foundation_knowledge_consumer_with<Run, RunFuture>(
    lookup: impl Fn(&str) -> Option<String>,
    cancel: CancellationToken,
    run_loop: Run,
) -> Result<KnowledgeConsumerRunStatus, KnowledgeConsumerRuntimeError>
where
    Run: FnOnce(FoundationKnowledgeRuntimeConfig, CancellationToken) -> RunFuture,
    RunFuture: Future<Output = Result<(), KnowledgeConsumerRuntimeError>>,
{
    let Some(config) = foundation_knowledge_runtime_config_from_lookup(lookup)? else {
        return Ok(KnowledgeConsumerRunStatus::Disabled);
    };
    run_loop(config, cancel).await?;
    Ok(KnowledgeConsumerRunStatus::Stopped)
}

pub async fn run_foundation_knowledge_consumer(
    cancel: CancellationToken,
) -> Result<KnowledgeConsumerRunStatus, KnowledgeConsumerRuntimeError> {
    run_foundation_knowledge_consumer_with(
        |key| std::env::var(key).ok(),
        cancel,
        |config, loop_cancel| async move {
            FoundationKnowledgeConsumerRuntime::connect(config)
                .await?
                .run(loop_cancel)
                .await
        },
    )
    .await
}

struct FoundationKnowledgeConsumerRuntime {
    consumer: KafkaEventConsumer,
    projection: Arc<dyn KnowledgeProjectionPort>,
    dead_letter_publisher: KafkaEventProducer,
    schemas: FoundationKnowledgeConsumerSchemas,
}

impl FoundationKnowledgeConsumerRuntime {
    async fn connect(
        config: FoundationKnowledgeRuntimeConfig,
    ) -> Result<Self, KnowledgeConsumerRuntimeError> {
        let mut registry_config = PostgresKnowledgeSourceRegistryConfig::new(
            config.database_url,
            config.database_timeout_seconds,
        )
        .map_err(|_| KnowledgeConsumerRuntimeError::PostgresRegistry)?;
        registry_config = registry_config
            .with_max_connections(config.database_max_connections)
            .map_err(|_| KnowledgeConsumerRuntimeError::PostgresRegistry)?;
        let registry = PostgresKnowledgeSourceRegistry::connect(registry_config)
            .await
            .map_err(|_| KnowledgeConsumerRuntimeError::PostgresRegistry)?;
        let projection: Arc<dyn KnowledgeProjectionPort> =
            Arc::new(UpsertKnowledgeSource::new(registry));

        let karapace = KarapaceClient::new(KarapaceClientConfig {
            base_url: config.karapace_url,
            timeout_seconds: config.karapace_timeout_seconds,
        })
        .map_err(|_| KnowledgeConsumerRuntimeError::Karapace)?;
        let source_schema_text = foundation_knowledge_source_upserted_fixture_schema_str();
        let dead_letter_schema_text =
            include_str!("../../../schemas/intelligence.dead-letter.v1.avsc");
        let source_subject = schema_subject_for_topic(&config.consumer.source_topic);
        let dead_letter_subject = schema_subject_for_topic(DEAD_LETTER_TOPIC);
        karapace
            .set_backward_transitive(&source_subject)
            .await
            .map_err(|_| KnowledgeConsumerRuntimeError::Karapace)?;
        let source_schema_id = karapace
            .register_avro_schema(&source_subject, source_schema_text)
            .await
            .map_err(|_| KnowledgeConsumerRuntimeError::Karapace)?;
        karapace
            .set_backward_transitive(&dead_letter_subject)
            .await
            .map_err(|_| KnowledgeConsumerRuntimeError::Karapace)?;
        let dead_letter_schema_id = karapace
            .register_avro_schema(&dead_letter_subject, dead_letter_schema_text)
            .await
            .map_err(|_| KnowledgeConsumerRuntimeError::Karapace)?;
        let schemas = FoundationKnowledgeConsumerSchemas {
            source_schema: Schema::parse_str(source_schema_text)
                .map_err(|_| KnowledgeConsumerRuntimeError::Schema)?,
            source_schema_id,
            dead_letter_schema: Schema::parse_str(dead_letter_schema_text)
                .map_err(|_| KnowledgeConsumerRuntimeError::Schema)?,
            dead_letter_schema_id,
        };

        let consumer = KafkaEventConsumer::new(
            KafkaConsumerConfig {
                bootstrap_servers: config.consumer.bootstrap_servers.clone(),
                group_id: config.consumer.group_id,
                client_id: config.consumer.client_id.clone(),
                enable_auto_commit: false,
                session_timeout_ms: config.consumer.session_timeout_ms,
            },
            &[config.consumer.source_topic.as_str()],
        )
        .map_err(|_| KnowledgeConsumerRuntimeError::Kafka)?;
        let dead_letter_publisher = KafkaEventProducer::new(KafkaProducerConfig {
            bootstrap_servers: config.consumer.bootstrap_servers,
            client_id: config.consumer.client_id,
            linger_ms: 0,
            message_timeout_ms: 30_000,
        })
        .map_err(|_| KnowledgeConsumerRuntimeError::Kafka)?;

        Ok(Self {
            consumer,
            projection,
            dead_letter_publisher,
            schemas,
        })
    }

    async fn run(self, cancel: CancellationToken) -> Result<(), KnowledgeConsumerRuntimeError> {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                message = self.consumer.recv_owned() => {
                    let message = match message {
                        Ok(message) => message,
                        Err(_) => {
                            tracing::error!("foundation knowledge consumer receive failed; offset not committed");
                            continue;
                        }
                    };
                    match handle_foundation_knowledge_message(
                        message,
                        self.projection.clone(),
                        &self.dead_letter_publisher,
                        &self.consumer,
                        &self.schemas,
                    )
                    .await
                    {
                        Ok(KnowledgeConsumerStep::Projected { source_id }) => tracing::info!(
                            source_id = %source_id,
                            "foundation knowledge source projected"
                        ),
                        Ok(KnowledgeConsumerStep::DeadLettered) => tracing::warn!(
                            "foundation knowledge event dead-lettered and offset committed"
                        ),
                        Err(error) => tracing::error!(
                            error = error.safe_message(),
                            "foundation knowledge event processing failed; offset not committed"
                        ),
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KnowledgeConsumerStep {
    Projected { source_id: String },
    DeadLettered,
}

#[derive(Debug, Error)]
pub enum KnowledgeConsumerError {
    #[error("foundation knowledge event payload is invalid: {message}")]
    InvalidPayload { message: String },
    #[error("knowledge projection durable effect failed: {message}")]
    Projection { message: String },
    #[error("knowledge event dead-letter publish failed: {message}")]
    DeadLetterPublish { message: String },
    #[error("knowledge event offset commit failed: {message}")]
    OffsetCommit { message: String },
}

impl KnowledgeConsumerError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidPayload { .. } => "foundation knowledge event payload is invalid",
            Self::Projection { .. } => "knowledge projection durable effect failed",
            Self::DeadLetterPublish { .. } => "knowledge event dead-letter publish failed",
            Self::OffsetCommit { .. } => "knowledge event offset commit failed",
        }
    }
}

pub async fn handle_foundation_knowledge_message<P, C>(
    message: KafkaSourceMessage,
    projection: Arc<dyn KnowledgeProjectionPort>,
    dead_letter_publisher: &P,
    committer: &C,
    schemas: &FoundationKnowledgeConsumerSchemas,
) -> Result<KnowledgeConsumerStep, KnowledgeConsumerError>
where
    P: EventPayloadPublisher + Sync,
    C: EventOffsetCommitter + Sync,
{
    match decode_upsert_event(&message, schemas) {
        Ok((schema_id, event)) => {
            let source_id = event.source_id.clone();
            match projection
                .record_source_upsert(knowledge_source_upserted_from_event(event))
                .await
            {
                Ok(()) => {}
                Err(error @ KnowledgeProjectionError::InvalidEvent { .. }) => {
                    publish_dead_letter(
                        &message,
                        Some(schema_id),
                        "invalid_projection_event",
                        error.safe_message(),
                        dead_letter_publisher,
                        schemas,
                    )
                    .await?;
                    committer
                        .commit_source_offset(&message)
                        .await
                        .map_err(offset_commit_error)?;
                    return Ok(KnowledgeConsumerStep::DeadLettered);
                }
                Err(error) => {
                    return Err(KnowledgeConsumerError::Projection {
                        message: error.to_string(),
                    });
                }
            }
            committer
                .commit_source_offset(&message)
                .await
                .map_err(offset_commit_error)?;
            Ok(KnowledgeConsumerStep::Projected { source_id })
        }
        Err(DecodeFailure {
            schema_id,
            safe_message,
        }) => {
            publish_dead_letter(
                &message,
                schema_id,
                "invalid_payload",
                &safe_message,
                dead_letter_publisher,
                schemas,
            )
            .await?;
            committer
                .commit_source_offset(&message)
                .await
                .map_err(offset_commit_error)?;
            Ok(KnowledgeConsumerStep::DeadLettered)
        }
    }
}

fn decode_upsert_event(
    message: &KafkaSourceMessage,
    schemas: &FoundationKnowledgeConsumerSchemas,
) -> Result<(i32, FoundationKnowledgeSourceUpsertedEvent), DecodeFailure> {
    let payload = message.payload.as_deref().ok_or_else(|| DecodeFailure {
        schema_id: None,
        safe_message: "missing payload".to_string(),
    })?;

    let (schema_id, value) =
        ConfluentAvroCodec::decode(&schemas.source_schema, payload).map_err(|error| {
            DecodeFailure {
                schema_id: None,
                safe_message: error.safe_message().to_string(),
            }
        })?;

    if schema_id != schemas.source_schema_id {
        return Err(DecodeFailure {
            schema_id: Some(schema_id),
            safe_message: "unexpected foundation knowledge fixture schema id".to_string(),
        });
    }

    let event =
        FoundationKnowledgeSourceUpsertedEvent::from_avro_value(value).map_err(|error| {
            DecodeFailure {
                schema_id: Some(schema_id),
                safe_message: error.safe_message().to_string(),
            }
        })?;

    Ok((schema_id, event))
}

async fn publish_dead_letter<P>(
    message: &KafkaSourceMessage,
    schema_id: Option<i32>,
    failure_class: &str,
    safe_message: &str,
    publisher: &P,
    schemas: &FoundationKnowledgeConsumerSchemas,
) -> Result<(), KnowledgeConsumerError>
where
    P: EventPayloadPublisher + Sync,
{
    let source_key = safe_source_key(message);
    let key = source_key
        .clone()
        .unwrap_or_else(|| format!("{}:{}:{}", message.topic, message.partition, message.offset));
    let record = DeadLetterRecord::from_safe_metadata(
        DeadLetterSourceMetadata {
            event_id: safe_cloud_event_id(message.header_value("ce_id"))
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            source_topic: message.topic.clone(),
            source_partition: message.partition,
            source_offset: message.offset,
            source_key,
            schema_id,
            event_type: safe_cloud_event_type(message.header_value("ce_type")),
            trace_id: trace_id_from_traceparent(message.header_value("traceparent")),
            occurred_at_millis: chrono::Utc::now().timestamp_millis(),
        },
        failure_class,
        safe_message,
    );
    let payload = ConfluentAvroCodec::encode(
        schemas.dead_letter_schema_id,
        &schemas.dead_letter_schema,
        &dead_letter_to_avro_value(&record),
    )
    .map_err(codec_error)?;

    DeadLetterPublisher::new(publisher)
        .publish_encoded(&key, payload, record)
        .await
        .map_err(publish_error)
}

fn knowledge_source_upserted_from_event(
    value: FoundationKnowledgeSourceUpsertedEvent,
) -> KnowledgeSourceUpserted {
    value.into()
}

#[derive(Debug)]
struct DecodeFailure {
    schema_id: Option<i32>,
    safe_message: String,
}

fn trace_id_from_traceparent(traceparent: Option<&str>) -> Option<String> {
    let traceparent = traceparent?;
    let mut parts = traceparent.split('-');
    let _version = parts.next()?;
    let trace_id = parts.next()?;
    if trace_id.len() == 32 && trace_id.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(trace_id.to_string())
    } else {
        None
    }
}

fn safe_source_key(message: &KafkaSourceMessage) -> Option<String> {
    message
        .key_as_utf8_string()
        .filter(|value| is_safe_metadata_value(value, 512))
}

fn safe_cloud_event_id(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if uuid::Uuid::parse_str(value).is_ok() {
        Some(value.to_string())
    } else {
        None
    }
}

fn safe_cloud_event_type(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if is_safe_cloud_event_type(value) {
        Some(value.to_string())
    } else {
        None
    }
}

fn is_safe_cloud_event_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/'))
}

fn is_safe_metadata_value(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value.chars().all(|ch| !ch.is_control() && ch != '\u{FFFD}')
}

fn codec_error(error: EventCodecError) -> KnowledgeConsumerError {
    KnowledgeConsumerError::InvalidPayload {
        message: error.to_string(),
    }
}

fn publish_error(error: KafkaPublishError) -> KnowledgeConsumerError {
    KnowledgeConsumerError::DeadLetterPublish {
        message: error.to_string(),
    }
}

fn offset_commit_error(error: KafkaCommitError) -> KnowledgeConsumerError {
    KnowledgeConsumerError::OffsetCommit {
        message: error.to_string(),
    }
}

fn parse_bool_env(raw: Option<String>, key: &str) -> Result<Option<bool>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(Some(true)),
        "false" | "0" | "no" => Ok(Some(false)),
        _ => Err(format!("{key} must be true or false")),
    }
}

fn parse_positive_u64_env(raw: Option<String>, key: &str, default: u64) -> Result<u64, String> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse::<u64>()
        .map_err(|error| format!("{key} is invalid: {error}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

fn parse_positive_u32_env(raw: Option<String>, key: &str, default: u32) -> Result<u32, String> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse::<u32>()
        .map_err(|error| format!("{key} is invalid: {error}"))?;
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    Ok(value)
}

fn required_env(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
) -> Result<String, KnowledgeConsumerRuntimeError> {
    lookup(key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_runtime_config(format!("{key} is required")))
}

fn invalid_runtime_config(message: String) -> KnowledgeConsumerRuntimeError {
    KnowledgeConsumerRuntimeError::InvalidConfig { message }
}

impl From<FoundationKnowledgeEventError> for KnowledgeConsumerError {
    fn from(error: FoundationKnowledgeEventError) -> Self {
        Self::InvalidPayload {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::foundation_knowledge_consumer_config_from_lookup;

    #[test]
    fn config_absent_bootstrap_disables_consumer() {
        let config = foundation_knowledge_consumer_config_from_lookup(|_| None).unwrap();

        assert!(config.is_none());
    }

    #[test]
    fn config_requires_fixture_contract_gate_when_bootstrap_is_set() {
        let values = BTreeMap::from([(
            "FOUNDATION_KNOWLEDGE_CONSUMER_BOOTSTRAP_SERVERS",
            "127.0.0.1:19092",
        )]);

        let error = foundation_knowledge_consumer_config_from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap_err();

        assert!(error.contains("FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT=true"));
    }

    #[test]
    fn config_accepts_explicit_fixture_contract_for_local_consumer() {
        let values = BTreeMap::from([
            (
                "FOUNDATION_KNOWLEDGE_CONSUMER_BOOTSTRAP_SERVERS",
                "127.0.0.1:19092",
            ),
            (
                "FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT",
                "true",
            ),
            ("FOUNDATION_KNOWLEDGE_CONSUMER_GROUP_ID", "ip-knowledge"),
            ("FOUNDATION_KNOWLEDGE_CONSUMER_CLIENT_ID", "ip-knowledge-1"),
        ]);

        let config = foundation_knowledge_consumer_config_from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap()
        .unwrap();

        assert_eq!(config.bootstrap_servers, "127.0.0.1:19092");
        assert_eq!(config.group_id, "ip-knowledge");
        assert_eq!(config.client_id, "ip-knowledge-1");
        assert_eq!(
            config.source_topic,
            "intelligence-platform.fixture.foundation-knowledge-source.upserted.v1"
        );
        assert_eq!(config.session_timeout_ms, 45_000);
    }
}
