use async_trait::async_trait;
use intelligence_contracts::EventHeader;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::TopicPartitionList,
    ClientConfig, Offset,
};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaProducerConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub linger_ms: u64,
    pub message_timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaConsumerConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub client_id: String,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: u64,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum KafkaConfigError {
    #[error("{message}")]
    Invalid { message: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum KafkaPublishError {
    #[error("{message}")]
    Publish { message: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum KafkaCommitError {
    #[error("{message}")]
    Commit { message: String },
}

pub struct KafkaEventProducer {
    producer: FutureProducer,
}

pub struct KafkaEventConsumer {
    consumer: StreamConsumer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaSourceMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<EventHeader>,
}

#[async_trait]
pub trait EventPayloadPublisher: Send + Sync {
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
        headers: &[EventHeader],
    ) -> Result<(), KafkaPublishError>;
}

#[async_trait]
pub trait EventOffsetCommitter: Send + Sync {
    async fn commit_source_offset(
        &self,
        message: &KafkaSourceMessage,
    ) -> Result<(), KafkaCommitError>;
}

impl KafkaProducerConfig {
    pub fn validate(&self) -> Result<(), KafkaConfigError> {
        require_non_empty("bootstrap_servers", &self.bootstrap_servers)?;
        require_non_empty("client_id", &self.client_id)?;

        if self.message_timeout_ms == 0 {
            return Err(KafkaConfigError::Invalid {
                message: "message_timeout_ms must be greater than zero".to_string(),
            });
        }

        Ok(())
    }
}

impl KafkaEventProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self, KafkaConfigError> {
        config.validate()?;

        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", &config.client_id)
            .set("linger.ms", config.linger_ms.to_string())
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .create::<FutureProducer>()
            .map_err(|error| KafkaConfigError::Invalid {
                message: format!("failed to create kafka producer: {error}"),
            })?;

        Ok(Self { producer })
    }

    pub async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
        headers: &[EventHeader],
    ) -> Result<(), KafkaPublishError> {
        <Self as EventPayloadPublisher>::publish(self, topic, key, payload, headers).await
    }
}

#[async_trait]
impl<T> EventPayloadPublisher for &T
where
    T: EventPayloadPublisher + ?Sized,
{
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
        headers: &[EventHeader],
    ) -> Result<(), KafkaPublishError> {
        (**self).publish(topic, key, payload, headers).await
    }
}

#[async_trait]
impl EventPayloadPublisher for KafkaEventProducer {
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
        headers: &[EventHeader],
    ) -> Result<(), KafkaPublishError> {
        let headers = owned_headers(headers);
        let record = FutureRecord::to(topic)
            .key(key)
            .payload(payload)
            .headers(headers);
        let delivery_status = self.producer.send(record, Duration::from_secs(30)).await;

        delivery_status
            .map(|_| ())
            .map_err(|(error, _)| KafkaPublishError::Publish {
                message: error.to_string(),
            })
    }
}

impl KafkaConsumerConfig {
    pub fn validate(&self) -> Result<(), KafkaConfigError> {
        require_non_empty("bootstrap_servers", &self.bootstrap_servers)?;
        require_non_empty("group_id", &self.group_id)?;
        require_non_empty("client_id", &self.client_id)?;

        if self.session_timeout_ms == 0 {
            return Err(KafkaConfigError::Invalid {
                message: "session_timeout_ms must be greater than zero".to_string(),
            });
        }

        if self.enable_auto_commit {
            return Err(KafkaConfigError::Invalid {
                message: "enable_auto_commit must be false for manual commits".to_string(),
            });
        }

        Ok(())
    }
}

impl KafkaEventConsumer {
    pub fn new(config: KafkaConsumerConfig, topics: &[&str]) -> Result<Self, KafkaConfigError> {
        config.validate()?;

        if topics.is_empty() {
            return Err(KafkaConfigError::Invalid {
                message: "topics must not be empty".to_string(),
            });
        }

        for topic in topics {
            require_non_empty("topic", topic)?;
        }

        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("client.id", &config.client_id)
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .create::<StreamConsumer>()
            .map_err(|error| KafkaConfigError::Invalid {
                message: format!("failed to create kafka consumer: {error}"),
            })?;

        consumer
            .subscribe(topics)
            .map_err(|error| KafkaConfigError::Invalid {
                message: format!("failed to subscribe kafka consumer: {error}"),
            })?;

        Ok(Self { consumer })
    }

    pub async fn recv(&self) -> Result<BorrowedMessage<'_>, KafkaError> {
        self.consumer.recv().await
    }

    pub async fn recv_owned(&self) -> Result<KafkaSourceMessage, KafkaError> {
        let message = self.recv().await?;
        Ok(KafkaSourceMessage::from_borrowed(&message))
    }

    pub fn assignment(&self) -> Result<TopicPartitionList, KafkaError> {
        self.consumer.assignment()
    }

    pub fn commit_message(&self, message: &BorrowedMessage<'_>) -> Result<(), KafkaCommitError> {
        self.consumer
            .commit_message(message, CommitMode::Sync)
            .map_err(|error| KafkaCommitError::Commit {
                message: error.to_string(),
            })
    }

    pub fn commit_source_offset(
        &self,
        message: &KafkaSourceMessage,
    ) -> Result<(), KafkaCommitError> {
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(
                &message.topic,
                message.partition,
                Offset::Offset(message.offset + 1),
            )
            .map_err(|error| KafkaCommitError::Commit {
                message: error.to_string(),
            })?;
        self.consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(|error| KafkaCommitError::Commit {
                message: error.to_string(),
            })
    }
}

#[async_trait]
impl EventOffsetCommitter for KafkaEventConsumer {
    async fn commit_source_offset(
        &self,
        message: &KafkaSourceMessage,
    ) -> Result<(), KafkaCommitError> {
        KafkaEventConsumer::commit_source_offset(self, message)
    }
}

impl KafkaSourceMessage {
    pub fn from_borrowed(message: &BorrowedMessage<'_>) -> Self {
        let headers = message
            .headers()
            .map(|headers| {
                (0..headers.count())
                    .map(|index| {
                        let header = headers.get(index);
                        EventHeader {
                            key: header.key.to_string(),
                            value: header
                                .value
                                .map(|value| String::from_utf8_lossy(value).to_string())
                                .unwrap_or_default(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            key: message.key().map(ToOwned::to_owned),
            payload: message.payload().map(ToOwned::to_owned),
            headers,
        }
    }

    pub fn key_as_lossy_string(&self) -> Option<String> {
        self.key
            .as_ref()
            .map(|key| String::from_utf8_lossy(key).to_string())
    }

    pub fn key_as_utf8_string(&self) -> Option<String> {
        self.key
            .as_ref()
            .and_then(|key| std::str::from_utf8(key).ok())
            .map(ToOwned::to_owned)
    }

    pub fn header_value(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|header| header.key == key)
            .map(|header| header.value.as_str())
    }
}

pub fn headers_to_owned_pairs(headers: &[EventHeader]) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|header| (header.key.clone(), header.value.clone()))
        .collect()
}

fn owned_headers(headers: &[EventHeader]) -> OwnedHeaders {
    headers.iter().fold(
        OwnedHeaders::new_with_capacity(headers.len()),
        |owned_headers, header| {
            owned_headers.insert(Header {
                key: &header.key,
                value: Some(&header.value),
            })
        },
    )
}

fn require_non_empty(field: &str, value: &str) -> Result<(), KafkaConfigError> {
    if value.trim().is_empty() {
        Err(KafkaConfigError::Invalid {
            message: format!("{field} must not be empty"),
        })
    } else {
        Ok(())
    }
}
