// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use apache_avro::Schema;
use async_trait::async_trait;
use intelligence_contracts::{EventHeader, FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC};
use intelligence_worker::knowledge_consumer::{
    handle_foundation_knowledge_message, FoundationKnowledgeConsumerSchemas, KnowledgeConsumerStep,
};
use knowledge_application::{KnowledgeProjectionError, KnowledgeProjectionPort};
use knowledge_domain::KnowledgeSourceUpserted;
use messaging_infrastructure::{
    avro_codec::ConfluentAvroCodec,
    dead_letter_publisher::dead_letter_from_avro_value,
    foundation_knowledge_avro::{
        foundation_knowledge_source_upserted_fixture_schema_str,
        FoundationKnowledgeSourceUpsertedEvent,
    },
    kafka::{
        EventOffsetCommitter, EventPayloadPublisher, KafkaCommitError, KafkaPublishError,
        KafkaSourceMessage,
    },
};

#[tokio::test]
async fn valid_upsert_records_projection_before_committing_source_offset() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let projection = Arc::new(CapturingProjection::new(order.clone(), None));
    let publisher = CapturingPublisher::default();
    let committer = CapturingCommitter::new(order.clone());
    let schemas = schemas();
    let source_schema = source_schema();
    let payload =
        ConfluentAvroCodec::encode(11, &source_schema, &upsert_event().to_avro_value()).unwrap();

    let step = handle_foundation_knowledge_message(
        message_with_payload(payload),
        projection.clone(),
        &publisher,
        &committer,
        &schemas,
    )
    .await
    .unwrap();

    assert_eq!(
        step,
        KnowledgeConsumerStep::Projected {
            source_id: "source-1".to_string()
        }
    );
    assert_eq!(projection.calls.lock().unwrap().len(), 1);
    assert_eq!(
        projection.calls.lock().unwrap()[0].source_uri,
        "s3://foundation/silver/source-1.json"
    );
    assert!(publisher.calls.lock().unwrap().is_empty());
    assert_eq!(committer.calls.lock().unwrap().len(), 1);
    assert_eq!(&*order.lock().unwrap(), &["projection", "commit"]);
}

#[tokio::test]
async fn invalid_payload_is_dead_lettered_before_committing_source_offset() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let projection = Arc::new(CapturingProjection::new(order.clone(), None));
    let publisher = CapturingPublisher::with_order(order.clone());
    let committer = CapturingCommitter::new(order.clone());
    let schemas = schemas();

    let step = handle_foundation_knowledge_message(
        message_with_payload(b"not a confluent avro payload".to_vec()),
        projection.clone(),
        &publisher,
        &committer,
        &schemas,
    )
    .await
    .unwrap();

    assert_eq!(step, KnowledgeConsumerStep::DeadLettered);
    assert!(projection.calls.lock().unwrap().is_empty());
    assert_eq!(publisher.calls.lock().unwrap().len(), 1);
    assert_eq!(committer.calls.lock().unwrap().len(), 1);
    assert_eq!(&*order.lock().unwrap(), &["dead_letter", "commit"]);

    let published = publisher.calls.lock().unwrap();
    assert_eq!(published[0].topic, "intelligence.dead-letter.v1");
    assert_eq!(published[0].key, "tenant-1:product-1:source-1");
    let (_, dead_letter_value) =
        ConfluentAvroCodec::decode(&dead_letter_schema(), &published[0].payload).unwrap();
    let record = dead_letter_from_avro_value(dead_letter_value).unwrap();
    assert_eq!(
        record.source_topic(),
        FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC
    );
    assert_eq!(record.source_offset(), 41);
    assert_eq!(record.failure_class(), "invalid_payload");
}

#[tokio::test]
async fn unexpected_fixture_schema_id_is_dead_lettered_before_commit() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let projection = Arc::new(CapturingProjection::new(order.clone(), None));
    let publisher = CapturingPublisher::with_order(order.clone());
    let committer = CapturingCommitter::new(order.clone());
    let schemas = schemas();
    let source_schema = source_schema();
    let payload =
        ConfluentAvroCodec::encode(12, &source_schema, &upsert_event().to_avro_value()).unwrap();

    let step = handle_foundation_knowledge_message(
        message_with_payload(payload),
        projection.clone(),
        &publisher,
        &committer,
        &schemas,
    )
    .await
    .unwrap();

    assert_eq!(step, KnowledgeConsumerStep::DeadLettered);
    assert!(projection.calls.lock().unwrap().is_empty());
    assert_eq!(committer.calls.lock().unwrap().len(), 1);
    assert_eq!(&*order.lock().unwrap(), &["dead_letter", "commit"]);

    let published = publisher.calls.lock().unwrap();
    let (_, dead_letter_value) =
        ConfluentAvroCodec::decode(&dead_letter_schema(), &published[0].payload).unwrap();
    let record = dead_letter_from_avro_value(dead_letter_value).unwrap();
    assert_eq!(record.schema_id(), Some(12));
    assert_eq!(record.failure_class(), "invalid_payload");
    assert_eq!(
        record.safe_error_message(),
        "unexpected foundation knowledge fixture schema id"
    );
}

#[tokio::test]
async fn projection_invalid_event_is_dead_lettered_before_commit() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let projection = Arc::new(CapturingProjection::new(
        order.clone(),
        Some(KnowledgeProjectionError::InvalidEvent {
            message: "source_id conflicts with registry rules".to_string(),
        }),
    ));
    let publisher = CapturingPublisher::with_order(order.clone());
    let committer = CapturingCommitter::new(order.clone());
    let schemas = schemas();
    let source_schema = source_schema();
    let payload =
        ConfluentAvroCodec::encode(11, &source_schema, &upsert_event().to_avro_value()).unwrap();

    let step = handle_foundation_knowledge_message(
        message_with_payload(payload),
        projection,
        &publisher,
        &committer,
        &schemas,
    )
    .await
    .unwrap();

    assert_eq!(step, KnowledgeConsumerStep::DeadLettered);
    assert_eq!(committer.calls.lock().unwrap().len(), 1);
    assert_eq!(
        &*order.lock().unwrap(),
        &["projection", "dead_letter", "commit"]
    );

    let published = publisher.calls.lock().unwrap();
    let (_, dead_letter_value) =
        ConfluentAvroCodec::decode(&dead_letter_schema(), &published[0].payload).unwrap();
    let record = dead_letter_from_avro_value(dead_letter_value).unwrap();
    assert_eq!(record.schema_id(), Some(11));
    assert_eq!(record.failure_class(), "invalid_projection_event");
    assert_eq!(record.safe_error_message(), "knowledge event is invalid");
}

#[tokio::test]
async fn projection_failure_does_not_commit_or_dead_letter_retryable_source_event() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let projection = Arc::new(CapturingProjection::new(
        order.clone(),
        Some(KnowledgeProjectionError::StoreUnavailable {
            message: "postgres unavailable".to_string(),
        }),
    ));
    let publisher = CapturingPublisher::default();
    let committer = CapturingCommitter::new(order.clone());
    let schemas = schemas();
    let source_schema = source_schema();
    let payload =
        ConfluentAvroCodec::encode(11, &source_schema, &upsert_event().to_avro_value()).unwrap();

    let error = handle_foundation_knowledge_message(
        message_with_payload(payload),
        projection,
        &publisher,
        &committer,
        &schemas,
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.safe_message(),
        "knowledge projection durable effect failed"
    );
    assert!(publisher.calls.lock().unwrap().is_empty());
    assert!(committer.calls.lock().unwrap().is_empty());
    assert_eq!(&*order.lock().unwrap(), &["projection"]);
}

#[tokio::test]
async fn invalid_payload_dead_letter_omits_untrusted_binary_key_and_bad_headers() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let projection = Arc::new(CapturingProjection::new(order.clone(), None));
    let publisher = CapturingPublisher::with_order(order.clone());
    let committer = CapturingCommitter::new(order.clone());
    let schemas = schemas();
    let mut message = message_with_payload(b"not a confluent avro payload".to_vec());
    message.key = Some(vec![0xff, b'a', b'b']);
    message.headers = vec![
        EventHeader {
            key: "ce_id".to_string(),
            value: "not-a-uuid\nwith-control".to_string(),
        },
        EventHeader {
            key: "ce_type".to_string(),
            value: "foundation bad type".to_string(),
        },
        EventHeader {
            key: "traceparent".to_string(),
            value: "00-not-a-trace-id-00f067aa0ba902b7-01".to_string(),
        },
    ];

    let step =
        handle_foundation_knowledge_message(message, projection, &publisher, &committer, &schemas)
            .await
            .unwrap();

    assert_eq!(step, KnowledgeConsumerStep::DeadLettered);
    let published = publisher.calls.lock().unwrap();
    assert_eq!(
        published[0].key,
        format!("{FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC}:3:41")
    );
    let (_, dead_letter_value) =
        ConfluentAvroCodec::decode(&dead_letter_schema(), &published[0].payload).unwrap();
    let record = dead_letter_from_avro_value(dead_letter_value).unwrap();
    uuid::Uuid::parse_str(record.event_id()).unwrap();
    assert_eq!(record.source_key(), None);
    assert_eq!(record.event_type(), None);
    assert_eq!(record.trace_id(), None);
}

fn schemas() -> FoundationKnowledgeConsumerSchemas {
    FoundationKnowledgeConsumerSchemas {
        source_schema: source_schema(),
        source_schema_id: 11,
        dead_letter_schema: dead_letter_schema(),
        dead_letter_schema_id: 29,
    }
}

fn source_schema() -> Schema {
    Schema::parse_str(foundation_knowledge_source_upserted_fixture_schema_str()).unwrap()
}

fn dead_letter_schema() -> Schema {
    Schema::parse_str(include_str!(
        "../../../schemas/intelligence.dead-letter.v1.avsc"
    ))
    .unwrap()
}

fn upsert_event() -> FoundationKnowledgeSourceUpsertedEvent {
    FoundationKnowledgeSourceUpsertedEvent {
        event_id: "018f7c6a-0000-7000-8000-000000000201".to_string(),
        tenant_id: "tenant-1".to_string(),
        product_id: "product-1".to_string(),
        source_id: "source-1".to_string(),
        source_kind: "document".to_string(),
        source_uri: "s3://foundation/silver/source-1.json".to_string(),
        content_uri: Some("s3://foundation/documents/source-1.pdf".to_string()),
        content_checksum_sha256: Some(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        ),
        occurred_at_millis: 1_783_641_600_000,
        metadata: BTreeMap::from([("schema_version".to_string(), "fixture.v1".to_string())]),
    }
}

fn message_with_payload(payload: Vec<u8>) -> KafkaSourceMessage {
    KafkaSourceMessage {
        topic: FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC.to_string(),
        partition: 3,
        offset: 41,
        key: Some(b"tenant-1:product-1:source-1".to_vec()),
        payload: Some(payload),
        headers: vec![
            EventHeader {
                key: "ce_id".to_string(),
                value: "018f7c6a-0000-7000-8000-000000000201".to_string(),
            },
            EventHeader {
                key: "ce_type".to_string(),
                value: "foundation-platform.knowledge-source.upserted".to_string(),
            },
            EventHeader {
                key: "traceparent".to_string(),
                value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            },
        ],
    }
}

struct CapturingProjection {
    calls: Mutex<Vec<KnowledgeSourceUpserted>>,
    order: Arc<Mutex<Vec<&'static str>>>,
    error: Option<KnowledgeProjectionError>,
}

impl CapturingProjection {
    fn new(order: Arc<Mutex<Vec<&'static str>>>, error: Option<KnowledgeProjectionError>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            order,
            error,
        }
    }
}

#[async_trait]
impl KnowledgeProjectionPort for CapturingProjection {
    async fn record_source_upsert(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<(), KnowledgeProjectionError> {
        self.order.lock().unwrap().push("projection");
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        self.calls.lock().unwrap().push(event);
        Ok(())
    }
}

#[derive(Default)]
struct CapturingPublisher {
    calls: Arc<Mutex<Vec<PublishedEvent>>>,
    order: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl CapturingPublisher {
    fn with_order(order: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            order: Some(order),
        }
    }
}

struct PublishedEvent {
    topic: String,
    key: String,
    payload: Vec<u8>,
}

#[async_trait]
impl EventPayloadPublisher for CapturingPublisher {
    async fn publish(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
        _headers: &[EventHeader],
    ) -> Result<(), KafkaPublishError> {
        if let Some(order) = &self.order {
            order.lock().unwrap().push("dead_letter");
        }
        self.calls.lock().unwrap().push(PublishedEvent {
            topic: topic.to_string(),
            key: key.to_string(),
            payload: payload.to_vec(),
        });
        Ok(())
    }
}

struct CapturingCommitter {
    calls: Arc<Mutex<Vec<(String, i32, i64)>>>,
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl CapturingCommitter {
    fn new(order: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            order,
        }
    }
}

#[async_trait]
impl EventOffsetCommitter for CapturingCommitter {
    async fn commit_source_offset(
        &self,
        message: &KafkaSourceMessage,
    ) -> Result<(), KafkaCommitError> {
        self.order.lock().unwrap().push("commit");
        self.calls
            .lock()
            .unwrap()
            .push((message.topic.clone(), message.partition, message.offset));
        Ok(())
    }
}
