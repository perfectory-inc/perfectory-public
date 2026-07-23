#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use apache_avro::{types::Value as AvroValue, Schema};
use intelligence_contracts::{
    schema_subject_for_topic, DeadLetterRecord, DeadLetterSourceMetadata, DEAD_LETTER_TOPIC,
};
use messaging_infrastructure::{
    avro_codec::ConfluentAvroCodec,
    dead_letter_publisher::{dead_letter_to_avro_value, DeadLetterPublisher},
    kafka::{KafkaConsumerConfig, KafkaEventConsumer, KafkaEventProducer, KafkaProducerConfig},
    karapace::{KarapaceClient, KarapaceClientConfig},
};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, Message,
};

#[tokio::test]
async fn live_kafka_karapace_registers_publishes_consumes_and_commits() {
    let bootstrap = match std::env::var("INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping live Kafka test: INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS unset");
            return;
        }
    };
    let registry = match std::env::var("INTELLIGENCE_TEST_KARAPACE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping live Karapace test: INTELLIGENCE_TEST_KARAPACE_URL unset");
            return;
        }
    };

    let schema_str = include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc");
    let schema = Schema::parse_str(schema_str).unwrap();
    let subject = schema_subject_for_topic(DEAD_LETTER_TOPIC);
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let warmup_key = format!("warmup-key-{run_id}");
    let live_key = format!("live-key-{run_id}");

    let karapace = KarapaceClient::new(KarapaceClientConfig {
        base_url: registry,
        timeout_seconds: 10,
    })
    .unwrap();
    let schema_id = wait_for_schema_registration(&karapace, &subject, schema_str).await;

    let warmup_payload = ConfluentAvroCodec::encode(
        schema_id,
        &schema,
        &dead_letter_to_avro_value(&dead_letter_record(
            &warmup_key,
            schema_id,
            0,
            "warmup event",
        )),
    )
    .unwrap();
    let payload = ConfluentAvroCodec::encode(
        schema_id,
        &schema,
        &dead_letter_to_avro_value(&dead_letter_record(
            &live_key,
            schema_id,
            1,
            "event payload was invalid",
        )),
    )
    .unwrap();

    let producer = KafkaEventProducer::new(KafkaProducerConfig {
        bootstrap_servers: bootstrap.clone(),
        client_id: "ip-live-producer".to_string(),
        linger_ms: 1,
        message_timeout_ms: 30_000,
    })
    .unwrap();

    producer
        .publish(DEAD_LETTER_TOPIC, &warmup_key, &warmup_payload, &[])
        .await
        .unwrap();

    let consumer = KafkaEventConsumer::new(
        KafkaConsumerConfig {
            bootstrap_servers: bootstrap,
            group_id: format!("ip-live-consumer-{}", uuid::Uuid::new_v4()),
            client_id: "ip-live-consumer".to_string(),
            enable_auto_commit: false,
            session_timeout_ms: 45_000,
        },
        &[DEAD_LETTER_TOPIC],
    )
    .unwrap();

    wait_for_assignment(&consumer).await;

    producer
        .publish(DEAD_LETTER_TOPIC, &live_key, &payload, &[])
        .await
        .unwrap();

    let message = recv_message_with_key(&consumer, &live_key, "valid dead-letter").await;
    let bytes = message.payload().unwrap();
    let (decoded_schema_id, _decoded) = ConfluentAvroCodec::decode(&schema, bytes).unwrap();
    assert_eq!(decoded_schema_id, schema_id);

    consumer.commit_message(&message).unwrap();
}

#[tokio::test]
async fn live_invalid_source_payload_is_dead_lettered_before_source_offset_commit() {
    let bootstrap = match std::env::var("INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping live Kafka test: INTELLIGENCE_TEST_KAFKA_BOOTSTRAP_SERVERS unset");
            return;
        }
    };
    let registry = match std::env::var("INTELLIGENCE_TEST_KARAPACE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping live Karapace test: INTELLIGENCE_TEST_KARAPACE_URL unset");
            return;
        }
    };

    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let source_topic = format!("intelligence.live.invalid-source.{run_id}.v1");
    let source_group_id = format!("ip-live-source-{run_id}");
    let dlq_group_id = format!("ip-live-dlq-{run_id}");
    let source_key = format!("source-key-{run_id}");
    let schema_str = include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc");
    let schema = Schema::parse_str(schema_str).unwrap();
    let subject = schema_subject_for_topic(DEAD_LETTER_TOPIC);

    let karapace = KarapaceClient::new(KarapaceClientConfig {
        base_url: registry,
        timeout_seconds: 10,
    })
    .unwrap();
    let schema_id = wait_for_schema_registration(&karapace, &subject, schema_str).await;

    let source_producer = KafkaEventProducer::new(KafkaProducerConfig {
        bootstrap_servers: bootstrap.clone(),
        client_id: format!("ip-live-source-producer-{run_id}"),
        linger_ms: 1,
        message_timeout_ms: 30_000,
    })
    .unwrap();
    source_producer
        .publish(
            &source_topic,
            &source_key,
            b"{this is deliberately not a valid source event",
            &[],
        )
        .await
        .unwrap();

    let source_consumer = earliest_stream_consumer(
        &bootstrap,
        &source_group_id,
        &format!("ip-live-source-consumer-{run_id}"),
        &[source_topic.as_str()],
    );

    let warmup_key = format!("warmup-{run_id}");
    let warmup_payload = ConfluentAvroCodec::encode(
        schema_id,
        &schema,
        &dead_letter_to_avro_value(&dead_letter_record(
            &warmup_key,
            schema_id,
            -1,
            "warmup event",
        )),
    )
    .unwrap();
    source_producer
        .publish(DEAD_LETTER_TOPIC, &warmup_key, &warmup_payload, &[])
        .await
        .unwrap();

    let dlq_consumer = KafkaEventConsumer::new(
        KafkaConsumerConfig {
            bootstrap_servers: bootstrap.clone(),
            group_id: dlq_group_id,
            client_id: format!("ip-live-dlq-consumer-{run_id}"),
            enable_auto_commit: false,
            session_timeout_ms: 45_000,
        },
        &[DEAD_LETTER_TOPIC],
    )
    .unwrap();
    wait_for_assignment(&dlq_consumer).await;

    let source_message = tokio::time::timeout(Duration::from_secs(20), source_consumer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_message.topic(), source_topic);
    assert_eq!(source_message.key(), Some(source_key.as_bytes()));

    let source_partition = source_message.partition();
    let source_offset = source_message.offset();
    let record = DeadLetterRecord::from_safe_metadata(
        DeadLetterSourceMetadata {
            event_id: uuid::Uuid::new_v4().to_string(),
            source_topic: source_message.topic().to_string(),
            source_partition,
            source_offset,
            source_key: source_message
                .key()
                .map(|key| String::from_utf8_lossy(key).to_string()),
            schema_id: None,
            event_type: Some("live.invalid-source".to_string()),
            trace_id: Some(format!("trace-{run_id}")),
            occurred_at_millis: chrono::Utc::now().timestamp_millis(),
        },
        "invalid_payload",
        b"event payload was invalid",
    );
    let dlq_payload =
        ConfluentAvroCodec::encode(schema_id, &schema, &dead_letter_to_avro_value(&record))
            .unwrap();
    let dlq_publisher = DeadLetterPublisher::new(
        KafkaEventProducer::new(KafkaProducerConfig {
            bootstrap_servers: bootstrap.clone(),
            client_id: format!("ip-live-dlq-producer-{run_id}"),
            linger_ms: 1,
            message_timeout_ms: 30_000,
        })
        .unwrap(),
    );

    dlq_publisher
        .publish_encoded(&source_key, dlq_payload, record.clone())
        .await
        .unwrap();
    source_consumer
        .commit_message(&source_message, CommitMode::Sync)
        .unwrap();
    drop(source_message);
    drop(source_consumer);

    let dlq_message =
        recv_message_with_key(&dlq_consumer, &source_key, "invalid dead-letter").await;
    let (decoded_schema_id, decoded) =
        ConfluentAvroCodec::decode(&schema, dlq_message.payload().unwrap()).unwrap();
    assert_eq!(decoded_schema_id, schema_id);
    let fields = match decoded {
        AvroValue::Record(fields) => fields,
        other => panic!("expected AvroValue::Record, got {other:?}"),
    };
    assert_eq!(
        avro_field(&fields, "source_topic"),
        &AvroValue::String(source_topic.clone())
    );
    assert_eq!(
        avro_field(&fields, "source_offset"),
        &AvroValue::Long(source_offset)
    );
    assert_eq!(
        avro_field(&fields, "safe_error_message"),
        &AvroValue::String("event payload was invalid".to_string())
    );
    dlq_consumer.commit_message(&dlq_message).unwrap();

    let fresh_source_consumer = earliest_stream_consumer(
        &bootstrap,
        &source_group_id,
        &format!("ip-live-source-consumer-fresh-{run_id}"),
        &[source_topic.as_str()],
    );
    wait_for_stream_assignment(&fresh_source_consumer, "fresh source consumer").await;
    match tokio::time::timeout(Duration::from_secs(3), fresh_source_consumer.recv()).await {
        Err(_) => {}
        Ok(Ok(redelivered)) => panic!(
            "committed source message was redelivered: topic={} partition={} offset={}",
            redelivered.topic(),
            redelivered.partition(),
            redelivered.offset()
        ),
        Ok(Err(error)) => panic!("fresh source consumer failed: {error}"),
    }
}

fn dead_letter_record(
    key: &str,
    schema_id: i32,
    source_offset: i64,
    safe_error_message: &str,
) -> DeadLetterRecord {
    DeadLetterRecord::from_safe_metadata(
        DeadLetterSourceMetadata {
            event_id: uuid::Uuid::new_v4().to_string(),
            source_topic: "live.source.topic".to_string(),
            source_partition: 0,
            source_offset,
            source_key: Some(key.to_string()),
            schema_id: Some(schema_id),
            event_type: Some("live.test".to_string()),
            trace_id: Some("trace-live".to_string()),
            occurred_at_millis: chrono::Utc::now().timestamp_millis(),
        },
        "invalid_payload",
        safe_error_message,
    )
}

async fn wait_for_schema_registration(
    karapace: &KarapaceClient,
    subject: &str,
    schema_str: &str,
) -> i32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let error_message = match karapace.set_backward_transitive(subject).await {
            Ok(()) => match karapace.register_avro_schema(subject, schema_str).await {
                Ok(schema_id) => return schema_id,
                Err(error) => format!("schema registration failed: {error}"),
            },
            Err(error) => format!("compatibility update failed: {error}"),
        };

        assert!(
            tokio::time::Instant::now() < deadline,
            "karapace did not become ready within 30s: {}",
            error_message
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_assignment(consumer: &KafkaEventConsumer) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

    loop {
        let assignment = consumer.assignment().unwrap();
        if assignment.count() > 0 {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "kafka consumer did not receive a topic assignment within 20s"
        );
        let _ = tokio::time::timeout(Duration::from_millis(250), consumer.recv()).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn recv_message_with_key<'a>(
    consumer: &'a KafkaEventConsumer,
    expected_key: &str,
    label: &str,
) -> BorrowedMessage<'a> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

    loop {
        let now = tokio::time::Instant::now();
        assert!(
            now < deadline,
            "{label} with key '{expected_key}' was not consumed within 20s"
        );

        let message = tokio::time::timeout(deadline - now, consumer.recv())
            .await
            .unwrap()
            .unwrap();
        if message.key() == Some(expected_key.as_bytes()) {
            return message;
        }
    }
}

fn earliest_stream_consumer(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    topics: &[&str],
) -> StreamConsumer {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("client.id", client_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000")
        .create::<StreamConsumer>()
        .unwrap();
    consumer.subscribe(topics).unwrap();
    consumer
}

async fn wait_for_stream_assignment(consumer: &StreamConsumer, label: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

    loop {
        let assignment = consumer.assignment().unwrap();
        if assignment.count() > 0 {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "{label} did not receive a topic assignment within 20s"
        );
        match tokio::time::timeout(Duration::from_millis(250), consumer.recv()).await {
            Ok(Ok(message)) => panic!(
                "{label} received a message while waiting for assignment: topic={} partition={} offset={}",
                message.topic(),
                message.partition(),
                message.offset()
            ),
            Ok(Err(error)) => panic!("{label} failed while waiting for assignment: {error}"),
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn avro_field<'a>(fields: &'a [(String, AvroValue)], name: &str) -> &'a AvroValue {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value)
        .unwrap_or_else(|| panic!("field '{name}' missing from decoded record"))
}
