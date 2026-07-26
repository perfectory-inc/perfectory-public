#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(missing_docs)]

use std::{error::Error, sync::Arc, time::Duration};

use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use async_trait::async_trait;
use foundation_outbox::{
    kafka_broadcaster::from_env, EventBroadcaster, EventEnvelope, OutboxScope, PublishError,
    RAW_WRITTEN_AVRO_SCHEMA,
};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    ClientConfig,
};
use serde_json::json;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Default)]
struct NoopFallback;

#[async_trait]
impl EventBroadcaster for NoopFallback {
    async fn publish(&self, _event: &EventEnvelope) -> Result<(), PublishError> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires the pinned Redpanda+Karapace compose stack"]
async fn live_kafka_karapace_registers_publishes_consumes_and_commits_raw_written() -> TestResult {
    let Some((bootstrap, registry)) = required_live_endpoints()? else {
        return Ok(());
    };
    let topic = std::env::var("FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC")
        .unwrap_or_else(|_| foundation_outbox::DEFAULT_RAW_WRITTEN_TOPIC.to_owned());
    if std::env::var("FOUNDATION_PLATFORM_KAFKA_ENABLED").as_deref() != Ok("1") {
        return Err("FOUNDATION_PLATFORM_KAFKA_ENABLED=1 is required for the live test".into());
    }

    let run_id = Uuid::new_v4().simple().to_string();
    let scope_unit_id = format!("live-scope-{run_id}");
    let consumer = consumer(&bootstrap, &topic, &run_id)?;
    wait_for_assignment(&consumer).await?;

    let fallback: Arc<dyn EventBroadcaster> = Arc::new(NoopFallback);
    let broadcaster = from_env(fallback)
        .await?
        .ok_or("Kafka broadcaster was disabled even though FOUNDATION_PLATFORM_KAFKA_ENABLED=1")?;
    let event = raw_written_event(&scope_unit_id, &run_id);
    broadcaster.publish(&event).await?;

    let message = receive_key(&consumer, &scope_unit_id).await?;
    let payload = message.payload().ok_or("Kafka message had no payload")?;
    assert!(payload.len() > 5, "Confluent Avro payload is too short");
    assert_eq!(payload[0], 0, "Confluent Avro magic byte mismatch");
    let schema_id = i32::from_be_bytes(payload[1..5].try_into()?);
    assert!(schema_id > 0, "Schema Registry id must be positive");
    let schema = Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA)?;
    let mut datum = &payload[5..];
    let decoded = from_avro_datum(&schema, &mut datum, None)?;
    let fields = match decoded {
        AvroValue::Record(fields) => fields,
        other => return Err(format!("expected Avro record, got {other:?}").into()),
    };
    assert_eq!(field(&fields, "event_id"), event.event_id.to_string());
    assert_eq!(field(&fields, "scope_unit_id"), scope_unit_id);
    assert_eq!(field(&fields, "bronze_object_key"), "bronze/live/raw.json");
    consumer.commit_message(&message, CommitMode::Sync)?;
    let _ = registry;
    Ok(())
}

fn required_live_endpoints() -> TestResult<Option<(String, String)>> {
    let required = std::env::var("FOUNDATION_TEST_KAFKA_REQUIRED").as_deref() == Ok("1");
    let bootstrap = std::env::var("FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS").ok();
    let registry = std::env::var("FOUNDATION_TEST_KARAPACE_URL").ok();
    match (bootstrap, registry) {
        (Some(bootstrap), Some(registry)) => Ok(Some((bootstrap, registry))),
        _ if required => Err(
            "FOUNDATION_TEST_KAFKA_REQUIRED=1 requires FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS and FOUNDATION_TEST_KARAPACE_URL".into(),
        ),
        _ => Ok(None),
    }
}

fn consumer(bootstrap: &str, topic: &str, run_id: &str) -> TestResult<StreamConsumer> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("foundation-live-{run_id}"))
        .set("client.id", format!("foundation-live-consumer-{run_id}"))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000")
        .create::<StreamConsumer>()?;
    consumer.subscribe(&[topic])?;
    Ok(consumer)
}

async fn wait_for_assignment(consumer: &StreamConsumer) -> TestResult {
    for _ in 0..100 {
        if consumer.assignment()?.count() > 0 {
            return Ok(());
        }
        match tokio::time::timeout(Duration::from_millis(250), consumer.recv()).await {
            Ok(Ok(_message)) => {}
            Err(_) => {}
            Ok(Err(error)) => return Err(error.into()),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Kafka consumer did not receive a partition assignment".into())
}

async fn receive_key<'a>(
    consumer: &'a StreamConsumer,
    key: &str,
) -> TestResult<rdkafka::message::BorrowedMessage<'a>> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv()).await??;
        if message.key() == Some(key.as_bytes()) {
            return Ok(message);
        }
    }
}

fn field(fields: &[(String, AvroValue)], name: &str) -> String {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| match value {
            AvroValue::String(value) => value.clone(),
            other => format!("{other:?}"),
        })
        .unwrap_or_default()
}

fn raw_written_event(scope_unit_id: &str, run_id: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        event_type: "catalog.collection.raw_written.v1".to_owned(),
        payload: json!({
            "type": "catalog.collection.raw_written.v1",
            "schema_version": 1,
            "collection_snapshot_id": format!("snapshot-{run_id}"),
            "job_id": format!("job-{run_id}"),
            "scope_unit_id": scope_unit_id,
            "provider": "live-test",
            "endpoint": "fixture",
            "endpoint_slug": "live-test-fixture",
            "bronze_object_key": "bronze/live/raw.json",
            "bronze_object_count": 1,
            "bronze_checksum_sha256": "a".repeat(64),
            "bronze_size_bytes": 128,
            "source_record_count": 1,
            "request_count": 1,
            "request_fingerprint_sha256": "b".repeat(64),
            "request_fingerprint_schema_version": "foundation-platform.live-test.v1",
            "license": null,
            "srid": null,
            "reused_bronze_object": false,
            "fetched_at_utc": "2026-07-26T00:00:00Z",
            "occurred_at": "2026-07-26T00:00:01Z"
        }),
        occurred_at: chrono::DateTime::parse_from_rfc3339("2026-07-26T00:00:02Z")
            .expect("fixed timestamp")
            .with_timezone(&chrono::Utc),
        scope: OutboxScope::Catalog,
    }
}
