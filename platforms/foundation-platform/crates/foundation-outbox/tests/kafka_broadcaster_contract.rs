#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

use apache_avro::Schema;
use async_trait::async_trait;
use foundation_outbox::{
    EventBroadcaster, EventEnvelope, KafkaEventBroadcaster, KafkaPayloadPublisher, PublishError,
    DEFAULT_RAW_WRITTEN_TOPIC, RAW_WRITTEN_AVRO_SCHEMA,
};
use serde_json::json;
use uuid::Uuid;

type RecordedCall = (String, String, Vec<u8>);
type RecordedCalls = Arc<Mutex<Vec<RecordedCall>>>;

#[derive(Clone, Default)]
struct RecordingKafkaPublisher {
    calls: RecordedCalls,
}

#[async_trait]
impl KafkaPayloadPublisher for RecordingKafkaPublisher {
    async fn publish(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), PublishError> {
        self.calls
            .lock()
            .unwrap()
            .push((topic.to_owned(), key.to_owned(), payload.to_vec()));
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RecordingFallback {
    calls: Arc<Mutex<Vec<Uuid>>>,
}

#[async_trait]
impl EventBroadcaster for RecordingFallback {
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        self.calls.lock().unwrap().push(event.event_id);
        Ok(())
    }
}

#[tokio::test]
async fn raw_written_is_encoded_and_sent_to_kafka_with_scope_key() {
    let publisher = RecordingKafkaPublisher::default();
    let calls = Arc::clone(&publisher.calls);
    let fallback = RecordingFallback::default();
    let broadcaster = KafkaEventBroadcaster::new(
        publisher,
        "foundation.test.raw-written.v1",
        Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap(),
        17,
        Arc::new(fallback.clone()),
        false,
    )
    .unwrap();

    let event = raw_written_event();
    broadcaster.publish(&event).await.unwrap();

    {
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "foundation.test.raw-written.v1");
        assert_eq!(calls[0].1, "scope:parcel:1111010100");
        assert_eq!(calls[0].2[..5], [0, 0, 0, 0, 17]);
        assert!(calls[0].2.len() > 5);
        drop(calls);
    }
    assert!(fallback.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn canonical_topic_and_schema_are_claim_check_contracts() {
    assert_eq!(
        DEFAULT_RAW_WRITTEN_TOPIC,
        "foundation-platform.catalog.collection-raw-written.v1"
    );
    assert!(RAW_WRITTEN_AVRO_SCHEMA.contains("\"name\":\"event_id\""));
    assert!(RAW_WRITTEN_AVRO_SCHEMA.contains("\"name\":\"bronze_object_key\""));
    assert!(!RAW_WRITTEN_AVRO_SCHEMA.contains("bronze_bytes"));
    assert!(!RAW_WRITTEN_AVRO_SCHEMA.contains("object_content"));
}

#[tokio::test]
async fn non_raw_written_events_use_legacy_fallback_only() {
    let publisher = RecordingKafkaPublisher::default();
    let calls = Arc::clone(&publisher.calls);
    let fallback = RecordingFallback::default();
    let event = EventEnvelope {
        event_id: Uuid::new_v4(),
        event_type: "catalog.vector_tile_manifest.promoted.v1".to_owned(),
        payload: json!({ "type": "catalog.vector_tile_manifest.promoted.v1" }),
        occurred_at: chrono::Utc::now(),
        scope: foundation_outbox::OutboxScope::Catalog,
    };
    let event_id = event.event_id;
    let broadcaster = KafkaEventBroadcaster::new(
        publisher,
        "foundation.test.raw-written.v1",
        Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap(),
        17,
        Arc::new(fallback.clone()),
        false,
    )
    .unwrap();

    broadcaster.publish(&event).await.unwrap();

    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(fallback.calls.lock().unwrap().as_slice(), &[event_id]);
}

#[tokio::test]
async fn dual_publish_calls_legacy_fallback_after_kafka_success() {
    let publisher = RecordingKafkaPublisher::default();
    let calls = Arc::clone(&publisher.calls);
    let fallback = RecordingFallback::default();
    let broadcaster = KafkaEventBroadcaster::new(
        publisher,
        "foundation.test.raw-written.v1",
        Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap(),
        17,
        Arc::new(fallback.clone()),
        true,
    )
    .unwrap();

    let event = raw_written_event();
    let event_id = event.event_id;
    broadcaster.publish(&event).await.unwrap();

    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(fallback.calls.lock().unwrap().as_slice(), &[event_id]);
}

#[tokio::test]
async fn malformed_raw_written_payload_fails_before_kafka_or_fallback() {
    let publisher = RecordingKafkaPublisher::default();
    let calls = Arc::clone(&publisher.calls);
    let fallback = RecordingFallback::default();
    let broadcaster = KafkaEventBroadcaster::new(
        publisher,
        "foundation.test.raw-written.v1",
        Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap(),
        17,
        Arc::new(fallback.clone()),
        true,
    )
    .unwrap();
    let event = EventEnvelope {
        event_id: Uuid::new_v4(),
        event_type: "catalog.collection.raw_written.v1".to_owned(),
        payload: json!({ "type": "catalog.collection.raw_written.v1" }),
        occurred_at: chrono::Utc::now(),
        scope: foundation_outbox::OutboxScope::Catalog,
    };

    assert!(broadcaster.publish(&event).await.is_err());
    assert!(calls.lock().unwrap().is_empty());
    assert!(fallback.calls.lock().unwrap().is_empty());
}

fn raw_written_event() -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::parse_str("018f7c6a-0000-7000-8000-000000000010").unwrap(),
        event_type: "catalog.collection.raw_written.v1".to_owned(),
        payload: json!({
            "type": "catalog.collection.raw_written.v1",
            "schema_version": 1,
            "collection_snapshot_id": "snapshot-20260726",
            "job_id": "job-raw-written-1",
            "scope_unit_id": "scope:parcel:1111010100",
            "provider": "hub.go.kr",
            "endpoint": "building_register_main",
            "endpoint_slug": "hub-building-building_register_main",
            "bronze_object_key": "bronze/source=hub/file.zip",
            "bronze_object_count": 1,
            "bronze_checksum_sha256": "a".repeat(64),
            "bronze_size_bytes": 4096,
            "source_record_count": 42,
            "request_count": 1,
            "request_fingerprint_sha256": "b".repeat(64),
            "request_fingerprint_schema_version": "foundation-platform.bulk_request_fingerprint.v1",
            "license": null,
            "srid": null,
            "reused_bronze_object": false,
            "fetched_at_utc": "2026-07-26T00:00:00Z",
            "occurred_at": "2026-07-26T00:00:01Z"
        }),
        occurred_at: chrono::DateTime::parse_from_rfc3339("2026-07-26T00:00:02Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        scope: foundation_outbox::OutboxScope::Catalog,
    }
}
