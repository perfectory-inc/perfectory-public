#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use async_trait::async_trait;
use foundation_outbox::{
    EventBroadcaster, EventEnvelope, KafkaEventBroadcaster, KafkaPayloadPublisher, PublishError,
    DEFAULT_RAW_WRITTEN_TOPIC, RAW_WRITTEN_AVRO_SCHEMA,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

type RecordedCall = (String, String, Vec<u8>);
type RecordedCalls = Arc<Mutex<Vec<RecordedCall>>>;

#[derive(Debug, PartialEq, Eq)]
enum ConsumerDecision {
    Applied,
    Duplicate,
}

#[derive(Debug)]
struct DecodedClaimCheck {
    event_id: String,
    bronze_object_key: String,
    bronze_checksum_sha256: String,
    bronze_object_count: u64,
}

/// A deliberately small consumer-side contract probe.
///
/// It models the two invariants every real Silver/Gold consumer must preserve:
/// the outbox `event_id` is the at-least-once deduplication key, and the Kafka
/// value is only a claim-check whose Bronze object must be read and verified.
/// The production consumer is not invented here; this test makes its required
/// boundary executable before a concrete downstream owner is selected.
#[derive(Default)]
struct ClaimCheckConsumerProbe {
    objects: HashMap<String, Vec<u8>>,
    seen_event_ids: HashSet<String>,
    object_reads: usize,
}

impl ClaimCheckConsumerProbe {
    fn insert_object(&mut self, object_key: String, bytes: Vec<u8>) {
        self.objects.insert(object_key, bytes);
    }

    fn consume(&mut self, payload: &[u8]) -> Result<ConsumerDecision, String> {
        let claim_check = decode_claim_check(payload)?;
        if self.seen_event_ids.contains(&claim_check.event_id) {
            return Ok(ConsumerDecision::Duplicate);
        }

        let bytes = self
            .objects
            .get(&claim_check.bronze_object_key)
            .ok_or_else(|| {
                format!(
                    "Bronze claim-check object is missing: {}",
                    claim_check.bronze_object_key
                )
            })?;
        self.object_reads += 1;
        let actual_checksum = format!("{:x}", Sha256::digest(bytes));
        if actual_checksum != claim_check.bronze_checksum_sha256 {
            return Err(format!(
                "Bronze claim-check checksum mismatch for {}",
                claim_check.bronze_object_key
            ));
        }
        if claim_check.bronze_object_count == 0 {
            return Err("Bronze claim-check object count must be positive".to_owned());
        }

        self.seen_event_ids.insert(claim_check.event_id);
        Ok(ConsumerDecision::Applied)
    }
}

fn decode_claim_check(payload: &[u8]) -> Result<DecodedClaimCheck, String> {
    if payload.len() < 5 {
        return Err("Confluent-Avro payload is missing its five-byte header".to_owned());
    }
    let schema = Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).map_err(|error| error.to_string())?;
    let mut datum = &payload[5..];
    let AvroValue::Record(fields) =
        from_avro_datum(&schema, &mut datum, None).map_err(|error| error.to_string())?
    else {
        return Err("raw_written payload must decode as an Avro record".to_owned());
    };

    let string_field = |name: &str| {
        fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .and_then(|(_, value)| match value {
                AvroValue::String(value) => Some(value.clone()),
                _ => None,
            })
            .ok_or_else(|| format!("raw_written payload is missing string field {name}"))
    };
    let long_field = |name: &str| {
        fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .and_then(|(_, value)| match value {
                AvroValue::Long(value) => u64::try_from(*value).ok(),
                _ => None,
            })
            .ok_or_else(|| format!("raw_written payload is missing long field {name}"))
    };

    Ok(DecodedClaimCheck {
        event_id: string_field("event_id")?,
        bronze_object_key: string_field("bronze_object_key")?,
        bronze_checksum_sha256: string_field("bronze_checksum_sha256")?,
        bronze_object_count: long_field("bronze_object_count")?,
    })
}

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
async fn retrying_same_outbox_event_preserves_event_id_and_claim_check() {
    let publisher = RecordingKafkaPublisher::default();
    let calls = Arc::clone(&publisher.calls);
    let broadcaster = KafkaEventBroadcaster::new(
        publisher,
        DEFAULT_RAW_WRITTEN_TOPIC,
        Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap(),
        17,
        Arc::new(RecordingFallback::default()),
        false,
    )
    .unwrap();
    let event = raw_written_event();

    broadcaster.publish(&event).await.unwrap();
    broadcaster.publish(&event).await.unwrap();

    let payload = {
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0].1, calls[1].1,
            "partition key must be stable on retry"
        );
        assert_eq!(
            calls[0].2, calls[1].2,
            "event payload must be stable on retry"
        );
        calls[0].2.clone()
    };

    let schema = Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap();
    let mut datum = &payload[5..];
    let decoded = from_avro_datum(&schema, &mut datum, None).unwrap();
    let fields = match decoded {
        AvroValue::Record(fields) => fields,
        _ => Vec::new(),
    };
    assert!(
        !fields.is_empty(),
        "retry payload must decode as an Avro record"
    );
    assert_eq!(
        fields
            .iter()
            .find(|(name, _)| name == "event_id")
            .map(|(_, value)| value),
        Some(&AvroValue::String(event.event_id.to_string()))
    );
    assert!(fields.iter().all(|(name, _)| {
        name != "bronze_bytes" && name != "object_content" && name != "payload"
    }));
}

#[tokio::test]
async fn consumer_contract_deduplicates_event_id_and_reads_bronze_claim_check() {
    let publisher = RecordingKafkaPublisher::default();
    let calls = Arc::clone(&publisher.calls);
    let broadcaster = KafkaEventBroadcaster::new(
        publisher,
        DEFAULT_RAW_WRITTEN_TOPIC,
        Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA).unwrap(),
        17,
        Arc::new(RecordingFallback::default()),
        false,
    )
    .unwrap();

    let bronze_bytes = b"bronze-claim-check-fixture".to_vec();
    let mut event = raw_written_event();
    event.payload["bronze_checksum_sha256"] = json!(format!("{:x}", Sha256::digest(&bronze_bytes)));
    let bronze_object_key = event.payload["bronze_object_key"]
        .as_str()
        .unwrap()
        .to_owned();

    broadcaster.publish(&event).await.unwrap();
    let payload = calls.lock().unwrap()[0].2.clone();

    let mut consumer = ClaimCheckConsumerProbe::default();
    consumer.insert_object(bronze_object_key, bronze_bytes);
    assert_eq!(
        consumer.consume(&payload).unwrap(),
        ConsumerDecision::Applied,
        "first delivery must read and apply the Bronze claim-check"
    );
    assert_eq!(
        consumer.consume(&payload).unwrap(),
        ConsumerDecision::Duplicate,
        "redelivery must be ignored by the stable outbox event_id"
    );
    assert_eq!(consumer.object_reads, 1, "duplicate must not reread Bronze");
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
