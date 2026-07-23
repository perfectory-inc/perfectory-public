#![allow(clippy::unwrap_used, clippy::expect_used)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use intelligence_contracts::{DeadLetterRecord, DeadLetterSourceMetadata, EventHeader};
use messaging_infrastructure::{
    dead_letter_publisher::DeadLetterPublisher,
    kafka::{
        headers_to_owned_pairs, EventPayloadPublisher, KafkaConsumerConfig, KafkaEventConsumer,
        KafkaEventProducer, KafkaProducerConfig, KafkaPublishError,
    },
};
use std::sync::{Arc, Mutex};

type CapturedPublishCalls = Arc<Mutex<Vec<(String, String, Vec<u8>, Vec<EventHeader>)>>>;

fn dead_letter_record(
    event_id: &str,
    source_partition: i32,
    source_offset: i64,
    schema_id: i32,
    occurred_at_millis: i64,
) -> DeadLetterRecord {
    DeadLetterRecord::from_safe_metadata(
        DeadLetterSourceMetadata {
            event_id: event_id.to_string(),
            source_topic: "intelligence.normalization-proposal.submission-requested.v1".to_string(),
            source_partition,
            source_offset,
            source_key: Some("source-key".to_string()),
            schema_id: Some(schema_id),
            event_type: Some("normalization-proposal.submission-requested".to_string()),
            trace_id: None,
            occurred_at_millis,
        },
        "validation",
        "schema mismatch",
    )
}

#[test]
fn producer_config_requires_bootstrap_servers() {
    let config = KafkaProducerConfig {
        bootstrap_servers: String::new(),
        client_id: "intelligence-messaging-producer".to_string(),
        linger_ms: 10,
        message_timeout_ms: 1_000,
    };

    assert!(config.validate().is_err());
}

#[test]
fn producer_config_requires_client_id() {
    let config = KafkaProducerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        client_id: String::new(),
        linger_ms: 10,
        message_timeout_ms: 1_000,
    };

    assert!(config.validate().is_err());
}

#[test]
fn producer_config_requires_message_timeout_ms_to_be_positive() {
    let config = KafkaProducerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        client_id: "intelligence-messaging-producer".to_string(),
        linger_ms: 10,
        message_timeout_ms: 0,
    };

    assert!(config.validate().is_err());
}

#[test]
fn producer_config_accepts_fully_valid_settings() {
    let config = KafkaProducerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        client_id: "intelligence-messaging-producer".to_string(),
        linger_ms: 10,
        message_timeout_ms: 1_000,
    };

    assert!(config.validate().is_ok());
}

#[test]
fn event_producer_rejects_invalid_config_before_touching_network() {
    let config = KafkaProducerConfig {
        bootstrap_servers: String::new(),
        client_id: "ip-events".to_string(),
        linger_ms: 5,
        message_timeout_ms: 30_000,
    };

    assert!(KafkaEventProducer::new(config).is_err());
}

#[test]
fn consumer_config_requires_bootstrap_servers() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: String::new(),
        group_id: "intelligence-messaging".to_string(),
        client_id: "intelligence-messaging-consumer".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 10_000,
    };

    assert!(config.validate().is_err());
}

#[test]
fn consumer_config_requires_client_id() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "intelligence-messaging".to_string(),
        client_id: String::new(),
        enable_auto_commit: false,
        session_timeout_ms: 10_000,
    };

    assert!(config.validate().is_err());
}

#[test]
fn consumer_config_requires_group_id_and_manual_commit() {
    let missing_group_id = KafkaConsumerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: String::new(),
        client_id: "intelligence-messaging-consumer".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 10_000,
    };

    assert!(missing_group_id.validate().is_err());

    let auto_commit_enabled = KafkaConsumerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "intelligence-messaging".to_string(),
        client_id: "intelligence-messaging-consumer".to_string(),
        enable_auto_commit: true,
        session_timeout_ms: 10_000,
    };

    assert!(auto_commit_enabled.validate().is_err());
}

#[test]
fn consumer_config_requires_session_timeout_ms_to_be_positive() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "intelligence-messaging".to_string(),
        client_id: "intelligence-messaging-consumer".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 0,
    };

    assert!(config.validate().is_err());
}

#[test]
fn consumer_config_accepts_fully_valid_settings() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "intelligence-messaging".to_string(),
        client_id: "intelligence-messaging-consumer".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 10_000,
    };

    assert!(config.validate().is_ok());
}

#[test]
fn event_consumer_rejects_empty_topics() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "127.0.0.1:9092".to_string(),
        group_id: "ip-worker".to_string(),
        client_id: "ip-worker".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 45_000,
    };

    assert!(KafkaEventConsumer::new(config, &[]).is_err());
}

#[test]
fn event_consumer_rejects_blank_topic_names() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "127.0.0.1:9092".to_string(),
        group_id: "ip-worker".to_string(),
        client_id: "ip-worker".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 45_000,
    };

    assert!(KafkaEventConsumer::new(config.clone(), &[""]).is_err());
    assert!(KafkaEventConsumer::new(config, &["   "]).is_err());
}

#[test]
fn event_consumer_rejects_auto_commit() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "127.0.0.1:9092".to_string(),
        group_id: "ip-worker".to_string(),
        client_id: "ip-worker".to_string(),
        enable_auto_commit: true,
        session_timeout_ms: 45_000,
    };

    assert!(KafkaEventConsumer::new(config, &["topic-a"]).is_err());
}

#[tokio::test]
async fn event_consumer_exposes_empty_assignment_before_group_join() {
    let config = KafkaConsumerConfig {
        bootstrap_servers: "127.0.0.1:9092".to_string(),
        group_id: "ip-worker".to_string(),
        client_id: "ip-worker".to_string(),
        enable_auto_commit: false,
        session_timeout_ms: 45_000,
    };

    let consumer = KafkaEventConsumer::new(config, &["topic-a"]).expect("consumer must build");
    let assignment = consumer
        .assignment()
        .expect("assignment query must succeed");

    assert_eq!(assignment.count(), 0);
}

#[test]
fn header_mapping_preserves_cloud_event_values() {
    let headers = vec![
        EventHeader {
            key: "ce_id".to_string(),
            value: "018f7c6a-0000-7000-8000-000000000001".to_string(),
        },
        EventHeader {
            key: "ce_type".to_string(),
            value: "normalization-proposal.submission-requested".to_string(),
        },
        EventHeader {
            key: "ce_source".to_string(),
            value: "/intelligence-platform/normalization".to_string(),
        },
        EventHeader {
            key: "ce_time".to_string(),
            value: "2026-07-03T00:00:00+00:00".to_string(),
        },
        EventHeader {
            key: "ce_specversion".to_string(),
            value: "1.0".to_string(),
        },
        EventHeader {
            key: "traceparent".to_string(),
            value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        },
        EventHeader {
            key: "tracestate".to_string(),
            value: "perfectory=tenant-1".to_string(),
        },
    ];
    let mapped = headers_to_owned_pairs(&headers);

    assert_eq!(
        mapped,
        vec![
            (
                "ce_id".to_string(),
                "018f7c6a-0000-7000-8000-000000000001".to_string()
            ),
            (
                "ce_type".to_string(),
                "normalization-proposal.submission-requested".to_string()
            ),
            (
                "ce_source".to_string(),
                "/intelligence-platform/normalization".to_string()
            ),
            (
                "ce_time".to_string(),
                "2026-07-03T00:00:00+00:00".to_string()
            ),
            ("ce_specversion".to_string(), "1.0".to_string()),
            (
                "traceparent".to_string(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            ("tracestate".to_string(), "perfectory=tenant-1".to_string()),
        ]
    );
}

#[test]
fn dead_letter_publisher_uses_dead_letter_topic_and_source_key() {
    struct CapturingPublisher {
        calls: CapturedPublishCalls,
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
            self.calls.lock().unwrap().push((
                topic.to_string(),
                key.to_string(),
                payload.to_vec(),
                _headers.to_vec(),
            ));
            Ok(())
        }
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let publisher = CapturingPublisher {
        calls: Arc::clone(&calls),
    };
    let record = dead_letter_record(
        "018f7c6a-0000-7000-8000-000000000010",
        3,
        42,
        7,
        1_720_000_000_000,
    );

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        DeadLetterPublisher::new(publisher)
            .publish_encoded("source-key", vec![1, 2, 3], record)
            .await
            .unwrap();
    });

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "intelligence.dead-letter.v1");
    assert_eq!(calls[0].1, "source-key");
    assert_eq!(calls[0].2, vec![1, 2, 3]);
    let headers = &calls[0].3;
    assert!(headers.iter().any(|header| {
        header.key == "ce_id" && header.value == "018f7c6a-0000-7000-8000-000000000010"
    }));
    assert!(headers
        .iter()
        .any(|header| { header.key == "ce_type" && header.value == "intelligence.dead-letter" }));
    assert!(headers.iter().any(|header| {
        header.key == "ce_source" && header.value == "/intelligence-platform/dead-letter"
    }));
    assert!(headers
        .iter()
        .any(|header| { header.key == "ce_specversion" && header.value == "1.0" }));
    assert!(headers.iter().any(|header| {
        header.key == "ce_time"
            && header.value
                == DateTime::<Utc>::from_timestamp_millis(1_720_000_000_000)
                    .unwrap()
                    .to_rfc3339()
    }));
    assert!(!headers.iter().any(|header| header.key == "traceparent"));
    assert!(!headers.iter().any(|header| header.key == "tracestate"));
}

#[test]
fn dead_letter_publisher_rejects_invalid_occurred_at_without_invoking_publisher() {
    struct CapturingPublisher {
        calls: CapturedPublishCalls,
    }

    #[async_trait]
    impl EventPayloadPublisher for CapturingPublisher {
        async fn publish(
            &self,
            topic: &str,
            key: &str,
            payload: &[u8],
            headers: &[EventHeader],
        ) -> Result<(), KafkaPublishError> {
            self.calls.lock().unwrap().push((
                topic.to_string(),
                key.to_string(),
                payload.to_vec(),
                headers.to_vec(),
            ));
            Ok(())
        }
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let publisher = CapturingPublisher {
        calls: Arc::clone(&calls),
    };
    let record = dead_letter_record("018f7c6a-0000-7000-8000-000000000011", 4, 99, 9, i64::MAX);

    let error = tokio::runtime::Runtime::new().unwrap().block_on(async {
        DeadLetterPublisher::new(publisher)
            .publish_encoded("source-key", vec![1, 2, 3], record)
            .await
            .unwrap_err()
    });

    assert_eq!(
        error,
        KafkaPublishError::Publish {
            message: format!("invalid dead-letter occurred_at_millis: {}", i64::MAX),
        }
    );
    assert!(calls.lock().unwrap().is_empty());
}
