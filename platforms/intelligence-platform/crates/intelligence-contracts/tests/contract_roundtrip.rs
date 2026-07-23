#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::Utc;
use intelligence_contracts::{
    sanitize_safe_error_message, schema_subject_for_topic, DeadLetterRecord,
    DeadLetterSourceMetadata, EventEnvelope, EventHeader, TraceContext, DEAD_LETTER_TOPIC,
    DEFAULT_SAFE_ERROR_MESSAGE, FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC,
    NORMALIZATION_SUBMISSION_REQUESTED_TOPIC, SAFE_ERROR_MESSAGE_MAX_CHARS,
};

#[test]
fn public_contract_types_are_owned_by_contracts_crate() {
    let trace = TraceContext {
        trace_id: "trace-1".to_owned(),
        tenant_id: "tenant-1".to_owned(),
        human_user_id: "staff-1".to_owned(),
        product_id: "foundation-platform".to_owned(),
    };
    let envelope = EventEnvelope {
        event_id: "event-1".to_owned(),
        event_type: "normalization-proposal.submission-requested".to_owned(),
        source: "/intelligence-platform/normalization".to_owned(),
        occurred_at: Utc::now(),
        traceparent: Some(trace.trace_id.clone()),
        tracestate: None,
    };
    let headers: Vec<EventHeader> = envelope.cloud_event_headers();
    let dead_letter = DeadLetterRecord::from_safe_metadata(
        DeadLetterSourceMetadata {
            event_id: envelope.event_id,
            source_topic: "source-topic".to_owned(),
            source_partition: 0,
            source_offset: 1,
            source_key: None,
            schema_id: None,
            event_type: Some(envelope.event_type),
            trace_id: Some(trace.trace_id),
            occurred_at_millis: Utc::now().timestamp_millis(),
        },
        "decode",
        "decode failed",
    );
    assert!(!headers.is_empty());
    assert_eq!(dead_letter.source_topic(), "source-topic");
}

#[test]
fn schema_subject_uses_topic_name_strategy() {
    assert_eq!(
        schema_subject_for_topic(NORMALIZATION_SUBMISSION_REQUESTED_TOPIC),
        "intelligence.normalization-proposal.submission-requested.v1-value"
    );
    assert_eq!(
        schema_subject_for_topic(DEAD_LETTER_TOPIC),
        "intelligence.dead-letter.v1-value"
    );
    assert_eq!(
        schema_subject_for_topic(FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC),
        "intelligence-platform.fixture.foundation-knowledge-source.upserted.v1-value"
    );
}

#[test]
fn cloud_event_headers_include_required_binary_mode_fields() {
    let envelope = EventEnvelope {
        event_id: "018f7c6a-0000-7000-8000-000000000001".to_string(),
        event_type: "normalization-proposal.submission-requested".to_string(),
        source: "/intelligence-platform/normalization".to_string(),
        occurred_at: chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
        tracestate: Some("perfectory=tenant-1".to_string()),
    };

    let headers = envelope.cloud_event_headers();

    assert!(headers
        .iter()
        .any(|header| header.key == "ce_id" && header.value == envelope.event_id));
    assert!(headers
        .iter()
        .any(|header| header.key == "ce_type" && header.value == envelope.event_type));
    assert!(headers
        .iter()
        .any(|header| header.key == "ce_source" && header.value == envelope.source));
    assert!(headers
        .iter()
        .any(|header| header.key == "ce_specversion" && header.value == "1.0"));
    assert!(headers
        .iter()
        .any(|header| header.key == "ce_time" && header.value == "2026-07-03T00:00:00+00:00"));
    assert!(headers.iter().any(|header| header.key == "traceparent"));
    assert!(headers.iter().any(|header| header.key == "tracestate"));
}

#[test]
fn cloud_event_headers_omit_empty_trace_headers() {
    let with_none = EventEnvelope {
        event_id: "018f7c6a-0000-7000-8000-000000000002".to_string(),
        event_type: "normalization-proposal.submission-requested".to_string(),
        source: "/intelligence-platform/normalization".to_string(),
        occurred_at: chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        traceparent: None,
        tracestate: None,
    };

    let headers = with_none.cloud_event_headers();
    assert!(!headers.iter().any(|header| header.key == "traceparent"));
    assert!(!headers.iter().any(|header| header.key == "tracestate"));

    let with_empty = EventEnvelope {
        event_id: "018f7c6a-0000-7000-8000-000000000003".to_string(),
        event_type: "normalization-proposal.submission-requested".to_string(),
        source: "/intelligence-platform/normalization".to_string(),
        occurred_at: chrono::DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        traceparent: Some(String::new()),
        tracestate: Some(String::new()),
    };

    let headers = with_empty.cloud_event_headers();
    assert!(!headers.iter().any(|header| header.key == "traceparent"));
    assert!(!headers.iter().any(|header| header.key == "tracestate"));
}

#[test]
fn safe_error_message_sanitizer_removes_controls_repairs_utf8_and_caps_length() {
    let sanitized = sanitize_safe_error_message(b" line\none\t\x00bad:\xF0\x28\x8C\x28 ");

    assert_eq!(sanitized, "line one bad:?(?(");
    assert!(!sanitized.chars().any(char::is_control));

    let oversized = "a".repeat(SAFE_ERROR_MESSAGE_MAX_CHARS + 25);
    let capped = sanitize_safe_error_message(oversized.as_bytes());

    assert_eq!(capped.chars().count(), SAFE_ERROR_MESSAGE_MAX_CHARS);
}

#[test]
fn safe_error_message_sanitizer_uses_redacted_default_when_input_has_no_content() {
    assert_eq!(
        sanitize_safe_error_message(b"\n\t\x00\r"),
        DEFAULT_SAFE_ERROR_MESSAGE
    );
}

#[test]
fn dead_letter_record_from_safe_metadata_never_requires_raw_payload_or_error_text() {
    let source = DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000120".to_string(),
        source_topic: "source.topic.v1".to_string(),
        source_partition: 2,
        source_offset: 99,
        source_key: Some("source-key".to_string()),
        schema_id: Some(11),
        event_type: Some("source.event".to_string()),
        trace_id: Some("trace-1".to_string()),
        occurred_at_millis: 1_783_036_800_000,
    };

    let record = DeadLetterRecord::from_safe_metadata(
        source,
        "invalid_payload",
        b"event payload was invalid\n",
    );

    assert_eq!(record.source_topic(), "source.topic.v1");
    assert_eq!(record.source_partition(), 2);
    assert_eq!(record.source_offset(), 99);
    assert_eq!(record.source_key(), Some("source-key"));
    assert_eq!(record.schema_id(), Some(11));
    assert_eq!(record.event_type(), Some("source.event"));
    assert_eq!(record.trace_id(), Some("trace-1"));
    assert_eq!(record.failure_class(), "invalid_payload");
    assert_eq!(record.safe_error_message(), "event payload was invalid");
    assert_eq!(record.occurred_at_millis(), 1_783_036_800_000);
}

#[test]
fn dead_letter_record_exposes_read_only_safe_accessors() {
    let record = DeadLetterRecord::from_safe_metadata(
        DeadLetterSourceMetadata {
            event_id: "018f7c6a-0000-7000-8000-000000000121".to_string(),
            source_topic: "source.topic.v1".to_string(),
            source_partition: 2,
            source_offset: 99,
            source_key: Some("source-key".to_string()),
            schema_id: Some(11),
            event_type: Some("source.event".to_string()),
            trace_id: Some("trace-1".to_string()),
            occurred_at_millis: 1_783_036_800_000,
        },
        "invalid_payload",
        b"event payload was invalid\n",
    );

    assert_eq!(record.event_id(), "018f7c6a-0000-7000-8000-000000000121");
    assert_eq!(record.source_topic(), "source.topic.v1");
    assert_eq!(record.source_partition(), 2);
    assert_eq!(record.source_offset(), 99);
    assert_eq!(record.source_key(), Some("source-key"));
    assert_eq!(record.schema_id(), Some(11));
    assert_eq!(record.event_type(), Some("source.event"));
    assert_eq!(record.trace_id(), Some("trace-1"));
    assert_eq!(record.failure_class(), "invalid_payload");
    assert_eq!(record.safe_error_message(), "event payload was invalid");
    assert_eq!(record.occurred_at_millis(), 1_783_036_800_000);
}
