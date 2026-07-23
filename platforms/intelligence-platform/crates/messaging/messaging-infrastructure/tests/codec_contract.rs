#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;

use apache_avro::{types::Value as AvroValue, Reader, Schema, Writer};
use intelligence_contracts::{
    DeadLetterRecord, DeadLetterSourceMetadata, SAFE_ERROR_MESSAGE_MAX_CHARS,
};
use messaging_infrastructure::{
    avro_codec::{ConfluentAvroCodec, EventCodecError},
    dead_letter_publisher::{dead_letter_from_avro_value, dead_letter_to_avro_value},
};

const DEAD_LETTER_SCHEMA: &str =
    include_str!("../../../../schemas/intelligence.dead-letter.v1.avsc");
const REQUIRED_DEAD_LETTER_FIELDS: &[&str] = &[
    "event_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "failure_class",
    "safe_error_message",
    "occurred_at",
];

#[test]
fn dead_letter_schema_parses_and_round_trips() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let record = dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000099".to_string(),
        source_topic: "foundation-platform.knowledge-source.upserted.v1".to_string(),
        source_partition: 0,
        source_offset: 42,
        source_key: Some("tenant-1:product-1:source-1".to_string()),
        schema_id: Some(7),
        event_type: Some("foundation-platform.knowledge-source.upserted".to_string()),
        trace_id: Some("trace-1".to_string()),
        occurred_at_millis: 1_783_036_800_000,
    });

    let fields = round_trip_fields(&schema, &record);

    assert_eq!(
        field(&fields, "event_id"),
        &AvroValue::String("018f7c6a-0000-7000-8000-000000000099".to_string())
    );
    assert_eq!(
        field(&fields, "source_topic"),
        &AvroValue::String("foundation-platform.knowledge-source.upserted.v1".to_string())
    );
    assert_eq!(field(&fields, "source_partition"), &AvroValue::Int(0));
    assert_eq!(field(&fields, "source_offset"), &AvroValue::Long(42));
    assert_eq!(
        field(&fields, "source_key"),
        &AvroValue::Union(
            1,
            Box::new(AvroValue::String("tenant-1:product-1:source-1".to_string(),)),
        )
    );
    assert_eq!(
        field(&fields, "schema_id"),
        &AvroValue::Union(1, Box::new(AvroValue::Int(7)))
    );
    assert_eq!(
        field(&fields, "event_type"),
        &AvroValue::Union(
            1,
            Box::new(AvroValue::String(
                "foundation-platform.knowledge-source.upserted".to_string(),
            )),
        )
    );
    assert_eq!(
        field(&fields, "trace_id"),
        &AvroValue::Union(1, Box::new(AvroValue::String("trace-1".to_string())))
    );
    assert_eq!(
        field(&fields, "failure_class"),
        &AvroValue::String("invalid_payload".to_string())
    );
    assert_eq!(
        field(&fields, "safe_error_message"),
        &AvroValue::String("event payload was invalid".to_string())
    );
    assert_eq!(
        field(&fields, "occurred_at"),
        &AvroValue::TimestampMillis(1_783_036_800_000)
    );
}

#[test]
fn dead_letter_nullable_union_null_branches_round_trip() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let record = dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000100".to_string(),
        source_topic: "foundation-platform.knowledge-source.upserted.v1".to_string(),
        source_partition: 1,
        source_offset: 100,
        source_key: None,
        schema_id: None,
        event_type: None,
        trace_id: None,
        occurred_at_millis: 1_783_036_800_500,
    });

    let fields = round_trip_fields(&schema, &record);

    assert_eq!(
        field(&fields, "source_key"),
        &AvroValue::Union(0, Box::new(AvroValue::Null))
    );
    assert_eq!(
        field(&fields, "schema_id"),
        &AvroValue::Union(0, Box::new(AvroValue::Null))
    );
    assert_eq!(
        field(&fields, "event_type"),
        &AvroValue::Union(0, Box::new(AvroValue::Null))
    );
    assert_eq!(
        field(&fields, "trace_id"),
        &AvroValue::Union(0, Box::new(AvroValue::Null))
    );
}

#[test]
fn dead_letter_avro_conversion_round_trips_all_optional_fields_and_timestamp_forms() {
    let with_values = dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000102".to_string(),
        source_topic: "foundation-platform.knowledge-source.upserted.v1".to_string(),
        source_partition: -1,
        source_offset: 1_234_567,
        source_key: Some("tenant-1:product-1:source-1".to_string()),
        schema_id: Some(29),
        event_type: Some("foundation-platform.knowledge-source.upserted".to_string()),
        trace_id: Some("trace-1".to_string()),
        occurred_at_millis: 1_783_036_800_500,
    });
    let without_values = dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000103".to_string(),
        source_topic: "source.topic.v1".to_string(),
        source_partition: 0,
        source_offset: 0,
        source_key: None,
        schema_id: None,
        event_type: None,
        trace_id: None,
        occurred_at_millis: 1_783_036_800_000,
    });

    assert_eq!(
        dead_letter_from_avro_value(dead_letter_to_avro_value(&with_values)).unwrap(),
        with_values
    );
    assert_eq!(
        dead_letter_from_avro_value(dead_letter_to_avro_value(&without_values)).unwrap(),
        without_values
    );

    let mut timestamp_as_long = dead_letter_to_avro_value(&with_values);
    replace_record_field(
        &mut timestamp_as_long,
        "occurred_at",
        AvroValue::Long(with_values.occurred_at_millis()),
    );
    assert_eq!(
        dead_letter_from_avro_value(timestamp_as_long).unwrap(),
        with_values
    );
}

#[test]
fn dead_letter_decoder_rejects_unsafe_persisted_safe_error_messages() {
    let invalid_messages = vec![
        "contains\ncontrol".to_string(),
        "contains  repeated whitespace".to_string(),
        "contains \u{FFFD} replacement".to_string(),
        String::new(),
        "a".repeat(SAFE_ERROR_MESSAGE_MAX_CHARS + 1),
    ];

    for message in invalid_messages {
        let mut value = dead_letter_to_avro_value(&dead_letter_record(DeadLetterSourceMetadata {
            event_id: "018f7c6a-0000-7000-8000-000000000104".to_string(),
            source_topic: "source.topic.v1".to_string(),
            source_partition: 0,
            source_offset: 1,
            source_key: None,
            schema_id: None,
            event_type: None,
            trace_id: None,
            occurred_at_millis: 1_783_036_800_000,
        }));
        replace_record_field(
            &mut value,
            "safe_error_message",
            AvroValue::String(message.clone()),
        );

        assert!(
            dead_letter_from_avro_value(value).is_err(),
            "decoder accepted unsafe persisted safe_error_message: {message:?}"
        );
    }
}

#[test]
fn dead_letter_decoder_rejects_malformed_safe_error_message_field() {
    let mut value = dead_letter_to_avro_value(&dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000105".to_string(),
        source_topic: "source.topic.v1".to_string(),
        source_partition: 0,
        source_offset: 1,
        source_key: None,
        schema_id: None,
        event_type: None,
        trace_id: None,
        occurred_at_millis: 1_783_036_800_000,
    }));
    replace_record_field(&mut value, "safe_error_message", AvroValue::Null);

    assert!(dead_letter_from_avro_value(value).is_err());
}

#[test]
fn dead_letter_schema_defaults_cover_evolvable_fields() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let record_fields = match &schema {
        Schema::Record(record_schema) => &record_schema.fields,
        other => panic!("schema must be a Record, got {other:?}"),
    };
    let required: HashSet<&str> = REQUIRED_DEAD_LETTER_FIELDS.iter().copied().collect();
    let mut violations = Vec::new();

    for field in record_fields {
        if required.contains(field.name.as_str()) {
            continue;
        }
        if field.default.is_none() {
            violations.push(format!(
                "field '{}' is neither required-by-design nor defaulted for BACKWARD_TRANSITIVE evolution",
                field.name
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "dead-letter schema evolution tripwire triggered:\n{}",
        violations.join("\n")
    );
}

#[test]
fn confluent_wire_codec_prefixes_schema_id_and_decodes_record() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let value = dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000100".to_string(),
        source_topic: "topic-a".to_string(),
        source_partition: 1,
        source_offset: 99,
        source_key: None,
        schema_id: Some(9),
        event_type: None,
        trace_id: None,
        occurred_at_millis: 1_783_036_800_000,
    });
    let value = dead_letter_to_avro_value(&value);

    let encoded = ConfluentAvroCodec::encode(123, &schema, &value).expect("encode must work");

    assert_eq!(encoded[0], 0, "first byte must be Confluent magic byte");
    assert_eq!(&encoded[1..5], &123_i32.to_be_bytes());

    let (schema_id, decoded) =
        ConfluentAvroCodec::decode(&schema, &encoded).expect("decode must work");

    assert_eq!(schema_id, 123);
    assert_eq!(decoded, value);
}

#[test]
fn confluent_wire_codec_rejects_payloads_shorter_than_header() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let err = ConfluentAvroCodec::decode(&schema, &[0, 0, 0, 0]).expect_err("must reject");

    assert!(matches!(err, EventCodecError::InvalidWireFormat { .. }));
}

#[test]
fn confluent_wire_codec_rejects_non_zero_magic_byte() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let err = ConfluentAvroCodec::decode(&schema, &[1, 0, 0, 0, 0, 0]).expect_err("must reject");

    assert!(matches!(err, EventCodecError::InvalidWireFormat { .. }));
}

#[test]
fn confluent_wire_codec_rejects_trailing_bytes_after_valid_datum() {
    let schema = Schema::parse_str(DEAD_LETTER_SCHEMA).expect("schema must parse");
    let value = dead_letter_record(DeadLetterSourceMetadata {
        event_id: "018f7c6a-0000-7000-8000-000000000101".to_string(),
        source_topic: "topic-a".to_string(),
        source_partition: 1,
        source_offset: 99,
        source_key: None,
        schema_id: Some(9),
        event_type: None,
        trace_id: None,
        occurred_at_millis: 1_783_036_800_000,
    });
    let value = dead_letter_to_avro_value(&value);

    let mut encoded = ConfluentAvroCodec::encode(123, &schema, &value).expect("encode must work");
    encoded.extend_from_slice(&[1, 2, 3]);

    let err = ConfluentAvroCodec::decode(&schema, &encoded).expect_err("must reject");

    assert!(matches!(err, EventCodecError::InvalidWireFormat { .. }));
}

#[test]
fn confluent_wire_codec_round_trips_zero_byte_null_datum() {
    let schema = Schema::parse_str("\"null\"").expect("null schema must parse");
    let value = AvroValue::Null;

    let encoded = ConfluentAvroCodec::encode(123, &schema, &value).expect("encode must work");

    assert_eq!(encoded.len(), 5);
    assert_eq!(encoded[0], 0);
    assert_eq!(&encoded[1..5], &123_i32.to_be_bytes());

    let (schema_id, decoded) =
        ConfluentAvroCodec::decode(&schema, &encoded).expect("decode must work");

    assert_eq!(schema_id, 123);
    assert_eq!(decoded, value);
}

#[test]
fn event_codec_error_safe_messages_are_stable_for_callers() {
    let invalid_wire = EventCodecError::InvalidWireFormat {
        message: "wire format invalid".to_string(),
    };
    let avro = EventCodecError::Avro {
        message: "avro failed".to_string(),
    };

    assert_eq!(invalid_wire.safe_message(), "invalid wire format");
    assert_eq!(avro.safe_message(), "avro codec error");
}

fn round_trip_fields(schema: &Schema, record: &DeadLetterRecord) -> Vec<(String, AvroValue)> {
    let mut writer = Writer::new(schema, Vec::new());
    writer
        .append_value_ref(&dead_letter_to_avro_value(record))
        .expect("dead-letter record must satisfy schema");
    let encoded = writer.into_inner().expect("writer must finish");

    let mut reader = Reader::new(&encoded[..]).expect("reader must decode");
    let decoded = reader
        .next()
        .expect("reader must yield a record")
        .expect("dead-letter record must decode");

    assert!(
        reader.next().is_none(),
        "expected exactly one record in the Avro container"
    );

    match decoded {
        AvroValue::Record(fields) => fields,
        other => panic!("expected AvroValue::Record, got {other:?}"),
    }
}

fn dead_letter_record(source: DeadLetterSourceMetadata) -> DeadLetterRecord {
    DeadLetterRecord::from_safe_metadata(source, "invalid_payload", "event payload was invalid")
}

fn replace_record_field(value: &mut AvroValue, name: &str, replacement: AvroValue) {
    let AvroValue::Record(fields) = value else {
        panic!("dead-letter value must be an Avro record");
    };
    let (_, field_value) = fields
        .iter_mut()
        .find(|(field_name, _)| field_name == name)
        .unwrap_or_else(|| panic!("field '{name}' must exist"));
    *field_value = replacement;
}

fn field<'a>(fields: &'a [(String, AvroValue)], name: &str) -> &'a AvroValue {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value)
        .unwrap_or_else(|| panic!("field '{name}' missing from decoded record"))
}
