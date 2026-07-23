use apache_avro::types::Value as AvroValue;
use chrono::{DateTime, Utc};

use crate::kafka::{EventPayloadPublisher, KafkaPublishError};
use intelligence_contracts::{EventEnvelope, DEAD_LETTER_TOPIC};

pub use intelligence_contracts::{
    sanitize_safe_error_message, DeadLetterRecord, DeadLetterSourceMetadata,
    DEFAULT_SAFE_ERROR_MESSAGE, SAFE_ERROR_MESSAGE_MAX_CHARS,
};

pub fn dead_letter_to_avro_value(record: &DeadLetterRecord) -> AvroValue {
    AvroValue::Record(vec![
        (
            "event_id".to_string(),
            AvroValue::String(record.event_id().to_string()),
        ),
        (
            "source_topic".to_string(),
            AvroValue::String(record.source_topic().to_string()),
        ),
        (
            "source_partition".to_string(),
            AvroValue::Int(record.source_partition()),
        ),
        (
            "source_offset".to_string(),
            AvroValue::Long(record.source_offset()),
        ),
        (
            "source_key".to_string(),
            nullable_string(record.source_key()),
        ),
        ("schema_id".to_string(), nullable_int(record.schema_id())),
        (
            "event_type".to_string(),
            nullable_string(record.event_type()),
        ),
        ("trace_id".to_string(), nullable_string(record.trace_id())),
        (
            "failure_class".to_string(),
            AvroValue::String(record.failure_class().to_string()),
        ),
        (
            "safe_error_message".to_string(),
            AvroValue::String(record.safe_error_message().to_string()),
        ),
        (
            "occurred_at".to_string(),
            AvroValue::TimestampMillis(record.occurred_at_millis()),
        ),
    ])
}

pub fn dead_letter_from_avro_value(value: AvroValue) -> Result<DeadLetterRecord, String> {
    let AvroValue::Record(fields) = value else {
        return Err("dead-letter value must be an Avro record".to_string());
    };

    let source = DeadLetterSourceMetadata {
        event_id: required_string(&fields, "event_id")?,
        source_topic: required_string(&fields, "source_topic")?,
        source_partition: required_int(&fields, "source_partition")?,
        source_offset: required_long(&fields, "source_offset")?,
        source_key: nullable_string_field(&fields, "source_key")?,
        schema_id: nullable_int_field(&fields, "schema_id")?,
        event_type: nullable_string_field(&fields, "event_type")?,
        trace_id: nullable_string_field(&fields, "trace_id")?,
        occurred_at_millis: required_timestamp_millis(&fields, "occurred_at")?,
    };
    let failure_class = required_string(&fields, "failure_class")?;
    let safe_error_message = required_string(&fields, "safe_error_message")?;

    DeadLetterRecord::from_persisted_fields(source, failure_class, safe_error_message)
}

pub struct DeadLetterPublisher<P> {
    publisher: P,
}

impl<P> DeadLetterPublisher<P> {
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

impl<P> DeadLetterPublisher<P>
where
    P: EventPayloadPublisher,
{
    pub async fn publish_encoded(
        &self,
        key: &str,
        payload: Vec<u8>,
        record: DeadLetterRecord,
    ) -> Result<(), KafkaPublishError> {
        let occurred_at = DateTime::<Utc>::from_timestamp_millis(record.occurred_at_millis())
            .ok_or_else(|| KafkaPublishError::Publish {
                message: format!(
                    "invalid dead-letter occurred_at_millis: {}",
                    record.occurred_at_millis()
                ),
            })?;
        let envelope = EventEnvelope {
            event_id: record.event_id().to_string(),
            event_type: "intelligence.dead-letter".to_string(),
            source: "/intelligence-platform/dead-letter".to_string(),
            occurred_at,
            traceparent: None,
            tracestate: None,
        };

        self.publisher
            .publish(
                DEAD_LETTER_TOPIC,
                key,
                &payload,
                &envelope.cloud_event_headers(),
            )
            .await
    }
}

fn required_string(fields: &[(String, AvroValue)], name: &str) -> Result<String, String> {
    match avro_field(fields, name)? {
        AvroValue::String(value) => Ok(value.clone()),
        _ => Err(format!("{name} must be a string")),
    }
}

fn required_int(fields: &[(String, AvroValue)], name: &str) -> Result<i32, String> {
    match avro_field(fields, name)? {
        AvroValue::Int(value) => Ok(*value),
        _ => Err(format!("{name} must be an int")),
    }
}

fn required_long(fields: &[(String, AvroValue)], name: &str) -> Result<i64, String> {
    match avro_field(fields, name)? {
        AvroValue::Long(value) => Ok(*value),
        _ => Err(format!("{name} must be a long")),
    }
}

fn required_timestamp_millis(fields: &[(String, AvroValue)], name: &str) -> Result<i64, String> {
    match avro_field(fields, name)? {
        AvroValue::TimestampMillis(value) | AvroValue::Long(value) => Ok(*value),
        _ => Err(format!("{name} must be timestamp-millis")),
    }
}

fn nullable_string_field(
    fields: &[(String, AvroValue)],
    name: &str,
) -> Result<Option<String>, String> {
    match avro_field(fields, name)? {
        AvroValue::Union(0, value) if matches!(**value, AvroValue::Null) => Ok(None),
        AvroValue::Union(1, value) => match &**value {
            AvroValue::String(value) => Ok(Some(value.clone())),
            _ => Err(format!("{name} must be null or string")),
        },
        AvroValue::Null => Ok(None),
        AvroValue::String(value) => Ok(Some(value.clone())),
        _ => Err(format!("{name} must be null or string")),
    }
}

fn nullable_int_field(fields: &[(String, AvroValue)], name: &str) -> Result<Option<i32>, String> {
    match avro_field(fields, name)? {
        AvroValue::Union(0, value) if matches!(**value, AvroValue::Null) => Ok(None),
        AvroValue::Union(1, value) => match &**value {
            AvroValue::Int(value) => Ok(Some(*value)),
            _ => Err(format!("{name} must be null or int")),
        },
        AvroValue::Null => Ok(None),
        AvroValue::Int(value) => Ok(Some(*value)),
        _ => Err(format!("{name} must be null or int")),
    }
}

fn avro_field<'a>(fields: &'a [(String, AvroValue)], name: &str) -> Result<&'a AvroValue, String> {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value)
        .ok_or_else(|| format!("field {name} is missing"))
}

fn nullable_string(value: Option<&str>) -> AvroValue {
    match value {
        Some(value) => AvroValue::Union(1, Box::new(AvroValue::String(value.to_string()))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

fn nullable_int(value: Option<i32>) -> AvroValue {
    match value {
        Some(value) => AvroValue::Union(1, Box::new(AvroValue::Int(value))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}
