use std::collections::BTreeMap;

use apache_avro::types::Value as AvroValue;
use knowledge_domain::KnowledgeSourceUpserted;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoundationKnowledgeSourceUpsertedEvent {
    pub event_id: String,
    pub tenant_id: String,
    pub product_id: String,
    pub source_id: String,
    pub source_kind: String,
    pub source_uri: String,
    pub content_uri: Option<String>,
    pub content_checksum_sha256: Option<String>,
    pub occurred_at_millis: i64,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum FoundationKnowledgeEventError {
    #[error("{message}")]
    InvalidPayload { message: String },
}

impl FoundationKnowledgeEventError {
    pub fn safe_message(&self) -> &'static str {
        "foundation knowledge event payload is invalid"
    }
}

pub fn foundation_knowledge_source_upserted_fixture_schema_str() -> &'static str {
    r#"
{
  "type": "record",
  "namespace": "kr.perfectory.foundation.events.fixture",
  "name": "FoundationKnowledgeSourceUpsertedV1",
  "doc": "Fixture-only contract for intelligence-platform C2-B development. Production enablement requires Foundation Platform approval of topic and schema.",
  "fields": [
    { "name": "event_id", "type": "string" },
    { "name": "tenant_id", "type": "string" },
    { "name": "product_id", "type": "string" },
    { "name": "source_id", "type": "string" },
    { "name": "source_kind", "type": "string" },
    { "name": "source_uri", "type": "string" },
    { "name": "content_uri", "type": ["null", "string"], "default": null },
    { "name": "content_checksum_sha256", "type": ["null", "string"], "default": null },
    {
      "name": "occurred_at",
      "type": { "type": "long", "logicalType": "timestamp-millis" }
    },
    {
      "name": "metadata",
      "type": { "type": "map", "values": "string" },
      "default": {}
    }
  ]
}
"#
}

impl FoundationKnowledgeSourceUpsertedEvent {
    pub fn to_avro_value(&self) -> AvroValue {
        AvroValue::Record(vec![
            (
                "event_id".to_string(),
                AvroValue::String(self.event_id.clone()),
            ),
            (
                "tenant_id".to_string(),
                AvroValue::String(self.tenant_id.clone()),
            ),
            (
                "product_id".to_string(),
                AvroValue::String(self.product_id.clone()),
            ),
            (
                "source_id".to_string(),
                AvroValue::String(self.source_id.clone()),
            ),
            (
                "source_kind".to_string(),
                AvroValue::String(self.source_kind.clone()),
            ),
            (
                "source_uri".to_string(),
                AvroValue::String(self.source_uri.clone()),
            ),
            (
                "content_uri".to_string(),
                nullable_string(&self.content_uri),
            ),
            (
                "content_checksum_sha256".to_string(),
                nullable_string(&self.content_checksum_sha256),
            ),
            (
                "occurred_at".to_string(),
                AvroValue::TimestampMillis(self.occurred_at_millis),
            ),
            (
                "metadata".to_string(),
                AvroValue::Map(
                    self.metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), AvroValue::String(value.clone())))
                        .collect(),
                ),
            ),
        ])
    }

    pub fn from_avro_value(value: AvroValue) -> Result<Self, FoundationKnowledgeEventError> {
        let AvroValue::Record(fields) = value else {
            return Err(invalid("expected foundation knowledge upsert record"));
        };

        Ok(Self {
            event_id: required_string(&fields, "event_id")?,
            tenant_id: required_string(&fields, "tenant_id")?,
            product_id: required_string(&fields, "product_id")?,
            source_id: required_string(&fields, "source_id")?,
            source_kind: required_string(&fields, "source_kind")?,
            source_uri: required_string(&fields, "source_uri")?,
            content_uri: nullable_string_field(&fields, "content_uri")?,
            content_checksum_sha256: nullable_string_field(&fields, "content_checksum_sha256")?,
            occurred_at_millis: required_timestamp_millis(&fields, "occurred_at")?,
            metadata: string_map(&fields, "metadata")?,
        })
    }
}

impl From<FoundationKnowledgeSourceUpsertedEvent> for KnowledgeSourceUpserted {
    fn from(value: FoundationKnowledgeSourceUpsertedEvent) -> Self {
        Self {
            event_id: value.event_id,
            tenant_id: value.tenant_id,
            product_id: value.product_id,
            source_id: value.source_id,
            source_kind: value.source_kind,
            source_uri: value.source_uri,
            content_uri: value.content_uri,
            content_checksum_sha256: value.content_checksum_sha256,
            occurred_at_millis: value.occurred_at_millis,
            metadata: value.metadata,
        }
    }
}

fn required_string(
    fields: &[(String, AvroValue)],
    name: &str,
) -> Result<String, FoundationKnowledgeEventError> {
    match field(fields, name)? {
        AvroValue::String(value) if !value.trim().is_empty() => Ok(value.clone()),
        _ => Err(invalid(format!("{name} must be a non-empty string"))),
    }
}

fn nullable_string_field(
    fields: &[(String, AvroValue)],
    name: &str,
) -> Result<Option<String>, FoundationKnowledgeEventError> {
    match field(fields, name)? {
        AvroValue::Union(0, value) if matches!(**value, AvroValue::Null) => Ok(None),
        AvroValue::Union(1, value) => match &**value {
            AvroValue::String(value) => Ok(Some(value.clone())),
            _ => Err(invalid(format!("{name} must be null or string"))),
        },
        AvroValue::Null => Ok(None),
        AvroValue::String(value) => Ok(Some(value.clone())),
        _ => Err(invalid(format!("{name} must be null or string"))),
    }
}

fn required_timestamp_millis(
    fields: &[(String, AvroValue)],
    name: &str,
) -> Result<i64, FoundationKnowledgeEventError> {
    match field(fields, name)? {
        AvroValue::TimestampMillis(value) | AvroValue::Long(value) if *value > 0 => Ok(*value),
        _ => Err(invalid(format!(
            "{name} must be a positive timestamp-millis"
        ))),
    }
}

fn string_map(
    fields: &[(String, AvroValue)],
    name: &str,
) -> Result<BTreeMap<String, String>, FoundationKnowledgeEventError> {
    match field(fields, name)? {
        AvroValue::Map(values) => values
            .iter()
            .map(|(key, value)| match value {
                AvroValue::String(value) => Ok((key.clone(), value.clone())),
                _ => Err(invalid(format!("{name} values must be strings"))),
            })
            .collect(),
        _ => Err(invalid(format!("{name} must be a string map"))),
    }
}

fn field<'a>(
    fields: &'a [(String, AvroValue)],
    name: &str,
) -> Result<&'a AvroValue, FoundationKnowledgeEventError> {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value)
        .ok_or_else(|| invalid(format!("missing field {name}")))
}

fn nullable_string(value: &Option<String>) -> AvroValue {
    match value {
        Some(value) => AvroValue::Union(1, Box::new(AvroValue::String(value.clone()))),
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
    }
}

fn invalid(message: impl Into<String>) -> FoundationKnowledgeEventError {
    FoundationKnowledgeEventError::InvalidPayload {
        message: message.into(),
    }
}
