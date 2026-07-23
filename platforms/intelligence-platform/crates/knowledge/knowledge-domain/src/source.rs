use std::collections::BTreeMap;

use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KnowledgeSourceUpserted {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KnowledgeSourceRecord {
    pub tenant_id: String,
    pub product_id: String,
    pub source_id: String,
    pub source_kind: String,
    pub source_uri: String,
    pub content_uri: Option<String>,
    pub content_checksum_sha256: Option<String>,
    pub last_event_id: String,
    pub last_seen_at_millis: i64,
    pub metadata: BTreeMap<String, String>,
    pub version: u64,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum KnowledgeSourceValidationError {
    #[error("{message}")]
    InvalidEvent { message: String },
}

pub fn validate_knowledge_source_event(
    event: &KnowledgeSourceUpserted,
) -> Result<(), KnowledgeSourceValidationError> {
    require_non_empty("event_id", &event.event_id)?;
    require_non_empty("tenant_id", &event.tenant_id)?;
    require_non_empty("product_id", &event.product_id)?;
    require_non_empty("source_id", &event.source_id)?;
    require_non_empty("source_kind", &event.source_kind)?;
    validate_uri("source_uri", &event.source_uri)?;
    if let Some(content_uri) = &event.content_uri {
        validate_uri("content_uri", content_uri)?;
    }
    if let Some(checksum) = &event.content_checksum_sha256 {
        validate_sha256("content_checksum_sha256", checksum)?;
    }
    if event.occurred_at_millis <= 0 {
        return Err(invalid_event("occurred_at_millis must be positive"));
    }
    Ok(())
}

fn require_non_empty(field: &str, value: &str) -> Result<(), KnowledgeSourceValidationError> {
    if value.trim().is_empty() {
        Err(invalid_event(format!("{field} must be non-empty")))
    } else {
        Ok(())
    }
}

fn validate_uri(field: &str, value: &str) -> Result<(), KnowledgeSourceValidationError> {
    let value = value.trim();
    if value.starts_with("s3://") || value.starts_with("http://") || value.starts_with("https://") {
        Ok(())
    } else {
        Err(invalid_event(format!(
            "{field} must use s3, http, or https scheme"
        )))
    }
}

fn validate_sha256(field: &str, value: &str) -> Result<(), KnowledgeSourceValidationError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(invalid_event(format!("{field} must be hex sha256")))
    }
}

fn invalid_event(message: impl Into<String>) -> KnowledgeSourceValidationError {
    KnowledgeSourceValidationError::InvalidEvent {
        message: message.into(),
    }
}
