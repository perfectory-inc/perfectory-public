pub const SAFE_ERROR_MESSAGE_MAX_CHARS: usize = 512;
pub const DEFAULT_SAFE_ERROR_MESSAGE: &str = "error details redacted";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterRecord {
    event_id: String,
    source_topic: String,
    source_partition: i32,
    source_offset: i64,
    source_key: Option<String>,
    schema_id: Option<i32>,
    event_type: Option<String>,
    trace_id: Option<String>,
    failure_class: String,
    safe_error_message: String,
    occurred_at_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterSourceMetadata {
    pub event_id: String,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub source_key: Option<String>,
    pub schema_id: Option<i32>,
    pub event_type: Option<String>,
    pub trace_id: Option<String>,
    pub occurred_at_millis: i64,
}

impl DeadLetterRecord {
    pub fn from_safe_metadata(
        source: DeadLetterSourceMetadata,
        failure_class: impl Into<String>,
        safe_error_message: impl AsRef<[u8]>,
    ) -> Self {
        Self::from_canonical_safe_fields(
            source,
            failure_class,
            sanitize_safe_error_message(safe_error_message),
        )
    }

    pub fn from_persisted_fields(
        source: DeadLetterSourceMetadata,
        failure_class: impl Into<String>,
        safe_error_message: impl Into<String>,
    ) -> Result<Self, String> {
        let safe_error_message = safe_error_message.into();
        if safe_error_message != sanitize_safe_error_message(safe_error_message.as_bytes()) {
            return Err("dead-letter safe_error_message must be canonical and safe".to_string());
        }

        Ok(Self::from_canonical_safe_fields(
            source,
            failure_class,
            safe_error_message,
        ))
    }

    fn from_canonical_safe_fields(
        source: DeadLetterSourceMetadata,
        failure_class: impl Into<String>,
        safe_error_message: String,
    ) -> Self {
        Self {
            event_id: source.event_id,
            source_topic: source.source_topic,
            source_partition: source.source_partition,
            source_offset: source.source_offset,
            source_key: source.source_key,
            schema_id: source.schema_id,
            event_type: source.event_type,
            trace_id: source.trace_id,
            failure_class: failure_class.into(),
            safe_error_message,
            occurred_at_millis: source.occurred_at_millis,
        }
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn source_topic(&self) -> &str {
        &self.source_topic
    }

    pub fn source_partition(&self) -> i32 {
        self.source_partition
    }

    pub fn source_offset(&self) -> i64 {
        self.source_offset
    }

    pub fn source_key(&self) -> Option<&str> {
        self.source_key.as_deref()
    }

    pub fn schema_id(&self) -> Option<i32> {
        self.schema_id
    }

    pub fn event_type(&self) -> Option<&str> {
        self.event_type.as_deref()
    }

    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    pub fn failure_class(&self) -> &str {
        &self.failure_class
    }

    pub fn safe_error_message(&self) -> &str {
        &self.safe_error_message
    }

    pub fn occurred_at_millis(&self) -> i64 {
        self.occurred_at_millis
    }
}

pub fn sanitize_safe_error_message(message: impl AsRef<[u8]>) -> String {
    let normalized = String::from_utf8_lossy(message.as_ref())
        .chars()
        .map(|ch| {
            if ch == '\u{FFFD}' {
                '?'
            } else if ch.is_control() {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let sanitized = if normalized.is_empty() {
        DEFAULT_SAFE_ERROR_MESSAGE.to_string()
    } else {
        normalized
    };

    sanitized
        .chars()
        .take(SAFE_ERROR_MESSAGE_MAX_CHARS)
        .collect()
}
