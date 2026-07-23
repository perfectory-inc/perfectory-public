use chrono::{DateTime, Utc};

pub const NORMALIZATION_SUBMISSION_REQUESTED_TOPIC: &str =
    "intelligence.normalization-proposal.submission-requested.v1";
pub const DEAD_LETTER_TOPIC: &str = "intelligence.dead-letter.v1";
pub const FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_FIXTURE_TOPIC: &str =
    "intelligence-platform.fixture.foundation-knowledge-source.upserted.v1";

pub fn schema_subject_for_topic(topic: &str) -> String {
    format!("{topic}-value")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventHeader {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_type: String,
    pub source: String,
    pub occurred_at: DateTime<Utc>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl EventEnvelope {
    pub fn cloud_event_headers(&self) -> Vec<EventHeader> {
        let mut headers = vec![
            EventHeader {
                key: "ce_id".to_string(),
                value: self.event_id.clone(),
            },
            EventHeader {
                key: "ce_type".to_string(),
                value: self.event_type.clone(),
            },
            EventHeader {
                key: "ce_source".to_string(),
                value: self.source.clone(),
            },
            EventHeader {
                key: "ce_time".to_string(),
                value: self.occurred_at.to_rfc3339(),
            },
            EventHeader {
                key: "ce_specversion".to_string(),
                value: "1.0".to_string(),
            },
        ];

        if let Some(traceparent) = self.traceparent.as_ref().filter(|value| !value.is_empty()) {
            headers.push(EventHeader {
                key: "traceparent".to_string(),
                value: traceparent.clone(),
            });
        }
        if let Some(tracestate) = self.tracestate.as_ref().filter(|value| !value.is_empty()) {
            headers.push(EventHeader {
                key: "tracestate".to_string(),
                value: tracestate.clone(),
            });
        }

        headers
    }
}
