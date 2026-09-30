//! The review queue as the lakehouse hands it to the steward database (root ADR-0115 §9).
//!
//! `lineage_review_queue.handoff_document` writes it; this module decides whether a file is a whole,
//! well-formed queue before anything is loaded. The database is a projection: a load replaces the
//! unit's items, so a file that is partial, reordered in time, or inconsistent must be refused here.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::{evidence_etag, Candidate, ReviewStatus, StewardshipError};

/// The only schema this loader reads.
pub const HANDOFF_SCHEMA_VERSION: &str = "foundation-platform.lineage_review_handoff.v1";

/// One item of the queue as handed off.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffItem {
    /// Stable item id.
    pub item_id: Uuid,
    /// The parcel.
    pub subject_code: String,
    /// Why it is on the queue.
    pub status: ReviewStatus,
    /// Candidates exactly as the queue wrote them.
    pub candidates_json: String,
    /// The fingerprint the queue computed.
    pub evidence_etag: String,
    /// Earlier cadastral snapshot of the latest lineage row.
    pub from_snapshot_id: Option<String>,
    /// Later cadastral snapshot of the latest lineage row.
    pub to_snapshot_id: Option<String>,
}

/// A whole queue for one unit.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewHandoff {
    /// Must be [`HANDOFF_SCHEMA_VERSION`].
    pub schema_version: String,
    /// Only `parcel` today.
    pub unit: String,
    /// When the queue job published it.
    pub published_at_utc: DateTime<Utc>,
    /// How many items the writer wrote.
    pub item_count: usize,
    /// The items.
    pub items: Vec<HandoffItem>,
}

/// Parses and checks a handoff file.
///
/// # Errors
/// Returns [`StewardshipError::InvalidInput`] naming the first thing wrong with it.
pub fn parse_handoff(text: &str) -> Result<ReviewHandoff, StewardshipError> {
    let invalid = |message: String| StewardshipError::InvalidInput(message);
    let handoff: ReviewHandoff = serde_json::from_str(text)
        .map_err(|error| invalid(format!("handoff does not parse: {error}")))?;
    if handoff.schema_version != HANDOFF_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unknown handoff schema {}",
            handoff.schema_version
        )));
    }
    if handoff.unit != "parcel" {
        return Err(invalid(format!("unknown unit {}", handoff.unit)));
    }
    if handoff.item_count != handoff.items.len() {
        return Err(invalid(format!(
            "handoff says {} items but holds {}",
            handoff.item_count,
            handoff.items.len()
        )));
    }
    let mut ids = HashSet::new();
    let mut subjects = HashSet::new();
    for item in &handoff.items {
        let code = &item.subject_code;
        if code.len() != 19 || !code.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid(format!("{code} is not a 19-digit PNU")));
        }
        if !ids.insert(item.item_id) || !subjects.insert(code.as_str()) {
            return Err(invalid(format!("{code} appears twice")));
        }
        serde_json::from_str::<Vec<Candidate>>(&item.candidates_json)
            .map_err(|error| invalid(format!("{code}: candidates do not parse: {error}")))?;
        if item.evidence_etag != evidence_etag(item.status, &item.candidates_json) {
            return Err(invalid(format!(
                "{code}: evidence_etag does not match its candidates"
            )));
        }
    }
    Ok(handoff)
}
