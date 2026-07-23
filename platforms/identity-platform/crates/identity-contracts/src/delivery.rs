//! Published Identity event delivery envelopes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::events::IdentityEventV1;

/// Exact v1 HTTP envelope used to deliver one Identity outbox event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IdentityEventDeliveryV1 {
    /// Stable outbox event identifier and receiver idempotency key.
    pub event_id: Uuid,
    /// Published Identity event type.
    pub event_type: String,
    /// UTC domain occurrence timestamp.
    pub occurred_at: DateTime<Utc>,
    /// Versioned Identity event payload.
    pub payload: IdentityEventV1,
}
