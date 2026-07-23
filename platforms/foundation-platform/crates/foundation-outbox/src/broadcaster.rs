use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::{errors::PublishError, worker::OutboxScope};

#[derive(Clone, Debug)]
/// Event data loaded from a foundation-platform outbox table.
pub struct EventEnvelope {
    /// Stable event identifier from the outbox row.
    pub event_id: Uuid,
    /// Fully qualified event type, for example `catalog.vector_tile_manifest.promoted.v1`.
    pub event_type: String,
    /// Event payload as stored by the domain transaction.
    pub payload: Value,
    /// Domain event occurrence timestamp.
    pub occurred_at: DateTime<Utc>,
    /// Outbox scope that determines the source table and bounded context.
    pub scope: OutboxScope,
}

#[async_trait]
/// Publishes a single outbox event to an external or local sink.
pub trait EventBroadcaster: Send + Sync {
    /// 이벤트 발행에 실패하면 발행 오류를 반환해요.
    ///
    /// # Errors
    ///
    /// 브로드캐스터 구현체가 대상 시스템으로 이벤트를 전달하지 못하면 오류를 반환해요.
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError>;
}

#[derive(Clone, Debug, Default)]
/// Broadcaster implementation that records publication through structured logs only.
pub struct LoggingBroadcaster;

#[async_trait]
impl EventBroadcaster for LoggingBroadcaster {
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        tracing::info!(
            event_id = %event.event_id,
            event_type = %event.event_type,
            scope = ?event.scope,
            occurred_at = %event.occurred_at,
            "outbox event published"
        );
        Ok(())
    }
}
