//! Production [`RawWrittenSink`] that records `collection.raw_written` into the Postgres outbox.
//!
//! This is the producer-seam implementation from gongzzang ADR-0047 / foundation-platform ADR-0013
//! (Slice 3-B): a collection worker emits its typed [`CollectionRawWrittenV1`] on success and this
//! sink inserts it into `catalog.outbox_event`. The **existing** [`OutboxWorker`] then polls that
//! table and fans the event out through an [`EventBroadcaster`] — exactly like every other Catalog
//! event today. The sink therefore introduces no new fan-out path; it only feeds the existing one.
//!
//! Scope (Slice 3-B): operational `raw_written` outbox publishing. It does NOT convert the
//! collection executor to a Postgres job ledger — that (`PostgresJobBus`) and DB-backed quarantine
//! remain Option B. The success/quarantine state of collection itself stays on the JSONL ledger.
//!
//! [`RawWrittenSink`]: crate::jobbus::RawWrittenSink
//! [`OutboxWorker`]: crate::worker::OutboxWorker
//! [`EventBroadcaster`]: crate::broadcaster::EventBroadcaster

use async_trait::async_trait;
use foundation_shared_kernel::events::catalog_v1::{CatalogEvent, CollectionRawWrittenV1};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::jobbus::{JobBusError, RawWrittenSink};

/// A [`RawWrittenSink`] that inserts `collection.raw_written` into `catalog.outbox_event`.
#[derive(Clone)]
pub struct OutboxRawWrittenSink {
    pool: PgPool,
}

impl OutboxRawWrittenSink {
    /// Create a sink that writes to the Catalog outbox via `pool`.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Encode a `raw_written` event into the `(type, payload)` shape of a `catalog.outbox_event` row.
///
/// Mirrors the Catalog unit-of-work outbox writer: the payload is the full externally-tagged
/// `CatalogEvent` JSON and the `type` column is its `type` tag, so the existing `OutboxWorker`
/// hydrates an `EventEnvelope` from the row unchanged.
///
/// # Errors
/// Returns [`JobBusError::Backend`] if the event cannot be serialized or carries no `type` tag.
fn encode_outbox_row(event: &CollectionRawWrittenV1) -> Result<(String, Value), JobBusError> {
    let catalog_event = CatalogEvent::CollectionRawWritten(event.clone());
    let payload = serde_json::to_value(&catalog_event)
        .map_err(|error| JobBusError::Backend(format!("raw_written encode: {error}")))?;
    let type_tag = payload
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            JobBusError::Backend(
                "raw_written serialization missing 'type' tag — serde derive misconfigured"
                    .to_owned(),
            )
        })?
        .to_owned();
    Ok((type_tag, payload))
}

#[async_trait]
impl RawWrittenSink for OutboxRawWrittenSink {
    async fn emit(&self, event: &CollectionRawWrittenV1) -> Result<(), JobBusError> {
        let (type_tag, payload) = encode_outbox_row(event)?;
        sqlx::query(
            "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at)
             VALUES ($1, $2, $3, now())",
        )
        // v4 is sufficient here: event_id only needs PK uniqueness, and the OutboxWorker orders
        // pending rows by occurred_at (not event_id), so time-ordered ids are not required.
        .bind(Uuid::new_v4())
        .bind(&type_tag)
        .bind(&payload)
        .execute(&self.pool)
        .await
        .map_err(|error| JobBusError::Backend(format!("raw_written outbox insert: {error}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::encode_outbox_row;
    use chrono::{DateTime, Utc};
    use foundation_shared_kernel::events::catalog_v1::CollectionRawWrittenV1;

    fn fixed_time() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap_or_default()
    }

    fn raw_written() -> CollectionRawWrittenV1 {
        CollectionRawWrittenV1 {
            schema_version: 1,
            collection_snapshot_id: "registry:test".to_owned(),
            job_id: "job-b".to_owned(),
            scope_unit_id: "scope:legal-dong:1111010100".to_owned(),
            provider: "data.go.kr".to_owned(),
            endpoint: "getBrTitleInfo".to_owned(),
            endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".to_owned(),
            bronze_object_key: "bronze/source=x/page=0001/part-0001.json".to_owned(),
            bronze_object_count: 1,
            bronze_checksum_sha256: "b".repeat(64),
            bronze_size_bytes: 4_096,
            source_record_count: 42,
            request_count: 1,
            request_fingerprint_sha256: "a".repeat(64),
            request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
                .to_owned(),
            license: None,
            srid: None,
            reused_bronze_object: false,
            fetched_at_utc: fixed_time(),
            occurred_at: fixed_time(),
        }
    }

    #[test]
    fn encode_outbox_row_uses_wire_type_tag_and_carries_payload(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (type_tag, payload) = encode_outbox_row(&raw_written())?;

        // The `type` column is the externally-tagged wire name the OutboxWorker routes on.
        assert_eq!(type_tag, "catalog.collection.raw_written.v1");
        // The payload is the full CatalogEvent JSON (tag + fields), so a worker can re-hydrate it.
        assert_eq!(
            payload.get("type").and_then(|v| v.as_str()),
            Some("catalog.collection.raw_written.v1")
        );
        assert_eq!(
            payload
                .get("bronze_checksum_sha256")
                .and_then(|v| v.as_str()),
            Some("b".repeat(64).as_str())
        );
        assert_eq!(
            payload.get("job_id").and_then(|v| v.as_str()),
            Some("job-b")
        );
        Ok(())
    }
}
