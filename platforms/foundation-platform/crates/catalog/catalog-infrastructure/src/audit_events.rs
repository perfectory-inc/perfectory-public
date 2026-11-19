//! The Postgres audit record, `audit.event` (root ADR-0569 §2, §3; ADR-0584).
//!
//! Reads are plain `SELECT`s the API role is granted; the one write is `audit.append`, which the
//! database answers for: it refuses a source this role may not write. A write takes no lock, so
//! nothing here waits for one (root ADR-0584 §1).

use async_trait::async_trait;
use catalog_application::ports::{
    AuditAppend, AuditDigest, AuditEvent, AuditEventFilter, AuditEventStore, AuditExport,
    AuditImportStatus, AuditStoreError,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

/// The most rows one read answers.
pub const AUDIT_PAGE_MAX: u32 = 500;

const EVENT_COLUMNS: &str = "seq, event_id, occurred_at, recorded_at, source, source_event_id,
    action, operation, actor_kind, actor_staff_id, actor_subject, actor_external, actor_verified,
    on_behalf_of, object_type, object_id, before, after, reason, applied_via_kind,
    applied_via_ref, result, error, change_id, txid::text AS txid";

const DIGEST_COLUMNS: &str = "d.digest_no, d.from_snapshot::text AS from_snapshot,
    d.to_snapshot::text AS to_snapshot, d.row_count, encode(d.rows_sha256, 'hex') AS rows_sha256,
    encode(d.previous_digest_sha256, 'hex') AS previous_digest_sha256,
    encode(d.digest_sha256, 'hex') AS digest_sha256, w.rekor_log_index, d.made_at
    FROM audit.digest d LEFT JOIN audit.digest_witness w USING (digest_no)";

/// Appends an event in `tx` through `audit.append`: the call the ledger trigger's sibling rows and
/// the API's own events share.
///
/// # Errors
///
/// Returns the database's refusal.
pub async fn append_in(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: &AuditAppend,
) -> Result<Option<i64>, sqlx::Error> {
    let (via_kind, via_ref) = event
        .applied_via
        .as_ref()
        .map_or((None, None), |(kind, id)| {
            (Some(kind.as_str()), Some(id.as_str()))
        });
    sqlx::query_scalar(
        "SELECT audit.append($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, $9, NULL, $10, $11,
                             NULL, $12, $13, $14, $15, $16, $17, $18)",
    )
    .bind(event.event_id)
    .bind(event.occurred_at)
    .bind(&event.source)
    .bind(&event.source_event_id)
    .bind(&event.action)
    .bind(&event.operation)
    .bind(&event.actor_kind)
    .bind(event.actor_staff_id)
    .bind(event.actor_staff_id.is_some())
    .bind(&event.object_type)
    .bind(&event.object_id)
    .bind(&event.after)
    .bind(&event.reason)
    .bind(via_kind)
    .bind(via_ref)
    .bind(&event.result)
    .bind(&event.error)
    .bind(event.change_id)
    .fetch_one(&mut **tx)
    .await
}

/// Reads and appends the audit record.
pub struct PgAuditEventStore {
    pool: PgPool,
}

impl PgAuditEventStore {
    /// Creates the store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn unavailable(error: &sqlx::Error) -> AuditStoreError {
    AuditStoreError::Unavailable(error.to_string())
}

/// `LIKE` treats `%`, `_` and `\` as patterns; a prefix filter means the characters themselves.
fn like_prefix(prefix: &str) -> String {
    let mut escaped = String::with_capacity(prefix.len() + 1);
    for character in prefix.chars() {
        if matches!(character, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('%');
    escaped
}

/// `SELECT <events> FROM audit.event WHERE <every filter given>`.
fn select_events(filter: &AuditEventFilter) -> QueryBuilder<'static, Postgres> {
    let mut query = QueryBuilder::<Postgres>::new(format!(
        "SELECT {EVENT_COLUMNS} FROM audit.event WHERE true"
    ));
    if let Some(staff) = filter.actor_staff_id {
        query.push(" AND actor_staff_id = ").push_bind(staff);
    }
    if let Some(external) = &filter.actor_external {
        query
            .push(" AND actor_external = ")
            .push_bind(external.clone());
    }
    if let Some(object_type) = &filter.object_type {
        query
            .push(" AND object_type = ")
            .push_bind(object_type.clone());
    }
    if let Some(prefix) = &filter.object_id_prefix {
        query
            .push(" AND object_id LIKE ")
            .push_bind(like_prefix(prefix))
            .push(" ESCAPE '\\'");
    }
    if let Some(source) = &filter.source {
        query.push(" AND source = ").push_bind(source.clone());
    }
    if let Some(action) = &filter.action {
        query.push(" AND action = ").push_bind(action.clone());
    }
    if let Some(result) = &filter.result {
        query.push(" AND result = ").push_bind(result.clone());
    }
    if let Some(change) = filter.change_id {
        query.push(" AND change_id = ").push_bind(change);
    }
    if let Some(from) = filter.occurred_from {
        query.push(" AND occurred_at >= ").push_bind(from);
    }
    if let Some(to) = filter.occurred_to {
        query.push(" AND occurred_at < ").push_bind(to);
    }
    if let Some(before) = filter.before_seq {
        query.push(" AND seq < ").push_bind(before);
    }
    if let Some(after) = &filter.after_snapshot {
        // The rows the cursor's snapshot did not see: the same rule as a digest's range, so a row
        // committed after a later one is still found (root ADR-0584 §3).
        query
            .push(" AND NOT pg_visible_in_snapshot(txid, ")
            .push_bind(after.clone())
            .push("::pg_snapshot)");
    }
    query
}

fn event_from(row: &PgRow) -> Result<AuditEvent, sqlx::Error> {
    Ok(AuditEvent {
        seq: row.try_get("seq")?,
        event_id: row.try_get("event_id")?,
        occurred_at: row.try_get("occurred_at")?,
        recorded_at: row.try_get("recorded_at")?,
        source: row.try_get("source")?,
        source_event_id: row.try_get("source_event_id")?,
        action: row.try_get("action")?,
        operation: row.try_get("operation")?,
        actor_kind: row.try_get("actor_kind")?,
        actor_staff_id: row.try_get("actor_staff_id")?,
        actor_subject: row.try_get("actor_subject")?,
        actor_external: row.try_get("actor_external")?,
        actor_verified: row.try_get("actor_verified")?,
        on_behalf_of: row.try_get("on_behalf_of")?,
        object_type: row.try_get("object_type")?,
        object_id: row.try_get("object_id")?,
        before: row.try_get("before")?,
        after: row.try_get("after")?,
        reason: row.try_get("reason")?,
        applied_via_kind: row.try_get("applied_via_kind")?,
        applied_via_ref: row.try_get("applied_via_ref")?,
        result: row.try_get("result")?,
        error: row.try_get("error")?,
        change_id: row.try_get("change_id")?,
        txid: row.try_get("txid")?,
    })
}

fn digest_from(row: &PgRow) -> Result<AuditDigest, sqlx::Error> {
    Ok(AuditDigest {
        digest_no: row.try_get("digest_no")?,
        from_snapshot: row.try_get("from_snapshot")?,
        to_snapshot: row.try_get("to_snapshot")?,
        row_count: row.try_get("row_count")?,
        rows_sha256: row.try_get("rows_sha256")?,
        previous_digest_sha256: row.try_get("previous_digest_sha256")?,
        digest_sha256: row.try_get("digest_sha256")?,
        rekor_log_index: row.try_get("rekor_log_index")?,
        made_at: row.try_get("made_at")?,
    })
}

/// The export, read in `tx` (one snapshot).
async fn export_in(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    filter: &AuditEventFilter,
    max: u32,
) -> Result<AuditExport, sqlx::Error> {
    let mut query = select_events(filter);
    query
        .push(" ORDER BY seq LIMIT ")
        .push_bind(i64::from(max) + 1);
    let events = query
        .build()
        .fetch_all(&mut **tx)
        .await?
        .iter()
        .map(event_from)
        .collect::<Result<Vec<_>, _>>()?;
    let cursor: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(&mut **tx)
        .await?;
    let txids: Vec<&str> = events.iter().map(|event| event.txid.as_str()).collect();
    // Every digest whose range can hold one of these transactions: it must see the transaction
    // (`txid < xmax(to)`) and the digest before must not have (`txid >= xmin(from)`). Bounded by the
    // rows' own span, never the whole table of digests.
    let digests = sqlx::query(&format!(
        "WITH t AS (SELECT min(x) AS lo, max(x) AS hi FROM unnest($1::text[]::xid8[]) x)
         SELECT {DIGEST_COLUMNS}, t
         WHERE t.lo IS NOT NULL
           AND t.lo < pg_snapshot_xmax(d.to_snapshot)
           AND (d.from_snapshot IS NULL OR pg_snapshot_xmin(d.from_snapshot) <= t.hi)
         ORDER BY d.digest_no"
    ))
    .bind(&txids)
    .fetch_all(&mut **tx)
    .await?
    .iter()
    .map(digest_from)
    .collect::<Result<Vec<_>, _>>()?;
    let uncovered: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM unnest($1::text[]::xid8[]) x
         WHERE NOT coalesce(pg_visible_in_snapshot(x,
             (SELECT to_snapshot FROM audit.digest ORDER BY digest_no DESC LIMIT 1)), false)",
    )
    .bind(&txids)
    .fetch_one(&mut **tx)
    .await?;
    Ok(AuditExport {
        events,
        cursor,
        digests,
        uncovered,
    })
}

#[async_trait]
impl AuditEventStore for PgAuditEventStore {
    async fn page(&self, filter: &AuditEventFilter) -> Result<Vec<AuditEvent>, AuditStoreError> {
        let mut query = select_events(filter);
        query
            .push(" ORDER BY seq DESC LIMIT ")
            .push_bind(i64::from(filter.limit.clamp(1, AUDIT_PAGE_MAX)));
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|error| unavailable(&error))?;
        rows.iter()
            .map(event_from)
            .collect::<Result<_, sqlx::Error>>()
            .map_err(|error| unavailable(&error))
    }

    async fn append(&self, event: &AuditAppend) -> Result<Option<i64>, AuditStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| unavailable(&error))?;
        let seq = append_in(&mut tx, event)
            .await
            .map_err(|error| unavailable(&error))?;
        tx.commit().await.map_err(|error| unavailable(&error))?;
        Ok(seq)
    }

    async fn export(
        &self,
        filter: &AuditEventFilter,
        max: u32,
    ) -> Result<AuditExport, AuditStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| unavailable(&error))?;
        // One snapshot for the rows, the cursor and the digests, so the cursor is exactly what the
        // rows were read in.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|error| unavailable(&error))?;
        let export = export_in(&mut tx, filter, max)
            .await
            .map_err(|error| unavailable(&error))?;
        tx.commit().await.map_err(|error| unavailable(&error))?;
        Ok(export)
    }

    async fn import_status(&self) -> Result<Vec<AuditImportStatus>, AuditStoreError> {
        let rows = sqlx::query(
            "SELECT DISTINCT ON (source) source, cursor, state, advanced_at
             FROM audit.import_cursor ORDER BY source, advanced_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|error| unavailable(&error))?;
        rows.iter()
            .map(|row| {
                Ok(AuditImportStatus {
                    source: row.try_get("source")?,
                    cursor: row.try_get("cursor")?,
                    state: row.try_get("state")?,
                    advanced_at: row.try_get("advanced_at")?,
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(|error| unavailable(&error))
    }

    async fn latest_digest(&self) -> Result<Option<AuditDigest>, AuditStoreError> {
        sqlx::query(&format!(
            "SELECT {DIGEST_COLUMNS} ORDER BY d.digest_no DESC LIMIT 1"
        ))
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| unavailable(&error))?
        .as_ref()
        .map(digest_from)
        .transpose()
        .map_err(|error| unavailable(&error))
    }
}

#[cfg(test)]
mod tests {
    use super::like_prefix;

    #[test]
    fn a_prefix_matches_its_own_characters_only() {
        assert_eq!(like_prefix("job"), "job%");
        assert_eq!(like_prefix("a_b%c\\"), "a\\_b\\%c\\\\%");
    }
}
