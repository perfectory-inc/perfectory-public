//! The Postgres audit chain, `audit.event` (root ADR-0569 §2, §3, §7).
//!
//! Reads are plain `SELECT`s the API role is granted; the one write is `audit.append`, which the
//! database answers for: it refuses a source this role may not write.

use async_trait::async_trait;
use catalog_application::ports::{
    AuditAppend, AuditDigestHead, AuditEvent, AuditEventFilter, AuditEventStore, AuditImportStatus,
    AuditStoreError,
};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

/// The most rows one read answers.
pub const AUDIT_PAGE_MAX: u32 = 500;

/// How long a write waits for the chain's lock before it gives up (root ADR-0569 §3). The importer
/// writes at most 50 rows a transaction, so a request waits for one batch at most.
pub const AUDIT_LOCK_TIMEOUT: &str = "2s";

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

/// Starts a transaction that waits at most [`AUDIT_LOCK_TIMEOUT`] for the chain's lock.
///
/// # Errors
///
/// Returns the database's error.
pub async fn begin_bounded(
    pool: &PgPool,
) -> Result<sqlx::Transaction<'static, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(&format!("SET LOCAL lock_timeout = '{AUDIT_LOCK_TIMEOUT}'"))
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// Reads and appends the audit chain.
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

#[async_trait]
impl AuditEventStore for PgAuditEventStore {
    async fn page(&self, filter: &AuditEventFilter) -> Result<Vec<AuditEvent>, AuditStoreError> {
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT seq, event_id, occurred_at, recorded_at, source, source_event_id, action, operation,
                    actor_kind, actor_staff_id, actor_subject, actor_external, actor_verified,
                    on_behalf_of, object_type, object_id, before, after, reason, applied_via_kind,
                    applied_via_ref, result, error, change_id, encode(prev_hash, 'hex') AS prev_hash,
                    encode(row_hash, 'hex') AS row_hash
             FROM audit.event WHERE true",
        );
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
        query
            .push(" ORDER BY seq DESC LIMIT ")
            .push_bind(i64::from(filter.limit.clamp(1, AUDIT_PAGE_MAX)));
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|error| unavailable(&error))?;
        rows.iter()
            .map(|row| {
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
                    prev_hash: row.try_get("prev_hash")?,
                    row_hash: row.try_get("row_hash")?,
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(|error| unavailable(&error))
    }

    async fn append(&self, event: &AuditAppend) -> Result<Option<i64>, AuditStoreError> {
        let mut tx = begin_bounded(&self.pool)
            .await
            .map_err(|error| unavailable(&error))?;
        let seq = append_in(&mut tx, event)
            .await
            .map_err(|error| unavailable(&error))?;
        tx.commit().await.map_err(|error| unavailable(&error))?;
        Ok(seq)
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

    async fn latest_digest(&self) -> Result<Option<AuditDigestHead>, AuditStoreError> {
        let row = sqlx::query(
            "SELECT from_seq, to_seq, encode(head_hash, 'hex') AS head_hash,
                    encode(digest_sha256, 'hex') AS digest_sha256, rekor_log_index, made_at
             FROM audit.digest ORDER BY to_seq DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| unavailable(&error))?;
        row.map(|row| {
            Ok(AuditDigestHead {
                from_seq: row.try_get("from_seq")?,
                to_seq: row.try_get("to_seq")?,
                head_hash: row.try_get("head_hash")?,
                digest_sha256: row.try_get("digest_sha256")?,
                rekor_log_index: row.try_get("rekor_log_index")?,
                made_at: row.try_get("made_at")?,
            })
        })
        .transpose()
        .map_err(|error: sqlx::Error| unavailable(&error))
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
