//! Disposable-Postgres proof of the audit record `audit.event` (root ADR-0569 §3, ADR-0584).
//!
//! The record answers "who changed this". A ledger row without its audit row, a row the API could
//! rewrite, a writer that waits on another, or a row that could still arrive in a range after its
//! digest was made would each make that answer wrong without anything saying so. Each property is
//! planted here against the harness database (`cargo xtask integration foundation postgres`). Most
//! tests roll back; the concurrency tests commit, because concurrency needs several transactions.

#![allow(clippy::expect_used)]

use std::time::Duration;

use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

const OWNER: &str = "00000000-0000-4000-8000-0000000000aa";

async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation postgres`");
    PgPool::connect(&url).await
}

async fn pause(tx: &mut Transaction<'_, Postgres>) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.operator_action (id, job_id, action, actor, reason)
         VALUES ($1, 'shape_ledger_check', 'pause', $2, 'a stop for the night')",
    )
    .bind(id)
    .bind(OWNER)
    .execute(&mut **tx)
    .await?;
    Ok(id)
}

async fn refused(tx: &mut Transaction<'_, Postgres>, sql: &str) -> Result<bool, sqlx::Error> {
    sqlx::query("SAVEPOINT attempt").execute(&mut **tx).await?;
    let outcome = sqlx::query(sql).execute(&mut **tx).await;
    sqlx::query("ROLLBACK TO SAVEPOINT attempt")
        .execute(&mut **tx)
        .await?;
    Ok(outcome.is_err())
}

async fn recorded(pool: &PgPool, id: Uuid) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM audit.event WHERE source = 'dawneer' AND source_event_id = $1",
    )
    .bind(format!("operator_action:{id}:requested"))
    .fetch_one(pool)
    .await
}

/// Two fixed rows, in the table's column order: `seq`, the event's columns, then `txid`.
const FIXED_ROWS: &str = "
    SELECT (ROW(
        1, '00000000-0000-4000-8000-000000000001'::uuid, '2026-11-19T01:02:03.456789Z'::timestamptz,
        '2026-11-19T01:02:04Z'::timestamptz, 'dawneer', 'operator_action:x:requested',
        'operator_action.run_now', 'execute', 'staff', '00000000-0000-4000-8000-0000000000aa'::uuid,
        NULL, NULL, true, NULL, 'job', 'shape_ledger_check', NULL,
        '{\"generation\": 3, \"lanes\": \"a,b\"}'::jsonb, '한 줄 이유', 'api', 'x', 'requested', NULL,
        '00000000-0000-4000-8000-000000000002'::uuid, '1000'::xid8)::audit.event) AS first,
           (ROW(
        2, '00000000-0000-4000-8000-000000000003'::uuid, '2026-11-19T01:02:03.456789Z'::timestamptz,
        '2026-11-19T01:02:04Z'::timestamptz, 'dawneer', 'operator_action:y:requested',
        'operator_action.run_now', 'execute', 'staff', '00000000-0000-4000-8000-0000000000aa'::uuid,
        NULL, NULL, true, NULL, 'job', 'shape_ledger_check', NULL,
        '{\"generation\": 3, \"lanes\": \"a,b\"}'::jsonb, '한 줄 이유', 'api', 'x', 'requested', NULL,
        '00000000-0000-4000-8000-000000000002'::uuid, '1001'::xid8)::audit.event) AS second";

/// The bytes a digest covers are fixed: these vectors were computed once from the canonical text
/// with an independent SHA-256 (and the same computation reproduces the per-row chain's old vector).
/// A change to `audit.canon` or `audit.rows_sha256` that would make witnessed digests unverifiable
/// fails here.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_hash_of_fixed_rows_is_fixed() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let row = sqlx::query(&format!(
        "SELECT audit.canon(t.first) AS canon,
                encode(sha256(convert_to(audit.canon(t.first), 'UTF8')), 'hex') AS one,
                encode(audit.rows_sha256(ARRAY[t.second, t.first]), 'hex') AS two,
                encode(audit.rows_sha256('{{}}'), 'hex') AS none
         FROM ({FIXED_ROWS}) AS t"
    ))
    .fetch_one(&pool)
    .await?;
    let canon: String = row.get("canon");
    assert!(
        canon.starts_with(
            "{\"seq\": 1, \"txid\": \"1000\", \"after\": {\"lanes\": \"a,b\", \"generation\": 3},"
        ),
        "{canon}"
    );
    assert_eq!(
        row.get::<String, _>("one"),
        "f4c98a19e3fb8798cd25b7768969e2daa588e93e0e68701f46bd43bdc75df8a5",
        "canonical text: {canon}"
    );
    assert_eq!(
        row.get::<String, _>("two"),
        "2535e363bcac683b1615fb14212f45e74a3f50dcd008637dcb9da0fa8c575207",
        "rows are hashed in seq order, one canonical text per line, whatever order they are given in"
    );
    assert_eq!(
        row.get::<String, _>("none"),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn an_operator_ledger_row_is_an_audit_row_in_the_same_transaction() -> Result<(), sqlx::Error>
{
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let id = pause(&mut tx).await?;
    let row = sqlx::query(
        "SELECT action, actor_kind, actor_staff_id::text AS staff, object_type, object_id, result,
                reason, change_id, txid = pg_current_xact_id() AS this_transaction
         FROM audit.event WHERE source = 'dawneer' AND source_event_id = $1",
    )
    .bind(format!("operator_action:{id}:requested"))
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(row.get::<String, _>("action"), "operator_action.pause");
    assert_eq!(row.get::<String, _>("actor_kind"), "staff");
    assert_eq!(row.get::<String, _>("staff"), OWNER);
    assert_eq!(row.get::<String, _>("object_type"), "job");
    assert_eq!(row.get::<String, _>("object_id"), "shape_ledger_check");
    assert_eq!(row.get::<String, _>("result"), "requested");
    assert_eq!(row.get::<String, _>("reason"), "a stop for the night");
    assert_eq!(row.get::<Uuid, _>("change_id"), id);
    assert!(
        row.get::<bool, _>("this_transaction"),
        "txid names the writing transaction"
    );
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_rolled_back_change_leaves_no_row() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let id = pause(&mut tx).await?;
    tx.rollback().await?;
    assert_eq!(recorded(&pool, id).await?, 0);
    Ok(())
}

/// No writer lock: a writer whose transaction is still open does not hold back another.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn an_open_writer_does_not_hold_back_another() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut open = pool.begin().await?;
    pause(&mut open).await?;
    let other = tokio::time::timeout(Duration::from_secs(5), async {
        let mut tx = pool.begin().await?;
        let id = pause(&mut tx).await?;
        tx.commit().await?;
        Ok::<Uuid, sqlx::Error>(id)
    })
    .await
    .expect("the second writer waited on the first")?;
    assert_eq!(recorded(&pool, other).await?, 1);
    open.rollback().await
}

/// The digest's range ends at `pg_snapshot_xmin`: a row whose transaction is still open is never
/// below it, so it cannot arrive in a range that was already witnessed.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn an_open_row_is_never_below_the_digest_watermark() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut open = pool.begin().await?;
    let id = pause(&mut open).await?;
    let txid: String = sqlx::query_scalar(
        "SELECT txid::text FROM audit.event WHERE source = 'dawneer' AND source_event_id = $1",
    )
    .bind(format!("operator_action:{id}:requested"))
    .fetch_one(&mut *open)
    .await?;
    let below: bool =
        sqlx::query_scalar("SELECT $1::xid8 < pg_snapshot_xmin(pg_current_snapshot())")
            .bind(&txid)
            .fetch_one(&pool)
            .await?;
    assert!(
        !below,
        "an open transaction's row {txid} is below the watermark"
    );
    open.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn concurrent_changes_are_all_recorded_once() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut writers = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        writers.push(tokio::spawn(async move {
            let mut ids = Vec::new();
            for _ in 0..10 {
                let mut tx = pool.begin().await?;
                ids.push(pause(&mut tx).await?);
                tx.commit().await?;
            }
            Ok::<Vec<Uuid>, sqlx::Error>(ids)
        }));
    }
    let mut ids = Vec::new();
    for writer in writers {
        ids.extend(writer.await.expect("a writer panicked")?);
    }
    let keys: Vec<String> = ids
        .iter()
        .map(|id| format!("operator_action:{id}:requested"))
        .collect();
    let (rows, seqs): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT seq) FROM audit.event
         WHERE source = 'dawneer' AND source_event_id = ANY($1)",
    )
    .bind(&keys)
    .fetch_one(&pool)
    .await?;
    assert_eq!((rows, seqs), (80, 80));
    Ok(())
}

/// The lock's READ COMMITTED rule is gone with the lock.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_repeatable_read_transaction_may_append() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    pause(&mut tx).await?;
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_record_is_never_rewritten_and_a_role_writes_only_its_own_sources(
) -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    for sql in [
        "UPDATE audit.event SET reason = 'rewritten'",
        "DELETE FROM audit.event",
        "TRUNCATE audit.event",
        "UPDATE audit.digest SET row_count = 0",
        "DELETE FROM audit.digest",
        "UPDATE audit.source SET writer_role = 'postgres'",
        "DELETE FROM audit.import_cursor",
        // The harness role is no source's writer: it may not claim the console's events.
        "SELECT audit.append(gen_random_uuid(), now(), 'dawneer', 'forged', 'operator_action.pause',
            'execute', 'staff', '00000000-0000-4000-8000-0000000000aa', NULL, NULL, true, NULL, 'job',
            'shape_ledger_check', NULL, NULL, NULL, NULL, NULL, 'requested', NULL, NULL)",
        "SELECT audit.advance_cursor('deploy', 'all', 'running')",
    ] {
        assert!(refused(&mut tx, sql).await?, "accepted: {sql}");
    }
    tx.rollback().await
}
