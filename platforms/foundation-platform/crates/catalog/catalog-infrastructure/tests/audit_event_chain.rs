//! Disposable-Postgres proof of the audit chain `audit.event` (root ADR-0569 §3, §4).
//!
//! The chain is the record that answers "who changed this": a hole in `seq`, two rows on one
//! `prev_hash`, a ledger row without its audit row, or a row the API could rewrite would each make
//! that answer wrong without anything saying so. Each property is planted here against the harness
//! database (`cargo xtask integration foundation postgres`). Most tests roll back; the concurrency
//! test commits, because concurrency needs several transactions.

#![allow(clippy::expect_used)]

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

/// How many rows break the chain: a `seq` that is not the one before plus one, a `prev_hash` that is
/// not the row before's `row_hash`, or a `row_hash` that is not the row's own.
const BROKEN_LINKS: &str = "
    SELECT count(*) FROM (
        SELECT e.seq, e.prev_hash, e.row_hash, audit.row_hash(e) AS recomputed,
               lag(e.seq) OVER (ORDER BY e.seq) AS before_seq,
               lag(e.row_hash) OVER (ORDER BY e.seq) AS before_hash
        FROM audit.event AS e) AS c
    WHERE c.row_hash <> c.recomputed
       OR (c.before_seq IS NULL AND (c.seq <> 1 OR c.prev_hash <> decode(repeat('00', 32), 'hex')))
       OR (c.before_seq IS NOT NULL AND (c.seq <> c.before_seq + 1 OR c.prev_hash <> c.before_hash))";

/// The bytes a hash covers are fixed: this vector was computed once and checked with an independent
/// SHA-256 of the canonical text. A change to `audit.canon` that would make old digests unverifiable
/// fails here.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_row_hash_of_a_fixed_row_is_fixed() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let row = sqlx::query(
        "SELECT audit.canon(e) AS canon, encode(audit.row_hash(e), 'hex') AS hash FROM (SELECT (ROW(
            1, '00000000-0000-4000-8000-000000000001'::uuid, '2026-11-19T01:02:03.456789Z'::timestamptz,
            '2026-11-19T01:02:04Z'::timestamptz, 'dawneer', 'operator_action:x:requested',
            'operator_action.run_now', 'execute', 'staff', '00000000-0000-4000-8000-0000000000aa'::uuid,
            NULL, NULL, true, NULL, 'job', 'shape_ledger_check', NULL,
            '{\"generation\": 3, \"lanes\": \"a,b\"}'::jsonb, '한 줄 이유', 'api', 'x', 'requested', NULL,
            '00000000-0000-4000-8000-000000000002'::uuid, decode(repeat('00', 32), 'hex'),
            decode(repeat('ff', 32), 'hex'))::audit.event) AS e) AS t",
    )
    .fetch_one(&pool)
    .await?;
    let canon: String = row.get("canon");
    let hash: String = row.get("hash");
    assert!(
        canon.starts_with("{\"after\": {\"lanes\": \"a,b\", \"generation\": 3}, \"error\": null,"),
        "{canon}"
    );
    assert_eq!(
        hash, "843fd17047030c368bd4968202a919687587c8977cb0bee61d2924a8166732d2",
        "canonical text: {canon}"
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
                reason, change_id
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
    let broken: i64 = sqlx::query_scalar(BROKEN_LINKS).fetch_one(&mut *tx).await?;
    assert_eq!(broken, 0, "the chain is broken");
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_rolled_back_change_leaves_no_row_and_no_hole() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    tx.rollback().await?;
    let broken: i64 = sqlx::query_scalar(BROKEN_LINKS).fetch_one(&pool).await?;
    assert_eq!(
        broken, 0,
        "a rolled-back transaction left a hole in the chain"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn concurrent_changes_chain_one_after_another() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut writers = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        writers.push(tokio::spawn(async move {
            for _ in 0..10 {
                let mut tx = pool.begin().await?;
                pause(&mut tx).await?;
                tx.commit().await?;
            }
            Ok::<(), sqlx::Error>(())
        }));
    }
    for writer in writers {
        writer.await.expect("a writer panicked")?;
    }
    let broken: i64 = sqlx::query_scalar(BROKEN_LINKS).fetch_one(&pool).await?;
    assert_eq!(
        broken, 0,
        "two rows chained onto one prev_hash, or seq has a hole"
    );
    let shared: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT prev_hash FROM audit.event GROUP BY prev_hash HAVING count(*) > 1) AS d",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(shared, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_repeatable_read_transaction_may_not_append() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let outcome = pause(&mut tx).await;
    assert!(
        outcome.is_err(),
        "a ledger row was written under REPEATABLE READ"
    );
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_chain_is_never_rewritten_and_a_role_writes_only_its_own_sources(
) -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    for sql in [
        "UPDATE audit.event SET reason = 'rewritten'",
        "DELETE FROM audit.event",
        "TRUNCATE audit.event",
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
