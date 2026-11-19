//! Disposable-Postgres proof of the audit record `audit.event` (root ADR-0569 §3, ADR-0584).
//!
//! The record answers "who changed this". A ledger row without its audit row, a row the API could
//! rewrite, a writer that waits on another, a row that could still arrive in a range after its
//! digest was made, or a digest that does not follow the one before would each make that answer wrong without anything saying so. Each property is
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

/// The bytes a digest covers are fixed: these vectors were computed once, outside Postgres, by a
/// separate program that prints jsonb text by Postgres's rules and hashes it (the same program also
/// gives the per-row chain's old vector `843fd170…`, which is how it was checked).
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

async fn snapshot(pool: &PgPool) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT pg_current_snapshot()::text")
        .fetch_one(pool)
        .await
}

/// Which of `ids`' request rows lie between two snapshots.
async fn between(
    pool: &PgPool,
    from: &str,
    to: &str,
    ids: &[Uuid],
) -> Result<Vec<Uuid>, sqlx::Error> {
    let keys: Vec<String> = ids
        .iter()
        .map(|id| format!("operator_action:{id}:requested"))
        .collect();
    sqlx::query_scalar(
        "SELECT change_id FROM audit.rows_between($1::pg_snapshot, $2::pg_snapshot)
         WHERE source = 'dawneer' AND source_event_id = ANY($3) ORDER BY seq",
    )
    .bind(from)
    .bind(to)
    .bind(&keys)
    .fetch_all(pool)
    .await
}

/// A digest is the snapshot it was made in (ADR-0584 §2). A transaction still open when it is made
/// holds back only its own row: a row committed after that transaction began is in this range, and
/// the open one arrives in the next range once it commits — never in one already made.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_long_transaction_holds_back_only_its_own_rows() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let start = snapshot(&pool).await?;
    let mut long = pool.begin().await?;
    let late = pause(&mut long).await?;
    let mut tx = pool.begin().await?;
    let early = pause(&mut tx).await?;
    tx.commit().await?;

    let first = snapshot(&pool).await?;
    assert_eq!(
        between(&pool, &start, &first, &[late, early]).await?,
        [early]
    );

    long.commit().await?;
    let second = snapshot(&pool).await?;
    assert_eq!(
        between(&pool, &start, &first, &[late, early]).await?,
        [early],
        "a made range never changes"
    );
    assert_eq!(
        between(&pool, &first, &second, &[late, early]).await?,
        [late]
    );
    Ok(())
}

/// `txid` is the top-level transaction's id even inside a savepoint, so the row is covered when its
/// transaction commits.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_row_written_in_a_savepoint_is_covered_with_its_transaction() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let start = snapshot(&pool).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SAVEPOINT inner_step")
        .execute(&mut *tx)
        .await?;
    let id = pause(&mut tx).await?;
    sqlx::query("RELEASE SAVEPOINT inner_step")
        .execute(&mut *tx)
        .await?;
    let top: bool = sqlx::query_scalar(
        "SELECT txid = pg_current_xact_id() FROM audit.event
         WHERE source = 'dawneer' AND source_event_id = $1",
    )
    .bind(format!("operator_action:{id}:requested"))
    .fetch_one(&mut *tx)
    .await?;
    assert!(top, "a savepoint's row carries the outer transaction's id");
    tx.commit().await?;
    let end = snapshot(&pool).await?;
    assert_eq!(between(&pool, &start, &end, &[id]).await?, [id]);
    Ok(())
}

/// The range hash is the rows' hash over exactly the rows between the two snapshots.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_range_hash_is_the_hash_of_the_rows_between() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let start = snapshot(&pool).await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    tx.commit().await?;
    let end = snapshot(&pool).await?;
    let (same, rows): (bool, i64) = sqlx::query_as(
        "SELECT audit.range_sha256($1::pg_snapshot, $2::pg_snapshot)
                = audit.rows_sha256(ARRAY(SELECT r FROM audit.rows_between($1::pg_snapshot, $2::pg_snapshot) AS r)),
                (SELECT count(*) FROM audit.rows_between($1::pg_snapshot, $2::pg_snapshot))",
    )
    .bind(&start)
    .bind(&end)
    .fetch_one(&pool)
    .await?;
    assert!(same);
    assert!(rows >= 1);
    let nothing: bool = sqlx::query_scalar(
        "SELECT audit.range_sha256($1::pg_snapshot, $1::pg_snapshot) = sha256(''::bytea)",
    )
    .bind(&end)
    .fetch_one(&pool)
    .await?;
    assert!(nothing, "a snapshot to itself covers nothing");
    Ok(())
}

/// The next digest as the digest job would make it, with one thing changed to plant a fault: the
/// number moved by `$1`, the count by `$2`, a wrong own hash when `$3`, a snapshot from the future
/// when `$4`, and no previous hash when `$5`.
const NEXT_DIGEST: &str = "
    WITH prev AS (SELECT digest_no, to_snapshot, digest_sha256 FROM audit.digest
                  ORDER BY digest_no DESC LIMIT 1),
         n AS (SELECT coalesce((SELECT digest_no FROM prev), 0) + 1 + $1 AS no,
                      (SELECT to_snapshot FROM prev) AS f,
                      CASE WHEN $4 THEN format('%s:%s:',
                               pg_snapshot_xmax(pg_current_snapshot())::text::bigint + 1000,
                               pg_snapshot_xmax(pg_current_snapshot())::text::bigint + 1000)::pg_snapshot
                           ELSE pg_current_snapshot() END AS t,
                      CASE WHEN $5 THEN NULL ELSE (SELECT digest_sha256 FROM prev) END AS p),
         r AS (SELECT n.*, (SELECT count(*) FROM audit.rows_between(n.f, n.t)) + $2 AS c,
                      audit.range_sha256(n.f, n.t) AS h FROM n)
    INSERT INTO audit.digest
        (digest_no, from_snapshot, to_snapshot, row_count, rows_sha256, previous_digest_sha256, digest_sha256)
    SELECT no, f, t, c, h, p,
           CASE WHEN $3 THEN sha256('not this digest'::bytea) ELSE audit.digest_sha256(no, f, t, c, h, p) END
    FROM r";

async fn next_digest(
    tx: &mut Transaction<'_, Postgres>,
    fault: (i64, i64, bool, bool, bool),
) -> Result<(), sqlx::Error> {
    sqlx::query(NEXT_DIGEST)
        .bind(fault.0)
        .bind(fault.1)
        .bind(fault.2)
        .bind(fault.3)
        .bind(fault.4)
        .execute(&mut **tx)
        .await
        .map(|_| ())
}

async fn digest_refused(
    tx: &mut Transaction<'_, Postgres>,
    fault: (i64, i64, bool, bool, bool),
) -> Result<bool, sqlx::Error> {
    sqlx::query("SAVEPOINT attempt").execute(&mut **tx).await?;
    let outcome = next_digest(tx, fault).await;
    sqlx::query("ROLLBACK TO SAVEPOINT attempt")
        .execute(&mut **tx)
        .await?;
    Ok(outcome.is_err())
}

/// Digests follow one another and say only what is true of their rows (ADR-0584 §2): a skipped
/// number, a wrong count, a wrong own hash, a snapshot that sees transactions not yet ended, and a
/// missing link to the digest before are each refused; the true next digest is accepted.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_digest_follows_the_one_before_and_matches_its_rows() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    next_digest(&mut tx, (0, 0, false, false, false)).await?;
    for (what, fault) in [
        ("a skipped number", (1, 0, false, false, false)),
        ("a wrong count", (0, 1, false, false, false)),
        ("a wrong own hash", (0, 0, true, false, false)),
        ("a snapshot from the future", (0, 0, false, true, false)),
        ("no link to the digest before", (0, 0, false, false, true)),
    ] {
        assert!(digest_refused(&mut tx, fault).await?, "accepted {what}");
    }
    next_digest(&mut tx, (0, 0, false, false, false)).await?;
    tx.rollback().await
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
        "UPDATE audit.digest_witness SET rekor_log_index = 0",
        "DELETE FROM audit.digest_witness",
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
