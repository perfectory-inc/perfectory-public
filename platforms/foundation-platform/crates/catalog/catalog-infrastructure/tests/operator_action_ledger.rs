//! Disposable-Postgres proof that the operator action ledger is append-only (root ADR-0282 §2).
//!
//! Each refusal is planted and paired with the nearest write the database must still accept, so a
//! trigger that refused everything would fail here too. Every test runs inside a rolled-back
//! transaction against the harness database (`cargo xtask integration foundation postgres`).

#![allow(clippy::expect_used)]

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Connection for this `#[ignore]`d live suite. A missing `DATABASE_URL` aborts the test instead of
/// reporting success: an absent database is a provisioning failure.
async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation postgres`");
    PgPool::connect(&url).await
}

async fn request(tx: &mut Transaction<'_, Postgres>, action: &str) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.operator_action (id, job_id, action, actor, reason, expires_at)
         VALUES ($1, 'shape_ledger_check', $2, 'staff:owner', 'a supervised run',
                 CASE WHEN $2 = 'run_now' THEN now() + interval '10 minutes' END)",
    )
    .bind(id)
    .bind(action)
    .execute(&mut **tx)
    .await?;
    Ok(id)
}

/// Runs `sql` (with `id` bound as `$1` when given) inside a savepoint and reports whether Postgres
/// refused it.
async fn refused(
    tx: &mut Transaction<'_, Postgres>,
    sql: &str,
    id: Option<Uuid>,
) -> Result<bool, sqlx::Error> {
    sqlx::query("SAVEPOINT attempt").execute(&mut **tx).await?;
    let query = sqlx::query(sql);
    let outcome = match id {
        Some(id) => query.bind(id).execute(&mut **tx).await,
        None => query.execute(&mut **tx).await,
    };
    sqlx::query("ROLLBACK TO SAVEPOINT attempt")
        .execute(&mut **tx)
        .await?;
    Ok(outcome.is_err())
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_run_now_is_answered_once_and_nothing_else_of_it_changes() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let id = request(&mut tx, "run_now").await?;

    for sql in [
        "UPDATE catalog.operator_action SET reason = 'rewritten' WHERE id = $1",
        "UPDATE catalog.operator_action SET expires_at = expires_at + interval '1 day' WHERE id = $1",
        "UPDATE catalog.operator_action SET actor = 'staff:someone-else' WHERE id = $1",
        "DELETE FROM catalog.operator_action WHERE id = $1",
        // An answer is when and what together.
        "UPDATE catalog.operator_action SET consumed_at = now() WHERE id = $1",
    ] {
        assert!(refused(&mut tx, sql, Some(id)).await?, "accepted: {sql}");
    }
    let answer =
        "UPDATE catalog.operator_action SET consumed_at = now(), outcome = 'started' WHERE id = $1";
    assert!(
        !refused(&mut tx, answer, Some(id)).await?,
        "the first answer is refused"
    );
    sqlx::query(answer).bind(id).execute(&mut *tx).await?;
    let again = "UPDATE catalog.operator_action SET consumed_at = now(), outcome = 'skipped consumed' WHERE id = $1";
    assert!(
        refused(&mut tx, again, Some(id)).await?,
        "a request was answered twice"
    );
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn only_a_run_now_expires_and_only_a_pause_drops_what_is_pending() -> Result<(), sqlx::Error>
{
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let id = Uuid::new_v4();
    let insert = |action: &'static str,
                  drop_pending: bool,
                  expires: &'static str,
                  reason: &'static str| {
        format!(
            "INSERT INTO catalog.operator_action (id, job_id, action, drop_pending, actor, reason, expires_at)
             VALUES ($1, 'shape_ledger_check', '{action}', {drop_pending}, 'staff:owner', '{reason}', {expires})"
        )
    };
    for (sql, accepted) in [
        (insert("pause", true, "NULL", "a stop for the night"), true),
        (
            insert("resume", true, "NULL", "a stop for the night"),
            false,
        ),
        (insert("run_now", false, "NULL", "a supervised run"), false),
        (
            insert(
                "run_now",
                false,
                "now() - interval '1 minute'",
                "a supervised run",
            ),
            false,
        ),
        (
            insert(
                "pause",
                false,
                "now() + interval '10 minutes'",
                "a stop for the night",
            ),
            false,
        ),
        (insert("pause", false, "NULL", "   "), false),
        (insert("pause", false, "NULL", "two\nlines"), false),
        (insert("drop", false, "NULL", "a stop for the night"), false),
    ] {
        assert_eq!(!refused(&mut tx, &sql, Some(id)).await?, accepted, "{sql}");
    }
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_ledger_is_never_truncated() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    request(&mut tx, "pause").await?;
    assert!(
        refused(&mut tx, "TRUNCATE catalog.operator_action", None).await?,
        "the ledger was truncated"
    );
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_window_override_names_its_window_and_lasts_at_most_a_week() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let id = Uuid::new_v4();
    let insert = |action: &'static str,
                  expires: &'static str,
                  start: &'static str,
                  end: &'static str| {
        format!(
            "INSERT INTO catalog.operator_action (id, job_id, action, actor, reason, expires_at, window_start, window_end)
             VALUES ($1, 'shape_ledger_check', '{action}', 'staff:owner', 'collect in the day once', {expires}, {start}, {end})"
        )
    };
    for (sql, accepted) in [
        (
            insert(
                "set_window",
                "now() + interval '3 hours'",
                "'09:00'",
                "'18:00'",
            ),
            true,
        ),
        (
            insert(
                "set_window",
                "now() + interval '8 days'",
                "'09:00'",
                "'18:00'",
            ),
            false,
        ),
        (insert("set_window", "NULL", "'09:00'", "'18:00'"), false),
        (
            insert(
                "set_window",
                "now() + interval '3 hours'",
                "'24:00'",
                "'18:00'",
            ),
            false,
        ),
        (
            insert(
                "set_window",
                "now() + interval '3 hours'",
                "'09:00'",
                "NULL",
            ),
            false,
        ),
        (insert("pause", "NULL", "'09:00'", "'18:00'"), false),
        (insert("clear_window", "NULL", "NULL", "NULL"), true),
    ] {
        assert_eq!(!refused(&mut tx, &sql, Some(id)).await?, accepted, "{sql}");
    }
    let id = Uuid::new_v4();
    sqlx::query(&insert(
        "set_window",
        "now() + interval '3 hours'",
        "'09:00'",
        "'18:00'",
    ))
    .bind(id)
    .execute(&mut *tx)
    .await?;
    assert!(
        refused(
            &mut tx,
            "UPDATE catalog.operator_action SET window_end = '20:00' WHERE id = $1",
            Some(id)
        )
        .await?,
        "a window was rewritten"
    );
    tx.rollback().await
}
