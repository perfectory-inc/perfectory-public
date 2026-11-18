//! Disposable-Postgres proof of the hourly audit digest (root ADR-0569 §4): the next digest covers
//! the rows after the last one, a rewritten row is found, and only the importer may keep a digest.
//! Every test runs inside a rolled-back transaction (`cargo xtask integration foundation postgres`).

#![allow(clippy::expect_used)]

use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation postgres`");
    PgPool::connect(&url).await
}

async fn pause(tx: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO catalog.operator_action (id, job_id, action, actor, reason)
         VALUES ($1, 'shape_ledger_check', 'pause', '00000000-0000-4000-8000-0000000000aa', 'a stop')",
    )
    .bind(Uuid::new_v4())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn the_next_digest_covers_the_new_rows_and_hashes_its_own_text() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    pause(&mut tx).await?;
    let row = sqlx::query(
        "SELECT d.from_seq, d.to_seq, d.count, d.head_hash = (SELECT row_hash FROM audit.event WHERE seq = d.to_seq) AS head,
                d.digest_sha256 = sha256(convert_to(d.digest_text, 'UTF8')) AS hashed,
                d.to_seq = (SELECT max(seq) FROM audit.event) AS newest
         FROM audit.next_digest() AS d",
    )
    .fetch_one(&mut *tx)
    .await?;
    let from: i64 = row.get("from_seq");
    let to: i64 = row.get("to_seq");
    assert_eq!(row.get::<i64, _>("count"), to - from + 1);
    assert!(row.get::<bool, _>("head"));
    assert!(row.get::<bool, _>("hashed"));
    assert!(row.get::<bool, _>("newest"));
    let problems: Vec<String> = sqlx::query_scalar("SELECT * FROM audit.verify_all()")
        .fetch_all(&mut *tx)
        .await?;
    assert!(problems.is_empty(), "{problems:?}");
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_row_rewritten_behind_the_triggers_is_found() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    // What a table owner can do: switch the trigger off and rewrite a row.
    sqlx::query("ALTER TABLE audit.event DISABLE TRIGGER event_append_only")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE audit.event SET reason = 'rewritten' WHERE seq = (SELECT max(seq) FROM audit.event)")
        .execute(&mut *tx)
        .await?;
    let problems: Vec<String> = sqlx::query_scalar("SELECT * FROM audit.verify_all()")
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(problems.len(), 1, "{problems:?}");
    sqlx::query("SAVEPOINT attempt").execute(&mut *tx).await?;
    let next = sqlx::query("SELECT * FROM audit.next_digest()")
        .fetch_all(&mut *tx)
        .await;
    assert!(next.is_err(), "a range that does not chain was digested");
    sqlx::query("ROLLBACK TO SAVEPOINT attempt")
        .execute(&mut *tx)
        .await?;
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn only_the_importer_keeps_a_digest() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    pause(&mut tx).await?;
    let kept = sqlx::query(
        "SELECT audit.record_digest(d.to_seq, d.digest_sha256, 1, '{\"verificationMaterial\": {}}'::jsonb)
         FROM audit.next_digest() AS d",
    )
    .execute(&mut *tx)
    .await;
    assert!(kept.is_err(), "the harness role kept a digest");
    tx.rollback().await
}
