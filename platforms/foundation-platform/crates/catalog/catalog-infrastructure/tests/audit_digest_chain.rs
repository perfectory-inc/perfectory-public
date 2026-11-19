//! Disposable-Postgres proof of the hourly audit digest (root ADR-0569 §4, ADR-0584 §2): the next
//! digest is kept before its witness and covers the new rows, a witness must be for its digest's
//! hash, a row is in one digest only, and a row rewritten behind the triggers is found.
//! Each test commits one request row (a digest only covers committed rows) and makes its digests in
//! a rolled-back transaction (`cargo xtask integration foundation postgres`).

#![allow(clippy::expect_used)]

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation postgres`");
    PgPool::connect(&url).await
}

/// Commits one request row and answers its change id.
async fn committed_pause(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.operator_action (id, job_id, action, actor, reason)
         VALUES ($1, 'shape_ledger_check', 'pause', '00000000-0000-4000-8000-0000000000aa', 'a stop')",
    )
    .bind(id)
    .execute(pool)
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

/// A sigstore bundle v0.3 as cosign 3 writes it, cut to the fields the database reads.
fn bundle(digest_sha256_base64: &str, log_index: &str) -> String {
    format!(
        "{{\"mediaType\": \"application/vnd.dev.sigstore.bundle.v0.3+json\",
           \"messageSignature\": {{\"messageDigest\": {{\"algorithm\": \"SHA2_256\",
                                                     \"digest\": \"{digest_sha256_base64}\"}}}},
           \"verificationMaterial\": {{\"tlogEntries\": [{{\"logIndex\": \"{log_index}\"}}]}}}}"
    )
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_digest_is_kept_before_its_witness_and_the_witness_must_be_for_its_hash(
) -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let change = committed_pause(&pool).await?;
    let mut tx = pool.begin().await?;
    let (no, rows, hashed, covers): (i64, i64, bool, bool) = sqlx::query_as(
        "SELECT d.digest_no, d.row_count,
                d.digest_sha256 = sha256(convert_to(audit.digest_text(d.digest_no, d.from_snapshot,
                    d.to_snapshot, d.row_count, d.rows_sha256, d.previous_digest_sha256), 'UTF8')),
                EXISTS (SELECT FROM audit.rows_between(d.from_snapshot, d.to_snapshot) r
                        WHERE r.change_id = $1)
         FROM audit.make_digest() AS d",
    )
    .bind(change)
    .fetch_one(&mut *tx)
    .await?;
    assert!(rows >= 1);
    assert!(hashed, "the digest's hash is not the hash of its text");
    assert!(covers, "the new digest does not cover the committed row");

    let wrong = bundle("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "7");
    assert!(
        refused(
            &mut tx,
            &format!("SELECT audit.witness_digest({no}, '{wrong}'::jsonb)")
        )
        .await?,
        "a bundle for another hash was kept"
    );
    assert!(
        refused(
            &mut tx,
            &format!("SELECT audit.witness_digest({no}, '{{}}'::jsonb)")
        )
        .await?,
        "an empty bundle was kept"
    );
    let right: String = sqlx::query_scalar(
        "SELECT encode(digest_sha256, 'base64') FROM audit.digest WHERE digest_no = $1",
    )
    .bind(no)
    .fetch_one(&mut *tx)
    .await?;
    let index: i64 = sqlx::query_scalar("SELECT audit.witness_digest($1, $2::jsonb)")
        .bind(no)
        .bind(bundle(&right, "123456789"))
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(index, 123_456_789, "the log index is read from the bundle");
    assert!(
        refused(
            &mut tx,
            &format!(
                "SELECT audit.witness_digest({no}, '{}'::jsonb)",
                bundle(&right, "123456790")
            )
        )
        .await?,
        "a digest was witnessed twice"
    );
    let problems: Vec<String> = sqlx::query_scalar("SELECT * FROM audit.verify_all()")
        .fetch_all(&mut *tx)
        .await?;
    assert!(problems.is_empty(), "{problems:?}");
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_row_is_in_one_digest_only() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let change = committed_pause(&pool).await?;
    let mut tx = pool.begin().await?;
    // Tests run at once and commit rows of their own, so the second digest may exist; it must not
    // hold this test's row again.
    let covering = "SELECT count(*) FROM audit.make_digest() AS d,
                    audit.rows_between(d.from_snapshot, d.to_snapshot) AS r WHERE r.change_id = $1";
    let first: i64 = sqlx::query_scalar(covering)
        .bind(change)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(first, 1);
    let second: i64 = sqlx::query_scalar(covering)
        .bind(change)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(second, 0, "a row was digested twice");
    tx.rollback().await
}

#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_row_rewritten_behind_the_triggers_is_found() -> Result<(), sqlx::Error> {
    let pool = pool().await?;
    let change = committed_pause(&pool).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT * FROM audit.make_digest()")
        .execute(&mut *tx)
        .await?;
    // What a table owner can do: switch the trigger off and rewrite a row a digest covers.
    sqlx::query("ALTER TABLE audit.event DISABLE TRIGGER event_append_only")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE audit.event SET reason = 'rewritten' WHERE change_id = $1")
        .bind(change)
        .execute(&mut *tx)
        .await?;
    let problems: Vec<String> = sqlx::query_scalar("SELECT * FROM audit.verify_all()")
        .fetch_all(&mut *tx)
        .await?;
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("no longer matches its rows")),
        "{problems:?}"
    );
    tx.rollback().await
}
