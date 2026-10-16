//! Root ADR-0258: business verification lives only in `business_verification_queue`, every
//! status it passes through is kept in `business_verification_status_history`, and the old
//! `"user"` copy moves into the queue without losing a row.

use business_verification_domain::entity::BusinessVerification;
use business_verification_domain::repository::BusinessVerificationRepository;
use chrono::{Duration, Utc};
use gongzzang_persistence::business_verification::PgBusinessVerificationRepository;

use super::common::{setup_test_pool, test_ctx, truncate_all};
use super::{make_business_verification, seed_user};

/// The section of the shipped migration that acts on existing rows (between the markers).
const MIGRATION: &str = include_str!(
    "../../../../migrations/20261015130000_business_verification_lives_in_its_queue.sql"
);

/// Saves a transition. `save` compares against the version read before it (see
/// `update_bumps_version_in_db`), so it is handed that version and the DB bumps it.
async fn save_transition(repo: &PgBusinessVerificationRepository, bv: &mut BusinessVerification) {
    bv.version -= 1;
    repo.save(bv, test_ctx()).await.unwrap();
    bv.version += 1;
}

fn backfill_section() -> &'static str {
    let start = MIGRATION.find("-- backfill:begin").expect("begin marker");
    let end = MIGRATION.find("-- backfill:end").expect("end marker");
    &MIGRATION[start..end]
}

#[tokio::test]
async fn every_status_is_kept_and_the_verified_business_is_the_approved_row() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-bv-one-1", "bvone1@example.com").await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());
    let reviewer = seed_user(&pool, "zsub-bv-one-1r", "bvone1r@example.com").await;
    let now = Utc::now();

    let mut bv = make_business_verification(user_id.clone());
    repo.save(&bv, test_ctx()).await.unwrap();
    assert!(repo
        .find_verified_business(&user_id)
        .await
        .unwrap()
        .is_none());

    bv.approve(reviewer.clone(), None, now + Duration::minutes(1))
        .unwrap();
    save_transition(&repo, &mut bv).await;
    let verified = repo.find_verified_business(&user_id).await.unwrap();
    assert_eq!(verified.map(|v| v.id), Some(bv.id.clone()));

    bv.revoke(reviewer, "폐업".to_owned(), now + Duration::minutes(2))
        .unwrap();
    save_transition(&repo, &mut bv).await;
    assert!(repo
        .find_verified_business(&user_id)
        .await
        .unwrap()
        .is_none());

    let statuses: Vec<String> = sqlx::query_scalar(
        "select status from business_verification_status_history \
         where verification_id = $1 order by history_id",
    )
    .bind(bv.id.as_str())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(statuses, ["pending", "approved", "revoked"]);

    let err = sqlx::query("update business_verification_status_history set status = 'approved'")
        .execute(&pool)
        .await
        .expect_err("history is append-only");
    assert!(err.to_string().contains("append-only"), "{err}");
}

/// Production shape: users written before the migration carry the verification on their own
/// row. Replaying the shipped backfill over them must move every fact into the queue.
#[tokio::test]
async fn the_user_row_copy_moves_into_the_queue_without_losing_a_row() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let verified = seed_user(&pool, "zsub-bv-one-2", "bvone2@example.com").await;
    let number_only = seed_user(&pool, "zsub-bv-one-3", "bvone3@example.com").await;
    let already_queued = seed_user(&pool, "zsub-bv-one-4", "bvone4@example.com").await;
    let plain = seed_user(&pool, "zsub-bv-one-5", "bvone5@example.com").await;

    let repo = PgBusinessVerificationRepository::new(pool.clone());
    let mut queued = make_business_verification(already_queued.clone());
    repo.save(&queued, test_ctx()).await.unwrap();
    queued.approve(plain.clone(), None, Utc::now()).unwrap();
    save_transition(&repo, &mut queued).await;

    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        r#"alter table "user" add column business_number varchar(12),
                              add column business_verified_at timestamptz"#,
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    for (user, verified_at) in [
        (&verified, Some(Utc::now())),
        (&number_only, None),
        (&already_queued, Some(Utc::now())),
    ] {
        sqlx::query(
            r#"update "user" set business_number = '1234567891', business_verified_at = $2
               where id = $1"#,
        )
        .bind(user.as_str())
        .bind(verified_at)
        .execute(&mut *tx)
        .await
        .unwrap();
    }

    sqlx::raw_sql(backfill_section())
        .execute(&mut *tx)
        .await
        .expect("backfill runs on old-shape rows");

    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "select user_id, business_number, status from business_verification_queue \
         order by user_id, status",
    )
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    let mut expected = vec![
        (
            verified.as_str().to_owned(),
            "1234567891".to_owned(),
            "approved".to_owned(),
        ),
        (
            number_only.as_str().to_owned(),
            "1234567891".to_owned(),
            "revoked".to_owned(),
        ),
        (
            already_queued.as_str().to_owned(),
            "1234567891".to_owned(),
            "approved".to_owned(),
        ),
    ];
    expected.sort();
    assert_eq!(
        rows, expected,
        "one row per fact, none duplicated, none lost"
    );
    assert!(!rows.iter().any(|(u, _, _)| u == plain.as_str()));

    let history: i64 = sqlx::query_scalar(
        "select count(*) from business_verification_status_history h \
         join business_verification_queue q on q.id = h.verification_id \
         where q.user_id in ($1, $2)",
    )
    .bind(verified.as_str())
    .bind(number_only.as_str())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(history, 2, "each carried-over row starts its history");

    let user_columns: i64 = sqlx::query_scalar(
        "select count(*) from information_schema.columns \
         where table_name = 'user' and column_name like 'business%'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(user_columns, 0, "the second copy is gone");
    tx.rollback().await.unwrap();
}

/// Root ADR-0264: a resubmission overwrites the queue row's documents; the history keeps the
/// documents of every submission and refuses DELETE as well as UPDATE.
#[tokio::test]
async fn a_resubmission_keeps_the_documents_it_replaced() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-bv-docs-1", "bvdocs1@example.com").await;
    let reviewer = seed_user(&pool, "zsub-bv-docs-1r", "bvdocs1r@example.com").await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());
    let now = Utc::now();

    let mut bv = make_business_verification(user_id);
    let first_documents = bv.submitted_documents.clone();
    repo.save(&bv, test_ctx()).await.unwrap();
    bv.request_more_info(reviewer, "사업자등록증 재제출".to_owned(), now)
        .unwrap();
    save_transition(&repo, &mut bv).await;
    let second_documents =
        serde_json::json!({"document_keys": ["business_verification/abc/v2.pdf"]});
    bv.resubmit(second_documents.clone(), now + Duration::minutes(1))
        .unwrap();
    save_transition(&repo, &mut bv).await;

    let history: Vec<(String, Option<serde_json::Value>)> = sqlx::query_as(
        "select status, submitted_documents from business_verification_status_history \
         where verification_id = $1 order by history_id",
    )
    .bind(bv.id.as_str())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        history,
        vec![
            ("pending".to_owned(), Some(first_documents.clone())),
            ("needs_more_info".to_owned(), Some(first_documents)),
            ("pending".to_owned(), Some(second_documents)),
        ]
    );

    let err = sqlx::query("delete from business_verification_status_history")
        .execute(&pool)
        .await
        .expect_err("history is append-only");
    assert!(err.to_string().contains("append-only"), "{err}");
}
