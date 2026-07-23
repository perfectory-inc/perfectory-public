//! `PgListingReviewRepository` 통합 테스트 — OCC + transactional `audit_log`/`outbox_event`
//! 패턴 (SP5-iii T7).
//!
//! 5 시나리오:
//! 1. `save` (INSERT) — Listing Review Queue + `audit_log` 1행 (`resource_kind = 'listing_review'`)
//! 2. OCC 버전 불일치 → `Conflict` + tx rollback (`audit_log` 미증가)
//! 3. `save` (UPDATE) with `decide_approve` — `decision` + `version` bump 검증
//! 4. `find_pending` — `decision is null` 필터링 (decided 후 제외)
//! 5. `find_by_listing` — listing FK 로 Listing Review Queue 조회

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
#![cfg(feature = "integration")]

mod common;

use chrono::Utc;
use gongzzang_persistence::listing::PgListingRepository;
use gongzzang_persistence::listing_review::PgListingReviewRepository;
use gongzzang_persistence::user::PgUserRepository;
use listing_domain::entity::Listing;
use listing_domain::repository::ListingRepository;
use listing_review_domain::decision::ListingReviewDecision;
use listing_review_domain::entity::ListingReview;
use listing_review_domain::repository::{
    ListingReviewRepository, RepoError as ListingReviewRepoError,
};
use shared_kernel::area::AreaM2;
use shared_kernel::description::Description;
use shared_kernel::email::Email;
use shared_kernel::id::{Id, ListingMarker, ListingReviewMarker, UserMarker};
use shared_kernel::listing_title::ListingTitle;
use shared_kernel::listing_type::ListingType;
use shared_kernel::money::MoneyKrw;
use shared_kernel::mutation::MutationContext;
use shared_kernel::pnu::Pnu;
use shared_kernel::transaction_type::TransactionType;
use user_domain::entity::{User, UserKind};
use user_domain::repository::UserRepository;

use common::{setup_test_pool, test_ctx, truncate_all};

/// `User` + `Listing` 시드 — `listing_review_queue.listing_id` `FK` 충족.
async fn seed_listing_with_owner(
    pool: &sqlx::PgPool,
    zsub: &str,
    email: &str,
) -> (Id<UserMarker>, Id<ListingMarker>) {
    let user_repo = PgUserRepository::new(pool.clone());
    let now = Utc::now();
    let owner = User::try_new(
        Id::new(),
        zsub,
        Email::try_new(email).unwrap(),
        "Owner",
        UserKind::Individual,
        now,
    )
    .unwrap();
    let owner_id = owner.id.clone();
    user_repo.save(&owner, test_ctx()).await.unwrap();

    let listing_repo = PgListingRepository::new(pool.clone());
    let listing = Listing::try_new_draft(
        Id::new(),
        owner_id.clone(),
        Pnu::try_new("9999900101100070000").unwrap(),
        ListingType::Factory,
        TransactionType::Sale,
        MoneyKrw::try_new(100_000_000).unwrap(),
        None,
        None,
        AreaM2::try_new(100.00).unwrap(),
        ListingTitle::try_new("listing_review test").unwrap(),
        Description::try_new("").unwrap(),
        now,
    )
    .expect("listing");
    let listing_id = listing.id.clone();
    listing_repo.save(&listing, test_ctx()).await.unwrap();

    (owner_id, listing_id)
}

/// `reviewer_id` `FK` 용 admin 사용자 시드.
async fn seed_admin(pool: &sqlx::PgPool, zsub: &str, email: &str) -> Id<UserMarker> {
    let repo = PgUserRepository::new(pool.clone());
    let now = Utc::now();
    let admin = User::try_new(
        Id::new(),
        zsub,
        Email::try_new(email).unwrap(),
        "Admin",
        UserKind::Individual,
        now,
    )
    .unwrap();
    let admin_id = admin.id.clone();
    repo.save(&admin, test_ctx()).await.unwrap();
    admin_id
}

fn make_listing_review(listing_id: Id<ListingMarker>) -> ListingReview {
    let now = Utc::now();
    ListingReview::try_new_pending(
        Id::<ListingReviewMarker>::new(),
        listing_id,
        Some(80), // auto_check_score
        Some(serde_json::json!(["price_anomaly"])),
        now,
    )
    .expect("listing_review")
}

#[tokio::test]
async fn save_inserts_listing_review_audit_outbox_in_one_tx() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let (owner_id, listing_id) =
        seed_listing_with_owner(&pool, "zsub-listing_review-1", "lrq1@example.com").await;
    let repo = PgListingReviewRepository::new(pool.clone());

    let listing_review = make_listing_review(listing_id);
    let ctx = MutationContext::new_user_action(owner_id, "corr_01HXY3NK0Z9F6S1L01", "create");
    repo.save(&listing_review, ctx).await.expect("save");

    // Listing Review Queue row 1 개
    let listing_review_count: (i64,) =
        sqlx::query_as("select count(*) from listing_review_queue where id = $1")
            .bind(listing_review.id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(listing_review_count.0, 1);

    // audit_log row 1 개 (resource_kind = 'listing_review')
    let audit_count: (i64,) = sqlx::query_as(
        "select count(*) from audit_log where resource_kind = 'listing_review' and resource_id = $1",
    )
    .bind(listing_review.id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_count.0, 1);

    // outbox 0 개 (events 비어 있음)
    let outbox_count: (i64,) = sqlx::query_as("select count(*) from outbox_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outbox_count.0, 0);

    // version 은 1 그대로
    let v: i64 = sqlx::query_scalar("select version from listing_review_queue where id = $1")
        .bind(listing_review.id.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn occ_version_mismatch_rolls_back_audit_log() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let (owner_id, listing_id) =
        seed_listing_with_owner(&pool, "zsub-listing_review-2", "lrq2@example.com").await;
    let repo = PgListingReviewRepository::new(pool.clone());

    // 1) 첫 INSERT — version=1, audit_log 1
    let mut listing_review = make_listing_review(listing_id);
    let ctx =
        MutationContext::new_user_action(owner_id.clone(), "corr_01HXY3NK0Z9F6S1L02", "create");
    repo.save(&listing_review, ctx).await.unwrap();

    let initial_audit: (i64,) =
        sqlx::query_as("select count(*) from audit_log where resource_kind = 'listing_review'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(initial_audit.0, 1);

    // 2) version 강제 변경 — DB 는 1, 호출자가 99 라고 주장 → mismatch
    listing_review.version = 99;
    let ctx2 = MutationContext::new_user_action(owner_id, "corr_01HXY3NK0Z9F6S1L03", "approve");
    let err = repo.save(&listing_review, ctx2).await.unwrap_err();
    assert!(matches!(err, ListingReviewRepoError::Conflict));

    // 3) audit_log 가 그대로 1 — tx rollback 으로 새 audit_log 안 들어감
    let after_audit: (i64,) =
        sqlx::query_as("select count(*) from audit_log where resource_kind = 'listing_review'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(after_audit.0, 1);

    // 4) DB version 도 1 그대로
    let v: i64 = sqlx::query_scalar("select version from listing_review_queue where id = $1")
        .bind(listing_review.id.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn save_with_decision_approve_persists() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let (owner_id, listing_id) =
        seed_listing_with_owner(&pool, "zsub-listing_review-3", "lrq3@example.com").await;
    let admin_id = seed_admin(
        &pool,
        "zsub-listing_review-3-admin",
        "lrq3admin@example.com",
    )
    .await;
    let repo = PgListingReviewRepository::new(pool.clone());

    // 1) 첫 INSERT — version=1
    let mut listing_review = make_listing_review(listing_id);
    let ctx =
        MutationContext::new_user_action(owner_id.clone(), "corr_01HXY3NK0Z9F6S1L04", "create");
    repo.save(&listing_review, ctx).await.unwrap();

    // 2) 도메인 메서드 decide_approve — entity 가 version 을 1 → 2 로 bump
    listing_review
        .decide_approve(admin_id.clone(), Some("looks good".to_owned()), Utc::now())
        .expect("approve");
    assert_eq!(listing_review.version, 2);
    assert_eq!(
        listing_review.decision,
        Some(ListingReviewDecision::Approve)
    );

    // 3) Business Verification Queue T6 패턴: read 시점 version (=1) 으로 OCC 비교. DB UPDATE 가 +1 bump.
    listing_review.version = 1;
    let ctx2 = MutationContext::new_user_action(admin_id, "corr_01HXY3NK0Z9F6S1L05", "approve");
    repo.save(&listing_review, ctx2)
        .await
        .expect("approve save");

    // 4) round-trip 검증 — decision/version DB 반영
    let fetched = repo
        .find_by_id(&listing_review.id)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(fetched.decision, Some(ListingReviewDecision::Approve));
    assert_eq!(fetched.version, 2);
    assert!(fetched.decided_at.is_some());
    assert_eq!(fetched.reviewer_note.as_deref(), Some("looks good"));
}

#[tokio::test]
async fn find_pending_excludes_decided() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let (owner_id, listing_id) =
        seed_listing_with_owner(&pool, "zsub-listing_review-4", "lrq4@example.com").await;
    let admin_id = seed_admin(
        &pool,
        "zsub-listing_review-4-admin",
        "lrq4admin@example.com",
    )
    .await;
    let repo = PgListingReviewRepository::new(pool.clone());

    // 1) pending 상태 INSERT
    let mut listing_review = make_listing_review(listing_id);
    let ctx =
        MutationContext::new_user_action(owner_id.clone(), "corr_01HXY3NK0Z9F6S1L06", "create");
    repo.save(&listing_review, ctx).await.unwrap();

    let pending_before = repo.find_pending(10).await.unwrap();
    assert_eq!(pending_before.len(), 1);
    assert_eq!(pending_before[0].id.as_str(), listing_review.id.as_str());

    // 2) decide_reject (메모 필수)
    listing_review
        .decide_reject(admin_id, "bad listing".to_owned(), Utc::now())
        .expect("reject");
    listing_review.version = 1; // OCC 는 read 시점 version
    let ctx2 = MutationContext::new_user_action(owner_id, "corr_01HXY3NK0Z9F6S1L07", "reject");
    repo.save(&listing_review, ctx2).await.unwrap();

    // 3) find_pending 은 이제 0 — `where decision is null` 필터
    let pending_after = repo.find_pending(10).await.unwrap();
    assert_eq!(pending_after.len(), 0);
}

#[tokio::test]
async fn find_by_listing_returns_listing_review() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let (owner_id, listing_id) =
        seed_listing_with_owner(&pool, "zsub-listing_review-5", "lrq5@example.com").await;
    let repo = PgListingReviewRepository::new(pool.clone());

    let listing_review = make_listing_review(listing_id.clone());
    let ctx = MutationContext::new_user_action(owner_id, "corr_01HXY3NK0Z9F6S1L08", "create");
    repo.save(&listing_review, ctx).await.unwrap();

    let fetched = repo
        .find_by_listing(&listing_id)
        .await
        .unwrap()
        .expect("found by listing");
    assert_eq!(fetched.id.as_str(), listing_review.id.as_str());
    assert_eq!(fetched.listing_id.as_str(), listing_id.as_str());
    assert_eq!(fetched.auto_check_score, Some(80));
    assert!(fetched.decision.is_none());
    assert!(fetched.sla_due_at.is_some());
}
