//! `PgBusinessVerificationRepository` 통합 테스트 — OCC + transactional `audit_log`/`outbox_event`
//! 패턴 (SP5-iii T6).
//!
//! 5 시나리오:
//! 1. `save` (INSERT) — Business Verification Queue + `audit_log` 1행, outbox 0
//! 2. `save` with events — `outbox_event` 행 생성
//! 3. OCC 버전 불일치 → `Conflict` + tx rollback (`audit_log` 미증가)
//! 4. system action — `actor_id` `NULL` 로 기록
//! 5. `save` (UPDATE) — 도메인 메서드로 `version` bump 후 DB 에 반영 검증

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;

use business_verification_domain::entity::BusinessVerification;
use business_verification_domain::repository::{
    BusinessVerificationRepository, RepoError as BusinessVerificationRepoError,
};
use chrono::{DateTime, Utc};
use gongzzang_persistence::business_verification::PgBusinessVerificationRepository;
use gongzzang_persistence::user::PgUserRepository;
use shared_kernel::business_number::BusinessNumber;
use shared_kernel::domain_event::DomainEvent;
use shared_kernel::email::Email;
use shared_kernel::id::{BusinessVerificationMarker, Id, UserMarker};
use shared_kernel::mutation::MutationContext;
use user_domain::entity::{User, UserKind};
use user_domain::repository::UserRepository;

use common::{setup_test_pool, test_ctx, truncate_all};

/// 테스트용 단순 도메인 이벤트.
#[derive(Debug)]
struct TestEvent {
    event_type: &'static str,
    aggregate_id: String,
    payload: serde_json::Value,
    occurred_at: DateTime<Utc>,
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> &'static str {
        self.event_type
    }
    fn aggregate_id(&self) -> String {
        self.aggregate_id.clone()
    }
    fn payload(&self) -> serde_json::Value {
        self.payload.clone()
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}

/// 사용자 1명 시드 후 `id` 반환.
async fn seed_user(pool: &sqlx::PgPool, zsub: &str, email: &str) -> Id<UserMarker> {
    let repo = PgUserRepository::new(pool.clone());
    let now = Utc::now();
    let user = User::try_new(
        Id::new(),
        zsub,
        Email::try_new(email).unwrap(),
        "User",
        UserKind::Individual,
        now,
    )
    .unwrap();
    let user_id = user.id.clone();
    repo.save(&user, test_ctx()).await.unwrap();
    user_id
}

fn make_business_verification(user_id: Id<UserMarker>) -> BusinessVerification {
    let now = Utc::now();
    BusinessVerification::try_new_pending(
        Id::<BusinessVerificationMarker>::new(),
        user_id,
        // VALID checksum number — see shared_kernel::business_number tests
        BusinessNumber::try_new("123-45-67891").expect("valid bn"),
        serde_json::json!({"document_keys": ["business_verification/abc/biz_reg.pdf"]}),
        now,
    )
}

#[tokio::test]
async fn save_inserts_business_verification_audit_outbox_in_one_tx() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-business_verification-1", "bvq1@example.com").await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());

    let business_verification = make_business_verification(user_id.clone());
    let ctx = MutationContext::new_user_action(user_id, "corr_01HXY3NK0Z9F6S1B6", "create");
    repo.save(&business_verification, ctx).await.expect("save");

    // Business Verification Queue row 1 개
    let business_verification_count: (i64,) =
        sqlx::query_as("select count(*) from business_verification_queue where id = $1")
            .bind(business_verification.id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(business_verification_count.0, 1);

    // audit_log row 1 개 (resource_kind = 'business_verification')
    let audit_count: (i64,) = sqlx::query_as(
        "select count(*) from audit_log where resource_kind = 'business_verification' and resource_id = $1",
    )
    .bind(business_verification.id.as_str())
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
    let v: i64 =
        sqlx::query_scalar("select version from business_verification_queue where id = $1")
            .bind(business_verification.id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn save_with_events_creates_outbox_in_same_tx() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-business_verification-2", "bvq2@example.com").await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());

    let business_verification = make_business_verification(user_id.clone());

    let event1: Arc<dyn DomainEvent> = Arc::new(TestEvent {
        event_type: "business_verification.submitted",
        aggregate_id: business_verification.id.as_str().to_owned(),
        payload: serde_json::json!({"user_id": user_id.as_str()}),
        occurred_at: Utc::now(),
    });
    let event2: Arc<dyn DomainEvent> = Arc::new(TestEvent {
        event_type: "business_verification.notification_sent",
        aggregate_id: business_verification.id.as_str().to_owned(),
        payload: serde_json::json!({}),
        occurred_at: Utc::now(),
    });

    let ctx = MutationContext::new_user_action(user_id, "corr_01HXY3NK0Z9F6S1B7", "create")
        .with_events(vec![event1, event2]);
    repo.save(&business_verification, ctx).await.expect("save");

    let outbox_count: (i64,) = sqlx::query_as(
        "select count(*) from outbox_event \
         where aggregate_kind = 'business_verification' and aggregate_id = $1",
    )
    .bind(business_verification.id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox_count.0, 2);

    // published_at 은 NULL 로 들어가야
    let unpublished: (i64,) =
        sqlx::query_as("select count(*) from outbox_event where published_at is null")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unpublished.0, 2);
}

#[tokio::test]
async fn occ_version_mismatch_rolls_back_audit_log() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-business_verification-3", "bvq3@example.com").await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());

    // 1) 첫 INSERT — version=1, audit_log 1
    let mut business_verification = make_business_verification(user_id.clone());
    let ctx = MutationContext::new_user_action(user_id.clone(), "corr_01HXY3NK0Z9F6S1B8", "create");
    repo.save(&business_verification, ctx).await.unwrap();

    let initial_audit: (i64,) = sqlx::query_as(
        "select count(*) from audit_log where resource_kind = 'business_verification'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(initial_audit.0, 1);

    // 2) version 강제 변경 — DB 는 1, 호출자가 99 라고 주장 → mismatch
    business_verification.version = 99;
    let ctx2 = MutationContext::new_user_action(user_id, "corr_01HXY3NK0Z9F6S1B9", "approve");
    let err = repo.save(&business_verification, ctx2).await.unwrap_err();
    assert!(matches!(err, BusinessVerificationRepoError::Conflict));

    // 3) audit_log 가 그대로 1 — tx rollback 으로 새 audit_log 안 들어감
    let after_audit: (i64,) = sqlx::query_as(
        "select count(*) from audit_log where resource_kind = 'business_verification'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after_audit.0, 1);

    // 4) DB version 도 1 그대로
    let v: i64 =
        sqlx::query_scalar("select version from business_verification_queue where id = $1")
            .bind(business_verification.id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn save_system_action_records_null_actor() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-business_verification-4", "bvq4@example.com").await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());

    let business_verification = make_business_verification(user_id);
    let ctx = MutationContext::new_system_action("corr_01HXY3NK0Z9F6S1BA", "create");
    repo.save(&business_verification, ctx).await.expect("save");

    let null_actor_count: (i64,) = sqlx::query_as(
        "select count(*) from audit_log where resource_kind = 'business_verification' and actor_id is null",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(null_actor_count.0, 1);
}

#[tokio::test]
async fn update_bumps_version_in_db() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let user_id = seed_user(&pool, "zsub-business_verification-5", "bvq5@example.com").await;
    // approve 의 reviewer_id 는 user FK — admin 도 함께 시드해야 FK 통과
    let admin_id = seed_user(
        &pool,
        "zsub-business_verification-5-admin",
        "bvq5admin@example.com",
    )
    .await;
    let repo = PgBusinessVerificationRepository::new(pool.clone());

    // 1) 첫 INSERT — version=1
    let mut business_verification = make_business_verification(user_id.clone());
    let ctx = MutationContext::new_user_action(user_id.clone(), "corr_01HXY3NK0Z9F6S1BB", "create");
    repo.save(&business_verification, ctx).await.unwrap();

    let v_after_insert: i64 =
        sqlx::query_scalar("select version from business_verification_queue where id = $1")
            .bind(business_verification.id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(v_after_insert, 1);

    // 2) 도메인 메서드 approve — entity 가 version 을 1 → 2 로 bump
    business_verification
        .approve(admin_id, None, Utc::now())
        .expect("approve");
    assert_eq!(business_verification.version, 2);

    // 3) OCC 는 *읽었던* version (=1) 으로 비교해야 함. 기존 T2/T3 (user/listing)
    //    update_bumps_version 패턴 그대로: `business_verification.version = 1` 로 되돌리고 save.
    //    DB 의 UPDATE 가 +1 bump 해서 결과적으로 2 가 됨.
    //
    //    실제 application layer 도 read 시점의 version 을 보존했다가 OCC 에 사용해야 함
    //    — spec FU 후보 (BusinessVerificationRepository::save 에 expected_version 명시 인자).
    business_verification.version = 1;
    let ctx2 = MutationContext::new_user_action(user_id, "corr_01HXY3NK0Z9F6S1BC", "approve");
    repo.save(&business_verification, ctx2)
        .await
        .expect("approve save");

    let v_after_update: i64 =
        sqlx::query_scalar("select version from business_verification_queue where id = $1")
            .bind(business_verification.id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(v_after_update, 2); // DB UPDATE 가 +1 bump
}
