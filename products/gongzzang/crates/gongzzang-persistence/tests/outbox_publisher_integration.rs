//! `gongzzang_outbox::tick` 통합 테스트 (SP4-i).
//!
//! 4 시나리오:
//! 1. `tick_publishes_unpublished_rows` — 3 row INSERT → tick → 모두 published
//! 2. `tick_skips_already_published` — 이미 published 된 row 는 fetch 안 잡힘
//! 3. `tick_returns_zero_when_no_rows` — 빈 테이블에서 tick → 0
//! 4. `tick_failure_leaves_row_unpublished` — `FailingSink` → row 그대로

// pedantic 광범위 허용: 통합 테스트는 panic-on-bug 가 정상 동작 + DB 접근 시
// must_use return 무시 / similar_names (aggregate_id vs aggregate_kind) 등
// 일상적이라 차단.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::similar_names,
    clippy::let_underscore_must_use
)]
#![cfg(feature = "integration")]

mod common;

use async_trait::async_trait;
use chrono::Utc;
use gongzzang_outbox::{tick, CountingSink, Sink, SinkError};
use gongzzang_persistence::outbox::PgOutboxRepository;
use outbox_event_domain::entity::OutboxEvent;
use outbox_event_domain::repository::OutboxRepository;
use shared_kernel::id::{Id, OutboxEventMarker};
use std::collections::HashSet;
use std::time::Duration;

use common::{setup_test_pool, truncate_all};

/// 시드용 — 단일 outbox event row INSERT.
async fn insert_event(
    pool: &sqlx::PgPool,
    aggregate_id: &str,
    kind: &str,
) -> Id<OutboxEventMarker> {
    let repo = PgOutboxRepository::new(pool.clone());
    let event = OutboxEvent {
        id: Id::<OutboxEventMarker>::new(),
        event_type: format!("{kind}.test_event"),
        aggregate_kind: kind.to_owned(),
        aggregate_id: aggregate_id.to_owned(),
        payload: serde_json::json!({"k": "v"}),
        occurred_at: Utc::now(),
        published_at: None,
        correlation_id: "corr-test".to_owned(),
    };
    let id = event.id.clone();
    repo.save(&event).await.expect("seed save");
    id
}

#[tokio::test]
async fn tick_publishes_unpublished_rows() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let repo = PgOutboxRepository::new(pool.clone());

    insert_event(&pool, "agg-1", "user").await;
    insert_event(&pool, "agg-2", "listing").await;
    insert_event(&pool, "agg-3", "listing_photo").await;

    let sink = CountingSink::new();
    let report = tick(&repo, &sink, 100, "test-worker", Duration::from_mins(1))
        .await
        .expect("tick");

    assert_eq!(report.fetched, 3);
    assert_eq!(report.published, 3);
    assert_eq!(report.failed, 0);
    assert_eq!(sink.count(), 3);

    let published_count: (i64,) =
        sqlx::query_as("select count(*) from outbox_event where published_at is not null")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(published_count.0, 3);
}

#[tokio::test]
async fn tick_skips_already_published() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let repo = PgOutboxRepository::new(pool.clone());

    let already_id = insert_event(&pool, "agg-already", "user").await;
    repo.claim_unpublished(1, "test-worker", Duration::from_mins(1))
        .await
        .expect("claim before mark");
    repo.mark_published(&already_id, "test-worker", Utc::now())
        .await
        .expect("mark");

    insert_event(&pool, "agg-new", "user").await;

    let sink = CountingSink::new();
    let report = tick(&repo, &sink, 100, "test-worker", Duration::from_mins(1))
        .await
        .expect("tick");

    assert_eq!(report.fetched, 1);
    assert_eq!(report.published, 1);
    assert_eq!(sink.count(), 1);

    let published_count: (i64,) =
        sqlx::query_as("select count(*) from outbox_event where published_at is not null")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(published_count.0, 2);
}

#[tokio::test]
async fn tick_returns_zero_when_no_rows() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let repo = PgOutboxRepository::new(pool);

    let sink = CountingSink::new();
    let report = tick(&repo, &sink, 100, "test-worker", Duration::from_mins(1))
        .await
        .expect("tick");

    assert_eq!(report.fetched, 0);
    assert_eq!(report.published, 0);
    assert_eq!(report.failed, 0);
    assert_eq!(sink.count(), 0);
}

/// 항상 실패하는 sink — `tick_failure_leaves_row_unpublished` 시나리오용.
struct FailingSink;

#[async_trait]
impl Sink for FailingSink {
    async fn publish(&self, _event: &OutboxEvent) -> Result<(), SinkError> {
        Err(SinkError::Publish("intentional test failure".to_owned()))
    }
}

#[tokio::test]
async fn tick_failure_leaves_row_unpublished() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let repo = PgOutboxRepository::new(pool.clone());

    insert_event(&pool, "agg-fail", "user").await;

    let sink = FailingSink;
    let report = tick(&repo, &sink, 100, "test-worker", Duration::from_mins(1))
        .await
        .expect("tick");

    assert_eq!(report.fetched, 1);
    assert_eq!(report.published, 0);
    assert_eq!(report.failed, 1);

    // row 는 미발행 그대로
    let unpublished_count: (i64,) =
        sqlx::query_as("select count(*) from outbox_event where published_at is null")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unpublished_count.0, 1);
}

#[tokio::test]
async fn concurrent_workers_claim_each_event_once() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let repo_a = PgOutboxRepository::new(pool.clone());
    let repo_b = PgOutboxRepository::new(pool.clone());

    for index in 0..4 {
        insert_event(&pool, &format!("agg-{index}"), "user").await;
    }

    let (claimed_a, claimed_b) = tokio::join!(
        repo_a.claim_unpublished(2, "worker-a", Duration::from_mins(1)),
        repo_b.claim_unpublished(2, "worker-b", Duration::from_mins(1)),
    );
    let claimed_a = claimed_a.expect("worker a claim");
    let claimed_b = claimed_b.expect("worker b claim");

    let ids: HashSet<String> = claimed_a
        .iter()
        .chain(claimed_b.iter())
        .map(|event| event.id.as_str().to_owned())
        .collect();
    assert_eq!(claimed_a.len() + claimed_b.len(), 4);
    assert_eq!(ids.len(), 4);
}

#[tokio::test]
async fn expired_lease_is_reclaimable_by_another_worker() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let repo_a = PgOutboxRepository::new(pool.clone());
    let repo_b = PgOutboxRepository::new(pool.clone());
    insert_event(&pool, "agg-expiring", "user").await;

    let first_claim = repo_a
        .claim_unpublished(1, "worker-a", Duration::from_secs(1))
        .await
        .expect("first claim");
    assert_eq!(first_claim.len(), 1);

    let blocked_claim = repo_b
        .claim_unpublished(1, "worker-b", Duration::from_mins(1))
        .await
        .expect("claim before expiry");
    assert!(blocked_claim.is_empty());

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let reclaimed = repo_b
        .claim_unpublished(1, "worker-b", Duration::from_mins(1))
        .await
        .expect("claim after expiry");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id.as_str(), first_claim[0].id.as_str());
}
