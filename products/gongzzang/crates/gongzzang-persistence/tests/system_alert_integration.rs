//! `PgSystemAlertRepository` 통합 테스트.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
#![cfg(feature = "integration")]

mod common;

use chrono::Utc;
use gongzzang_persistence::system_alert::PgSystemAlertRepository;
use gongzzang_persistence::user::PgUserRepository;
use serde_json::json;
use shared_kernel::email::Email;
use shared_kernel::id::{Id, UserMarker};
use shared_kernel::mutation::MutationContext;
use system_alert_domain::repository::SystemAlertRepository;
use system_alert_domain::{SystemAlert, SystemAlertSeverity};
use user_domain::entity::{User, UserKind};
use user_domain::repository::UserRepository;

use common::{setup_test_pool, test_ctx, truncate_all};

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

fn make_alert(severity: SystemAlertSeverity, source: &str) -> SystemAlert {
    SystemAlert::try_new(
        severity,
        source,
        "Test alert title",
        Some("alert detail"),
        json!({}),
        Utc::now(),
    )
    .expect("valid alert")
}

#[tokio::test]
async fn save_records_system_alert_and_audit_metadata_in_one_transaction() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let admin = seed_admin(&pool, "zsub-alert-1", "alert1@example.com").await;
    let repo = PgSystemAlertRepository::new(pool.clone());
    let alert = make_alert(SystemAlertSeverity::Error, "pipeline.parcel_sync");
    let metadata = json!({"reason": "vworld_timeout"});

    repo.save(
        &alert,
        MutationContext::new_user_action(admin, "corr_01HXY8RRPT4F8S1L05", "create")
            .with_metadata(metadata.clone()),
    )
    .await
    .unwrap();

    let after_state: Option<serde_json::Value> = sqlx::query_scalar(
        "select after_state from audit_log where resource_kind = 'system_alert' and resource_id = $1",
    )
    .bind(alert.id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after_state, Some(metadata));

    let fetched = repo.find_by_id(&alert.id).await.unwrap().expect("present");
    assert_eq!(fetched.source, "pipeline.parcel_sync");
    assert_eq!(fetched.detail.as_deref(), Some("alert detail"));
}

#[tokio::test]
async fn find_unacknowledged_excludes_acknowledged_and_orders_by_severity() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let admin = seed_admin(&pool, "zsub-alert-2", "alert2@example.com").await;
    let repo = PgSystemAlertRepository::new(pool.clone());

    for severity in [
        SystemAlertSeverity::Warning,
        SystemAlertSeverity::Critical,
        SystemAlertSeverity::Info,
    ] {
        repo.save(
            &make_alert(severity, "test_source"),
            MutationContext::new_user_action(admin.clone(), "corr_01HXY8RRPT4F8S1L06", "create"),
        )
        .await
        .unwrap();
    }

    sqlx::query(
        "update system_alert set acknowledged_at = now(), acknowledged_by = $1 where severity = 'info'",
    )
    .bind(admin.as_str())
    .execute(&pool)
    .await
    .unwrap();

    let unacknowledged = repo.find_unacknowledged(10).await.unwrap();
    assert_eq!(unacknowledged.len(), 2);
    assert_eq!(unacknowledged[0].severity, SystemAlertSeverity::Critical);
    assert_eq!(unacknowledged[1].severity, SystemAlertSeverity::Warning);
}

#[tokio::test]
async fn save_without_events_does_not_create_outbox_event() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let admin = seed_admin(&pool, "zsub-alert-3", "alert3@example.com").await;
    let repo = PgSystemAlertRepository::new(pool.clone());

    repo.save(
        &make_alert(SystemAlertSeverity::Warning, "test"),
        MutationContext::new_user_action(admin, "corr_01HXY8RRPT4F8S1L07", "create"),
    )
    .await
    .unwrap();

    let outbox_count: i64 = sqlx::query_scalar("select count(*) from outbox_event")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outbox_count, 0);
}
