//! `PgFeaturedContentRepository` 통합 테스트.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
#![cfg(feature = "integration")]

mod common;

use chrono::{Duration, Utc};
use featured_content_domain::repository::FeaturedContentRepository;
use featured_content_domain::{
    FeaturedContent, FeaturedContentFeatureKind, FeaturedContentTargetKind,
};
use gongzzang_persistence::featured_content::PgFeaturedContentRepository;
use gongzzang_persistence::user::PgUserRepository;
use shared_kernel::email::Email;
use shared_kernel::id::{Id, UserMarker};
use shared_kernel::mutation::MutationContext;
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

fn make_featured(
    target_id: &str,
    feature_kind: FeaturedContentFeatureKind,
    weight: i32,
    starts_offset_secs: i64,
    duration_secs: i64,
) -> FeaturedContent {
    let now = Utc::now();
    let starts_at = now + Duration::seconds(starts_offset_secs);
    FeaturedContent::try_new(
        FeaturedContentTargetKind::Listing,
        target_id,
        feature_kind,
        weight,
        starts_at,
        starts_at + Duration::seconds(duration_secs),
        None,
        now,
    )
    .expect("valid featured content")
}

#[tokio::test]
async fn save_inserts_featured_content_and_audit_in_one_transaction() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let admin = seed_admin(&pool, "zsub-featured-1", "featured1@example.com").await;
    let repo = PgFeaturedContentRepository::new(pool.clone());
    let featured_content = make_featured(
        "lst_test123",
        FeaturedContentFeatureKind::HomepageFeatured,
        10,
        -3600,
        86_400,
    );

    repo.save(
        &featured_content,
        MutationContext::new_user_action(admin, "corr_01HXY8RRPT4F8S1L01", "create"),
    )
    .await
    .unwrap();

    let row_count: i64 = sqlx::query_scalar("select count(*) from featured_content where id = $1")
        .bind(featured_content.id.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row_count, 1);

    let audit_count: i64 = sqlx::query_scalar(
        "select count(*) from audit_log where resource_kind = 'featured_content' and resource_id = $1",
    )
    .bind(featured_content.id.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_count, 1);

    let fetched = repo
        .find_by_id(&featured_content.id)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(fetched.target_id, "lst_test123");
    assert_eq!(fetched.weight, 10);
}

#[tokio::test]
async fn find_active_filters_by_time_window_and_feature_kind() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let admin = seed_admin(&pool, "zsub-featured-2", "featured2@example.com").await;
    let repo = PgFeaturedContentRepository::new(pool);

    for featured_content in [
        make_featured(
            "lst_active",
            FeaturedContentFeatureKind::HomepageFeatured,
            5,
            -3600,
            7200,
        ),
        make_featured(
            "lst_future",
            FeaturedContentFeatureKind::HomepageFeatured,
            5,
            7200,
            3600,
        ),
        make_featured(
            "lst_other_kind",
            FeaturedContentFeatureKind::SearchTop,
            99,
            -3600,
            7200,
        ),
    ] {
        repo.save(
            &featured_content,
            MutationContext::new_user_action(admin.clone(), "corr_01HXY8RRPT4F8S1L02", "create"),
        )
        .await
        .unwrap();
    }

    let results = repo
        .find_active(FeaturedContentFeatureKind::HomepageFeatured, Utc::now())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].target_id, "lst_active");
}
