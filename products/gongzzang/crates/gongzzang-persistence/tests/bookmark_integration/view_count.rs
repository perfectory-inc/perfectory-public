use gongzzang_persistence::listing::PgListingRepository;
use listing_domain::repository::ListingRepository as ListingRepo;
use shared_kernel::id::{Id, ListingMarker};

use super::common::{setup_test_pool, test_ctx, truncate_all};
use super::{seed_active_listing, seed_user};

#[tokio::test]
async fn record_view_counts_each_view() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let owner = seed_user(&pool, "zsub-detail-5", "detail5@example.com").await;
    let viewer = seed_user(&pool, "zsub-detail-5v", "detail5v@example.com").await;
    let listing_id = seed_active_listing(&pool, owner).await;
    let l_repo = PgListingRepository::new(pool.clone());

    l_repo.record_view(&listing_id).await.expect("ok");
    l_repo.record_view(&listing_id).await.expect("ok");

    let detail = l_repo
        .find_detail_by_id(&listing_id, &viewer)
        .await
        .expect("ok")
        .expect("found");
    assert_eq!(detail.view_count, 2);
}

/// The audit's bug: a save wrote back the view count read before it, erasing views
/// recorded in between, and each view moved `updated_at` without a version bump.
#[tokio::test]
async fn a_save_keeps_views_recorded_after_the_aggregate_was_read() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let owner = seed_user(&pool, "zsub-detail-7", "detail7@example.com").await;
    let viewer = seed_user(&pool, "zsub-detail-7v", "detail7v@example.com").await;
    let listing_id = seed_active_listing(&pool, owner).await;
    let l_repo = PgListingRepository::new(pool.clone());

    let mut stale = l_repo.find(&listing_id).await.expect("ok").expect("found");
    l_repo.record_view(&listing_id).await.expect("ok");
    l_repo.record_view(&listing_id).await.expect("ok");
    l_repo.record_view(&listing_id).await.expect("ok");

    let after_views = l_repo.find(&listing_id).await.expect("ok").expect("found");
    assert_eq!(
        after_views.updated_at, stale.updated_at,
        "a view is not an aggregate change"
    );
    assert_eq!(after_views.version, stale.version);

    stale.version += 1;
    l_repo.save(&stale, test_ctx()).await.expect("save");

    let detail = l_repo
        .find_detail_by_id(&listing_id, &viewer)
        .await
        .expect("ok")
        .expect("found");
    assert_eq!(
        detail.view_count, 3,
        "save must not overwrite recorded views"
    );
}

#[tokio::test]
async fn view_events_are_append_only() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let owner = seed_user(&pool, "zsub-detail-8", "detail8@example.com").await;
    let listing_id = seed_active_listing(&pool, owner).await;
    let l_repo = PgListingRepository::new(pool.clone());
    l_repo.record_view(&listing_id).await.expect("ok");

    let err = sqlx::query("update listing_view_event set viewed_at = now()")
        .execute(&pool)
        .await
        .expect_err("update must be rejected");
    assert!(err.to_string().contains("append-only"), "{err}");
}

#[tokio::test]
async fn record_view_nonexistent_returns_not_found() {
    use listing_domain::repository::RepoError as ListingRepoError;

    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let l_repo = PgListingRepository::new(pool);

    let fake_id = Id::<ListingMarker>::new();
    let err = l_repo.record_view(&fake_id).await.expect_err("not found");
    assert!(matches!(err, ListingRepoError::NotFound));
}
