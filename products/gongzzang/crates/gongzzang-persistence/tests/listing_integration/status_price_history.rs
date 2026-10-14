//! `listing_status_price_history`: one typed row per status or price change (ADR-0221).

use chrono::Utc;
use gongzzang_persistence::listing::PgListingRepository;
use listing_domain::entity::ListingUpdate;
use listing_domain::repository::ListingRepository;
use shared_kernel::listing_title::ListingTitle;
use shared_kernel::money::MoneyKrw;

use super::common::{setup_test_pool, test_ctx, truncate_all};
use super::{make_listing_sale, seed_owner};

#[tokio::test]
async fn each_status_or_price_change_appends_one_row_and_nothing_else_does() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let owner = seed_owner(&pool, "zsub-history-1", "history1@example.com").await;
    let repo = PgListingRepository::new(pool.clone());

    let mut listing = make_listing_sale(owner);
    let first_price = listing.price.as_i64();
    repo.save(&listing, test_ctx()).await.unwrap();

    listing
        .update_editable_fields(
            ListingUpdate {
                price: Some(MoneyKrw::try_new(700_000_000).unwrap()),
                ..Default::default()
            },
            Utc::now(),
        )
        .unwrap();
    repo.save(&listing, test_ctx()).await.unwrap();

    listing
        .update_editable_fields(
            ListingUpdate {
                title: Some(ListingTitle::try_new("title only").unwrap()),
                ..Default::default()
            },
            Utc::now(),
        )
        .unwrap();
    repo.save(&listing, test_ctx()).await.unwrap();

    listing.submit_for_review(Utc::now()).unwrap();
    repo.save(&listing, test_ctx()).await.unwrap();

    let rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "select listing_version, status, price_krw from listing_status_price_history \
         where listing_id = $1 order by history_id",
    )
    .bind(listing.id.as_str())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "draft".to_owned(), first_price),
            (2, "draft".to_owned(), 700_000_000),
            (4, "pending_review".to_owned(), 700_000_000),
        ]
    );
}

#[tokio::test]
async fn history_rows_cannot_be_rewritten() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;
    let owner = seed_owner(&pool, "zsub-history-2", "history2@example.com").await;
    let repo = PgListingRepository::new(pool.clone());
    repo.save(&make_listing_sale(owner), test_ctx())
        .await
        .unwrap();

    let err = sqlx::query("update listing_status_price_history set price_krw = 1")
        .execute(&pool)
        .await
        .expect_err("update must be rejected");
    assert!(err.to_string().contains("append-only"), "{err}");
}
