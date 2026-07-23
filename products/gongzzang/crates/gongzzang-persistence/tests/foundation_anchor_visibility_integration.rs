//! Visibility boundary integration tests for Foundation Platform anchor imports.

#![allow(clippy::expect_used, clippy::unwrap_used)]
#![cfg(feature = "integration")]

mod common;

use chrono::Utc;
use gongzzang_persistence::foundation_anchor::{AnchorArtifactRow, FoundationPlatformAnchorImport};

use common::{setup_test_pool, truncate_all};

#[tokio::test]
async fn private_listing_anchor_import_never_creates_public_marker_artifacts() {
    let pool = setup_test_pool().await;
    truncate_all(&pool).await;

    let pnu = "9999900101100090000";
    let listing_id = "lst_01HXY3NK0Z9F6S1B2C3D4E5F6H";
    sqlx::query(
        r#"
        insert into "user" (
            id, zitadel_sub, email, display_name, user_kind, created_at, updated_at, version
        ) values (
            'usr_01HXY3NK0Z9F6S1B2C3D4E5F6H',
            'zsub-private-anchor-import',
            'private-anchor-import@example.com',
            'Private Anchor Import Owner',
            'individual',
            now(),
            now(),
            1
        )
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        r"
        insert into listing (
            id, owner_id, parcel_pnu, status, listing_type, transaction_type, price_krw, area_m2,
            title, description, created_at, updated_at, version
        ) values (
            $1,
            'usr_01HXY3NK0Z9F6S1B2C3D4E5F6H',
            $2,
            'draft',
            'factory',
            'sale',
            500000000,
            330.58,
            'Private anchor imported listing',
            'must not enter public marker artifacts',
            now(),
            now(),
            1
        )
        ",
    )
    .bind(listing_id)
    .bind(pnu)
    .execute(&pool)
    .await
    .unwrap();

    let report = gongzzang_persistence::foundation_anchor::import_anchor_rows(
        &pool,
        &FoundationPlatformAnchorImport {
            anchor_snapshot_id: "anchor-snapshot-private".to_owned(),
            source_geometry_version: "silver.parcel_boundaries@private".to_owned(),
            foundation_platform_updated_at: Utc::now(),
            rows: vec![AnchorArtifactRow {
                pnu: pnu.to_owned(),
                anchor_lng: 127.123_470_234_50,
                anchor_lat: 36.123_456_5,
                algorithm: "polylabel".to_owned(),
                algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
                source_geometry_checksum_sha256: "d".repeat(64),
            }],
        },
    )
    .await
    .unwrap();

    assert_eq!(report.upserted_anchor_count, 1);
    assert_eq!(report.refreshed_listing_projection_count, 0);
    assert_eq!(report.inserted_delta_count, 0);
    assert_eq!(report.inserted_dirty_tile_count, 0);

    let projection_count: i64 =
        sqlx::query_scalar("select count(*) from listing_marker_projection where listing_id = $1")
            .bind(listing_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let delta_count: i64 =
        sqlx::query_scalar("select count(*) from listing_marker_delta_log where listing_id = $1")
            .bind(listing_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let dirty_tile_count: i64 =
        sqlx::query_scalar("select count(*) from listing_marker_dirty_tile_queue")
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(projection_count, 0);
    assert_eq!(delta_count, 0);
    assert_eq!(dirty_tile_count, 0);
}
