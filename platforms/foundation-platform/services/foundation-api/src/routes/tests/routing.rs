use super::*;

#[tokio::test]
async fn router_allows_gongzzang_manifest_preflight() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/catalog/v1/vector-tiles/manifest")
                .header(header::ORIGIN, "http://localhost:3000")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&HeaderValue::from_static("http://localhost:3000"))
    );
    Ok(())
}

#[tokio::test]
async fn router_allows_gongzzang_preview_preflights() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    for uri in [
        "/catalog/v1/vector-tiles/manifest",
        "/map/v1/marker-tiles/contract",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri(uri)
                    .header(header::ORIGIN, "http://localhost:3900")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://localhost:3900"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn router_routes_marker_tile_pbf_requests() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/map/v1/marker-tiles/listing/0/0/0.pbf?filter_hash=all-active-v1")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn router_routes_complex_anchor_summary_requests() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/catalog/v1/complexes/not-a-uuid/anchor-summary")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn router_routes_parcel_marker_anchor_rebuild_requests() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/catalog/v1/parcel-marker-anchors:rebuild")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{
                        "source_snapshot_id":"iceberg:parcel-boundary-snapshot-20260522",
                        "algorithm_version":"postgis-st_maximuminscribedcircle-v1"
                    }"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_parcel_pnu_lookup_without_service_identity() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/catalog/v1/parcels/by-pnu/not-a-pnu")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_rejects_lakehouse_artifact_registration_without_service_identity(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/internal/lakehouse/artifacts")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{
                        "qualified_name":"gongzzang.gold.listing_photo_media",
                        "namespace_id":"gongzzang_r2_production",
                        "object_key":"media/listing-photo/listings/lst_1/photos/lph_1.webp",
                        "content_type":"image/webp",
                        "checksum_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "size_bytes":2048,
                        "logical_record_count":null
                    }"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn router_routes_lakehouse_artifact_registration_after_service_identity(
) -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test_with_identity_authorization(
        service_identity_authorization(),
    )?);
    let app = router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/internal/lakehouse/artifacts")
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::AUTHORIZATION,
                    "Bearer foundation-platform-gongzzang-worker-token-32-valid",
                )
                .body(Body::from(
                    r#"{
                        "qualified_name":"gongzzang.gold.listing_photo_media",
                        "namespace_id":"gongzzang_r2_production",
                        "object_key":"gold/listing-marker-tiles/0196e7e0-3c20-7000-8000-200000000001/0/0/0.pbf",
                        "content_type":"application/x-protobuf",
                        "checksum_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "size_bytes":2048,
                        "logical_record_count":null
                    }"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn router_preserves_lakehouse_artifact_success_contract() -> Result<(), Box<dyn Error>> {
    let unit_of_work = Arc::new(RecordingLakehouseRegistryUnitOfWork::default());
    let state = Arc::new(
        AppState::bootstrap_for_test_with_lakehouse_uow_and_identity_authorization(
            unit_of_work.clone(),
            service_identity_authorization(),
        )?,
    );
    let app = router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/internal/lakehouse/artifacts")
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::AUTHORIZATION,
                    "Bearer foundation-platform-gongzzang-worker-token-32-valid",
                )
                .body(Body::from(
                    r#"{
                        "qualified_name":"gongzzang.gold.listing_photo_media",
                        "namespace_id":"gongzzang_r2_production",
                        "object_key":"media/listing-photo/listings/lst_1/photos/lph_1.webp",
                        "content_type":"image/webp",
                        "checksum_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "size_bytes":2048,
                        "logical_record_count":null
                    }"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        payload,
        serde_json::json!({
            "artifact_id": "artifact-1",
            "qualified_name": "gongzzang.gold.listing_photo_media",
            "object_key": "media/listing-photo/listings/lst_1/photos/lph_1.webp"
        })
    );
    let commands = unit_of_work.commands.lock().await;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].dataset_version, "append_only_v1");
    drop(commands);
    Ok(())
}

#[tokio::test]
async fn router_allows_manifest_rollback_preflight() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/catalog/v1/vector-tiles/manifest:rollback")
                .header(header::ORIGIN, "http://localhost:3000")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&HeaderValue::from_static("http://localhost:3000"))
    );
    Ok(())
}

#[tokio::test]
async fn router_allows_manifest_promote_preflight() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(AppState::bootstrap_for_test()?);
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/catalog/v1/vector-tiles/manifest:promote")
                .header(header::ORIGIN, "http://localhost:3000")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "PUT")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&HeaderValue::from_static("http://localhost:3000"))
    );
    Ok(())
}
