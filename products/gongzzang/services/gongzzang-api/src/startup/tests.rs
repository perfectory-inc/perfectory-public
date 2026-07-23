#![allow(clippy::expect_used)]

use chrono::Utc;
use product_identity_infrastructure::verifier::Verifier;

use crate::photo_upload::ListingPhotoUploadConfigError;

use super::{
    build_building_reader_from_foundation_platform_base_url,
    build_parcel_lookup_from_foundation_platform_base_url,
    build_photo_download_issuer_from_config_result, build_photo_object_verifier_from_config_result,
    build_photo_upload_issuer_from_config_result, build_verifier, required_env, StartupError,
};

#[test]
fn required_env_returns_typed_error_when_missing() {
    const NAME: &str = "GONGZZANG_TEST_REQUIRED_ENV";
    std::env::remove_var(NAME);

    let result = required_env(NAME);

    assert!(matches!(result, Err(StartupError::MissingEnv { name }) if name == NAME));
}

#[test]
fn production_rejects_auth_dev_mode() {
    let result = build_verifier(true, true);

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason }) if reason.contains("AUTH_DEV_MODE"))
    );
}

#[test]
fn non_production_allows_auth_dev_mode() {
    let result = build_verifier(true, false);

    assert!(result.is_ok(), "expected dev verifier");
    if let Ok(verifier) = result {
        assert!(matches!(verifier.as_ref(), Verifier::Dev));
    }
}

#[test]
fn production_rejects_missing_listing_photo_upload_r2_config() {
    let result = build_photo_upload_issuer_from_config_result(
        true,
        Err(ListingPhotoUploadConfigError::MissingEnv(
            "LISTING_PHOTO_R2_BUCKET",
        )),
    );

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("listing photo upload")
                && reason.contains("LISTING_PHOTO_R2_BUCKET"))
    );
}

#[test]
fn production_rejects_missing_listing_photo_object_verifier_r2_config() {
    let result = build_photo_object_verifier_from_config_result(
        true,
        Err(ListingPhotoUploadConfigError::MissingEnv(
            "LISTING_PHOTO_R2_BUCKET",
        )),
    );

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("listing photo object verifier")
                && reason.contains("LISTING_PHOTO_R2_BUCKET"))
    );
}

#[test]
fn non_production_allows_disabled_listing_photo_object_verifier() {
    let result = build_photo_object_verifier_from_config_result(
        false,
        Err(ListingPhotoUploadConfigError::MissingEnv(
            "LISTING_PHOTO_R2_BUCKET",
        )),
    );

    assert!(result.is_ok());
}

#[test]
fn production_rejects_missing_foundation_building_base_url() {
    let result = build_building_reader_from_foundation_platform_base_url(true, None, None);

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("FOUNDATION_PLATFORM_API_BASE_URL"))
    );
}

#[test]
fn non_production_allows_missing_foundation_building_base_url() {
    let result = build_building_reader_from_foundation_platform_base_url(false, None, None);

    assert!(result.is_ok());
}

#[test]
fn production_accepts_foundation_building_workload_identity_token_file() {
    let token_file = write_workload_identity_token_file("zitadel-workload-token-32-valid");
    let result = build_building_reader_from_foundation_platform_base_url(
        true,
        Some("http://127.0.0.1:18080".to_owned()),
        Some(token_file.to_string_lossy().into_owned()),
    );

    let _ = std::fs::remove_file(token_file);
    assert!(result.is_ok());
}

#[test]
fn production_rejects_missing_foundation_workload_identity_for_building_reader() {
    let result = build_building_reader_from_foundation_platform_base_url(
        true,
        Some("http://127.0.0.1:18080".to_owned()),
        None,
    );

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE"))
    );
}

#[test]
fn production_rejects_missing_foundation_parcel_base_url() {
    let result = build_parcel_lookup_from_foundation_platform_base_url(true, None, None);

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("FOUNDATION_PLATFORM_API_BASE_URL"))
    );
}

#[test]
fn non_production_allows_missing_foundation_parcel_base_url() {
    let result = build_parcel_lookup_from_foundation_platform_base_url(false, None, None);

    assert!(result.is_ok());
}

#[test]
fn production_accepts_foundation_parcel_workload_identity_token_file() {
    let token_file = write_workload_identity_token_file("zitadel-workload-token-32-valid");
    let result = build_parcel_lookup_from_foundation_platform_base_url(
        true,
        Some("http://127.0.0.1:18080".to_owned()),
        Some(token_file.to_string_lossy().into_owned()),
    );

    let _ = std::fs::remove_file(token_file);
    assert!(result.is_ok());
}

#[test]
fn production_rejects_missing_foundation_workload_identity_for_parcel_lookup() {
    let result = build_parcel_lookup_from_foundation_platform_base_url(
        true,
        Some("http://127.0.0.1:18080".to_owned()),
        None,
    );

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE"))
    );
}

#[test]
fn non_production_rejects_foundation_base_url_without_workload_identity() {
    let parcel = build_parcel_lookup_from_foundation_platform_base_url(
        false,
        Some("http://127.0.0.1:18080".to_owned()),
        None,
    );
    let building = build_building_reader_from_foundation_platform_base_url(
        false,
        Some("http://127.0.0.1:18080".to_owned()),
        None,
    );

    assert!(
        matches!(parcel, Err(StartupError::ProductionConfig { reason })
        if reason.contains("FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE"))
    );
    assert!(
        matches!(building, Err(StartupError::ProductionConfig { reason })
        if reason.contains("FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE"))
    );
}

#[test]
fn production_rejects_missing_listing_photo_download_r2_config() {
    let result = build_photo_download_issuer_from_config_result(
        true,
        Err(ListingPhotoUploadConfigError::MissingEnv(
            "LISTING_PHOTO_R2_BUCKET",
        )),
    );

    assert!(
        matches!(result, Err(StartupError::ProductionConfig { reason })
            if reason.contains("listing photo download")
                && reason.contains("LISTING_PHOTO_R2_BUCKET"))
    );
}

fn write_workload_identity_token_file(token: &str) -> std::path::PathBuf {
    let token_file = std::env::temp_dir().join(format!(
        "gongzzang-startup-token-{}-{}.txt",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::write(&token_file, token).expect("write workload identity token file");
    token_file
}
