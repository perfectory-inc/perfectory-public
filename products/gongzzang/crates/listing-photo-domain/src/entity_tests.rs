//! `ListingPhoto` Aggregate 단위 테스트 — `entity.rs`/`errors.rs` 동작 검증.
//!
//! `entity.rs`에서 `#[path = "entity_tests.rs"] mod tests;` 형태로 포함해요.
//! 파일 자체가 테스트 모듈이므로 별도 `mod tests {}` 래퍼 없어요.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::{DateTime, TimeZone, Utc};
use shared_kernel::id::{Id, ListingMarker, ListingPhotoMarker};

use shared_kernel::media::{MediaFormat, Sha256Hex};

use super::{upload_format, ListingPhoto};
use crate::errors::ListingPhotoError;

// ── Fixtures ───────────────────────────────────────────────────────────────

fn sample_now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 2, 12, 0, 0).unwrap()
}

fn later_now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 3, 9, 30, 0).unwrap()
}

fn sample_id() -> Id<ListingPhotoMarker> {
    Id::<ListingPhotoMarker>::new()
}

fn sample_listing_id() -> Id<ListingMarker> {
    Id::<ListingMarker>::new()
}

const SAMPLE_R2_KEY: &str =
    "media/listing-photo/listings/lst_01HXY3NK0Z9F6S1B2C3D4E5F6G/photos/p1.jpg";
fn sample_sha() -> Sha256Hex {
    Sha256Hex::parse(&"0123456789abcdef".repeat(4)).unwrap()
}

/// 모든 `Some` 필드 + `Jpeg`로 happy path 빌드.
fn build_full() -> ListingPhoto {
    ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        Some(sample_sha()),
        Some("정문에서 본 외관"),
        0,
        Some(1920),
        Some(1080),
        Some(524_288),
        MediaFormat::Jpeg,
        sample_now(),
    )
    .expect("full happy path valid")
}

fn build_pending() -> ListingPhoto {
    ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        None,
        0,
        None,
        None,
        None,
        MediaFormat::Jpeg,
        sample_now(),
    )
    .expect("pending photo metadata valid")
}

// ── try_new happy paths ────────────────────────────────────────────────────

#[test]
fn try_new_full_fields_succeeds() {
    let photo = build_full();
    assert_eq!(photo.r2_key, SAMPLE_R2_KEY);
    assert_eq!(photo.content_sha256, Some(sample_sha()));
    assert_eq!(photo.caption.as_deref(), Some("정문에서 본 외관"));
    assert_eq!(photo.display_order, 0);
    assert_eq!(photo.width_px, Some(1920));
    assert_eq!(photo.height_px, Some(1080));
    assert_eq!(photo.file_size_bytes, Some(524_288));
    assert_eq!(photo.content_type, MediaFormat::Jpeg);
    assert_eq!(photo.uploaded_at, sample_now());
    assert!(photo.deleted_at.is_none());
}

#[test]
fn try_new_all_optionals_none_succeeds() {
    let photo = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        None,
        5,
        None,
        None,
        None,
        MediaFormat::Png,
        sample_now(),
    )
    .expect("optionals None is valid");
    assert!(photo.content_sha256.is_none());
    assert!(photo.caption.is_none());
    assert!(photo.width_px.is_none());
    assert!(photo.height_px.is_none());
    assert!(photo.file_size_bytes.is_none());
    assert_eq!(photo.display_order, 5);
}

#[test]
fn all_optionals_none_is_pending_upload() {
    let photo = build_pending();
    assert!(!photo.is_upload_confirmed());
}

#[test]
fn try_new_all_three_content_types_succeed() {
    for ct in [MediaFormat::Jpeg, MediaFormat::Png, MediaFormat::Webp] {
        let photo = ListingPhoto::try_new(
            sample_id(),
            sample_listing_id(),
            SAMPLE_R2_KEY,
            None,
            None,
            0,
            None,
            None,
            None,
            ct,
            sample_now(),
        )
        .expect("each content type valid");
        assert_eq!(photo.content_type, ct);
    }
}

#[test]
fn try_new_zero_display_order_accepted() {
    let photo = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        None,
        0,
        None,
        None,
        None,
        MediaFormat::Jpeg,
        sample_now(),
    )
    .expect("0 is non-negative");
    assert_eq!(photo.display_order, 0);
}

#[test]
fn try_new_caption_exactly_200_chars_accepted() {
    let caption: String = "가".repeat(200);
    let photo = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        Some(caption.as_str()),
        0,
        None,
        None,
        None,
        MediaFormat::Webp,
        sample_now(),
    )
    .expect("200 chars is the boundary");
    assert_eq!(photo.caption.as_deref(), Some(caption.as_str()));
}

// ── try_new errors ─────────────────────────────────────────────────────────

#[test]
fn try_new_empty_r2_key_rejected() {
    let err = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        "",
        None,
        None,
        0,
        None,
        None,
        None,
        MediaFormat::Jpeg,
        sample_now(),
    )
    .unwrap_err();
    assert_eq!(err, ListingPhotoError::EmptyR2Key);
}

#[test]
fn try_new_whitespace_r2_key_rejected_via_trim() {
    let err = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        "   \t\n  ",
        None,
        None,
        0,
        None,
        None,
        None,
        MediaFormat::Jpeg,
        sample_now(),
    )
    .unwrap_err();
    assert_eq!(err, ListingPhotoError::EmptyR2Key);
}

#[test]
fn try_new_negative_display_order_rejected() {
    let err = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        None,
        -1,
        None,
        None,
        None,
        MediaFormat::Jpeg,
        sample_now(),
    )
    .unwrap_err();
    assert_eq!(err, ListingPhotoError::NegativeDisplayOrder { actual: -1 });
}

#[test]
fn try_new_caption_201_chars_rejected() {
    let caption: String = "가".repeat(201);
    let err = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        Some(caption.as_str()),
        0,
        None,
        None,
        None,
        MediaFormat::Jpeg,
        sample_now(),
    )
    .unwrap_err();
    assert_eq!(err, ListingPhotoError::CaptionTooLong { actual: 201 });
}

// ── soft_delete + is_active ────────────────────────────────────────────────

#[test]
fn soft_delete_sets_deleted_at_and_marks_inactive() {
    let mut photo = build_full();
    assert!(photo.is_active());
    photo.soft_delete(later_now());
    assert_eq!(photo.deleted_at, Some(later_now()));
    assert!(!photo.is_active());
}

#[test]
fn soft_delete_idempotent_preserves_first_timestamp() {
    let mut photo = build_full();
    let first = later_now();
    let second = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    photo.soft_delete(first);
    photo.soft_delete(second);
    assert_eq!(photo.deleted_at, Some(first));
}

#[test]
fn soft_delete_preserves_uploaded_at() {
    let mut photo = build_full();
    let original_uploaded = photo.uploaded_at;
    photo.soft_delete(later_now());
    assert_eq!(photo.uploaded_at, original_uploaded);
}

// -- upload confirmation -----------------------------------------------------

#[test]
fn confirm_upload_sets_metadata_and_marks_confirmed() {
    let mut photo = build_pending();

    photo
        .confirm_upload(Some(1280), Some(720), 1_250_000, later_now())
        .expect("valid upload confirmation");

    assert!(photo.is_upload_confirmed());
    assert_eq!(photo.width_px, Some(1280));
    assert_eq!(photo.height_px, Some(720));
    assert_eq!(photo.file_size_bytes, Some(1_250_000));
    assert_eq!(photo.uploaded_at, later_now());
}

#[test]
fn confirm_upload_allows_missing_dimensions_when_storage_has_no_image_probe() {
    let mut photo = build_pending();

    photo
        .confirm_upload(None, None, 1_250_000, later_now())
        .expect("file size confirms object existence");

    assert!(photo.is_upload_confirmed());
    assert_eq!(photo.width_px, None);
    assert_eq!(photo.height_px, None);
    assert_eq!(photo.file_size_bytes, Some(1_250_000));
}

#[test]
fn confirm_upload_rejects_zero_file_size() {
    let mut photo = build_pending();

    let err = photo
        .confirm_upload(Some(1280), Some(720), 0, later_now())
        .unwrap_err();

    assert_eq!(err, ListingPhotoError::InvalidFileSizeBytes { actual: 0 });
    assert!(!photo.is_upload_confirmed());
}

#[test]
fn confirm_upload_rejects_non_positive_dimensions() {
    let mut photo = build_pending();

    let err = photo
        .confirm_upload(Some(0), Some(720), 1_250_000, later_now())
        .unwrap_err();

    assert_eq!(err, ListingPhotoError::InvalidWidthPx { actual: 0 });
    assert!(!photo.is_upload_confirmed());
}

#[test]
fn confirm_upload_rejects_non_positive_height() {
    let mut photo = build_pending();

    let err = photo
        .confirm_upload(Some(1280), Some(-1), 1_250_000, later_now())
        .unwrap_err();

    assert_eq!(err, ListingPhotoError::InvalidHeightPx { actual: -1 });
    assert!(!photo.is_upload_confirmed());
}

#[test]
fn confirm_upload_rejects_deleted_photo() {
    let mut photo = build_pending();
    photo.soft_delete(later_now());

    let err = photo
        .confirm_upload(Some(1280), Some(720), 1_250_000, later_now())
        .unwrap_err();

    assert_eq!(err, ListingPhotoError::DeletedCannotConfirm);
    assert!(!photo.is_upload_confirmed());
}

// ── reorder ────────────────────────────────────────────────────────────────

#[test]
fn reorder_happy_path_updates_display_order() {
    let mut photo = build_full();
    photo.reorder(7).expect("non-negative");
    assert_eq!(photo.display_order, 7);
}

#[test]
fn reorder_negative_rejected() {
    let mut photo = build_full();
    let err = photo.reorder(-3).unwrap_err();
    assert_eq!(err, ListingPhotoError::NegativeDisplayOrder { actual: -3 });
    // 원본 값 유지.
    assert_eq!(photo.display_order, 0);
}

// ── upload format ──────────────────────────────────────────────────────────

#[test]
fn upload_format_takes_jpeg_png_webp_only() {
    assert_eq!(upload_format("image/jpeg"), Ok(MediaFormat::Jpeg));
    assert_eq!(upload_format("image/png"), Ok(MediaFormat::Png));
    assert_eq!(upload_format("image/webp"), Ok(MediaFormat::Webp));
    assert_eq!(
        upload_format("image/gif"),
        Err(ListingPhotoError::UnsupportedContentType(
            "image/gif".to_owned()
        ))
    );
}

#[test]
fn try_new_rejects_a_format_listings_do_not_take() {
    let err = ListingPhoto::try_new(
        sample_id(),
        sample_listing_id(),
        SAMPLE_R2_KEY,
        None,
        None,
        0,
        None,
        None,
        None,
        MediaFormat::Pdf,
        sample_now(),
    )
    .unwrap_err();
    assert_eq!(
        err,
        ListingPhotoError::UnsupportedContentType("application/pdf".to_owned())
    );
}

// ── ListingPhoto serde ─────────────────────────────────────────────────────

#[test]
fn listing_photo_serde_json_roundtrip() {
    let photo = build_full();
    let json = serde_json::to_string(&photo).expect("serialize");
    let back: ListingPhoto = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, photo);
}
