//! `ListingPhoto` Aggregate (spec § 5.1, 12 필드).

// `ListingPhoto` 처럼 모듈명 반복은 의도된 공개 API 형태.
#![allow(clippy::module_name_repetitions)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use shared_kernel::id::{Id, ListingMarker, ListingPhotoMarker};
use shared_kernel::media::{MediaFormat, Sha256Hex};

use crate::errors::ListingPhotoError;

/// 매물 사진으로 받는 형식 (업로드 `content_type` 이 이 중 하나여야 해요).
pub const UPLOAD_FORMATS: [MediaFormat; 3] =
    [MediaFormat::Jpeg, MediaFormat::Png, MediaFormat::Webp];

/// 업로드 `content_type`(MIME) 을 받는 형식으로 읽어요.
///
/// # Errors
///
/// [`UPLOAD_FORMATS`] 밖이면 [`ListingPhotoError::UnsupportedContentType`].
pub fn upload_format(content_type: &str) -> Result<MediaFormat, ListingPhotoError> {
    MediaFormat::from_content_type(content_type)
        .filter(|format| UPLOAD_FORMATS.contains(format))
        .ok_or_else(|| ListingPhotoError::UnsupportedContentType(content_type.to_owned()))
}

/// `ListingPhoto` Aggregate.
///
/// spec § 5.1 `listing_photo` 테이블 12 필드 1:1 매핑이에요.
/// `version` 컬럼은 *없어요* — 사진은 append/soft-delete 흐름이라
/// 동시 관리자 수정 충돌이 없어요.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingPhoto {
    /// `lph_<26 ULID>` 형식 ID.
    pub id: Id<ListingPhotoMarker>,
    /// 소속 매물 (`FK` + `ON DELETE CASCADE`).
    pub listing_id: Id<ListingMarker>,
    /// `R2` 객체 키 (예: `'media/listing-photo/listings/lst_01HXY/photos/p1.jpg'`).
    pub r2_key: String,
    /// 원본 바이트의 sha256 — 있으면 `r2_key` 는 내용주소 키이고 표시 사다리가 그 해시 옆에 있어요
    /// (root ADR-0427). 옛 id 키 사진은 백필이 대응을 적기 전까지 `None`.
    pub content_sha256: Option<Sha256Hex>,
    /// 캡션 (≤200자, 선택).
    pub caption: Option<String>,
    /// 표시 순서 (≥0).
    pub display_order: i32,
    /// 너비 px (선택, 메타데이터).
    pub width_px: Option<i32>,
    /// 높이 px (선택, 메타데이터).
    pub height_px: Option<i32>,
    /// 파일 크기 bytes (선택).
    pub file_size_bytes: Option<i64>,
    /// 형식 (`DB` 에는 `MIME` 으로 저장).
    pub content_type: MediaFormat,
    /// 업로드 시각.
    pub uploaded_at: DateTime<Utc>,
    /// Soft-delete 시각. `None`이면 활성.
    pub deleted_at: Option<DateTime<Utc>>,
}

impl ListingPhoto {
    /// 검증 후 새 `ListingPhoto` 생성. `deleted_at = None`,
    /// `uploaded_at = now`로 설정해요.
    ///
    /// # Errors
    ///
    /// - `r2_key`가 trim 후 빈 문자열 → [`ListingPhotoError::EmptyR2Key`].
    /// - `display_order < 0` → [`ListingPhotoError::NegativeDisplayOrder`].
    /// - `caption` 200자 초과 → [`ListingPhotoError::CaptionTooLong`].
    #[allow(clippy::too_many_arguments)] // 의도된 풀 생성자 (12 필드 매핑).
    pub fn try_new(
        id: Id<ListingPhotoMarker>,
        listing_id: Id<ListingMarker>,
        r2_key: &str,
        content_sha256: Option<Sha256Hex>,
        caption: Option<&str>,
        display_order: i32,
        width_px: Option<i32>,
        height_px: Option<i32>,
        file_size_bytes: Option<i64>,
        content_type: MediaFormat,
        now: DateTime<Utc>,
    ) -> Result<Self, ListingPhotoError> {
        let r2_key_trimmed = r2_key.trim();
        if r2_key_trimmed.is_empty() {
            return Err(ListingPhotoError::EmptyR2Key);
        }
        if !UPLOAD_FORMATS.contains(&content_type) {
            return Err(ListingPhotoError::UnsupportedContentType(
                content_type.content_type().to_owned(),
            ));
        }
        if display_order < 0 {
            return Err(ListingPhotoError::NegativeDisplayOrder {
                actual: display_order,
            });
        }
        if let Some(c) = caption {
            let len = c.chars().count();
            if len > 200 {
                return Err(ListingPhotoError::CaptionTooLong { actual: len });
            }
        }

        Ok(Self {
            id,
            listing_id,
            r2_key: r2_key_trimmed.to_owned(),
            content_sha256,
            caption: caption.map(str::to_owned),
            display_order,
            width_px,
            height_px,
            file_size_bytes,
            content_type,
            uploaded_at: now,
            deleted_at: None,
        })
    }

    /// Soft-delete. `deleted_at`을 설정해요. 이미 삭제된 경우
    /// 무시 (idempotent — 첫 삭제 시각 보존).
    pub const fn soft_delete(&mut self, at: DateTime<Utc>) {
        if self.deleted_at.is_none() {
            self.deleted_at = Some(at);
        }
    }

    /// `display_order` 변경. 음수 거부.
    ///
    /// # Errors
    ///
    /// `new_order < 0` → [`ListingPhotoError::NegativeDisplayOrder`].
    pub const fn reorder(&mut self, new_order: i32) -> Result<(), ListingPhotoError> {
        if new_order < 0 {
            return Err(ListingPhotoError::NegativeDisplayOrder { actual: new_order });
        }
        self.display_order = new_order;
        Ok(())
    }

    /// Mark the R2 object as present after storage verification.
    ///
    /// `width_px` and `height_px` can stay `None` when the storage confirmation path only has
    /// object metadata. `file_size_bytes` is required because it proves a concrete object was
    /// observed instead of only a pre-signed URL being issued.
    ///
    /// # Errors
    ///
    /// Returns [`ListingPhotoError`] when the photo was deleted or supplied metadata is invalid.
    pub const fn confirm_upload(
        &mut self,
        width_px: Option<i32>,
        height_px: Option<i32>,
        file_size_bytes: i64,
        confirmed_at: DateTime<Utc>,
    ) -> Result<(), ListingPhotoError> {
        if self.deleted_at.is_some() {
            return Err(ListingPhotoError::DeletedCannotConfirm);
        }
        if let Some(width) = width_px {
            if width <= 0 {
                return Err(ListingPhotoError::InvalidWidthPx { actual: width });
            }
        }
        if let Some(height) = height_px {
            if height <= 0 {
                return Err(ListingPhotoError::InvalidHeightPx { actual: height });
            }
        }
        if file_size_bytes <= 0 {
            return Err(ListingPhotoError::InvalidFileSizeBytes {
                actual: file_size_bytes,
            });
        }
        self.width_px = width_px;
        self.height_px = height_px;
        self.file_size_bytes = Some(file_size_bytes);
        self.uploaded_at = confirmed_at;
        Ok(())
    }

    /// 활성 여부 (soft-delete 안 됨).
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }

    /// Whether a storage object has been verified for this photo.
    #[must_use]
    pub const fn is_upload_confirmed(&self) -> bool {
        self.file_size_bytes.is_some()
    }
}

#[cfg(test)]
#[path = "entity_tests.rs"]
mod tests;
