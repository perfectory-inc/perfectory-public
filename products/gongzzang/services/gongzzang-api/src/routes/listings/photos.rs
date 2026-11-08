use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Redirect;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use listing_domain::entity::Listing;
use listing_photo_domain::entity::ListingPhoto;
use product_identity_infrastructure::middleware::AuthenticatedUser;
use product_identity_infrastructure::role_guard::require_role;
use serde::{Deserialize, Serialize};
use shared_kernel::id::{Id, ListingMarker as ListingIdMarker, ListingPhotoMarker, UserMarker};
use shared_kernel::media::{
    content_addressed_key, rendition_key, MediaFormat, Sha256Hex, LISTING_PHOTO_MEDIA_PREFIX,
    RENDITION_FORMATS, RENDITION_VERSION, RENDITION_WIDTHS,
};
use user_domain::entity::UserRole;

use crate::http::mutation_ctx::http_user_action;
use crate::http::problem::{problem, ProblemResponse};
use crate::photo_upload::{
    PhotoDownloadUrlError, PhotoDownloadUrlRequest, PhotoObjectVerifyError,
    PhotoObjectVerifyRequest, PhotoUploadUrlError, PhotoUploadUrlRequest, UploadHeader,
};

use super::mutation::load_listing_for_actor;
use super::state::ListingsState;

#[path = "photo_events.rs"]
mod photo_events;

#[derive(Debug, Deserialize)]
pub struct RequestPhotoUploadRequest {
    /// 표시 순서 (≥0).
    pub display_order: i32,
    /// MIME content-type (`image/jpeg`/`image/png`/`image/webp`).
    pub content_type: String,
    /// 캡션 (≤200자, 선택).
    pub caption: Option<String>,
    /// 파일 바이트의 sha256 (소문자 hex). 주면 사진은 내용주소 키
    /// (`media/listing-photo/sha256/<hex>.<ext>`)에 올라가고, 저장소가 그 해시가 아닌 바이트를 거절해요
    /// (root ADR-0427). 없으면 옛 id 키 — 그 키를 쓰는 클라이언트가 계속 동작해요.
    pub content_sha256: Option<String>,
}

/// `GET /listings/:listing_id/photos/:photo_id` query.
#[derive(Debug, Deserialize)]
pub struct PhotoDownloadQuery {
    /// One rung of the display ladder (`<width>.<format>`, e.g. `400.webp`); absent is the stored
    /// file itself (root ADR-0427).
    pub rendition: Option<String>,
}

/// `POST /listings/:id/photos` 응답.
#[derive(Debug, Serialize)]
pub struct RequestPhotoUploadResponse {
    /// 새 사진 ID (`lph_<26 ULID>`).
    pub photo_id: String,
    /// pre-signed PUT URL.
    pub presigned_put_url: String,
    /// Headers that must be sent with the PUT request.
    pub upload_headers: Vec<UploadHeader>,
    /// R2 객체 키 (`listings/<lst_id>/<lph_id>.<ext>`).
    pub r2_key: String,
    /// URL 만료 시각.
    pub expires_at: DateTime<Utc>,
}

/// `POST /listings/:listing_id/photos/:photo_id/confirm` response.
#[derive(Debug, Serialize)]
pub struct ConfirmPhotoUploadResponse {
    /// Photo ID (`lph_<26 ULID>`).
    pub photo_id: String,
    /// R2 object key.
    pub r2_key: String,
    /// Verified file size.
    pub file_size_bytes: i64,
    /// Verified MIME content-type.
    pub content_type: String,
    /// Upload confirmation timestamp.
    pub uploaded_at: DateTime<Utc>,
}

/// `POST /listings/:id/photos` — pre-signed URL 발급 (Broker + 소유자 전용).
#[allow(clippy::too_many_lines)]
#[tracing::instrument(skip(state, auth, body), fields(actor = %auth.user.id, listing_id = %id))]
pub async fn request_photo_upload(
    State(state): State<ListingsState>,
    Extension(auth): Extension<AuthenticatedUser>,
    Path(id): Path<String>,
    Json(body): Json<RequestPhotoUploadRequest>,
) -> Result<(StatusCode, Json<RequestPhotoUploadResponse>), ProblemResponse> {
    require_role(&auth, UserRole::Broker).map_err(|_| {
        problem(
            "forbidden",
            "broker 권한이 필요해요",
            StatusCode::FORBIDDEN,
            None,
        )
    })?;

    let listing = load_listing_for_actor(&state, &auth, &id).await?;

    let content_type =
        listing_photo_domain::entity::upload_format(&body.content_type).map_err(|e| {
            problem(
                "validation",
                "content_type 가 유효하지 않아요 (image/jpeg, image/png, image/webp)",
                StatusCode::BAD_REQUEST,
                Some(e.to_string()),
            )
        })?;
    let content_sha256 = body
        .content_sha256
        .as_deref()
        .map(Sha256Hex::parse)
        .transpose()
        .map_err(|e| {
            problem(
                "validation",
                "content_sha256 가 유효하지 않아요 (소문자 hex 64자)",
                StatusCode::BAD_REQUEST,
                Some(e.to_string()),
            )
        })?;

    let photo_id = Id::<ListingPhotoMarker>::new();
    let r2_key = content_sha256.as_ref().map_or_else(
        || {
            format!(
                "media/listing-photo/listings/{}/photos/{}.{}",
                listing.id.as_str(),
                photo_id.as_str(),
                content_type.extension()
            )
        },
        |hash| content_addressed_key(LISTING_PHOTO_MEDIA_PREFIX, hash, content_type),
    );

    let now = Utc::now();
    let photo = ListingPhoto::try_new(
        photo_id.clone(),
        listing.id.clone(),
        &r2_key,
        content_sha256.clone(),
        body.caption.as_deref(),
        body.display_order,
        None,
        None,
        None,
        content_type,
        now,
    )
    .map_err(|e| {
        problem(
            "validation",
            "사진 메타가 유효하지 않아요",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;

    let upload_url = state
        .photo_upload_issuer
        .issue_upload_url(PhotoUploadUrlRequest {
            r2_key: r2_key.clone(),
            content_type,
            content_sha256,
        })
        .await
        .map_err(|error| photo_upload_problem(&error))?;

    let ctx = http_user_action(&auth, "request_photo_upload");
    state.photo_repo.save(&photo, ctx).await.map_err(|e| {
        tracing::error!(error = %e, "photo save failed");
        // listing-photo 의 RepoError 와 listing 의 RepoError 는 별개 enum.
        // 매핑은 간단히 재현 (NotFound/Conflict/Database).
        match e {
            listing_photo_domain::repository::RepoError::NotFound => problem(
                "not-found",
                "리소스를 찾을 수 없어요",
                StatusCode::NOT_FOUND,
                None,
            ),
            listing_photo_domain::repository::RepoError::Conflict => problem(
                "version-conflict",
                "충돌이 발생했어요",
                StatusCode::CONFLICT,
                None,
            ),
            listing_photo_domain::repository::RepoError::Database(_) => problem(
                "internal-error",
                "내부 서버 오류",
                StatusCode::INTERNAL_SERVER_ERROR,
                None,
            ),
        }
    })?;

    Ok((
        StatusCode::CREATED,
        Json(RequestPhotoUploadResponse {
            photo_id: photo_id.as_str().to_owned(),
            presigned_put_url: upload_url.url,
            upload_headers: upload_url.required_headers,
            r2_key,
            expires_at: now + chrono::Duration::minutes(15),
        }),
    ))
}

fn photo_upload_problem(error: &PhotoUploadUrlError) -> ProblemResponse {
    tracing::error!(error = %error, "photo upload URL issue failed");
    problem(
        "photo-upload-unavailable",
        "사진 업로드를 지금 사용할 수 없어요",
        StatusCode::SERVICE_UNAVAILABLE,
        None,
    )
}

/// `GET /listings/:listing_id/photos/:photo_id` redirects to a short-lived R2 signed URL of the
/// stored file, or with `?rendition=<width>.<format>` of one rung of its display ladder.
#[tracing::instrument(skip(state, auth, query), fields(actor = %auth.user.id, listing_id = %listing_id, photo_id = %photo_id))]
pub async fn get_photo_download_redirect(
    State(state): State<ListingsState>,
    Extension(auth): Extension<AuthenticatedUser>,
    Path((listing_id, photo_id)): Path<(String, String)>,
    Query(query): Query<PhotoDownloadQuery>,
) -> Result<Redirect, ProblemResponse> {
    let listing_id = Id::<ListingIdMarker>::from_str(&listing_id).map_err(|e| {
        problem(
            "validation",
            "listing id가 유효하지 않아요",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;
    let listing = state
        .listing_repo
        .find(&listing_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "listing find failed");
            crate::http::problem::from_listing_repo_error(&e)
        })?
        .ok_or_else(|| {
            problem(
                "not-found",
                "매물을 찾을 수 없어요",
                StatusCode::NOT_FOUND,
                None,
            )
        })?;
    if !can_view_photo(&listing, &auth.user.id) {
        return Err(problem(
            "not-found",
            "매물을 찾을 수 없어요",
            StatusCode::NOT_FOUND,
            None,
        ));
    }

    let pid = Id::<ListingPhotoMarker>::from_str(&photo_id).map_err(|e| {
        problem(
            "validation",
            "photo_id가 유효하지 않아요",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;
    let photo = state
        .photo_repo
        .find(&pid)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "photo find failed");
            photo_repo_problem(&e)
        })?
        .ok_or_else(|| {
            problem(
                "not-found",
                "사진을 찾을 수 없어요",
                StatusCode::NOT_FOUND,
                None,
            )
        })?;
    if photo.listing_id != listing.id || !photo.is_upload_confirmed() {
        return Err(problem(
            "not-found",
            "사진을 찾을 수 없어요",
            StatusCode::NOT_FOUND,
            None,
        ));
    }

    let r2_key = match query.rendition.as_deref() {
        None => photo.r2_key,
        Some(rung) => {
            rendition_object_key(photo.content_sha256.as_ref(), rung).ok_or_else(|| {
                problem(
                    "not-found",
                    "사진을 찾을 수 없어요",
                    StatusCode::NOT_FOUND,
                    None,
                )
            })?
        }
    };
    let download = state
        .photo_download_issuer
        .issue_download_url(PhotoDownloadUrlRequest { r2_key })
        .await
        .map_err(|error| photo_download_problem(&error))?;

    Ok(Redirect::temporary(&download.url))
}

/// The ladder key of one rung (`<width>.<format>`) of a photo stored by its hash (root ADR-0427);
/// `None` for a rung the ladder does not have or a photo with no hash yet.
fn rendition_object_key(content_sha256: Option<&Sha256Hex>, rung: &str) -> Option<String> {
    let (width, extension) = rung.split_once('.')?;
    let width = width.parse::<u32>().ok()?;
    let format = MediaFormat::from_extension(extension)?;
    if !RENDITION_WIDTHS.contains(&width) || !RENDITION_FORMATS.contains(&format) {
        return None;
    }
    Some(rendition_key(
        LISTING_PHOTO_MEDIA_PREFIX,
        RENDITION_VERSION,
        content_sha256?,
        width,
        format,
    ))
}

/// `POST /listings/:listing_id/photos/:photo_id/confirm` verifies R2 object presence.
#[tracing::instrument(skip(state, auth), fields(actor = %auth.user.id, listing_id = %listing_id, photo_id = %photo_id))]
pub async fn confirm_photo_upload(
    State(state): State<ListingsState>,
    Extension(auth): Extension<AuthenticatedUser>,
    Path((listing_id, photo_id)): Path<(String, String)>,
) -> Result<Json<ConfirmPhotoUploadResponse>, ProblemResponse> {
    require_role(&auth, UserRole::Broker).map_err(|_| {
        problem(
            "forbidden",
            "broker 권한이 필요해요",
            StatusCode::FORBIDDEN,
            None,
        )
    })?;

    let listing = load_listing_for_actor(&state, &auth, &listing_id).await?;
    let pid = Id::<ListingPhotoMarker>::from_str(&photo_id).map_err(|e| {
        problem(
            "validation",
            "photo_id가 유효하지 않아요",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;

    let mut photo = state
        .photo_repo
        .find(&pid)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "photo find failed");
            photo_repo_problem(&e)
        })?
        .ok_or_else(|| {
            problem(
                "not-found",
                "사진을 찾을 수 없어요",
                StatusCode::NOT_FOUND,
                None,
            )
        })?;
    if photo.listing_id != listing.id {
        return Err(problem(
            "not-found",
            "사진을 찾을 수 없어요",
            StatusCode::NOT_FOUND,
            None,
        ));
    }

    let metadata = state
        .photo_object_verifier
        .verify_uploaded_object(PhotoObjectVerifyRequest {
            r2_key: photo.r2_key.clone(),
            expected_content_type: photo.content_type,
        })
        .await
        .map_err(|error| photo_object_verify_problem(&error))?;
    let uploaded_at = Utc::now();
    photo
        .confirm_upload(None, None, metadata.file_size_bytes, uploaded_at)
        .map_err(|e| {
            problem(
                "validation",
                "사진 업로드 상태가 유효하지 않아요",
                StatusCode::CONFLICT,
                Some(e.to_string()),
            )
        })?;

    let event = photo_events::upload_confirmed_event_for_photo(
        &photo,
        metadata.file_size_bytes,
        uploaded_at,
    );
    let ctx = http_user_action(&auth, "confirm_photo_upload").with_events(vec![event]);
    state.photo_repo.save(&photo, ctx).await.map_err(|e| {
        tracing::error!(error = %e, "photo confirm save failed");
        photo_repo_problem(&e)
    })?;

    Ok(Json(ConfirmPhotoUploadResponse {
        photo_id: photo.id.as_str().to_owned(),
        r2_key: photo.r2_key,
        file_size_bytes: metadata.file_size_bytes,
        content_type: metadata.content_type.content_type().to_owned(),
        uploaded_at,
    }))
}

fn photo_object_verify_problem(error: &PhotoObjectVerifyError) -> ProblemResponse {
    match error {
        PhotoObjectVerifyError::NotFound => problem(
            "photo-object-not-found",
            "업로드된 사진 파일을 찾을 수 없어요",
            StatusCode::CONFLICT,
            None,
        ),
        PhotoObjectVerifyError::MissingContentLength
        | PhotoObjectVerifyError::InvalidContentLength { .. }
        | PhotoObjectVerifyError::MissingContentType
        | PhotoObjectVerifyError::ContentTypeMismatch { .. } => problem(
            "photo-object-invalid",
            "업로드된 사진 파일 정보가 유효하지 않아요",
            StatusCode::CONFLICT,
            Some(error.to_string()),
        ),
        PhotoObjectVerifyError::Disabled | PhotoObjectVerifyError::Head(_) => {
            tracing::error!(error = %error, "photo object verify failed");
            problem(
                "photo-upload-unavailable",
                "사진 업로드 확인을 지금 사용할 수 없어요",
                StatusCode::SERVICE_UNAVAILABLE,
                None,
            )
        }
    }
}

fn photo_download_problem(error: &PhotoDownloadUrlError) -> ProblemResponse {
    tracing::error!(error = %error, "photo download URL issue failed");
    problem(
        "photo-download-unavailable",
        "사진 다운로드를 지금 사용할 수 없어요",
        StatusCode::SERVICE_UNAVAILABLE,
        None,
    )
}

fn can_view_photo(listing: &Listing, viewer_id: &Id<UserMarker>) -> bool {
    use shared_kernel::listing_status::ListingStatus;
    matches!(
        listing.status,
        ListingStatus::Active | ListingStatus::Sold | ListingStatus::Expired
    ) || listing.owner_id == *viewer_id
}

fn photo_repo_problem(error: &listing_photo_domain::repository::RepoError) -> ProblemResponse {
    match error {
        listing_photo_domain::repository::RepoError::NotFound => problem(
            "not-found",
            "사진을 찾을 수 없어요",
            StatusCode::NOT_FOUND,
            None,
        ),
        listing_photo_domain::repository::RepoError::Conflict => problem(
            "version-conflict",
            "충돌이 발생했어요",
            StatusCode::CONFLICT,
            None,
        ),
        listing_photo_domain::repository::RepoError::Database(_) => problem(
            "internal-error",
            "내부 서버 오류",
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
        ),
    }
}

/// `DELETE /listings/:id/photos/:photo_id` soft-deletes a listing photo.
#[tracing::instrument(skip(state, auth), fields(actor = %auth.user.id, listing_id = %listing_id, photo_id = %photo_id))]
pub async fn delete_photo(
    State(state): State<ListingsState>,
    Extension(auth): Extension<AuthenticatedUser>,
    Path((listing_id, photo_id)): Path<(String, String)>,
) -> Result<StatusCode, ProblemResponse> {
    require_role(&auth, UserRole::Broker).map_err(|_| {
        problem(
            "forbidden",
            "broker 권한이 필요해요",
            StatusCode::FORBIDDEN,
            None,
        )
    })?;

    // 매물 ownership 검증 (사진 단독 ownership 컬럼은 없음 — listing 으로 추적).
    let listing = load_listing_for_actor(&state, &auth, &listing_id).await?;

    let pid = Id::<ListingPhotoMarker>::from_str(&photo_id).map_err(|e| {
        problem(
            "validation",
            "photo_id 가 유효하지 않아요",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;

    let ctx = http_user_action(&auth, "delete_photo");
    state
        .photo_repo
        .delete(&pid, &listing.id, ctx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "photo delete failed");
            match e {
                listing_photo_domain::repository::RepoError::NotFound => problem(
                    "not-found",
                    "사진을 찾을 수 없어요",
                    StatusCode::NOT_FOUND,
                    None,
                ),
                listing_photo_domain::repository::RepoError::Conflict
                | listing_photo_domain::repository::RepoError::Database(_) => problem(
                    "internal-error",
                    "내부 서버 오류",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    None,
                ),
            }
        })?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod rendition_tests {
    use shared_kernel::media::Sha256Hex;

    use super::rendition_object_key;

    #[test]
    fn a_rung_of_the_ladder_names_its_key_and_nothing_else_does() {
        let h = "0123456789abcdef".repeat(4);
        let hash = Sha256Hex::parse(&h).ok();
        assert_eq!(
            rendition_object_key(hash.as_ref(), "400.webp"),
            Some(format!("media/listing-photo/renditions/r1/{h}/400.webp"))
        );
        assert_eq!(
            rendition_object_key(hash.as_ref(), "1920.avif"),
            Some(format!("media/listing-photo/renditions/r1/{h}/1920.avif"))
        );
        for rung in ["401.webp", "400.jpg", "400", "../400.webp", "x.webp"] {
            assert_eq!(rendition_object_key(hash.as_ref(), rung), None, "{rung}");
        }
        // An id-key photo the backfill has not mapped has no ladder.
        assert_eq!(rendition_object_key(None, "400.webp"), None);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// SP6-iii: GET /listings/:id 매물 상세
// ─────────────────────────────────────────────────────────────────────────
