//! `ListingReviewRepository` port. **Optimistic locking** — `save` 는 `version` 컬럼으로
//! 동시 검토 충돌을 차단해요.
//!
//! 구현체는 sub-project 5 (`crates/gongzzang-persistence`) 에서 추가해요.

#![allow(clippy::module_name_repetitions)]

use async_trait::async_trait;
use shared_kernel::id::{Id, ListingMarker, ListingReviewMarker};
use shared_kernel::mutation::MutationContext;
use thiserror::Error;

use crate::entity::ListingReview;

/// `ListingReview` 저장/조회 포트.
#[async_trait]
pub trait ListingReviewRepository: Send + Sync {
    /// `id` 로 조회. 없으면 `Ok(None)`.
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_by_id(
        &self,
        id: &Id<ListingReviewMarker>,
    ) -> Result<Option<ListingReview>, RepoError>;

    /// 결정되지 않은 (pending) 큐를 SLA 임박 순으로 최대 `limit` 건 조회 (어드민 워크큐용).
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_pending(&self, limit: u32) -> Result<Vec<ListingReview>, RepoError>;

    /// 매물 ID 로 큐 조회 (`UNIQUE` 가정 — 매물당 활성 큐 1건).
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_by_listing(
        &self,
        listing_id: &Id<ListingMarker>,
    ) -> Result<Option<ListingReview>, RepoError>;

    /// 저장 (`INSERT` or `UPDATE`). Optimistic lock 충돌 시 [`RepoError::Conflict`].
    ///
    /// # Errors
    ///
    /// 버전 불일치 → [`RepoError::Conflict`]. DB 통신 실패 → [`RepoError::Database`].
    async fn save(
        &self,
        listing_review: &ListingReview,
        ctx: MutationContext,
    ) -> Result<(), RepoError>;
}

/// `Repository` 에러.
#[derive(Debug, Error)]
pub enum RepoError {
    /// 대상 Aggregate 미존재.
    #[error("not found")]
    NotFound,
    /// Optimistic lock 버전 불일치.
    #[error("conflict (version mismatch)")]
    Conflict,
    /// DB 통신/SQL 에러 (정보 누설 방지로 메시지만).
    #[error("database error: {0}")]
    Database(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn assert_obj_safe(_repo: &dyn ListingReviewRepository) {}

    #[test]
    fn trait_is_object_safe() {
        // Compile-time check via above fn signature.
    }

    #[test]
    fn repo_error_messages() {
        assert_eq!(RepoError::NotFound.to_string(), "not found");
        assert_eq!(
            RepoError::Conflict.to_string(),
            "conflict (version mismatch)"
        );
        assert_eq!(
            RepoError::Database("oops".to_owned()).to_string(),
            "database error: oops"
        );
    }
}
