//! `FeaturedContentRepository` 저장/조회 포트.

#![allow(clippy::module_name_repetitions)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use shared_kernel::id::{FeaturedContentMarker, Id};
use shared_kernel::mutation::MutationContext;
use thiserror::Error;

use crate::{FeaturedContent, FeaturedContentFeatureKind};

/// 추천 콘텐츠 저장/조회 포트.
#[async_trait]
pub trait FeaturedContentRepository: Send + Sync {
    /// 저장 (`INSERT` or `UPDATE`). 버전 컬럼이 없으므로 OCC 충돌은 없어요.
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn save(
        &self,
        featured_content: &FeaturedContent,
        ctx: MutationContext,
    ) -> Result<(), RepoError>;

    /// `id`로 단건 조회해요. 없으면 `Ok(None)`을 반환해요.
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_by_id(
        &self,
        id: &Id<FeaturedContentMarker>,
    ) -> Result<Option<FeaturedContent>, RepoError>;

    /// 특정 시각에 활성인 슬롯의 콘텐츠를 weight 내림차순으로 반환해요.
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_active(
        &self,
        feature_kind: FeaturedContentFeatureKind,
        at: DateTime<Utc>,
    ) -> Result<Vec<FeaturedContent>, RepoError>;
}

/// 추천 콘텐츠 저장소 에러.
#[derive(Debug, Error)]
pub enum RepoError {
    /// 대상 추천 콘텐츠가 없어요.
    #[error("not found")]
    NotFound,
    /// DB 통신 또는 SQL 에러예요.
    #[error("database error: {0}")]
    Database(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn assert_obj_safe(_repo: &dyn FeaturedContentRepository) {}

    #[test]
    fn trait_is_object_safe() {}

    #[test]
    fn repo_error_messages_are_stable() {
        assert_eq!(RepoError::NotFound.to_string(), "not found");
        assert_eq!(
            RepoError::Database("connection refused".to_owned()).to_string(),
            "database error: connection refused"
        );
    }
}
