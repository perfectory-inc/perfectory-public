//! `SystemAlertRepository` 저장/조회 포트.

#![allow(clippy::module_name_repetitions)]

use async_trait::async_trait;
use shared_kernel::id::{Id, SystemAlertMarker};
use shared_kernel::mutation::MutationContext;
use thiserror::Error;

use crate::SystemAlert;

/// 시스템 알림 저장/조회 포트.
#[async_trait]
pub trait SystemAlertRepository: Send + Sync {
    /// 저장 (`INSERT` or `UPDATE`).
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn save(&self, alert: &SystemAlert, ctx: MutationContext) -> Result<(), RepoError>;

    /// `id`로 단건 조회해요. 없으면 `Ok(None)`을 반환해요.
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_by_id(
        &self,
        id: &Id<SystemAlertMarker>,
    ) -> Result<Option<SystemAlert>, RepoError>;

    /// 확인하지 않은 알림을 심각도와 생성 시각 순으로 반환해요.
    ///
    /// # Errors
    ///
    /// DB 통신 실패 시 [`RepoError::Database`].
    async fn find_unacknowledged(&self, limit: u32) -> Result<Vec<SystemAlert>, RepoError>;
}

/// 시스템 알림 저장소 에러.
#[derive(Debug, Error)]
pub enum RepoError {
    /// 대상 시스템 알림이 없어요.
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
    fn assert_obj_safe(_repo: &dyn SystemAlertRepository) {}

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
