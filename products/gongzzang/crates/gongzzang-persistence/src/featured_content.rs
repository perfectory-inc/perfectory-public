//! `FeaturedContentRepository`의 `Postgres` 구현체.

#![allow(clippy::module_name_repetitions)]

mod repository;
mod rows;

use sqlx::PgPool;

/// 추천 콘텐츠의 `Postgres` 저장소예요.
#[derive(Debug, Clone)]
pub struct PgFeaturedContentRepository {
    pool: PgPool,
}

impl PgFeaturedContentRepository {
    /// 새 저장소를 만들어요.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
