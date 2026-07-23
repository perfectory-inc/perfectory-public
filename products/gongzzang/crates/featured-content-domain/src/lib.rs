//! 홈페이지 추천, 광고, 스폰서 노출을 소유하는 `FeaturedContent` capability.
//!
//! Spec § 5.5 `featured_content` 테이블에 대응하는 Aggregate와
//! [`crate::repository::FeaturedContentRepository`] 저장소 포트를 제공해요.
//!
//! ## ID prefix 주의
//!
//! - `FeaturedContent` — Spec inline 은 `fc_` (2-char) 로 적혀있지만 본 프로젝트
//!   30자 ID 불변식 (3-char prefix 와 `_` 와 26-char ULID 합) 충족 위해 `fea` 사용.
//!   Plan 2c T17 결정. Spec FU 11 에서 reconcile 예정.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod entity;
pub mod errors;
pub mod feature_kind;
pub mod repository;
pub mod target_kind;

pub use entity::FeaturedContent;
pub use errors::FeaturedContentError;
pub use feature_kind::FeaturedContentFeatureKind;
pub use target_kind::FeaturedContentTargetKind;
