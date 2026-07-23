//! 운영자가 확인하고 처리하는 시스템 알림 capability.
//!
//! Spec § 5.5 `system_alert` 테이블에 대응하는 Aggregate와
//! [`crate::repository::SystemAlertRepository`] 저장소 포트를 제공해요.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod entity;
pub mod errors;
pub mod repository;
pub mod severity;

pub use entity::SystemAlert;
pub use errors::SystemAlertError;
pub use severity::SystemAlertSeverity;

pub use repository::SystemAlertRepository;
