//! `SQLx` `Postgres` `Repository` 구현체.
//!
//! 도메인 BC 가 정의한 `*Repository` trait 의 구현. `error_map` 모듈이 공통
//! `sqlx::Error` 매핑을 제공해요.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod admin_action;
pub mod analysis_report;
pub mod audit_log;
mod audit_state;
pub mod bookmark;
pub mod business_verification;
pub mod error_map;
pub mod featured_content;
pub mod foundation_anchor;
pub mod listing;
pub mod listing_photo;
pub mod listing_report;
pub mod listing_review;
pub mod notification;
pub mod outbox;
pub mod search_history;
pub mod system_alert;
pub mod user;
