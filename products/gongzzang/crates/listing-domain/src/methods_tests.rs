//! `Listing` 도메인 메서드 단위 테스트 — 상태 전이.
//!
//! `entity.rs`에서 `#[path = "methods_tests.rs"] mod methods_tests;`로 포함.
//! 파일 자체가 테스트 모듈이라 별도 `mod tests {}` 래퍼 없어요.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "methods_tests/fixtures.rs"]
mod fixtures;
#[path = "methods_tests/lifecycle.rs"]
mod lifecycle;
#[path = "methods_tests/state_transitions.rs"]
mod state_transitions;
#[path = "methods_tests/update_editable_fields.rs"]
mod update_editable_fields;
