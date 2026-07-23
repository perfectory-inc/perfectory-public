//! Transactional Outbox 이벤트의 published language.
//!
//! ADR 0032 기둥 2 + ADR 0033 G4: 모든 이벤트는 `schema_version` 리터럴 필드를
//! 갖는다. Breaking change 시 동일 이름 + 버전 suffix (예: `_v2`) 의 신규 파일을
//! 생성하고 기존 파일은 동결한다. CI 의 `schema-registry-check` job 이 파일 해시 drift 를 차단.
//!
//! 이벤트는 consumer 가 `idempotent` 하게 처리해야 한다 (at-least-once 보장).

pub mod catalog_v1;

/// Outbox 이벤트 헤더 — 모든 페이로드가 공통으로 갖는 메타데이터.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OutboxEnvelope<T> {
    /// 이벤트 고유 식별자 — consumer 의 멱등성 키.
    pub event_id: uuid::Uuid,
    /// 이벤트 발생 시각 (UTC).
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    /// 페이로드 (각 이벤트 모듈의 struct).
    pub payload: T,
}
