---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# user-domain

공짱 `User` Aggregate (RDS 동적).

## 범위

- `User` struct — 식별(`id`, `zitadel_sub`, `email`, `phone_kr_hash`), 표시(`display_name`, `user_kind`),
  중개사(`broker_license_number`, `broker_verified_at`), `roles`, 동의·인증 시각(`nice_verified_at`,
  `marketing_consent_at`), `created_at` / `updated_at` / `last_login_at` / `deleted_at`, `version`.
  필드 정본은 `src/entity.rs`.
- `UserKind` enum — 개인(`individual`) / 법인(`corporation`).
- `UserRole` enum 7종 — `Buyer`, `Seller`, `Broker`, `Developer`, `Enterprise`, `Operator`, `Admin`
  (한 사용자가 여러 역할 보유 가능).
- 생성자 `try_new` / `try_new_full` — `display_name` (≤100자), `zitadel_sub` (≤255자), `phone_kr_hash`(64자 hex),
  중개사 검증 정합성 검사.
- mutation 메서드 — `verify_broker` / `revoke_broker_verification`, `record_nice_verification`,
  `record_marketing_consent` / `revoke_marketing_consent`, `record_login`, `add_role` / `remove_role`, `soft_delete`.
- 사업자 인증은 이 Aggregate가 아니라 `business-verification-domain` 큐가 소유해요.
- `UserRepository` trait — `find_by_id` / `find_by_zitadel_sub` / `find_by_email` / `save`.
  `Postgres` 구현은 `gongzzang-persistence`의 `PgUserRepository`.
- `UserError` / `RepoError` — thiserror

## 의존

- `shared-kernel` — `Email`, `Id<UserMarker>`, `BrokerLicense`, `MutationContext`
- 그 밖에는 `chrono`, `serde`, `thiserror`, `async-trait`만 (DB·HTTP 의존 없음)

→ [ADR 0001](../../docs/adr/0001-language-rust-ts.md), [Rust 컨벤션](../../docs/conventions/rust.md)
