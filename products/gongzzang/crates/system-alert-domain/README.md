---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# system-alert-domain

운영자가 확인하고 처리하는 시스템 알림을 소유하는 `SystemAlert` 도메인 crate에요.

## 책임

- `system_alert` 테이블(`migrations/20260719000105_operations_tables.sql`)에 대응하는 Aggregate를 정의해요.
- severity(`info` / `warning` / `error` / `critical`), `acknowledge`, `resolve` 불변식을 소유해요.
- `SystemAlertRepository` (`save` / `find_by_id` / `find_unacknowledged`)가 이 capability의 저장/조회 계약을
  소유해요. `Postgres` 구현은 `gongzzang-persistence`의 `PgSystemAlertRepository`.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `SystemAlertMarker`, `MutationContext`).
