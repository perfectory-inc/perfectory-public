---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# notification-domain

`Notification` 도메인 (RDS 동적) crate에요.

## 책임

- `notification` 테이블(`migrations/20260719000103_insights_tables.sql`)에 대응하는 Aggregate를 정의해요.
- 사용자 알림 append-mostly (이벤트 발생 시 1 row INSERT).
- `kind`: `NotificationKind` enum — `listing_approved` / `listing_rejected` / `listing_bookmarked` / `other`.
  DB 컬럼은 `varchar(50)` 문자열이고, 모르는 라벨은 `Other`로 읽어요(forward-compat).
- `payload`: 이벤트 컨텍스트 (`JSONB`).
- `mark_read` 멱등 — 이미 읽은 알림 재호출 시 `read_at` 보존.
- 보존 365일은 마이그레이션 주석상의 정책이에요. 오래된 row를 지우는 작업은 아직 없어요.
- `NotificationRepository` trait — `find_unread_by_user` / `find_recent_by_user` / `insert` / `mark_read` /
  `mark_all_read_by_kind`. `Postgres` 구현은 `gongzzang-persistence`의 `PgNotificationRepository`.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `NotificationMarker`, `MutationContext`).
