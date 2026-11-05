---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# audit-log-domain

`AuditLog` 도메인 (RDS 동적) crate에요.

## 책임

- `audit_log` 테이블(`migrations/20260719000104_system_tables.sql`)에 대응하는 Aggregate를 정의해요.
- **불변 (immutable) — append-only**. `20260719000107_audit_immutable_trigger.sql` 트리거가 `UPDATE`/`DELETE`를 차단하고,
  `20261016160000_audit_log_is_not_deleted_without_an_archive.sql`이 archiver 역할의
  `DELETE` 권한까지 회수해 지금은 어떤 역할도 행을 지울 수 없어요.
- `actor_id` `None` = 시스템 행위 (system action, `is_system_action()`).
- `before_state` / `after_state` 는 `JSONB` 스냅샷.
- `correlation_id` 분산 추적용 — 한 요청의 모든 audit 묶이게 해요.
- `AuditLogRepository` trait — `insert` + 3개 read(`find_by_resource` / `find_by_actor` / `find_by_correlation_id`)만.
  `save`/`update`/`delete` *없음*. `Postgres` 구현은 `gongzzang-persistence`의 `PgAuditLogRepository`.

## 불변 (immutable) 보장

`AuditLog` 는 **mutation 메서드가 하나도 없어요**. `try_new` 로 생성한 뒤
어떤 필드도 바꿀 수 없어요. 이건 audit immutable trigger 마이그레이션과 함께 컴파일 + 런타임
양쪽에서 audit 무결성을 보장하기 위한 *의도된 설계* 에요.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `AuditLogMarker`).
