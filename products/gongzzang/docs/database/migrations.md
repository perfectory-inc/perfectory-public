---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
---

# 마이그레이션 운영 가이드

마이그레이션 작성 규칙(명명·적용 순서·forward-only·로컬 검증·CI 검증·블루-그린 변경·실패 복구)의
정본은 [`migrations/README.md`](../../migrations/README.md)예요. 모노레포 공통 명명 규칙은
[루트 ADR-0001](../../../../docs/adr/0001-monorepo-governance-and-conventions.md) 결정 7번이에요.
여기서는 그 문서에 없는 데이터 보관(retention) 현황만 적어요.

## 데이터 보관 (retention)

목표와 지금 실제로 도는 것을 나눠 적어요. 아래 삭제·가명화를 주기적으로 실행하는 작업은 아직 없어요.

| 테이블 | 목표 | 지금 |
|--------|------|------|
| `audit_log` | RDS 1년, 이후 R2 IA 6년 (총 7년 — PIPA + ISMS-P) | 보관(archive) 작업이 없어서 어떤 역할도 지우거나 고칠 수 없어요 (`20261016160000_audit_log_is_not_deleted_without_an_archive.sql`, 루트 ADR-0264) |
| `outbox_event` | published 후 30일 뒤 삭제 | 삭제 작업 없음 |
| `notification` | 365일 뒤 삭제 | 조회만 365일 이내로 거름 (`gongzzang-persistence/src/notification.rs`) |
| `search_history` | 90일 뒤 `user_id` 가명화, 1년 뒤 삭제 (PIPA) | 조회만 90일 이내로 거름; 가명화 함수 `pseudonymize_older_than` 은 있으나 부르는 작업 없음 (`gongzzang-persistence/src/search_history.rs`) |

## 참고 링크

- Database decision: [ADR-0004](../adr/0004-db-postgres-postgis.md)
- Executable schema SSOT: `migrations/*.sql`
- 루트 작성 절차: `.claude/skills/add-migration`
