---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# admin-action-domain

`AdminAction` 도메인 crate에요. 목적은 `Cargo.toml` `description` 이 정본이에요.

## 책임

- `admin_action` 테이블(`migrations/20260719000105_operations_tables.sql`)에 대응하는 Aggregate 정의해요.
- **append-only**. 어드민 액션은 한 번 기록되면 수정/삭제하지 않아요.
- `target_kind` / `target_id` 는 *둘 다 Some 또는 둘 다 None* (`try_new` 가 검사하는 도메인 invariant).
- `correlation_id` 분산 추적용 — 한 어드민 작업의 모든 액션이 묶여요.
- `AdminActionRepository` trait — `insert` + read 3개(`find_by_admin`·`find_by_target`·`find_by_correlation_id`)만. `save`/`update`/`delete` *없음*.

## append-only 보장

`AdminAction` 에는 mutation 메서드가 없고, 저장소 trait 에도 갱신·삭제 경로가 없어요. 필드는
`pub` 이라 메모리 안의 값 자체를 컴파일러가 얼려 주지는 않아요 — 보장은 "기록된 행을 바꿀 길이
저장소에 없다"는 데서 와요. `AuditLog` 와 같은 의도된 설계예요.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `AdminActionMarker`, `MutationContext`).
