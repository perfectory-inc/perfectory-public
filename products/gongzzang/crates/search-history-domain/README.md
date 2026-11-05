---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# search-history-domain

`SearchHistory` 도메인 (RDS 동적) crate에요.

## 책임

- `search_history` 테이블(`migrations/20260719000103_insights_tables.sql`)에 대응하는 Aggregate를 정의해요.
- 사용자 검색 이력 append-mostly 기록 (매 검색마다 1 row INSERT).
- `BRIN` 인덱스 (`created_at`) 기반 시간순 조회 가정.
- `PIPA` 가명화 — `SearchHistory::pseudonymize`와 `SearchHistoryRepository::pseudonymize_older_than`
  (`user_id` → `NULL`, 대량 처리는 audit row 하나에 건수 기록)이 있어요. 마이그레이션 주석의 보존 정책은
  90일 가명화·1년 삭제지만, 이 메서드를 주기적으로 부르거나 오래된 row를 지우는 작업은 아직 없어요.
- `SearchHistoryRepository` trait — `find_recent_by_user` / `insert` / `pseudonymize_older_than`.
  `Postgres` 구현은 `gongzzang-persistence`의 `PgSearchHistoryRepository`.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `SearchHistoryMarker`, `MutationContext`).
