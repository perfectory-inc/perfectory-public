---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# analysis-report-domain

`AnalysisReport` 도메인 (RDS 동적) crate에요.

## 책임

- `analysis_report` 테이블(`migrations/20260719000103_insights_tables.sql`)에 대응하는 Aggregate를 정의해요.
- 사용자가 다수 필지를 묶어 저장한 분석 리포트.
  - `target_pnus`: ≥1, ≤50 (응답 크기 제한).
  - `snapshot`: `R2` 데이터 시점 캐시 (`JSONB`).
- Optimistic locking (`version: i64`) — `rename` / `update_snapshot` 시 bump + `updated_at` 갱신.
  `updated_at` 컬럼은 `migrations/20260719000111_analysis_report_updated_at.sql`이 추가해요.
- `AnalysisReportRepository` trait — `Postgres` 구현은 `gongzzang-persistence`의 `PgAnalysisReportRepository`.

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `AnalysisReportMarker`, `Pnu`, `MutationContext`).
