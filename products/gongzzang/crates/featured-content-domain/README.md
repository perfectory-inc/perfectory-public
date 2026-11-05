---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# featured-content-domain

홈페이지 추천, 광고, 스폰서 노출을 소유하는 `FeaturedContent` 도메인 crate에요.

## 책임

- `featured_content` 테이블(`migrations/20260719000105_operations_tables.sql`)에 대응하는 Aggregate를 정의해요.
- **No OCC** — `version` 컬럼 없이 단순 UPDATE를 사용해요.
- `FeaturedContentRepository` (`save` / `find_by_id` / `find_active`)가 이 capability의 저장/조회 계약을
  소유해요. `Postgres` 구현은 `gongzzang-persistence`의 `PgFeaturedContentRepository`.

## FeaturedContent (홈페이지 추천/광고/스폰서)

- ID prefix **`fea`** (`shared-kernel`의 `FeaturedContentMarker`, 30자 ID 불변식의 3-char prefix).
- `target_kind` 3값 — `listing` / `industrial_complex` / `manufacturer`.
- `feature_kind` 4값 — `homepage_featured` / `search_top` / `sponsored_marker` / `newsletter`.
- `ends_at > starts_at` 불변식 — 도메인 검증 + DB CHECK (`migrations/20260719000110_featured_content_time_bound.sql`).
- `is_active_at(t)` — `starts_at <= t < ends_at` 인지 검사.
- `record_impression` / `record_click` — saturating 카운터 (race 허용).

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `FeaturedContentMarker`, `MutationContext`).
