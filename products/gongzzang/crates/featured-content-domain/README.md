# featured-content-domain

홈페이지 추천, 광고, 스폰서 노출을 소유하는 `FeaturedContent` 도메인 crate에요.

## 책임

- spec § 5.5 `featured_content` 테이블에 대응하는 Aggregate를 정의해요.
- **No OCC** — `version` 컬럼 없이 단순 UPDATE를 사용해요.
- `FeaturedContentRepository`가 이 capability의 저장/조회 계약을 소유해요.

## FeaturedContent (홈페이지 추천/광고/스폰서)

- ID prefix **`fea`** — spec inline 은 `fc_` 로 적혀있지만 본 프로젝트 30자 ID 불변식 (3-char prefix) 충족 위해 `fea` 사용. Spec FU 11 에서 reconcile 예정.
- `target_kind` 3값 — `listing` / `industrial_complex` / `manufacturer`.
- `feature_kind` 4값 — `homepage_featured` / `search_top` / `sponsored_marker` / `newsletter`.
- **V003_03 invariant** — `ends_at > starts_at` (DB CHECK 동시).
- `is_active_at(t)` — `starts_at <= t < ends_at` 인지 검사.
- `record_impression` / `record_click` — saturating 카운터 (race 허용).

## 의존

- `shared-kernel` (`Id`, `UserMarker`, `FeaturedContentMarker`).
