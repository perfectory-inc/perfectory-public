# ADR 0220: gongzzang 매물 조회수는 덧붙이기만 하는 이벤트 기록이다

- Status: Accepted
- Date: 2026-10-14
- Related: GZ-ADR-0032 (일관성 전략)

## Context

`listing.view_count` 는 매물 행의 열이었다. 두 경로가 같은 칸을 썼다.

- `increment_view_count`: `UPDATE listing SET view_count = view_count + 1, updated_at = now()` — `version` 은 그대로.
- `save` (낙관적 잠금 upsert): 미리 읽은 `Listing.view_count` 를 `view_count = excluded.view_count` 로 다시 썼다.

그래서 매물을 읽고 → 그 사이 조회가 쌓이고 → 저장하면 쌓인 조회가 사라졌다. 조회 증가는 `version` 을 올리지
않으니 잠금이 이 충돌을 못 본다. 또 조회 한 번마다 `updated_at` 이 바뀌어 "마지막 수정" 이 "마지막 조회" 가 됐다.
`bookmark_count` 도 같은 모양이었다: SP6-iii 에서 응답은 `bookmark_listing` 을 세도록 바뀌었지만 열과
`record_bookmark`/`release_bookmark`(deprecated) 는 남아 저장마다 다시 쓰였다.

## Decision

1. 조회는 `listing_view_event(view_event_id, listing_id, viewed_at)` 에 한 줄씩 덧붙인다
   (`ListingRepository::record_view`). 매물 행·`version`·`updated_at` 은 건드리지 않는다.
2. 조회수는 읽을 때 센다: 상세(`ListingDetail.view_count`)와 카드(`ListingCardSummary.view_count`)가
   `count(*)` 로 계산한다. 집계 루트 `Listing` 에는 조회수·북마크 수 필드가 없다 — 저장이 쓸 값이 없으니
   덮어쓰기는 컴파일 단계에서 불가능하다.
3. 덧붙이기만: `UPDATE` 는 트리거(`trg_listing_view_event_no_update`, SQLSTATE 45000)가 거부하고, 앱 역할에는
   `select, insert` 만 준다. 행은 매물이 지워질 때 `on delete cascade` 로만 떠난다.
4. 마이그레이션 `20261014090100_listing_view_event_log.sql` 이 기존 수를 이벤트로 옮기고(`viewed_at` 은 옛 열이
   남긴 유일한 시각인 `updated_at`) `view_count`·`bookmark_count` 열을 지운다.

## Consequences

- 매물 저장과 조회 기록이 서로의 값을 지우지 않는다. 통합 테스트
  `a_save_keeps_views_recorded_after_the_aggregate_was_read` 가 옛 동작에서 실패한다.
- 기각한 대안: (a) 저장 upsert 에서 `view_count` 만 빼기 — 같은 칸을 두 주인이 쓰는 구조가 남는다.
  (b) 조회에도 `version` 올리기 — 조회 한 번이 편집 중인 판매자의 저장을 충돌로 만든다.
- 비용: 읽을 때 `count(*)` (`listing_view_event_listing_idx`). 유저 0 인 지금은 충분하다. 매물당 조회가 커져
  카드 검색이 느려지면 이 기록에서 다시 만들 수 있는 집계 표(일 단위 롤업)를 더한다 — 원장은 그대로 이 표다.
