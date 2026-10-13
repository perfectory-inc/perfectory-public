# ADR 0221: gongzzang 매물의 상태·가격 이력은 따로 덧붙이는 표다

- Status: Accepted
- Date: 2026-10-14
- Related: ADR-0220 (매물 조회 이벤트 기록)

## Context

매물의 과거 가격·상태를 아는 곳은 `audit_log.before_state`/`after_state` 뿐이었다. 거기에는 저장마다 행 전체가
jsonb 로 들어간다. 가격 변동이나 상태 타임라인을 보려면 감사 JSON 을 열어 키를 꺼내 비교해야 하고, 감사 기록은
"누가 무엇을 했나" 를 위한 것이지 제품이 읽는 이력 계약이 아니다(형태가 행 스키마를 따라 바뀐다).

## Decision

1. `listing_status_price_history(history_id, listing_id, listing_version, status, price_krw, deposit_krw,
   monthly_rent_krw, changed_at)` — 매물 행이 만들어질 때 한 줄, 그 뒤 `status`·`price_krw`·`deposit_krw`·
   `monthly_rent_krw` 중 하나라도 바뀐 갱신마다 한 줄.
2. 쓰는 주체는 DB 트리거(`trg_listing_status_price_history_insert`/`_update`) 하나다. 앱 코드 경로가 몇 개든
   (저장 upsert, 운영 조작) 빠뜨릴 수 없다. 함수는 `security definer` — 앱 역할에는 `select` 만 준다.
3. `changed_at` 은 그 버전의 `listing.updated_at`(도메인 시각)이다. 삽입 시각이 아니다.
4. 덧붙이기만: `UPDATE` 는 트리거(`trg_listing_status_price_history_no_update`, SQLSTATE 45000)가 거부한다.
   행은 매물이 지워질 때 `on delete cascade` 로만 떠난다.
5. 기존 매물은 마이그레이션 시점의 상태 한 줄로 이력을 시작한다(감사 JSON 에서 과거를 복원하지 않는다 — 유저 0).

## Consequences

- 통합 테스트 `each_status_or_price_change_appends_one_row_and_nothing_else_does` 가 제목만 바꾼 저장은 줄을 남기지
  않고, 가격·상태 변경은 정확히 한 줄씩 남는 것을 확인한다.
- 기각한 대안: (a) `save()` 에서 앱이 직접 쓰기 — 다른 쓰기 경로가 생기면 빠진다. (b) 감사 JSON 을 읽는 뷰 —
  감사 기록의 형태에 제품이 묶인다. (c) 상태·가격 표 둘 — 같은 버전의 두 사실을 두 곳에서 맞춰야 한다.
- 이 표를 읽는 API(가격 변동 표시)는 화면이 필요해질 때 더한다.
