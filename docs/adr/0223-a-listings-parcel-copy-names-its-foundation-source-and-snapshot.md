# ADR 0223: 매물에 복사한 필지 값은 Foundation 계약과 스냅샷을 함께 적는다

- Status: Proposed
- Date: 2026-10-14
- Related: [ADR-0096](./0096-parcel-attributes-are-served-from-pre-baked-r2-objects.md) (필지 문서는 Gold 스냅샷의 순수 함수), [ADR-0109](./0109-gongzzang-reads-parcel-and-building-detail-from-the-r2-edge.md) (공짱은 필지 상세를 R2 엣지에서 읽는다), GZ-ADR-0016 (매물 행에 필지 값 복사)

## Context

필지 데이터의 주인은 Foundation Platform 이고 공짱은 계약만 소비한다. 그런데 매물을 만들 때
(`create.rs` `populate_parcel_denormalize`) 필지 값 셋을 `listing` 행에 복사한다
(`20260719000114_listing_polygon_denormalize.sql`). 복사본은 한 번 쓰이고 다시 갱신되지 않는다.
주석이 말하는 "월간 재매핑 cron" 은 존재하지 않는다. 옆에 적힌 `parcel_lookup_at` 은 `now()` —
공짱이 *언제 물었는지*이지 Foundation 의 *어느 데이터*를 받았는지가 아니다. 그래서 어떤 복사본이 낡았는지
알아낼 방법이 없다.

복사된 칸을 읽는 곳 전수 (`git grep`, 2026-10-14 main):

| 칸 | 읽는 곳 | 용도 |
|---|---|---|
| `admin_code` | `card_summaries.rs` `admin_code LIKE $11 \|\| '%'` | 카드 검색 SQL 필터 |
| `parcel_land_use_type` | `card_summaries.rs` `parcel_land_use_type = $12` (+부분 인덱스) | 카드 검색 SQL 필터 |
| `parcel_zoning` | 없음 (쓰기만) | — |
| `parcel_lookup_at` | 없음 (쓰기만) | — |

마커 타일·정렬은 이 칸들을 읽지 않는다.

## Decision

1. **복사본은 남긴다 — 읽을 때 조회로 바꾸지 않는다.** 두 칸이 카드 검색의 `WHERE` 안에 있다. 읽을 때마다
   Foundation 엣지를 묻는 방식은 SQL 필터 안에 들어갈 수 없고, 후보 매물마다 HTTP 를 부르는 것은 검색이 아니다.
2. **복사본은 출처와 as-of 를 값 옆에 적는다.** 새 칸 둘:
   - `parcel_source_contract` — 엣지 문서의 `schema_version` (예: `foundation-platform.parcel_by_pnu_profile.v1`)
   - `parcel_source_snapshot_id` — 엣지 문서 `source.iceberg_snapshot_id`, 그 값이 구워진 Gold 스냅샷.

   as-of 를 시각이 아니라 스냅샷 id 로 적는 이유: ADR-0096 의 필지 문서는 벽시계를 담지 않는 Gold 스냅샷의
   순수 함수다. Foundation 이 그 데이터에 붙인 정체는 스냅샷 id 뿐이고, 낡음 판정에 필요한 것은 "지금 발행된
   스냅샷과 같은가" 하나다. 공짱의 `now()` 를 as-of 로 쓰는 것은 금지 — 그래서 `parcel_lookup_at` 은 삭제한다.
3. **출처 없는 복사본은 쓰지 않는다.** 엣지 문서가 `schema_version`·`source` 를 싣지 않으면 `ParcelInfo.source` 는
   `None` 이고, 매물 생성은 복사를 건너뛴다 (지목이 없을 때와 같은 분기). DB 도 `listing_parcel_copy_names_its_source_chk`
   로 거부한다: 지목·용도지역이 있으면 계약과 스냅샷이 반드시 있다. `NOT VALID` — 이 결정 이전에 쓰인 행은 갱신이
   다시 쓸 때까지 그대로 두고, 배포 마이그레이션이 기존 행 때문에 막히지 않는다.
4. **갱신은 이벤트로 한다.** Foundation 이 필지 엣지의 새 세대를 발행하면 그 사실을 기존 inbox
   (`foundation_platform_event_inbox`, scope `catalog`)로 보내고, 공짱은 `parcel_source_snapshot_id` 가 새 스냅샷과
   다른 매물만 다시 조회해 덮어쓴다. 오늘 Foundation 은 그런 이벤트를 내지 않는다 (발행되는 `catalog.*` 이벤트에
   필지 엣지 세대가 없다). 생산자가 생기는 PR 이 소비자와 inbox 의 event_type·effect 제약을 함께 넓힌다. 이 결정은
   그 갱신이 무엇을 비교할지를 먼저 행에 적어 둔다.
5. **`admin_code` 는 Foundation 데이터가 아니다.** PNU 앞 8자리에서 계산된다 (`admin_from_pnu`). PNU 는 매물의
   정체라 낡지 않는다. 후속으로 카드 검색이 `parcel_pnu` 접두사로 거르게 하면 이 칸은 지울 수 있다. 이 ADR 의
   CHECK 는 그래서 `admin_code` 를 묶지 않는다.

## Rejected

- **읽을 때 Foundation 조회 + 복사 칸 삭제**: 결정 1. 필터가 SQL 에 있다.
- **`parcel_lookup_at` 기준의 주기적 재조회 cron**: 공짱이 물은 시각은 데이터가 바뀌었는지 말해 주지 않는다.
  전 매물을 매번 다시 묻거나, 아무것도 모르는 채 기다리게 된다.
- **R2 객체 `Last-Modified` 를 as-of 로**: 엣지 Worker 가 넘겨 준다는 계약이 없고, 세대 재업로드 시각이지 데이터의
  정체가 아니다.

## Consequences

- 새로 만든 매물은 지목·용도지역과 함께 어느 계약·어느 Gold 스냅샷인지를 행에 갖는다.
- 출처를 싣지 않는 엣지 문서에서는 복사가 일어나지 않아, 그 매물은 지목 필터에 걸리지 않는다 (PNU 검색은 된다).
- 이 결정 전에 만든 매물은 출처가 비어 있다. 갱신 경로(결정 4)가 생기면 출처 없는 행도 "다른 스냅샷"으로 잡혀 다시 쓰인다.
