# ADR 0383: 공매 Gold 는 짓기마다 모든 회차를 현재 법정동 코드로 옮기고, 코드 변경이 닿는 회차만 다시 짓는다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md)(정본 해석기 하나),
  [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md) §3(`views.changed_codes`),
  [ADR-0351](./0351-a-masked-lot-is-inferred-only-from-measured-evidence-and-every-link-follows-code-changes.md) §3,
  ADR-0382(경매의 같은 결정, PR #978)

## Context

공매 Silver(`data_go_kr_onbid.py`)는 적재 때 원천 코드(`ltnoPnu`)를 정본 해석기(`place_identity_resolver.resolve`)로
지금 PNU 로 바꾼다(ADR-0288 1단계). 하지만 Gold(`onbid_public_auction_silver_to_gold`)는 그 `pnu` 를 그대로 쓴다.
적재 뒤 법정동 코드가 바뀌면(분구·통합·번호 바꿈) 이미 실린 회차는 옛 코드에 남는다. 사장 규칙(2026-10-31):
모든 데이터는 법정동 코드 변경을 따라가야 한다.

운영 Silver 실측(2026-11-04, Trino 읽기 전용, 현재 행):

| | 회차 |
|---|---:|
| 열린 회차(삭제 표지 없음, 마감이 앞) | 48,580 |
| 그중 적재 때 `pnu` 가 정해진 것 | 37,478 |
| `pnu` 는 비었지만 원천 `ltno_pnu_raw` 가 19자리인 것 | 2,476 |

`reference.legal_dong_code_change` 의 쌍은 3,519 행이다.

## Decision

1. **짓기마다 옮긴다.** Gold 는 `reference.legal_dong_code_change` 를 필수 입력으로 고정(pin)한다. 회차의 자리는
   적재 때의 `pnu`, 비었으면 원천의 `ltno_pnu_raw`(19자리일 때)다. 이 값을 지금 쌍(`place_identity_resolver.crosswalk_of`,
   ADR-0329 의 대체 반영 view)으로 현재 코드로 옮긴다(`place_identity_resolver.located`). 지금 필지에 있는지는 여기서
   보지 않는다. 필지 이력 따라가기와 `servable`(ADR-0292)이 정한다. 옛 판과 새 판을 모두 같은 방식으로 옮기므로,
   코드 변경은 그 변경이 옮긴 행으로만 나타난다.
2. **바뀐 코드 아래만 다시 짓는다(ADR-0312 §2).** 코드 변경 표는 통째 입력이 아니다. 표가 바뀐 짓기는
   `views.changed_codes` 의 양쪽 코드 접두 아래 회차만 영향 키에 넣는다(`place_identity_resolver.under_changed_codes`,
   접두 길이마다 `isin` 하나). 물건 표가 바뀐 날은 지금처럼 통째다(작업의 `full_reason` 그대로).
3. **도구는 경매와 하나다.** `located`·`under_changed_codes`·`carried_to_current`·`crosswalk_of` 는 정본 해석기
   모듈에 있고, 경매(ADR-0382)와 공매가 같은 함수를 쓴다.
4. **시험.** 옛 코드로 적재된 회차가 새 쌍이 생긴 뒤 짓기에서 현재 필지로 옮겨지고, 영향 PNU 는 그 쌍의 코드
   아래뿐이다(`test_onbid_public_auction_gold.IncrementalParitySparkTest.test_a_code_change_re_places_a_round_and_reaches_only_the_rounds_under_it`,
   합성 99999 코드).

## Consequences

- 코드 변경 뒤 첫 짓기부터 공매 회차가 현재 코드의 필지에 실린다.
- `pnu` 가 비었던 2,476 회차(실측)가 원천 PNU 로 이력 따라가기를 탄다. 필지 패널에 없는 것은 지금처럼 `lineage_missing`
  으로 세고, 서빙 Gold 에는 넣지 않는다(ADR-0292).
- 비용: 짓기마다 코드 변경 표 약 3,500 행을 한 번 읽는다(필지 패널과 같다). Spark 는 자체 서버. 공매 Gold 는 하루
  한 번 통째 짓기이고(2026-10-28 실측 143 초) 그 시간은 거의 그대로다.

## Rejected

- **Silver 를 다시 적재해 고치기:** 적재 때 한 번 정하는 구조를 그대로 두는 일이다. 다음 코드 변경에서 또 남는다.
- **코드 변경 때 공매 Gold 통째 재생성:** 바뀐 코드 아래 회차만 닿는다(ADR-0314).
