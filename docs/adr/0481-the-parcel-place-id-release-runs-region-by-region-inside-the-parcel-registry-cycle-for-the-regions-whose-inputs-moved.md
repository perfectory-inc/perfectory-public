# ADR 0481: 필지 단위 place_id 발행은 필지 ID 원장 주기 안에서, 입력이 바뀐 지역만 지역마다 한다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §8(등록부·다리·변경이력),
  [ADR-0243](./0243-the-parcel-registry-is-registered-region-by-region-by-a-scheduled-job.md)(원장은 지역마다 예약 작업이 등록),
  [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md) §5(place_id 가 조인 열쇠),
  [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md) §3(`views.changed_codes`)
- 관련: ADR-0480(서빙판의 코드 형태), ADR-0171(작업 결과 줄), ADR-0001

## Context

2026-11-11 운영을 읽기 전용으로 쟀다.

- `silver.parcel_registry` 는 17개 시도 39,862,511 개 필지 id 를 들고 있다. 모두 2026-06-01 판에서 등록됐다.
- 그러나 `gold.place_id_*` 에는 `unit = admin`(읍면동 5,067 id) 뿐이다.
- `place_id_release_to_gold --unit parcel` 은 코드로만 있다. 그것을 부르는 작업이 없다.
- 그런데도 파이프라인 그래프는 `silver-parcel-registry → gold-place-id-*` 세 간선을 `implemented` 라고 했다.
  주인은 읍면동 경계 작업(`admin-boundary-release`)으로 적혀 있었지만, 그 작업은 필지를 읽지 않는다.
- 옛 코드는 지역 하나의 행을 모두 드라이버 파이썬으로 모은다(전남 46 = 593만 행). 그리고 날짜 두 개를 손으로 받는다.
  예약 작업으로 돌릴 모양이 아니었다.
- 필지 id 가 없으니 원천들이 조인할 안정 열쇠도 없다(ADR-0288 §5).
- 2026-07-01 전남광주·인천 개편 아래 열린 필지 약 648만 개가 문제다(29: 387,255 · 46: 5,925,255 ·
  28: 148,273 · 27: 22,719). 서빙판(202606)은 이것들을 옛 코드로만 들고 있다.
  그래서 새 코드로 들어온 참조는 같은 땅을 찾지 못한다(ADR-0480).

## Decision

1. **발행은 필지 ID 원장 작업(`parcel_registry`, `scripts/ops/parcel-registry-cycle.sh`)의 3단계다.**
   - 새 유닛을 두지 않는다. 같은 지역 단위, 같은 Spark, 같은 시작 예산(100분)을 쓴다.
   - 등록(2단계)이 예산을 다 쓰면 다음 실행이 발행을 잇는다.
   - 그래프의 세 간선은 이 작업이 소유한다(정의 `parcel-registry`).
2. **지역(scope) = 그 id 가 처음 등록된 PNU 의 시도.** 이 값은 바뀌지 않는다.
   - 시도가 바뀐 필지(29 → 12)도 id 를 낸 지역에 남는다. 그래서 지역 하나의 발행은 제 id 만 읽는다.
3. **바뀐 지역만 읽는다.** `--probe-stale` 은 다음 셋 중 하나인 지역을 작은 것부터 낸다.
   - 한 번도 발행되지 않음
   - 지난 발행 뒤에 기록된 원장 행이 있음
   - 지난 발행 시각의 법정동 변경표와 지금 변경표 사이에 바뀐 코드(`views.changed_codes`, ADR-0329 §3)가
     그 지역의 열린 PNU 위에 있음. 그 시각의 변경표를 못 읽으면(스냅숏 만료) 그 지역을 다시 읽는다. 건너뛰지 않는다.

   바뀌지 않은 지역은 읽지 않는다. 물은 지역은 쓴다. 그래서 같은 변경으로 다시 읽히지 않는다.
   모든 지역의 첫 발행은 선언한 전량 예외다. 비교할 이전 판이 없다.
4. **한 지역의 발행(Spark, 드라이버로 모으지 않음, `parcel_place_id_release`).**
   - **등록부:** id 마다 가장 최근 행(같은 시각이면 current 가 이긴다).
   - **다리:** (id, PNU) 마다 하나의 기간.
     - 열린 PNU 의 코드를 변경쌍이 대체했으면 현재 코드로 한 줄 더 잇는다
       (`valid_from = valid_to = legal-dong-code-change`, admin 다리와 같은 말).
     - 두 id 가 같은 새 코드를 가지게 되면(통합된 두 옛 동의 같은 지번) 어느 쪽에도 잇지 않는다.
       "한 코드는 한 시점에 한 place_id" 계약 때문이다.
     - 원장이 스스로 새 PNU 를 가지면(판 이어 붙이기) 쌍 줄은 빠진다.
   - **변경 이력:** 그 지역의 발행된 등록부와 비교한다(added·retired·renumbered·redirected).
     등록부가 바뀐 때만 덧붙인다(append-only).
   - **쓰기:** `(unit=parcel, scope)` 파티션만 바꾼다(동적 파티션 덮어쓰기). 다른 지역과 admin 은 그대로다.
5. 결과 줄은 `released=…·unreleased=…` 를 싣는다. 아무 지역도 등록·발행하지 않으면 `unchanged` 다(ADR-0171).

## Consequences

- 계산(운영 읽기 2026-11-11):
  - 첫 발행 뒤 `gold.place_id_registry` 의 parcel id 는 0 → 39,862,511 이다.
  - 다리는 같은 수의 기간에, 새 코드 줄이 최대 약 648만 줄 더해진다(통합 충돌은 빠진다).
  - 첫 발행은 전량 예외다. 17 지역을 예산 안에서 작은 지역부터 하며, 며칠에 걸칠 수 있다.
  - 그 뒤 매일은 바뀐 지역만 읽는다. 바뀐 것이 없으면 Spark 탐색 한 번(원장 전체를 두 열로 훑음)으로 끝난다.
- 비용: 자체 서버 Spark. 가장 큰 지역(46, 593만 id)은 등록과 같은 규모다. 과금 없음.
- 원천 Gold 에 place_id 열을 붙이는 일(ADR-0288 §5 조인)은 이 발행을 입력으로 받는 다음 단계다.
- 위험: 첫 발행 날에는 등록이 끝난 뒤 남은 예산만 발행에 쓴다. 지금은 모든 지역이 등록돼 있어 예산 전부를 쓴다.
