# ADR 0596: 상가만 바뀐 주변 커밋은 절반 넘게 바뀌어도 그 이유를 기록하고, 굽기는 그 기록으로 전량을 굽는다

- Status: Accepted
- Date: 2026-11-20
- Amends: [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)
  §3-4(코드·판이 그대로인 커밋은 현재 행의 절반을 넘게 바꾸지 못한다),
  [ADR-0346](./0346-a-change-set-over-the-delta-bound-goes-full-when-a-record-explains-it.md)(상한을 넘는 변경 집합은 기록이 설명하면 전량)
- Builds on: [ADR-0465](./0465-a-parcel-shows-what-is-near-it-computed-in-gold-with-sedona-and-h3.md) §1·§3(상가 수, 바뀐 칸 둘레만 다시 계산),
  [ADR-0567](./0567-the-parcel-nearby-gold-is-born-on-duckdb-as-a-table-of-row-versions-with-the-exact-nearest-and-ties-broken-by-the-facility-key.md)

## Context

**주변 레인은 다음 분기 상가 파일에서 두 곳에서 멈춘다.**

- Gold 커밋(`parcel_nearby_versions_build`): 코드·서빙 판이 그대로인데 현재 행의 0.5(`MAX_CHANGED_KEY_FRACTION`)를 넘게 바뀌면
  아무것도 덧붙이지 않고 실패한다.
- 굽기의 변경 집합(`export-gold-versions-change-set`): `max_delta_fraction` 0.5 를 넘고 기록이 설명하지 않으면 `not_a_delta` 로
  거부한다. 그 거부는 Dawneer 의 `force_full` 승인을 기다린다. 사람의 손이다.

**상가 갱신 한 번이 0.5 를 넘을 수 있다**(2026-11-20, 11-19 Gold snapshot 1517963643317918524, 읽기 전용 측정):

- 필지 39,861,511 개 가운데 32,039,468 개(80.38%)가 `commercial_store_count` > 0 이다. 상가 2,753,778 곳, 필지당 칸 19 개
  안의 수는 사분위 1 / 5 / 38, p90 511, 평균 168.
- Silver 에는 상가 판이 하나뿐이어서(11-12/13) 분기 차이를 직접 잴 수 없다. 실제 수로 모형을 세웠다. 상가의 몫 f 가 바뀌면
  필지는 그 칸들의 상가 n 곳이 하나도 안 바뀔 때만 수가 그대로다: (1−f)^n. 바뀌는 필지의 몫은 f = 1% 24.1%, 5% 40.2%,
  10% 49.2% 이고, 빈 칸에 새로 연 상가가 더한다.
- 분기 폐업·개업 몇 % 씩이면 40–50% 이상이다. 상한에 닿거나 넘는다.

이 변경은 표류가 아니다: 입력 하나(상가)가 실제로 바뀌었고, 바뀐 것은 문서의 한 칸이다. 그래도 0.5 의 문은 "코드·판이 그대로인데
대부분이 바뀌면 의심하라"는 다른 원인들(지문 표류, 잘못된 기준, 깨진 입력)을 위해 남아야 한다.

## Decision

1. **Gold 커밋은 상가만의 변경을 스스로 확인하고 기록한다.** 코드·서빙 판이 그대로이고 바뀐 키가 0.5 를 넘을 때, 다음이 모두
   참이면 커밋한다:
   - 움직인 입력이 `silver.commercial_district` 하나뿐이다.
   - 새로 생기거나 빠진 키가 없다(`added` = `removed` = 0).
   - 바뀐 행마다 머리의 행과 `nearby_json` 이 `commercial_store_count` 한 칸만 다르다(`json_merge_patch` 로 그 칸을 지운 뒤 같다).
     `place_id`·`attached_via` 도 같다.
   - 바뀐 필지마다 그 칸이, 상가가 들거나 난 칸의 둘레 두 겹 안이다(`changed_sidos` 가 이미 구하는 칸,
     `ChangedSidos::store_cells`).
   - 그 커밋은 스냅숏 요약에 `foundation.recorded-explanation` 을 적는다("store-only: …"). 아니면 지금처럼 실패한다.
2. **굽기는 그 기록을 받아들인다.** `export-gold-versions-change-set` 은 반영한 판 다음부터 새 판까지의 커밋을 본다. 하나라도
   `foundation.recorded-explanation` 을 적었고, 적지 않은 커밋들의 `foundation.keys-changed` 합만으로는 `max_delta_fraction`
   안이면, `explained_full` 로 전량을 굽는다(ADR-0346 의 길, 사람 없음). 바뀐 키 수를 적지 않은 커밋은 상한 밖으로 센다.
3. **0.5 는 다른 모든 원인에 그대로다.** 거리·이름·출입시설이 바뀌었거나, 칸 밖의 필지가 바뀌었거나, 다른 입력도 움직였거나,
   기록 없는 커밋들만으로 상한을 넘으면 두 곳 모두 지금처럼 멈춘다.
4. 시험이 막는 실제 사고(루트 ADR-0001): **다음 분기 상가 파일로 주변 레인이 두 곳에서 얼고 사람의 승인을 기다린다.**
   - `parcel_nearby_versions_build::tests` 의 두 시험: 상가 수만 바뀌면 설명이 붙는다. 거리·`place_id`·칸 밖·다른 입력·새 키는
     아니다.
   - `gold_versions_change_set::tests` 의 두 시험: 기록된 상가만의 커밋은 `explained_full` 이다. 기록 없는 나머지가 상한을
     넘거나 기록이 없으면 `not_a_delta` 다.

## Consequences

- 다음 상가 판은 사람 없이 커밋되고 전량으로 구워진다. 그 run 은 바뀐 몫과 그 이유를 run.log 와 요약
  (`recorded_explanation`)에 남긴다.
- 전량 굽기는 그 판마다 주변 문서 3,986만 개를 다시 쓴다. 굽기 시간은 레인의 첫 발행이 처음 잰다.
- 다른 행 판 레인도 자기 입력의 확인 규칙을 가지면 같은 성질의 `foundation.recorded-explanation` 을 적을 수 있다. 굽기 쪽은
  레인을 가리지 않는다.
- 상가 갱신의 자동 시험은 저장소 안의 합성 표다. 실제 첫 확인은 다음 분기 상가 판이 들어올 때다.
