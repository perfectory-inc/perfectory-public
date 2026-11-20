# ADR 0596: 주변 Gold 는 상가 수만 바뀐 커밋을 스스로 설명하고, 굽기는 그 설명으로 전량을 굽는다 — 주변이 플랫폼 GIS 로 옮길 때까지

- Status: Accepted
- Date: 2026-11-20
- Amends: [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)
  §3.4("변경 비율 상한을 넘으면 거부 — … 넘는 변경을 싣는 것은 사유를 적은 운영자 실행이다"),
  [ADR-0346](./0346-a-change-set-over-the-delta-bound-goes-full-when-a-record-explains-it.md)(상한을 넘는 변경 집합은 기록이 설명하면 전량)
- Builds on: [ADR-0465](./0465-a-parcel-shows-what-is-near-it-computed-in-gold-with-sedona-and-h3.md) §1·§3(상가 수, 바뀐 H3 칸 둘레만 다시 계산),
  [ADR-0567](./0567-the-parcel-nearby-gold-is-born-on-duckdb-as-a-table-of-row-versions-with-the-exact-nearest-and-ties-broken-by-the-facility-key.md)
- 성격: **일부러 둔 임시 장치다.** 주변이 플랫폼 GIS 로 옮기면 지운다(공간 관계 검토 2026-11-20 §8, 저장소 밖의 설계 검토).

이 글의 낱말: **필드**는 문서 JSON 의 키, **H3 칸**은 해상도 9 의 육각 칸, **상가 적재본**은 `silver.commercial_district` 의 한 스냅숏,
**Gold 판**은 `gold.parcel_nearby_versions` 의 `version_seq`, **서빙 필지 판**은 서빙하는 필지 edition 이다. **커밋 상한**은 Gold 커밋의
`MAX_CHANGED_KEY_FRACTION`(0.5), **델타 상한**은 굽기의 `max_delta_fraction`(0.5)이다.

## Context

**예견된 멈춤(모형, 2026-11-20).** 다음 분기 상가 적재본으로 주변 레인이 두 곳에서 멈출 수 있다.

- Gold 커밋(`parcel_nearby_versions_build`): 코드·서빙 필지 판이 그대로인데 현재 행의 커밋 상한을 넘게 바뀌면, 아무것도 덧붙이지 않고
  실패한다. ADR-0564 §3.4 의 "사유를 적은 운영자 실행"은 실제 스위치가 없다.
- 굽기의 변경 집합(`export-gold-versions-change-set`): 델타 상한을 넘고 기록이 설명하지 않으면 `not_a_delta` 로 거부한다. 그 거부는
  Dawneer 의 `force_full` 승인을 기다린다. 사람의 손이다.

**상가 갱신 한 번이 상한을 넘을 수 있다**(11-19 Gold snapshot 1517963643317918524, 읽기 전용 측정):

- 필지 39,861,511 개 가운데 32,039,468 개(80.38%)가 `commercial_store_count` > 0 이다. 상가 2,753,778 곳. 필지당 H3 칸 19 개 안의 수는
  사분위 1 / 5 / 38, p90 511, 평균 168.
- Silver 에는 상가 적재본이 하나뿐이어서(11-12/13) 분기 차이를 직접 잴 수 없다. 실제 수로 모형을 세웠다: 상가의 몫 f 가 바뀌면 필지는
  그 칸들의 상가 n 곳이 하나도 안 바뀔 때만 수가 그대로다, (1−f)^n. 바뀌는 필지의 몫은 f = 1% 24.1%, 5% 40.2%, 10% 49.2% 이고,
  빈 칸에 새로 연 상가가 더한다.
- 분기마다 바뀌는 상가의 몫은 잰 적이 없다(가정). 몇 % 면 40–50% 이상이고, 상한에 닿거나 넘는다.

이 변경은 표류가 아니다. 입력 하나(상가)가 실제로 바뀌었고, 바뀐 것은 문서의 필드 하나다. 그래도 상한은 다른 원인들(지문 표류,
잘못된 기준, 깨진 입력)을 위해 남아야 한다. Silver 적재기는 상가 행 수를 이미 막는다(`data-go-kr-file-data.contract.json`
`commercial_district.member.bounds`: `min_rows` 2,480,000, `max_shrink_share` 0.15, `data_go_kr_file_data.py`). 그러나 좌표를 못 읽는
적재본은 행 수가 그대로여서 그 문을 지난다.

## Decision

1. **Gold 커밋은 상가만의 변경을 스스로 확인한다.** 크기와 상관없이, 다음이 모두 참이면 상가만의 변경이다:
   - 움직인 입력이 `silver.commercial_district` 하나뿐이다.
   - 위치가 있는 상가(한국 범위 안의 좌표)가 0 이 아니고, 머리의 수에서 `kind_tables.max_row_loss_fraction`(gold-panel-rebuild
     계약, 커밋의 행 하한과 같은 값) 넘게 늘거나 줄지 않았다. 비거나 깨진 적재본은 여기서 걸린다.
   - 새로 생기거나 빠진 키가 없다(`added` = `removed` = 0).
   - 바뀐 행마다 머리의 행과 `commercial_store_count` 필드가 다르고, 그 필드를 지운 나머지 `nearby_json`
     (`json_merge_patch(…, '{"commercial_store_count": null}')`)과 `place_id`·`attached_via` 가 같다.
   - 바뀐 필지마다 그 H3 칸이, 상가가 들거나 난 H3 칸의 둘레 두 겹 안이다(`changed_sidos` 가 구하는 `ChangedSidos::store_cells`).
2. **커밋은 그 이유를 적는다.** 상가만의 변경이면, 커밋 상한 아래여도 스냅숏 요약에 `foundation.recorded-explanation`("store-only: …")을
   적는다. 상한 아래의 상가만 커밋이 여럿 쌓이면, 기록 없이 굽기의 델타 상한을 넘기 때문이다. 코드나 서빙 필지 판이 움직인 커밋도 같은
   속성에 그 사실을 적는다. 그런 커밋은 지금도 크기와 상관없이 받아들여진다.
3. **굽기는 그 기록을 받아들인다.** 이 기록의 이름은 계약의 `over_max_delta_fraction.explained_by` 에 `recorded_explanation` 으로 더했다.
   Rust 변경 집합과 Spark 델타 작업이 같은 목록을 읽는다. Spark 델타 작업은 이 이름을 찾지 않는다. 행 판 표에만 있다.
   - 반영한 Gold 판 다음부터 새 스냅숏까지 바뀐 키가 현재 행 × 델타 상한을 넘으면, `explained_full`(전량, 사람 없음)이 되는 조건은 둘이다.
     (a) 그 사이 커밋 가운데 하나 이상이 기록을 적었다. (b) 기록 없는 커밋들의 `foundation.keys-changed` 합이 현재 행 × 델타 상한 이하다.
   - 바뀐 키 수를 적지 않은 커밋은 상한 밖으로 센다. 상한 안이면 지금처럼 `delta` 다.
   - 변경 집합은 Spark 델타 작업과 같은 모양을 쓴다: `explained_by` 는 이름 → 이유, `explanations_checked` 는 확인한 이름들.
4. **상한은 다른 모든 원인에 그대로다.**
   - 커밋 쪽: 거리·이름·출입시설이 바뀌었거나, 칸 밖의 필지가 바뀌었거나, 다른 입력도 움직였거나, 위치 있는 상가 수가 범위를 벗어났으면
     설명이 붙지 않는다. 그러면 커밋 상한을 넘을 때 지금처럼 실패한다.
   - 굽기 쪽: 기록 없는 커밋들만으로 델타 상한을 넘으면 지금처럼 `not_a_delta` 다.
5. 시험이 막는 예견된 멈춤(루트 ADR-0001):
   - `parcel_nearby_versions_build::tests`: 상가 수만 바뀌면 설명이 붙는다. 거리·`place_id`·`attached_via`·수가 안 바뀐 표류·다른 입력·
     새 키·빠진 키·칸 밖·비거나 반이 된 상가 적재본은 아니다. 상한 아래의 상가만 커밋도 기록된다.
   - `parcel_nearby_stage::tests`: 움직인 상가는 두 H3 칸의 둘레 두 겹을 건드린다. 둘째 겹은 안, 셋째 겹은 밖이다.
   - `gold_versions_change_set::tests`: 기록된 상가만 커밋은 `explained_full`, 기록 없는 나머지가 상한을 넘거나 기록이 없으면 `not_a_delta`.
     이름은 계약 목록에 있다.
   - `test_by_pnu_serving_bake_versions`: 실제 모양의 `explained_by` 로 굽기가 전량으로 간다(EGM 2026-11-20 B1: 목록 모양이면 증거 읽기가
     죽어 exit 1).

## Consequences

- §1 의 조건을 모두 만족하면, 다음 상가 적재본은 사람 없이 커밋되고 전량으로 구워진다. Gold run 은 그 이유를 run.log 와 요약
  (`recorded_explanation`)에 남긴다. 굽기 요약은 `explained_by` 에 남긴다.
- 전량 굽기는 그때마다 주변 문서 3,986만 개를 다시 쓴다. 비용은 첫 발행의 bake 가 처음 잰다(그 run 의 굽기 요약 `seconds`). 고칠 곳은
  끝 문단이다. 플랫폼 GIS 의 요청 경로는 문서를 굽지 않는다.
- 실제 첫 확인은 다음 분기 상가 적재본이 들어올 때다. 저장소의 시험은 합성 표다(PNU 99999…).
- 이 장치는 주변 전용이다. 다른 레인이 같은 일을 하려면 자기 ADR 이 필요하다.
- **이 ADR 의 끝.** 주변 배치가 은퇴하면(플랫폼 GIS 로 옮김, 공간 관계 검토 §8 단계 8) 다음을 지운다: `store_only`·`STORE_COUNT_FIELD`·
  `recorded_or_refused` 의 상가 몫, `parcels_outside_cells`·`store_cell`·`ChangedSidos::store_cells`·`located_stores`·`cells_around_moves`,
  `RECORDED_EXPLANATION_PROPERTY`·`recorded_explanations`·`finish` 의 그 사용, 계약의 `recorded_explanation`, 이 ADR 의 시험들. 그때까지 다른
  작성자가 생겼으면 그 몫만 남긴다.

## 기각한 대안

- **상한을 올린다.** 다른 모든 원인의 문까지 넓힌다.
- **분기마다 `RULES_VERSION` 을 손으로 올린다.** 코드 판 이동으로 설명되지만, 사장이나 운영자가 매 분기 해야 한다.
- **ADR-0564 §3.4 의 사유 적은 운영자 실행.** 스위치가 없고, 있어도 사람의 손이다.
- **기존 `recorded_change_set` 에 싣는다.** 그 이름은 Gold 커밋이 바뀐 키를 `__changes` 에 기록했다는 뜻이다(ADR-0312). 행 판 표는 이미
  `version_seq` 로 바뀐 키를 안다. 이유를 적는 것과는 다른 기록이다.
