# ADR 0074: 호는 자기 건물에 매달리고, NULL 도 답이다

- Status: Accepted
- Date: 2026-09-03

## Context

계보의 마지막 끈이 비어 있다. ADR-0072 가 호를 필지에 직접 붙였고(의도된 첫 단계), ADR-0073 이
건물 7,603,113동을 그 사이에 세웠다. 이제 "202호가 **어느 건물**에 있는가" 만 남았다.

연결 재료는 정규화 때 이미 만들어져 있었고, 실측했다 (2026-09-03, Silver 조인):

| | |
|---|---|
| 호 전체 | 19,765,555 |
| `building_mgm_bldrgst_pk` 보유 | 19,625,450 (99.29%) |
| 그 열쇠가 표제부에 실존 | **19,624,045 — 보유분의 99.993%** |
| 열쇠는 있는데 표제부에 없음 | 1,405 |
| 연결 방법 | 동명 정합 15,366,049 · 필지 내 유일 건물 4,259,401 · 미해결 140,105 |

`catalog.building_unit` 은 건물 열쇠를 싣지 않았다 — 붙일 건물 표가 없던 시점의 투영이라서다.
열쇠는 Silver 에 있고, 건물의 `register_pk` 와 같은 대장 PK 체계다.

## Decision

1. **`catalog.building_unit.building_id uuid NULL`** 을 추가하고 `catalog.building(id)` FK 와
   인덱스를 건다. `parcel_id` 는 그대로 둔다 — "이 필지의 모든 호" 는 건물을 거치지 않는
   조회이고, 원천이 준 독립 사실이다. 건물 연결은 추가이지 대체가 아니다.
2. **NULL 은 실패가 아니라 답이다.** 세 경우가 NULL 로 남는다: Silver 가 미해결로 기록한
   140,105호, 열쇠가 표제부 밖인 1,405호, 그리고 건물이 orphan 이라 `catalog.building` 에
   없는 호. 셋 다 세어 요약에 남기고, 지어 붙이지 않는다.
3. **채움은 산술이다.** `building_id_for_register_pk(building_mgm_bldrgst_pk)` — 건물 행의
   id 가 이미 그 유도식으로 발급됐으므로 조회 없이 계산으로 잇는다. 단 FK 가 실존을
   요구하므로 `EXISTS(catalog.building)` 인 행만 채운다.
4. **관은 세 번째 매니페스트 관이다.** Spark 가 (호 register_pk, 건물 register_pk) 쌍을
   시군구별 객체 + 매니페스트로 내리고, Rust 명령이 스테이징 → `UPDATE ... FROM` 조인으로
   채운다. 계약·매니페스트 검증·판정은 `handoff_manifest_support` 를 그대로 쓴다.
5. **판정 등식**: `updated + unit_missing + building_missing = staged`. 갱신 대상 호가
   canonical 에 없거나(재적재 시차), 건물이 없어 못 채운 수를 따로 세고, 등식이 어긋나면
   완료를 거부한다.

## Consequences

- 필지 → 건물 → 호 3층 계보가 완성된다. "이 건물의 호 목록" 이 처음으로 서빙 가능해진다.
- 예상 채움율 상한은 99.28%이고, 실측 채움율과 세 NULL 사유의 실측 분해는 실행 요약이
  기록한다 — ADR-0073 각주의 전례대로, 실측이 이 문서의 각주로 돌아온다.
- 미해결 140,105호를 줄이는 일(동명 정규화 개선)은 이 ADR 의 범위 밖이며, Silver 의
  `building_link_method` 가 이미 그 작업의 원자료다.
