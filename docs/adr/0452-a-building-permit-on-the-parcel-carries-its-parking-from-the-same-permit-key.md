# ADR 0452: 필지 화면의 건축인허가는 같은 허가 키의 주차장 대수를 함께 싣는다

- Status: Accepted
- Date: 2026-11-09
- Builds on: ADR-0402(건축인허가 주차장 Silver), ADR-0451(건축인허가 종류), ADR-0336(스키마 레지스트리 개정),
  ADR-0312 §2(바뀐 키만)

## Context

`silver.building_permit_parking`(건축HUB 건축인허가 주차장, 매달 적재·켜짐)은 정의에서 만든 표(`production-readiness` §10)의
"없음" 줄이었다. ADR-0402 는 이 표가 "물류 차량을 받을 수 있는 필지인지" 를 말한다고 적고, 소비자를 다음 PR 로 미뤘다.

**측정(ADR-0402, 2026-10 판).** 2,835,072 행, 관리허가대장PK 가 행마다 다르다 — 건축인허가 기본개요(ADR-0451)와 **같은 키**다.
옥내자주식·옥외자주식·옥내기계식 대수와 면적이 짝으로 있다.

주차장은 허가의 속성이다. 따로 종류를 두면 화면이 같은 허가를 두 구획에 나눠 보이고, 두 문서를 PNU 로 다시 맞춰야 한다.

### 실사례

- **같은 키의 사실은 읽기 모델에서 한 레코드로.** 정규화된 원천(허가 ↔ 주차장)을 화면용 문서로 펴는 것은 CQRS 읽기 모델의 기본
  모양이다. CoStar 의 건물 화면도 주차 대수를 건물 레코드 안의 한 칸으로 둔다.
- **필드를 더하는 개정은 같은 major.** Confluent Schema Registry 의 `FULL_TRANSITIVE` 에서 선택 필드 추가는 앞뒤로 호환된다.

## Decision

1. **허가 레코드에 세 칸.** `parcel-building-permits` 문서의 `building_permits.records[]` 가 같은 관리허가대장PK 의 주차장 행에서
   `indoor_self_parking_count`·`outdoor_self_parking_count`·`indoor_mechanical_parking_count` 를 갖는다(원천 글자 그대로, 빈 값
   null, 주차장 행이 없는 허가는 셋 다 null). 면적은 싣지 않는다 — 화면이 대수만 읽는다.
2. **레지스트리 개정 1.2**(`parcel_building_permits_by_pnu/1/2.schema.json`), `schema_version` 은 `.v1` 그대로. 웹 pact 가 세 칸을
   읽는다(`pnpm gen:schemas`).
3. **부분 처리.** 주차장은 PNU 가 아니라 허가 키로 붙는다. 그래서 주차장 행이 바뀐 키(`changed_rows`, 양방향)를 고정한 허가와 지금
   허가 둘 다에서 PNU 로 바꿔(`parking_pnus`) 그 PNU 만 다시 계산한다. 부분 빌드의 입력 자르기도 남은 허가의 키로 주차장을 자른다.
   법정동 코드 변경은 허가 행이 따라가므로(ADR-0451 §3) 주차장 쪽에 따로 둘 것이 없다.
4. **화면.** 허가마다 한 줄 더: "옥내자주식 N대 · 옥외자주식 N대 · 옥내기계식 N대". 0 이나 빈칸은 보이지 않는다.

## Consequences

- §10 표에서 건축인허가 주차장 줄이 "없음 → 길 있음·꺼짐" 이 된다(정의에서 계산, 연결 관계 `joins`). 작업이 켜지면 기본개요와
  함께 "화면" 이다.
- **비용(계산).** 빌드마다 283만 행 Silver 의 네 칸을 한 번 더 읽고 키로 한 번 붙인다 — 653만 행 기본개요 빌드에 수 분을 더한다.
  Gold 문서는 허가마다 수십 바이트 늘어난다.
- 부설주차장(`building_permit_annex_parking`)은 Silver 가 아직 없다. 생기면 같은 키로 이 레코드에 더한다.
