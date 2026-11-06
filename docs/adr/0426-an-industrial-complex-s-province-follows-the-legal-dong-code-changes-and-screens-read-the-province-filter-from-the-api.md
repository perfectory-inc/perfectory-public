# ADR 0426: 산업단지의 시도·시군구는 법정동 코드 변경을 따라가고, 화면의 시도 필터는 API 에서 읽는다

- Status: Accepted
- Date: 2026-11-05
- Builds on: ADR-0103 §2(코드→지역 상수 금지), ADR-0145(코드 변경 기록 하나), ADR-0288 §1(장소 식별 해석기 하나),
  ADR-0329 §3(`views.changed_codes`), ADR-0372(통합 전 시도 코드는 기록된 쌍으로 읽는다), ADR-0403 §4(남은 손 목록 셋)

## Context

2026-07-01 광주(29)와 전남(46)이 전남광주통합특별시(12)로 합쳐졌다. 법정동 표(`reference.legal_dong_code_snapshot`)와
코드 변경 기록(`reference.legal_dong_code_change`)은 이미 `2900000000 → 1200000000`, `4600000000 → 1200000000` 과
시군구 쌍(`2917000000 → 1230000000` 등 27개)을 적었다. 그런데 운영 `catalog.industrial_complex` 는 2026-11-04 에 아직
시도 29 를 19개, 46 을 116개 단지에 싣는다. ADR-0403 §4 는 그래서 화면의 손 목록 셋(더니어 `SIDO_LABEL`, 공짱
`SIDO_CODES` 와 `ko.json` 의 `complexes.sido.*`)을 지우지 못하고 줄어들기만 하는 목록에 남겼다.

**원인(세 겹).**

1. 원천이 옛 코드를 준다. 단지의 시도·시군구는 주소 해석 단계(`build-industrial-complex-address-resolution`)가 ILIS 목록의
   지역 코드와 브이월드 시군구 경계 DBF 에서 읽는다. 2026-10-01 적재 때도 둘 다 29·46 이었다.
2. 변환을 적재 때 한 번만 했다. Silver → Gold(`industrial_complex_silver_to_gold.py`)는 코드를 그대로 옮겼고, 어느 단계도 코드
   변경 기록을 읽지 않았다.
3. 다시 놓는 단계가 없다. 산업단지 사슬(주소 해석 → Bronze → Silver → Gold → canonical 적재)은 운영자가 손으로 돌린다.
   코드 변경 기록이 바뀌어도 그것을 읽고 단지를 다시 놓는 작업이 없었다. `catalog.industrial_complex` 의 모든 행은
   2026-10-01 05:34 에 한 번 쓰인 뒤 바뀌지 않았다.

**이미 있는 것(새로 만들기 전 조사, 저장소 전체의 `*.py`·`*.rs`·`*.sh`·`*.json`·`*.ts`).**

- 코드 사슬을 현재 코드까지 따라가는 해석기: `place_identity_resolver.code_crosswalk`(ADR-0288 §1).
- 표의 두 읽기 사이에서 바뀐 짝의 코드: `legal_dong_code_change_views.changed_codes`, 그 코드가 덮는 접두어 `code_prefix`.
- 표를 쓰는 단위: `lineage_stewardship`(`legal-dong-code-load.sh`)뿐이다. 소비자는 그 뒤에 붙는다.
- Gold → `catalog.industrial_complex` 적재(`load-industrial-complex-canonical`)는 바뀐 행만 쓴다(`Unchanged` 는 쓰지 않는다).
- 단지 목록 API(`GET /catalog/v1/complexes`)와 그 OpenAPI 문서, 공짱·더니어의 생성된 타입.
- Postgres 의 행정 단위 표(`catalog.administrative_unit`)는 2026-06-01 판(경계 원천)에 멈췄고 시도 행이 0 이다. 법정동 표가
  아니라서 시도 이름의 원천으로 쓰지 않는다.

## Decision

1. **단지의 시도·시군구는 원천 코드를 기록된 짝으로 현재까지 옮긴 것이다.** `place_identity_resolver.current_region` 이
   각 단계를 10자리 코드(시도 `xx00000000`, 시군구 `xxxxx00000`)로 읽어 `code_crosswalk` 의 사슬을 따른다. 갈라지는
   코드는 그 단계의 코드를 두지 않는다(스튜어드 몫). 가장 세밀하게 풀린 단계가 위 단계를 정한다(시군구가 제 쌍으로 시도를
   옮기면 시도도 따라간다). Gold 의 새 칸 `sido_name` 은 현재 시도 코드가 가장 새 법정동 스냅숏에서 갖는 이름이다
   (`sido_names`: `xx00000000` 행, 없으면 그 밑 가장 작은 현존 코드의 첫 낱말). Gold 를 지을 때마다 이렇게 계산한다.
   살아 있는 Gold 는 두 표 없이 짓지 않는다(`build_region` 이 거부한다).
2. **코드 변경이 닿는 단지만 다시 놓는다.** `industrial_complex_silver_to_gold.py --follow-region-codes` 가 하는 일은 다음과 같다.
   - 지난 따라가기가 읽은 변경표 행(상태 파일)과 지금 행을 `views.changed_codes` 로 비교한다.
   - 바뀐 코드의 접두어 밑에 원천 코드나 Gold 코드가 있는 단지만 다시 계산한다.
   - 계산한 칸이 Gold 와 다른 단지만 `MERGE` 로 바꾼다(`sido_code`·`sigungu_code`·`sido_name`).
   - 바뀐 코드가 없으면 아무것도 읽거나 쓰지 않는다.
   - 상태 파일이 없는 첫 따라가기는 모든 단지에 닿는다.
   `lineage_stewardship` 은 법정동 단계가 짝을 새로 썼거나 아직 한 번도 따라가지 않았을 때 이것을 돌리고 canonical 적재를 잇는다.
   상태 파일은 둘 다 성공한 뒤에만 바뀐다. 실패하면 계보는 돌고 단위는 끝에 실패한다.
   canonical 적재는 Gold 를 읽기 키 한 쌍으로 읽는다(ADR-0152).
3. **`catalog.industrial_complex.sido_name`**(새 칸, 비어도 된다)은 canonical 적재가 Gold 에서 쓴다. 이름이 바뀌어도 그 행만
   `Updated` 다.
4. **시도 필터의 선택지는 API 가 준다.** `GET /catalog/v1/complexes` 응답에 `sido_facet` 이 생긴다. 살아 있는 단지가 하나라도 있는
   시도마다 `{sido_code, sido_name?, complex_count}` 를 코드 순으로 준다. 요청의 거르기와 무관하다. 공짱 API 는 그대로 넘긴다.
   공짱 웹과 더니어는 선택지와 이름을 여기서 읽는다(이름이 없으면 코드를 보인다). `SIDO_LABEL`, `SIDO_CODES`(`lib/complexes/sido.ts`),
   `complexes.sido.<code>` 이름은 지운다. `tools/ci/hardcoded-sido-codes.baseline.json` 은 비고(`files: []`), 가드
   `sido-codes-come-from-the-legal-dong-table` 가 새 손 목록을 막는다.
5. `primary_bjdong_code` 는 원천이 채우는 행이 0 이라(ADR-0040) Gold 에 없다. 채워지면 같은 `current_region` 이 10자리로 옮긴다.

**기각한 대안.**

- 화면 목록만 12 로 고치기. 필터가 29·46 에 남은 135개 단지를 못 찾는다(ADR-0403 §4).
- 읽을 때 Postgres 에서 바꾸기. 법정동 표와 변경표가 Postgres 에 없고, 행정 단위 표는 경계 원천이라 판이 늦다.
- 새 예약 작업. 변경표를 쓰는 단위는 하나다. 그 뒤에 붙이면 따라갈 때를 따로 맞출 필요가 없다.
- Gold 전량 다시 쓰기. 1,442행이라 싸지만 바뀌지 않은 단지까지 다시 쓰고, canonical 적재의 `Updated` 신호가 흐려진다.
- Postgres 에 시도 참조 표를 따로 두기. 표·이관·권한·원장이 하나씩 늘고, 필터가 필요한 것은 단지가 있는 시도의 이름뿐이다.

## Consequences

- **측정(2026-11-04·05, 운영 읽기 전용).** 고치기 전은 `catalog.industrial_complex` 의 실측이다. 고친 뒤는 같은 행
  (시도·시군구·법정동 묶음 181개)에 운영 `reference.legal_dong_code_change` 3,518행과 가장 새 스냅숏(2026-10-04, 53,402행)으로
  `current_region`·`sido_names` 를 적용해 계산한 값이다.

  | 시도 | 고치기 전 | 고친 뒤(계산) | 이름(스냅숏) |
  |---|---:|---:|---|
  | 12 | 0 | **135** | 전남광주통합특별시 |
  | 29 | **19** | 0 | (폐지) |
  | 46 | **116** | 0 | (폐지) |
  | 그 밖 15개 시도 | 1,171 | 1,171 | 바뀌지 않음 |
  | 코드 없음 | 1 | 1 | |

  시군구도 135개 단지가 옮겨 가고, 갈라져 코드를 잃는 단지는 0 이다.
- 첫 따라가기는 모든 단지에 닿는다. 시도·시군구는 135개 행을, `sido_name` 은 처음 채우므로 이름이 있는 1,441개 행을 바꾼다.
  그 뒤로는 바뀐 짝 밑의 단지만 바뀐다.
- 비용: 따라가기는 짝이 바뀐 날에만 Spark 한 번(변경표 약 3,500행, 스냅숏 약 5만 행, Gold·Silver 각 1,442행을 읽는다)과
  canonical 적재 한 번이다. API 는 목록 요청마다 1,442행 `GROUP BY` 한 번을 더 한다.
- 원천 쪽(주소 해석이 옛 코드를 받는 것)은 그대로다. 이 결정은 원천이 늦어도 단지가 따라가게 한다.
- 단지 목록 응답에 필수 칸 `sido_facet` 이 생긴다. 공짱의 계약 고정본(`foundation-platform-client/openapi/catalog.v1.json`)과
  생성 타입을 함께 바꾼다.
