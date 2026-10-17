# ADR 0273: 패널 Gold 는 법정동 코드 변경·용도지역 코드·필지 계보의 변경도 PNU 로 사상한다

- Status: Accepted
- Date: 2026-10-18
- Changes: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) §2(용도지역 코드표는
  PNU 로 사상하지 않는다)·§4(`WHOLE_TABLE_INPUTS` 이면 전량)
- Keeps: ADR-0180 §1(바뀐 행 = 빌드가 읽는 열이 다른 행)·§3(copy-on-write 합치기)·§4의 나머지 전량 조건
  (`max_changed_key_fraction`, 패리티 표본)·§5·§6

## Context

- `gold.parcel_panel` 은 `reference.legal_dong_code_change`, `silver.land_use_zone_code`,
  `silver.parcel_lineage` 중 하나만 바뀌어도 전국을 다시 만들었다(`WHOLE_TABLE_INPUTS`). 2026-10-17
  운영: 인천 스튜어드 짝 두 행이 쌓이자 2시간 7분짜리 전량 재빌드가 돌았다. 바뀐 것은 동 두 개였다.
- by-PNU 종류(`by_pnu_kind_silver_to_gold`)는 같은 목록을 물려받아 함께 전량으로 갔다.
- 세 입력 모두 Gold 행에 닿는 길이 정해져 있다.
  - 코드 변경은 속성 행을 재번호로 다시 키 매긴다(`read_renumbering`). 커버리지 검사도 이것으로 센다.
  - 용도지역 코드는 앵커(`resolve_zoning_anchors`)로만 읽힌다.
  - 계보 한 줄은 두 필지의 계보 섹션에 들어가고, 이어받기 후보를 바꾼다.

## Decision

1. `WHOLE_TABLE_INPUTS` 는 비운다. 세 입력의 변경은 `affected_pnus` 가 PNU 로 사상한다.
   - **코드 변경**: 다른 행(빌드 열 `kind, old_code, new_code, level` 기준)의 옛 코드·새 코드 아래 필지.
     "아래" 는 코드의 단위가 정하는 앞자리다(시도 2, 시군구 5, 읍면동 8, 리 10 자리,
     `code_go_kr_legal_dong.code_level`). 옛 판과 새 판의 필지 모두에서 찾는다.
   - **용도지역 코드**: 두 코드표로 앵커를 각각 계산해, 앵커가 달라진(생기거나 없어진) 코드로 용도지역이
     잡힌 필지. 앵커에 닿지 않는 열(`uname`)의 변경은 빌드 열이 아니므로 변경도 아니다.
   - **계보**: 다른 행의 선대·후계 PNU 둘 다. 기존 사상(후보가 달라진 후계)은 그대로다.
   - 위 PNU 도 다른 바뀐 PNU 처럼 이어받기 후계에게 퍼진다.
2. 세 표는 좁히지 않고 통째로 빌드에 넘긴다(조회 표). Gold 행은 자기 필지의 행을 통해서만 읽는다.
3. 전량으로 돌아가는 나머지 조건은 그대로다. 바뀐 PNU 가 Gold 의 `max_changed_key_fraction`(0.3)을 넘으면
   전량이다. 시도 통합처럼 넓은 코드 변경은 이 조건으로 전량이 된다. 패리티 표본 불일치도 마찬가지로 전량이다.
4. 시험: `test_gold_incremental_iceberg.py` 가 세 입력을 차례로 바꿔 매번 증분 결과 = 전량 결과(모든 PNU 의 내용
   열·`row_digest`)와 닿은 PNU 수를 확인한다. 사례는 다음과 같다.
   - 용도지역 코드의 생성과 앵커 이동
   - 코드 변경 행의 삭제
   - 계보 줄의 추가

## Consequences

- 스튜어드 짝·계보 몇 줄이 쌓인 날의 패널 Gold 는 그 동의 필지만 다시 만들어 합친다. 측정은 다음 운영 실행의
  `rebuild.counts` 로 한다.
- 시군구 이상 단위의 코드 변경은 그 아래 필지가 많아 대개 30% 를 넘고 전량이다. 이는 의도한 동작이다.
- 사상이 놓친 의존이 있으면 패리티 표본이 잡아 전량으로 간다(ADR-0180 §5). 표본에 걸리는 만큼만이다.
