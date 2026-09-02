# ADR 0072: 호(戶)는 PNU 로 필지에 붙고, 못 붙는 행은 지어내지 않고 센다

- Status: Accepted
- Date: 2026-09-03

## Context

서빙 카탈로그에 필지는 있고 건물은 없다. `catalog.parcel` 은 39,861,511행이 실려 있고,
`catalog.building` 과 `catalog.building_unit` 은 그릇만 있다 — 생산자가 없다 (G2 진단의
"생산자 없는 canonical 표" 그대로).

레이크하우스 쪽 원천은 실측했다 (2026-09-03, Iceberg REST 카탈로그와 Spark 조인으로):

| | |
|---|---|
| `silver.building_register_units` (전유부) | 19,765,555 행 · 서로 다른 건물 546,917 |
| `silver.building_register_unit_areas` (면적) | 113,813,264 행 |
| 건물 표제부 | **Silver 에 없음** — Bronze 원문만 있고 정제 파이프라인이 없다 |
| 호의 서로 다른 PNU | 353,736 (형식 불량은 단 1개) |
| 그중 `silver.parcel_boundaries` 와 일치 | 352,956 — **99.78%** |
| PNU 가 일치하는 호 행 | 19,536,363 — **98.84%** |
| PNU 가 아예 없는 호 행 | 150,639 — 0.76% |

`catalog.building` 의 필수 칸(용도·구조·연면적·층수·준공년)은 표제부의 것이라, 표제부
Silver 없이는 채울 수 없다. 반면 `catalog.building_unit` 의 칸은 전유부+면적으로 전부
만들어진다.

`catalog.building_unit` 에는 자연키 칸이 없다. 원천 레코드를 가리키는 칸이 없으면 재적재는
매번 전체가 새 행이고, 이는 133,583,046행이 이중 적재될 뻔했던 결함(root ADR-0069)과 같은
모양이다.

## Decision

1. **이번 범위는 호 → 필지다.** `silver.building_register_units` (+`_unit_areas`) 를
   `catalog.building_unit` 으로 투영한다. `catalog.building`(표제부)은 표제부 Bronze→Silver
   정제가 생긴 뒤의 별도 결정으로 남긴다 — 원천이 없는 칸을 지어 채우지 않는다.
2. **연결 열쇠는 PNU 하나다.** `parcel_id` 는 `catalog_domain::parcel_id_for_pnu(pnu)` 로
   유도한다 — `catalog.parcel` 의 id 가 이미 그 유도식으로 발급됐으므로, 조회 없이 계산으로
   붙는다. 도형 포함관계로 판정하지 않는다 (root ADR 원칙: polygons are not evidence).
3. **못 붙는 행은 건너뛰고 센다.** PNU 가 없거나(150,639), 있어도 `catalog.parcel` 에 없는
   행은 적재하지 않고 실행 요약에 `null_pnu_count` / `orphan_pnu_count` /
   `orphan_unit_row_count` 로 기록한다. 0이어도 기록한다 — 수집 안 한 지표와 0인 지표는
   달라야 한다. 경계 잡의 join count 정책과 같다.
4. **자연키는 `mgm_bldrgst_pk`(관리건축물대장 PK)다.** `catalog.building_unit` 에
   `register_pk text NOT NULL UNIQUE` 를 추가하는 마이그레이션을 이 결정과 함께 낸다.
   행의 `id` 는 `perfectory.catalog.building_unit.v1\0` 네임스페이스와 `register_pk` 의
   SHA-256 앞 16바이트로 만든 UUIDv8 — 필지 id 와 같은 방식이라 재적재가 같은 id 를 낸다.
5. **칸 대응** (원천에 없는 값은 NULL/기본값 — 이웃 행에서 베끼지 않는다):
   - `building_name` ← `dong_join_name`, `dong_name` ← `dong_name_raw`,
     `ho_name` ← `unit_label_ko`(없으면 `unit_name_raw`)
   - `floor_label` ← `floor_kind`+`floor_number` 로 조립
   - `exclusive_area_m2` ← 같은 `mgm_bldrgst_pk` 의 `_unit_areas` 중 `area_kind='exclusive'`
     행들의 합. `usage_name`/`structure_name` ← 그중 면적이 가장 큰 행의 raw 값
6. **관은 필지 전례를 그대로 쓴다.** Spark 가 두 표를 조인해 핸드오프 객체(R2, 계약이
   `handoff_prefix` 선언)로 내리고, Rust 명령이 스테이징 COPY → `ON CONFLICT (register_pk)
   DO UPDATE` 로 넣는다. 객체 단위 재시도·행수 검증 포함. 19.7M 행은 이미 39.8M 으로 검증된
   규모다.

## Consequences

- 검색·패널이 "이 필지에 무슨 호가 몇 개, 전유면적 얼마" 를 답할 수 있게 된다 — 상류
  133,578,819행이 처음으로 서빙 경로에 연결된다.
- 1.16%의 호 행(약 229K)은 붙지 않은 채 남고, 그 수가 요약에 남는다. 이 비율이 커지면
  필지 커버리지 문제이지 이 관의 문제가 아니다 — 지표가 그걸 구분해 준다.
- `register_pk` 마이그레이션이 배포되어야 적재기가 돈다. `/readyz` 가 스키마를 비교하므로
  (root ADR-0071) 마이그레이션 없이 이미지가 나가면 배포가 스스로 거부한다.
- 표제부(`catalog.building`)는 여전히 비어 있다. 다음 결정은 표제부 Bronze→Silver 정제다.
