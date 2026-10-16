# ADR 0267: 버전을 쌓는 표는 현재 행 규칙을 계약에 적고, 모든 독자는 그 규칙으로만 읽는다

- Status: Accepted
- Date: 2026-10-16
- Area: `platforms/foundation-platform`
- Related: [ADR-0250](./0250-an-industrial-complex-has-one-id-derived-from-its-official-code.md)

## Context

`foundation-complex-boundary-release` 가 멈췄다: `silver.industrial_complexes holds 1442 official_complex_code values with more than one row (current_row_predicate=None)`.

2026-10-16 운영 실측(Trino, `r2.silver.industrial_complexes`):

| 항목 | 값 |
|---|---|
| 행 | 4,326 = 1,442 단지 × 3 버전 |
| `source_snapshot_id` | 하나(같은 원천 달) |
| `valid_from_utc` | 모두 같음 |
| `valid_to_utc` | 모두 NULL |
| 버전을 가르는 칸 | `ingested_at_utc` 뿐(적재 3회) |

추가 전용이라 다시 적재할 때마다 버전이 쌓이고, 옛 행은 닫히지 않는다. 옛 행을 닫는 것도 덮어쓰기다. 그런데 "어느 행이 현재인가"는 계약에 없었고 독자마다 따로 정하고 있었다.

| 독자 | 현재 행을 고른 방법 |
|---|---|
| `industrial_complex_silver_to_gold.py` | 자기만의 창: `complex_id` 마다 최신 `valid_from_utc`·`ingested_at_utc` |
| `industrial_complex_boundaries_handoff_to_silver.py` | 규칙 없음 → 중복을 보고 멈춤(이번 장애) |
| `industrial_complex_boundaries_silver_to_postgis_handoff.py`, 그것을 가져다 쓰는 `industrial_complex_boundary_served_gold.py` | `valid_to_utc IS NULL` 만 → 버전 3개가 모두 통과해 경계가 세 배가 됨(시험으로 재현) |

계약의 `current_row_predicate` 는 WHERE 한 줄이라 "키마다 최신"을 말할 수 없다.

## Decision

1. `LakehouseTableContract` 에 `current_row_latest: Option<LakehouseCurrentRowLatest { key, newest_first }>` 를 둔다. 현재 행은 `current_row_predicate` 를 먼저 적용한 뒤, `key` 마다 `newest_first` 내림차순으로 가장 새 행이다.
2. `silver.industrial_complexes` 의 규칙:

   | 항목 | 값 |
   |---|---|
   | `current_row_predicate` | `valid_to_utc IS NULL` |
   | `key` | `official_complex_code` |
   | `newest_first` | `valid_from_utc`, `ingested_at_utc` |

   단지코드가 곧 신원이다(ADR-0250).
3. 독자는 `platform_contracts.current_rows(frame, contract)` 하나로만 현재 행을 고른다. 모든 정렬 칸이 같은 두 행은 고르지 않고 거부한다.
4. 기계적 차단:
   - Rust 시험: 규칙이 있는 표는 이 둘뿐이다. 규칙은 그 표의 칸만 가리킨다. JSON 산출물은 Rust 상수와 같다.
   - Python 시험: 단지 독자 세 파일이 `current_rows` 를 부른다. 그 파일에 `Window.partitionBy` 나 `complexes.where(` 가 없다.
   - Spark 시험: 버전 여럿을 담은 합성 표로 현재 행 선택, 동률 거부, 경계 1:1 조인을 확인한다. 옛 PostGIS 독자로 돌리면 경계 조인 시험이 `3 != 1` 로 실패한다.

## Rejected

- **옛 버전의 `valid_to_utc` 를 닫기**(SCD2 갱신): 쌓인 행을 고쳐 쓰는 것이라 추가 전용 규칙에 어긋난다. 이미 쌓인 4,326행에는 쓸 수도 없다.
- **표 전체 최대 `ingested_at_utc` 를 술어로**: 부분 적재가 생기면 틀린다. 표 이름을 품은 하위 질의가 필요하다.
- **잡마다 필터 두기**: 그것이 바로 이번 결함이다. 세 독자가 세 규칙을 가졌다.

## Consequences

- 경계 발행 잡은 단지마다 행 하나를 받아 다시 돈다. Gold 카탈로그 결과는 같다. 운영 실측에서 규칙의 1순위가 단지코드마다 정확히 하나였고 1,442개 모두 최신 적재였다.
- 다른 표가 버전을 쌓기 시작하면 `current_row_latest` 를 계약에 적는 것으로 끝난다.
