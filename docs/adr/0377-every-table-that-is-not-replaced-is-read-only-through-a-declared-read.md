# ADR 0377: 덮어쓰지 않는 표는 모두 선언된 읽기로만 읽는다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0286](./0286-every-table-contract-declares-how-its-rows-are-versioned.md) §3(읽는 쪽은 한 곳),
  [ADR-0376](./0376-row-versions-stages-four-to-six-follow-the-decisions-made-since-adr-0286.md) §3(남은 사적 판 고르기),
  ADR-0379(서빙 지적 판, #994 에서 추가)

## Context

#809(ADR-0267)의 독자 가드는 `LatestPerKey` 표, 즉 키마다 버전을 쌓는 표만 본다. 표를 읽는 잡이 계약의
현재 행 선택(`current_rows` 등)을 거치지 않으면 거부한다. 판을 통째로 쌓는 `Editions` 표는 가드 밖에 있었다.
2026-11-04 ADR-0376 이 4·6단계를 옮긴 뒤에도 그런 표의 독자가 저마다 판을 골랐다.

| 표 | 독자 | 사적 규칙 |
|---|---|---|
| `reference.legal_dong_code_snapshot` | 행정경계 served Gold 의 대조 관문(`newest_snapshot_codes`), `address_pnu`, 실거래 적재, 필지 대조 관문, 필지 패널 | `max(snapshot_date)`. 다섯 곳이 따로 적었다 |
| 같은 표 | Rust 토지 레인 `SidoCalendar::from_snapshot_rows` | 같은 규칙을 Rust 로 한 벌 더 |
| `silver.registered_factory` | 행 수 관문 `latest_edition_rows` | 판 id 의 문자열 역순. 계약의 순서는 `ingested_at_utc` 라 서로 달랐다 |
| `silver.parcel_boundaries` | served Gold | `WHERE source_snapshot_id = …` |
| `silver.building_register_floors` | 층 이력 인증 | `WHERE source_snapshot_id = …` |
| 공시가격·지번변경 이력 | 스키마 백필 | 표 전체. 다시 쓰기라 모든 행이 맞다 |

2026-11-04 운영 실측(읽기 전용 Trino):
- 법정동 스냅숏은 날짜 2개이고, 날짜마다 적재가 하나다.
- 공장은 판 1개(`20251231`)다.
- 필지 두 판(202606·202609)에는 닫힌 행이 0개다.

그래서 아래 이전으로 고르는 행은 바뀌지 않는다.

## Decision

1. **독자 가드는 `Replaced`·`Events`·`Unwritten` 이 아닌 모든 표를 본다.** 대상은 `LatestPerKey` 와 `Editions` 다
   (`test_platform_contracts.versioned_tables`). 잡이 그런 표를 이름으로 가리키면서 그 표를 쓰지 않으면, 선언된
   읽기 중 하나에 닿아야 한다. 선언된 읽기는 다음과 같다.
   - 현재 행: `current_rows`, `current_rows_of_versioned`, `rows_as_of`
   - 판 하나: `pinned_edition`
   - 모든 판: `all_editions`
   - 다시 쓰기의 모든 행: `all_rows`(새로 둔다)
   - 서빙 지적 판: `served_rows`(ADR-0379, #994)

   자기 모듈에서 부르지 않아도 그 함수를 부르는 모듈을 가져오면 닿은 것으로 본다.
   - 실패가 막는 사고: 10-16 산단 경계 릴리스가 버전 세 벌을 통째로 읽어 실패했다.
   - 11-04 의 사적 `max(snapshot_date)` 여섯 곳도 같은 모양이다.
   - 자기 시험(`test_the_guard_covers_every_table_that_appends_editions`)은 날 `max(snapshot_date)` 독자와 날
     공장 독자를 심어 둘 다 거부되는지 확인한다.
2. **법정동 코드 목록은 한 곳에서 읽는다.** `legal_dong_code_snapshot_to_reference.newest_snapshot` 은
   `current_rows`(`Editions`, 판 열 = 순서 열 `snapshot_date`)다. Spark 독자 다섯 곳이 이 함수를 거친다. 같은 날
   두 표를 실으면 한 판이 되므로, 옛 `WHERE snapshot_date = max` 와 같은 행을 읽는다. 생산자 자신의 행 수
   관문(`latest_snapshot_row_count` 등)은 적재 단위를 세는 쓰는 쪽 검사라 그대로 둔다.
3. **Rust 도 계약으로 고른다.** `LakehouseTableContract::newest_edition_rows` 는 Python `newest_edition` 의
   짝이다. 판별 `newest_first` 최댓값을 텍스트 순서로 비교하고(ISO 날짜·`YYYYMMDD`·시각), 동률은 거부한다.
   토지 레인의 시도 달력이 이것을 거친다.
4. **공장 판의 순서는 `[source_snapshot_id, ingested_at_utc]` 다.** 판 id 가 제공자의 기준일(`YYYYMMDD`)이다.
   ADR-0328 의 모양을 따라, 같은 기준일을 다시 실으면 나중 적재가 이기고 옛 기준일을 늦게 실어도 이기지
   못한다. 옛 사적 규칙(id 역순)과 같다. 계약의 옛 값(`ingested_at_utc` 만)은 그것과 달랐다.
5. **argparse `help` 문자열은 문서다.** 가드는 docstring 처럼 이것을 건너뛴다. 범용 도구가 예시로 적은 표 이름
   (`lakehouse_maintenance --table`, `lakehouse_registry_backfill --table`)은 읽기가 아니다. `default=` 는 여전히
   표를 가리킨다.
6. **#994(ADR-0379)와 겹치지 않는다.** 서빙 우주의 읽기는 #994 의 `served_rows` 가 정본이다. 이 ADR 은 그것을
   선언된 읽기로 셀 뿐이고, `address_pnu`·실거래의 필지 읽기는 건드리지 않는다. 둘 다 병합되면 `served_rows` 는
   `pinned_edition(frame, contract, served_id)` 와 같은 뜻이다(술어까지 적용). 나중에 병합되는 쪽이 rebase 한다.

## Consequences

| 항목 | 전 | 뒤 | 근거 |
|---|---:|---:|---|
| 가드가 보는 표 | 29(`LatestPerKey`) | 38(+`Editions` 9) | 계약 산출물 집계 |
| 사적 판 고르기 | Spark 8 + Rust 1 | 0 | 코드 읽기, 가드 통과 |
| 고르는 행 | — | 같음 | 실측: 코드 날짜당 1적재, 공장 1판, 필지 닫힌 행 0 |

- 새 원천의 독자가 판을 직접 고르면 CI 가 그 잡과 표를 이름으로 거부한다.
- Rust 쪽에는 같은 가드가 없다. 지금 Rust 의 Editions 독자는 토지 레인 하나이고, 그것은 계약 함수를 거친다.
  Rust 독자가 늘면 같은 검사(계약 표 이름 + `newest_edition_rows` 미사용)를 Rust 시험으로 둔다.
