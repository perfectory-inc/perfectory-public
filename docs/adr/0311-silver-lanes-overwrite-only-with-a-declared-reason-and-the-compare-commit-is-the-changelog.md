# ADR 0311: Silver 레인은 이유를 적어야만 덮어쓰고, 비교 커밋이 곧 변경 기록이다

- Status: Accepted
- Date: 2026-10-28
- Amends: [ADR-0191](./0191-a-release-becomes-a-changelog-once-and-every-layer-reads-only-the-changes.md)
  (Proposed → 이 ADR 의 형태로 받아들인다: 변경 기록의 위치와 소비 경로)
- Extends: [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md)
  §4.3 (나머지 레인을 하나씩 옮긴다)
- Builds on: [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md),
  [ADR-0189](./0189-an-appended-silver-input-is-its-own-change-set.md),
  [ADR-0259](./0259-ci-migrates-the-production-table-shape.md), [ADR-0267](./0267-a-versioned-table-declares-its-current-row-and-every-reader-takes-it-from-the-contract.md)

## Context

ADR-0275 는 Accepted 이지만 2026-10-28 까지 비교 후 추가로 옮긴 레인은 토지 이동 이력 하나다. Silver 레인 12개 중
11개가 판마다 표 전체를 다시 쓴다(9개는 `overwrite`, 가격·전유부 다리 2개는 판 전체를 `append`). 운영 Trino
`$snapshots` 요약(`added-records`·`added-files-size`, 읽기 전용, 2026-10-28)으로 잰 값이다.

| 표 | 한 번 적재가 쓴 행 | 실제 변경 |
|---|---:|---|
| `land_forest_ledger` | 39,917,568 (매주: 10-15·10-19·10-26) | 앞 판과 행 수 차이 +5,112 |
| `building_register_units` | 19,860,453 (10-20) | +12,849 |
| `building_register_unit_areas` | 114,461,830 · 8.38 GB (10-20) | +93,447 |
| `land_transfer_history` (이미 비교) | 브랜치 131,664,668 → **main 203,061** (10-21) | 201,476 행 + 삭제 표시 1,585 |

`land_forest_ledger` 는 약 5천 행이 바뀌는 판을 위해 매주 3,990만 행을 다시 썼고, 레인 계약 어디에도 "이 레인은
통째로 쓴다, 그 값은 이만큼이다" 라는 말이 없었다. 덮어쓰기는 계약의 `iceberg_write_mode: overwrite` 한 칸으로
조용히 허용됐다.

ADR-0191 은 "적재 때 한 번 변경 기록을 만든다" 를 정했지만 `changelog.py` 는 시험만 부르고, 변경 기록 표
`silver.<t>__changes` 는 만들어진 적이 없다. 그사이 ADR-0275 의 비교 작업(`silver_release_compare.py`)이 판마다
바뀐 키의 행과 삭제 표시만 main 에 한 커밋으로 append 하게 됐다. 그 커밋이 이미 ADR-0191 이 말한 변경 기록이다.
같은 행을 `__changes` 에 한 번 더 쓰면 같은 사실이 두 곳에 산다.

## Decision

1. **덮어쓰는 레인은 이유를 적는다.** `silver_refresh.spark.iceberg_write_mode: overwrite` 인 레인 계약은 다음 둘 중
   **정확히 하나**를 적는다. append 레인은 둘 다 적지 않는다.
   - `compare.content_columns`: 판을 load branch 에만 통째로 올리고, main 에는 바뀐 키만 쌓는다(ADR-0275).
   - `full_reason { reason, measured_cost }`: 아직 통째로 써야 하는 이유와, 잰 비용(행·바이트, 출처)이다.
   - 관문은 두 곳이다. 레인 계약 파서(`silver_refresh/lane.rs` `checked_full_reason`)가 계약을 거부하고, 쓰기 자리
     (`lakehouse_ingest.append_batch_once`)가 같은 규칙으로 쓰기 전에 거부한다(`overwrite_declaration`). 비교하는
     레인은 main 을 덮어쓰지 못한다. 행이 있는 main 에 대한 overwrite 는 거부되고, load branch 만 덮어쓴다.
     `_smoke` 사본은 버리는 증명 표라 제외한다(작업들의 `--allow-non-smoke-overwrite` 관례와 같다).
   - 막는 사고: `silver.land_forest_ledger` 가 약 5천 행 변경에 매주 39.9M 행을 다시 쓴 일(ADR-0001).
2. **비교로 옮기는 레인(이 ADR).** 표가 삭제 표시를 할 수 있어야 비교할 수 있다. 다음 넷이 `row_state` 열(null 허용)과
   `current_row_latest { key, newest_first: [source_snapshot_id], removal_column: row_state, keep_ties: true }` 를 얻고,
   레인 계약에 `compare.content_columns`(키·적재 열·`data_reference_date` 제외 전부)를 적는다.

   | 표 | 키 | 근거 |
   |---|---|---|
   | `land_characteristic` | `pnu` | 필지마다 한 행. 기준연도 열은 내용이다 |
   | `land_forest_ledger` | `pnu` | 필지마다 한 행 |
   | `land_individual_price` | `pnu` | 판마다 한 기준연도. 키에 연도를 넣으면 해가 바뀔 때 키 전부가 사라짐 + 새로 생김(두 배)이 된다 |
   | `land_right_registration` | `pnu, right_serial_no` | 계약의 정렬 키 |

   `keep_ties: true` 라 키가 판 안에서 유일하지 않아도 맞다. 한 키의 버전은 그 키의 가장 새 판의 행 전부다
   (ADR-0275 의 토지 이동 이력과 같은 규칙). 키를 실측해 유일성을 확정하는 일은 필요 없다. 키가 거칠면 바뀐 키 하나에
   쓰는 행이 늘 뿐이다. `source_snapshot_id` 는 `vworldkr-<slug>:<YYYYMMDD>` 라 문자열 순서가 판 순서다(운영 값 확인).
   독자는 이미 전부 `current_rows` 를 거친다(필지 패널 `read_source`, 매칭 관문 `current_rows_of_versioned`, dbt
   `foundation_current_rows`). 운영 표 모양 원장(ADR-0259)에서 배포의 `plan_table` 은 넷 모두 `row_state` 추가만,
   백필·재정렬 없이 계획한다(`test_lakehouse_production_shape`).
3. **`full_reason` 으로 남는 레인(잰 비용을 계약에 적는다).**

   | 레인 | 이유 | 잰 비용 (Trino `$snapshots`) |
   |---|---|---|
   | 표제부·호·호 면적 | 허브 레인은 브랜치 없이 main 에 한 번에 쓰고 비교 블록을 받지 않으며, `gold.building_panel` 이 입력마다 판 하나만 받는다(`assert_single_snapshot`) | 8,064,354행·668 MB / 19,860,453행·1.39 GB / 114,461,830행·8.38 GB (10-20) |
   | 용도지역(`land_use_plan`) | 비교가 1억 3,166만 행에서 87분(10-21, 첫 시도는 150분 단위 상한에 걸려 실패). 2억 4,758만 행이면 선형 추정 약 165분으로 상한을 넘는다 | 247,584,668행·2.68 GB (09-05) |
   | 용도지역 코드표 | 1,270행·37 KB: 비교 작업 한 번보다 통째 쓰기가 싸고, `source_snapshot_id` 가 날짜가 아니라 판 순서가 될 수 없다 | 1,270행·37,025 B (10-14) |
4. **변경 기록은 비교 커밋이다(ADR-0191 을 이렇게 받아들인다).**
   - 생산자: `silver_release_compare` 의 main append 커밋이 스냅숏 요약에 `foundation.change-record =
     silver-release-compare` 와 바뀐 키 행 수·삭제 키 수를 남긴다(`changelog.change_record_options`). `from` 은 그
     커밋의 Iceberg 부모이므로 따로 적지 않는다. 변경이 없는 판도 빈 커밋으로 남아 빈 걸음이 된다.
   - `silver.<t>__changes` 표는 만들지 않는다. 같은 행을 두 번 쓰고, 새 표마다 계약·원장·카탈로그가 늘어난다.
   - 소비자 경로 하나: `changelog.changes_between(spark, table, old, new)`. `old`(소비자가 고정한 판)에서 `new` 까지
     main 의 모든 걸음이 기록된 변경이거나 압축(`replace`)이면, 걸음마다 그 커밋이 더한 행(바뀐 키의 새 행 + 삭제
     표시)을 돌려준다. 하나라도 아니면(첫 적재, 아직 덮어쓰는 레인, 만료된 계보) None 이고, 소비자는 ADR-0189 →
     ADR-0180 으로 내려간다. 돌려준 행의 키가 곧 현재 행이 바뀐 키다. 옛 모습이 필요한 소비자는 그 키로 고정 판의
     `current_rows` 를 semi-join 한다.
   - Gold 증분(`gold_incremental.py`, `by_pnu_panel_delta.py`)이 이 경로를 읽게 하는 일은 그 파일의 소유자가 한다.
     이 ADR 은 인터페이스(`changes_between`, 요약 키 `foundation.change-record`)만 정한다.
   - ADR-0191 의 Z-set(`change_set`·`net`)·지문·닫힘 검사·`chain` 은 소비자 쪽 부품으로 남는다. `chain` 은 위 경로가
     쓴다. ADR-0191 §2 의 `row_fingerprint` 열 추가와 §3 의 "브랜치를 빨리감기로 공개" 는 하지 않는다(비교 레인은
     공개하지 않고 바뀐 키만 append 한다).

## Consequences

판마다 main 에 쓰는 행 (전 = 잰 값, 후 = 토지 이동 이력은 잰 값, 나머지는 계산·추정):

| 레인 | 전 | 후 | 후의 근거 |
|---|---:|---:|---|
| `land_transfer_history` | 131,664,668 | 203,061 | 잰 값(10-21 비교) |
| `land_forest_ledger` | 39,917,568 (매주) | 약 5천–2만 | 추정: 행 수 차이 +5,112 가 하한, 내용만 바뀐 키는 더해진다 |
| `land_right_registration` | 27,967,962 | 약 14만 | 추정: ADR-0275 평상시 변경 0.5% |
| `land_characteristic` | 35,888,121 | 평상시 약 18만, 기준연도가 바뀌는 판은 거의 전부 | 추정 |
| `land_individual_price` | 35,887,377 | 평상시 약 18만, 새 공시연도 판은 거의 전부 | 추정 |
| 표제부·호·호 면적·용도지역·코드표 | 그대로 | 그대로 | `full_reason` |
| 가격·전유부 다리(append) | 판마다 202,058,239 / 19,860,453 | 그대로 | 이 관문 밖(아래 남은 일) |

- 비교 레인도 판 전체를 load branch 에 한 번 쓴다(ADR-0275 가 받아들인 값). 그 파일은 브랜치를 지우고 스냅숏이
  만료되면 사라진다. main 의 보존 사본은 판마다 변경분만 는다.
- 비교 시간은 표 크기에 비례한다: 4천만 행 안팎인 넷은 약 30분(87분 × 0.3, 추정)으로 150분 상한 안이다.
- 남은 일(사장 판단 또는 후속 PR):
  - 허브 세 레인: 브랜치 적재 + 건물 패널 독자 이전 뒤 비교로.
  - `land_use_plan`: 비교를 시도 단위로 나누거나 단위 상한을 정한다(배포 설정이라 이 ADR 밖).
  - 가격·전유부 다리(append 레인)와 `silver.parcel_number_change_history`(제공자가 파일 날짜만 바꿔도 같은 1,370,351 행을
    다시 append)도 판 전체를 쓴다. 덮어쓰기가 아니라 이 관문에 걸리지 않는다. 같은 비교 규칙을 넓히는 일은 따로 정한다.
