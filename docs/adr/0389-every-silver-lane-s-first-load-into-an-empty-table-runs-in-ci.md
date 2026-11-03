# ADR 0389: 모든 Silver 레인의 빈 표 첫 적재는 CI 에서 돈다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0183](./0183-a-release-loaded-in-batches-is-staged-on-a-branch-and-published-at-once.md)(여러 묶음 판은 load branch 에서 한 커밋으로), [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md)·[ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md)(위생 = 기계 강제 불변식), [ADR-0365](./0365-the-spark-runtime-tests-run-only-what-a-change-reaches-split-across-parallel-runners.md)(런타임 시험 선택·샤드)
- 막는 사고(ADR-0001): 2026-10-30 `silver.land_ownership`·`silver.land_use_zone_area` 첫 실행 실패(#986)

## Context

2026-10-30 22:20 UTC `foundation-silver-refresh@land-ownership` 첫 실행이 CPU 1시간 18분 뒤
`main records 4 of the release's 17 objects; a release is one commit` 로 죽었다. 22:48 UTC
`@land-use-zone-area` 도 같은 이유(15 중 4)였다. 두 표는 **처음 만들어진 표**였고, 적재기는 main 스냅숏이
없으면 branch 를 만들 수 없어 첫 묶음을 main 에 바로 썼다. 형제 레인들은 표를 손으로 만들어 둔 상태라 그 길을
한 번도 타지 않았다. #986 이 적재기를 고쳤고(빈 커밋으로 branch 기반), 시험
`test_silver_release_first_load_iceberg.py` 를 더했다.

그 시험은 일반화되지 않는다. 레인 하나(`silver.land_transfer_history`)를 `append_batch_once` 로 직접 부를 뿐이다.
2026-11-04 main 의 Silver 표는 42개(계약 `industrial_complex_lakehouse_contracts.json` = pipeline graph
`silver_table` 노드)이고, 쓰는 길은 다섯 가지다.

| 쓰는 길 | 표 | 빈 표 첫 적재를 CI 가 돌렸나 (이 ADR 전) |
|---|---:|---|
| silver-refresh `write_silver_iceberg` + load branch | 16 | 1 (#986, 적재기 함수 직접) |
| `append_source_object` (객체마다 커밋: 공매·경매·실거래·지번변경) | 13 | 0 (가짜 Spark 단위 시험뿐) |
| main 의 `append_batch_once`·`append_and_read_back` | 8 | 0 |
| 자기 쓰기(INSERT·`writeTo`) | 4 | 2 (`vintage_versions` 두 표) |
| 아직 쓰는 작업 없음 | 1 | — |

새 레인이 하나 늘 때마다 그 레인의 첫 실행이 곧 그 길의 첫 실험이었다. 운영에서 한 시간을 태우고 나서야 안다.

큰 곳의 방식: 계약에서 시험 행렬을 만든다(Bazel 의 `test_suite` 를 타깃 목록에서 생성, dbt 의 `schema.yml` 마다
돌리는 generic test, Iceberg 자체의 catalog 별 `TestBase` 매개변수화). 목록을 손으로 적지 않으므로 새 항목이
시험 없이 들어올 수 없다.

## Decision

1. **레인은 선언에서 읽는다.** `tests/test_silver_first_load_iceberg.py` 가 Silver 표를 계약과 pipeline graph 두
   곳에서 읽고, 둘이 다르면 실패한다. 표마다 그 표를 쓰는 작업(graph 간선의 `via` 중 `spark/jobs/*.py`)을 찾아 경우를
   정한다: `*-source-objects.json` 의 `silver_refresh` 레인이면 `staged`, 작업이 `append_source_object` 를 부르면
   `object`, `append_batch_once`·`append_and_read_back` 이면 `batch`, `OWN_WRITERS` 에 이름이 있으면 `own`, 쓰는
   작업이 없으면 `none`. 어느 것에도 맞지 않는 표는 **실패**한다 — 새 레인은 자기 첫 적재 시험 없이 들어오지 못한다.
2. **경우마다 실제 Iceberg 에서 빈 표에 첫 적재를 한다**(고정 런타임, 합성 99999 행, 계약 열에서 만든 값).
   - `staged`: 레인 자신의 쓰기(`write_silver_iceberg`, 표도 그것이 만든다)로 두 묶음. 첫 묶음은 레인 계약의
     `iceberg_write_mode`, 나머지는 append(`land.rs batch_write_mode`). 모든 묶음이 load branch 에 오르고,
     main 은 행도 객체 기록도 없으며(빈 기반 커밋 하나까지), fast-forward 한 번 뒤 main 이 판 전체를 갖는다.
   - `object`: 객체 둘(하나는 행 없음)이 각자 한 커밋, 둘 다 기록, 같은 객체 다시는 쓰지 않음.
   - `batch`: 첫 묶음이 main 한 커밋, 같은 묶음 다시는 쓰지 않음.
   - `own`: `OWN_WRITERS` 가 이름 붙인 시험(`module.Class.method`, 존재와 Iceberg 런타임 여부를 검사). 행정경계
     (`main()` 을 그대로), 지도 편집 장부(`append_to_ledger`), 단가 판 두 표(기존 `vintage_versions` 시험).
3. **가드가 잡는지 증명한다.** #986 이전 적재기(첫 묶음은 main, 나머지는 branch)를 `land_ownership` 에 넣으면
   `staged` 검사가 실패해야 통과하는 시험, 그리고 맞는 경우가 없는 합성 레인·한 곳에만 선언된 합성 레인이 거부되는
   시험이 같은 파일에 있다.
4. **CI 에서 돈다.** 파일은 Spark 런타임 시험(skip 사유 "requires the pinned Iceberg runtime")이라 ADR-0365 의
   샤드에서 돈다. 클래스가 모두 `TestCase` 를 직접 상속해 클래스 단위로 나뉜다. 쓰기 모듈·계약·graph 를 import 하거나
   이름으로 읽으므로 그것이 바뀐 PR 이 이 파일을 고른다. 선언 검사(`FirstLoadCaseTableTest`)는 Spark 없이도 돈다.

## Consequences

- 빈 표 첫 적재를 CI 가 도는 Silver 표: 3/42 → 41/42(나머지 1은 쓰는 작업이 없는 `complex_parcel_memberships`;
  작업이 생기면 1항이 경우를 요구한다). 위 숫자는 시험 파일을 세어 얻은 값이다.
- 이 시험은 쓰는 길(함수)을 레인의 계약으로 돈다. 작업 고유의 계획 단계(예: 실거래 `planned_objects` 가 빈 표를
  읽는 일)는 각 작업의 시험이 맡는다.
- 위험: 작업이 쓰는 함수를 graph `via` 에 없는 파일에서 부르면 경우를 잘못 고른다. 그때는 `staged`·`object`·`batch`
  검사가 그 표에 맞지 않아 실패하거나, 맞지 않는 것을 본 리뷰가 `OWN_WRITERS` 에 넣는다.
- 위험(ADR-0365 와 같음): 쓰는 작업 파일만 바뀐 PR 은 이 파일을 고르지 않을 수 있다. 밤 전체 실행이 잡는다.
