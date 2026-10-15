# ADR 0234: 흐름 지도는 작업이 있어야 할 데이터에만 "자동 작업 없음"을 보인다 — 작업 없는 데이터셋은 그 까닭을 그래프에 적는다

- Status: Accepted
- Date: 2026-10-14
- Amends: [ADR-0188](./0188-dawneer-shows-the-data-flow-as-one-graph-with-live-job-state.md)(칸의 상태),
  [ADR-0086](./0086-the-pipeline-graph-names-every-dataset-once.md)(그래프 노드의 칸)

## Context

사장 지시(2026-10-14): "실제 데이터는 전부 예약 작업이 만든다." 더니어 흐름 지도의 "전체 보기"는 130 칸 중 79 칸에
"자동 작업 없음"을 붙인다. 규칙은 `consoles/dawneer/web/src/lib/dataFlow.ts` 의 `strongest()` → `flowStatus(null)`:
**쓰는 작업이 없으면 무조건** `manual` 이다. 그래서 79 칸이 서로 다른 넷을 한 말로 뭉갠다(origin/main 1f6d885e 실측).

| 칸 종류 | 수 | 실제 |
| --- | --- | --- |
| `serving_group` (Postgres 스키마 묶음, `schema_defined`) | 36 | 작업이 쓰는 데이터 단계가 아니라 저장소다. 표의 존재는 마이그레이션이 정한다 |
| `serving_surface` (계획·대시보드·경보·API 점검) | 24 | 기능이다. 제 `status`(waiting·missing·planned)가 이미 진실을 말한다 |
| `source_group` | 8 | 수집 그룹. 제 `status`(disabled·planned·collection_available·partial)가 말한다 |
| silver·gold 표 | 11 | **진짜 질문이 있는 칸.** 아래 표 |

silver·gold 11 칸을 하나씩 확인했다(카탈로그에 물었다: Trino `$snapshots`, 2026-10-14):

| 칸 | 운영 실물 | 오늘 만드는 법 | 판정 |
| --- | --- | --- | --- |
| `silver-administrative-boundaries` | 스냅숏 2, 마지막 2026-09-29 | `scripts/tiles/admin-boundary/README.md` 손 절차 | 읍면동 경계 **회차가 올 때만**(연 몇 번) — `on_event` |
| `gold-place-id-registry`·`-bridge`·`-changelog` | 각 1, `unit=admin` 5,067 행(2026-09-29) | 같은 회차에 `place_id_release_to_gold.py --unit admin` 손 실행(런북에 없던 단계 — 이 ADR 이 적었다) | `on_event` |
| `silver-industrial-complexes`·`gold-complex-catalog` | 4·3, 마지막 2026-10-01 | `docs/runbooks/lakehouse-compute-engines.md`. 원천(ILIS·VWorld 산단)은 매일 수집 대상이 아니다 | `on_event` |
| `silver-industrial-complex-boundaries` | 3, 마지막 2026-09-01 | 런북 없음 | `needs_job` |
| `gold-parcel-boundary-served` | 1, 2026-10-13 | 열린 PR #483 이 작업 `parcel_tile_bake` 을 등록(꺼짐) | `needs_job` (#483 병합 시 지운다 — 아래 가드가 강제) |
| `silver-parcel-registry` | **표 없음** | 손 스크립트·런북 없음. 첫 전국 bootstrap 은 아직 결정 안 됨 | `not_started` |
| `silver-complex-parcel-memberships`·`gold-complex-spatial-locator` | 표 없음 | 생산자 코드 없음 | 이미 `status: contract_only` |

즉 79 칸 중 "작업이 있어야 하는데 없다"는 2 칸이다. 나머지 77 칸의 "자동 작업 없음"은 소음이고, 소음은 진짜 빈칸을
가린다. 반대로 지금은 새 Silver 표가 작업 없이 들어와도 지도에 79 번째가 80 번째가 될 뿐 아무도 모른다.

### 벤치마크

| 도구 | 본 것 | 가져온 것 |
| --- | --- | --- |
| Dagster | asset 마다 자동화 조건(스케줄·센서·declarative automation)이 있거나 없다. 없는 asset 은 "never materialized"/"manual" 로 따로 보이고, external asset 은 그래프에 있되 실행 대상이 아니다 | 데이터 칸과 실행 대상 아님을 구분. "왜 자동이 아닌가"는 asset 의 속성 |
| dbt | model(만들어지는 데이터)과 exposure(대시보드·앱 같은 소비처), source(바깥에서 오는 것)를 다른 종류로 둔다. exposure 는 실행되지 않는다 | 소비처·저장소는 작업 배지를 받지 않는다 |
| dbt_project_evaluator | "root model"·"unused source" 같은 규칙을 저장소 검사로 돌린다 | 선언을 CI 검사로 강제(ADR-0086 이 이미 이 방식) |

## Decision

1. **작업 배지는 레이크하우스 데이터셋(`silver_table`·`gold_table`·`reference_table`)에만 붙는다.** 다른 칸은 작업의
   유무가 아니라 제 값을 보인다:
   - `serving_group`(Postgres 표 묶음)은 저장소 층이다. 작업 배지가 없다. 표의 존재는 마이그레이션이 정하고,
     `scripts/catalog/pipeline_graph.py` 의 `serving tables` 대조가 그래프의 표 목록을 마이그레이션의 표와 맞춘다.
     그래서 표를 지우는 PR(#465 의 `catalog.parcel`·`building`·`building_unit`)은 같은 PR 에서 그 칸을 지워야 CI 가
     통과하고, 지도는 그래프를 그리므로 함께 사라진다. 새 장치는 필요 없다.
   - `serving_surface` 중 굽는 것(`surface_kind` `tiles`·`r2_baked_documents`)은 지금처럼 작업이 있거나 면제
     사유가 있다(`jobs.v1.json` `serving_surfaces_without_a_job`). 나머지(계획·대시보드·경보·API 점검)는 제
     `status` 를 보인다.
   - `source_group` 은 제 `status` 를 보인다. 매일 수집이 쓰는 그룹은 `source_sweep` 의 edge 가 이미 이어 준다.
2. **작업이 쓰지 않는 레이크하우스 데이터셋은 그래프 노드에 `unscheduled` 로 까닭을 적는다.** 값은 셋이다:
   - `on_event` — 입력 사건(새 회차·새 원천 판)이 올 때 사람이 `runbook_ref` 의 런북으로 만든다. `runbook_ref`
     파일이 있어야 한다.
   - `not_started` — 운영이 한 번도 쓴 적 없다(코드는 있다; `status: implemented` 는 코드 경로만 뜻한다).
   - `needs_job` — 작업이 있어야 하는데 아직 없다. **지도의 "자동 작업 없음"은 이 값에만 붙는다.**
   작업이 쓰는 데이터셋(꺼진 작업 포함)은 `unscheduled` 를 갖지 않는다. `contract_only` 칸도 갖지 않는다.
3. **가드**: `orchestration/dags/job_specs.py` `datasets_without_a_job` 가 그래프와 작업 목록을 함께 읽어 거부한다 —
   까닭 없는 무작업 데이터셋, 작업이 생겼는데 남은 선언, 모르는 값, 파일 없는 런북, 데이터셋 아닌 칸의 선언.
   `orchestration/tests/test_job_specs.py` `EveryDatasetSaysHowItIsMade` 가 실물로 통과하고 심은 위반을 거부한다.
   막는 사고: 새 Silver·Gold 표가 예약 작업 없이 손으로만 채워지는데 지도에서 그 사실이 다른 77 칸에 묻히는 것.
4. **데이터 쪽은 값만 준다.** `unscheduled` 는 열거값이고, 화면 말("일이 생기면 사람이", "아직 안 만듦",
   "자동 작업 없음")은 더니어가 정한다. OpenAPI `PipelineGraphNode.unscheduled` 가 그 열거를 고정한다.
5. 더니어의 표시 규칙(1·2)은 별도 PR 로 바꾼다. 이 ADR 의 PR 은 데이터 쪽(그래프·가드·런북)만 바꾼다.

## Consequences

- 지도의 "자동 작업 없음"은 79 칸 → 2 칸(`silver-industrial-complex-boundaries`, `gold-parcel-boundary-served`)이
  된다. 둘 다 진짜 빈칸이다.
- PR #483(`parcel_tile_bake`)은 병합 전에 `gold-parcel-boundary-served` 의 `unscheduled` 를 지워야 한다. 가드가
  `says unscheduled 'needs_job' but job parcel_tile_bake writes it` 로 알려 준다.
- 남은 일(작업으로 옮길 것):
  - 산단 경계 Silver 의 런북 또는 작업. 산단 원천(ILIS·VWorld `sandan_*`)을 매일 수집에 넣으면 산단 세 칸을
    `started_by: inputs` 작업 하나로 묶을 수 있다.
  - 읍면동 회차: `boundary_emd` 를 매일 수집이 받고 새 회차를 알아보면, README 1–4 단계와 place-id 단계를 작업 하나로
    옮길 수 있다.
  - `silver-parcel-registry` 첫 전국 bootstrap 결정(ADR-0113). 그 뒤 `lineage_stewardship` 의 뒤를 잇는
    `started_by: inputs` 작업(필지 단위 place-id 포함)이 된다. 2026-10-14 기준 spark 풀 굶주림 검사는 30–60 분짜리
    작업 하나를 받는다.
