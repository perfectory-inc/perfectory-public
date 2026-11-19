# ADR 0583: 작업은 묶음 넷(Gold → Silver → 수집 → 운영)으로 Dagster 로 옮긴다 — 묶음마다 스위치 하나, 작업별 이중 실행 문턱은 첫 묶음과 함께 지운다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [ADR-0458](./0458-a-job-moves-from-airflow-to-dagster-on-agreeing-decisions-of-every-kind-it-makes-not-on-calendar-days.md)
  전체(작업마다의 문턱), [ADR-0431](./0431-scheduled-jobs-move-from-airflow-to-dagster-one-family-at-a-time-after-a-dual-run-in-which-both-decide.md)
  §2(이중 실행의 비교)·§7(가족과 순서). ADR-0431 §1(단계 필드)·§3(Dagster 가 지킬 규칙)·§5(단계는 조각마다 구현)·§6(하류 먼저)은 그대로다.
- Amends: [ADR-0565](./0565-a-job-dagster-decides-is-reached-through-dagster-by-everything-that-reached-it-through-its-airflow-dag.md)
  §1(단계를 여는 PR 이 묶음 검사를 함께 연다)·§10(이중 실행은 Airflow 보다 먼저 지운다).
- Amends: ADR-0462 의 대기 노화(`queue_aging`)와 ADR-0435 의 선점(`job_units.py preempted`) — Dagster 로 옮기지 않는다(§7).
- Builds on: ADR-0417(강제 명령이 실행기), ADR-0574(한도는 측정으로, #1443), ADR-0117·ADR-0123(DataHub 의 대안 B)

용어: 단계(`dagster_stage`)는 ADR-0431 §1 의 `airflow`·`shadow`·`proxied`·`dagster`. "Airflow 가 결정한다" 는 `dagster` 가 아닌 세 단계 모두다.
입력 시작 작업은 `started_by: inputs`, fallback 틱은 그런 작업이 입력 사건 없이도 도는 자기 시계 틱(ADR-0171)이다.

## Context

ADR-0431·0458 은 작업을 하나씩, 이중 실행의 일치 결정을 세어 옮기기로 했다. 열흘 뒤 잰 값은 그 방식이 아무것도 옮기지 않았다고 말한다.

| 무엇 | 잰 값 (2026-11-19, `origin/main` 의 작업 목록) |
|---|---|
| 작업 | 100개, 켜진 것 96개: `shadow` 90, `proxied` 6, `dagster` 0 |
| 켜진 작업의 묶음(§1) | Gold 37(입력 시작 31), Silver 42(34), 수집 9(0), 운영 8(0, 자산 조정자 포함). 합 96 |
| 비교하는 것 | cron 의 틱 계산과 입력 사건 뒤의 run: Dagster 의 스케줄·센서가 보장하는 것을 매일 다시 증명한다 |
| 비교가 든 비용 | `orchestration/dagster/dual_run.py` 617줄과 그 시험, 코드 서버의 검사 run, `dual-run` 호출 경로 |

- **문턱은 결정 규칙을 증명했지만 작업을 옮기지는 못했다.** `proxied` 이후의 경로(ADR-0565 §2–§9, §11)가 서기 전에는 문턱을 넘어도 갈 곳이
  없었다. 그 경로들은 작업마다가 아니라 모든 작업에 한 번 선다. 작업마다 증거를 모으는 동안 늘어난 것은 비교 run 의 부하뿐이다.
- **작업마다 옮기면 배포마다 경계가 바뀐다.** 같은 풀(`spark`)을 두 오케스트레이터가 나눠 쓰고, 입력 시작 작업은 생산자의 오케스트레이터에
  묶인다(ADR-0431 §6). 96개를 하나씩 옮기면 경계가 96번 바뀌고, 묶음으로 옮기면 네 번 바뀐다.
- **대기 노화와 선점은 Dagster 에 없는 우리 규칙이다.** Dagster 1.13 의 run 큐(공식 문서 `deployment/execution/customizing-run-queue-priority`,
  `guides/operate/managing-concurrency/run-tag-limits`, 고정판 1.13.27)는 두 가지를 한다. `dagster/priority` 가 높은 run 을 먼저 띄우고, 태그마다
  동시 run 수를 막는다. 한도에 막힌 run 은 뒤의 run 을 막지 않는다. 문서가 말하는 규칙은 이 둘뿐이다. 나이로 순위를 올리거나 도는 run 을
  멈추는 규칙은 없다. 둘 다 이미 쓰고 있다: `orchestration/dagster/dagster.yaml` 의 `QueuedRunCoordinator` `tag_concurrency_limits`
  (`foundation/pool` spark 3·documents 1, `foundation/run=job` 5)와 `job_specs.PRIORITY_TAG`.

## Decision

1. **묶음은 넷이고, 하류부터 옮긴다.** 작업의 묶음은 그 작업이 쓰는 것으로 정한다.
   - **Gold**: Gold 재빌드, by-PNU 굽기, 필지·용도지역 타일 굽기, 행정·단지 경계 릴리스, 지도 편집 접기, `parcel_registry`, `parcel_served_gold`.
     Gold·Silver 를 읽어 서빙·문서로 내는 `industrial_complex_edge_export`, `real_estate_broker_lookup_load`, `intelligence_document_*` 도 여기다.
     `intelligence_document_*` 는 `silver.industrial_complex_notice_attachment` 를 읽는다.
   - **Silver**: `silver_refresh_*`, `*_silver`, `building_register_floor`, `data_go_kr_file_data`, `registered_factory`, `lineage_stewardship`,
     `unit_price_versions_migration`.
   - **수집**: `*_collect`, `juso_electronic_map`, `vworld_parcel_edition`, `legal_dong_code_changes`, `parcel_number_change_history`, `source_sweep`.
   - **운영**: `data_quality`, `shape_ledger_check`, `lakehouse_maintenance`, `media_*`, `outbox_publish`, `parcel_edition_readiness`, `asset_reconcile`.

   하류 먼저인 이유는 ADR-0431 §6 그대로다. Airflow 의 입력 시작 작업은 Dagster run 의 사건을 보지 못하지만, Dagster 는 airlift 관찰자로
   Airflow run 을 본다(`definitions.py`). `job_specs.dagster_order` 가 이것을 계속 검사한다.
   CI 시험은 실제 작업 목록에서 네 묶음을 이 순서로 하나씩 `dagster` 로 바꾸고, 걸음마다 `dagster_order` 가 빈 목록인지 본다.
   자산 조정자는 지금 `proxied` 이고, 운영 묶음과 함께 마지막에 `dagster` 로 간다. 그때는 그것이 시작하는 작업이 모두 `dagster` 이므로
   `dagster_order` 의 조정자 규칙(ADR-0565 §4)이 한 PR 안에서 선다.
2. **묶음은 작업 목록의 한 필드이고, 묶음은 함께 넘어간다.** 정의 파일의 작업마다 `dagster_group`(`gold`·`silver`·`collect`·`operations`)을 적는다.
   꺼진 작업도 적는다. 생성기(`job_specs.dagster_groups`)는 한 묶음 안에서 켜진 작업이 모두 `dagster` 이거나 하나도 `dagster` 가 아닐 때만
   작업 목록을 만든다. 그래서 묶음 하나가 스위치 하나다: 묶음을 `dagster` 로 바꾸는 PR 은 그 묶음의 켜진 작업을 모두 함께 바꾼다.
   새로 켜는 작업은 자기 묶음이 있는 쪽에서 태어난다. `dagster` 가 아닌 단계(`airflow`·`shadow`·`proxied`)는 이 검사에서 같은 쪽이다.
3. **되돌리기는 앞으로 가는 PR 하나이고, 가장 나중에 옮긴 묶음부터다.** 되돌리는 PR 은 그 묶음의 켜진 작업을 옮기기 전 단계로 되돌린다.
   §4 전에는 그 단계가 스위치 PR 을 되돌린 것과 같고, §4 뒤에는 `airflow` 다. 하류 먼저이므로 되돌리기는 상류부터다:
   Silver 가 `dagster` 인 동안 Gold 만 되돌리면 `dagster_order` 가 거부한다. 앞의 묶음은 뒤의 묶음을 먼저 되돌린 뒤에 되돌린다.
4. **이중 실행은 Gold 의 멈춤 창이 닫힌 뒤 PR 하나로 지운다.** Gold 스위치를 배포하고 7일이 지나 §6 의 멈춤 규칙이 한 번도 트지 않으면, 다음
   PR 하나가 이 순서로 한다.
   1. `dual_run.py` 에서 다른 곳이 쓰는 것을 옮긴다: `decide.py` 의 `schedule_ticks`·`DAY_SECONDS`, `place_identity_checks.py` 의
      `post_alert`·`PAGE_HOLD_HOURS`, `test_dagster_decide.py` 의 `due_ticks`. 작은 공용 모듈이나 `decide.py` 로 간다.
   2. 비교·`gate`·검사(`definitions.py` 의 `dual_run.checks`)·원장 쓰기·`FoundationDualRunDisagreed` 와 `dual-run` 경로를 지운다.
   3. `job_specs.dagster_stage` 의 "켜진 작업은 처음부터 `shadow`" 규칙(ADR-0458)을 §2 의 묶음 규칙으로 바꾼다.
   4. 모든 `shadow` 를 `airflow` 로 적고, `DAGSTER_STAGES`·`COMPARED` 에서 `shadow` 를 뺀다. 남은 `shadow` 를 거부하는 시험을 둔다.

   `proxied` 는 남는다: Airflow 가 결정하고 Dagster 가 실행하며, 아무것도 비교하지 않는다. 남은 묶음은 옮길 때까지 Airflow 가 결정하고 비교하지
   않는다. 원장 파일은 호스트에 추가 전용 기록으로 남는다.
5. **옮기기 전의 증거는 Gold 에 한 번, 묶음째다.** Gold 스위치 PR 은 ready 전에 Gold 작업들의 지난 7일 비교를 표 하나로 싣는다.
   셀은 `dual_run.py gate --family gold=<작업들>` 이 세는 서로 다른 일치 결정 `(종류, 틱 또는 run 시작)` 과 차이 수다. 원장의 줄 값을 더하지 않는다:
   하루 창이 겹친다. 차이는 이름과 원인을 적고 면제하지 않는다. 줄이 하나도 없는 작업이 있으면 ready 하지 않는다.
   §4 뒤의 묶음은 비교가 없다. 그 묶음들의 증거는 Gold 의 7일 운영 기록(§6)과 결정 규칙의 시험(`test_dagster_decide.py`)이다.
   작업마다의 문턱(`GATE_AGREEING`, 종류마다 한 번)은 어느 묶음에도 쓰지 않는다.
6. **스위치 PR 은 엔진을 바꾸지 않고, 배포 순서와 배포 뒤 확인을 적는다.**
   - 오케스트레이터와 엔진은 한 PR 에서 바꾸지 않는다. 실행기는 계속 강제 명령의 systemd 유닛이다(ADR-0417, `systemd_pipes.py`).
   - **배포 순서**: 묶음의 Airflow DAG 를 멈추고(ADR-0565 §5 의 배포 정지), Dagster 위치를 다시 읽고, 센서를 켜고, Airflow 가 그 DAG 를 더 만들지
     않는 릴리스로 간다.
   - **놓친 입력 없음**: 센서의 첫 평가는 그 작업의 마지막 Airflow run 을 airlift 가 관찰한 시각부터 빚을 센다. 지금 `decide.py` 는 Dagster run 이
     있어야만 빚을 세므로, `shadow` 에서 바로 온 작업은 배포 사이의 입력 변경을 fallback 틱까지 놓친다. 이것은 단계를 여는 PR(§9 c)이 고친다.
   - **배포 뒤 확인(읽기 전용)**, 스크립트 하나(`scripts/ops/verify-dagster-group.sh <묶음>`, 스위치 PR 이 더한다)가 다음을 말한다.
     - 스위치 뒤 작업마다 Dagster run 수(`foundation/run=job` 태그, 작업 이름).
     - 같은 결정에서 두 run 이 뜨지 않았는지.
     - Airflow REST 에 그 DAG 가 없는지.
     - 데몬 메모리와 run 띄우기 실패.
   - **Gold 는 사용자가 보는 서빙을 짓는다.** 배포 뒤 24시간 동안 한 시간마다 서빙 manifest 의 나이를 지난주 같은 시각과 견준다.
     - by-PNU 레인: `foundation_by_pnu_served_manifest_age_seconds <= (foundation_by_pnu_served_manifest_age_seconds offset 7d) + 그 레인의 일정 간격`.
     - 타일: 나이 지표가 아직 없다. Gold 스위치 PR 이 by-PNU 지표를 내는 곳에 `foundation_tile_served_manifest_age_seconds` 를 더하고 같은 식으로 견준다.
   - **멈춤 규칙**: 묶음의 어느 작업이든 스위치 뒤 기대한 run 을 두 번 놓치면, 원인을 찾기 전에 §3 의 되돌리기 PR 을 연다. 되돌린 뒤 원인을 찾는다.
     기대한 run 은 시계 틱, 그리고 입력 사건 뒤의 run 이다. 기한은 `min(틱 둘, 48시간)` 이다(매주·매달 fallback 이 몇 주를 기다리지 않도록).
     위 스크립트가 셈을 말한다.
7. **슬롯은 Dagster 의 줄이, 호스트 허가는 `job_units.py` 가 세고, 노화·선점은 옮기지 않는다.**
   - Dagster 가 결정하는 run 은 이미 있는 태그 한도(`foundation/pool`, `foundation/run=job`)와 `dagster/priority`(작업 목록 `priority_classes` 의 값)로
     줄을 선다. 묶음마다 스위치 PR 이 `max_concurrent_runs` 와 job run 한도를 다시 셈한다. 이 셈은 두 쪽에 다 열리는 `default_pool` run 도 포함한다.
   - 호스트의 `job_units.py` 는 두 오케스트레이터와 운영자의 시작을 함께 세는 유일한 곳으로 남는다: `pool-full`, 메모리 허가(ADR-0563),
     용량 claim(ADR-0435), 결과 원장, 배포 정지. 여기에 새 규칙을 더하지 않는다.
   - `queue_aging`·`aged` 와 `preempted` 는 Dagster 로 옮기지 않는다. Dagster 가 결정하는 작업에는 쓰이지 않고, 마지막 묶음이 옮길 때 지운다.
     굶주림은 풀을 메모리 종류로 나눠 막는다(큰 일이 작은 일의 자리를 쥐지 않는다).
8. **데몬의 한도는 묶음을 옮기기 전에 측정으로 정한다.** 센서가 결정하는 작업이 늘면 데몬이 무거워진다.
   - 규칙은 ADR-0574(#1443)의 것이다: 상한 = 14일 잰 최대 × 1.5. 최대는 cAdvisor `max_over_time(container_memory_working_set_bytes{id=~".*<컨테이너>.*"}[14d])`
     다. 측정 창이 그 모양의 컨테이너가 산 시간보다 길면, 산 시간만큼만 잰다.
   - 상한은 `compose.orchestration.yml` 의 `dagster-daemon` `mem_limit` 에 잰 값과 함께 적는다.
   - 첫 값은 #1477 이다: 379 MiB, 상한 384m 에 눌린 값이라 576m. 묶음마다 스위치 PR 이 다시 잰다.
9. **단계를 여는 순서.**
   - (a) 이 ADR.
   - (b) ADR-0565 의 남은 조각이 병합된다: §7 계보(#1432), §8 운영 지도(#1440), §8 의 읽기 전용 웹서버와 쓰기 Dagster 웹서버의 호출 제한(#1472).
     §2–§6, §9, §11 은 병합됐다.
   - (c) 단계를 여는 PR: `IMPLEMENTED_DAGSTER_STAGES` 에 `dagster` 를 더하고, §6 의 첫 평가를 airlift 관찰에서 세게 한다.
     묶음 필드와 검사(§2)는 그보다 먼저 들어간다(운영 영향 0).
   - (d) 데몬 한도(§8, #1477).
   - 그 뒤 묶음마다 스위치 PR 하나(§5, §6, §7). 마지막 묶음 뒤 ADR-0565 §10 대로 Airflow 를 지운다.

## Consequences

- PR 은 묶음 필드, 단계 열기, 데몬 한도, 스위치 넷, 이중 실행 삭제 하나, Airflow 삭제 하나다. 같은 풀을 두 오케스트레이터가 나누는 기간은
  네 걸음이다.
- 작업마다의 일치 증거는 없어진다. 대신 Gold 의 7일 표 하나(§5), 배포 뒤 확인, 멈춤 규칙(§6)이 지킨다. 결정 규칙 자체(ADR-0431 §3)는 시험이 지킨다.
- §4 가 지우는 것: 비교와 문턱, 그 시험, 코드 서버의 검사 run, `dual-run` 경로, `shadow` 단계. 다른 곳이 쓰는 도움 함수는 옮겨 남는다.
  `job_units.py` 의 노화·선점은 마지막 묶음과 함께 없어진다(§7). 나머지 허가는 남는다.
- DataHub 는 대안 B(`docs/designs/data-catalog-datahub-vs-dagster.md`)로 지운다: 품질 규칙은 ODCS 파일에서 온다(#1474). 운영 화면은 읽기 전용
  Dagster 웹서버다(#1472). #1432(Dagster run 의 OpenLineage)는 첫 묶음이 옮길 때까지 필요한 만큼만 둔다.
- 시험(CI `dagster-observe`, `test_job_specs.py`):
  - 한 묶음에 `dagster` 와 다른 단계가 섞이면 작업 목록이 만들어지지 않는다.
  - 묶음이 없거나 모르는 묶음이면 거부한다.
  - 네 묶음을 순서대로 옮기는 걸음마다 `dagster_order` 가 비어 있다.

## 기각한 대안

- **작업마다 문턱(ADR-0458 그대로)**: 위 표 — 열흘 동안 0개를 옮겼고, 경계가 96번 바뀐다.
- **한 번에 전부**: 되돌리기가 전체이고, 사용자 서빙(Gold)과 수집 비용(수집 묶음)이 한 배포에 묶인다.
- **상류(수집)부터**: Airflow 의 입력 시작 작업이 Dagster run 의 사건을 보지 못해 Silver·Gold 가 굶는다(ADR-0431 §6).
- **스위치 PR 을 되돌리는 것을 되돌리기로**: 뒤의 묶음이 `dagster` 이면 `dagster_order` 가 거부하고, §4 뒤에는 `shadow` 가 없다(§3).
- **대기 노화를 Dagster 에 다시 만들기**: Dagster 에 없는 규칙을 센서로 흉내 내는 것은 새 자작 장치다. 메모리 종류로 나눈 풀이 같은 굶주림을 막는다.
