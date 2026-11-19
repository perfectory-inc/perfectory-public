# ADR 0583: 작업은 묶음 넷(Gold → Silver → 수집 → 운영)으로 Dagster 로 옮긴다 — 묶음마다 스위치 하나, 작업마다의 이중 실행 문턱은 첫 묶음과 함께 지운다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [ADR-0458](./0458-a-job-moves-from-airflow-to-dagster-on-agreeing-decisions-of-every-kind-it-makes-not-on-calendar-days.md)
  전체(작업마다의 문턱), [ADR-0431](./0431-scheduled-jobs-move-from-airflow-to-dagster-one-family-at-a-time-after-a-dual-run-in-which-both-decide.md)
  §2(이중 실행)·§7(가족과 순서). ADR-0431 §1(단계 필드)·§3(Dagster 가 지킬 규칙)·§5(단계는 조각마다 구현)·§6(하류 먼저)은 그대로다.
- Amends: [ADR-0565](./0565-a-job-dagster-decides-is-reached-through-dagster-by-everything-that-reached-it-through-its-airflow-dag.md)
  §1(단계를 여는 PR 이 묶음 규칙을 함께 연다), §10(Airflow 를 지우는 때는 그대로, 이중 실행은 그보다 먼저 지운다);
  ADR-0462 의 대기 노화(`queue_aging`)와 ADR-0435 의 선점(`job_units.py preempted`)은 Dagster 로 옮기지 않는다.
- Builds on: ADR-0417(강제 명령이 실행기), ADR-0574(한도는 측정으로), ADR-0117·ADR-0123(DataHub 의 대안 B 로 가는 길)

## Context

ADR-0431·0458 은 작업을 하나씩, 이중 실행의 일치 결정을 세어 옮기기로 했다. 열흘 뒤 잰 값은 그 방식이 아무것도 옮기지 않았다고 말한다.

| 무엇 | 잰 값 (2026-11-19, `origin/main` 의 정의) |
|---|---|
| 작업 | 99개, 켜진 것 95개: `shadow` 89, `proxied` 6, `dagster` 0 |
| 켜진 작업의 묶음 | Gold·by-PNU·타일 29(입력 시작 23), Silver 41(입력 시작 33), 수집 10 + 그 끝의 문서 2, 운영 9(모두 시계 시작) + 자산 조정자 |
| 비교하는 것 | cron 의 틱 계산과 입력 사건 뒤의 run: Dagster 의 `ScheduleDefinition`·자산 센서가 이미 보장하는 것을 매일 다시 증명한다 |
| 비교가 들인 비용 | `orchestration/dagster/dual_run.py` 617줄과 그 시험, 데몬 안의 검사 run(데몬 310/384MiB, ADR-0574 측정 89%), `dual-run` 호출 경로 |

- **문턱은 결정 규칙을 증명했지만 작업을 옮기지는 못했다.** `proxied` 이후의 경로(ADR-0565 §2–§11)가 서기 전에는 문턱을 넘어도 갈 곳이 없었고,
  그 경로들은 작업마다가 아니라 모든 작업에 한 번 선다. 그러니 작업마다의 증거를 모으는 동안 늘어난 것은 비교 run 의 부하뿐이다.
- **작업마다 옮기면 배포마다 경계가 생긴다.** 같은 풀(`spark`)을 Airflow 와 Dagster 가 나눠 쓰고, 입력 시작 작업은 생산자의 오케스트레이터에 묶인다
  (ADR-0431 §6). 95개를 하나씩 옮기면 경계가 95번 바뀐다. 묶음으로 옮기면 네 번이다.
- **대기 노화와 선점은 Dagster 에 없는 우리 규칙이다.** `job_units.py` 의 `queue_aging`(Slurm 의 나이 인자)과 `preempted` 는 Airflow 풀이 우선순위만
  알던 때의 보충이다. Dagster 1.13 의 run 큐는 두 가지만 한다(공식 문서 `deployment/execution/customizing-run-queue-priority`,
  `guides/operate/managing-concurrency/run-tag-limits`·`concurrency-pools`, 고정판 1.13.27): `dagster/priority` 태그가 높은 run 을 먼저 띄우고,
  태그마다(또는 풀마다, `pools.granularity: run`) 동시 run 수를 막는다. 한도에 막힌 run 은 뒤의 run 을 막지 않는다. 문서가 말하는 규칙은 이
  둘뿐이고, 나이로 순위를 올리거나 도는 run 을 멈추는 규칙은 없다. 그래서 굶주림은 노화가 아니라 풀을 메모리 종류로 나눠 막는다(큰 일이 작은 일의 자리를 쥐지 않는다).

## Decision

1. **묶음은 넷이고, 하류부터 옮긴다.** Gold(Gold 재빌드·by-PNU 굽기·필지/용도지역 타일 굽기·행정/단지 경계 릴리스·지도 편집 접기·`parcel_registry`·
   `parcel_served_gold`) → Silver(`silver_refresh_*`·`*_silver`·`building_register_floor`·`data_go_kr_file_data`·`registered_factory`·
   `lineage_stewardship`·`unit_price_versions_migration`) → 수집(`*_collect`·`juso_electronic_map`·`vworld_parcel_edition`·`legal_dong_code_changes`·
   `parcel_number_change_history`·`real_estate_broker_lookup_load`·`industrial_complex_edge_export`, 그리고 그 끝의 `intelligence_document_*`) →
   운영(`data_quality`·`shape_ledger_check`·`lakehouse_maintenance`·`media_*`·`outbox_publish`·`source_sweep`·`parcel_edition_readiness`, 마지막에
   `asset_reconcile`). 하류 먼저인 이유는 ADR-0431 §6 그대로다: Airflow 의 입력 시작 작업은 Dagster run 의 사건을 보지 못하고, Dagster 는 airlift
   관찰자로 Airflow run 을 본다. `job_specs.dagster_order` 가 계속 이것을 검사한다.
2. **묶음은 작업 목록의 한 필드이고, 묶음의 단계는 하나다.** 정의 파일의 작업마다 `dagster_group`(`gold`·`silver`·`collect`·`operations`)을 적고,
   생성기는 한 묶음의 켜진 작업이 모두 같은 `dagster_stage` 일 때만 작업 목록을 만든다(`job_specs.py`). 그래서 묶음 하나가 스위치 하나다:
   단계를 바꾸는 PR 은 그 묶음의 모든 작업을 함께 바꾸고, 되돌리기는 그 PR 을 되돌리는 것 하나다. 새로 켜는 작업은 자기 묶음의 단계로 태어난다
   (ADR-0458 의 "처음부터 `shadow`" 를 대신한다). 꺼진 작업은 `airflow` 에 머물고 켤 때 묶음의 단계를 받는다.
3. **묶음이 옮기기 전의 증거는 한 번, 묶음째다.** 묶음을 `dagster` 로 바꾸는 PR 은 ready 전에 그 묶음 작업들의 지난 7일 비교를 표 하나로 싣는다
   (이미 있는 원장 `comparisons.jsonl`; 작업마다 일치·차이 수). 차이는 이름으로 적고 원인을 적는다; 면제하지 않는다. 작업마다의 문턱
   (`dual_run.py gate`, `GATE_AGREEING`, 종류마다 한 번)은 쓰지 않는다.
4. **이중 실행은 첫 묶음이 옮긴 뒤 지운다.** Gold 묶음이 `dagster` 로 간 릴리스가 배포되고 확인된 뒤, 다음 PR 하나가 `dual_run.py`·그 검사·
   `FoundationDualRunDisagreed` 와 `dual-run` 경로·`shadow` 의 비교를 지운다. 남은 묶음은 그 뒤 옮길 때까지 Airflow 가 결정하고 실행하며
   비교하지 않는다(단계 값 `shadow` 는 `airflow` 로 합친다). 원장 파일은 추가 전용 기록으로 남는다.
5. **묶음의 스위치 PR 은 엔진을 바꾸지 않는다.** 오케스트레이터와 엔진은 한 PR 에서 바꾸지 않는다(작업 규칙). 실행기는 계속 강제 명령의 systemd
   유닛이다(ADR-0417, `systemd_pipes.py`).
6. **배포 뒤 확인과 멈춤 규칙은 스위치 PR 에 적는다.** 읽기 전용으로: 각 작업의 다음 run 을 Dagster 가 띄웠는지(Airflow 가 아니라), 두 번 뜬 run 이
   없는지, Airflow 에 그 DAG 가 없는지, 데몬의 메모리와 띄우기 실패. **Gold 묶음은 사용자가 보는 서빙을 짓는다:** 배포 뒤 24시간 동안
   서빙 manifest 들(by-PNU 레인·타일)의 나이를 지난주 같은 시각과 견준다. **멈춤 규칙:** 묶음의 어느 작업이든 스위치 뒤 기대한 run 을 두 번
   놓치면(시계 틱 둘, 또는 입력이 바뀐 뒤 그 작업의 fallback 틱 둘), 원인을 찾기 전에 스위치 PR 을 되돌린다. 되돌린 뒤 원인을 찾는다.
7. **슬롯은 Dagster 의 줄이, 바이트는 커널이 센다.** Dagster 가 결정하는 작업의 run 은 `QueuedRunCoordinator` 의 풀 한도(`foundation/pool`)와
   `dagster/priority`(작업의 우선순위 종류 값)로 줄을 선다. 풀은 메모리 종류로 나눈다(지금 `spark` 3, `documents` 1; 기본 풀은 run 한도).
   Airflow 가 결정하는 묶음이 남아 있는 동안 호스트의 `job_units.py pool-full` 이 두 쪽을 함께 세는 유일한 셈으로 남는다. `job_units.py` 에
   새 규칙을 더하지 않는다. `queue_aging` 과 `preempted` 는 Dagster 로 옮기지 않고, 마지막 묶음이 옮길 때 `job_units.py` 와 함께 지운다.
8. **데몬의 한도는 묶음을 옮기기 전에 측정으로 정한다.** 센서가 결정하는 작업이 늘면 데몬이 무거워진다. 데몬의 상한은 ADR-0574 의 규칙(14일 잰 최대
   × 1.5, `scripts/ops/unit_limits.py`)으로 정하고, 그 숫자를 PR 에 적는다. 묶음마다 다시 잰다. 감으로 올리지 않는다.
9. **단계를 여는 순서.** (a) 이 ADR. (b) ADR-0565 의 남은 조각(§7 계보, §8 운영 지도와 읽기 전용 웹서버, 쓰기 관문)이 병합된다. (c) 단계를 여는 PR:
   `IMPLEMENTED_DAGSTER_STAGES` 에 `dagster`, 이 결정 §2 의 묶음 필드와 검사. (d) 풀과 우선순위(§7). (e) 데몬 한도(§8). 그 뒤 묶음마다 스위치 PR
   하나(§2, §3, §6). 마지막 묶음 뒤 ADR-0565 §10 대로 Airflow 를 지운다.

## Consequences

- 옮기는 PR 은 넷이고, 묶음마다 한 번의 배포다. 마지막 묶음까지 같은 풀을 두 오케스트레이터가 나누는 기간은 Gold → 운영의 네 걸음이다.
- 작업마다의 일치 증거는 없어진다. 대신 묶음마다 7일 표 하나(§3)와 배포 뒤 확인·멈춤 규칙(§6)이 지킨다. 결정 규칙 자체(ADR-0431 §3)는 이미
  원장이 증명했고, 시험(`test_dagster_decide.py`)이 지킨다.
- 이중 실행을 지우면 `dual_run.py` 617줄, 그 시험, 데몬의 검사 run, `dual-run` 경로가 없어진다(§4). `job_units.py` 의 노화·선점은 그 뒤 마지막 묶음과
  함께 없어진다(§7).
- DataHub 는 대안 B(`docs/designs/data-catalog-datahub-vs-dagster.md`)로 지운다: 품질 규칙은 ODCS 파일에서(#1474), 운영 화면은 읽기 전용 Dagster 웹서버
  (#1472). #1432(Dagster run 의 OpenLineage)는 첫 묶음이 옮길 때까지 필요한 만큼만 둔다.
- 시험(CI `dagster-observe`): 한 묶음의 단계가 섞이면 작업 목록이 만들어지지 않는다; 새로 켠 작업이 묶음의 단계를 받는다; `dagster_order` 는 그대로.

## 기각한 대안

- **작업마다 문턱(ADR-0458 그대로)**: 위 표 — 열흘 동안 0개를 옮겼고, 경계가 95번 바뀐다.
- **한 번에 전부**: 되돌리기가 전체이고, 사용자 서빙(Gold)과 수집 비용(수집 묶음)이 한 배포에 묶인다.
- **상류(수집)부터**: Airflow 의 입력 시작 작업이 Dagster run 의 사건을 보지 못해 Silver·Gold 가 굶는다(ADR-0431 §6).
- **대기 노화를 Dagster 에 다시 만들기**: Dagster 에 없는 규칙을 센서로 흉내 내는 것은 새 자작 장치다. 메모리 종류로 나눈 풀이 같은 굶주림을 막는다.
