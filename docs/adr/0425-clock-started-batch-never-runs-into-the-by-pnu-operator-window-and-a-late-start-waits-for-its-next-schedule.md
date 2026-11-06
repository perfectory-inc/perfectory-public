# ADR 0425: 시계로만 시작하는 배치는 by-PNU 운영자 창으로 넘어가지 않는다 — 늦은 시작은 아무것도 돌리지 않고 다음 스케줄을 기다린다

- Status: Accepted
- Date: 2026-11-06
- Builds on: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(Airflow 는 작업의 systemd 유닛을 시작),
  [ADR-0184](./0184-one-answer-for-running-jobs-and-a-release-the-kernel-keeps-immutable.md)(어떤 작업이 도는가의 답은 `job_units.py` 하나),
  [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(run 은 결과를 말하고, 기다림은 실패가 아니다),
  [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)(작업은 한 명세·한 실행기, 시간 제한은 한 숫자),
  [ADR-0282](./0282-operator-switches-are-issued-by-foundation-api-and-land-as-files-the-runner-reads.md)(운영자 run-now),
  [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md)(공시가격 버전 이행)

## Context

2026-11-05 ~ 06 사고(잰 값, 호스트 journal·Airflow 메타DB·cut-over 증거 파일).

| 시각(UTC) | 무슨 일 |
|---|---|
| 11-05 22:16 | #1099 의 DAG `foundation_unit_price_versions_migration` 이 Airflow 에 처음 올라옴(`dag_version` 1) |
| 22:22 · 22:51 · 23:18 | 배포 세 번이 DAG 를 멈춤(`cli_dag_pause`) |
| 23:39:50 | 릴리스 `082a5596`(#1099) 가 `current` 로 바뀜 |
| **23:46:32** | 배포 7단계가 DAG 를 다시 켬(`cli_dag_unpause`) |
| **23:46:33** | Airflow 가 `scheduled__2026-11-05T18:45:00+00:00` run 을 **바로** 만듦 — 이미 지난 18:45 틱 |
| 23:46:36 | `foundation-unit-price-versions-migrate.service` 시작, 한도 160분 + 정지 여유 5분 → 02:31 까지 가능 |
| 11-06 00:37 | 사장이 parcel-ownership cut-over 시작 |
| 01:22 | gold 단계가 Spark 를 45분 기다리다 실패: "Spark still runs after 45 min: foundation-unit-price-versions-migrate.service" (`/data/foundation-platform/by-pnu-bake/cutover/20261106T003736Z.jsonl`) |

**왜 23:46 에 돌았나 — 첫 배포의 따라잡기.** 손으로 시작한 것도, 스케줄러 오작동도 아니다. Airflow 3.3 의
`CronTriggerTimetable.next_dagrun_info` 는 `catchup=False` 에서도 후보에 `_align_to_prev(now)`(가장 최근 지난 틱)를 넣는다.
그래서 DAG 가 켜지는 순간 그날 18:45 틱이 아직 run 이 없으면 즉시 돈다. 18:45 이후 켜진 DAG, 그리고 18:45 즈음
배포가 DAG 를 멈춰 둔 날의 DAG 모두 같다. 배포는 매번 DAG 를 멈췄다 켜고 자동배포는 시간을 가리지 않으므로, 이것은 한 번의 사고가
아니라 **반복되는 경로**다. 배포가 진 빚(`deferred`)을 `dags trigger` 로 갚는 run 도 같은 늦은 시작이다.

#1099 는 "18:45 에 시작하면 21:25 에 끝나 cut-over 의 밤을 비켜 간다" 를 설명문으로만 적었다. 그 약속은 **시작 시각이 스케줄과 같을
때만** 참이고, 아무도 그것을 시작 시점에 확인하지 않았다.

운영자 창은 이미 한 곳에서 계산된다. `by-pnu-lane-cutover.sh` 가 jobs 목록에서 spark 풀 작업의 첫 일일 시작(04:30)부터 마지막
시작 + 15분(21:50)까지를 "스케줄 창" 으로 보고, 그 안에서는 cut-over 단계를 시작하지 않는다. 나머지 21:50–04:30 이 cut-over 의
몫(운영자 창)이다. 그런데 반대 방향 — 배치가 운영자 창에 들어오지 않는다 — 은 아무도 지키지 않았다.

## 업계 비교

| 방식 | 하는 일 | 이 사고에 |
|---|---|---|
| Airflow pools · `priority_weight` | 슬롯이 빌 때 **기다리는 것** 중 무엇을 먼저 시작할지 | 이미 도는 run 은 그대로. cut-over 는 Airflow 작업도 아니다. 못 막음 |
| Dagster run queue `tag_concurrency_limits` | 같은 태그의 run 수를 시작 시점에 제한 | 시작 순서·동시 수만. 시간대 개념이 없다. 못 막음 |
| Kubernetes PriorityClass 선점 · YARN 선점 | 높은 우선순위가 오면 낮은 것을 죽인다 | 막지만, 선점당한 쪽 단계 하나를 버린다. cut-over 가 모든 배치의 정지 순서를 알아야 한다 |
| **Slurm 예약(reservation) + 백필** | 예약 구간이 있으면 **시간 제한이 예약에 닿는 작업은 시작하지 않는다** | 정확히 이 사고. 시작 시점 하나에서 판정, 작업 코드 수정 없음 |
| Borg/Omega 의 prod·batch 구분 | prod 가 batch 를 선점, batch 는 남는 용량에서만 | 원리는 같고, 우리 규모에선 Slurm 형이 더 단순 |

## Decision

1. **운영자 창은 jobs 목록에서 한 번 계산한다.** `scripts/deploy/job_units.py` 의 `scheduled_window(jobs)` 가 spark 풀 작업의
   일일 시작(`M H * * *`) 중 가장 이른 것부터 가장 늦은 것 + 15분까지를 스케줄 창으로 돌려준다(2026-11-06: 04:30–21:50).
   나머지가 by-PNU 운영자 창(21:50–04:30)이다. `by-pnu-lane-cutover.sh` 는 자기 안의 계산을 지우고 이 함수를 쓴다. 정본 설명은
   `definitions/platform.definition.json` → `orchestration/jobs.v1.json` 의 `operator_window_means` 다. 새 플래그나 손으로 적은
   시각은 없다.
2. **창에서 제외할 배치는 선언에서 유도한다.** spark 풀이고 `started_by: schedule`(시계만 시작)이며, 스케줄 시작 +
   `time_limit_minutes` + `stop_grace_minutes` 가 스케줄 창 안에서 끝나는 작업이다(`kept_out_of_operator_window`). 스케줄이
   이미 그렇게 놓았다는 것이 곧 선언이라 두 번 적지 않는다. 2026-11-06 기준 대상: `unit_price_versions_migration`,
   `building_register_floor`, `lineage_stewardship`, `registered_factory`, `lakehouse_maintenance`(꺼진 `parcel_registry`,
   `parcel_served_gold` 도 해당).
3. **판정은 호스트의 단 하나의 시작 관문에서 한다.** `scripts/ops/start-scheduled-job.sh` 는 위 작업의 시작(시계든 배포가 진 빚의
   `triggered` 든)이 지금 시각 + 한도 + 정지 여유로 운영자 창에 닿으면 아무것도 시작하지 않고
   `foundation-job-outcome waiting <이유>` 를 말하고 0 으로 끝난다(ADR-0260 의 기다림). 다음 스케줄 시작이 그 run 을 돈다.
   Airflow 가 왜 늦게 시작했는지(따라잡기, 스케줄러 중단, 배포 빚)는 묻지 않는다 — 시작 시점 하나가 모든 경로를 덮는다.
4. **운영자가 돌리는 것은 막지 않는다.** run-now(ADR-0282, `<job> requested <id>`)는 운영자 자신의 행위라 통과한다. check
   서비스는 Spark 를 쓰지 않아 묻지 않는다.
5. **막지 않는 것.** `inputs` 작업(입력 이벤트가 밤에 시작한다: 21:35 공매 Gold 뒤의 공매 bake), 자기 스케줄이 이미 21:50 을
   넘는 작업(21:35 공매 Gold — 창의 끝을 만드는 작업), 매시 작업(폴드·공매·실거래 Silver). 이것들은 cut-over 의 기존 Spark
   대기(45분)가 기다린다.

## 버린 대안

- **이행의 예산을 창 시작까지 자르기**(예산 클리핑). 이행은 단계마다 이어 가므로 가능하지만, 단계 예산을 아는 작업 하나에만 맞는
  코드다. 같은 늦은 시작을 겪는 FLOOR·lineage·maintenance 는 못 덮는다.
- **cut-over 가 단계 경계에서 선점.** ADR-0308 의 배포 정지(step 0)를 쓰면 되지만, 사장이 직접 시작한 단계가 배치를 멈추고 그
  배치의 진행 중 단계(이행의 경우 최대 53분, 11-06 00:45 의 조각 1/4 = 3,175.6 s)를 버린다. 시작을 막으면 버리는 것이 없다.
- **DAG 를 `CronTriggerTimetable(run_immediately=False)` 로.** 첫 켬에만 듣고(`restriction.earliest` 가 있으면 무시), 배포가
  멈춘 사이 놓친 틱의 따라잡기와 배포 빚 run 은 못 막는다. 근본은 "늦게 시작한 run 이 창으로 넘어간다" 이고 원인은 여럿이다.

## Consequences

- 사고 재현 시험: `orchestration/tests/test_start_scheduled_job.py::BatchStaysOutOfTheOperatorWindow`(23:46 따라잡기와 배포
  빚 run 은 아무것도 시작 안 함, 18:45 와 run-now 는 시작, `inputs`·창 끝·매시 작업은 밤에도 시작),
  `orchestration/tests/test_job_units.py::TheOperatorWindowIsTheCutOvers`(실제 jobs 목록에서 이행이 제외 대상이고 18:45 계획이
  창 안에 듦).
- 계산된 효과(사고 날 기준): 23:46 시작 → 시작 안 함. cut-over 의 gold 대기 45분 → 0분(그 시각 다른 spark 일일 작업 없음, 매시
  작업만). 이행은 11-06 18:45 에 이어 간다 — 늦어지는 것은 최대 하루, 이행은 일회성이고 단계마다 이어 가므로 잃는 것은 없다.
- 비용: 시작마다 파이썬 한 번(jobs 목록 읽기, ms 단위). 돈 0.
- 대상 작업의 늦은 시작이 밤에 막히면 그날 run 은 다음 날로 밀린다. FLOOR 가 그런 날은 하루 늦다. 대신 사장이 깨어 있는 시간의
  cut-over 를 잃지 않는다.
- 남은 구멍: 21:35 공매 Gold 가 만드는 입력 이벤트로 밤에 도는 공매 bake(최대 115분)는 여전히 cut-over 의 Spark 대기에 걸릴 수
  있다. 이벤트 작업을 창에 묶을지는 데이터 신선도와 맞바꾸는 결정이라 여기서 정하지 않는다.
