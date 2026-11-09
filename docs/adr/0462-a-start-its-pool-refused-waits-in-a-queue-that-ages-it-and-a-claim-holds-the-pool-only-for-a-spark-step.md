# ADR 0462: 풀이 거절한 시작은 나이를 먹는 대기열에서 기다리고, claim 은 Spark 단계 동안만 풀을 잡는다

- Status: Accepted
- Date: 2026-11-09
- Amends: [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)
  §2(막힌 시작은 `waiting` 으로 끝나고 아무것도 남지 않았다), §4(cut-over 러너는 run 동안 풀을 claim 한다)
- Builds on: ADR-0333 §7(자산 조정기), ADR-0439(클래스 순서), ADR-0260(배포가 미룬 작업의 기록), ADR-0431(Dagster 이행, W13e)

## Context

2026-11-09 호스트(journal, Airflow 메타 DB, `/var/lib/foundation-run-now/capacity`, 잰 값).

| 시각(UTC) | 무슨 일 |
|---|---|
| 06:13:03 | cut-over run 시작, `by_pnu_lane_cutover` claim(first_publish, spark 3슬롯) since 1794204783 |
| 06:13:35..38 | 그 run 의 parcel-register-masters 굽기·equality 끝, **latency 단계**(Spark 없음) 시작 |
| 07:00:27..56 | 새 Silver 적재 9개 + 기존 lane 들이 `not started: the spark pool is kept for claims queued before …: by_pnu_lane_cutover` |
| 07:10:19 | cut-over run 실패로 끝남, claim 풀림 — 거절된 시작은 아무도 다시 하지 않음 |
| 11:40:01 | `data_go_kr_file_data` 첫 실행도 같은 이유로 `skipped` |
| 12:40 | 다음 cut-over run(12:13 시작)이 다시 claim 을 잡고 팩 latency 단계(Spark 없음)를 도는 중 |

원인 셋.

1. **거절은 버려졌다.** spark 풀 작업은 모두 `wait_limit_minutes` 0 이고, `pool-full` 거절은 `foundation-job-outcome waiting` 만 남겼다.
   배포 빌드가 미룬 시작만 기록되고(ADR-0260) 풀 거절은 기록·재시도되지 않았다. 하루 한 번 도는 Silver 적재는 하루에 한 번만 시도한다.
   W38(11-09)이 잰 대로 새 적재 10개의 첫 실행이 전부 거절됐다.
2. **claim 은 run 전체를 잡았다.** cut-over run 은 history·preview·parity·verify·equality·latency·publish 처럼 Spark 를 쓰지 않는 단계에서도
   3슬롯을 예약했다. auto-promote 가 15분마다 새 run 을 띄우므로 claim 은 하루 내내 갱신됐다(11-09 하루 run 15회).
3. **기다림이 순위를 바꾸지 않았다.** default(100) 시작은 first_publish(500) claim 을 영원히 넘지 못한다. 굶주림의 상한이 없다.

## 업계 비교

| 방식 | 거절된 일 | 굶주림 방지 | 예약 |
|---|---|---|---|
| Dagster run queue (QueuedRunCoordinator) | run 은 `QUEUED` 로 남고, 풀(`tag_concurrency_limits`)에 자리가 나면 데몬이 꺼낸다 | 우선순위 태그, 같은 순위는 FIFO | — |
| Slurm (multifactor priority) | 대기열에 남는다 | **age factor**: 기다린 시간만큼 우선순위가 오른다(`PriorityWeightAge`, `PriorityMaxAge` 상한) | backfill 은 높은 작업의 예약 시각만 지킨다 |
| YARN fair scheduler | 대기 | fair share + preemption timeout | — |
| Kubernetes scheduler | Pod 은 `Pending` 으로 남고 백오프(1s→10s 상한)로 다시 스케줄된다 | PriorityClass(`preemptionPolicy: Never` 가능) | nominatedNode 는 실제 선점 중일 때만 |
| Kubernetes workqueue | 지수 백오프 재큐 | — | — |

공통점: 거절은 기록되어 다시 시도되고, 기다림은 순위에 반영되며, 예약은 그 자원을 정말 쓸 준비가 된 동안만 있다.

## Decision

1. **거절된 시작은 대기열에 남는다.** `start-scheduled-job.sh` 는 풀이 거절한 시작(운영자 run-now 제외)을
   `job_units.py queue <job> <이유>` 로 `/var/lib/foundation-jobs/queue/<job>.json` 에 적는다(첫 거절 시각 `since`, 거절 횟수, 마지막 이유).
   상태 디렉터리는 `jobs.v1.json` `state_directories`(소유 foundation-scheduler, 0755, 조정기가 읽음)다. 그 작업의 시작이 실제로 일어나면
   (또는 그 check 가 `unchanged` 면) `dequeue` 가 기록을 지우고, 얼마 기다렸는지를 `<job>.started.json` 과 `history.jsonl`(append-only)에 남긴다.
2. **자산 조정기가 다시 시작한다.** `asset_reconcile.py` 는 5분마다 대기열을 읽고, 작업이 돌지 않고 백오프(5분에서 두 배씩, 1시간 상한)가
   지났고 `pool-full` 이 자리를 주면 `foundation-asset-owed <job> <key>` 를 찍는다. 키는 (job, since, 거절 횟수)라 재시도마다 한 번만 뜬다.
   조정기의 DAG 가 시작할 수 있는 작업은 `reconciles` 와 새 필드 `requeues_refused_starts_in`(풀 목록, 지금 `["spark"]`)의 활성 작업이다
   (`job_specs.requeued`). Dagster 단계(`dagster`) 작업은 DAG 가 없으므로 빠진다 — Dagster 로 옮기면 QueuedRunCoordinator 가 같은 일을 한다.
3. **기다림이 순위를 올린다.** `jobs.v1.json` `queue_aging`(시간당 100점, 상한 900점). 대기열 기록이 있는 시작과 claim 모두
   `since` 부터 나이를 먹고(`job_units.aged`), 같은 순위면 먼저 줄 선 쪽이 먼저다. 07:00 에 거절된 default 시작은 4시간 뒤 새 first_publish claim 과
   같아지고 그 뒤로 앞서며, 9시간 뒤 publish 와 같아진다. 도는 작업의 슬롯을 빼앗지는 않는다(선점 아님).
4. **claim 은 Spark 단계 동안만.** cut-over 러너는 `gold`·`bake` 직전에 claim 하고 그 단계가 끝나면 지운다. 다른 단계는 claim 이 없다.
   만료 시각은 두지 않는다(ADR-0435 버린 대안 그대로): "준비된 동안만" 이 시각보다 정확하다.
5. **대기 시간을 재고 예산으로 알린다.** 클래스마다 `queue_wait_budget_minutes`(publish 120, first_publish 360, default 360, backfill 1440).
   조정기가 `foundation_job_queue_wait_seconds`·`foundation_job_queue_refusals`·`foundation_job_queue_wait_budget_seconds`·
   `foundation_job_queue_last_wait_seconds` 를 쓰고, `FoundationJobQueueWaitOverBudget` 가 예산을 10분 넘게 넘으면 울린다.

## 버린 대안

- **spark 작업마다 `wait_limit_minutes` 를 늘리기.** SSH 연결과 Airflow 슬롯을 시간 단위로 붙잡고, 상한이 다시 하나의 시도다. 굶주림은 남는다.
- **Airflow 재시도(`retries` + exponential backoff).** 실패와 거절이 한 숫자를 나눠 쓰고, 첫 실행은 `retries 0` 이어야 한다(ADR-0457).
- **claim 에 만료 시각.** 2h 넘게 도는 gold 단계 안에서 만료되면 그 사이 다른 시작이 끼어든다. 준비된 동안만 잡는 것이 옳다.
- **조정기 대신 새 데몬.** 조정기가 이미 상태 기반으로 owed run 을 띄우고 DAG 가 키마다 한 번만 시작한다(ADR-0333 §7). 새 장치가 필요 없다.

## Consequences

- 거절된 Silver 적재의 다음 시도: 이전 **24시간 뒤**(다음 날 07:00, 잰 값) → 풀이 빈 첫 조정 틱, 늦어도 백오프 상한 **1시간**(계산값).
- 11-09 06:13–07:10 의 claim 은 Spark 를 쓰지 않는 latency 단계 동안 **57분** 3슬롯을 잡았다(잰 값). 이 ADR 뒤: 그 단계의 claim **0분**(계산값).
- 굶주림 상한: default 시작은 새 claim 보다 **4시간** 뒤 앞선다(계산값, `queue_aging`). 예산 6시간을 넘으면 알린다.
- 사고 재현 시험: `test_start_scheduled_job.py::test_a_refused_start_is_queued_and_its_next_start_takes_it_off_with_its_wait`,
  `test_job_units.py::test_a_start_refused_long_enough_outranks_a_claim_queued_anew`,
  `test_asset_reconcile.py::QueuedStarts`, `test_by_pnu_lane_cutover.py::test_a_step_that_uses_no_spark_holds_no_claim`.
- 비용: 돈 0. 거절 한 번에 파이썬 한 번, 조정 틱마다 디렉터리 하나 읽기.
- Dagster 이행(ADR-0431, W13e): 대기열 기록·나이·예산은 Dagster run queue 의 `QUEUED` run·`dagster/priority`·풀 한도로 그대로 옮긴다.
  호스트의 `pool-full` 이 정본인 동안은 이 대기열이 정본이다.
