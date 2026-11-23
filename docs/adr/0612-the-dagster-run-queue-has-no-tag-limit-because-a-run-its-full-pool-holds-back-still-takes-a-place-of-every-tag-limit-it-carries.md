# ADR 0612: Dagster 의 run 줄에는 태그 한도를 두지 않는다 — 풀이 막은 run 도 제가 단 태그 한도의 자리를 쓴다

- Status: Accepted
- Date: 2026-11-23
- Supersedes: [ADR-0583](./0583-jobs-move-to-dagster-in-four-groups-one-switch-each-and-the-per-job-dual-run-gate-retires-with-the-first.md)
  §7 의 job run 한도 6(`foundation/run=job`)과 Dagster 가 정하는 run 의 한도 5(`foundation/decided-by=dagster`);
  [ADR-0605](./0605-scheduled-job-pools-are-cut-by-the-resource-they-guard-and-counted-by-dagsters-own-pools-and-a-jobs-wait-is-measured-not-computed.md)
  §6 의 "`foundation/decided-by=dagster` 한도 5 는 풀 칸 합에 기본 풀 적재 한 자리를 더한 값" 줄.

## Context

**11-23 06:40Z 에 잰 것.** Dagster 복구(05:17Z)와 de518845 배포(06:3xZ) 뒤 Dagster 의 줄은 이랬다.

| 것 | 값 |
|---|---|
| 줄에 선 run | 51개(가장 오래된 것 05:23Z) |
| 도는 run | 3개 |
| 줄 전체 한도 | `max_concurrent_runs` 7 |
| `duckdb` 풀 | 두 칸 모두 비었다. 그런데 `duckdb` run 17개가 75분째 줄에 서 있었다 |
| 시작된 run(10분마다, 05:50Z 부터) | 2, 1, 3, 1, 2 |

줄의 우선순위(`dagster/priority`, ADR-0605)로 맨 위 10개는 모두 `spark` run 이었다. `spark` 는 한 칸이고 그 칸은 차 있었다.

**Dagster 가 줄을 비우는 순서.** 1.13.27 `dagster/_daemon/run_coordinator/queued_run_coordinator_daemon.py` 의 `_get_runs_to_dequeue` 는 run 을 우선순위 차례로 본다. 같은 날 업스트림 master 도 같다. run 마다 이렇게 한다.

1. 태그 한도에 걸리는지 본다. 걸리지 않으면 **태그 계수기를 올린다**(`update_counters_with_launched_item`).
2. 그다음에 풀(전역 동시 한도)에 걸리는지 본다. 걸리면 그 run 을 빼지만, **올린 태그 계수기는 되돌리지 않는다.**

그래서 꽉 찬 풀에 막힌 run 도 제가 단 모든 태그 한도의 자리를 쓴다. 06:40Z 에는 도는 run 3개가 `decided-by=dagster` 한도 5 의 세 자리를 쥐고 있었다. 줄 맨 위의 막힌 `spark` run 둘이 남은 두 자리를 판마다 다 썼다. 그 아래의 `duckdb`·기본 풀 run 은 태그 한도에 걸려 조용히 빠졌다. 이 거름은 로그를 남기지 않는다. 풀에 막힌 것만 run 마다 한 번 로그를 남긴다.

일시적인 밀림이 아니다. 줄에 선 `spark` run 이 태그 한도의 빈자리보다 많으면 늘 이렇게 된다. Gold 의 `spark` run 은 몇 시간 가므로 그런 때가 잦다. 원인은 둘이 겹친 데 있다. ADR-0605 가 들인 run 단위 풀과, 그 전부터 있던 태그 한도 둘이다. 모든 job run 은 두 태그를 단다(`job_specs.JobSpec.dagster_tags`).

**업스트림.** 11-23 에 찾아보니 이 동작을 다룬 이슈는 없었다. 가까운 것은 줄 고르기를 선형으로 만드는 PR [dagster-io/dagster#34473](https://github.com/dagster-io/dagster/pull/34473) 인데, 이 순서는 그대로 둔다. 올릴 사실은 위의 두 단계와 아래 시험이다.

## Decision

1. **줄에 태그 한도를 두지 않는다.** `concurrency.runs` 에는 `max_concurrent_runs` 만 둔다. 자원마다의 한도는 Dagster 의 풀(`spark` 1, `duckdb` 2, `gpu` 1, ADR-0605)뿐이다.
   - 풀은 막힌 run 을 세지 않는다(계수기는 막히지 않은 run 만 올린다).
   - 7 은 그대로다: 풀 칸 넷, 꽉 찬 풀 곁의 기본 풀 적재 하나, proxied run 하나, 검사 run 하나. 코드 서버 메모리 셈(ADR-0583 §7)도 7 로 맞다.
2. **잃는 것은 자리 예약이다.** 두 태그 한도는 자리를 비워 두는 일만 했다. 한 자리는 Airflow 가 넘기는 proxied run 몫이고, 한 자리는 airlift 의 검사 run 몫이다.
   - 이제는 빈자리가 줄의 우선순위 차례로 간다. proxied run 은 job 의 우선순위를 단다. airlift 의 검사 run 은 우선순위가 없어 맨 뒤다.
   - 둘 다 Airflow 의 것이라 11-25 Airflow 를 걷어 낼 때 끝난다. 그때까지 검사 run 은 밀린 줄 뒤에서 기다릴 수 있다.
3. **시험이 Dagster 의 줄 비우기를 그대로 돌린다**(`test_dagster_observe.py` `TheQueueDequeues`, 실제 Dagster 1.13.27). 줄은 06:40Z 를 작게 줄인 것이다: `spark` 한 칸을 쥔 run, 그 위에 선 `spark` run 여섯, 그 아래 `duckdb` run 하나, 빈 `duckdb` 두 칸.
   - 저장소의 `dagster.yaml` 로 `duckdb` run 이 시작된다.
   - 같은 줄에 태그 한도 하나를 더하면 아무것도 시작되지 않는다. 이 시험은 Dagster 의 동작을 못 박는다. 실패하면 Dagster 가 순서를 바꾼 것이니 이 ADR 을 다시 읽는다.
   - `test_job_specs.py` 는 7 을 풀 칸 합 + 3 으로, 태그 한도가 없음을, 코드 서버 메모리를 지킨다.

이 시험이 막는 실제 사고: 11-23 06:40Z 에 `duckdb` run 17개가 빈 두 칸 곁에서 75분 기다렸다.

## Consequences

- Dagster 가 정하는 `duckdb`·기본 풀 run 이 `spark` 의 밀린 줄 뒤에서 굶지 않는다.
- Dagster 가 정하는 run 이 일곱 자리를 다 채울 수 있다. 풀이 넷을 넘지 못하게 하므로, 나머지는 기본 풀 적재다(수집 등, 대개 짧다).
- Airflow 가 넘기는 proxied run(예: 자산 조정자, 우선순위 100001)은 11-25 까지 빈자리를 우선순위대로 기다린다. 예약된 자리는 없다.

| 것 | 구분 | 언제까지 |
|---|---|---|
| run 줄(`QueuedRunCoordinator`), 우선순위, 풀(`concurrency.pools`) | **Dagster 기본 기능** | 계속 |
| 7 의 셈, 풀 칸(작업 목록) | 우리 것, 계속 | 계속 |
| 태그 한도 없음 | 우리 것, 계속 | Dagster 가 풀을 먼저 볼 때까지(못 박은 시험이 실패할 때) |

## 기각한 대안

- **태그 한도를 막힐 수 없는 run 에만 단다**(예: 기본 풀 run 에만 `foundation/pool=default` 한도). 막히지 않는 run 만 계수기를 올리므로 거름 순서는 맞다. 하지만 새 태그와 그 셈이 하나 더 생긴다. 지킬 자리(proxied·검사)는 11-25 에 끝난다.
- **풀에 막힌 run 의 우선순위를 낮춘다.** 그러면 `spark` 칸이 빌 때 `spark` run 이 태그 한도에 걸린다. 타일 굽기의 굶김(ADR-0605 의 계기)이 반대로 돌아온다.
- **Dagster 를 올린다.** master 도 순서가 같다.
- **호스트에서 손으로 지운다.** 코디네이터가 PR 로만 하라고 정했다. 하루 신선도가 한 시간을 견딘다.
