# ADR 0179: 배포는 받는 작업을 먼저 켜고, 데이터가 시작시킨 실행은 차례를 기다리지 않는다

- Status: Accepted
- Date: 2026-10-11
- Amends: [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(데이터 기반 연결),
  [ADR-0173](./0173-a-failed-post-deploy-run-does-not-pause-the-schedule.md)(배포 뒤 DAG 재개),
  [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(takes_turns)

## Context

2026-10-10 처음으로 원천 → Silver(표제부 9월판, 806만 행, 8분 30초) → Gold(건물 595만 동, 37분)를 운영 경로로 돌렸다.
Gold 는 새 스냅숏을 커밋했지만 굽기는 시작되지 않았다. 원인 둘:

1. **신호가 버려졌다.** 같은 시각 배포가 DAG 를 멈췄다가 다시 켰다. Gold 를 굽기보다 먼저 켰고, 멈춰 있던 Gold 의
   `publish_outputs` 가 0.5 초 만에 실행되어 `gold.building_panel` 사건을 냈다. 그때 굽기는 아직 멈춰 있었고, Airflow 는
   멈춘 DAG 로 가는 사건을 대기열에 넣지 않는다(asset_dag_run_queue 비어 있음, 실측).
2. **차례에 막혔다.** 손으로 굽기를 시작하자 `start-scheduled-job.sh` 가 takes_turns 로 미뤘다: 같은 풀의 FLOOR·계보가
   오늘 굽기 뒤로 아직 시작하지 않았다는 이유. 이 규칙은 며칠짜리 전체 굽기가 시간 작업을 굶기지 않게 만든 것인데,
   데이터가 바뀌어 시작된 굽기까지 다음 날 아침으로 밀었다.

## Decision

1. **배포는 받는 작업을 먼저 켠다.** `job_specs.unpause_order()` 가 작업 목록의 의존 그래프(`producers`, ADR-0171)에서
   켜는 순서를 정한다: 입력으로 시작되는 작업은 그 작업을 시작시키는 모든 작업보다 먼저. 배포의 7단계와 중단 시 재개
   (ADR-0173)가 이 순서를 쓴다. 목록을 따로 적지 않는다.
2. **차례 규칙은 시계가 시작한 실행에만.** DAG 는 시계가 시작한 실행이면 작업 id 만, 그 밖(입력 사건, 운영자 trigger)이면
   `<id> triggered` 를 보낸다(Airflow 의 run_type 으로 렌더). `start-scheduled-job.sh` 는 `triggered` 실행을 takes_turns 로
   미루지 않는다. 그 밖의 말은 여전히 거부한다.

## Consequences

- Gold 가 바뀌면 굽기가 바로 이어진다. 데이터 기반 연결이 배포 시각과 차례 규칙 때문에 하루씩 밀리지 않는다.
- 입력 사건은 상류가 바뀔 때만 오므로(하루 몇 번 이하) 굶김 상한(ADR-0138)은 그대로 유지된다. 시계가 시작한 굽기는
  지금처럼 차례를 지킨다.
- 시험: `test_job_specs.TheUnpauseOrder`(실제 목록에서 모든 받는 작업이 보내는 작업보다 먼저, 굽기 < Gold),
  `test_start_scheduled_job`(triggered 는 미루지 않음, 다른 말은 거부), `test_job_dags`(명령 템플릿).
