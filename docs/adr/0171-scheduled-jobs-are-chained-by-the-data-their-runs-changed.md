# ADR 0171: 예약 작업은 시계가 아니라 실행이 바꾼 데이터로 잇는다

- Status: Accepted
- Date: 2026-10-10
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(Airflow·계보),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(작업 = systemd 유닛),
  [ADR-0165](./0165-dawneer-shows-operations-status-from-the-data-catalog-and-the-job-list.md)(더니어 실행 기록)
- Amends: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(굶주림 상한의 셈법,
  5항), [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)
  §4·§5(데이터 기반 예약, 레인 등록)

## Context

2026-10-09 측정:

- 예약 작업(`platforms/foundation-platform/orchestration/jobs.v1.json` → `orchestration/dags/foundation_jobs.py`,
  작업마다 DAG 하나, Airflow 3.3.2)은 시각으로만 이어져 있다. source_sweep 03:40 UTC → … → gold_panel_rebuild
  08:45 → by_pnu_serving_bake 10:15. 일은 몇 분인데, 03:40 뒤에 도착한 원천은 서빙까지 하루 반을 기다린다.
- `spark` 풀의 굶주림 상한(job_specs.py `STARVATION_CYCLES=20`, ADR-0138)이 꽉 찼다. 매시 접기가 1,200/1,200 분을
  기다린다. ADR-0169 ②의 허브 Silver 레인 다섯(각 3 슬롯)을 시각 작업으로 더하면 접기가 2,000 분이 되어 등록할 수
  없었다.
- 기존 셈법은 접기를 막는 모든 작업을 더했다. 그러나 Airflow 는 대기 중인 무거운 작업이 들어가지 않을 때만
  가벼운 작업을 대신 시작한다(`scheduler_job_runner.py` "we can execute tasks with lower priority if there's
  enough room"). 접기 이상의 슬롯을 쓰는 가벼운 작업은 접기가 못 들어가는 자리에 들어갈 수 없으므로 앞질러 시작하는
  일이 없다. 접기 차례가 왔을 때 이미 돌고 있을 수 있을 뿐이다.

## Decision

1. **입력과 출력이 Airflow 자산이다.** 작업이 쓰는 파이프라인 그래프 노드마다 자산 하나다. 자산의 이름은 그 노드의
   데이터 카탈로그 데이터셋이다(`<플랫폼>://<이름>`, `job_specs.asset_uri`, `infra/datahub/dataset_names.py`).
   어느 작업이 어느 작업을 먹이는지는 각 작업의 `pipeline_graph_edges` 에서 읽는다(`job_specs.producers`).
   의존 목록을 따로 적는 곳은 없다.
2. **작업은 시각이나 입력으로 시작한다.** `jobs.v1.json` 의 작업마다 `started_by` 를 적는다.
   - `schedule`: `schedule` 시각에 시작한다. 밖의 세계나 직원이 움직이는 것이 여기에 든다. source_sweep, 접기,
     outbox, 수집기, 계보 관리(직원 결정을 접는다), FLOOR(신규 수집과 무관하게 매일 재확인, 작업 설명), data_quality
     (매일 보고한다)가 그렇다.
   - `inputs`: 다른 작업의 실행이 그 작업의 입력을 바꿨다고 하면 곧바로 시작하고, `schedule` 시각에도 시작한다.
     Airflow 구성은 `AssetOrTimeSchedule(timetable=CronTriggerTimetable(schedule, "UTC"), assets=AssetAny(입력…))`
     이다. `AssetAny` 를 쓴다. 목록을 그대로 주면 모든 자산을 기다리는 `AssetAll` 이 된다.
   - 시각은 대체 경로로 남긴다. 이벤트가 알리지 않는 일이 있기 때문이다. 손 적재(Silver 13개가 아직 그렇다,
     ADR-0169), Airflow 밖 실행(배포의 `started_once_after_deploy`), DAG 가 멈춰 있던 동안의 이벤트가 그렇다.
     Airflow 는 멈춘 DAG 몫의 이벤트를 큐에 넣지 않는다(`assets/manager.py`). 실패한 실행과, 차례를 미룬 굽기
     (`takes_turns`)도 이 경로로 다시 돈다. 하류 작업은 할 일이 없으면 싸게 끝난다(Gold "nothing to do", 굽기
     "nothing to do", 레인 `unchanged`).
3. **바뀐 것만 다음을 부른다.** 작업 유닛은 마지막 줄에 `foundation-job-outcome changed|unchanged` 를 찍는다. DAG 의
   `run`(SSH) 태스크는 출력 끝 64 KiB 에서 이 줄의 마지막 것을 읽고, 그 낱말만 XCom `outcome` 으로 남긴다. 저널
   전체는 더 이상 XCom 에 남지 않는다. 출력이 있는 작업에는 둘째 태스크 `publish_outputs` 가 있다. 출력 자산을
   `outlets` 로 가지고, `unchanged` 면 건너뛴다(`AirflowSkipException`). 건너뛴 태스크는 자산 이벤트를 남기지 않는다
   (`task_runner.py`: 이벤트는 `SucceedTask` 에만 실린다). 줄이 없으면 `changed` 로 센다. 아직 말하지 않는 작업이나
   저널 추적이 끝 줄을 놓친 실행이 하류를 굶기지 않게 하려는 것이다.
   - 이 변경 뒤 줄을 찍는 작업: source_sweep(새 파일이 Bronze 에 왔는지), Silver 레인 다섯(발행기의
     `silver-refresh-outcome … outcome=` 을 옮긴다. `--plan` 은 찍지 않는다), gold_panel_rebuild(어느 표든 스냅숏을
     커밋했는지), by_pnu_serving_bake(어느 레인이든 발행했는지, 반영만 한 매니페스트 포함). 그리고 호스트의
     `start-scheduled-job.sh` 가 차례를 미룬 실행에 `unchanged` 를 찍는다.
   - 찍지 않는 작업(항상 `changed`): FLOOR, 계보 관리, 수집기 셋, outbox, 접기 둘. FLOOR·계보가 돌 때마다 Gold
     재생성이 시작되고, 바뀐 것이 없으면 "nothing to do" 와 `unchanged` 로 끝나 굽기는 시작되지 않는다.
4. **계보는 그대로다.** `run` 태스크는 지금처럼 OpenLineage `Dataset` 을 inlets/outlets 로 가져 DataHub 에 보고한다.
   자산 스킴(`iceberg`, `perfectory`)은 어떤 제공자도 OpenLineage 데이터셋으로 바꾸지 않는다. `publish_outputs`
   는 `extend_global_openlineage_emission_policy(…, emit_task_events=False)` 로 자기 이벤트를 내지 않는다. DataHub 의
   흐름에는 지금처럼 DAG 와 `run` 만 있고, 더니어가 읽는 "가장 최근 실행"(ADR-0165)도 바뀌지 않는다.
5. **굶주림 상한(ADR-0138)의 셈법.**
   - 작업 하나가 자기 슬롯을 기다리는 최악 시간 = 앞질러 시작할 수 있는 작업의 점유 시간 합 + 차례가 왔을 때
     이미 풀을 잡고 있을 수 있는 가장 나쁜 묶음이 슬롯을 내줄 때까지의 시간. 앞지를 수 있는 작업은 같거나 무거운
     작업과, 더 가볍지만 더 적은 슬롯을 쓰는 작업이다. 더 가볍고 슬롯이 같거나 많은 작업은 앞지를 수 없고 둘째
     항에만 든다(`job_specs.longest_wait_minutes`).
   - `inputs` 작업의 주기 = 자기 대체 시각과 먹이는 작업들 주기 중 가장 짧은 것이다(재귀, `job_specs.cycle_minutes`).
     먹이는 작업보다 자주 시작할 수 없기 때문이다. 상한은 이 주기의 20배다.
   - `job_specs.py` 가 거부하는 것: 먹이는 작업이 목록에 없는 `inputs` 작업, 먹이는 작업이 모두 꺼진 켜진
     `inputs` 작업, 자기 입력을 쓰는 `inputs` 작업(스스로를 부른다), 서로를 부르는 원.
   - 수치(분, 한도 1,200 = 매시 20번): 접기 1,200 → 1,200(FLOOR 505 + 계보 265 + 다른 접기 265 + 이미 돌던
     Gold 165. 레인 하나는 160 이라 Gold 보다 짧다). 옛 셈법이었다면 레인 다섯을 더해 2,000 이다. FLOOR 2,220,
     계보 2,460, 레인 3,365, Gold 3,360, 굽기 1,735 이고, 모두 1일 주기의 한도 28,800 안이다.
   - 그래서 레인은 `priority_weight` 4(접기 5 아래, Gold 3 위), `retries` 0, `timeout_minutes` 160 이고, 유닛의
     `TimeoutStartSec` 는 4h 에서 150m 이 되었다. 레인이 Gold 의 165 분보다 오래 슬롯을 잡으면 접기가 상한을
     넘는다. 첫 감독 실행이 150 분을 넘게 재면 이 결정을 다시 연다. 유닛 줄만 올리지 않는다(시험이 거부한다).
6. **레인 다섯을 작업으로 등록한다.** `silver_refresh_building_register_{titles,units,unit_areas,apartment_price,
   exclusive_unit}` 는 `foundation-silver-refresh@<레인>.service`, `spark` 3 슬롯, `started_by: inputs`(source_sweep
   이 쓰는 `source-building-hub-bulk`), 대체 시각 07:00 이다. 모두 `enabled: false` 로 둔다. 레인마다 첫 감독
   실행 뒤 켠다(ADR-0122 §4). 릴리스는 이제 작업 목록 반복문으로 템플릿의 릴리스 승인을 설치한다. 메모리 예산
   계약(`tools/host-memory-budget.contract.json`)에는 레인마다 유닛 `MemoryMax` 와 compose `spark` 를 적었다.
   gold_panel_rebuild(대체 08:45)와 by_pnu_serving_bake(대체 10:15)는 `inputs` 가 된다.
7. **배포는 그대로다.** `airflow-runtime.sh` 의 pause/unpause 와 `foundation-deploy.sh` 의 일시정지 →
   `started_once_after_deploy` → 재개는 DAG id 로만 움직이므로 자산 예약 DAG 에도 같다. 배포 중 멈춘 동안의
   이벤트는 사라지고, 대체 시각이 그 몫을 메운다.

## Consequences

- 원천이 03:40 뒤에 도착해도 Silver → Gold → 서빙이 실행 시간만큼 걸린다(레인이 켜진 뒤). Gold 재생성은 FLOOR 와
  계보 관리가 끝나는 대로 돌고, 바뀐 것이 있을 때만 굽기를 부른다.
- 하루에 같은 작업이 두 번 돌 수 있다(이벤트 한 번 + 대체 시각 한 번). 둘째는 할 일이 없어 짧다.
- 더니어의 "다음 시작"은 `inputs` 작업에서 대체 시각을 보인다. "늦어도 이때"라는 뜻이다. 화면에 `started_by` 를
  보이는 일은 후속이다.
- source_sweep 의 결과는 작업 단위다. VWorld 파일만 와도 허브 레인 다섯이 시작되고 `unchanged` 로 몇 초 만에 끝난다.
  출력별 결과는 필요해지면 다음 결정이다.
- 굽기는 Gold 가 커밋하면 곧 시작되지만 `takes_turns` 로 미뤄질 수 있다. 그때는 10:15 대체 시각에 돈다.
- 검사: `orchestration/tests/test_job_specs.py`(셈법·거부·결과 줄), `test_job_dags.py`(DAG 배선은 대역으로 늘
  돌고, Airflow 가 있는 곳에서는 실제 직렬화까지 확인한다), 각 스크립트 시험(마지막 줄).
