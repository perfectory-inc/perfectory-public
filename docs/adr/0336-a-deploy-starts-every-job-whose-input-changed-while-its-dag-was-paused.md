# ADR 0336: 배포는 DAG 가 멈춰 있는 동안 입력이 바뀐 작업을, 다시 켠 뒤 시작시킨다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0179](./0179-a-deploy-unpauses-readers-first-and-a-triggered-run-is-not-deferred.md)(받는 작업 먼저 켜기),
  [ADR-0173](./0173-a-failed-post-deploy-run-does-not-pause-the-schedule.md)(배포 뒤 DAG 재개)

## Context

2026-10-28 21:38:23Z 공매 Gold 재빌드(`gold_public_auction_rebuild`)가 `gold.parcel_public_auction` 을 바꾸고 asset
event 3912 를 냈다. 이 사건으로 시작해야 할 `by_pnu_public_auction_bake` 는 시작되지 않았고, 대기열에도 아무것도
들어가지 않았다. 굽기는 다음 날 10:45 대체 일정(fallback)에서야 돌 수 있었다: 화면 반영이 약 13시간 늦었다.

Airflow DB(읽기 전용)에서 확인한 것:

| 항목 | 값 |
|---|---|
| 연결 | 굽기 DAG 의 `dag_schedule_asset_reference` 가 `iceberg://gold.parcel_public_auction/` 를 가리킴; 재빌드의 `publish_outputs` 가 같은 asset 에 사건을 냄. URI·timetable(`Asset or 45 10 * * *`) 모두 정상 |
| 사건 3912 | `asset_dag_run_queue` 0행, `dagrun_asset_event` 연결 0행 |
| 굽기 DAG | 21:19:26 배포가 멈춤(`cli_dag_pause`) → 21:40:55 다시 켬. 사건은 그 사이 |
| 재빌드 DAG | 같은 배포가 21:32:29 먼저 켬(D5, ADR-0270 §3: 붙잡지 않은 작업은 빌드 뒤 바로 재개). 21:35 일정 실행이 21:38 에 사건을 냄 |

Airflow 3.3.2 `AssetManager.register_asset_change` 는 멈춘 DAG 를 대기열 대상에서 뺀다
(`{ref.dag for ref in asset_model.scheduled_dags if not ref.dag.is_paused}`). 다시 켜도 놓친 사건을 되살리지 않는다.

ADR-0179 는 같은 손실(2026-10-10)을 "받는 작업을 먼저 켠다"로 막았지만, 순서만으로는 막을 수 없는 경우가 남았다.

1. 배포가 받는 작업만 붙잡고(hold) 보내는 작업은 붙잡지 않는다. by-PNU 굽기는 Gold 표에 쓰지 않아서(`table_name` 없음)
   표가 하나라도 바뀌면 늘 붙잡히고, 그 Gold 재빌드는 대개 붙잡히지 않는다. D5 가 보내는 작업을 먼저 켠다.
2. 0단계가 받는 작업을 멈춘 뒤에도 이미 돌던 보내는 작업의 실행은 끝나며 사건을 낸다.
3. 5단계 `sync_enabled` 는 작업 목록 순서로 켠다. 사람이 손으로 멈추고 켜도 같다.

최근 14일 사건 전수(사건 × 그 asset 을 기다리는 DAG, 실행으로 이어지지 않은 쌍): 진짜 손실은 3912 하나. 다른 4건
(`gold_panel_rebuild` ← `reference.legal_dong_code_snapshot`, 10-15~10-18)은 그 DAG 가 2026-10-18 22:16 에야 이 asset 을
기다리기 시작해서 생긴 것으로, 손실이 아니다.

## Decision

1. **놓친 사건은 순서가 아니라 상태로 되찾는다.** 배포는 7단계(그리고 전환 전 중단 시 재개, ADR-0173)에서 켤 DAG 를
   모두 켠 **뒤**, 작업이 진 빚을 묻는다. `job_specs.owed_by_inputs()` 가 답한다: 입력으로 시작되는(`started_by: inputs`)
   켜진 작업 가운데, 시작 입력(`starting_inputs`, 제가 쓰는 것 제외) 하나의 최신 사건이 그 DAG 의 최신 실행(종류 무관)보다
   늦고, Airflow 가 그 DAG 의 대기열 항목을 갖고 있지 않은 작업. 작업을 켠 시각(`enabled_since`, ADR-0239) 이전 변화는
   빚이 아니다. 누가 누구를 시작시키는지는 jobs.v1.json 과 pipeline graph 에서만 읽는다(ADR-0171).
2. Airflow 상태는 스케줄러 컨테이너 안에서 `scripts/deploy/owed_by_inputs.py` 가 읽기만 한다(사건별 최신 시각, DAG 별
   최신 실행, 대기열). 배포는 답한 작업마다 `airflow dags trigger` 를 한 번 보낸다(`<id> triggered`, ADR-0179 §2). 이미
   ADR-0260 미뤄진 작업으로 시작시킨 작업은 다시 시작시키지 않는다. 답을 못 얻으면 배포는 실패하지 않고 한 줄로 알리며,
   대체 일정이 남는다. 이 질문이 없는 릴리스(되돌린 릴리스)는 묻지 않는다.
3. 다 켠 뒤에 묻기 때문에, 그 뒤의 사건은 Airflow 가 직접 대기열에 넣는다. 묻는 것은 사건의 역사가 아니라 지금 상태이므로
   같은 사건으로 두 번 시작되지 않는다(실행이 생기면 그 실행 시각이 사건보다 늦다).

이는 Kubernetes 컨트롤러가 사건(edge)이 아니라 상태(level)로 맞추는 방식, Dagster 선언형 자동화가 "마지막 실행 뒤 상류가
바뀌었는가"를 매 틱 평가하는 방식과 같다. Airflow 는 멈춘 DAG 의 사건을 저장해 두지 않으므로, 배포가 자기가 멈춘 구간을
스스로 메운다.

## Consequences

- 2026-10-28 경우: 사건 21:38:23 → 7단계(21:43 무렵)에 굽기 시작, 지연 약 5분(배포 남은 시간). 고치기 전 13시간 7분.
  멈추지 않은 DAG 의 사건은 지금처럼 Airflow 가 1초 안에 실행을 만든다(2026-10-18 사건 2070 → 실행 1초 뒤, 실측).
- 운영 Airflow 에 대고 같은 판단을 돌린 결과(2026-10-29 05시, 읽기 전용): 빚진 작업은 `by_pnu_public_auction_bake`
  하나, 1.1초. 이 변경이 배포되는 첫 배포가 그 굽기를 시작시킨다(마지막 실행 50초).
- 배포가 아닌 손 멈춤·재개는 다음 배포가 메운다. 상시 조정 루프(배포 밖)는 자산 중심 오케스트레이션 개편(별도 제안 중)에
  맡기고 여기서는 만들지 않는다.
- 보내는 작업을 받는 작업과 함께 붙잡는 일(hold 의 상류 닫힘)과 `sync_enabled` 의 켜는 순서는 지연만 줄일 뿐 손실은 이
  결정이 막으므로, 이번에는 바꾸지 않는다.
- 시험: `test_job_specs.AnEventAPausedReaderMissedIsOwed`(사고 재현, jobs.v1.json·graph 에서 끌어낸 모든 보내는 작업 →
  받는 작업 쌍이 빚으로 잡힘, `enabled_since`, 자기 출력), `test_foundation_autodeploy`(다 켠 뒤 묻고 시작, 중복 없음,
  답 실패는 배포 실패 아님, 질문 없는 릴리스). 정적 연결(보내는 쪽 outlet ∩ 받는 쪽 schedule)은 기존
  `test_job_dags.test_what_a_job_publishes_is_what_the_jobs_it_feeds_wait_for` 가 지킨다.
