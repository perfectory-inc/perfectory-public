# ADR 0625: ai-server 는 수집과 그에 필요한 것만 돌린다 — 호스트 범위 스위치 하나(host_scope)가 정하고, 작업의 부류는 파이프라인 그래프에서 계산한다

- Status: Accepted (사장 결정 2026-11-27 "AI 빼고 전부 AWS, ai-server 는 AI 전용", 코디네이터가 분류와 방식을 승인)
- Date: 2026-11-27
- 다루는 것: ai-server 에서 어떤 예약 작업·호스트 타이머·서비스가 도는가. AWS 쪽 레인은 [ADR-0616](./0616-data-jobs-run-as-dagster-assets-on-aws-ecs-and-the-host-job-layer-is-not-ported.md)·[ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) 그대로다.

## Context

사장은 2026-11-27 에 AI 를 뺀 모든 것을 AWS 로 옮기고 ai-server 를 AI 전용으로 쓰기로 했다. 1단계는 지금이다. ai-server 의
파생 계산(Silver·Gold·타일·by-PNU 굽기와 그 검사)을 모두 끄고 원천 수집만 남긴다. 2단계에서는 수집 레인마다 AWS 쪽이 검증되면
호스트의 그 레인을 끈다.

수집이 남는 이유: 어떤 원천은 지난 것을 주지 않는다(달마다의 필지 판, 용도지역, 법원경매, 온비드, 팩토리온 공고). 멈춘 수집기가
놓친 기간은 영영 없다. 파생 계산은 멈춰도 된다. Bronze 가 남으므로 Silver·Gold·타일은 나중에 AWS 에서 다시 만든다. 출시 전에는
서빙 신선도가 목표가 아니다.

2026-11-27 ai-server 를 읽기만 하여 잰 것(Prometheus node-exporter·cAdvisor, 7 일):

| 무엇 | 값 |
|---|---|
| CPU 바쁨 / iowait | 2.7 코어 / 5.1 코어 (20 코어). 디스크가 병목이다 |
| MemAvailable 평균 / 최저 | 44 GB / 19.7 GB (64 GB) |
| 큰 메모리 | Spark run 하나 약 20 GB, Trino 최고 12.1 GB, Gold DuckDB 빌드 최고 11.7 GB, DataHub 상시 약 4.5 GB |
| 켜진 작업 | 109 개 중 파생 94, 수집 15 |

끄는 방법을 손으로 정하면 작업 100 개에 `enabled: false` 를 적어야 한다. 그러면 결정 하나가 100 군데에 흩어지고, 되돌리기도
100 군데다. 또 `job_specs.py` 는 `dagster_stage` 가 `airflow` 가 아닌 꺼진 작업을 거부하므로 단계까지 함께 고쳐야 한다.

## Decision

1. **스위치는 하나다.** `orchestration/jobs.v1.json` 의 `host_scope`(정본은 `definitions/platform.definition.json`)이다. 값은
   `all` 이나 `collection` 이고, 없으면 `all` 이다. 배포가 읽는 같은 정의 파일에 `observers.dagster.enabled` 와 나란히 있다.
2. **작업의 부류는 계산하고, 작업마다 적지 않는다.** `orchestration/dags/job_specs.py` `host_classes` 가 작업 목록과 파이프라인
   그래프만 읽어 부류를 정한다.
   - `collection`: `dagster_group` 이 `collect` 인 작업.
   - `collection_support`: 수집에 필요한 작업이다. 다음 셋 중 하나에 해당한다.
     - 수집 작업이 읽는 노드를 쓴다. 법원경매 수집기는 `silver.court_auction_item_detail` 등에서 오늘 밤 읽을 물건을 계산한다.
     - 스스로 Bronze 에 내린다(원천 그룹 노드를 쓴다).
     - 다른 작업을 조정한다. 조정기는 결과 원장을 수집기의 호출([ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)·[ADR-0614](./0614-a-collector-of-a-source-whose-provider-drops-what-ended-is-held-to-its-reads-whether-or-not-it-is-enabled.md))로 접는다.
   - `service`: 어느 작업도 쓰지 않는 Postgres 기록을 비우는 작업이다. 모든 Bronze 적재가 쓰는 outbox, 그리고 감사 사슬이 그것이다.
   - `derived`: 그 밖의 모든 작업. Bronze 가 남으므로 어디서든 다시 만든다.
   - 그래프에 빠져 있던 실제 간선 둘을 더했다.
     - `outbox_publish` 가 `serving-outbox`(catalog.outbox_event)를 읽어 두 수신기에 넘긴다.
     - `registered_factory` 가 data.go.kr 파일 수집으로 zip 을 Bronze 에 내린다.
   - 2026-11-27 의 목록으로 계산한 결과는 다음과 같다. 코디네이터가 승인한 분류와 같다.
     - 호스트에 남는 것 24 개: 수집 15, 보조 6, 서비스 3.
     - 꺼지는 것: 파생 100 개.
3. **"이 호스트에서 도는가"의 답은 하나다.** 답은 `runs_on_host` 다. 켜져 있고(`enabled`) 범위가 그 부류를 남기면 돈다.
   작업 목록을 그 답대로 다시 그린 사본은 `on_host` 다. 범위가 뺀 작업은 꺼진 것으로 보이고 `host_scope_off` 가 붙는다.
   이 답을 읽는 곳은 다음과 같다.
   - 호스트의 강제 명령 `start-scheduled-job.sh`: 범위 밖 작업은 운영자의 run-now 도 거부한다.
   - 스케줄러의 sudoers.
   - Dagster 의 센서(`decide.sensors`)와 `job_orchestrator.decided`.
   - 배포의 목록(`foundation-deploy.sh jobs_of`, `unpause_order`)과 한 번 시작 판정(`deploy_steps.py start-once.<job>`).
   - Airflow 의 일시정지(`airflow-runtime.sh`).
   - 조정기(`asset_reconcile.py`)와 준비 보고서.
   - foundation-api 의 운영 현황(`job-dependencies.v1.json` 의 `runs_on_host`·`host_class`).
4. **`collection` 에서 배포가 하는 일.** 다음 자동배포의 `timers` 단계가 다음을 한다.
   - 범위 밖 작업의 도는 유닛을 멈추고 실패 상태를 지운다. 다시 시도하지 않는다. 유닛 파일은 남는다.
   - 파생 데이터를 서빙하고 재는 by-PNU 타이머 넷(자동 발행 제어기, 에지 탐침, 주간 화면 탐침, 레인마다의 서빙 모니터)을 끈다.
   - Trino 와 DataHub 컨테이너는 배포의 자기 단계에서 멈춘다. 지우지 않는다. 지우는 것은 정리 프로그램과 [ADR-0609](./0609-the-data-catalogs-facts-live-in-files-and-apis-we-own-and-datahub-is-removed-run-history-from-dagster-quality-results-as-dagster-asset-checks-generated-from-the-contracts.md) 의 몫이다.
   - 상시 Spark 클러스터는 없다. Spark 는 파생 작업이 run 마다 띄우는 컨테이너였다.
   - `host_scope: all` 로 되돌리면 한 릴리스로 모두 다시 켜진다.
5. **파생의 경보는 그 작업과 함께 멈춘다.**
   - 범위 밖 작업은 결과 시계열이 없다. 조정기가 판정하지 않는다.
   - 조정기는 `foundation_host_runs_derived_compute` 를 낸다. 경보 `FoundationHostRunsNoDerivedCompute` 는 이 값이 0 일 때 뜬다.
     등급은 `severity: none` 이고 받는 곳은 `inhibition-only` 다. 아무에게도 가지 않는다.
   - 이 경보가 Alertmanager `inhibit_rules` 로 `service: serving-freshness` 와 `computed_by: derived`(미디어 대조기) 경보를 누른다.
   - 수집기·유닛·호스트·배포·백업의 경보는 그대로다.
   - 가드 `alertmanager-routes-pass-their-tests` 는 `none` 을 `inhibition-only` 로 받는다.

## 검토한 대안

| 대안 | 버린 이유 |
|---|---|
| 작업 100 개에 `enabled: false` | 결정 하나가 100 군데에 흩어진다. 단계도 함께 고쳐야 하고, 되돌리기도 100 군데다 |
| systemd `mask` | 유닛 파일이 `/etc/systemd/system` 에 있어 mask 가 덮지 못한다 |
| 파생 유닛 파일을 설치하지 않음 | 되돌리기가 무겁다. 지우는 것은 정리 프로그램의 몫이다 |
| 유닛마다 `ExecCondition` 으로 릴리스 승인에서 거부 | 모든 시작 경로를 systemd 에서 막는 가장 강한 형태다. 그러나 승인 코드를 넓힌다. 지금의 거부 지점은 `enabled` 를 지키는 곳과 같다 |

## Consequences

- **첫 자동배포.** 이 배포는 바꾸는 릴리스의 배포 스크립트로 돈다. 그래서 판정은 모두 새 릴리스의 코드에 있다.
  - 센서·`decided`·`unpause_order`·한 번 시작 판정은 배포가 `current` 에서 부른다.
  - 강제 명령과 sudoers 는 `timers` 가 새 릴리스로 다시 만든다.
  - 그 배포의 7단계가 범위 밖 작업을 다시 켜거나 시작하지 않는다.
- Trino 를 읽는 Dagster 검사(`parcel_source_resolution`) 가운데 남는 자산에 붙은 것은 막지 않는 검사이고, 자동으로 실행되지 않는다.
- 이제 멈추는 것:
  - 더니어의 계보 검토 큐, 품질 결과, 준비 보고서의 파생 칸.
  - shape ledger 의 머리. CI 는 72 시간 뒤 경고만 한다.
  - 레이크하우스 유지보수.
- 2단계(레인마다 호스트에서 끄기)는 그 레인의 `enabled: false` 다. 이 스위치를 쓰지 않는다. 마지막 수집기가 떠나면 `host_scope` 에
  남길 작업이 없다. 그때 무엇을 할지는 이 ADR 이 정하지 않는다.
