# ADR 0277: 작업은 풀을 기다리기 전에 할 일이 있는지 묻는다 — Silver 갱신 레인의 칸 없는 확인

- Status: Accepted
- Date: 2026-10-18
- Amends: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md) §2 (호스트 명령의 문법에
  `<job> check` 를 더한다), [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md) (DAG 의 task 에 `check` 를 더한다)

## Context

2026-10-17 아침 `silver_refresh_*` 12개 레인은 각각 spark 풀 3칸 전부를 잡은 뒤에야 "새 판이 없다"를 알았다.
Airflow 의 기록(task_instance)으로 본 그날:

| 구간 | 무슨 일 |
|---|---|
| 07:32–08:10 | 자동 배포. DAG 가 멈춰 있어 레인 실행은 08:06 에 줄을 섰다 |
| 08:10–11:28 | spark 풀을 lineage_stewardship(3칸, 30분씩 두 번 실패), building_register_floor(3칸), 폴드(2칸)가 차례로 잡았다. 3칸 작업은 풀이 다 빌 때까지 기다리고, 그 사이 2칸 폴드가 빈 칸을 먼저 가져갔다 |
| 11:28–11:31 | 레인 12개가 차례로 칸을 얻고 각각 약 0.2분 만에 `unchanged`(이미 반영) 로 끝났다 |

레인이 "할 일이 있는가"를 판단하는 데는 Spark 가 필요 없다([ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md) §3). 판단은 Bronze 장부(Postgres), Silver 표의
적재 기록(Iceberg REST 카탈로그), 허브 레인의 매니페스트(R2)만 읽는다. VWorld 토지 레인이 읽는 ZIP 안 이름도
장부 옆 표(`catalog.bronze_object_member`)에 있다. 이 판단은 이미 `silver-refresh.sh <lane> --plan` 으로 있다.
그런데 Airflow 의 `run` task 가 SSH 실행 내내 `pool_slots=3` 을 잡기 때문에, 판단도 풀 칸을 얻은 뒤에야 했다.

**판단 시간 실측.** 2026-10-14–18 에 `unchanged` 로 끝난 레인 실행(`--plan` 과 같은 판단을 하고 더 하는 일 없이
끝나는 경로)의 journal 에서, 유닛 시작부터 끝까지 잰 값이다. 릴리스 승인(ExecStartPre)과 DB 연결 해석도 포함한다.

| 레인 | 실행 수 | 최소 | 중앙값 | 최대 |
|---|---:|---:|---:|---:|
| building-register-titles | 8 | 1.5초 | 1.6초 | 2.1초 |
| building-register-units | 9 | 1.6초 | 1.7초 | 3.1초 |
| building-register-unit-areas | 8 | 1.5초 | 1.7초 | 2.0초 |
| building-register-apartment-price | 9 | 1.8초 | 1.9초 | 2.1초 |
| building-register-exclusive-unit | 9 | 1.8초 | 1.9초 | 2.7초 |
| land-characteristic | 8 | 1.5초 | 3.7초 | 4.2초 |
| land-forest-ledger | 7 | 1.5초 | 3.5초 | 4.1초 |
| land-individual-price | 8 | 1.6초 | 3.6초 | 4.0초 |
| land-right-registration | 7 | 1.6초 | 3.7초 | 3.9초 |
| land-transfer-history | 8 | 1.6초 | 3.8초 | 3.9초 |
| land-use-plan | 8 | 1.6초 | 3.7초 | 4.1초 |
| land-use-zone-code | 8 | 1.4초 | 1.7초 | 1.9초 |

토지 레인의 중앙값이 2초쯤 긴 것은 ZIP 안 이름 표를 읽기 때문이다. 가장 느린 것도 4.2초다.

**검토한 방법** (Airflow 3.3.2, 운영 판):

- 지연 실행(deferrable) 연산자·센서: 지연 상태의 task 는 풀 칸을 차지하지 않는다(풀의 `include_deferred` 기본값
  false). 그러나 triggerer 컴포넌트가 없고(compose 에 없음), SSH 로 판단하는 트리거를 새로 써야 한다. 센서는
  "조건이 참이 될 때까지 기다리는" 모양이라, "한 번 판단하고 없으면 끝"과 다르다. 기다리는 동안 `max_active_runs=1` 이
  다음 실행도 막는다. 기각.
- 센서 reschedule 모드: 확인할 때만 슬롯을 쓴다. 같은 이유(기다림의 모양)로 기각.
- Asset 만으로 시작(시각 대체 제거)·AssetWatcher: 레인은 이미 `AssetOrTimeSchedule` 이다. 시각 대체를 없애면 놓친
  이벤트 하나가 영영 실행되지 않는 판이 된다. 수집(source_sweep)을 원천별 asset 이벤트로 쪼개는 일은 이것보다 훨씬
  크다. AssetWatcher 는 메시지 큐 같은 외부 이벤트 원천용이다. 기각.
- 칸 없는 사전 확인 → 건너뛰기: Dagster 센서가 할 일이 없으면 `SkipReason` 만 남기고 실행을 만들지 않는 것과 같은 모양.
  Airbnb 가 확인 작업이 동시 task 의 70% 를 차지한 것을 Smart Sensor(AIP-17)로 모았던 것도 같은 교훈이다. 채택.

## Decision

1. **`check_service`.** `orchestration/jobs.v1.json` 의 작업은 `check_service` 를 가질 수 있다. 할 일이 있는지만
   판단하는 systemd 서비스다. 오늘은 12개 Silver 갱신 레인만 `foundation-silver-refresh-plan@<lane>.service` 를 갖는다.
   `orchestration/dags/job_specs.py` 는 플랫폼 서비스 이름이 아니거나, 유닛 파일이 없거나, 다른 작업의 서비스인
   `check_service` 를 거부한다.
2. **유닛.** `infra/systemd/foundation-silver-refresh-plan@.service` 는 `silver-refresh.sh %i --plan` 을 서비스 계정으로
   돌린다. 잠금을 잡지 않고 컨테이너를 띄우지 않으며 아무것도 쓰지 않는다. 마지막 줄은
   `foundation-job-check unchanged|would_change` 다. plan 은 실행이 아니므로 `foundation-job-outcome` 은 쓰지 않는다.
   환경 파일은 runtime-secrets 계약의 유닛 항목이 정한다. 메모리 상한은 1G 이고, 시간 상한은 5분이다(실측 최대 4.2초).
3. **호스트 명령.** `start-scheduled-job.sh` 는 `<job> check` 를 받으면 그 작업의 `check_service` 를 시작한다.
   이 확인은 차례 지키기(takes_turns)로 미뤄지지 않고, 작업의 시작 기록에도 남지 않는다. journal 을 흘려보내지 않으며,
   systemd 가 성공이라 한 확인의 마지막 `foundation-job-check` 줄만 되풀이한다. 릴리스 빌드 중이면 확인을 시작하지 않고
   답 없이 끝난다. 그러면 실행이 평소처럼 미뤄지고 기록된다. `check_service` 가 없는 작업과 `triggered` 가 붙은
   확인은 거부한다.
4. **DAG.** `check_service` 가 있는 작업의 DAG 는 `check >> run` 이다. `check` 는 Airflow 의 `default_pool` 에서
   1칸으로 돌고, 재시도하지 않으며, 데이터 카탈로그에 따로 보고하지 않는다. 답이 `unchanged` 면 `check` 가
   skip 되고, 그 뒤의 `run`·`publish_outputs` 도 skip 된다. 그래서 할 일 없는 실행은 spark 풀을 기다리지 않는다.
   그 밖의 답·답 없음·확인 실패는 모두 `run` 을 시작한다. `run` 은 지금처럼 스스로 다시 판단한다. 그러므로 확인은
   기다림만 줄이고 결과는 바꾸지 않는다. 확인과 실행 사이에 판이 바뀌어도 실행의 판단이 맞다.
5. **권한과 승인.** `foundation-release.sh timers` 는 scheduler 계정에 작업의 서비스와 같이 `check_service` 의
   `systemctl start --no-block`·`stop` 만 허락한다. `job_units.py services` 가 확인 서비스도 내놓으므로, 릴리스 승인
   drop-in 이 걸리고 배포가 그 실행을 기다린다.

## Consequences

- 할 일 없는 레인은 정해진 시각(07:00)에 몇 초 만에 끝난다. spark 풀 대기열에는 할 일이 있는 작업만 남는다.
  `building_register_floor`·Gold 재생성·by-PNU 굽기 앞에 3칸짜리 진입자가 매일 12개 줄어든다.
- 데이터 카탈로그(foundation-api)는 DAG 실행 단위로 결과를 읽는다. 확인으로 skip 된 날은 성공으로 보이며, 실행이
  시작한 적이 없으므로 시작 시각이 비어 있다. 대기나 지연으로 잡히지 않는다.
- 풀 기아 상한(`longest_wait_minutes`)은 그대로다. 레인은 할 일이 있으면 여전히 3칸을 잡으므로 상한 계산에서 빼지 않는다.
- `AIRFLOW__CORE__PARALLELISM=4` 라서 07:00 에 확인 12개가 몇 초씩 줄을 선다. 확인 하나가 몇 초이므로 전체가 1분을
  넘지 않는다.
- 운영자의 `by-pnu-pack-operator.sh silver plan` 은 지금처럼 `systemd-run` 으로 같은 plan 을 돌린다. 그 실행 항목
  `silver-refresh-plan` 과 새 유닛 항목은 같은 환경 묶음 세 개를 갖는다. 운영자 명령을 이 유닛으로 옮기면 하나가 된다.
  후속 작업이다.
- 다른 3칸 작업(Gold 재생성, 경계 발행)도 같은 칸으로 확인을 붙일 수 있다. 유닛 하나와 `check_service` 한 줄이면 된다.
- **완료 기준.** 배포 뒤 다음 07:00 의 `silver_refresh_*` 실행에서 할 일 없는 레인의 풀 대기(task_instance 의
  `queued_dttm`→`start_date`)가 0 에 가깝고, `run` 이 skip 된다.

## References

- Airflow 3.3.2 [Pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html),
  [Deferrable Operators](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html),
  [Sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html),
  [Event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html)
- [AIP-17 Smart Sensor](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/103093484/AIP-17+Consolidate+and+de-duplicate+sensor+tasks+in+airflow+Smart+Sensor)
- [Dagster Sensors](https://dagster.io/docs/guides/automate/sensors)
- [Astronomer: Deferrable operators](https://astronomer.io/docs/learn/deferrable-operators)

---

2026-10-28 개정 주: §3 의 "작업의 시작 기록에도 남지 않는다"는 [ADR-0304](./0304-a-check-that-found-no-work-is-that-jobs-turn.md) 가 고쳤다. `unchanged` 라고 답한 확인은 그 작업의 차례로 적힌다.
