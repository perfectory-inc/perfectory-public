# ADR 0565: Dagster 가 결정하는 작업은 Airflow DAG 로 닿던 모든 것이 Dagster 로 닿는다 — 경로마다 장치 하나, 단계는 그것이 다 선 뒤에 연다

- Status: Accepted
- Date: 2026-11-14
- Builds on: [ADR-0431](./0431-scheduled-jobs-move-from-airflow-to-dagster-one-family-at-a-time-after-a-dual-run-in-which-both-decide.md)
  (§1 의 단계 표, §3 결정 규칙, §5 "단계는 조각마다 구현된다", 2026-11-09 각주가 남긴 목록),
  [ADR-0458](./0458-a-job-moves-from-airflow-to-dagster-on-agreeing-decisions-of-every-kind-it-makes-not-on-calendar-days.md)(옮기는 문턱),
  [ADR-0417](./0417-dagster-runs-a-job-as-its-systemd-unit-through-the-forced-command-and-reads-the-journal-as-a-pipes-session.md)(강제 명령),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md) §7(자산 조정자),
  ADR-0462(시작 큐), [ADR-0308](./0308-the-deploy-stops-only-the-jobs-its-release-holds-and-skips-a-step-whose-inputs-did-not-change.md)(배포가 쥐는 작업),
  ADR-0337(멈춘 동안 놓친 입력), ADR-0282(운영자 스위치), ADR-0277(check), ADR-0118 §2(계보)
- Amends: ADR-0431 §7 과 ADR-0458 §2 의 가족 2 — 자산 조정자는 가족 2 와 함께 옮기지 않는다(§4). 나머지는 ADR-0431 §5 가 남긴
  `dagster` 조각의 설계이고, 이 ADR 의 PR 이 첫 조각이다.

## Context

2026-11-13 운영에서 잰 값(W82, 읽기 전용): 켜진 작업 95개 전부를 Airflow 가 결정하고 실행한다(`shadow` 71, 비교 없음 24, `proxied` 0,
`dagster` 0). 증거(ADR-0458)는 모이고 있지만 Airflow 를 끌 수 없는 이유는 증거가 아니다. `job_specs.py` 가 `dagster` 단계를 거부한다:
Dagster 가 결정하고(`decide.py`) Airflow 가 DAG 를 만들지 않는 것까지는 있는데, 작업에 닿는 다른 길이 전부 Airflow DAG 를 지난다.

전 트리를 조사한 결과(범위: `platforms/foundation-platform` 의 `orchestration/`·`scripts/`·`services/`·`crates/`·`infra/`·`compose*.yml`,
`consoles/dawneer`, 루트 `scripts/`; `git grep -il airflow` 209개 파일 중 ADR 밖 141개):

| 작업에 닿는 것 | 오늘 Airflow DAG 로 하는 일 | 어디 |
|---|---|---|
| 실행 과제 둘레 | `check` 과제(풀을 잡기 전에 할 일이 있는지), 실패한 `run` 의 재시도(`retries`, 5분 뒤) | `orchestration/dags/foundation_jobs.py` |
| 자산 조정자 | 조정자의 run 이 "빚진 run" 을 말하면 DAG 의 `select`·`start` 과제가 그 작업의 DAG 를 띄운다(하루 69번) | 같은 파일, `scripts/ops/asset_reconcile.py` |
| 배포 | 0단계 `dags pause`, 2b·7단계 `dags unpause`, 멈춘 작업·미룬 작업의 `dags trigger`, 멈춘 동안 놓친 입력 사건 묻기 | `foundation-deploy.sh`, `deploy-start-once.sh`, `owed_by_inputs.py` |
| 운영자 run-now | `dags trigger --conf {"operator_request": id}` | `scripts/ops/run-now-dispatch.sh` |
| run 계보 | Airflow 의 OpenLineage 공급자가 `run` 과제의 시작·끝과 입출력을 DataHub 로 보낸다 | `compose.orchestration.yml`, `foundation_jobs.py` 의 `inlets`·`outlets` |
| 운영 지도 | 진행 중인 run(`GET /api/v2/dags/~/dagRuns`)과 실패한 run 의 로그(`.../taskInstances/run/logs/N`) | `foundation-api` `data_catalog/facts.rs` |
| 운영 지도 | run 이력은 DataHub 에서 `flowId == dag id` 인 흐름과 그 자식 `<dag id>.run` 으로 읽는다 | `foundation-api` `data_catalog/operations.rs` |
| 운영 지도 화면 | 실패 run 링크 `<airflow>/dags/<dag id>/runs/<run>/tasks/run`, API 필드 `dag_id`·`scheduler_run_id`·`scheduler_url` | `consoles/dawneer` `dataFlow.ts`·`FlowDetails.tsx`, `operations.rs` |
| Airflow 기동 | 배포 5단계의 `wait_for_declared_dags`(작업마다 DAG 를 기다림), `sync_enabled`(작업마다 pause/unpause), `trigger` | `scripts/deploy/airflow-runtime.sh` |
| 호스트 Docker 이전 | 긴 복사 동안 DAG 를 멈추고 되돌림 | `scripts/ops/host-docker-data-root.sh`, `host_docker_layout.py` |
| 경보·런북 | 경보 문구가 Airflow 과제·스케줄러 로그를 가리킴, `FoundationDagsterCodeLocationDown` 은 "비교가 멈춘다" 는 뜻; 런북의 `airflow dags list-runs\|test\|pause` | `foundation-api.rules.yml`, `production-orchestrator-cutover.md`·`map-edit-fold.md`·`lakehouse-compute-engines.md` |

이미 있는 것(다시 만들지 않는다): 작업의 단계 필드(`dagster_stage`), 호스트의 강제 명령과 그 run 종류(`triggered`·`requested <id>`),
Dagster run 의 태그(`job_specs.OUTCOME_TAG`·`OWED_TAG`·`RUN_KIND_TAG`), 작업 목록이 이름 대는 Dagster 주소(`observers.dagster.ui`, 조정자의 지표가
이미 GraphQL 로 묻는다), 작업마다의 센서(`foundation_decide__<id>`)와 작업마다의 Dagster 작업(`foundation_job__<id>`).

또 하나 잰 사실: 배포는 매번 Dagster 의 두 컨테이너를 재시작한다(`airflow-runtime.sh` `sync_dagster`: `compose up -d` 뒤 `compose restart`,
코드 위치가 릴리스 트리에서 로드되기 때문). run 은 데몬 컨테이너의 자식 프로세스다(ADR-0431 2026-11-09 각주). 24일 동안 배포는 197번이었다(W83).
Airflow 가 결정하는 동안은 `run` 과제의 재시도가 같은 유닛 run 에 다시 붙지만(ADR-0270 §3), Dagster 가 결정하면 그 재시도가 없다.

대기업·오픈소스가 하는 방식:
- **dagster-airlift 이행 안내의 마지막 단계(decommission)**: 프록시한 과제가 다 넘어간 DAG 는 Dagster 의 스케줄·센서가 결정하고 DAG 를 지운다.
  Airflow 에 남은 것이 Dagster 의 것을 시작해야 하면 Dagster 의 API 로 띄운다(그 반대가 아니다).
- **Dagster OSS 배포 구조(공식 문서 "Open source deployment architecture")**: 코드 위치는 웹서버·데몬과 따로 도는 gRPC 서버이고, 코드를 바꾸는
  배포는 그 서버만 바꾼다. 죽은 run 은 run monitoring 이 찾아 실패로 닫고 run retries 가 다시 띄운다.
- **Kubernetes 컨트롤러의 일시정지(`spec.suspend`)**: 멈춤은 컨트롤러의 상태 하나이고, 풀릴 때 무엇을 따라잡을지는 컨트롤러의 규칙이 정한다
  (CronJob `startingDeadlineSeconds`). 멈춘 쪽이 놓친 것을 밖에서 다시 세지 않는다.
- **OpenLineage**: 계보의 정체는 오케스트레이터가 아니라 job 의 namespace 와 name 이다. 실행기를 바꿔도 같은 이름으로 보내면 같은 이력이다.

## Decision

1. **단계는 아래 경로가 모두 선 뒤에 연다.** `job_specs.IMPLEMENTED_DAGSTER_STAGES` 에 `dagster` 를 더하는 것은 §2–§9 와 §11 이 모두 병합된 PR
   하나다. 그 전에는 어느 작업도 `dagster` 일 수 없다(지금과 같다). 조각 1(이 ADR 의 PR)은 §2–§4, §5 의 센서 경로와 덮기 규칙, §6, 그리고
   Airflow 기동이 `dagster` 작업을 기다리지 않는 것(`airflow-runtime.sh`)을 구현한다. §5 의 호스트 관문, §7–§9, §11 은 각자의 조각이다.
   조각마다 운영 영향은 0 이다: `dagster` 작업이 없으면 새 경로는 불리지 않는다.

2. **어느 쪽으로 닿는지는 작업의 `dagster_stage` 하나가 정한다.** 새 스위치·환경변수·목록을 두지 않는다. Dagster 쪽 이름은 `job_specs.py` 한 곳에
   있다(`DAGSTER_JOB_PREFIX`, `DAGSTER_SENSOR_PREFIX`, `DAGSTER_LOCATION`, `DAGSTER_REPOSITORY`, run 태그는 `run_tags()`): Airflow 의 프록시 과제,
   코드 위치, 호스트의 배포·run-now 가 같은 것을 읽는다.

3. **Airflow 과제가 하던 일은 그 작업의 Dagster op 이 한다**(`orchestration/dagster/run_jobs.py`, `dagster` 작업만; `proxied` 작업은 여전히 Airflow 가 한다).
   - `check_service` 가 있는 작업은 op 이 먼저 강제 명령 `<작업> check` 를 부르고, `unchanged` 면 유닛을 시작하지 않고 run 을 `unchanged` 로 끝낸다.
     다른 답, 답 없음, 실패는 run 이 스스로 정한다(ADR-0277 그대로). check 는 run 큐의 풀 자리를 몇 초 쥔다(가장 느린 레인 4.2초):
     Airflow 에서는 풀 밖에서 돌았다. 호스트의 `pool-full` 이 실제 자리를 정하므로 큐의 자리는 순서일 뿐이다.
   - 실패한 run 은 op 의 재시도 정책으로 다시 돈다: 작업의 `retries` 번, `RETRY_DELAY_MINUTES` 뒤. 다음 시도는 아직 도는 유닛에 붙는다.
     재시도를 기다리는 5분 동안 run 은 `STARTED` 라 run 큐의 자리(작업 run 5, spark 3)를 쥔다. Airflow 는 그동안 풀 자리를 놓았다. 값: 작업당
     최대 `retries`(0–3) × 5분이고, 실제 자리는 호스트의 `pool-full` 이 정하므로 다른 작업의 유닛이 못 도는 일은 없다(큐에서 늦게 뜰 뿐).
     재시도는 이 하나뿐이다: §9 의 run 재시도는 op 실패가 아닌 run 의 죽음(데몬·코드 서버)만 다시 띄운다(`run_retries.retry_on_asset_or_op_failure: false`).
   - 배포가 쥔 작업의 재시도도 관문(§5)을 지난다: 쥐는 동안의 시도는 `waiting` 으로 끝나고 빚으로 남는다.

4. **조정자가 말한 빚은 그 작업 자신의 센서가 읽는다.** 조정자의 Dagster run 은 빚진 run 을 태그로 남긴다(`OWED_TAG`, `proxied` 부터 있다). `dagster`
   작업의 센서는 지난 평가 뒤에 끝난 조정자 run 의 태그에서 자기 것을 읽어 run 하나를 시작한다(run key `reconcile:<key>`: 같은 key 는 한 번).
   열린 run 이 있거나 틱·입력 사건이 지금 run 을 시작하면 그 run 이 빚을 덮는다(Airflow `select` 가 진행 중인 run 을 거른 것과 같다).
   센서가 멈춰 있으면(배포가 쥔 작업) 빚도 시작하지 않는다. 그래서 일시정지가 따로 장치 없이 빚에도 미친다.
   - Airflow 쪽 `select` 는 `dagster` 작업을 건너뛴다(없는 DAG 를 띄우지 않는다). 시작 큐의 다시 시작(ADR-0462)은 이제 `dagster` 작업도 포함한다
     (전에는 "띄울 DAG 가 없다"며 목록에서 뺐다 — 옮긴 작업의 거부된 시작이 버려질 뻔했다).
   - **순서**(`job_specs.dagster_order` 가 거부): 조정자가 시작하는 작업이 `dagster` 면 조정자는 `proxied` 이상이어야 하고(그래야 읽을 Dagster run 이
     있다), 조정자가 `dagster` 면 그것이 시작하는 작업이 전부 `dagster` 여야 한다(DAG 없는 조정자는 Airflow 의 DAG 를 띄울 수 없다). 조정자는
     자기가 시작하는 첫 작업보다 먼저 `proxied` 가 되고 마지막 작업보다 뒤에 `dagster` 가 된다. 그래서 ADR-0431 §7·ADR-0458 §2 의 가족 2 는 조정자
     없이 넷이 `dagster` 로 가고, 조정자는 `proxied` 에 남아 spark·documents 풀의 마지막 작업 뒤에 간다. 조정자는 `started_by: schedule` 이라 순서가
     서로를 기다리는 일은 없다.
   - **놓친 틱의 규칙은 하나다.** 조정자의 "놓친 시작" 따라잡기(`asset_reconcile.missed_starts`, ADR-0467)는 `dagster` 작업을 건너뛴다: 그 작업의 놓친
     틱은 센서가 정한다(다음 틱 전에 끝날 때만 늦게, ADR-0431 2026-11-09 각주). 둘 다 두면 센서가 돌리지 않기로 한 틱을 15분 뒤 조정자가 시작한다.

5. **배포는 `dagster` 작업을 센서로 결정에서 빼고, 호스트의 관문으로 시작에서 뺀다**(`scripts/deploy/job_orchestrator.py`, 호스트 루프백의 Dagster GraphQL).
   - **둘은 다른 일이다.** 센서 정지는 새 결정을 멈출 뿐이다. 이미 큐에 든 run, op 의 재시도(§3), run-now, Dagster UI 의 launch 는 센서를 지나지
     않는다. 그래서 쥔 작업의 시작은 모든 시작이 지나는 곳, 호스트의 강제 명령(`start-scheduled-job.sh`)이 막는다: 배포가 쓰는 쥠 파일
     (`/run/foundation-deploy-hold`, 배포의 pid 와 쥔 작업 또는 `*`)에 이름이 있고 그 배포가 살아 있으면 `foundation-job-outcome waiting held by the deploy`
     로 끝내고, 그 시작은 미룬 run 의 기록(ADR-0260 `deferred`)에 빚으로 남아 7단계가 시작한다. 7단계의 시작은 쥠 파일을 지운 뒤에 한다.
     이 관문은 Airflow 가 결정하는 작업에도 같다(일시정지된 DAG 의 재시도도 막힌다). 관문은 조각 4 이고 단계를 여는 PR 보다 먼저다.
   - **덮기 규칙.** run 은 누가 띄웠든(센서, 배포, run-now, `proxied` 이던 때의 Airflow) 그것이 시작하기 전의 틱과 입력 사건을 덮는다(`decide.py`):
     멈췄던 센서가 다시 켜져도, 작업이 `proxied` 로 내려갔다 몇 주 뒤 돌아와도, 그 사이 다른 run 이 한 일을 다시 하지 않는다(2026-11-05 사고의 부류).
     입력으로 시작하는 작업의 센서가 처음 볼 때는 작업의 마지막 run 보다 새 입력 사건을 빚으로 잡아 run 하나를 시작한다: `dagster` 로 옮기는 배포는
     그 DAG 를 멈추고, Airflow 는 멈춘 DAG 의 사건을 버린다(ADR-0337 의 부류).
   - 쥐기 = 그 작업의 센서 정지(`stopSensor`), 놓기 = 정지된 센서 시작(`startSensor`), 빚진 run 시작 = 그 작업의 Dagster 작업을 run 종류
     `triggered` 로 띄우기(`launchRun`). 배포 스크립트는 작업마다 단계를 보고 `airflow dags …` 또는 이것을 부른다(`deploy-start-once.sh`
     `deploy_pause_jobs`·`deploy_unpause_jobs`·`deploy_start_job`). 무엇을 쥘지(ADR-0308), 도는 run 을 멈출지(ADR-0301)는 그대로다.
   - 멈춘 동안 놓친 입력 사건을 배포가 묻지 않는다(`owed_by_inputs` 는 `dagster` 작업을 뺀다): 센서의 커서가 사건을 쥐고 있다가 풀릴 때 run 하나를
     시작한다(ADR-0431 2026-11-09 각주). ADR-0337 은 Airflow 가 사건을 버려서 생긴 장치다.
   - 놓을 때 센서가 Dagster 에 없으면(코드 위치가 그 릴리스의 작업 목록을 못 읽었다) 있는 센서는 다 켜고 배포는 실패한다. 지금까지 "Dagster 가 안
     뜨면 경고" 였던 것은 Dagster 가 아무것도 결정하지 않아서였다. 7단계는 Dagster 가 그 릴리스의 코드 위치를 로드했다고 답할 때까지 기다린 뒤(§9 의
     조각) 센서를 켜고, Airflow 쪽 시작(미룬 run, 놓친 입력)은 Dagster 의 답과 상관없이 한다.
   - 쥘 때 Dagster 가 답하지 않으면 배포는 경고하고 계속한다: 시작은 호스트의 관문이 막으므로 쥠은 Dagster 에 기대지 않는다. 그래서 코드 위치를 깨는
     릴리스(2026-11-06..09, #1204)도 다음 배포로 고칠 수 있다(관문과 함께, 조각 4).
   - 손으로 되돌리는 명령은 `job_orchestrator.py unpause <jobs.v1.json> <작업>…` 이고, 배포의 대문자 메시지와 런북이 그것을 이름 댄다.

6. **운영자의 run-now 는 같은 요청으로 Dagster 작업을 띄운다**(`run-now-dispatch.sh` → `job_orchestrator.py start <작업> requested <id>`). 요청의 확인과
   1회 사용은 그대로 호스트의 관문이 한다(ADR-0282 §4): run 종류 태그가 강제 명령까지 간다. 꺼진 작업의 경로는 바뀌지 않는다.

7. **계보는 같은 이름으로, run 을 돌리는 op 이 보낸다**(조각 2). `dagster` 작업의 op 은 OpenLineage 표준 클라이언트(`openlineage-python`)로 지금 Airflow 가
   보내는 것과 같은 namespace(`perfectory`)·job 이름(`foundation_<id>` 와 `foundation_<id>.run`)·입출력 데이터셋을 DataHub 의 같은 끝점으로 보낸다.
   DataHub 에서 작업의 run 이력은 한 줄로 이어지고 `operations.rs` 는 바뀌지 않는다. 조각 2 는 CI 의 DataHub 에 Airflow 가 보낸 흐름과 op 이 보낸
   흐름이 같은 `flowId` 의 같은 자식으로 붙는지를 시험으로 고정한 뒤에 병합한다(붙지 않으면 그 조각이 읽는 쪽을 함께 고친다).

8. **운영 지도의 진행 중·실패 로그는 작업의 단계에 따라 Dagster 에서 읽는다**(조각 3, `foundation-api` `facts.rs`). `dagster` 작업의 진행 중인 run 은
   Dagster 의 run 목록(작업 이름·열린 상태)에서, 실패한 run 의 줄은 그 run 의 이벤트 로그에서 읽는다(op 이 유닛의 저널을 줄마다 남긴다). 원인 분류
   (`classify`)와 가리기(`mask`)는 줄을 받는 같은 함수다. `proxied` 이하는 지금처럼 Airflow 다. `foundation-api` 는 쓰기 mutation 이 없는 읽기 전용
   웹서버(`dagster-webserver --read-only`)에 닿는다: 운영 쪽 웹서버는 센서·run 을 바꿀 수 있어 API 에 주지 않는다. `scheduler_run_id`·`scheduler_url`
   은 Dagster run id 와 그 run 의 주소가 되고 Dawneer 의 링크 문구는 "실패 로그" 로 바뀐다.

9. **배포가 끊은 Dagster run 은 닫히고 다시 떠서 도는 유닛에 붙는다**(조각 4, 단계를 여는 PR 보다 먼저). 기본 run 실행기에서 run 은 코드 위치를
   띄운 프로세스의 자식이고, 코드 위치는 모든 릴리스가 바꾼다(197 배포/24일). 그래서 run 이 배포를 넘어 살게 만들려면 run 마다 컨테이너가 필요하다
   (`DockerRunLauncher`, 이미지·네트워크·호스트 키 전달이 새 표면). 그 대신 run 은 배포에 끊겨도 잃는 것이 없게 한다: 유닛은 systemd 의 것이라
   계속 돌고(ADR-0270 §3), 다시 뜬 run 은 강제 명령이 그 유닛에 붙여 결과를 읽는다. 조각 4 는 1.13.27 위에서 (a) 데몬 재시작이 끊은 run 을 run
   monitoring 이 실제로 실패로 닫는지(기본 실행기가 작업자 상태 확인을 지원하지 않으면 `max_runtime_seconds` 와 시작 시간 상한으로 닫는다), (b)
   `run_retries`(`max_retries: 1`, `retry_on_asset_or_op_failure: false`)가 그 run 을 같은 태그로 다시 띄우는지 시험으로 고정하고, (c) 작업의 run 한도보다
   오래 열린 Dagster run 을 경보한다(닫히지 않은 run 은 센서가 영원히 "열린 run" 으로 보고 작업을 다시 시작하지 않는다).

10. **Airflow 는 켜진 작업이 하나도 남지 않은 릴리스에서 지운다**(ADR-0431 §7 의 5단계, ADR-0458 §5): DAG 생성(`orchestration/dags/foundation_jobs.py`),
    Airflow 이미지와 compose 서비스 여섯, 배포 5단계와 `airflow-runtime.sh` 의 Airflow 부분, 이중 실행(`dual_run.py`·검사·경로), `job_orchestrator.py`
    와 배포 스크립트의 Airflow 갈래. Dagster 의 데이터베이스는 그 전에 `airflow-db` 에서 자기 것으로 옮긴다. 꺼진 작업은 DAG 없이 꺼져 있다.

11. **결정하는 쪽의 경보와 런북은 단계를 여는 PR 보다 먼저다**(조각 5). 센서가 배포 밖에서 멈춰 있거나 센서 틱이 실패하면 경보한다(Dagster 의
    `instigationStates`·틱 상태를 조정자의 지표가 이미 묻는 GraphQL 로). `FoundationDagsterCodeLocationDown` 은 `dagster` 작업이 있으면 "아무것도
    결정되지 않는다" 로 뜻이 바뀐다. Airflow 를 가리키는 경보 문구·런북 명령은 작업의 단계에 맞는 명령을 같이 적는다. 호스트 Docker 이전
    (`host-docker-data-root.sh`)은 `dagster` 작업을 `job_orchestrator.py pause`/`unpause` 로 쥔다.

## Consequences

- **조각 1(이 PR)의 운영 영향은 0 이다.** `dagster` 작업이 없어 `job_orchestrator.py` 는 불리지 않고, 센서·check·재시도 갈래도 돌지 않는다. 바뀌는
  것은 거부 규칙 둘(§4 순서)과 `requeued` 목록의 정의뿐이고, 오늘의 작업 목록에서 둘 다 결과가 같다.
- 배포 스크립트는 한 배포 늦게 선다: 배포는 자기가 시작한 control 체크아웃의 스크립트로 끝까지 돈다. §1 이 단계를 따로 여는 이유 하나다.
- Dagster 웹서버는 이제 배포와 run-now 의 길이다. 루프백에만 열려 있고(`127.0.0.1:19085`) 계정이 없다: 호스트의 누구나 작업을 띄울 수 있다는 점은
  `proxied` 에서 이미 같고, 무엇이 실제로 도는지는 여전히 강제 명령과 유닛이 정한다.
- 조정자는 가장 오래 Airflow 에 남는다(§4 순서: spark 풀의 모든 작업보다 뒤). Airflow 를 끄는 마지막 PR 이 조정자를 옮긴다.
- run-now 가 run 이 도는 동안 오면 도는 유닛에 붙는다(Airflow 에서는 끝난 뒤 새 run 이 하나 더 돌았다). 요청은 그 run 으로 쓰인다.
- §10 에서 Dagster 의 데이터베이스를 옮길 때 센서의 커서·run key·정지 상태가 함께 옮겨진다(run key 가 같은 결정을 두 번 시작하지 않게 하는 근거).
- 설계 검토(EGM 디자인 비평, 2026-11-14): 차단 3·주요 8 중 B3(Airflow 기동이 `dagster` 작업의 DAG 를 기다림)·M1(놓친 틱 규칙 둘)·M2(되돌렸다
  다시 올릴 때 지난 것을 다시 함)·M3(옮기는 배포의 입력 사건 유실)과 사소 2 는 조각 1 의 코드로, B1·B2·M4–M8 은 위 §3·§4·§5·§8·§9·§11 의 결정으로 반영했다.
- 시험: 기본 CI — `test_job_specs.py`(순서 거부, requeue, owed_by_inputs), `test_job_dags.py`(`select` 가 건너뜀), `test_foundation_autodeploy.py`
  (쥐기·놓기·빚진 run·실패), `test_run_now_dispatch.py`, `test_dagster_job_orchestrator.py`(대역 GraphQL). CI `dagster-observe`(코드 위치 이미지) —
  같은 요청을 실제 Dagster 1.13.27 인스턴스와 코드 위치에 대고(센서 정지·시작, run 시작과 태그), 센서가 조정자 run 의 태그에서 빚을 읽는지,
  check 가 유닛을 시작하지 않는지, 재시도 정책.
- 비용 0원. 메모리: 센서의 빚 조회는 조정자가 시작하는 작업마다 30초에 색인 조회 하나다(60개면 초당 2번).

## 기각한 대안

- **Dagster 가 Airflow 의 DAG 를 띄우거나 Airflow 가 Dagster 의 센서를 대신하는 다리**: 두 오케스트레이터가 서로를 시작하는 길이 둘이 된다.
  순서 규칙(§4) 하나면 다리가 필요 없다.
- **조정자마다 "빚 센서" 하나가 모든 작업을 시작**: 작업의 일시정지(그 작업의 센서 정지)가 빚에는 미치지 않아, 배포가 쥔 작업을 빚이 시작한다.
- **배포가 Dagster CLI 를 데몬 컨테이너 안에서 실행**(`dagster sensor stop`): 호출마다 코드 위치를 통째로 로드한다(169 MiB, 데몬의 상한 안에서).
  GraphQL 은 조정자의 지표가 이미 쓰는 길이다.
- **DataHub 의 Dagster 플러그인(`acryl-datahub-dagster-plugin`)이나 `openlineage-dagster` 로 계보**: 흐름의 정체가 Dagster 의 작업 이름으로 바뀌어
  작업마다 이력이 둘로 끊기고, `operations.rs` 가 두 이름을 이어야 한다. 플러그인은 Dagster 버전에 묶인다(잠금 파일의 1.13.27 과 따로 움직인다).
- **단계를 조각마다 반쯤 열기**(예: 계보 없이 `dagster` 허용): 옮긴 작업이 운영 지도에서 사라진다. §1.
