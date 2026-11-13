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
- 이 ADR 은 ADR-0431 의 결정을 바꾸지 않는다. §5 가 남긴 `dagster` 조각의 설계를 정하고 첫 조각을 구현한다.

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

1. **단계는 아래 경로가 모두 선 뒤에 연다.** `job_specs.IMPLEMENTED_DAGSTER_STAGES` 에 `dagster` 를 더하는 것은 §2–§9 가 모두 병합된 PR 하나다.
   그 전에는 어느 작업도 `dagster` 일 수 없다(지금과 같다). 이 ADR 의 PR(조각 1)은 §2–§6 을 구현하고, §7–§9 는 각자의 조각이다.
   조각마다 운영 영향은 0 이다: `dagster` 작업이 없으면 새 경로는 불리지 않는다.

2. **어느 쪽으로 닿는지는 작업의 `dagster_stage` 하나가 정한다.** 새 스위치·환경변수·목록을 두지 않는다. Dagster 쪽 이름은 `job_specs.py` 한 곳에
   있다(`DAGSTER_JOB_PREFIX`, `DAGSTER_SENSOR_PREFIX`, `DAGSTER_LOCATION`, `DAGSTER_REPOSITORY`, run 태그는 `run_tags()`): Airflow 의 프록시 과제,
   코드 위치, 호스트의 배포·run-now 가 같은 것을 읽는다.

3. **Airflow 과제가 하던 일은 그 작업의 Dagster op 이 한다**(`orchestration/dagster/run_jobs.py`, `dagster` 작업만; `proxied` 작업은 여전히 Airflow 가 한다).
   - `check_service` 가 있는 작업은 op 이 먼저 강제 명령 `<작업> check` 를 부르고, `unchanged` 면 유닛을 시작하지 않고 run 을 `unchanged` 로 끝낸다.
     다른 답, 답 없음, 실패는 run 이 스스로 정한다(ADR-0277 그대로). check 는 run 큐의 풀 자리를 몇 초 쥔다(가장 느린 레인 4.2초):
     Airflow 에서는 풀 밖에서 돌았다. 호스트의 `pool-full` 이 실제 자리를 정하므로 큐의 자리는 순서일 뿐이다.
   - 실패한 run 은 op 의 재시도 정책으로 다시 돈다: 작업의 `retries` 번, `RETRY_DELAY_MINUTES` 뒤. 다음 시도는 아직 도는 유닛에 붙는다.

4. **조정자가 말한 빚은 그 작업 자신의 센서가 읽는다.** 조정자의 Dagster run 은 빚진 run 을 태그로 남긴다(`OWED_TAG`, `proxied` 부터 있다). `dagster`
   작업의 센서는 지난 평가 뒤에 끝난 조정자 run 의 태그에서 자기 것을 읽어 run 하나를 시작한다(run key `reconcile:<key>`: 같은 key 는 한 번).
   열린 run 이 있거나 틱·입력 사건이 지금 run 을 시작하면 그 run 이 빚을 덮는다(Airflow `select` 가 진행 중인 run 을 거른 것과 같다).
   센서가 멈춰 있으면(배포가 쥔 작업) 빚도 시작하지 않는다. 그래서 일시정지가 따로 장치 없이 빚에도 미친다.
   - Airflow 쪽 `select` 는 `dagster` 작업을 건너뛴다(없는 DAG 를 띄우지 않는다). 시작 큐의 다시 시작(ADR-0462)은 이제 `dagster` 작업도 포함한다
     (전에는 "띄울 DAG 가 없다"며 목록에서 뺐다 — 옮긴 작업의 거부된 시작이 버려질 뻔했다).
   - **순서**(`job_specs.dagster_order` 가 거부): 조정자가 시작하는 작업이 `dagster` 면 조정자는 `proxied` 이상이어야 하고(그래야 읽을 Dagster run 이
     있다), 조정자가 `dagster` 면 그것이 시작하는 작업이 전부 `dagster` 여야 한다(DAG 없는 조정자는 Airflow 의 DAG 를 띄울 수 없다). 조정자는
     자기가 시작하는 첫 작업보다 먼저 `proxied` 가 되고 마지막 작업보다 뒤에 `dagster` 가 된다.

5. **배포는 `dagster` 작업을 센서로 쥐고 놓는다**(`scripts/deploy/job_orchestrator.py`, 호스트 루프백의 Dagster GraphQL).
   - 쥐기 = 그 작업의 센서 정지(`stopSensor`), 놓기 = 정지된 센서 시작(`startSensor`), 빚진 run 시작 = 그 작업의 Dagster 작업을 run 종류
     `triggered` 로 띄우기(`launchRun`). 배포 스크립트는 작업마다 단계를 보고 `airflow dags …` 또는 이것을 부른다(`deploy-start-once.sh`
     `deploy_pause_jobs`·`deploy_unpause_jobs`·`deploy_start_job`). 무엇을 쥘지(ADR-0308), 도는 run 을 멈출지(ADR-0301)는 그대로다.
   - 멈춘 동안 놓친 입력 사건을 배포가 묻지 않는다(`owed_by_inputs` 는 `dagster` 작업을 뺀다): 센서의 커서가 사건을 쥐고 있다가 풀릴 때 run 하나를
     시작한다(ADR-0431 2026-11-09 각주). ADR-0337 은 Airflow 가 사건을 버려서 생긴 장치다.
   - 놓을 때 센서가 Dagster 에 없으면(코드 위치가 그 릴리스의 작업 목록을 못 읽었다) 배포는 실패한다. 지금까지 "Dagster 가 안 뜨면 경고" 였던 것은
     Dagster 가 아무것도 결정하지 않아서였다.

6. **운영자의 run-now 는 같은 요청으로 Dagster 작업을 띄운다**(`run-now-dispatch.sh` → `job_orchestrator.py start <작업> requested <id>`). 요청의 확인과
   1회 사용은 그대로 호스트의 관문이 한다(ADR-0282 §4): run 종류 태그가 강제 명령까지 간다. 꺼진 작업의 경로는 바뀌지 않는다.

7. **계보는 같은 이름으로, run 을 돌리는 op 이 보낸다**(조각 2). `dagster` 작업의 op 은 OpenLineage 표준 클라이언트(`openlineage-python`)로 지금 Airflow 가
   보내는 것과 같은 namespace(`perfectory`)·job 이름(`foundation_<id>` 와 `foundation_<id>.run`)·입출력 데이터셋을 DataHub 의 같은 끝점으로 보낸다.
   DataHub 에서 작업의 run 이력은 한 줄로 이어지고 `operations.rs` 는 바뀌지 않는다. 조각 2 는 CI 의 DataHub 에 Airflow 가 보낸 흐름과 op 이 보낸
   흐름이 같은 `flowId` 의 같은 자식으로 붙는지를 시험으로 고정한 뒤에 병합한다(붙지 않으면 그 조각이 읽는 쪽을 함께 고친다).

8. **운영 지도의 진행 중·실패 로그는 작업의 단계에 따라 Dagster 에서 읽는다**(조각 3, `foundation-api` `facts.rs`). `dagster` 작업의 진행 중인 run 은
   Dagster 의 run 목록(작업 이름·열린 상태)에서, 실패한 run 의 줄은 그 run 의 이벤트 로그에서 읽는다(op 이 유닛의 저널을 줄마다 남긴다). 원인 분류
   (`classify`)와 가리기(`mask`)는 줄을 받는 같은 함수다. `proxied` 이하는 지금처럼 Airflow 다.

9. **배포는 도는 Dagster run 을 끊지 않는다**(조각 4, 단계를 여는 PR 보다 먼저). 코드 위치를 자기 컨테이너의 gRPC 서버로 내고, 배포는 릴리스가
   바뀔 때 그 컨테이너만 바꾼다. 데몬과 웹서버는 자기 이미지나 설정이 바뀐 릴리스에서만 다시 뜬다. 그래도 죽은 run(데몬 재시작·OOM)은 run
   monitoring 이 닫고 run retries 가 한 번 다시 띄운다: 유닛은 systemd 의 것이라 계속 돌고(ADR-0270 §3) 다시 뜬 run 이 그 유닛에 붙어 결과를 읽는다.

10. **Airflow 는 켜진 작업이 하나도 남지 않은 릴리스에서 지운다**(ADR-0431 §7 의 5단계, ADR-0458 §5): DAG 생성(`orchestration/dags/foundation_jobs.py`),
    Airflow 이미지와 compose 서비스 여섯, 배포 5단계와 `airflow-runtime.sh` 의 Airflow 부분, 이중 실행(`dual_run.py`·검사·경로), `job_orchestrator.py`
    와 배포 스크립트의 Airflow 갈래. Dagster 의 데이터베이스는 그 전에 `airflow-db` 에서 자기 것으로 옮긴다. 꺼진 작업은 DAG 없이 꺼져 있다.

## Consequences

- **조각 1(이 PR)의 운영 영향은 0 이다.** `dagster` 작업이 없어 `job_orchestrator.py` 는 불리지 않고, 센서·check·재시도 갈래도 돌지 않는다. 바뀌는
  것은 거부 규칙 둘(§4 순서)과 `requeued` 목록의 정의뿐이고, 오늘의 작업 목록에서 둘 다 결과가 같다.
- 배포 스크립트는 한 배포 늦게 선다: 배포는 자기가 시작한 control 체크아웃의 스크립트로 끝까지 돈다. §1 이 단계를 따로 여는 이유 하나다.
- Dagster 웹서버는 이제 배포와 run-now 의 길이다. 루프백에만 열려 있고(`127.0.0.1:19085`) 계정이 없다: 호스트의 누구나 작업을 띄울 수 있다는 점은
  `proxied` 에서 이미 같고, 무엇이 실제로 도는지는 여전히 강제 명령과 유닛이 정한다.
- 조정자는 가장 오래 Airflow 에 남는다(§4 순서: spark 풀의 모든 작업보다 뒤). Airflow 를 끄는 마지막 PR 이 조정자를 옮긴다.
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
