# ADR 0431: 예약 작업은 한 가족씩 Airflow 에서 Dagster 로 옮긴다 — 둘이 함께 결정하고 하나만 실행하는 이중 실행을 거친 뒤

- Status: Accepted (2026-11-06, 소유자 지시 "근본 구조전환까지 전부 알아서 빠르게 진행해")
- Date: 2026-11-06
- Builds on: [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(엔진은 Dagster, 0단계 airlift observe #946),
  ADR-0432(발행 검사, #1067), ADR-0417(강제 명령으로 systemd 유닛을 돌리는 Pipes, #1070),
  [ADR-0415](./0415-every-dataset-group-is-one-declared-definition-and-every-copy-is-generated-from-it.md)·[ADR-0418](./0418-the-artifacts-the-declared-definitions-generate-are-never-committed-and-every-checkout-and-release-generates-them.md)(정의가 정본),
  [ADR-0425](./0425-clock-started-batch-never-runs-into-the-by-pnu-operator-window-and-a-late-start-waits-for-its-next-schedule.md)(운영자 창),
  [ADR-0420](./0420-ci-capacity-is-not-taken-from-the-production-host-and-a-self-hosted-runner-runs-only-trusted-events-on-a-dedicated-machine.md)(운영 호스트에 CI 러너 없음),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(systemd 가 실행기)

## Context

ADR-0333 의 0단계(#946)에서 Dagster 는 Airflow 를 보기만 한다. 발행 검사(ADR-0432)와 실행기(ADR-0417)도 준비됐다. 3단계는
작업의 결정(언제 시작하나)과 실행(무엇이 유닛을 돌리나)을 Dagster 로 옮기는 일이다. 작업 69개 중 켜진 54개는 공유 독자
(`data_quality`·`asset_reconcile`·`lakehouse_maintenance` 등)를 통해 거의 모두 한 덩어리로 이어져 있어(연결 성분 2개: 53 + 1), 한 번에
옮기면 되돌릴 단위가 없다.

두 오케스트레이터의 결정 규칙은 같아 보여도 다르다. 기록된 운영의 하루(2026-10-28, `orchestration/tests/fixtures/airflow-recorded-day.json`)를
Dagster 의 규칙으로 다시 결정하면 두 가지가 바로 드러났다(이 ADR 의 비교기로 잰 값).

| 무엇 | 잰 값 |
|---|---|
| 그날 돈 작업 · 결정 | 34개 · 178개(시계 틱 + 입력 사건) |
| Airflow 가 아무 DAG 도 시작하지 않은 구간 | 15:50–18:04, 19:00–20:47 UTC(배포의 일시정지·스케줄러 정지) — 그 안의 결정 10개 |
| 한 run 이 도는 동안의 틱 | onbid Silver 17:50 run 이 18:05–21:05 를 돌았고, 18:50·19:50 틱은 없이 20:50 이 다음에 돌았다(`max_active_runs=1`, `catchup=False`) |
| 위 두 규칙을 넣은 뒤 다른 결정 | **0 / 168**(정지 구간의 10개는 따로) |

Dagster 의 기본 스케줄은 진행 중인 run 과 상관없이 틱마다 run 을 만든다. 규칙을 옮기지 않고 실행을 옮기면 같은 작업이 겹쳐 돈다.

대기업·오픈소스가 옮기는 방식:
- **dagster-airlift 의 이행 안내**(peer → observe → migrate → decommission): 작업마다 `proxied` 상태를 켜면 Airflow 가 계속 스케줄하고
  실행만 Dagster 가 맡는다. 되돌리기는 상태 하나다.
- **병행 실행(Parallel Run)**: 새 구현이 옛 구현과 같은 입력으로 같이 결정하고, 결과를 비교하되 옛 쪽만 효과를 낸다(GitHub Scientist,
  Sam Newman *Monolith to Microservices* "Parallel Run", Shopify·Stripe 의 이중 쓰기/섀도 읽기).
- **하류 먼저 끊기**: 소비자가 새 시스템의 사건을 받을 수 있게 된 뒤에 생산자를 옮긴다(Strangler Fig).

## Decision

1. **단계는 작업의 한 필드다.** `orchestration/jobs.v1.json` 작업의 `dagster_stage`(정의 파일의 `jobs` 항목, 없으면 `airflow`):

   | 단계 | 누가 결정 | 누가 실행 | Airflow DAG |
   |---|---|---|---|
   | `airflow` | Airflow | Airflow | 그대로 |
   | `shadow` | Airflow, Dagster 가 옆에서 결정·비교 | Airflow | 그대로 |
   | `proxied` | Airflow | Dagster(ADR-0417 의 강제 명령) | 실행 과제가 Dagster 에 위임 |
   | `dagster` | Dagster | Dagster | 만들지 않음 |

   새 스위치를 만들지 않는다: 작업 목록이 이미 두 오케스트레이터가 읽는 정본이다. `job_specs.py` 는 모르는 단계, 아직 구현되지
   않은 단계, 꺼진 작업의 `airflow` 아닌 단계를 거부한다.
2. **이중 실행(`shadow`)은 둘이 결정하고 하나만 실행한다.** 같은 유닛을 두 번 돌리는 이중 실행은 하지 않는다: 쓰기는 추가 전용이고(소유자 원칙: 모든 데이터는 쌓는다),
   한 run 이 Spark 를 몇 시간 쥔다. Dagster 는 Airflow 의 API 에서 그 작업의 모든 상태의 run 을, airlift 가 관찰한 입력 자산의
   물질화를 읽어 자기 규칙으로 결정하고 비교한다(`orchestration/dagster/dual_run.py`, 검사 `dual_run_matches_airflow`, 작업의 DAG 자산 위,
   WARN·막지 않음). 비교 규칙:
   - **시계 틱**: Dagster 자신의 cron 반복(UTC)의 틱 대 Airflow `scheduled` run 의 틱. run 이 도는 동안 온 틱은 그 run 이 끝나기 전 마지막
     틱 하나로 접힌다(`due_ticks`).
   - **입력 사건**: 시작 입력(`starting_inputs`)의 물질화마다 그 뒤 작업의 시간·대기 한도 안에 `asset_triggered` run 이 있다. 이전 run
     뒤로 입력 사건이 없는 `asset_triggered` run 은 Dagster 가 하지 않았을 결정이다.
   - **Airflow 정지**: 아무 DAG 도 시작하지 않은 구간(가장 짧은 시계 주기의 두 배보다 긴)은 결정 규칙이 아니라 가용성이다. 그 안의
     결정은 `airflow_out` 으로 따로 남기고 차이로 세지 않는다. 입력 사건의 마감은 그 구간만큼 밀린다.
   - `manual` run 은 운영자의 것이라 비교하지 않는다.
3. **Dagster 가 결정할 때 지킬 규칙은 비교가 증명한 Airflow 의 규칙이다.** `dagster` 단계의 스케줄은 진행 중인 run 이 있으면 틱을 접고
   (작업당 대기 run 하나, 마지막 틱), 입력 사건은 진행 중인 run 동안 쌓여 다음 run 하나가 된다. 운영자 창(ADR-0425)과 run 당 상한은
   호스트의 강제 명령(`scripts/ops/start-scheduled-job.sh`)이 지키므로 어느 오케스트레이터가 시작해도 같다.
4. **옮기는 문턱은 잰 값이다.** 한 가족은 `shadow` 에서 연속 7일 동안 매일 검사가 통과하고(차이 0, 결정 1개 이상) 그 사이 정지 구간의
   결정을 사람이 확인했을 때 `proxied` 로, `proxied` 에서 7일 동안 run 결과(`foundation-job-outcome`)와 물질화가 Airflow 때와 같은
   분포일 때 `dagster` 로 간다. 되돌리기는 그 가족의 `dagster_stage` 를 한 단계 내리는 PR 하나다.
5. **단계는 조각마다 구현된다.** 이 ADR 의 조각은 `airflow`·`shadow` 를 구현한다. `proxied` 는 airlift 의 프록시 과제(Airflow 이미지의
   `dagster-airlift[in-airflow]`, Airflow → Dagster 웹서버 연결)를, `dagster` 는 §3 의 스케줄과 Airflow DAG 생성 제외를 각자의 조각에서
   더하고, 그 전에는 `job_specs.py` 가 두 단계를 거부한다.
6. **하류 가족이 먼저 `dagster` 로 간다.** Airflow 에 남은 `inputs` 작업은 Airflow 의 자산 사건으로 시작한다. `dagster` 작업의 run 은 그
   사건을 남기지 않으므로, `dagster` 작업의 출력을 입력으로 읽는 `inputs` 작업이 `dagster` 가 아니면 `job_specs.dagster_order` 가 거부한다.
   반대(상류는 Airflow, 하류는 Dagster)는 airlift 가 상류의 물질화를 관찰하므로 된다. `shadow`·`proxied` 는 Airflow 가 계속 사건을
   남기므로 순서 제약이 없다.
7. **가족과 순서.** 가족은 함께 옮기는 작업의 묶음이고, 단계는 작업마다 적혀 있으므로 가족 목록은 이 표가 계획이고 정본은 작업 목록이다.

   | 순서 | 가족 | 작업 | 왜 이 순서 |
   |---|---|---|---|
   | 1 | 단가 | `gold_unit_price_rebuild`, `by_pnu_unit_price_bake` | 작고 이미 두 작업으로 나뉘었고(#940), 검사가 붙어 있다. **이 조각에서 `shadow`** |
   | 2 | 공유 독자 | `data_quality`, `shape_ledger_check`, `asset_reconcile`, `media_reconcile`, `lakehouse_maintenance` | 모든 레인의 하류: §6 에 따라 어느 생산자든 `dagster` 로 가기 전에 먼저 |
   | 3 | 원천 수집·Silver | 수집기와 그 Silver 갱신(공매·경매·실거래·건축물대장 …), 원천 하나씩 | 바깥 API 만 읽고, 하류는 2 가 이미 Dagster |
   | 4 | Gold·by-PNU 레인 | 레인 하나씩(ADR-0333 §1 의 `sido` 파티션과 함께) | 파티션 자산이 이 이행의 목적 |
   | 5 | 나머지 | 경계·타일·지도 편집·outbox | 마지막에 Airflow 해체 |

## Consequences

- 이 조각의 운영 영향은 0 이다: `observers.dagster.enabled` 가 꺼져 있고, 켜도 `shadow` 는 Airflow 의 결정과 실행을 바꾸지 않는다.
  검사는 단가 가족의 Airflow run 마다 한 번, Airflow API 호출 1–3번(하루+여유 구간의 run 수백 개, 쪽당 100)과 이벤트 기록 조회 몇 번이다.
  비용은 0원이고 메모리는 #946 데몬 상한(480m) 안이다.
- 시험(CI `dagster-observe`, 코드 위치 이미지 안): 기록된 하루를 재생해 정지 구간 밖의 결정 168개가 모두 같고 정지 구간이 위 두 구간인지,
  접힘·입력 사건·정지 규칙, 코드 위치에서 검사가 `shadow` 작업의 DAG 자산에 붙는지. `job_specs` 의 단계·순서 거부는 기본 CI 의
  `orchestration/tests/test_job_specs.py` 가 본다. 실물 Airflow 에 대고 돈 비교는 아직 없다: 관찰자가 켜진 뒤 첫 7일이 그 측정이다.
- ADR-0420: 비교는 운영 호스트의 Dagster 데몬 안에서 운영 런타임으로 돌고, CI 는 기록된 하루로만 시험한다. 운영 호스트에 CI 러너를
  두지 않는다.

## 기각한 대안

- **유닛을 두 번 실제로 돌려 결과를 비교**: 추가 전용 쓰기를 두 번 하고, Spark·원천 API 할당을 두 배로 쓴다. 결정이 같고 실행기가
  같은 유닛(ADR-0417)이면 실행 결과의 차이는 결정의 차이에서만 나온다.
- **한 번에 전부 옮기기**: 되돌릴 단위가 없다. 기록된 하루에서만 규칙 차이 두 가지가 나왔다.
- **가족 단계를 정의 파일의 새 최상위 절로**: 한 가족이 정의 둘 이상에 걸친다(단가: `gold-unit-price-rebuild`·`unit-price`). 작업이 단위인
  기존 정본에 필드 하나를 더하는 편이 스위치를 쪼개지 않는다.

## 개정 각주 — 2026-11-07, 운영 8일 재생과 관찰자 켜기

이 각주는 결정을 바꾸지 않는다. §2 의 비교 규칙을 §3 대로 "비교가 증명한 Airflow 의 규칙"으로 채우고, §4 의 문턱이 읽을 증거를 남긴다.

- **운영 8일 재생(잰 값).** 운영 Airflow(2026-10-29..11-05 UTC, 읽기 전용 SELECT: `dag_run`·`asset_event`·`log` 의
  `cli_dag_pause`/`cli_dag_unpause`)를 켜진 작업 54개 모두에 대해 다시 결정했다. 첫 조각의 규칙: 결정 4,313개 중 **차이 200**(작업
  20개, 작업-날 81개). 아래 규칙을 넣은 뒤: 결정 4,074개 중 **차이 0**(작업-날 432개 모두), 정지 구간 1개, 일시정지 268개는 따로.
- **일시정지는 결정이 아니다.** 배포는 하루에도 여러 번 DAG 를 멈추고 푼다(0단계·7단계, 열흘에 9,792번). 멈춘 동안의 틱은
  나오지 않고, 풀릴 때 가장 늦은 틱 하나가 돈다(`catchup=False`). 멈춘 동안의 입력 사건은 run 을 만들지 않는다(ADR-0179, 배포가
  한 번 직접 시작한다 ADR-0337). 상태는 그 전 마지막 사건으로 읽는다: 두 사건 모두 반복되기 때문이다. 사건이 하나도 없으면 멈춘 것이다
  (Airflow 는 DAG 를 멈춘 채로 만든다). 비교는 이것을 `paused` 로 따로 남긴다.
- **스케줄러 지연.** 막힌 구간(진행 중인 run, 운영자의 run 도 포함, 또는 멈춤)이 끝나기 60초 안쪽의 틱은 따라잡는 run 다음에 따로
  돈다(운영에서 4·5·13초, 세 번). 풀린 직후 운영자의 run 이 먼저 자리를 잡으면 틱은 계속 기다린다.
- **`dagster` 단계가 지킬 규칙(§3 에 더함).** 배포가 Dagster 의 작업을 멈추는 방식은 데몬의 스케줄을 멈추는 것이고, 다시 켤 때
  놓친 틱은 하나만 돈다(`schedules.max_catchup_runs: 1` 과 같은 뜻). 이것은 `dagster` 단계 조각에서 구현한다.
- **증거 원장.** 비교는 모두 `/var/lib/dagster/dual-run/comparisons.jsonl`(인스턴스 볼륨 `dagster-state`)에 한 줄씩 덧붙고 고쳐 쓰지
  않는다. `python orchestration/dagster/dual_run.py gate` 가 그것을 §4 의 문턱으로 읽는다: 작업마다 어제까지 연속으로 통과한 날 수
  (그날의 비교가 모두 같고 결정이 하나 이상)와 사람이 확인할 정지·멈춤 구간의 결정. 원장을 못 쓰면 검사 run 이 실패한다.
- **관찰자 켜기.** 이중 실행은 관찰자가 켜져야 증거를 남긴다. `observers.dagster.enabled` 를 켠다(#946 에서 꺼 둔 스위치).

## 개정 각주 — 2026-11-09, §4 대체

§4 의 문턱(연속 7일 통과 + 사람이 정지 구간 결정을 확인)은 [ADR-0458](./0458-a-job-moves-from-airflow-to-dagster-on-agreeing-decisions-of-every-kind-it-makes-not-on-calendar-days.md)
이 대체한다: 작업마다 서로 다른 일치 결정 3개와 그 작업의 모든 결정 종류(매일·매주·매달·사건)의 일치 하나, 차이 하나는 셈을 처음으로
돌리고 호출한다. 이 날 잰 원장은 비어 있었다 — 코드 위치가 2026-11-06 21:16Z 부터 로드되지 않았다(#1204).

## 개정 각주 — 2026-11-09, `proxied` 단계 구현(§5)

이 각주는 결정을 바꾸지 않는다. §5 가 남긴 `proxied` 조각을 채운다. `job_specs.py` 는 이제 `proxied` 를 받고, `dagster` 는 여전히 거부한다.

- **Airflow 이미지.** 릴리스가 `orchestration/airflow/Dockerfile` 로 `apache/airflow:3.3.2`(같은 digest)에 `dagster-airlift[in-airflow]==0.29.27`
  휠 하나를 해시로 얹는다(Apache Airflow 의 운영 권고: 기준 이미지를 확장하고 시작 때 설치하지 않는다). 휠의 의존(click·structlog·requests·PyYAML)은
  Airflow 의 핀 그대로 두고 `pip check` 로 확인한다. 이미지는 릴리스 빌드가 만들고 릴리스 승인이 `foundation-platform-airflow:local` 로 태그한다
  (`OPTIONAL_COMPOSE_IMAGES`: 이 이미지 이전 릴리스는 자기 compose 의 기본 이미지를 쓰므로 되돌리기에 필요 없다). 호스트는 만들지 않는다:
  compose 가 이 이미지를 부르는데 없으면 `airflow-runtime.sh` 가 거부한다. 3.3.2 에서 `airflow.models.BaseOperator` 는
  `airflow.sdk.bases.operator.BaseOperator` 그 자체다(운영 스케줄러에서 확인) — airlift 의 프록시 과제가 Airflow 3 에서 그대로 서는 근거.
- **`run` 과제만 넘긴다.** `proxied` 작업의 DAG·스케줄·풀·`check`·`publish_outputs`·`select` 는 그대로다. `run` 은 airlift 의
  `BaseProxyTaskToDagsterOperator` 를 이은 과제로, Dagster 웹서버에 그 작업의 Dagster 작업 `foundation_job__<id>` 를 이름으로 띄우고 끝을 기다린다
  (10초마다). airlift 의 기본 프록시는 과제에 매핑된 자산을 고르지만, 여기서는 수집기와 그 Silver 갱신이 같은 Silver 표 16개를 쓰고 작업 5개는
  쓰는 데이터셋이 없어 자산 정의 하나로 묶을 수 없다. 그래서 작업마다 op 하나짜리 Dagster 작업이 유닛을 돌리고 출력의 물질화를 기록한다
  (`orchestration/dagster/run_jobs.py`). 출력 자산은 관찰 자산 그대로이고, airlift 는 `proxied` DAG 를 출력에 매핑하지 않는다(물질화가 두 번 되지 않게).
- **답은 run 태그로.** 어떻게 시작됐는지(시계·`triggered`·`requested <id>`, `job_specs.RUN_KIND`)를 Airflow 가 태그로 보내고, Dagster run 은 끝나기 전에
  결과 단어와(조정자는 빚진 run 목록도) 태그로 남긴다. `run` 과제는 그것을 오늘과 같은 XCom 으로 넣는다: `waiting`·`skipped` 는 건너뜀, 실패는 실패
  (Airflow 재시도는 같은 유닛 run 에 다시 붙는다, ADR-0270 §3). 과제가 죽으면 Dagster run 을 끝내고, 유닛은 systemd 의 것이라 계속 돈다.
- **호스트 연결은 스케줄러의 것.** ADR-0417 의 귀결은 "Dagster 용 키를 운영자가 1회" 였지만, 사람 단계는 두지 않는다(소유자 11-07). Dagster 데몬은
  runtime.env 의 같은 연결(`AIRFLOW_CONN_FOUNDATION_HOST` → `FOUNDATION_HOST_CONNECTION`)을 compose 로 받는다. 그 키는 강제 명령 하나만,
  오케스트레이션 프로젝트 네트워크(`airflow_default`)에서만 쓸 수 있다(`authorized_keys` 의 `restrict,from=,command=`). 데몬은 네트워크 셋에 붙어
  있으므로 `default` 에 `gw_priority: 1` 을 주어 호스트로 가는 길이 그 네트워크다(11-09 잰 값: 이미 `airflow_default` 쪽 eth0 이 기본 경로였다, 이제는 선언).
  클라이언트는 Airflow SSH 공급자와 같은 paramiko 5.0.0, 호스트 키는 연결에 박힌 것만 받고(RejectPolicy), 개인 키는 디스크에 쓰지 않는다.
- **실행 큐.** Airflow 가 결정하는 동안 동시 job run 은 Airflow 병렬도(4)를 넘지 못한다. `dagster.yaml` 은 job run 4, 전체 5(검사 run 자리 하나),
  `spark` 풀 run 3(슬롯 수)을 허용하고, 우선순위 클래스 값을 `dagster/priority` 로 단다(ADR-0435). 호스트의 `job_units.py pool-full` 이 여전히
  누가 시작하든 클래스와 슬롯으로 막는다. 숫자는 `test_job_specs.py` 가 compose·작업 목록과 맞춘다.
- **메모리.** run 은 데몬 컨테이너의 자식 프로세스다. 11-09 운영 측정(VmHWM): 데몬 190 MiB, 코드 서버 169 MiB(재적재 중 둘). run 하나를 코드 서버
  크기로 보면 190 + 2×169 + 5×169 = 1,373 MiB, ×1.17 → 상한 480m → **1600m**. 그날 480m 상한은 04:31Z 시작 뒤 20분 동안 1,240번 닿았다
  (`memory.events max`, job run 없이). 호스트 예산 57.75g → 58.84g / 62g(`every-container-has-a-memory-cap.sh`). 첫 `proxied` 가족 뒤 실측으로 다시 잰다.
- **시험.** 기본 CI: `test_job_dags.py`(대역으로 프록시 과제의 풀·태그·XCom·건너뜀·실패·조정자), `test_job_specs.py`(단계·태그·관찰자 꺼짐 거부·큐 숫자).
  CI `dagster-observe`: Dagster 이미지에서 op 작업(바뀜→출력 물질화와 태그, 보류→없음, 실패→실패 태그, 실행 종류 전달과 이상한 종류 거부, 남의
  데이터셋 보고 거부, 조정자의 빚), paramiko 강제 명령(대역 SSH 서버: 요청은 한 명령, 다른 호스트 키 거부), 정의(프록시 작업은 Dagster 작업이고
  airlift 매핑이 빠지며 비교 검사는 남음); **Airflow 이미지에서** 실제 Airflow 3.3.2·airlift 휠로 프록시 과제를 GraphQL 대역에 대고 돌리고, DAG 파일을
  Airflow 로 직렬화한다(`test_job_dags.py WithAirflow` 가 처음으로 CI 에서 돈다).
- **이 조각의 운영 영향은 0 이다.** 어느 작업도 `proxied` 가 아니다(ADR-0458 문턱이 정한다). `proxied` 작업이 없으면 DAG 파일은 airlift 를 import 하지 않는다.
  되돌리기는 그 작업의 `dagster_stage` 를 `shadow` 로 내리는 PR 하나다.

## 개정 각주 — 2026-11-09, `dagster` 단계의 결정(§3)

이 각주는 결정을 바꾸지 않는다. §3 과 11-07 각주가 `dagster` 조각에 남긴 결정 규칙을 채우고, 11-05 사고에 대한 답을 적는다.

- **작업마다 센서 하나** (`orchestration/dagster/decide.py`, `foundation_decide__<id>`, 30초): Dagster 의 스케줄 정의가 아니라 센서다. 스케줄은
  진행 중인 run 과 상관없이 틱마다 run 을 만들고(§Context), 접힌 틱을 run 이 끝난 뒤에 돌릴 수 없다. 센서는 비교기가 증명한 규칙을 그대로 쓴다:
  틱은 Dagster 의 cron 반복(`dual_run.schedule_ticks`), 열린 run 동안의 틱은 기다렸다가 가장 늦은 하나만 돈다(`dual_run.due_ticks` 와 같은 결정 —
  시험이 기록된 날의 공매 Silver 접힘으로 둘을 맞춘다), 입력 사건(시작 입력의 물질화, 이벤트 기록의 storage id 로 셈)은 열린 run 동안 쌓여 run 하나가
  된다. run key 가 결정의 이름이라(`tick:<iso>`·`inputs:<iso>`) 같은 결정은 두 번 시작하지 않는다.
- **11-05 에 대한 답 — 처음 보는 센서는 지난 틱을 돌리지 않는다.** 2026-11-05 23:46:32 배포가 새 DAG 를 켜자 Airflow 3.3 의 `CronTriggerTimetable`
  이 그날 18:45 틱을 바로 돌렸다(ADR-0425). 센서의 첫 평가(작업이 `dagster` 로 막 옮겨졌거나 센서가 초기화됨)는 지금부터 센다. 시험이 그 순간을 재생한다:
  다음 날 18:45 전에는 아무것도 돌지 않는다.
- **평가가 멈춘 동안 놓친 틱(데몬 정지·센서 정지, 300초 넘는 틈)은 그 run 이 작업의 다음 틱 전에 끝날 때만 늦게 돈다**(작업 자신의
  `time_limit_minutes + stop_grace_minutes`; Kubernetes CronJob `startingDeadlineSeconds` 와 같은 뜻, 손으로 적은 시각 없음). 아니면 다음 틱이 같은 일을 한다.
  Airflow 는 시각과 상관없이 마지막 놓친 틱을 돌렸다(경매 수집기의 11-05 16:05 틱을 11-06 07:48 에). 그래서 11-07 각주의 `max_catchup_runs: 1` 대신 이것이다.
- **놓친 입력 사건은 run 하나를 시작한다**: 데이터가 바뀌었고 그것을 읽는 것이 그 run 이다(바뀐 것만 처리). Airflow 는 버렸고(ADR-0179) 배포가 한 번
  시작했다(ADR-0337). 의도된 차이다.
- **시각이 아니라 용량이 시작을 정한다**(ADR-0435): 요청마다 우선순위 클래스 값이 `dagster/priority` 로 붙어 큐가 높은 클래스부터 시작하고, 호스트의
  `job_units.py pool-full` 이 누가 시작하든 더 높은 claim 의 자리를 막는 시작을 `waiting` 으로 돌린다. 늦게 도는 낮은 클래스는 발행을 밀어내지 못한다.
- **Airflow 는 `dagster` 작업의 DAG 를 만들지 않는다**(`foundation_jobs.py`). 그러나 `job_specs.py` 는 아직 `dagster` 를 거부한다: 배포의 드레인과
  배포 뒤 한 번 시작(`foundation-deploy.sh`·`deploy-start-once.sh`), 운영자의 run-now(`run-now-dispatch.sh`), run 의 계보(OpenLineage, ADR-0118 §2), 운영 지도의
  진행 중·실패 로그(`foundation-api` `data_catalog/facts.rs`)가 아직 작업을 Airflow DAG 로 찾는다. 그것들을 옮기는 조각이 단계를 연다.

## 개정 각주 — 2026-11-14, 입력 사건은 Airflow 가 기록한 시각으로 읽는다(§2)

이 각주는 결정을 바꾸지 않는다. §2 의 비교가 읽는 사실을 §3 대로 "Airflow 가 결정한 그 사실"로 맞춘다.

- **잰 값(운영 원장, 2026-11-09 04:00Z..11-13 16:31Z, 2,532줄, 작업 71개 모두 `shadow`).** 시계 틱은 2,803개가 같고 3개가 달랐다.
  입력 사건은 **3개만 같고** Airflow 만 한 결정 81개·Dagster 만 한 결정 56개가 작업 39개에 걸쳐 있었다. ADR-0458 의 문턱을
  지난 작업은 25개이고, 못 지난 46개 중 43개는 `event` 종류의 일치가 하나도 없어서였다.
- **원인.** 비교는 입력 사건을 airlift 가 관찰한 물질화의 시각으로 읽었다. 그 시각은 airlift 센서가 다음에 Airflow 를 읽은
  때이고, Airflow 스케줄러는 그 전에 `asset_triggered` run 을 시작한다(11-13 `source_sweep`: 자산 사건 04:14:31.9, 토지이용계획
  Silver run 시작 04:14:39.7, 물질화 04:14:59.1 — 3~27초 늦다). 그래서 run 은 "사건 없는 run", 사건은 "run 없는 사건"이 됐다.
  11-07 각주의 8일 재생이 차이 0 이었던 것은 Airflow 의 `asset_event` 표를 읽었기 때문이다.
- **고침.** 비교는 입력 사건을 Airflow 의 자산 사건(`GET /api/v2/assets/events`, Viewer 계정, `dual_run.airflow_input_events`)에서
  읽는다. Airflow 가 결정하는 동안(`shadow`·`proxied`)의 사실은 Airflow 의 것이다. `dagster` 단계의 센서(`decide.py`)는 그대로
  Dagster 의 이벤트 기록으로 결정한다: 같은 결정을 관찰 지연만큼 늦게 한다.
- **시계 틱 3개.** (a) 막힌 구간이 끝나고 스케줄러 지연(60초) 안에 Airflow 를 읽으면 따라잡는 run 이 아직 없다
  (`unit_price_versions_migration` 11-09: 막던 run 이 21:49:53 에 끝났고 검사는 21:50:11.0 에 읽었고 run 은 21:50:11.7 에 생겼다) — 그
  틱은 다음 비교가 센다. (b) 풀리기 60초 안쪽의 틱이 있을 때 그 앞 틱까지 도는지는 스케줄러 루프와의 경주다: 11-03·11-04 에는
  둘 다 돌았고 11-12 `rt_molit_real_transaction_collect`(18:25:13 에 풀림)에는 18:25 만 돌았다 — 앞 틱은 어느 쪽이어도 차이가 아니다.
  (c) `registered_factory` 11-12 11:20 틱은 Airflow 가 15:48:11 에 돌렸고 검사가 15:48:06 에 읽었다: 같은 틱이고, 다음 비교가 같다고 센다.
- **고친 규칙으로 같은 기간을 다시 비교(운영 Airflow API 읽기 전용, 한 창).** 시계 틱 2,338개·입력 사건 80개가 같고 **차이 0**,
  일시정지 구간의 결정 130개는 따로. 원장은 추가 전용이라 옛 줄의 차이는 남는다: 작업마다 마지막 차이 뒤로 다시 센다(ADR-0458 §3).

## 개정 각주 — 2026-11-14, `dagster` 단계의 나머지 경로

이 각주는 결정을 바꾸지 않는다. 위 각주가 남긴 목록(배포의 드레인과 한 번 시작, run-now, 계보, 운영 지도)과 그 목록에 없던 둘(자산 조정자가
말한 빚을 누가 시작하나, `check` 과제와 재시도)의 설계는 [ADR-0565](./0565-a-job-dagster-decides-is-reached-through-dagster-by-everything-that-reached-it-through-its-airflow-dag.md)
가 정한다: 경로마다 장치 하나, 어느 쪽으로 닿는지는 작업의 `dagster_stage` 하나, `job_specs.py` 는 그 경로가 모두 선 PR 에서 단계를 연다.
