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
