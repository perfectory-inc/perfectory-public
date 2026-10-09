# ADR 0165: 더니어 운영 현황은 데이터 카탈로그의 실행·품질 기록과 작업 목록을 읽어 보인다

- Status: Accepted
- Date: 2026-10-09
- Builds on: [ADR-0117](./0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md)(사람이 보는 곳은 데이터 카탈로그),
  [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(실행마다 OpenLineage),
  [ADR-0119](./0119-staff-read-the-data-catalog-in-dawneer-through-foundation.md)(더니어 → Foundation → DataHub),
  [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)(작업 목록 `jobs.v1.json`)

## Context

자동 파이프라인이 실제로 도는지 사장이 볼 화면이 없다. 예약 작업의 결과는 Airflow 화면·systemd 저널·데이터
카탈로그에 흩어져 있고, 품질 검사 결과는 DataHub 검사 항목에, 원천 수집·적재 기록은 Foundation 의
`GET /catalog/v1/pipeline-graph` 실시간 덧씌움에 있다.

같은 사실을 두 번째로 모으면 안 된다(ADR-0117 §3 "필요한 요약만 DataHub API 에서 받아 그린다"). 확인한 것:

- Airflow 의 실행마다 OpenLineage 가 DataHub GMS 로 간다(`compose.orchestration.yml` `AIRFLOW__OPENLINEAGE__TRANSPORT`).
  DataHub 1.7 의 OpenLineage 변환기는 작업 이름 `<dag_id>.<task_id>` 의 첫 마디를 흐름(DataFlow) id 로, 실행 id 를
  DataProcessInstance 로 만들고, 시작은 `STARTED`, 끝은 `COMPLETE` + `SUCCESS`/`FAILURE` 로 적는다. GraphQL 에서
  `DataJob.runs` · `DataProcessInstance.state` 로 읽힌다.
- 품질 작업(`scripts/ops/data-quality-check.py`)은 계약마다 `odcs` 플랫폼 데이터셋의 검사 항목에
  `reportAssertionResult` 로 결과를 남긴다. `Assertion.runEvents` 로 읽힌다.
- **카탈로그에 없는 것**: 작업 목록 자체(한 번도 돌지 않은 작업은 카탈로그에 없다), 꺼진 작업과 그 이유, 다음 실행
  시각. 앞의 둘은 `jobs.v1.json` 에 있고, 다음 시각은 그 일정(cron, UTC)에서 계산된다.
- Airflow REST API 를 직접 읽는 길은 기각했다. 같은 실행 사실의 두 번째 출처가 되고, 직원 로그인이 Zitadel OIDC 뿐이라
  (`webserver_config.py`) API 토큰을 받으려면 인증 관리자를 바꿔야 한다.
- by-PNU 서빙(필지·건물)이 반영한 Gold 스냅숏과 발행 시각은 R2 manifest 에만 있고, Foundation API 에는 그것을 읽는
  경로가 없다.

## Decision

1. 더니어에 메뉴 **운영 현황**(`#/operations`)을 둔다. 세 칸을 보인다.

   | 칸 | 보이는 것 | 출처 |
   |---|---|---|
   | 예약 작업 | 작업마다 일정·다음 실행·마지막 실행 시작·걸린 시간·결과, 꺼진 작업은 그 이유 | 목록·일정·켜짐·이유: `jobs.v1.json` / 다음 실행: 그 cron 에서 계산 / 실행: DataHub 의 OpenLineage 실행 기록 |
   | 데이터 품질 검사 | 계약마다 검사 수·통과·실패·결과 없음·마지막 검사, 펼치면 검사별 결과와 품질 작업이 적은 값 | DataHub `odcs` 데이터셋의 검사 항목과 마지막 결과 |
   | 수집·적재 기록 | 데이터 지도에서 실행 기록과 묶인 항목마다 상태와 마지막 기록 시각 | `GET /catalog/v1/pipeline-graph` 의 `runtime` (Foundation 이 기록 DB 에서 실시간 확인) |

2. Foundation 데이터 카탈로그 API 에 읽기 경로 둘을 더한다. 권한·중계는 ADR-0119 와 같다
   (`foundation.metadata:read`, 더니어 `/api/data-catalog/` 허용 목록, GET 만).
   - `GET /data-catalog/v1/scheduled-jobs` — `jobs.v1.json` 의 모든 작업을 그 순서로 내고, DataHub 에서 흐름 id 가
     DAG id 와 **정확히 같은** 흐름의 하위 작업 실행 중 마지막 상태 변화가 가장 늦은 것을 마지막 실행으로 붙인다.
     DataHub 가 답하지 않으면 작업 목록은 그대로 내고 `run_history_error` 에 이유를 적는다(기록 없음과 읽지 못함을 섞지
     않는다). 카탈로그가 상한(흐름 5,000)보다 크면 일부만 읽고 넘어가지 않고 실패로 알린다.
   - `GET /data-catalog/v1/quality-checks` — `odcs` 데이터셋마다 검사 항목과 그 마지막 결과.
   계약 문서는 `docs/openapi/data-catalog.v1.json`, 화면 타입은 거기서 생성한다.

3. **DAG id 는 한 곳에서 정한다.** `jobs.v1.json` 의 `dag_id_prefix` 를 DAG 생성기(`job_specs.py`)와 Foundation 이 함께
   읽는다. 배포 셸 두 곳(`airflow-runtime.sh`, `foundation-deploy.sh`)은 아직 글자로 적고 있고, 그중 `airflow-runtime.sh` 의
   글자는 시험(`test_airflow_readiness.py`)이 선언 값과 대조한다. `foundation-deploy.sh` 0단계는 직전 릴리스의 작업 목록을
   읽으므로 이 필드가 운영에 한 번 배포된 뒤에 옮긴다.

4. **작업 목록은 이미지에 실려 간다.** `foundation-api` 이미지가 `orchestration/jobs.v1.json` 을 데이터 지도와 같은 방식으로
   싣고 `FOUNDATION_PLATFORM_SCHEDULED_JOBS_PATH` 로 가리킨다(`deploy_contract.rs` 가 대조).

5. 더니어 중계 허용 목록에 `GET pipeline-graph` 를 다시 연다. ADR-0119 §6 이 걷어 낸 것은 선언 그래프를 그리는 화면이고,
   이 화면은 선언이 아니라 실시간 덧씌움(수집·적재·발행 기록)만 읽는다. 그 API 는 ADR-0117 §8 의 조건이 차면 지워지고,
   이 칸은 그때 DataHub 실행 기록으로 옮긴다.

6. 다음 실행은 `croner`(MIT)로 계산한다. 작업 목록의 일정은 `job_specs.py` 가 `M H * * *` 꼴로 묶어 두므로 Airflow 의
   cron 해석과 같다.

## Consequences

- 서버에서 할 일: 없음(새 네트워크·비밀값·서비스 없음). 기존 `FOUNDATION_PLATFORM_DATAHUB_GMS_URL` 과
  `metadata-shared` 망을 그대로 쓴다. 배포 뒤 확인할 것: Airflow 실행이 DataHub 에 흐름 `foundation_<id>` 로 들어와
  있는지(들어와 있지 않으면 화면이 "실행 기록 없음"을 보이며, 그것이 고칠 일이다).
- `takes_turns` 작업이 차례를 미룬 실행도 Airflow 에는 성공으로 남는다. 화면은 Airflow 가 적은 대로 보인다.
- **남은 일 — 서빙 반영**: 필지·건물 by-PNU 서빙이 반영한 Gold 스냅숏·발행 시각은 이 화면에 없다. 근본 해법은 굽기
  작업이 그 값을 OpenLineage 실행 기록(출력 데이터셋의 facet)으로 DataHub 에 남기고, 이 화면이 같은 경로로 읽는 것이다.
  그 전까지는 `gold_panel_rebuild` · `by_pnu_serving_bake` 의 마지막 실행이 대리 지표다.
- 출처: [DataHub OpenLineage 변환기](https://github.com/datahub-project/datahub/blob/v1.7.0.1/metadata-integration/java/openlineage-converter/src/main/java/io/datahubproject/openlineage/converter/OpenLineageToDataHub.java),
  [DataHub GraphQL runs](https://github.com/datahub-project/datahub/blob/v1.7.0.1/datahub-graphql-core/src/main/resources/runs.graphql),
  [Airflow FAB 토큰(OAuth 는 직접 구현)](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/token.html),
  [croner](https://github.com/hexagon/croner-rust).
