# ADR 0616: 데이터 작업은 AWS 의 ECS 에서 Dagster 자산으로 돈다 — ai-server 의 작업 계층은 옮기지 않는다

- Status: Proposed (셋째 초안. 사장 결정 2026-11-24 "AWS 가 전제다" 뒤 다시 썼다. AWS 자원은 사장이 항목별 비용을 본 뒤에만 만든다)
- Date: 2026-11-24
- Supersedes (받아들여지면, AWS 에서 도는 작업에 대해):
  [ADR-0417](./0417-dagster-runs-a-job-as-its-systemd-unit-through-the-forced-command-and-reads-the-journal-as-a-pipes-session.md)
  (강제 명령의 유닛으로 시작),
  [ADR-0605](./0605-scheduled-job-pools-are-cut-by-the-resource-they-guard-and-counted-by-dagsters-own-pools-and-a-jobs-wait-is-measured-not-computed.md)
  §3(호스트 메모리 claim),
  [ADR-0480](./0480-every-job-is-held-to-the-outcome-its-definition-expects-not-to-its-units-exit.md)(기대 결과 지표),
  [ADR-0431](./0431-scheduled-jobs-move-from-airflow-to-dagster-one-family-at-a-time-after-a-dual-run-in-which-both-decide.md) §3(우리 센서의 결정 규칙),
  [ADR-0565](./0565-a-job-dagster-decides-is-reached-through-dagster-by-everything-that-reached-it-through-its-airflow-dag.md) §4~§6(배포·run-now·조정자가 Dagster 로 닿는 길),
  [ADR-0583](./0583-jobs-move-to-dagster-in-four-groups-one-switch-each-and-the-per-job-dual-run-gate-retires-with-the-first.md) §7(호스트에 남는 것),
  ADR-0605 §1·§6(기계 크기의 풀과 그 한도 설정).
  ai-server 에서 도는 동안에는 그 ADR 들이 그대로다. ADR-0565 §3·§9(run 재시도는 죽은 run 에만)는 그대로 가져간다.
- 함께 읽는 것: ADR-0615(W97b, 파티션 Gold 레인. 그 발행기 코드가 main 에 있어 Status 는 Accepted 이고, 머리말이 "정한 것"과 "방향만"을 나눈다: Dagster 쪽은 방향만이다), ADR-0618(W86c, 타일 굽기를 자산으로, Proposed).
- 용어: **태스크** = ECS 태스크(컨테이너 묶음 하나의 실행). **run** = Dagster run. **줄** = Dagster 의 run 줄.

## Context

**사장의 결정(2026-11-24).** 사용자가 없다. 서빙 데이터를 최신으로 되돌리는 일과 ai-server 의 우리 작업 계층·배포 스크립트·유닛을 고치는 일을
멈춘다. 처음부터 AWS 로 설계한다. 수집(Bronze)은 멈추지 않는다. AWS 자원은 항목별 비용을 사장이 본 뒤에만 만든다.
"오픈소스를 끝까지 쓰고 우리 코드는 접착제만."

**ai-server 의 지금 모양.** Dagster 는 언제 돌지만 정하고(그것도 우리 센서로), 시작은
op → SSH 강제 명령 → `start-scheduled-job.sh` → systemd 유닛이다. 그 아래 우리 것: 호스트 허가·풀·메모리 claim·대기 줄·배포 보류,
유닛 98 파일, 유닛 생성 크레이트, 결과 원장과 수출기, 배포 스크립트 33 파일. 이 계층은 "컨테이너를 띄울 수 있는 것은 호스트의 root"라는
한 가지 사실 때문에 생겼다(ADR-0565 의 2026-11-18 각주). AWS 에서는 그 자리에 IAM 이 있다.

**잰 것(유닛의 systemd 저널 7일, 11-24 까지, 릴리스의 `job-run-times.py` 짝짓기; 정의의 `memory_max`).** Dagster 의 run 기록은 쓰지 않았다:
묶음마다 Dagster 로 넘어온 날부터만 있다.

| 묶음 · 풀 | run | 시간 | 선언 메모리 × 시간 |
|---|---|---|---|
| 수집 · default | 583 | 77.8 h | 201.5 GiB·h |
| 운영 · default(그중 조정자 1,879 run · 26.3 h) | 2,962 | 77.6 h | 153.8 GiB·h |
| Silver · default / duckdb / spark | 396 / 155 / 143 | 30.1 / 20.3 / 25.8 h | 75.9 / 299.4 / 35.1 GiB·h |
| Gold · default / duckdb / spark | 55 / 233 / 352 | 0.4 / 55.2 / 69.2 h | 0.4 / 356.7 / 80.6 GiB·h |
| 합 | 4,887 | 360.8 h | 1,205.6 GiB·h(Spark 는 래퍼의 한도만 선언돼 있다) |

켜진 작업 116: 입력으로 시작 77, 예약 39. 표를 쓰지 않는 작업 6. 표 209개 중 43개는 쓰는 작업이 둘 이상.
Dagster 자신: 웹서버 222 MiB, 데몬 255 MiB, 읽기 전용 웹서버 210 MiB(코드 서버는 지금 run 을 품어 따로다), DB 1,001 MB. 이미지: 발행기 293 MB, Dagster 554 MB, Spark 1.87 GB.

**읽은 것.**

| 사실 | 어디서 |
|---|---|
| `EcsRunLauncher` 는 run 마다 태스크를 만든다. 클러스터·서브넷·보안 그룹은 지금 태스크의 것을 쓴다(`use_current_ecs_task_config`) | docs "Deploying Dagster to AWS" → "Customizing the launched run's task"; `dagster_aws/ecs/launcher.py` |
| 작업마다 크기: 태그 `ecs/cpu`·`ecs/memory`·`ecs/ephemeral_storage`. 기본값은 `run_resources`. Fargate 의 임시 디스크는 20~200 GiB | docs 같은 쪽 "Customizing CPU, memory, and ephemeral storage in ECS" |
| 작업마다 `ecs/run_task_kwargs` 태그(예: `capacityProviderStrategy: FARGATE_SPOT`) | docs 같은 쪽 |
| run 태스크는 지금 태스크의 컨테이너 정의를 물려받고 이미지만 코드 위치의 것으로 바꾼다. `secrets` 를 실을 수 있다 | `dagster_aws/ecs/tasks.py` `get_task_definition_dict_from_current_task` |
| Pipes 컨텍스트는 `partition_key`·`partition_key_range`·`extras` 를 싣는다 | 운영 이미지 1.13.27 `dagster_pipes.PipesContextData` |
| `AutomationCondition.eager/on_cron/on_missing/code_version_changed`, `FreshnessPolicy.time_window/cron` 이 1.13.27 에 있다 | 운영 이미지에서 확인 |
| 줄은 우선순위 내림차순, 같은 값은 선착순. 풀은 run 하나에 칸 하나 | 1.13.27 `_priority_sort`, `claim_concurrency_slot` |

문서 인용은 master 문서다. 태그 `1.13.27` 의 `dagster_aws/ecs/launcher.py` 에서 다시 확인한 것: run 의 태그 `ecs/cpu`·`ecs/memory`(623-624),
`ecs/ephemeral_storage`(643), `ecs/task_overrides`(636), `ecs/container_overrides`(652), `ecs/run_task_kwargs`(660, JSON 을 run_task 인자 위에 덮는다).
재시도의 간격과 신선도 검사는 §4·§6 에 소스 줄과 함께 적었다.

## Decision

### 0. 원칙

정하고·줄 세우고·허가하고·시작하는 우리 코드는 AWS 에 없다. ai-server 의 그 계층은 옮기지 않고, 그 위에서 도는 일이 AWS 로 넘어가면 끈다.
남는 우리 것마다 맡지 못한 기능과 이유를 §9 에 적는다.

### 1. 놓이는 곳

| 것 | AWS | 크기의 근거 |
|---|---|---|
| Dagster 웹서버 + 데몬 + 코드 위치 | ECS(Fargate) 서비스 하나, 항상 켬 | 지금 세 컨테이너 합 687 MiB |
| Dagster 의 저장소 | RDS PostgreSQL, 단일 AZ | 지금 1,001 MB |
| run | `EcsRunLauncher`: run 마다 태스크 | — |
| 이미지 | ECR, digest 로 참조 | 발행기 293 MB, Dagster 554 MB |
| 비밀(R2 키, 원천 API 키) | Secrets Manager. 일의 진입점이 제 태스크 역할로 읽는다(§2-8). 태스크 정의에는 싣지 않는다 | 지금 `/etc/foundation-platform/*.env` 여덟 |
| 로그 | CloudWatch Logs | — |
| 레이크하우스 | **R2 그대로**(Iceberg 카탈로그도) | AWS → R2 쓰기는 AWS 의 인터넷 송신 요금, R2 → AWS 읽기는 양쪽 무료 |
| UI 접근 | SSM 포트 포워딩. 공개 주소 없음 | — |

계정은 조직의 platform 계정이다(SSO 프로필 `perfectory-org-admin`). 공짱 운영 계정과 286958704196 은 쓰지 않는다.
그 계정과 리전은 ADR-0619 가 `platforms/foundation-platform/config/aws-platform.contract.json` 의 `account`·`region` 에 고정한다. 이 ADR 은 id 를 다시 적지 않고 그것을 따른다.

### 2. 일을 시작하는 길

1. **기본: run 태스크가 일을 한다.** 코드 위치 이미지에 그 릴리스의 발행기 바이너리와 DuckDB 확장을 함께 넣는다. 자산은
   `PipesSubprocessClient` 로 발행기를 부른다. 태스크는 run 마다 하나이고, 크기는 작업 정의에서 나온 `ecs/cpu`·`ecs/memory`
   (디스크가 필요하면 `ecs/ephemeral_storage`) 태그다. 유닛 파일·드롭인·메모리 claim 이 하던 일이다.
2. **이미지가 다른 일은 코드 위치가 다르다.** 실행기는 run 의 태스크를 그 작업이 속한 코드 위치의 이미지로 띄운다. 그래서 도구가 다른 일은
   코드 위치를 따로 둔다: `data`(발행기)와 `tiles`(발행기 + GDAL + tippecanoe, ADR-0618). 이쪽도 run 태스크가 일을 하고 `ecs/*` 태그가 그대로 든다.
   둘째 태스크(`PipesECSClient`)는 쓰지 않는다. **Spark 작업은 목표에 없다**: AWS 로 가는 것은 DuckDB 로 새로 지은 레인뿐이다.
   ai-server 의 Spark 작업은 그 레인이 넘어가면 꺼지고, 넘어갈 레인이 없는 것은 사장·코디네이터가 끄라고 할 때 꺼진다(열린 질문 2).
3. **권한은 IAM 이다.** Dagster 의 태스크 역할은 `ecs:RunTask`(정해진 태스크 정의 계열), `iam:PassRole`(정해진 역할), 로그 읽기만 갖는다.
   일의 태스크 역할은 제 비밀만 읽는다(읽기 키와 쓰기 키는 다른 역할). 강제 명령·SSH 키·`start-scheduled-job.sh`·호스트 허가가 하던 일이다.
4. **릴리스 = 이미지 digest.** 릴리스 디렉터리와 `admitted-writer-runtime.sh` 가 하던 확인은 "서명된 digest 만 ECR 에 올리고, 태스크 정의는
   digest 를 가리킨다"가 대신한다. 배포는 태스크 정의의 새 개정과 서비스 갱신, 코드 위치 다시 읽기다. 스키마 마이그레이션은 그 전에 도는 태스크 하나다.
5. **작업을 붙잡기.** 마이그레이션이 건드리는 자산의 자동화를 멈추고(센서·예약) 줄에 있는 그 작업의 run 을 끝낸다. 호스트 관문은 없다.
   "이 자산들만"을 Dagster 가 한 번에 해 주는 기능은 없으므로 이 호출 묶음이 우리 것으로 남는다(§9).
6. **Spot.** 다시 돌려도 되는 작업은 `ecs/run_task_kwargs` 로 Fargate Spot 을 쓴다. 중단은 태스크의 죽음이다(7).
7. **태스크가 죽으면.** run 태스크가 일을 하므로, Spot 중단이나 메모리 초과는 재시도 정책을 쥔 프로세스를 죽인다. 살리는 것은 Dagster 의
   run monitoring(실행기가 태스크의 건강을 묻는다: `launcher.py` `check_run_worker_health`)과 **죽은 run 에 한한** run 재시도다
   (ADR-0565 §3·§9 의 `retry_on_asset_or_op_failure: false` 그대로). 재시도의 표는 §4 에 하나 있다.
8. **작업마다의 역할과 비밀.** run 태스크는 Dagster 서비스 태스크에서 복제되므로, 그대로면 일이 Dagster 의 역할과 비밀을 물려받는다. 그러지 않는다:
   - 역할: run 의 태그 `ecs/task_overrides` 가 `{"taskRoleArn": …}` 을 싣는다(`launcher.py:636`). 역할은 작업마다가 아니라 **종류마다**다
     (읽기만, 레이크하우스 쓰기, 원천 수집). 종류는 작업 정의의 한 낱말이다.
   - 비밀: 태스크 정의에 싣지 않는다. 일의 진입점이 제 태스크 역할로 Secrets Manager 에서 읽는다. 그래서 역할이 읽을 수 있는 비밀만 읽힌다.
     Dagster 서비스의 역할은 그 비밀을 읽지 못한다.
9. **크기를 run 에 싣는 길.** 선언형 자동화가 만든 run 의 태그는 그 자동화 센서의 `run_tags` 에서 온다
   (`AutomationConditionSensorDefinition(target=…, run_tags=…)`, 1.13.27 에서 확인). 그래서 크기는 작업마다의 수가 아니라 **크기 등급**이다:
   등급마다 센서 하나가 그 등급의 자산을 고르고 `ecs/cpu`·`ecs/memory`·`ecs/ephemeral_storage`·역할(8)을 싣는다. 예약으로 도는 job 은 job 의 태그다.
   한 run 은 한 등급의 자산만 품는다.

### 3. 작업은 자산이다

| 모양 | 수 | Dagster 로 |
|---|---|---|
| 작업 하나가 제 표를 쓴다 | 대부분 | `@multi_asset` |
| PNU 로 키를 가진 Gold 레인 | ADR-0615 | 시도로 나뉜 자산 |
| 여러 작업이 한 표를 쓴다 | 표 43개 | 표 하나에 쓰는 자산 하나. 표마다의 분류는 아직 없다(열린 질문 1) |
| 표를 쓰지 않는 일 | 6 | Dagster job + `ScheduleDefinition` |

**파티션은 "Dagster 가 상태를 지니는 단위"다.** 무엇으로 나누는지는 자산마다 다르다: PNU 로 키를 가진 Gold 는 시도(ADR-0615),
Bronze 는 수집한 밤(시간 파티션), 타일은 피처 id 접두(ADR-0618). 아래는 시도 파티션의 문장이다(ADR-0615).
키는 더하기만 한다. 표가 정본이다: 표 머리가 시도마다 지은 버전을 지닌다.
ECS 에서는 Dagster 가 파티션 하나를 시작할 수 있다: 자산의 몸이 run 의 파티션 키·범위를 읽어 일에 넘긴다(Gold 레인은 환경 변수 `FOUNDATION_PLATFORM_GOLD_VERSIONS_PARTITIONS`, ADR-0615; 발행기는 Pipes 컨텍스트를 스스로 풀지 않는다). 그래서 ADR-0615 의
"관측 자산이 시도별 데이터 버전을 내고 하류가 `eager()`" 가 목표 모양이다. "레인의 run 이 낡은 시도를 모두 따라잡는다"는 ai-server 에서의 모양이다.
어느 쪽이든 실패한 run 의 사건에 기대지 않는다: 표준 Pipes 클라이언트는 태스크가 끝난 뒤에 결과를 돌려준다. 다음 run 이 표에서 다시 보고한다.

**파티션 자산의 모양(ADR-0615 의 1.13.27 실험 (a)·(d), 프로세스 안에서).** `@asset(partitions_def=…, output_required=False)` 이고 몸은 `Output` 을
내지 않는다. 그러면 범위 run 이 성공해도 **일이 보고한 파티션만** 구체화된다(하나에 사건 하나, 나머지는 그대로). `Output` 을 내면 범위의 모든
파티션이 구체화되고, 값을 돌려주면 기본 IO 관리자가 범위 run 을 거부한다. "바뀐 것 없음"에 구체화가 필요 없는 것(§6)은 이 모양 덕분이다.

### 3-1. Silver 가 Bronze 를 읽는 길 — 경매는 형식 지정 읽기 (사장 결정 2026-11-25)

근거: `concepts_brief.bronze-objects-at-write.md`(EGM 일곱 검토)와 그 S0 측정. 사장은 그 가운데 "가"(형식 지정 읽기)를 골랐다.

**Bronze 는 그대로다.** 원천의 답을 그대로 담은 gzip JSON Lines 객체와 그 등록부(`catalog.bronze_object`), 더하기만 한다. Bronze 를
Iceberg 표로 두지 않고 쓸 때 Parquet 으로 바꾸지 않는다: 원천의 정확한 답을 지켜야 하고(ADR-0540), 등록부와 계보(ADR-0046·0084)를 둘로
만들지 않는다. 바뀌는 것은 Silver 가 읽는 쪽이다.

**경매 검색은 나누지 않는다.** 수집기 run 하나의 물건 검색·매각결과 검색은 객체 하나다. 지워진 물건의 표시(removal)는 그 객체 한 개의
줄로 "모든 조건을 마지막 쪽까지 읽었나"(`search_complete`)를 판정해 정한다. 검색이 여러 객체로 나뉘면 어느 객체도 완결이 아니어서 표시가
소리 없이 멈춘다.

**Silver 는 날로 나누지 않는다.** Silver 는 §8 기준 레인의 모양 그대로 파티션 없는 `@multi_asset` 이고 `eager()` 로 돈다. 무엇을 읽을지는
지금의 계획기(`plan-silver-load-objects`: 표의 커밋이 기록하지 않은 객체, 창 안, 오래된 것부터)가 고른다. Bronze 의 파티션은 §3 의 "수집한
밤"으로 남는다. 수집기 run 의 id(키 끝의 스냅숏 id)는 run 전체를 보는 규칙(removal)에서만 묶음 단위다.

**경매 Silver 는 Bronze 를 형식 지정으로 읽는다.** `DuckDB` 의 내장 기능만 쓴다:

- 객체는 `read_json(…, format = 'newline_delimited', columns = {…})` 로 읽는다. 봉투의 열(`request`, `http_status`, `read_outcome`, `body` 와
  역할이 읽는 그 밖의 칸)은 선언한다. 줄 번호는 `WITH ORDINALITY` 다: 빈 줄을 건너뛴 줄의 차례로, 발행기가 지금 매기는 번호와 같다(시험함).
- `body` 는 원천의 답이 문자열로 든 것이라 `json_transform` 으로 한 번 푼다. 역할마다 읽는 모양만 선언하고, 그 안의 값은 JSON 으로 둔다:
  레인은 옛 작업의 값 규칙(`python_values.sql`, 예: `5` 와 `"5"` 를 가른다)을 JSON 값에 적용하므로, 값을 문자열이나 숫자로 미리 바꾸지 않는다.
- 레인의 SQL 은 줄 전체나 `body` 전체를 JSON 으로 다시 읽지 않고 선언한 열을 읽는다. 그래서 메모리는 객체의 크기를 따르지 않는다.
- 읽을 때 줄을 바이트로 묶는 일(`duckdb_parquet_stage::line_parts` 의 묶음, `memory_per_line_byte`)은 없앤다. #1718 은 병합하지 않고 닫는다.

**메모리 등급은 작다.** 경매 Silver 의 run 은 작은 등급(2 GiB 의 `DuckDB`)으로 돈다. 이 레인에는 객체 크기 상한이 필요 없다.

| S0 측정 (2026-11-25, 한 객체: 2026-11-09 의 물건 검색, 394 MiB 의 줄, 2,435 줄) | 결과 |
|---|---|
| 통째로, 지금의 SQL (AWS ARM, W101) | `DuckDB` 한도 8 GiB 와 16 GiB 사이에서 처음 통과. 16 GiB: 17 초, RSS 7.2 GB |
| 형식 지정 읽기 시제품 (AWS ARM, W101) | 2 GiB 에서 통과: 1 초, RSS 324 MiB |
| 형식 지정 읽기 == 지금의 SQL (노트북, `DuckDB` 2.0.0, 2 GiB, 2 스레드) | 같다: `ca_search_rows` 64,059 행, 모든 열의 sha256 이 같다(`source_line_number` 포함). 걸린 시간 17 초, 끝난 뒤 30 MiB |

W101 의 시제품이 센 121,608 은 키로 줄이기 전의 물건 수다: 검색은 네 조건을 묻고(25,572 + 29,602 + 32,431 + 34,003), 한 조건 안에서는
겹치지 않으며, 40,721 개가 둘 이상의 조건에 나온다. 키마다 한 줄이면 64,059 다.

**그 다음.** 레인의 역할마다 SQL 을 형식 지정 읽기로 옮긴다(역할의 차례와 시험은 그 PR 이 적는다). 각 역할은 고정물에서 형식 지정 읽기가
지금의 SQL 과 같은 행을 내는 시험을 지닌다. 코드가 바뀌어 옛 객체를 다시 읽는 길(`follow_search`, ADR-0120 다시 짓기)도 같은 읽기라
등급이 크지 않아도 된다.

**넣지 않는 것.**
- 다른 Silver 레인(온비드, 산업단지 공고·분양공고, 산업단지 Silver, data.go.kr 파일)은 지금처럼 통째로 읽는다. 저마다 형식 지정 읽기로
  옮기는 것은 그 레인을 AWS 의 새 모양으로 옮길 때 정한다.
- 원천 제공자의 파일(data.go.kr 의 zip·csv)은 이 절 밖이다: 키가 바이트를 이름한다. 그 읽기는 따로 정한다(`DuckDB` 의 `read_csv` 가 후보,
  확인 안 함).
- 모든 수집기가 쓰는 JSON Lines 작성기 하나(gzip 도우미 셋·저장 몸통 둘의 중복)는 메모리 때문에는 필요 없어졌다. 중복을 줄일지는 따로 정한다.

**말.** 상태를 지니는 "조각"은 Dagster 의 파티션뿐이다(ADR-0615). 묶음(group), 수집기 run(run id 하나), 수집한 밤(§3). 읽을 때의
"조각(part)"이라는 말은 그 코드와 함께 없앤다.

**기각한 것.** 64 MiB 에서 굴리고 Silver 를 수집한 날로 나누기(검색이 나뉘면 removal 이 소리 없이 멈추고, 날은 run 의 단위가 아니다).
크기 등급만 키우기(통째 읽기는 8~16 GiB 가 필요하고 객체와 함께 자란다). Bronze 를 Iceberg 표로(두 번째 Bronze, 등록부와의 끈이 끊김). 쓸 때
Parquet(원천의 정확한 답을 잃음). 읽을 때 줄 묶음을 `shard_plan` 에 합치기(#1718: 서로 무관한 두 계획기).

### 4. 언제

| 지금(`decide.py`·조정자) | AWS |
|---|---|
| 예약, 상류 없음(원천 수집) | `ScheduleDefinition` |
| 예약, 상류 있음 | `AutomationCondition.on_cron(<제 cron>)` |
| 입력으로 시작 | `AutomationCondition.eager()` |
| 코드가 바뀜 | 자산의 `code_version` = 이미지 digest 에서 나온 값, `code_version_changed` |
| 호스트가 거부한 시작, 줄에 다시 넣기, 놓친 시작 | 없다: 호스트가 없다. run 은 Dagster 의 줄에 있다 |
| 레인을 처음 켤 때(자동화의 첫 평가 전에 이미 관측된 입력) | 자동으로는 아무것도 요청되지 않는다(같은 실험). 켜는 PR 이 백필을 한 번 내거나, 조건에 "없으면"(`on_missing()` 의 항)을 더한다. 레인마다 어느 쪽인지는 그 레인의 ADR 이 적는다 |
| 실패 뒤 물러났다 다시(10분 → 4시간) | 길게 물러나는 식은 두지 않는다: 아래 재시도 표 |
| 관측 자산(파티션 레인의 입력)의 박자 | 그 관측 job 의 `ScheduleDefinition`(레인의 cron). 관측은 가장 작은 등급의 태스크에서 읽기 역할로 돈다 |
| 표 머리와 핀을 대조 | 쓰는 일이 모두 Dagster 의 자산이면 필요 없다. 운영자의 일도 Dagster 의 run 으로 한다 |

**재시도 — 세 ADR(0615·0616·0618)의 표는 이것 하나다.**

| 무엇이 실패했나 | 누가 다시 하나 | 왜 |
|---|---|---|
| 태스크가 죽었다(Spot 중단, 메모리 초과) | Dagster 의 run 재시도, 죽은 run 에 한해 | 재시도 정책을 쥔 프로세스가 죽었다. run 재시도에는 간격이 없다(1.13.27 `_daemon/auto_run_reexecution` 에 지연이 없다) |
| 일이 실패했다(원천의 일시 오류, 커밋 충돌) | 자산의 `RetryPolicy`, 짧게(분 단위, 몇 번) | 태스크가 살아 있어 싸다 |
| 그래도 실패 | 다음 박자(cron 틱이나 다음 입력) | 길게 물러나는 동안 태스크를 띄워 두면 돈이 든다. 박자가 이미 물러남이다 |
| 그래도 실패, 그리고 `eager()` 로 도는 파티션 | 조건에 항을 더한다: `eager()` 또는 (`execution_failed()` 이고 레인의 cron 틱이 지남) | `eager()` 만으로는 FAILED 로 남은 파티션을 다시 요청하지 않는다. 입력의 버전이 바뀔 때까지 그대로다(ADR-0615 의 1.13.27 실험 (c), 데몬 없이 프로세스 안에서) |
| 고쳐야 도는 실패(코드, 계약) | 아무도. 검사 실패·run 실패의 알림(§6) | 다시 해도 같다 |
| 커밋 충돌(다른 쓰는 쪽이 먼저 커밋) | 일 자신이 그 자리에서 다시 커밋한다(ADR-0615 2-4). Dagster 의 재시도가 아니다 | 지은 것을 버리지 않는다 |
| 관문의 거부(타일의 막는 검사가 통과하지 않음) | 아무도. 일은 0 으로 끝나지만 Dagster 는 그 스텝과 **run 을 실패로** 적는다(`DagsterAssetCheckFailedError`). `RetryPolicy` 는 그 스텝을 다시 돌리지 않고, 같은 run 의 승격은 돌지 않으며, 다음 틱들도 요청하지 않는다(ADR-0618 의 1.13.27 실험, 프로세스 안에서). 그래서 run 실패 알림이 거부에도 울린다: 알림은 거부와 고장을 검사 이름으로 가른다(§6) | 다시 해도 같다 |

### 5. 메모리와 풀

- 메모리는 태스크의 크기다. 한 작업이 넘으면 그 태스크만 죽는다. 호스트 예산·메모리 claim·`every-container-has-a-memory-cap` 의 작업 부분은 없다.
- 풀은 남의 한계에만 쓴다. `spark 1`·`duckdb 2` 같은 "기계가 작아서"의 풀은 없다. 지금 아는 풀은 셋뿐이다: 쿼터가 있거나 겹치면 안 되는 수집기마다 칸 1(§8 의 표), 타일의 `release` 칸 1(ADR-0618), 그리고 원천 제공자가 동시 접속을 세는 경우 그 제공자마다. Gold 레인의 커밋 충돌에는 풀을 두지 않는다: 일이 낙관적으로 다시 커밋한다(ADR-0615 2-4).
- 그 결과 동시에 도는 태스크 수에 위가 없다. 돈의 위는 계정의 Fargate vCPU 할당량과 줄의 `max_concurrent_runs` 로 건다(값은 비용 승인과 함께).

### 6. 보고·신선도·경보

- 일은 Pipes 로 보고한다: 구체화(파티션·`data_version`)와 검사. Pipes 에는 관측을 보고하는 메시지가 없다(1.13.27 `PipesContext`:
  `report_asset_materialization`·`report_asset_check`·`report_custom_message`·`log`).
- **"바뀐 것 없음"은 구체화가 아니다.** 구체화는 `data_version` 이 같아도 하류의 `eager` 를 일으킨다(ADR-0615 의 1.13.27 실험, 문서도 같다).
  그래서 새로 쓴 것이 없는 run 은 구체화를 내지 않고, "제 차례에 읽었고 새것이 없었다"를 **통과한 자산 검사**로 낸다(`report_asset_check`).
  하류는 움직이지 않고, 차례가 왔다는 사실은 남는다.
- 예외 둘. 첫째(ADR-0615): 표에는 이미 지어져 있는데 Dagster 가 모르는 파티션(앞 run 이 커밋 뒤 실패, 운영자의 재실행)은 구체화로 다시 보고한다.
  그때 하류가 한 번 돈다. 하류도 표가 정본이라 할 일 없이 끝난다. 이 연쇄는 받아들인다.
  둘째(ADR-0618): 타일의 접두 파티션은 늘 구체화한다. 행이 없는 접두도 "행 없음" 곁 파일과 함께 구체화해 합치는 자산의 `eager` 를 막지 않는다.
- `foundation-job-outcome` 줄과 결과 원장은 AWS 에 없다.
- 신선도는 둘로 본다. `expected_outcome` 의 수 49개는 AWS 에 없다.
  - "제 차례를 했는가"를 읽는 곳은 한 군데다: Dagster 가 지닌 그 검사의 평가 기록. 표의 머리(ADR-0615)나 파티션 상태(ADR-0618)는
    "무엇이 지어져 있나"이고 이 물음의 답이 아니다. 그 검사가 제 박자 안에 평가됐는가를 본다. 검사의 평가는 구체화가 아니므로 `build_last_update_freshness_checks` 가 읽지 못한다.
    이 줄을 Dagster 의 무엇으로 읽는지는 열린 질문 4 에 더한다(검사 결과의 나이를 읽는 길: Dagster 의 자산 건강 상태, 또는 run 실패 센서로 충분한가).
  - "데이터가 낡지 않았는가": `build_last_update_freshness_checks(deadline_cron=…, lower_bound_delta=…)`. 원천이 실제로 바뀌는 박자를 아는 자산에만 건다.
- **Dagster 자신**은 Dagster 가 지키지 못한다: ECS 서비스의 건강 검사가 태스크를 다시 띄우고, 서비스가 원하는 수만큼 돌지 않으면
  CloudWatch 경보가 Slack 으로 간다. 이것은 처음부터 CloudWatch 의 일이다.
- 경보: Dagster OSS 에는 경보 정책이 없다(**확인 필요**). run 실패·검사 실패·신선도 넘김을 Slack 으로 보내는 길은 Dagster 의 센서
  (`dagster-slack` 의 run 실패 센서 등)를 먼저 본다. 그것으로 안 되는 것만 CloudWatch 경보로 한다. Prometheus·Alertmanager 를 AWS 에 세우지 않는다(열린 질문 4).

### 7. 옮기는 순서 — 수집은 멈추지 않는다

서빙 최신은 지금 목표가 아니다. 그래서 순서는 상류부터다(ADR-0583 의 "하류부터"와 반대: 그때는 서빙을 지켜야 했다).

| 걸음 | 하는 일 | ai-server 에서 끄는 것 |
|---|---|---|
| A0 | 사장이 항목별 비용을 본다. 계정·VPC·ECR·RDS·ECS 를 코드(IaC)로 정의한다. 아무것도 만들지 않은 채 검토 | — |
| A1 | Dagster 를 AWS 에 세운다. 작업 없음. 배포 = 이미지 digest | — |
| A1b | **Bronze 원장의 자리를 RDS 에 만든다(빈 채로).** 원장(`catalog.bronze_object`·`ingestion_run`)은 원천마다의 줄이다. 한 원천의 줄은 그 수집기와 함께 넘어간다(넘기는 규칙의 "상태를 옮긴다"에 든다). 그래서 어느 때든 한 원천의 원장은 한 곳이고, ai-server 와 RDS 사이에 망을 잇지 않으며, 수집이 멈추는 것은 넘기는 그 원천의 한 박자뿐이다 | — |
| A2 | 수집 한 개를 AWS 에서 돌린다: 쿼터도 없고 run 사이 상태도 없는 원천. 아래 "넘기는 규칙"의 겹쳐 돌리기. 경매처럼 주소에 민감한 원천은 그 전에 AWS 주소에서 잰다(§8 J) | 그 작업 하나 |
| A3 | 나머지 수집 14개, 하나씩, "넘기는 규칙"대로 | 수집 묶음, 그 유닛 |
| A4 | Silver 적재(DuckDB)를 자산으로. 표 43개의 분류가 여기서 필요하다 | Silver 묶음 |
| A5 | Gold 레인(ADR-0615 의 파티션 자산) | Gold 묶음, 조정자 |
| A6 | 굽기·서빙(이 ADR 밖: API·타일·서빙 저장소의 AWS 설계가 따로 있다) | 남은 것, 호스트 |

**넘기는 규칙(수집기 하나를 AWS 로).** 원천의 성질이 둘 중 무엇인지는 작업 정의에 적는다.

| 원천 | 넘기는 법 | 증거 | 되돌리기 |
|---|---|---|---|
| 쿼터 없음 · 상태 없음 | 사흘 겹쳐 돌린다(양쪽이 같은 객체를 쓰면 내용 주소라 한 벌이다) | 사흘 동안 AWS 쪽 run 의 검사 통과 | AWS 쪽을 끈다 |
| 쿼터 있음, 또는 run 사이 상태 있음(온비드의 호출 예산, data.go.kr 의 `accepted.json`) | **겹치지 않는다.** 한 번의 인계: ① ai-server 에서 끈다(정의의 `enabled: false`, 배포) ② 그 상태를 정본의 자리로 옮긴다(§8 C) ③ AWS 에서 켠다. ①과 ③ 사이에 그 원천은 돌지 않는다(한 박자) | AWS 쪽 첫 run 의 검사 통과와, 상태를 이어받았다는 표시(다시 받은 양 0) | 역순: AWS 에서 끄고, 상태를 되돌리고, ai-server 에서 켠다 |

**A2 와 A4 사이에 생기는 틈(받아들인다).** AWS 로 넘어간 수집기의 Bronze 는 R2 와 RDS 의 원장에 내리지만, ai-server 의 Silver 는 그 수집기의
구체화 사건을 보지 못한다(사건은 AWS 쪽 Dagster 에 있다). 그래서 그 원천의 Silver·Gold 는 A4·A5 까지 따라오지 않는다. 서빙 최신은 지금 목표가
아니므로 다리를 놓지 않는다. 그 대신 넘긴 작업과 그 하류의 ai-server 쪽 기대 결과 알림은 넘기는 PR 이 함께 끈다(정의에서 그 작업을 끄면 알림도 사라진다).

ai-server 에서 지금 꺼도 되는 것(서빙 최신이 목표가 아니므로): Gold 재빌드, by-PNU 굽기, 타일 굽기, 수집이 필요로 하지 않는 유지보수.
끄는 것은 정의의 `enabled: false` 한 줄씩이고, 무엇을 끌지는 사장·코디네이터가 정한다. A3 가 끝날 때까지 ai-server 에 남아야 하는 것:
수집 15개와 그것을 시작하는 Dagster, 발행기, R2 쓰기 키.

### 8. 호스트에 기대던 것, 종류마다

작업자들이 레인마다 찾은 호스트 가정을 종류로 묶었다(`orca-coord/aws-design-inputs-20261124.md`, A~J). 종류마다 AWS 에서의 자리는 하나다.

| | 지금 호스트에 기대는 것 | AWS 에서 |
|---|---|---|
| A | 릴리스 디렉터리에서만 도는 스크립트(`admitted-writer-runtime.sh`), 실행 때 릴리스 트리에서 읽는 계약 파일, 릴리스 파일로 싣는 DuckDB 확장 | 전부 이미지 안에 있다. 셸 드라이버 층(`gold-versions-build.sh`, `job-journal.sh`, `duckdb-stage-orphans.sh` 등)은 얇은 이미지 진입점 하나가 된다 |
| B | `/etc/foundation-platform/*.env`(입력 목록의 일곱과 `parcel-publication.env`) | 일의 진입점이 제 태스크 역할로 읽는 비밀(§2-8). 명령이 읽는 비밀의 목록은 바이너리의 `describe-requirements` 에서 생성하고, 그 역할이 못 읽는 것이 있으면 이미지를 올리는 단계가 거부한다 |
| C | run 사이에 남는 로컬 상태(온비드 `state.json`, data.go.kr `accepted.json`, Gold 판의 `resume/`·판정 파일, 굽기의 `join-next.json`·파티션 캐시, 품질의 `first-checked.json`, 멈춤 스위치 디렉터리) | 태스크의 디스크는 run 과 함께 사라진다. 그래서 상태는 셋 중 하나로 간다: (가) 이미 정본에 있는 것은 거기서 읽는다(Iceberg 커밋 요약의 핀·버전, Bronze 원장) (나) Dagster 가 지닌다(파티션 상태, 구체화 메타데이터, 센서 커서) (다) 그래도 남는 것은 R2 의 객체 하나다(온비드의 호출 예산·열린 물건). 종류 (다)는 레인마다 이름으로 적고, 없애는 쪽이 기본이다 |
| D | 서빙 중인 필지 판이 호스트 파일이다(승격 원장, `readiness.json`). 여러 레인이 읽는다 | 카탈로그의 사실이 된다: 그 Gold 스냅숏의 태그(`served-…`)와 Dagster 구체화. 파일은 없다 |
| E | 작업 디스크·스필(DuckDB 단계 32 GB 예약, 스풀 16.7 GB, source-sweep 스풀 수십 GB, 타일 굽기는 접두마다 25.0~27.3 GB·나누지 않으면 43.7 GB: `config/tile-bake-containers.contract.json`) | 태스크의 임시 디스크. 크기 등급의 `ecs/ephemeral_storage`(20~200 GiB). 호스트 SSD 를 Postgres 와 나누던 예약 수는 없다. 지금 아는 일은 모두 200 GiB 안이다 |
| F | 메모리 크기를 유닛의 `MemoryMax` 에서 읽는다 | 태스크의 메모리가 cgroup 한도다. 읽는 길은 그대로이고 몫만 태스크 크기 기준으로 |
| G | 수집기가 Postgres 의 Bronze 원장을 쓴다(`catalog.bronze_object`, `ingestion_run`). Silver 적재 계획이 그것을 읽는다 | **Postgres 를 둔다**: Dagster 의 RDS 인스턴스에 데이터베이스 하나를 더 둔다. 원장을 Iceberg 나 Dagster 파티션으로 옮기는 일은 이 ADR 이 하지 않는다(읽는 쪽이 여럿이다) |
| H | 호스트의 도구: GDAL 을 `docker run`, 호스트에서 지은 tippecanoe 이미지, 오래 떠 있는 Spark 컨테이너 | 도구는 이미지다(ECR). 도구가 다른 일은 코드 위치가 다르다(§2-2). 호스트에서 짓는 이미지는 없다 |
| I | 보고: `foundation-job-outcome` 줄, 저널을 Pipes 로, node-exporter 텍스트 파일, 유닛의 `OnFailure` → Slack | Pipes 메시지와 Dagster 의 사건(§6). 유닛이 없으므로 `OnFailure` 도 없다 |
| J | **망**: 경매 수집기는 사무실의 한국 주소에서 브라우저처럼 묻는다. 사이트는 싫은 세션을 막는다. AWS 주소에서는 재 본 적이 없다 | 아래 문단 |

**J — 모든 수집은 AWS 에서 돈다(사장 결정 2026-11-24). 경매도 그렇다.** 사무실 호스트를 일의 자리로 두지 않는다(ECS Anywhere 를 쓰지 않는다).
레인은 지금 모양 그대로가 아니라 새 모양으로 옮긴다. A2 전에 AWS 서울 주소에서 읽기 전용 목록 검색 한 번으로 잰다(자원을 만드는 시험이라 사장의 승인 뒤).
제공자가 AWS 주소를 막으면 **돌아가지 않는다**: 사장에게 알리고 사장이 정한다.

**우리 계층이 해 주던 것 여덟(W93b)의 자리.**

| 지금 계층이 해 주는 것 | AWS 에서 |
|---|---|
| 한 작업은 한 번에 하나, 복사본이 줄에 서지 않는다(11-23: 줄의 93 중 74 가 복사본) | 선언형 자동화는 진행 중인 run 을 기다린다(`in_progress`). 예약으로 도는 job 은 그 job 의 풀 칸 1 이 겹침을 막는다(쿼터 있는 수집기에 필요) |
| 놓친 예약은 따라잡는다(media_reconcile 43 h) | 조건은 상태라 놓침이 없다. `ScheduleDefinition` 으로 도는 원천은 놓친 틱을 다시 돌지 않는다: 다음 틱이 같은 일을 한다. 그것으로 안 되는 원천은 자산 + `on_cron` 으로 둔다 |
| 나쁜 것 하나가 run 전체를 실패시키지 않는다 | 파티션(밤·시도·접두)마다의 상태. run 안에서 깨진 본문을 다시 받는 것은 수집기의 코드로 남는다 |
| 명령은 제가 읽는 비밀만 받는다 | §2-8 |
| 꺼진 작업은 아무것도 남기지 않는다 | 정의에 없으면 자산·예약·검사가 없다. 알릴 유닛도 없다 |
| 생산자 없는 레인은 서빙되지 않는다 | 서빙 자산은 제 생산 자산이 있을 때만 정의된다(공장이 거부한다). 신선도는 자산 검사다 |
| 배포가 작업을 붙잡고 비우고 다시 시작한다 | §2-5. 빚진 run 의 장부는 없다 |
| 작업마다 메모리·시간 한도 | 크기 등급(§2-9)과 run 의 최대 실행 시간(run monitoring 의 `max_runtime`) |

**기준 레인(W71c 의 경매 레인).** 이 표의 모양을 처음 따르는 레인이다: Bronze 는 원천 슬러그마다 자산, 밤마다의 시간 파티션, 수집기는 `@multi_asset` 하나;
Silver 28표는 `@multi_asset` 하나; Gold 는 자산 하나. 수집은 cron 예약, Silver·Gold 는 `eager()`. 호스트 상태 없음. 검사는 일이 Pipes 로 낸다.

### 9. 남는 우리 것

| 남는 것 | 맡지 못한 기능과 이유 |
|---|---|
| 작업 정의와 공장 | 접착제 |
| IaC(VPC·ECS·RDS·IAM·ECR) | Dagster 는 자기가 놓일 곳을 만들지 않는다 |
| 서명된 digest 만 올리는 릴리스 빌드 | Dagster·ECS 는 이미지를 검증하지 않는다 |
| 마이그레이션 동안 작업을 붙잡는 호출 묶음 | "이 자산들만 멈춤"이 없다 |
| 발행기가 Pipes 메시지를 내는 코드 | 프로토콜은 JSON 줄이다. 유지되는 Rust 클라이언트가 있으면 그것을 쓴다(**확인 필요**) |
| `code_version` 을 digest 에서 내는 한 줄 | `code_version` 은 값을 받을 뿐이다 |

### 10. 비용(가장 작은 시작)

단가는 AWS 가격 목록의 서울 리전 파일에서 읽었다(`pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<Offer>/current/ap-northeast-2/index.json`,
2026-11-11~23 게시). 양은 운영에서 쟀다(유닛의 7일 저널을 릴리스의 `job-run-times.py` 짝짓기로 더함: 수집 77.8 h·201.5 GiB·h, 모든 작업 360.8 h·1,205.6 GiB·h; cAdvisor 의 수집 CPU 27.5 코어·시간; Bronze 원장의 30일 548 GB).

**단가(서울, 온디맨드; 세 ADR 의 모든 비용 줄은 양 × 이 단가다).**

| 것 | USD | 가격 목록 파일(게시일) |
|---|---|---|
| Fargate Linux x86 vCPU·시간 / GB·시간 | 0.04656 / 0.00511 | AmazonECS(11-11) |
| Fargate ARM vCPU·시간 / GB·시간 | 0.03725 / 0.00409 | AmazonECS(11-11) |
| Fargate 임시 디스크, 20 GB 초과분 GB·시간 | 0.000127 | AmazonECS(11-11) |
| 공인 IPv4 시간 | 0.005 | AmazonVPC(11-23) |
| RDS PostgreSQL 단일 AZ db.t4g.micro / db.t4g.small 시간 | 0.025 / 0.051 | AmazonRDS(11-20) |
| RDS gp3 GB·월 | 0.131 | AmazonRDS(11-20) |
| 인터넷으로 송신 GB(첫 10 TB) | 0.126 | AWSDataTransfer(11-19) |
| CloudWatch Logs 수집 GB / 보관 GB·월 | 0.76 / 0.0314 | AmazonCloudWatch(11-19) |
| ECR 보관 GB·월 | 0.10 | AmazonECR(11-13) |
| Secrets Manager 비밀·월 | 0.40 | AWSSecretsManager(11-11) |

**수집만(A0~A3).**

| 줄 | 월(USD) | 근거 |
|---|---|---|
| Dagster 상시 태스크 0.5 vCPU / 2 GB | 24.5 | 메모리는 잼(웹서버·데몬 687 MiB; run 을 품지 않는 코드 서버는 재지 않았다). CPU 는 가정(1 vCPU 면 41.4) |
| RDS db.t4g.micro + 20 GB | 20.9 | Dagster DB 1,001 MB. **Bronze 원장의 크기는 재지 않았다**(A2 전에 잰다). 20 GB 를 넘으면 GB 마다 0.131 |
| 수집 태스크(온비드 4 vCPU / 8 GB 113 h, 나머지 14개 0.5 vCPU / 2 GB 220 h) | 33.0 | 시간·CPU 잼 |
| 공인 IPv4(NAT 없음) | 5.3 | — |
| R2 로의 송신(Bronze) | 11.3(평월 90 GB) / 69(일괄 수집이 있던 지난달 548 GB) | 원장에서 잼. 무료 100 GB 는 우리 것이 아니라고 가정 |
| ECR 약 10 GB | 1.0 | 가정 |
| CloudWatch Logs 5 GB | 4.0 | 가정(재지 않음) |
| Secrets Manager 8개 | 3.2 | env 파일 수(입력 목록의 일곱과 `parcel-publication.env`) |
| **합(수집만)** | **103.2(평월) / 160.9(일괄 수집 달)** | |

NAT 게이트웨이와 ALB 는 설계에 없다.

**모든 작업을 옮긴 뒤 더해지는 것**(Spark 와 타일 제외 — 타일은 아래 표; 같은 7일 저널 × 30/7):

| 줄 | 월(USD) | 근거 |
|---|---|---|
| default 풀의 나머지 작업(조정자 제외) 351 h, 0.5 vCPU; `media_ladder_backlog` 만 2 vCPU | 25.8 | 시간 잼. 그 작업의 CPU 는 잼(7일 52 코어·시간) |
| duckdb 작업 324 h · 2,812 GiB·h, 4 vCPU | 74.7 | 시간·메모리 잼. **CPU 는 가정**(작업별로 재지 못했다: 일이 컨테이너 안에서 돈다) |
| 그 태스크들의 공인 IPv4 | 3.4 | — |
| R2 로의 송신(Silver·Gold 월 195 GB) | 24.6 | 스냅숏 요약 7일 45.5 GB |
| 로그 5 GB 더 | 3.8 | 가정 |
| **더해지는 합** | **132.3** | |
| **수집 + 모든 작업** | **235.5(평월)** | |

Spark 작업은 옮기지 않는다(§2-2). 지금 그대로 옮긴다면 426 h 를 4 vCPU / 20 GB 로 약 123 을 더한다(크기는 가정).
**타일 굽기**(양은 ADR-0618 §8 의 것, 단가는 이 표의 것; 필지 전량 굽기 한 번):

| 줄 | 한 번(USD) | 근거 |
|---|---|---|
| 파티션 태스크 14.5 vCPU·h | 0.68 | 태스크 217 분은 가정(z16 에서 169 분을 잼), 4 vCPU 가정 |
| 메모리 29~58 GB·h | 0.15 ~ 0.30 | 8~16 GB 가정(최대 5.42 GiB 를 잼) |
| 임시 디스크 30 GB(20 GB 초과분 10 GB × 3.6 h) | 0.005 | 25~27 GB 를 잼 |
| R2 로 올리기 16.4 GiB | 2.07 | 세대 하나의 크기를 잼 |
| 합치는 태스크 | 재지 않음 | 시간을 재지 않았다(ADR-0618) |
| **한 번의 합** | **약 2.9 ~ 3.1 + 합치는 태스크** | |

필지 판은 한 달에 한 번 바뀌므로 평월에 한 번이다. 이 범위는 작업 계산만이다(API·서빙 저장소 제외).

**아직 값이 없는 줄.** 파티션 레인의 관측 run(틱마다, 가장 작은 등급), 타일의 합치는 태스크와 굽기 횟수, 타일의 접두별 입력 읽기(ADR-0618 열린 질문 3), Bronze 원장의 디스크, duckdb 작업의 실제 CPU, 타일 태스크의 공인 IPv4, `tiles` 코드 위치의 이미지 보관, 옛 타일 세대의 R2 보관, "이미 맞는 시도"를 다시 보고할 때 하류가 한 번 도는 run, 원천마다 원장 줄을 옮기는 일, CloudWatch 경보, AWS 주소에서의 경매 시험 한 번.
그래서 사장에게 묻는 것은 둘로 나뉜다: **지금은 A0~A3(수집, 평월 103.2)**, A4~A6 는 위 줄들을 잰 뒤 다시 항목별로.
재지 못하는 양에는 위를 건다: 계정의 Fargate vCPU 할당량, 줄의 `max_concurrent_runs`, run 의 최대 실행 시간.

## 열린 질문

| # | 질문 | 누가 | 언제 |
|---|---|---|---|
| 1 | 표 43개: 표마다 쓰는 자산을 어디로 | W82c | A4 전 |
| 2 | Spark 작업 22개 중 DuckDB 로 새로 짓지 않을 것이 있는가. 있으면 그것은 끄는가 | Gold·Silver 담당, 사장·코디네이터 | A4 전 |
| 3 | (닫음) 200 GiB 를 넘는 작업 디스크: 지금 아는 일에는 없다(타일 계약 25~44 GB) | — | — |
| 4 | 경보: Dagster 센서 + Slack 으로 되는 것과 CloudWatch 가 필요한 것. "제 차례를 했는가"(검사 평가의 나이)를 Dagster 의 무엇으로 읽나 | W82c | A1 |
| 5 | (닫음, 1.13.27 소스) `ecs/*` 태그는 있다; run 재시도는 간격이 없다(§4); `FreshnessPolicy` 는 관측을 세지 않는다(§6) | — | — |
| 6 | 사설 서브넷 + NAT 이 필요한가(보안 요구) | 사장·코디네이터 | A0 |
| 7 | 한 이미지(Dagster + 발행기)의 빌드와 서명: 지금 릴리스 빌드를 어떻게 줄이나 | W82c | A1 |
| 8 | (없앰) 사무실 자리는 두지 않는다(사장 결정) | — | — |
| 9 | (닫음, 2026-11-24 22:01Z) 경매 사이트가 AWS 서울 주소를 막는가: 막지 않는다. 플랫폼 계정 091109217099 의 Fargate 태스크 하나(공인 IP 3.34.138.177)에서 `check-courtauction-go-kr-reach` 가 목록 조회 한 번에 HTTP 200, 50 행(총 22,062), 0.37 초로 답을 받았다. 만든 자원은 모두 지웠다 | — | — |
| 10 | 종류 C 의 (다): R2 객체로 남길 상태를 레인마다 이름으로. **수집기의 것(온비드 `state.json`, data.go.kr `accepted.json`·`past.json`, 실거래 수집기의 상태)은 A2 전에** | 레인 담당 | 수집기: A2 전. 나머지: 그 레인을 옮기기 전 |
| 11 | (닫음, ADR-0615 의 실험) 같은 `data_version` 의 구체화도 하류의 `eager` 를 일으킨다. 그래서 "바뀐 것 없음"은 통과한 검사로 낸다(§6) | — | — |
| 12 | `code_version` 을 이미지 digest 로 두면 배포마다 모든 자산이 다시 돈다. 자산마다의 버전을 무엇에서 내나(지금의 `job_code_version.py` 는 그 작업이 읽는 파일의 해시다) | W82c | A4 전 |

## Consequences

- ai-server 의 작업 계층(약 9,800 줄), 유닛 98 파일, 손으로 적은 수 대부분, 배포 스크립트의 작업 부분은 옮기지 않고 끈다. Airflow 삭제의 남은 PR 도 쓰지 않는다.
- Dagster 가 태스크를 띄우는 권한은 IAM 역할 하나의 범위다. Docker 소켓 문제는 생기지 않는다(모든 일이 Fargate 에서 돈다).
- 크기 등급을 새로 정해야 한다(지금은 작업마다 메모리만 선언한다). 등급의 수와 경계는 잰 값에서 낸다.
- R2 가 AWS 밖에 있어 쓰기마다 송신 요금이 붙는다. 양을 재기 전에는 범위로만 안다.
- 옮기는 동안 수집은 한 작업씩 넘어간다. 쿼터나 상태가 있는 원천은 양쪽에서 겹쳐 돌지 않는다(§7 의 넘기는 규칙).

## 기각한 대안

- **ai-server 에서 컨테이너 시작 경로를 먼저 짓는다.** 첫 초안. Docker 소켓 = root 이고, 지은 것을 AWS 에서 버린다.
- **ai-server 에서 자산·자동화만 먼저 한다.** 둘째 초안. 사장 결정으로 그 계층을 고치지 않는다.
- **EKS + `K8sRunLauncher`.** 운영할 것이 한 겹 는다. ECS 가 같은 일을 한다.
- **EC2 한 대에 지금 구성을 그대로.** 우리 계층이 그대로 따라간다.
- **일마다 `PipesECSClient` 로 둘째 태스크.** run 태스크와 일 태스크가 둘씩 뜬다. 도구가 다른 일은 코드 위치를 따로 두면 된다(§2-2).
- **수집을 사무실 호스트에 남긴다(ECS Anywhere).** 넷째 초안. 사장 결정: 모든 것이 AWS 에서 돌고, 제공자가 막으면 돌아가지 않고 알린다.
- **Dagster+ (관리형).** 사장: 오픈소스. 경보 정책 하나를 위해 바꾸지 않는다.

| 구분 | 내용 |
|---|---|
| 기본 기능 | `EcsRunLauncher`, `ecs/*` 태그, `PipesSubprocessClient`, 자산·파티션, `ScheduleDefinition`, `AutomationCondition` 과 그 센서의 `run_tags`, `RetryPolicy`, run monitoring 과 run 재시도, last-update 신선도 검사, 자산 검사; ECS·IAM·RDS·ECR·Secrets Manager·CloudWatch |
| 임시 | 옮기는 동안 ai-server 에 남는 수집과 그 계층 |
| 운영 | 정의 → 공장, IaC, 서명된 digest 릴리스, 발행기의 Pipes 메시지 |

2026-11-25: 구현 점검 — Proposed 유지. main 에 있는 것: AWS 계정의 바탕(`platforms/foundation-platform/infra/aws/main.tf`, ADR-0619)과
기준 레인(`platforms/foundation-platform/orchestration/dagster/court_auction_lane.py`, 그 시험
`platforms/foundation-platform/orchestration/tests/test_dagster_court_auction_lane.py`). 없는 것: 열린 질문 1·2·4·6·7·9·10·12 의 답,
§10 에서 값이 없는 줄(A4~A6 의 비용).
