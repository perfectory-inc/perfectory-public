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
- 고치는 곳: [ADR-0622](./0622-duckdb-does-every-read-transform-staged-file-and-its-own-memory-and-our-code-does-only-the-commit-the-domain-and-what-duckdb-cannot.md)
  §2 의 "스레드" 줄(정하지 않는다 → `DuckDB` 의 수와 한도 1 GB 마다 하나 중 작은 것, §3-1, 2026-11-27).
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
| Dagster 웹서버, 데몬 | 저마다 ECS(Fargate) 서비스 하나, 항상 켬(코디네이터 결정 2026-11-27, Dagster 의 Docker·Helm 배치의 모양). 웹서버는 여럿이 돌 수 있어 새 태스크가 뜬 뒤 옛 것이 멈추고, 데몬은 하나뿐이라 옛 것이 멈춘 뒤 새 것이 뜬다. 이미지는 Dagster 의 것(`Dockerfile.aws-dagster`, 위치의 코드 없음): 그 입력이 바뀔 때만 바뀐다 | 각 0.25 vCPU / 0.5 GB(계약의 `dagster.services`). 잼: 2026-11-27 AWS 에서 데몬 최고 264 MiB·0.085 vCPU, 웹서버 247 MiB·0.018 vCPU; ai-server 71 작업에서 데몬 299 MiB·중앙 0.15 코어. 최고점 × 1.5 < 512 MiB(ADR-0574) |
| 코드 위치 | 위치마다 ECS 서비스 하나, gRPC 코드 서버(`dagster code-server`), 위치의 이미지. 웹서버·데몬은 사설 DNS 이름(Cloud Map)으로 부른다(코디네이터 결정 2026-11-26, Dagster 의 ECS 배치 안내의 모양) | 0.25 vCPU / 0.5 GB. 잼: 2026-11-26~27 CloudWatch 최고 242 MiB, 평균 0.05 vCPU. 위치에 레인이 들면 다시 잰다 |
| Dagster 의 저장소 | RDS PostgreSQL, 단일 AZ | 지금 1,001 MB |
| run | `EcsRunLauncher`: run 마다 태스크 | — |
| 이미지 | GHCR 의 공개 패키지, digest 로 참조(ADR-0619 §7) | 발행기 293 MB, Dagster 554 MB |
| 비밀(R2 키, 원천 API 키) | 레이크하우스 자격은 발급 창구가 레인마다 준다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §2). 원천 API 키는 Secrets Manager 에서 레인의 역할로 읽는다. 태스크 정의에는 싣지 않는다 | 지금 `/etc/foundation-platform/*.env` 여덟 |
| 로그 | CloudWatch Logs | — |
| 레이크하우스 | **R2 그대로**(Iceberg 카탈로그도) | AWS → R2 쓰기는 AWS 의 인터넷 송신 요금, R2 → AWS 읽기는 양쪽 무료 |
| UI 접근 | SSM 포트 포워딩. 공개 주소 없음 | — |

계정은 조직의 platform 계정이다(SSO 프로필 `perfectory-platform-prod-admin`, ADR-0619 §3). 공짱 운영 계정과 286958704196 은 쓰지 않는다.
그 계정과 리전은 ADR-0619 가 `platforms/foundation-platform/config/aws-platform.contract.json` 의 `account`·`region` 에 고정한다. 이 ADR 은 id 를 다시 적지 않고 그것을 따른다.

### 2. 일을 시작하는 길

1. **기본: run 태스크가 일을 한다.** 코드 위치 이미지에 그 릴리스의 발행기 바이너리와 DuckDB 확장을 함께 넣는다. 자산은
   `PipesSubprocessClient` 로 발행기를 부른다. 태스크는 run 마다 하나이고, 크기는 작업 정의에서 나온 `ecs/cpu`·`ecs/memory`
   (디스크가 필요하면 `ecs/ephemeral_storage`) 태그다. 유닛 파일·드롭인·메모리 claim 이 하던 일이다.
2. **이미지가 다른 일은 코드 위치가 다르다.** 실행기는 run 의 태스크를 그 작업이 속한 코드 위치의 이미지로 띄운다(코드 서버가 제 이미지를
   알린다: `DAGSTER_CURRENT_IMAGE`). 그래서 도구가 다른 일은 코드 위치를 따로 둔다: `data`(발행기)와 `tiles`(발행기 + GDAL + tippecanoe,
   ADR-0618). 위치마다 제 ECS 서비스의 코드 서버이고(`infra/aws/code-servers.tf`, 계약의 `dagster.code_locations`), 코드 릴리스는 그 코드
   서버만 다시 띄운다(새 것이 뜬 뒤 옛 것이 멈춘다). 웹서버·데몬은 그대로 돌아 예약의 틱을 놓치지 않는다. 이쪽도 run 태스크가 일을 하고 `ecs/*` 태그가 그대로 든다.
   둘째 태스크(`PipesECSClient`)는 쓰지 않는다. **Spark 작업은 목표에 없다**: AWS 로 가는 것은 DuckDB 로 새로 지은 레인뿐이다.
   ai-server 의 Spark 작업은 그 레인이 넘어가면 꺼지고, 넘어갈 레인이 없는 것은 사장·코디네이터가 끄라고 할 때 꺼진다(열린 질문 2).
   **예외는 하나, 표 유지보수다**(사장 결정 2026-11-27): Iceberg 의 표준 프로시저(`expire_snapshots`·`remove_orphan_files`·`rewrite_data_files`)를
   상류 그대로의 Spark 컨테이너 하나가 돌리고, Dagster 가 AWS 에서 필요할 때만 그 태스크를 띄운다. 그 컨테이너에는 우리 도메인 규칙이 없다.
   우리 코드가 정하는 것은 표 속성과 태그뿐이다. Lakekeeper Plus(유료판의 유지보수)는 쓰지 않는다. 근거와 장부의 끝(1)은
   [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)
   의 2026-11-27 개정 메모에 있다.
3. **권한은 IAM 이다.** Dagster 의 태스크 역할은 `ecs:RunTask`(run 의 태스크 정의 계열만)와 그 정의의 등록, `iam:PassRole`(계약의 run 경로
   `ecs.run_role_path` 아래 역할만, 제 역할은 거기 없다), 실패한 run 의 로그 읽기(run 계열의 줄만), ECS Exec 를 갖는다. 어느 run 도 이 역할을 갖지 않는다(8).
   일의 역할은 레인마다 하나이고 레이크하우스 자격은 발급 창구로만 받는다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §2). 강제 명령·SSH 키·`start-scheduled-job.sh`·호스트 허가가 하던 일이다.
4. **릴리스 = 이미지 digest.** 릴리스 디렉터리와 `admitted-writer-runtime.sh` 가 하던 확인은 "서명을 확인한 digest 만 쓴다"가 대신한다.
   릴리스 워크플로의 `aws-images` 잡이 커밋마다 모든 이미지의 digest 를 기록(`aws-images.json`)에 적어 이미지와 함께 서명하고, 기록을
   `<commit>-aws-images` 로 올린다(ADR-0620). 적용의 입력은 커밋 하나다: `scripts/deploy/aws-release-images.py` 가 그 기록과 기록의 모든
   digest 를 계약(`images`)의 저장소·워크플로·ref 와 그 커밋(`gh attestation verify --source-digest`)에 대해 확인한 뒤에만 digest 를 적고,
   하나라도 아니면 멈춘다. 스택은 계약의 저장소 밖 이미지를 거부한다. 배포는 태스크 정의의 새 개정과 서비스 갱신, 코드 위치 다시 읽기다.
   스키마는 그 전에 도는 일회성 태스크 둘이 바꾼다(`infra/aws/foundation-database.tf`). 부트스트랩(master)은 역할·비밀번호·데이터베이스를
   만들고, 제 정의나 비밀번호의 판이 바뀔 때만 돈다. 마이그레이션 태스크는 한 태스크의 컨테이너가 차례로 돈다: open(master 가
   마이그레이터에게 CREATE 를 준다) → migrate(런타임 이미지의 `foundation-migrate`) → grants(마이그레이터) → finalize(master 가 CREATE 를
   거둔다). 앞 것이 0 으로 끝나야 다음이 시작하고(ECS `dependsOn` SUCCESS, 시작 시한 없음), 제 정의가 바뀔 때 돈다(런타임 이미지가 그
   안에 있다). 코드 위치 이미지만 바뀐 배포와 되돌리기는 어느 것도 돌리지 않는다. 적용은 태스크가 멈추고 모든 컨테이너가 0 으로
   끝나야 서비스를 옮긴다(`infra/aws/pre-deploy.sh`).
5. **마이그레이션은 앞 릴리스를 깨지 않는다(expand/contract).** 마이그레이션은 옛 서비스와 옛 run 이 도는 동안 적용되고(4), 되돌리기는
   코드만 되돌리고 스키마는 그대로 둔다. 그래서 마이그레이션은 바로 앞 릴리스의 코드가 그대로 도는 것만 한다: 표·칸·색인을 더한다
   (새 칸은 NULL 이 되거나 기본값을 갖는다). 지우기·이름 바꾸기·형 바꾸기·NOT NULL 걸기·트리거 지우기는 그것을 읽고 쓰는 코드가 모두
   떠난 뒤의 릴리스에서 따로 한다(contract 단계). 검사는 squawk 다: `scripts/ci/lint_changed.py` 가 변경이 더한 foundation 마이그레이션을
   `.squawk.toml` 의 규칙(앞 릴리스를 깨는 것; 잠금·문체 규칙은 뺀다)으로 읽고, 규칙 전의 마이그레이션은 줄기만 하는 기준선에 둔다.
   contract 단계의 문장은 그 위에 `-- squawk-ignore <규칙>` 과 이유(그것을 읽던 코드가 떠난 릴리스)를 적는다. 그래서 마이그레이션 동안
   작업을 붙잡는 장치는 두지 않는다. GitLab 의 배포 뒤 마이그레이션(post-deployment migration), Rails `strong_migrations` 의
   `safety_assured` 와 같은 모양이다(이 둘은 기억으로 적었다). 사고: 2026-10-14 #465 의 지우는 마이그레이션이 배포 중에 실패해 API 가
   약 30 분 멈췄다.
6. **Spot.** 다시 돌려도 되는 작업은 `ecs/run_task_kwargs` 로 Fargate Spot 을 쓴다. 중단은 태스크의 죽음이다(7).
7. **태스크가 죽으면.** run 태스크가 일을 하므로, Spot 중단이나 메모리 초과는 재시도 정책을 쥔 프로세스를 죽인다. 살리는 것은 Dagster 의
   run monitoring(실행기가 태스크의 건강을 묻는다: `launcher.py` `check_run_worker_health`)과 **죽은 run 에 한한** run 재시도다
   (ADR-0565 §3·§9 의 `retry_on_asset_or_op_failure: false` 그대로). 재시도의 표는 §4 에 하나 있다.
8. **작업마다의 역할과 비밀.** run 태스크는 서비스의 복제가 아니다. 실행기는 run 의 태스크 정의를 인스턴스 설정(`dagster.yaml` 의
   `run_launcher.task_definition`)으로 등록한다(dagster-aws 0.29.27: 그 매핑이 있으면 지금 태스크를 복제하지 않는다). 그 태스크 역할은 정책이
   없는 기본 run 역할이라, 레인이 제 역할을 주지 않은 run 은 아무것에도 닿지 않는다(닫힌 쪽으로 실패). run 이 가질 수 있는 역할(기본 역할,
   run 의 실행 역할, 레인마다의 역할)은 모두 계약의 run 경로 아래에 있다. 2026-11-26 까지는 복제였고, 그래서 모든 run 이 Dagster 의 역할
   (태스크 정의의 등록·시작·멈춤, 역할 넘기기, ECS Exec)을 가졌다(W107 검토). 그 위에서:
   - 역할: run 의 태그 `ecs/task_overrides` 가 `{"taskRoleArn": …}` 을 싣는다(`launcher.py:636`). 역할은 레인마다 하나다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §2-2).
   - 비밀: 태스크 정의에 싣지 않는다. 레이크하우스 자격은 발급 창구만 준다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §2). 원천의 비밀은 일의 진입점이 제 역할로
     Secrets Manager 에서 읽는다. Dagster 서비스의 역할은 어느 것도 읽지 못한다.
   - Dagster 서비스 자신의 DB 비밀번호는 일의 비밀이 아니다(코디네이터 결정 2026-11-25, A1). 서비스의 태스크 정의는 그 비밀의 ARN 만
     `secrets` 의 `valueFrom` 으로 든다. 값은 정의에도 상태 파일에도 없다. run 의 태스크 정의도 같은 ARN 을 든다(인스턴스 설정의 `secrets`):
     run 의 프로세스가 사건을 그 DB 에 쓰기 때문이다. 그것을 넣는 run 의 실행 역할은 이 비밀과 run 계열의 로그 줄만 쓴다.
   - 그래서 그 비밀은 RDS 의 master 사용자의 것이 아니다(최소 권한, 코디네이터 결정 2026-11-25). Dagster 는 제 데이터베이스만 가진
     역할로 붙고, 그 역할은 제 비밀을 갖는다. 비밀번호는 적용 때 만들어 write-only 로 쓴다. master 의 비밀은 그 역할을 만드는
     일회성 부트스트랩 태스크의 실행 역할만 읽는다(`infra/aws/database-bootstrap.sql`). `orchestration/tests/test_aws_dagster_secrets.py`
     가 다른 곳에서 master 비밀을 이름 대면 실패한다.
9. **크기를 run 에 싣는 길.** 선언형 자동화가 만든 run 의 태그는 그 자동화 센서의 `run_tags` 에서 온다
   (`AutomationConditionSensorDefinition(target=…, run_tags=…)`, 1.13.27 에서 확인). 그래서 크기는 작업마다의 수가 아니라 **크기 등급**이다:
   등급마다 센서 하나가 그 등급의 자산을 고르고 `ecs/cpu`·`ecs/memory`·`ecs/ephemeral_storage`·역할(8, 레인의 것)을 싣는다. 평범한 예약은 없다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1).
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

근거: 사장의 브레인스토밍 2026-11-25(EGM, 일곱 검토)와 그 S0 측정. 사장은 그 가운데 "가"(형식 지정 읽기)를 골랐다.

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

- 객체는 `DuckDB` 가 직접 읽는다: `read_json(…, format = 'newline_delimited', columns = {…})`. 봉투의 열(`request`, `http_status`,
  `read_outcome`, `body` 와 역할이 읽는 그 밖의 칸)은 JSON 으로 선언한다. 줄 번호는 `WITH ORDINALITY` 다: 빈 줄을 건너뛴 줄의 차례로,
  발행기가 지금 매기는 번호와 같다(시험함). JSON 이 아닌 줄, 객체가 아닌 줄은 객체를 거부한다(수집기의 고장을 소리 내어 알린다).
- 검색이 아닌 역할은 공통 읽기 하나를 쓴다: 그 읽기가 봉투를 작은 JSON(`j`)으로, `body` 를 한 번 푼 JSON 으로 내고, 역할의 SQL 은 그대로
  그 위에서 돈다. 값은 JSON 그대로라 레인의 값 규칙(`python_values.sql`, 예: `5` 와 `"5"` 를 가른다)이 지금과 같은 값을 받는다.
- 검색(물건 검색·매각결과 검색)은 제 읽기를 쓴다: 파일을 한 번 읽어 줄마다 165 KiB 의 `body` 를 `json_transform` 으로 한 번 풀어 물건
  목록과 쪽의 `totalCnt` 로 내린다. 물건마다 선언한 모양(읽는 칸만, 칸마다 JSON)이라 칸마다 물건을 다시 풀지 않고, 쪽은 같은 답에서 센다.
  매각공고도 제 읽기다(공고 목록·사건 수·세 목록만 푼다). 줄 전체나 `body` 전체를 표로 남기지 않는다.
- 읽을 때 줄을 바이트로 묶는 일(`duckdb_parquet_stage::line_parts` 의 묶음)은 없앤다. #1718 은 병합하지 않고 닫는다.

**메모리: 작은 등급, 넘으면 거부.** 경매 Silver 의 run 은 작은 등급으로 돈다. `DuckDB` 의 한도는 컴파일된 값(2,048 MiB)이 아니라 태스크의
메모리를 따른다. 모든 읽기(공통 읽기, 검색·매각공고의 제 읽기)는 객체를 읽기 전에 그 글의 크기 × 역할마다 잰 비율(풀린 바이트당
`DuckDB` 메모리, 계약의 `silver_load.memory_per_text_byte`)을 run 의 한도와 견주고, 넘으면
메모리 초과로 죽는 대신 그 이유를 적어 객체를 거부한다. 손으로 정한 객체 크기 상한은 없다.

**스레드는 메모리를 따른다(모든 `DuckDB`).** `DuckDB` 는 제 스레드 수를 CPU 로만 정한다(cgroup 의 쿼터, 없으면 호스트의 스레드). 발행기의
모든 `DuckDB` 는 제 한도 1 GB 마다 스레드 하나까지만, 적어도 하나로 돈다(`duckdb_parquet_stage::MEMORY_PER_THREAD`). `DuckDB` 2.0 의
'Environment' 안내(<https://duckdb.org/docs/2.0/guides/performance/environment>)의 "Aim for 1-4 GB memory per thread" 의 아래 끝이다.
2026-11-26 21:31Z 운영 run 은 CPU 쿼터가 없는 유닛에서 1.4 GiB 아래 20 스레드로 돌았다. 두 번 실패한 물건 검색 2026-11-22 는 통과해
커밋했고(물건 +974 행), 매각공고 2026-11-22(52 MiB)의 창(`QUALIFY`) 문장에서 메모리를 넘겼다. 노트북에서 그 순서 그대로(물건 검색, 물건상세
part0001, 매각공고) 파일 단계로 재현했다. 앞 객체의 표 169 MiB 는 흘려 보내졌고(spill) 원인이 아니었다.

**읽는 줄의 한도도 메모리다.** `read_json` 의 `maximum_object_size` 는 계약의 `silver_load.maximum_line_bytes` 하나다(16 MiB, `DuckDB` 의
기본값). JSON 읽기는 읽기마다 그 두 배의 버퍼를 든다(v2.0.0 `extension/json/json_scan.cpp`). 32 MiB 이던 때 매각공고는 한 스레드에서
1.4 GiB 를 넘었고, 16 MiB 에서 통과했다. 객체마다 가장 긴 줄은 물건상세 5.3 MiB 다. 역할마다 잰 비율은 이 모양에서 다시 잰다(아래 표):
한 스레드, 16 MiB, 파일에 둔 단계, 장소 참조를 올린 단계다.

**역할이 자라면.** 한 역할의 객체가 그 등급을 넘게 자라면(거부가 그 신호다) 그 역할은 선언한 모양으로 옮긴다: `body` 를 통째로 두지 않고
그 역할이 읽는 멤버만 `json_transform` 으로 한 번 풀어 잰 비율을 줄인다(검색과 매각공고가 지금 그 모양이다).

| 측정 (2026-11-25, 노트북은 `DuckDB` 2.0.0, 2 GiB, 2 스레드, 지금의 SQL 과 모든 표의 모든 행을 견줌) | 결과 |
|---|---|
| 물건 검색 2026-11-09 (394 MiB 의 줄, 2,435 줄), 통째로 지금의 SQL (AWS ARM, W101) | `DuckDB` 한도 8 GiB 와 16 GiB 사이에서 처음 통과. 16 GiB: 17 초, RSS 7.2 GB |
| 같은 객체, 형식 지정 읽기 (AWS ARM, W101) | 2 GiB 에서 통과: 1 초, RSS 324 MiB |
| 같은 객체, 검색의 형식 지정 읽기 (노트북) | 같다: `ca_search_rows` 64,059 행, 모든 열의 sha256 이 같다(`source_line_number` 포함). 17 초, 끝난 뒤 30 MiB. 단 법정동 코드 칸을 NULL 로 채운 비교였다: 진짜 코드 칸(물건마다 여덟 번 더 푼다)으로는 2 GiB 를 넘었다(아래) |
| 운영 2026-11-25 21:31Z, 물건 검색 2026-11-22 (370 MiB) | 메모리 초과로 실패(`DuckDB` 한도 1.4 GiB, 유닛 3 GiB): 물건마다 JSON 을 38 번 풀었고 쪽을 세려고 파일을 다시 읽었다. 거부는 공통 읽기에만 있었다 |
| 2026-11-26, 물건마다 선언한 모양 + 한 번 읽기 (노트북, 진짜 코드 칸) | 2026-11-22: 지금의 SQL 과 같다(62,827 행, 2,281 쪽), 650 MiB 에서 통과, 11 초. 2026-11-09: 같다(64,059 행, 2,435 쪽), 650 MiB. 지금의 SQL 은 2 GiB 에서 실패, 6 GiB 에서 28 초 |
| 2026-11-26, 매각결과 검색 2026-11-21 (61 MiB)·매각공고 2026-11-22 (52 MiB) | 같다(15,367 행·384 쪽; 168 공고). 550 MiB·1,000 MiB 에서 통과(매각공고는 `data` 가 객체인지 `json_type(text, '$.data')` 로 물어 `data` 를 다시 쓰지 않는다) |
| 사건상세 2026-11-19 (352 MiB, 26,875 줄), 공통 읽기 + 역할 SQL 그대로 (노트북) | 다섯 표 모두 같다. 32 초, 끝난 뒤 469 MiB |
| 물건상세 2026-11-15 (253 MiB, 10,000 줄), 공통 읽기 + 역할 SQL 그대로 (노트북) | 여덟 표 모두 같다. 31 초, 끝난 뒤 321 MiB |
| 운영 2026-11-26 21:31Z (#1754 가 든 릴리스, 한도 1.4 GiB, CPU 쿼터 없는 유닛: 20 스레드) | 물건 검색 2026-11-22 통과·커밋(+974 행), 물건상세 part0001 커밋, 매각공고 2026-11-22(52 MiB)에서 메모리 초과. 유닛 최고점 2.28 GiB / 3 GiB |
| 2026-11-27 재현 (노트북, 파일 단계, 1.4 GiB, 그 run 의 순서): 20 스레드·앞 객체의 표를 둔 채·32 MiB | 매각공고가 창(`QUALIFY`) 문장에서 실패: 운영과 같다. 앞 객체의 표 169 MiB 는 흘려 보내졌다 |
| 같은 순서, 1 스레드·16 MiB (이 결정의 모양) | 셋 다 통과: 14·8·8 초. 앞 객체의 표를 둔 채로도 통과 |
| 매각공고 2026-11-22 혼자, 32 MiB, 통과한 가장 작은 한도 | 1 스레드 1,434~2,048 MiB(읽기에서 실패), 2·4 스레드 1,024 이하, 8·11 스레드 1,024~1,434, 16·20 스레드 1,434~2,048(창에서 실패) |
| 2026-11-27 역할마다 가장 큰 객체, 1 스레드·16 MiB·장소 참조를 올린 파일 단계 | 사건상세 1,280 MiB·현황조사 1,024·감정평가 192·문건송달 512·현황조사서 256·물건상세 1,024·배당요구 1,434·물건 검색 640·매각결과 검색 512·매각공고 1,024 에서 통과. 비율은 계약의 `memory_per_text_byte` 다 |

W101 의 시제품이 센 121,608 은 키로 줄이기 전의 물건 수다: 검색은 네 조건을 묻고(25,572 + 29,602 + 32,431 + 34,003), 한 조건 안에서는
겹치지 않으며, 40,721 개가 둘 이상의 조건에 나온다. 키마다 한 줄이면 64,059 다.

**쓰는 쪽 하나(결정, W104 에서 만듦).** 모든 수집기는 `BronzeCommitter` 위의 공통 JSON Lines 객체 작성기 하나로 쓴다
(`foundation-publisher-collection` 의 `bronze_line_objects`). 인터페이스는 `push_group(&[줄])` 하나이고, 묶음(group)은 쪼개지 않는다.
지운 것: gzip 도우미 셋, 저장 몸통(경매·온비드·산업단지 공고·분양공고), `write_local_copy` 둘, 개수로 굴리는 설정 둘
(`commit_every_lines`·`commit_every`). SSOT 를 위한 결정이지 메모리 때문이 아니다: 크기로 굴리지 않는다. 그 개수 설정이 지키던 것(죽은 run 이
커밋한 것을 지키기)은 시간이 맡는다(코디네이터 결정 2026-11-25): 작성기는 채우던 객체의 첫 묶음에서 계약
`bronze-line-objects.contract.json` 의 `close_after_seconds` 가 지나면 다음 묶음 경계에서 그 객체를 닫는다(Kinesis Data Firehose 의
버퍼 간격, Kafka Connect S3 sink 의 `rotate.interval.ms`). 멈춤·오류 경로도 읽은 것을 커밋한다. 검색은 묶음 하나라 얼마나 걸리든 객체 하나다.
키 모양은 레인마다 지금대로 둔다(Bronze 가 바뀌지 않는다). Silver 가 키 모양을 읽지 않고 등록부(`catalog.bronze_object`)로 객체를 찾게
되면 그때 모양을 하나로 한다(후속).

**넣지 않는 것.**
- 다른 Silver 레인(온비드, 산업단지 공고·분양공고, 산업단지 Silver, data.go.kr 파일)은 지금처럼 통째로 읽는다. 저마다 형식 지정 읽기로
  옮기는 것은 그 레인을 AWS 의 새 모양으로 옮길 때 정한다.
- 원천 제공자의 파일(data.go.kr 의 zip·csv)은 이 절 밖이다: 키가 바이트를 이름한다. 그 읽기는 따로 정한다(`DuckDB` 의 `read_csv` 가 후보,
  확인 안 함).

**말.** 상태를 지니는 "조각"은 Dagster 의 파티션뿐이다(ADR-0615). 묶음(group), 수집기 run(run id 하나), 수집한 밤(§3). 읽을 때의
"조각(part)"이라는 말은 그 코드와 함께 없앤다.

**기각한 것.** 64 MiB 에서 굴리고 Silver 를 수집한 날로 나누기(검색이 나뉘면 removal 이 소리 없이 멈추고, 날은 run 의 단위가 아니다).
크기 등급만 키우기(통째 읽기는 8~16 GiB 가 필요하고 객체와 함께 자란다). 모든 역할을 처음부터 선언한 모양으로(지금의 객체에는 공통 읽기로
충분하다: 잰 값). Bronze 를 Iceberg 표로(두 번째 Bronze, 등록부와의 끈이 끊김). 쓸 때 Parquet(원천의 정확한 답을 잃음). 읽을 때 줄 묶음을
`shard_plan` 에 합치기(#1718: 서로 무관한 두 계획기).

**개정 주 (2026-11-25, W103): 제공자의 CSV 는 `DuckDB` 의 `read_csv` 가 읽고, 발행기는 글자만 바꾼다(코디네이터 결정).** 위 "따로
정한다"의 첫 레인은 국토교통부 실거래가 Silver(`rt_molit_silver_stage`)다. 내보내기는 CP949 글이다. 발행기는 `encoding_rs`(이미 크레이트에
있다)로 그것을 UTF-8 로 바꾸고, 머리말의 달만 본다. 따옴표·칸 안의 줄 끝·칸 수·줄 거부 같은 CSV 읽기는 모두 `read_csv` 다
(`records.sql`). 제공자의 행 번호를 차례로 걷는 규칙 하나만 루프다(SQL 로는 루프 없이 말할 수 없다). 손으로 짠 CSV 읽기는 두지 않는다.
- 기각: `DuckDB` 의 `encodings` 확장(v2.0.0, `cp949` 를 읽는다). 확장 하나가 플랫폼마다 풀면 290 MB(gz 68 MB)다. 확장 계약은 모든
  확장을 모든 레이크하우스 단계에 싣는다. 그래서 단계마다 290 MB 를 싣거나 "이 단계만 싣는" 새 장치가 필요하고, 두 이미지가 약 290 MB
  씩 커진다.
- 잰 것(2026-11-25, 읽기 전용, 운영 Bronze 객체 셋과 그 달의 현재 Silver 행 167,478 개):
  - `encoding_rs` 의 글은 파이썬 `cp949` 의 글과 바이트까지 같다.
  - PR 의 SQL 이 낸 이름(`row_key`)·`provider_fields_json`·형을 가진 칸 39 개·줄 번호가 Spark 가 쓴 행과 모두 같다. 그래서 옮겨도
    이미 있는 행이 다시 쌓이지 않는다.
- 한계: `read_csv` 는 빈 줄을 세지 않는다. 행 사이에 빈 줄이 있으면 그 뒤 행의 `source_line_number` 가 하나 작다(적재의 칸이라 견주지
  않는다). 잰 세 객체에는 행 사이의 빈 줄이 없었다.

### 4. 언제

| 지금(`decide.py`·조정자) | AWS |
|---|---|
| 예약, 상류 없음(원천 수집) | `cron_tick_passed(신선도 박자).since_last_handled() & ~in_progress()`([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1). 평범한 `ScheduleDefinition` 은 없다 |
| 예약, 상류 있음 | `eager()`([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1) |
| 입력으로 시작 | `AutomationCondition.eager()` |
| 코드가 바뀜 | 자산의 `code_version` = 이미지 digest 에서 나온 값, `code_version_changed` |
| 호스트가 거부한 시작, 줄에 다시 넣기, 놓친 시작 | 없다: 호스트가 없다. run 은 Dagster 의 줄에 있다 |
| 레인을 처음 켤 때(자동화의 첫 평가 전에 이미 관측된 입력) | 자동으로는 아무것도 요청되지 않는다(같은 실험). 켜는 PR 이 백필을 한 번 내거나, 조건에 "없으면"(`on_missing()` 의 항)을 더한다. 레인마다 어느 쪽인지는 그 레인의 ADR 이 적는다 |
| 실패 뒤 물러났다 다시(10분 → 4시간) | 길게 물러나는 식은 두지 않는다: 아래 재시도 표 |
| 관측 자산(파티션 레인의 입력)의 박자 | [ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1 의 시작 조건(신선도 박자, 진행 중인 run 없음). 관측은 가장 작은 등급의 태스크에서 읽기 역할로 돈다 |
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
- 위는 줄에 건다(`dagster.yaml` 의 `concurrency.runs`). 남의 한계 하나가 이미 있다: Dagster 의 저장소와 Bronze 원장이 함께 쓰는 db.t4g.micro
  하나의 연결(약 80, 계산). 그래서 run 은 모두 10 개까지, 그중 원장을 쓰는 종류(태그 `perfectory-platform/kind`)는 수집 3·Silver 1 까지다.
  계산은 그 파일에 있다. 한도를 넘는 run 은 줄에서 기다린다. 돈의 위는 그 줄과 계정의 Fargate vCPU 할당량이다.

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
  그 길은 AWS Chatbot(Amazon Q Developer in chat applications)이다(사장 결정 2026-11-26). SNS 주제 하나가 Slack 채널 하나로 가고,
  저장하는 토큰은 없다. Slack 으로 가는 경보는 그 주제에 알리는 경보, 곧 코드다(처음은 'Dagster 가 돌지 않는다' 하나).
  늘 켠 서비스(웹서버·데몬·코드 서버)가 태스크 크기에 다가가는 것도 죽기 전에 부른다(ADR-0574 §5 의 짝, 코디네이터 2026-11-27):
  ECS 서비스 지표의 메모리·CPU 가 15 분 동안 계약의 `alarms.near_limit` 몫을 넘으면. 고치는 것은 계약의 잰 크기다.
  사장이 Slack 작업 공간을 콘솔에서 한 번 허가하고, 채널 묶음은 `infra/aws/chatbot.tf` 다. 이 서비스는 서울에 제어 평면이 없다
  (us-east-2·us-west-2·ap-southeast-1·eu-west-1). 설정은 us-east-2 로 다루고, 주제와 경보는 서울에 남는다.
  알림은 그 네 리전에서 처리되므로, 이 주제에는 개인 정보나 사업 데이터를 싣지 않는 경보(메타데이터)만 알린다(코디네이터 결정 2026-11-26).
- 경보: Dagster OSS 에는 경보 정책이 없다. run 실패와 신선도 넘김은 코드 위치의 내장 센서 둘이 같은 경보 주제에 알린다
  (`orchestration/dagster/aws/alerts.py`, 코디네이터 결정 2026-11-26): `run_failure_sensor` 는 job·run id·위치를, 다른 하나는 신선도 상태가
  FAIL 이 된 자산의 키를(Dagster 의 `get_freshness_state_records`). 메시지는 AWS Chatbot 의 사용자 지정 알림이고 이름과 id 만 싣는다:
  run 의 실패 메시지는 원천의 답이나 로그 줄을 담을 수 있어 보내지 않는다(그 주제의 규칙). 코드 서버의 역할은 그 주제에 알리는 것뿐이다.
  Prometheus·Alertmanager 를 AWS 에 세우지 않는다.

### 7. 옮기는 순서 — 수집은 멈추지 않는다

서빙 최신은 지금 목표가 아니다. 그래서 순서는 상류부터다(ADR-0583 의 "하류부터"와 반대: 그때는 서빙을 지켜야 했다).

| 걸음 | 하는 일 | ai-server 에서 끄는 것 |
|---|---|---|
| A0 | 사장이 항목별 비용을 본다. 계정·VPC·RDS·ECS 를 코드(IaC)로 정의한다(이미지는 GHCR, ADR-0619 §7). 아무것도 만들지 않은 채 검토 | — |
| A1 | Dagster 를 AWS 에 세운다. 작업 없음. 배포 = 이미지 digest | — |
| A1b | **Bronze 원장의 자리를 RDS 에 만든다(빈 채로).** 원장(`catalog.bronze_object`·`ingestion_run`)은 원천마다의 줄이다. 한 원천의 줄은 그 수집기와 함께 넘어간다(넘기는 규칙의 "상태를 옮긴다"에 든다). 그래서 어느 때든 한 원천의 원장은 한 곳이고, ai-server 와 RDS 사이에 망을 잇지 않으며, 수집이 멈추는 것은 넘기는 그 원천의 한 박자뿐이다. 스키마는 데이터 호스트와 같은 sqlx 마이그레이션이 만든다(데이터베이스 `foundation`, 정의는 하나; 코디네이터 결정 2026-11-26). 적용이 새 digest 로 옮길 때 서비스보다 먼저 부트스트랩 → 마이그레이션 → 권한·마무리가 돌고, 끝 코드 0 이어야 서비스가 옮긴다(`infra/aws/pre-deploy.sh`). 다음 걸음은 A2 뒤다: 적용을 노트북이 아니라 main 의 CI 가 GitHub OIDC 역할로 한다(오래 사는 키 없음, 사람이 돌리는 배포 없음) | — |
| A2 | 수집 한 개를 AWS 에서 돌린다: 쿼터도 없고 run 사이 상태도 없는 원천. 아래 "넘기는 규칙"의 겹쳐 돌리기. 경매처럼 주소에 민감한 원천은 그 전에 AWS 주소에서 잰다(§8 J) | 그 작업 하나 |
| A3 | 나머지 수집 14개, 하나씩, "넘기는 규칙"대로 | 수집 묶음, 그 유닛 |
| A4 | Silver 적재(DuckDB)를 자산으로. 표 43개의 분류가 여기서 필요하다 | Silver 묶음 |
| A5 | Gold 레인(ADR-0615 의 파티션 자산) | Gold 묶음, 조정자 |
| A6 | 굽기·서빙(이 ADR 밖: API·타일·서빙 저장소의 AWS 설계가 따로 있다) | 남은 것, 호스트 |

**넘기는 규칙(수집기 하나를 AWS 로).** 원천의 성질이 둘 중 무엇인지는 레인의 정의(`config/aws-platform.contract.json` 의
`collect.lanes.<작업>.handover`)에 적는다(코디네이터 결정 2026-11-27, 사장 "옮길 수 있는 것은 먼저 다 옮기고 뒤에 확인").
수집은 끊기지 않는다: 호스트 쪽은 AWS 쪽이 제 검사를 통과할 때까지 돈다.

| 원천 | 넘기는 법 | 증거 | 되돌리기 |
|---|---|---|---|
| 쿼터 없음(run 사이 상태가 있어도: data.go.kr 의 `accepted.json`, juso 의 `cursor.json`) — `overlap` | 사흘 겹쳐 돌린다. 둘 다 Bronze 에 쓰고, 같은 객체는 내용 주소라 한 벌이다(같은 바이트를 만난 생성 전용 쓰기는 원장 줄만 남긴다, ADR-0016). AWS 쪽은 비어서 시작하지 않는다: 호스트 쪽이 run 마다 남기는 상태 객체(§8 C) 가운데 가장 새것에서 한 번 시작하고, 그 뒤로는 제 상태를 쓴다. 원장은 겹치는 동안 둘이다(호스트의 Postgres 와 RDS). 호스트 쪽을 끌 때 그 원천의 호스트 원장 줄을 RDS 로 옮긴다(`object_key` 로 겹침을 걸러 낸다) | 사흘 동안 AWS 쪽 run 의 검사 `collected` 통과 | AWS 쪽을 끈다(레인을 계약에서 뺀다) |
| 쿼터 있음(온비드의 하루 호출 예산, 실거래의 하루 90 내려받기) — `handoff` | **겹치지 않는다. 한 박자 안의 인계이고, 쿼터를 두 번 쓰지 않는다.** 스위치는 하나다: 호스트 작업의 `enabled`(ADR-0625 의 `runs_on_host`). AWS 의 코드 위치는 그 작업이 호스트에서 꺼진 릴리스에서만 그 레인을 정의한다(`aws/collect_lane.py` `lanes_here`). 그래서 그 작업을 끄는 PR 하나가 호스트 쪽을 끄고 AWS 쪽을 켠다. AWS 쪽 첫 run 은 호스트 쪽의 마지막 상태 객체에서 시작한다(호출 수·내려받은 수가 그 안에 있다). 원장 줄은 끄는 PR 뒤에 옮긴다(위와 같다) | AWS 쪽 첫 run 의 검사 통과와, 호스트의 상태를 이어받았다는 표시(run 의 `state_read` 가 `host/` 객체) | 역순: 그 작업을 호스트에서 다시 켠다(AWS 쪽 레인은 같은 릴리스에서 사라진다). 상태는 AWS 쪽 마지막 객체를 호스트에 되돌린다 |

어느 쪽이든 상태가 없으면 묻지 않는다: 비어서 시작한 수집기는 제 이력을 처음부터 다시 묻고, 쿼터가 있는 원천이면 하루 예산을 두 번 쓴다.

**2026-11-27 덧붙임(W101c): 실거래는 호스트에 남는다.** rt.molit.go.kr 이 AWS 주소의 TCP 연결을 받지 않는다. 인계 뒤 첫 AWS run(15:31Z)이
내보내기 양식에 연결하다 30 초 만에 끊겼다. 같은 서브넷의 탐침도 27.101.224.26:443 에서 두 번 끊겼고, 같은 정부망의 apis.data.go.kr(27.101.236.63)
은 바로 붙었으며, ai-server 는 0.07 초에 붙었다. 원천이 AWS 주소를 막으면 그 레인은 멈추고 우회하지 않는다(W101c 지시). 그래서 위의 되돌리기를
그대로 했다: 그 작업을 호스트에서 다시 켠다(같은 릴리스에서 AWS 레인이 사라진다). 그 AWS run 은 한 건도 내려받지 못했으므로 호스트의 상태
디렉터리가 그대로 최신이고, 되돌릴 상태 객체가 없다. 한 run 의 내려받기는 호스트 단위의 30 분에 맞던 30 으로 돌린다(하루 90 은 그대로).
레인 선언은 남긴다: 원천이 AWS 를 받아들이면 스위치 하나로 다시 옮긴다.
AWS 쪽이 제 검사를 통과한 레인을 호스트에서 끄는 것은 그 작업 정의의 `enabled: false` 다(ADR-0625 의 2단계). 묶음마다 PR 하나다.

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
| C | run 사이에 남는 로컬 상태(온비드 `state.json`, data.go.kr `accepted.json`, Gold 판의 `resume/`·판정 파일, 굽기의 `join-next.json`·파티션 캐시, 품질의 `first-checked.json`, 멈춤 스위치 디렉터리) | 태스크의 디스크는 run 과 함께 사라진다. 그래서 상태는 셋 중 하나로 간다: (가) 이미 정본에 있는 것은 거기서 읽는다(Iceberg 커밋 요약의 핀·버전, Bronze 원장) (나) Dagster 가 지닌다(파티션 상태, 구체화 메타데이터, 센서 커서) (다) 그래도 남는 것은 R2 의 객체 하나다(온비드의 호출 예산·열린 물건). 종류 (다)는 레인마다 이름으로 적고, 없애는 쪽이 기본이다. **수집기의 (다)(코디네이터 결정 2026-11-27):** 수집기의 상태 디렉터리를 run 마다 객체 하나로 남긴다: `collector-state/<레인>/aws/<UTC 시각>-<run id>.tar.gz`(계약 `collect.state_prefix`), 덮어쓰지 않고(append-only) 가장 새것을 읽는다. 명령이 실패한 run 도 남긴다(물은 것·센 것은 이미 썼다). 발급 창구는 레인마다 제 `collector-state/<레인>/` 만 준다(레인의 id 에서 만들고 레인마다 적지 않는다). 호스트 쪽 수집기는 run 마다 `collector-state/<레인>/host/` 에 같은 객체를 쓴다(`scripts/ops/collector-state-publish.sh`): 호스트의 상태 디렉터리는 서비스 계정만 읽을 수 있어 손으로 옮길 길이 없고, 이 길은 자동배포로 들어간다 |
| D | 서빙 중인 필지 판이 호스트 파일이다(승격 원장, `readiness.json`). 여러 레인이 읽는다 | 카탈로그의 사실이 된다: 그 Gold 스냅숏의 태그(`served-…`)와 Dagster 구체화. 파일은 없다 |
| E | 작업 디스크·스필(DuckDB 단계 32 GB 예약, 스풀 16.7 GB, source-sweep 스풀 수십 GB, 타일 굽기는 접두마다 25.0~27.3 GB·나누지 않으면 43.7 GB: `config/tile-bake-containers.contract.json`). 남은 것을 치우는 일도 호스트의 것이다: 2026-11-23~24 에 유닛 한도로 죽은 takeover 판정 run 넷이 `runs/<run>/verdict/` 아래 DuckDB 단계 34.37 GB 를 남겼고, `duckdb-stage-orphans.sh` 는 맨 위 단계만 보므로 그것을 지우지 못했다. 그래서 경계 섀도 빌드가 11-24 13:26Z 부터 SSD 예약에 막혔다(W86d) | 태스크의 임시 디스크. 크기 등급의 `ecs/ephemeral_storage`(20~200 GiB). 호스트 SSD 를 Postgres 와 나누던 예약 수는 없다. 지금 아는 일은 모두 200 GiB 안이다. run 의 작업 공간은 태스크와 함께 사라지므로 남는 단계를 치우는 스크립트(`duckdb-stage-orphans.sh`)는 옮기지 않는다 |
| F | 메모리 크기를 유닛의 `MemoryMax` 에서 읽는다 | 태스크의 메모리가 cgroup 한도다. 읽는 길은 그대로이고 몫만 태스크 크기 기준으로 |
| G | 수집기가 Postgres 의 Bronze 원장을 쓴다(`catalog.bronze_object`, `ingestion_run`). Silver 적재 계획이 그것을 읽는다 | **Postgres 를 둔다**: Dagster 의 RDS 인스턴스에 데이터베이스 하나를 더 둔다. 원장을 Iceberg 나 Dagster 파티션으로 옮기는 일은 이 ADR 이 하지 않는다(읽는 쪽이 여럿이다) |
| H | 호스트의 도구: GDAL 을 `docker run`, 호스트에서 지은 tippecanoe 이미지, 오래 떠 있는 Spark 컨테이너 | 도구는 이미지다(GHCR 의 공개 패키지, ADR-0619 §7). 도구가 다른 일은 코드 위치가 다르다(§2-2). 호스트에서 짓는 이미지는 없다 |
| I | 보고: `foundation-job-outcome` 줄, 저널을 Pipes 로, node-exporter 텍스트 파일, 유닛의 `OnFailure` → Slack | Pipes 메시지와 Dagster 의 사건(§6). 유닛이 없으므로 `OnFailure` 도 없다 |
| J | **망**: 경매 수집기는 사무실의 한국 주소에서 브라우저처럼 묻는다. 사이트는 싫은 세션을 막는다. AWS 주소에서는 재 본 적이 없다 | 아래 문단 |

**J — 모든 수집은 AWS 에서 돈다(사장 결정 2026-11-24). 경매도 그렇다.** 사무실 호스트를 일의 자리로 두지 않는다(ECS Anywhere 를 쓰지 않는다).
레인은 지금 모양 그대로가 아니라 새 모양으로 옮긴다. A2 전에 AWS 서울 주소에서 읽기 전용 목록 검색 한 번으로 잰다(자원을 만드는 시험이라 사장의 승인 뒤).
제공자가 AWS 주소를 막으면 **돌아가지 않는다**: 사장에게 알리고 사장이 정한다.

**우리 계층이 해 주던 것 여덟(W93b)의 자리.**

| 지금 계층이 해 주는 것 | AWS 에서 |
|---|---|
| 한 작업은 한 번에 하나, 복사본이 줄에 서지 않는다(11-23: 줄의 93 중 74 가 복사본) | 시작 조건의 `~in_progress()` 가 겹침을 막는다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1) |
| 놓친 예약은 따라잡는다(media_reconcile 43 h) | 조건은 상태라 놓침이 없다. 틱은 처리될 때까지 남는다([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1) |
| 나쁜 것 하나가 run 전체를 실패시키지 않는다 | 파티션(밤·시도·접두)마다의 상태. run 안에서 깨진 본문을 다시 받는 것은 수집기의 코드로 남는다 |
| 명령은 제가 읽는 비밀만 받는다 | §2-8 |
| 꺼진 작업은 아무것도 남기지 않는다 | 정의에 없으면 자산·예약·검사가 없다. 알릴 유닛도 없다 |
| 생산자 없는 레인은 서빙되지 않는다 | 서빙 자산은 제 생산 자산이 있을 때만 정의된다(공장이 거부한다). 신선도는 자산 검사다 |
| 배포가 작업을 붙잡고 비우고 다시 시작한다 | 붙잡지 않는다: 마이그레이션이 앞 릴리스를 깨지 않는다(§2-5). 빚진 run 의 장부는 없다 |
| 작업마다 메모리·시간 한도 | 크기 등급(§2-9). 시간은 작업마다 두지 않는다: 원천의 멈춤 규칙과 플랫폼의 run 상한 하나([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §3) |

**기준 레인(W71c 의 경매 레인).** 이 표의 모양을 처음 따르는 레인이다: Bronze 는 원천 슬러그마다 자산, 밤마다의 시간 파티션, 수집기는 `@multi_asset` 하나;
Silver 28표는 `@multi_asset` 하나; Gold 는 자산 하나. 수집은 [ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §1 의 시작 조건, Silver·Gold 는 `eager()`. 호스트 상태 없음. 검사는 일이 Pipes 로 낸다.

### 9. 남는 우리 것

| 남는 것 | 맡지 못한 기능과 이유 |
|---|---|
| 작업 정의와 공장 | 접착제 |
| IaC(VPC·ECS·RDS·IAM) | Dagster 는 자기가 놓일 곳을 만들지 않는다 |
| 서명된 digest 만 올리는 릴리스 빌드 | Dagster·ECS 는 이미지를 검증하지 않는다 |
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
| CloudWatch 경보(표준 해상도)·월 | 0.10 | AmazonCloudWatch(11-25) |
| Cloud Map 등록 자원·월 / Route 53 호스팅 영역·월(전역, 처음 25 개) | 0.10 / 0.50 | AWSCloudMap(11-25) / AmazonRoute53(11-18) |

**수집만(A0~A3).**

| 줄 | 월(USD) | 근거 |
|---|---|---|
| Dagster 웹서버·데몬, 저마다 서비스(각 0.25 vCPU / 0.5 GB ARM) | 16.6 | 계산(8.29 × 2). 크기는 잼(§1). 2026-11-27 전에는 한 태스크 0.5 vCPU / 2 GB 였다: 이 줄은 x86 단가로 24.5(ARM 이면 19.6) |
| RDS db.t4g.micro + 20 GB | 20.9 | Dagster DB 1,001 MB. **Bronze 원장의 크기는 재지 않았다**(A2 전에 잰다). 20 GB 를 넘으면 GB 마다 0.131 |
| 수집 태스크(온비드 4 vCPU / 8 GB 113 h, 나머지 14개 0.5 vCPU / 2 GB 220 h) | 33.0 | 시간·CPU 잼 |
| 공인 IPv4(NAT 없음): 웹서버·데몬 둘, 수집 태스크 333 h | 9.0 | 계산(3.65 × 2 + 1.7). 웹서버를 나누기 전에는 5.3 |
| R2 로의 송신(Bronze) | 11.3(평월 90 GB) / 69(일괄 수집이 있던 지난달 548 GB) | 원장에서 잼. 무료 100 GB 는 우리 것이 아니라고 가정 |
| 이미지 보관(GHCR 공개 패키지) | 0 | ADR-0619 §7. 2026-11-26 전에는 ECR 약 10 GB 의 1.0 이었다 |
| CloudWatch Logs 5 GB | 4.0 | 가정(재지 않음) |
| Secrets Manager 8개 | 3.2 | env 파일 수(입력 목록의 일곱과 `parcel-publication.env`) |
| 코드 서버(위치 하나, 0.25 vCPU / 0.5 GB ARM) + 공인 IPv4 + Cloud Map 이름공간·인스턴스 | 12.5 | 계산(6.8 + 1.5 + 3.65 + 0.5 + 0.1). 코디네이터 승인 2026-11-26. 1 GB 였을 때 13.9 |
| **합(수집만)** | **110.5(평월) / 168.2(일괄 수집 달)** | 계산(2026-11-27: 웹서버·데몬을 나누고 세 서비스를 잰 크기로; 그 전 116.1 / 173.8). 사장에게 물은 값은 103.2 / 160.9 였다 |

NAT 게이트웨이와 ALB 는 설계에 없다.

**늘 켠 자원만**(2026-11-27 적용된 모양, 단가는 가격 목록 API 의 서울 판 2026-11-18~27, 한 달 730 시간; run·송신·일괄 수집 제외):

| 줄 | 월(USD) |
|---|---|
| Fargate ARM 서비스 셋(웹서버·데몬·코드 서버, 각 0.25 vCPU / 0.5 GB: 6.80 + 1.49) | 24.9 |
| 공인 IPv4 셋(서비스마다 하나) | 11.0 |
| RDS db.t4g.micro 단일 AZ + gp3 20 GB | 18.25 + 2.62 = 20.9 |
| Secrets Manager 넷(Dagster 역할, 마이그레이터, 런타임, RDS master) | 1.6 |
| Cloud Map 이름공간의 사설 호스팅 영역 + 등록 인스턴스 하나 | 0.6 |
| CloudWatch 경보 아홉(웹서버·데몬 멈춤, DB 디스크, 늘 켠 서비스 셋의 메모리·CPU 근접), 로그 하루 약 0.8 MB | 0.9 + 0.02 |
| **합** | **59.8** |

웹서버·데몬을 나누기 전(한 태스크 0.5 vCPU / 2 GB 19.57, 코드 서버 1 GB 9.78, IPv4 둘 7.30, 경보 둘 0.20)은 59.9 였다. 경보는 계정의 무료 10 개 안이다. 예산·SNS·Chatbot·인터넷 게이트웨이·AWS 관리 KMS 키는 0 이다.

**모든 작업을 옮긴 뒤 더해지는 것**(Spark 와 타일 제외 — 타일은 아래 표; 같은 7일 저널 × 30/7):

| 줄 | 월(USD) | 근거 |
|---|---|---|
| default 풀의 나머지 작업(조정자 제외) 351 h, 0.5 vCPU; `media_ladder_backlog` 만 2 vCPU | 25.8 | 시간 잼. 그 작업의 CPU 는 잼(7일 52 코어·시간) |
| duckdb 작업 324 h · 2,812 GiB·h, 4 vCPU | 74.7 | 시간·메모리 잼. **CPU 는 가정**(작업별로 재지 못했다: 일이 컨테이너 안에서 돈다) |
| 그 태스크들의 공인 IPv4 | 3.4 | — |
| R2 로의 송신(Silver·Gold 월 195 GB) | 24.6 | 스냅숏 요약 7일 45.5 GB |
| 로그 5 GB 더 | 3.8 | 가정 |
| **더해지는 합** | **132.3** | |
| **수집 + 모든 작업** | **242.8(평월)** | 계산(2026-11-27: 웹서버·데몬을 나눔; 2026-11-26 248.4, 그 전 235.5) |

Spark 작업은 옮기지 않는다(§2-2). 지금 그대로 옮긴다면 426 h 를 4 vCPU / 20 GB 로 약 123 을 더한다(크기는 가정).
예외인 유지보수 컨테이너(§2-2)의 줄은 아직 없다: 필요할 때만 뜨므로 그 시간과 크기를 dev 에서 잰 뒤(표준 프로시저의 시험) 이 표에 넣는다.
**타일 굽기**(양은 ADR-0618 §8 의 것, 단가는 이 표의 것; 필지 전량 굽기 한 번):

| 줄 | 한 번(USD) | 근거 |
|---|---|---|
| 파티션 태스크 14.5 vCPU·h | 0.68 | 태스크 217 분은 가정(z16 에서 169 분을 잼), 4 vCPU 가정 |
| 메모리 29~58 GB·h | 0.15 ~ 0.30 | 8~16 GB 가정(최대 5.42 GiB 를 잼) |
| 임시 디스크 30 GB(20 GB 초과분 10 GB × 3.6 h) | 0.005 | 25~27 GB 를 잼 |
| R2 로 올리기 16.4 GiB | 2.07 | 세대 하나의 크기를 잼 |
| 합치는 태스크(ARM 1 vCPU / 2 GB × 250 초, 임시 디스크 30 GB, 공인 IPv4) | 0.004 | 시간·메모리 잼(W86d, 발행기의 합치기 #1685, 16 파일 8.71 GB: 250 초, 최고 180 MiB, 타일의 1.3 % 만 풀어 합침). 크기는 가정(코어 수를 재지 않았다). 합친 파일을 올리는 바이트는 위 "R2 로 올리기" 줄에 이미 있다 |
| **한 번의 합** | **약 2.9 ~ 3.1** | 같은 줄들을 ARM 단가로 다시 적으면 접두 태스크 0.54, 메모리 0.12~0.24 라 **약 2.7 ~ 2.9** 다 |

필지 판은 한 달에 한 번 바뀌므로 평월에 한 번이다. 이 범위는 작업 계산만이다(API·서빙 저장소 제외).

**아직 값이 없는 줄.** 파티션 레인의 관측 run(틱마다, 가장 작은 등급), 타일의 굽기 횟수, 타일의 접두별 입력 읽기(ADR-0618 열린 질문 3), Bronze 원장의 디스크, duckdb 작업의 실제 CPU, 타일 태스크의 공인 IPv4, `tiles` 코드 위치의 이미지 보관, 옛 타일 세대의 R2 보관, "이미 맞는 시도"를 다시 보고할 때 하류가 한 번 도는 run, 원천마다 원장 줄을 옮기는 일, AWS 주소에서의 경매 시험 한 번.
그래서 사장에게 묻는 것은 둘로 나뉜다: **지금은 A0~A3(수집, 평월 103.2; 이미지 줄을 빼고 코드 서버를 더하고 웹서버·데몬을 나눈 지금 계산은 110.5)**, A4~A6 는 위 줄들을 잰 뒤 다시 항목별로.
재지 못하는 양에는 위를 건다: 계정의 Fargate vCPU 할당량, 줄의 `max_concurrent_runs`, 플랫폼의 run 상한 하나([ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §3).

2026-11-27(W101c): A3 의 첫 묶음 — 레인의 정의는 계약의 `collect.lanes` 하나이고 자산·센서·검사는 공장 하나가 만든다
(`orchestration/dagster/aws/definitions.py`, `collect_lane.py`). 박자는 원천 계약이 적는다(`beat`, ADR-0623 §1): 모두 하루 한 번이다. 온비드도 그렇다(사장 결정 2026-11-27): 목록에 하루가 안 된 회차는 하루 박자가 영영 못 본다. 잰 것: 회차 2,227 개 중 12 개(0.54 %), 사장이 그 손실을 받아들였다. 발급 창구는 객체 절반만
적용했다: 표를 읽는 레인이 없고, 표의 절반(`pin`, 카탈로그 토큰)은 지웠다. 표를 읽는 수집기(법원경매, 산단·분양공고 넷)와 Silver 까지 적재하는
둘(data.go.kr 파일, 등록공장)은 카탈로그 결정(ADR-0624)을 기다린다. 산단 주소 원천 수집기는 다른 원천의 Bronze 를 호스트 원장으로 읽어
그 원천(source_sweep)이 넘어올 때까지 호스트에 남는다. 셸 드라이버가 호스트의 컨테이너(GDAL, aws-cli)나 호스트 파일 넘김에 기대는
넷(source_sweep, 법정동 코드, 연속지적도 판, 필지번호 변동)은 새 모양으로 다시 지은 뒤 옮긴다.

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
| 기본 기능 | `EcsRunLauncher`, `ecs/*` 태그, `PipesSubprocessClient`, 자산·파티션, `ScheduleDefinition`, `AutomationCondition` 과 그 센서의 `run_tags`, `RetryPolicy`, run monitoring 과 run 재시도, last-update 신선도 검사, 자산 검사; ECS·IAM·RDS·Secrets Manager·CloudWatch, GHCR 공개 패키지와 `gh attestation verify` |
| 임시 | 옮기는 동안 ai-server 에 남는 수집과 그 계층 |
| 운영 | 정의 → 공장, IaC, 서명된 digest 릴리스, 발행기의 Pipes 메시지 |

2026-11-25: 구현 점검 — Proposed 유지. main 에 있는 것: AWS 계정의 바탕(`platforms/foundation-platform/infra/aws/main.tf`, ADR-0619)과
기준 레인(`platforms/foundation-platform/orchestration/dagster/court_auction_lane.py`, 그 시험
`platforms/foundation-platform/orchestration/tests/test_dagster_court_auction_lane.py`). 없는 것: 열린 질문 1·2·4·6·7·9·10·12 의 답,
§10 에서 값이 없는 줄(A4~A6 의 비용).

2026-11-27 (W109c): 카탈로그는 AWS 의 자체 호스팅 Lakekeeper 이고 데이터는 R2 에 그대로이며, 표 정비는 상류 정비 컨테이너 하나다: 루트 [ADR-0624](./0624-the-lakehouse-catalog-is-self-hosted-open-source-lakekeeper-and-it-holds-only-table-pointers-commits-credentials-grants-and-protection.md) §1·§5.
