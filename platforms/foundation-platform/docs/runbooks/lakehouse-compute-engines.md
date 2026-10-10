---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-02
---

# Lakehouse Compute Engines

## 목적

`foundation-platform` 의 Netflix-style lakehouse compute layer 를 로컬에서 opt-in 으로 검증한다.
기본 개발 환경은 `postgres`, `valkey` 만 실행하고, Trino/Spark 는 필요할 때 profile 로 켠다.

```text
Trino = Iceberg SQL query smoke
Spark = Bronze -> Silver -> Gold batch/write PoC
Rust foundation-platform = control-plane, API, promotion, rollback
```

## Profiles

| Profile | Service | 목적 |
|---|---|---|
| `lakehouse-query` | `trino` | R2 Data Catalog / Iceberg table SQL 조회 |
| `lakehouse-batch` | `spark` | Spark batch job PoC 실행 환경 |

## 실행 위치 원칙

Spark/Trino 는 `foundation-platform` 제품 요청 경로가 아니라 교체 가능한 compute runtime 이다. 로컬 PC,
`<lakehouse-host>` 같은 내부 Linux 서버, 이후 AWS Fargate/EMR/ECS 로 옮겨도 같은 contract 를 사용한다.
원격 실행 시 host 주소는 문서/코드에 하드코딩하지 않고
`FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET` env 로 주입한다
(→ [remote-lakehouse-job-runner](./remote-lakehouse-job-runner.md)).

불변인 것은 실행 서버가 아니라 다음 경계다.

- canonical raw/object state: R2 Bronze 와 Iceberg table
- control-plane state: foundation-platform Postgres audit/promotion metadata
- schema/quality contract: `infra/lakehouse/contracts/*` 와 Rust domain type
- compute runtime: Spark/Trino container, 언제든 교체 가능

따라서 내부 Linux 서버에서 돌릴 때도 canonical 데이터를 서버 디스크에 두지 않는다. 서버 디스크는 Docker
image/cache 와 `target/lakehouse` smoke output 정도만 맡는다. Trino port 는 기본적으로
`127.0.0.1:${FOUNDATION_PLATFORM_TRINO_PORT:-18081}` 에만 bind 한다. 다른 PC 에서 접속해야 하면 포트를 LAN 에
그냥 열지 말고 SSH tunnel 또는 인증이 붙은 reverse proxy 를 사용한다.

## 예약 FLOOR 실행의 호스트 준비

설계 정본은 [ADR-0128](../../../../docs/adr/0128-floor-inputs-bind-to-bronze-and-scalar-retries-bind-to-their-append.md)다.
주기·pool·timeout은 `orchestration/jobs.v1.json`을 읽는다. `foundation-building-register-floor.service`가
기존 원장 선택부터 Silver 적재까지 한 주기를 실행한다. 큰 자료 처리는 지정된 배치 Linux 서버에서만 한다.

배포할 때 `foundation-release.sh prepare <sha> <archive>`로 소스를 인증·설치하고 같은 SHA의
publisher·JAR을 빌드한다([ADR-0134](../../../../docs/adr/0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)). 소스는 `releases/<sha>`(읽기 전용), 빌드 산출물은
`/opt/foundation-platform/artifacts/<sha>/`(`foundation-outbox-publisher`, `jars/`, `build.json`)에
놓이고 `current`와 `previous`는 바뀌지 않는다. 호출자가 바이너리를 넘기는 명령은 없다.
wrapper는 `scripts/ops/admitted-writer-runtime.sh --installed`로 자기 release id의 산출물을 찾고,
`build.json`의 sha256과 다르면 실행하지 않는다.
native image는 `build.json`의 `publisher_image`(같은 바이너리를 꺼낸 image ID)로 지정한다. 호스트와 image 내부
바이너리 SHA-256을 비교하고 실제 명령이 실행되는지 검사한다. 예약 호출은 빌드하지 않는다.
`FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_NETWORK`에는 기존 DB bridge 이름,
`FOUNDATION_PLATFORM_LAKEHOUSE_DATABASE_ENDPOINT`에는 그 network에서의 DB DNS 이름과 내부 port를
지정한다. 컨테이너 IP를 고정하지 않는다. 같은 `DATABASE_URL`의 계정·비밀번호·DB를 재사용하고
child 프로세스의 주소만 바꾸므로 새 비밀번호 파일은 없다. 실행별 임시 Compose 파일은 고정 native image와 native 전용 external network,
두 실행기의 같은 읽기 전용 과거 근거 mount·경로·SHA를 포함한다. 기존 DB network의 수명 주기는 바꾸지 않는다.
`infra/systemd/building-register-floor.env.example`의 키만 채운 비밀 없는 파일을
`floor-config <sha> <file>`로 `/opt/foundation-platform/config/<sha>/building-register-floor.env`에
설치한다. release 안에는 쓰지 않는다(인증이 추가 파일로 거부한다). ROOT는 해당 release의
실제 경로다. image·DB network·작업 경로가 release id로 묶이므로 `current` 전환 시 함께 바뀐다.
`RuntimeDirectory`의 실행 ID별 공개 설정 사본을 시작과 `ExecStopPost`에서 함께 읽어
시작했던 release를 끝까지 사용한다. systemd가 종료 처리 후 임시 사본을 제거한다.
전환된 release는 다음 실행부터 적용된다. FLOOR가 없던
이전 release로 되돌릴 때는 해당 DAG를 멈춘 상태로 유지한다.
systemd는 기존 `recovery.env`, `source-sweep.env`, `map-edit-fold.env`의 자격증명을 재사용한다.
DB URL을 따로 저장하지 않는다. wrapper는 `docker compose config --format json --no-env-resolution`이
해석한 `foundation-api`의 DB 계정과 `postgres`의 공개 loopback port를 사용한다. 주소 외의 역할·
비밀번호·DB·옵션은 유지하며 모호하거나 외부로 열린 port는 거부한다. Compose 출력과 DB URL은
로그에 남기지 않는다. 새 최고관리자 계정을 만들지 않는다. `timers`는 비밀 없는 설정에서 state/Ivy
경로를 읽어 서비스 사용자·그룹의 `2770`으로 준비한다. 설치기의 동일한 전용 namespace 허용 목록이
설정의 경로 검사와 systemd `ReadWritePaths`를 소유한다. 두 허용 root를 sandbox에 고정하고
없는 root는 무시하므로, 버전 전환·롤백 때 선택된 작업 경로가 달라져도 쓰기 권한이 어긋나지 않는다.
실제로 준비하는 폴더는 설정에서 고른 root와 하위 작업 폴더뿐이다. unit에 경로 목록을 복사하지 않는다.
심볼릭 링크를 따르지 않는 디렉터리 descriptor로 소유권을 확인하고 권한을 적용한다.
Spark가 만든 하위 파일의 소유권을 재귀적으로 바꾸지 않는다.
데이터 디스크를 사용하는 경우 `/data/foundation-platform/building-register-floor`처럼 허용된 전용 경로를 선택한다.
설치 전 해당 디스크의 공간을 확인하며 기본 시스템 디스크에 큰 작업 파일을 계속 쌓지 않는다.
release는 프로그램과 계약을 보관하고 state는 재사용할
원본·handoff·작업 결과를 보관한다. 이 작업 파일은 원본/적재 완료 원장을 대체하지 않는다.

과거 층 적재 근거는 공개 코드에 포함하지 않는다. 운영자는 보존된 기존 근거를 원장·원본·Iceberg와
대조하고, 릴리스 밖의 root 소유 읽기 전용 파일로 준비한다.
과거 근거 전용 디렉터리는 예를 들어 `/etc/foundation-platform-inputs`를
`root:foundation-platform 0750`으로, 그 안의 `building-register-floor-history.json`은
`root:foundation-platform 0440`으로 준비한다. 모든 상위 디렉터리에 서비스 그룹의 탐색 권한이
있어야 한다. 기존 비밀 디렉터리의 권한을 넓히지 말고 별도 전용 디렉터리를 사용한다.
`FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH`에 그 절대 경로를 지정한다.
설치기는 root 소유·쓰기 불가 조건에 더해 호스트 서비스 UID와 Spark UID185가 동일 서비스
GID로 파일을 읽을 수 있는지 실제로 검사한다. 코드 저장소의 가짜
시험 fixture를 운영 근거로 쓰지 않는다. parent가 계산한 내용 SHA-256을 native/Spark가 함께
검증하므로 별도 SHA 설정값을 수동 관리하지 않는다. 파일이 없거나 실행 도중 바뀌면 중단한다.
이 파일 준비와 서비스 설치는 병합된 릴리스의 배포 단계이며 PR 검증 중에는 운영에 쓰지 않는다.

병합된 코드로 배포할 때는 DAG를 멈춘 상태에서 `prepare <sha> <archive>`(인증·빌드) →
`floor-config` → `activate <sha>` → `migrate` → `timers` 순서로 설정·릴리스·스키마·서비스를 준비한다.
모든 명령은 sudo로 제어 체크아웃의 배포기
`/opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh`를
실행한다. 처음 한 번은 아래 "릴리스 인증 전환" 절을 먼저 따른다.
`activate`는 스키마를 옮기지 않는다. DB 마이그레이션과 레이크하우스 표 맞춤(ADR-0124)은 `migrate`가 하고,
이 단계를 건너뛴 배포는 끝난 배포가 아니다. `timers`는 FLOOR 설정이 틀려도 나머지 unit·timer·허용 목록을
설치하고, FLOOR unit만 빼고 실패로 끝난다. host unit·환경·바이너리·image·Spark
의존성을 확인한 뒤 Airflow의 제한된 host 시작 경로로 시험한다. `airflow-runtime.sh`의
`up`·`restart`·`start`는 `enabled:true`인 DAG를 다시 켜므로, 이 명령은 가동 검증을
마친 뒤에만 실행한다. PR 검증 중에는 실행하지 않는다. 중단은 systemd를 통해 수행한다.
`ExecStopPost`가 이번 `INVOCATION_ID`에 속한 컨테이너만 끝까지 정리한다. 수동 CLI 검사는
운영 예약과 겹치지 않는 격리 시험 환경에서만 한다. 테스트용 HadoopCatalog 증거를 운영 R2
반영 증거로 바꾸어 읽지 않는다. 실제 설치와 자료 반영 상태는
[출시 로드맵](../../../../docs/roadmap/production-readiness.md)이 정본이다.

이미 검증한 Cargo 산출물이 있으면 Dockerfile을 복제하거나 다시 컴파일할 필요가 없다.
[BuildKit named context](https://docs.docker.com/reference/cli/docker/buildx/build/#additional-build-contexts---build-context)로
기존 `Dockerfile.lakehouse-control`의 `rust-builder` stage만 대체할 수 있다. 이 context는
검증한 바이너리를 `src/target/release/foundation-outbox-publisher`에, 해당 빌드 환경의 공개 CA
bundle을 `etc/ssl/certs/ca-certificates.crt`에 제공한다. 경로명은 최적화 프로필의 증거가 아니다.
검증한 소스·Cargo 프로필·바이너리 hash와 최종 image ID를 함께 기록한다. 런타임 정의와 기본
Cargo release 빌드 경로는 기존 Dockerfile 하나가 계속 소유한다.

## Trino Catalog 설정

실제 catalog 파일에는 R2 key/token 이 들어가므로 git 에 커밋하지 않는다.

Trino 는 `${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}/r2.properties` **파일 하나**를 읽기 전용으로
mount 한다([ADR-0134](../../../../docs/adr/0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md) §4). compose 는 긴 문법 bind 에 `create_host_path: false` 를 두므로 파일이
없으면 Linux Docker engine 이 `bind source path does not exist` 로 컨테이너를 만들지 않는다. 예전 짧은
문법은 없는 경로를 빈 디렉터리로 만들어 Trino 가 `r2` 없이 떴다. 변수의 기본값은 개발용
`./infra/lakehouse/trino/catalog` 이다. 운영 릴리스는 읽기 전용이고 추가 파일을 가질 수 없으므로
운영에서는 반드시 릴리스 밖 디렉터리를 지정한다(예: `/var/lib/foundation-platform/trino/catalog`,
디렉터리 `0700`, 파일 `0644` — 파일 하나만 mount 하므로 Trino UID 가 디렉터리를 읽을 필요가 없다).

```bash
export FOUNDATION_PLATFORM_TRINO_CATALOG_DIR=/var/lib/foundation-platform/trino/catalog   # 개발은 생략
install -d -m 0700 "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}"
cp infra/lakehouse/trino/templates/r2-iceberg.properties.template \
   "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}/r2.properties"
chmod 0644 "${FOUNDATION_PLATFORM_TRINO_CATALOG_DIR}/r2.properties"
```

그 다음 그 파일의 placeholder 를 실제 값으로 바꾼다.

**이 파일은 Trino 가 떠 있는 동안에도 지우지 않는다.** Trino 는 시작할 때만 catalog 파일을 읽으므로, 지운 뒤에도
돌던 Trino 는 `r2` 를 계속 보여 준다. 그러다 컨테이너를 다시 만들면 `r2` 가 사라진다(2026-10-01 메모리 상한을
걸려고 다시 만들었을 때 실제로 일어났다). 지금은 다시 만들 때 파일이 없으면 기동 자체가 실패한다.
Trino 는 읽기만 하므로 R2 reader 키를 넣는다.

필수 값:

```text
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID
FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY
```

## Trino 실행

같은 `FOUNDATION_PLATFORM_TRINO_CATALOG_DIR` 를 export 한 shell 에서 띄운다. 파일이 없으면 여기서 실패한다.

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-query up -d trino
docker compose -f compose.lakehouse.yml --profile lakehouse-query ps
```

Trino 는 기본적으로 `127.0.0.1:18081` 에만 노출된다.

```bash
docker exec -it foundation-platform-trino trino
```

예시 query:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM r2;
SHOW TABLES FROM r2.silver;
SELECT * FROM r2.silver.industrial_complexes LIMIT 10;
```

## Spark 실행

Spark profile 은 container 를 오래 띄워 두는 batch job shell 로 시작한다.

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm spark spark-submit --version
```

Spark container 는 repo 전체를 mount 하지 않는다. 컨테이너가 보는 것은 lakehouse Spark job, lakehouse
contract, `target/lakehouse` output 뿐이다. `.env.local`, `.git`, source tree 전체를 compute container 에
넣지 않는다.

Linux bind mount 는 host directory 가 root 소유로 자동 생성될 수 있다. `lakehouse-batch` profile 은
`lakehouse-target-init` 을 먼저 실행해 `target/lakehouse` 를 Spark container uid/gid
(`FOUNDATION_PLATFORM_LAKEHOUSE_UID`, `FOUNDATION_PLATFORM_LAKEHOUSE_GID`, 기본 `185:185`) 로 맞춘 뒤 Spark 를 시작한다.

Bronze -> Silver 변환 contract smoke:

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm spark spark-submit \
  /workspace/infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py \
  --input /workspace/infra/lakehouse/spark/fixtures/bronze/industrial_complexes.jsonl \
  --output /workspace/target/lakehouse/silver/industrial_complexes \
  --summary-output /workspace/target/lakehouse/smoke/summaries/industrial_complexes.json \
  --lineage-output /workspace/target/lakehouse/smoke/summaries/industrial_complexes_lineage.json
```

이 job 은 `infra/lakehouse/spark/fixtures/bronze/industrial_complexes.jsonl` 을 읽고
`silver.industrial_complexes` column contract 에 맞춘 Parquet 을 `target/lakehouse` 아래에 쓴 뒤
다시 읽어 quality gate 를 검증한다.

Scalar Silver handoff -> lakehouse smoke:

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm spark spark-submit \
  /workspace/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py \
  --input /workspace/infra/lakehouse/spark/fixtures/silver_handoff/building_register_floors.jsonl \
  --contract silver.building_register_floors \
  --output /workspace/target/lakehouse/smoke/silver/building_register_floors \
  --expected-count 2 \
  --summary-output /workspace/target/lakehouse/smoke/building_register_floors-summary.json
```

이 경로는 Rust 가 이미 정규화해서 내보낸 Silver JSONL handoff 를 저장 엔진으로 넘기는 용도다.
Spark 는 record 를 다시 정규화하지 않고, 계약 컬럼/필수값/체크섬/상태값을 검증한 뒤
Parquet 또는 Iceberg 에 쓰고 다시 읽어 `foundation-platform.spark_run_summary.v1` 를 남긴다.
`docker compose run` 으로 live Iceberg smoke 를 실행할 때는 `.env.lakehouse` 를 host shell 에서
source 한 뒤 `-e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE
-e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN` 처럼 변수 이름을 명시해서 Spark container 에 전달한다.
`--env-file` 은 Compose interpolation 에 쓰이지만, run container 의 process env 로 모든 값을 자동 주입하지
않는다는 점을 전제로 둔다.

Spark job 은 성공 시 `foundation-platform.spark_run_summary.v1` JSON 을 출력하고, smoke 는
`target/lakehouse/smoke/summaries/industrial_complexes.json` 도 검증한다. 이 summary 는
Rust control-plane 이 batch audit, lineage, promotion 판단에 사용할 handoff contract 다.
secret 은 포함하지 않고 `job_name`, `contract`, `write_mode`, `write_disposition`, target,
row count, persisted row count, source snapshot ids, quality metrics 만 담는다.
Rust 쪽에서는 `lakehouse-domain::SparkRunSummary` 가 이 JSON 을 파싱하고 static
`LakehouseTableContract` 와 대조한다. schema version, column order, required columns,
target/write mode, row count, persisted row count, full source lineage, blocking quality metric 이
맞지 않으면 promotion 입력으로 쓰지 않는다.
검증된 summary 는 `lakehouse-application::ports::LakehouseBatchRunAudit` port 를 통해
`catalog.lakehouse_batch_run` 에 저장한다. 이 audit row 는 원본 summary JSONB 를 보존하면서
contract, target, write disposition, row count, source snapshot ids 를 별도 컬럼으로 둔다.
따라서 DB 는 전체 lakehouse 원본을 담지 않고, promotion/운영 감사에 필요한 얇은 control-plane
metadata 만 보관한다.

Blocking quality rule contract 는
[`docs/data-quality/lakehouse-quality-rules.v1.example.json`](../data-quality/lakehouse-quality-rules.v1.example.json)
에 고정한다. 이 fixture 는 `foundation-platform.spark_run_summary.v1` 에서 읽을 metric 중
promotion 을 막는 rule 을 `silver.industrial_complexes` 와 `gold.complex_catalog` 별로
명시한다. `foundation-outbox-publisher evaluate-lakehouse-quality-rules`
는 Spark run summary JSON 에 이 rule 을 적용해 blocking metric 위반을 실패로 만들며, rules JSON 이
계약과 다르면 파싱 단계에서 즉시 실패한다. (별도 static shape gate 였던
`check-lakehouse-quality-rules` 는 2026-06-22 self-verifying evidence-gate ceremony 정리에서 삭제되었다.)
Great Expectations / Soda 같은 external runtime DQ framework 는 아직 없다.

Lineage event contract 는
[`docs/events/lineage/lakehouse-lineage-event.v1.example.json`](../events/lineage/lakehouse-lineage-event.v1.example.json)
에 고정한다. 이 fixture 는 `foundation-platform.spark_run_summary.v1` 과 연결되는
`foundation-platform.lakehouse_lineage_event.v1` 예시이며, `silver.industrial_complexes` 입력에서
`gold.complex_catalog` 출력으로 이어지는 source snapshot, quality metric, column lineage 를
담는다. lineage event contract 검증은 publish 경로가 수행한다 —
`foundation-outbox-publisher publish-lakehouse-lineage-event` 가 emit 전에
`LakehouseLineagePublisher::validate_event` 로 artifact 를 검증한다. (별도 fixture shape gate 였던
`check-lineage-contract` 는 2026-06-22 self-verifying evidence-gate ceremony 정리에서 삭제되었다.)
`industrial_complex_bronze_to_silver.py` 와
`industrial_complex_silver_to_gold.py` 는 materialized write 성공 시 `--lineage-output` 경로에 같은
schema 의 runtime lineage artifact 를 쓴다. 아직 OpenLineage / Marquez receiver E2E 는 없다.

Lineage artifact 를 endpoint 로 보내기 전에는 dry-run plan 을 먼저 생성한다.

```bash
FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH_INPUT_PATH=target/lakehouse/smoke/summaries/gold_complex_catalog_lineage.json \
FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH_ENDPOINT="https://lineage.example.com/api/v1/lineage" \
FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH_PLAN_OUTPUT_PATH=target/lakehouse/lineage-publish-plan.json \
cargo run -p foundation-outbox-publisher -- publish-lakehouse-lineage-event
```

실제 network emit 은 `FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH_EXECUTE` 와
`FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH_CONFIRM_LINEAGE_NETWORK_EMIT` 를 둘 다 설정해야 열린다.
Bearer token 이 필요한 endpoint 는 `FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH_AUTH_TOKEN_ENV` 로
process 환경변수 이름만 넘긴다. 이 명령은
OpenLineage / Marquez receiver E2E 를 대체하지 않으며, production receiver URL 과 수신 검증은 별도
운영 cutover 항목이다.

Rust 경로에서는 `foundation-outbox::LakehouseLineagePublisher` 가 같은 event contract 를 검증한 뒤
HTTPS 또는 loopback receiver 로 POST 할 수 있다. 이 publisher 도 production OpenLineage / Marquez
receiver E2E 를 대체하지 않는다.

Application orchestration 은 `lakehouse-application::RecordLakehouseBatchRun` use case 가 맡는다. 이 use case 는
Spark stdout line 이나 `--summary-output` 파일에서 얻은 JSON 문자열을 받아 domain summary 로 파싱하고,
static table contract 검증이 끝난 경우에만 audit port 를 호출한다. foundation-platform API bootstrap 은
이 use case 를 `PgLakehouseBatchRunAudit` 과 연결해 둔다.

후속 promotion workflow 는 `lakehouse-application::GetLakehousePromotionCandidate` 를 통해 후보를 읽는다.
`PgLakehouseBatchRunRepository` 는 `validate_only` 를 제외하고, source snapshot id 가 잘리지 않았고,
persisted row count 가 candidate row count 와 같은 최신 row 만 반환한다. use case 는 반환된 row 의
정규 컬럼과 `summary_json` 을 다시 맞춰 보고 static contract 검증을 한 번 더 수행한다.

R2 Data Catalog / Iceberg write smoke (live R2/Iceberg credential 을 환경에 주입한 뒤 실행):

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI="<catalog-uri>" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE="foundation-platform" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN="<catalog-token>" \
  spark spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages "$(python3 -c 'import sys; sys.path.insert(0, "infra/lakehouse/spark/jobs"); from lakehouse_engine import iceberg_packages; print(iceberg_packages())')" \
  /workspace/infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py \
  --input /workspace/infra/lakehouse/spark/fixtures/bronze/industrial_complexes.jsonl \
  --write-mode iceberg \
  --iceberg-table industrial_complexes_smoke \
  --summary-output /workspace/target/lakehouse/smoke/summaries/industrial_complexes_iceberg.json \
  --lineage-output /workspace/target/lakehouse/smoke/summaries/industrial_complexes_iceberg_lineage.json
```

write smoke가 성공하면 read-only smoke는 같은 전용 테이블을 다음
`smoke-vworld-cadastral` / `smoke-r2` subcommands or a direct Trino read:

```sql
SELECT * FROM r2.silver.industrial_complexes_smoke LIMIT 1;
```

기본 target 은 `silver.industrial_complexes_smoke` 이다. live catalog 를 건드리려면 live write 모드를
명시해야 하며, table 이름이 `_smoke` 로 끝나지 않으면 non-smoke 허용 flag 없이는 거부한다.
non-smoke table 은 fixture 입력을 사용할 수 없다. canonical table 을 대상으로 할 때는 실제 Bronze
handoff 경로를 입력으로 명시해야 한다.
Spark job 은 Cloudflare R2 Data Catalog 의 Iceberg REST endpoint 에 붙고, token 은 Compose run
환경변수 전달로만 넘긴다. script 는 secret 값을 출력하지 않는다.
live write smoke 도 같은 run summary contract 를 `industrial_complexes_iceberg.json` 으로 남기고,
`foundation-platform.lakehouse_lineage_event.v1` runtime lineage artifact 를 쓴 뒤 target qualified table,
persisted row count, lineage shape 를 검증한다.

### Industrial-complex Gold projection write smoke

Silver handoff 또는 smoke 입력이 있으면 Gold `complex_catalog` projection을 전용 Iceberg
smoke 테이블에 기록한다.

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI="<catalog-uri>" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE="foundation-platform" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN="<catalog-token>" \
  spark spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages "$(python3 -c 'import sys; sys.path.insert(0, "infra/lakehouse/spark/jobs"); from lakehouse_engine import iceberg_packages; print(iceberg_packages())')" \
  /workspace/infra/lakehouse/spark/jobs/industrial_complex_silver_to_gold.py \
  --input-mode iceberg \
  --write-mode iceberg \
  --source-iceberg-table industrial_complexes_smoke \
  --target-iceberg-table complex_catalog_smoke \
  --iceberg-snapshot-id "<source-snapshot-id>" \
  --summary-output /workspace/target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg.json \
  --lineage-output /workspace/target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg_lineage.json
```

#### 실 테이블 재실행 (`gold.complex_catalog`)

계약이 넓어졌을 때 실 테이블을 다시 만드는 경로다. smoke 실행과 다른 점은 세 가지다.

```bash
  --source-iceberg-table industrial_complexes \
  --target-iceberg-table complex_catalog \
  --iceberg-write-mode overwrite \
  --allow-non-smoke-overwrite \
```

- **`overwrite` 이지 `append` 가 아니다.** `gold.complex_catalog` 는 투영이고 품질 게이트가
  `complex_id` 당 한 행을 요구한다. append 하면 같은 단지가 두 행이 되고, 잡이 쓴 뒤 다시 읽어
  검증하는 `assert_unique_complex_ids` 가 그 자리에서 실패한다. 통과하더라도 canonical 적재기가
  `official_complex_code` 중복으로 전체를 거부한다. 데이터를 잃는 선택이 아니다 — Iceberg 의
  overwrite 는 새 스냅샷을 만들 뿐이고 이전 스냅샷은 시간여행으로 그대로 남는다.
- **`--allow-non-smoke-overwrite` 가 필요하다.** 잡이 실 테이블 overwrite 를 기본으로 거부한다.
- **스키마 진화가 먼저 돈다.** 실 테이블은 이전 계약으로 만들어져 있으므로, 쓰기 전에 계약에
  있고 표에 없는 컬럼을 `ALTER TABLE ... ADD COLUMN` 으로 맞춘다. 어떤 컬럼이 더해졌는지는
  실행 요약의 `schema_evolution_added_columns` 에 나온다. 이미 맞는 표에는 아무것도 하지 않는다.

기본 target은 `gold.complex_catalog_smoke`다. job은 non-smoke
flag is present, and it refuses fixture input for non-smoke targets. The Spark job emits a
`foundation-platform.spark_run_summary.v1` summary with `contract = gold.complex_catalog`,
`persisted_row_count`, source snapshot ids, and blocking quality metrics, applies the lakehouse
quality rules (`foundation-outbox-publisher evaluate-lakehouse-quality-rules`), writes the
`--lineage-output`, and is verified against the `foundation-platform.lakehouse_lineage_event.v1` contract.

전체 Silver-to-Gold chain을 검증하려면 같은 전용 smoke 테이블을 대상으로 Bronze-to-Silver
job 다음 Silver-to-Gold job을 순서대로 실행한다. Silver job은
`silver.industrial_complexes_smoke`, and the Gold job reads that Iceberg table as its source and
writes `gold.complex_catalog_smoke`. The Gold run summary must show `input.kind = iceberg` and
`input.qualified_table = r2.silver.industrial_complexes_smoke`.

### 계약이 넓어졌을 때의 실 테이블 재실행 3단계 (root ADR-0044)

원천 컬럼을 계약에 새로 올리면 **Bronze JSONL 재생성 → Silver 재실행 → Gold 재실행** 순서를
전부 돌려야 새 컬럼이 실제로 채워진다. 중간부터 시작하면 아래 층의 값이 없으므로 새 컬럼이
전부 `null` 인 채로 승격된다.

1. **Bronze JSONL 재생성** — `export-industrial-complex-bronze-raw-jsonl`.
   출력 경로가 이미 있으면 **거부한다**(append-only 증거). 새 경로를 준다.
2. **Silver 재실행** — `--iceberg-write-mode overwrite --allow-non-smoke-overwrite`.
   `append` 는 안 된다: 같은 `source_snapshot_id` 로 두 번 쓰면
   `(official_complex_code, source_snapshot_id)` 유일성 게이트가 쓰고 다시 읽는 자리에서 실패한다.
3. **Gold 재실행** — 같은 이유로 `overwrite` + `--allow-non-smoke-overwrite`, 그리고 2단계가 만든
   Silver 스냅샷 id 를 `--iceberg-snapshot-id` 로 넘긴다.

두 잡 모두 쓰기 직전에 **스키마 진화 단계**를 돈다. 발행된 표는 이전 계약으로 만들어져 있고
`CREATE TABLE IF NOT EXISTS` 는 없는 표만 만들므로, 넓힌 계약이 살아 있는 표에 닿는 길은 이
단계뿐이다. 어떤 컬럼이 더해졌는지는 실행 요약의 `schema_evolution_added_columns` 에 나오고,
이미 계약과 같은 표에는 아무것도 하지 않는다. 단계 자체는 두 잡이 공유하는
`platform_contracts.evolve_iceberg_table_to_contract` 하나다 — 같은 코드를 두 벌 두면 한쪽 표만
넓히는 일이 가능해진다.

### Industrial-complex Gold profile artifact export

`gold.complex_catalog` 를 Iceberg 카탈로그에서 읽어 산업단지 하나마다 프로필 객체 하나를
`gold/industrial-complex/profiles/{artifact_id}.json` 에 create-only 로 쓴다(root ADR-0036).
포인터는 발행하지 않는다 — 요약 JSON 이 다음 단계의 입력이다.

```bash
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_CONFIRM_EXPORT=true
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_OUTPUT_STORAGE_DRIVER=local   # or r2
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_OUTPUT_ROOT=target/lakehouse/gold-profiles
# 선택. 주면 요약이 각 산출물의 URL 을 함께 낸다. 서빙 호스트가 아직 없으면 비워 둔다 —
# 주소는 포인터 발행 단계의 필수 입력이다(root ADR-0037).
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_URL_TEMPLATE="https://<public-lakehouse-host>/{object_key}"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_EXPECTED_ROW_COUNT=1442
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_SUMMARY_PATH=target/lakehouse/gold-profiles/summary.json

cargo run -p foundation-outbox-publisher -- export-industrial-complex-gold-profiles
```

읽기에도 쓰기에도 `FOUNDATION_PLATFORM_LAKEHOUSE_*`(Iceberg REST) 와
`FOUNDATION_PLATFORM_R2_LAKEHOUSE_*`(객체 저장소) 설정이 필요하다. 산출물은 lakehouse 버킷의
`gold/industrial-complex/profiles/` 아래에 쓰이며, parcel marker anchor 아티팩트와 같은
자리다(root ADR-0039). 요약의 `output_bucket` 이 실제로 쓴 버킷을 적는다. 같은 Gold 스냅샷을 다시
export 하면 같은 키에 바이트가 같은 객체가 나오므로 재실행은 `reused_object_count` 로 보고되고
아무것도 덮어쓰지 않는다. 같은 키에 다른 바이트가 있으면 실패한다.

산출된 프로필의 `attributes.parcel_count` 는 **모든 행에서 0** 이고
`attributes.calculated_area_sqm` 는 **모든 행에서 null** 이다. 그 0 은 "필지가 없다"는 사실이
아니라 자리표시다: 필지 소속을 계산하지 않기로 했으므로 Spark 잡이 상수를 넣는다. 소비자는 이
값을 필지 수로 읽어서는 안 된다. 요약의 `placeholder_parcel_count_row_count` 가 그 규모를 낸다.

### Industrial-complex Gold pointer publish

위 export 가 낸 요약의 `artifacts[]` 항목 하나가 포인터 하나의 입력이다. 이 단계는
`source_record`, `file_asset`, `industrial_complex_gold_pointer`, outbox event 를 한 트랜잭션에
기록한다.

```bash
export DATABASE_URL="postgres://foundation_platform:foundation_platform_dev_2026@localhost:15434/foundation_platform"
# 아래 값은 export 요약의 artifacts[] 한 항목에서 그대로 온다.
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_COMPLEX_ID="<artifacts[].complex_id>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_CURRENT_VERSION="<artifacts[].current_version>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_EXPECTED_CURRENT_VERSION=""
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_OBJECT_KEY="<artifacts[].profile_object_key>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_URL_TEMPLATE="https://<public-lakehouse-host>/{object_key}"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE="foundation-platform.spark.industrial_complex_gold"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE_EXTERNAL_ID="<spark-run-id>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_SOURCE_SNAPSHOT_ID="<artifacts[].source_snapshot_id>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_ICEBERG_SNAPSHOT_ID="<artifacts[].iceberg_snapshot_id>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_ROW_COUNT="<artifacts[].profile_row_count>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_SIZE_BYTES="<artifacts[].profile_size_bytes>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PROFILE_CHECKSUM_SHA256="<artifacts[].profile_checksum_sha256>"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_PUBLISHED_AT_UTC="<artifacts[].published_at_utc>"

cargo run -p foundation-outbox-publisher -- publish-industrial-complex-gold-pointer
```

기존 pointer를 바꿀 때 stale-write 방지를 위해
`FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTER_EXPECTED_CURRENT_VERSION`을 사용한다.
첫 공개에서만 비워 둔다.

`..._PROFILE_URL_TEMPLATE` 은 필수이며 `{object_key}` 를 정확히 한 번 담아야 한다(root ADR-0037).
클라이언트가 그 자리를 `profile_object_key` 또는 `spatial_locator_object_key` 로 치환해 주소를
만든다. export 요약의 `profile_url_template` 과 같은 값을 쓴다.

### Industrial-complex canonical load (Gold → Postgres)

`gold.complex_catalog` 의 현재 Iceberg 스냅샷을 읽어 `catalog.industrial_complex` 에
`official_complex_code` 자연키로 upsert 한다(root ADR-0040). `GET /catalog/v1/complexes` 가 읽는
표가 바로 이것이므로, 이 커맨드가 그 엔드포인트의 생산자다.

```bash
export DATABASE_URL="postgres://foundation_platform:<password>@localhost:15434/foundation_platform"
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_CONFIRM=true
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_EXPECTED_ROW_COUNT=1442   # 선택
export FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_SUMMARY_PATH=target/lakehouse/canonical-load/summary.json

cargo run -p foundation-outbox-publisher -- load-industrial-complex-canonical
```

읽기에 `FOUNDATION_PLATFORM_LAKEHOUSE_*`(Iceberg REST) 와 `FOUNDATION_PLATFORM_R2_LAKEHOUSE_*`
(객체 저장소) 설정이 필요하다. 요약 JSON 이 읽은 행수 · 삽입 · 갱신 · 무변경 · 건너뜀을 낸다.

주소·상태·지정일·착공일·준공일·관리기관·시행자·시도/시군구 코드에 더해, 분양상태·조성진행률·
사업기간(원문과 파생 두 달)·지정근거법·개발방식·조성목적·유치업종이 모두 신원 필드와 같은 Gold
행에서 함께 적재된다(root ADR-0044). `lakehouse_complex_id` 도 함께 쓴다 — Gold 프로필 오브젝트
키가 그 값에서 파생되므로, 그 칸이 없으면 canonical 행이 R2 의 어떤 오브젝트도 가리키지 못한다.

이 커맨드가 **쓰지 않는 것**을 요약이 스스로 적는다.

- `columns_left_null` — `primary_bjdong_code` 는 항상 `null` 이다. `silver.industrial_complexes`
  1,442행 중 그 값을 담은 행이 **0건**이라 Gold 투영이 그 컬럼을 아예 나르지 않는다. 채우는
  생산자가 생기면 그때 Silver → Gold → canonical 순으로 따라온다.
- `gold_columns_not_loaded` — Gold 계약 컬럼에서 이 커맨드가 읽는 것을 뺀 나머지다. 하드코딩된
  목록이 아니라 계약과 대조해 만들므로, Gold 에 컬럼이 늘고 여기서 읽지 않으면 다음 실행의
  요약에 그대로 드러난다.
- `skipped_rows` — `official_area_sqm` 이 없거나 정수가 아니거나 음수인 행. `area_m2` 는
  `bigint NOT NULL` 이고 반올림은 없는 값을 지어내는 것이므로 그런 행은 건너뛰고 사유와 함께 센다.

빈 값은 빈 값으로 적재된다. 준공일이 없는 단지는 `completion_date` 가 `NULL` 이고, 빈 문자열도
0 도 아니다. `status` 가 계약 밖 값이면 한 행을 건너뛰지 않고 **전체가 실패한다** — 출처의 공백과
깨진 계약은 다른 사실이다.

재실행은 안전하다. 같은 스냅샷을 다시 적재하면 모든 행이 `unchanged` 로 보고되고 버전도 올라가지
않는다. 지우는 경로는 없다 — 표에 있고 스냅샷에 없는 행은 그대로 남는다.

Spark Iceberg runtime 은 `spark-submit --packages` 로 driver 시작 전에 주입한다. Docker Spark image 는
기본 Ivy cache path 가 writable 이 아니므로 script 는 `spark.jars.ivy=/tmp/.ivy2` 를 명시한다.
`FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI` 가 없으면 job 은 catalog URI 뒤에 `/v1/oauth/tokens` 를
붙여 Iceberg REST OAuth2 endpoint 를 명시한다.

Spark container 를 띄우고 batch shell 로 들어가려면:

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm spark bash
```

Spark는 작업별 컨테이너에서 실행하므로 `docker compose ... run --rm`으로
들어갈 수 있다.

초기에는 Spark 를 product API path 에 넣지 않는다. Spark 는 다음 작업만 맡는다.

- Bronze raw object read
- Silver Parquet/Iceberg table write
- Gold projection write / smoke table write
- backfill/rewrite/compaction PoC

## 배포 때 표를 계약에 맞추기

[ADR-0124](../../../../docs/adr/0124-a-deploy-brings-every-lakehouse-table-to-its-contract.md).
`foundation-release.sh migrate` 가 DB 마이그레이션 뒤에 `foundation-lakehouse-migrate` 서비스
(`scripts/ops/lakehouse-migrate.sh` → `lakehouse_schema_migrate.py --mode apply`)를 돌린다.

| 운영 표의 상태 | 배포가 하는 일 |
|---|---|
| 계약에 있는 칸이 없음(빈 값 허용) | 칸을 계약 순서 자리에 붙인다 |
| 계약에 있는 필수 칸이 없고 데이터가 있음 | `BACKFILLS` 에 등록된 그 표의 빌드 함수로 채운다. 등록이 없으면 **배포를 멈춘다** |
| 등록된 필수 백필 칸이 이미 있지만 빈 값이 남음 | 실패 후 재시도로 보고 다시 채운다 |
| 칸 순서가 계약과 다름 | 메타데이터만으로 순서를 바꾼다(데이터 다시 쓰지 않음) |
| 칸 타입이 계약과 다름 | **배포를 멈춘다** |
| 계약에 없는 칸이 있음 | **배포를 멈춘다**. ADR-0124의 `lakehouse-known-drift.json`에 이름·타입·nullable을 명시한 칸만 값을 보존하고 별도로 보고한다 |
| 표가 아직 없음 | 그 표의 적재가 만든다 |

바꾸지 않고 보기만: 같은 작업을 `--mode plan` 으로 돌린다. 실행 기록은
`/var/lib/foundation-platform/lakehouse-migrate/runs/<시각>/run.log`.

## 평소 배포: 서버가 main 을 스스로 받는다

[ADR-0159](../../../../docs/adr/0159-the-host-deploys-main-by-itself-once-its-checks-pass.md).
`foundation-autodeploy.timer` 가 10분마다 main 의 머리를 보고, check run 이 모두 통과한 새 커밋이면
`scripts/deploy/foundation-deploy.sh <sha>` 로 배포한다. 절차 전체와 그 작업 목록은 그 스크립트와
`orchestration/jobs.v1.json` 에 있다.

| 할 일 | 명령 (root) |
|---|---|
| 자동 배포 켜기 (1회) | `echo FOUNDATION_DEPLOYER=<Airflow 상태를 가진 계정> > /etc/foundation-platform/release-deploy.conf` 뒤 다음 배포의 `timers` 가 타이머를 켠다 |
| 잠시 끄기 | `touch /etc/foundation-platform/autodeploy.off` (지우면 다시 돈다) |
| 지금 상태 | `journalctl -u foundation-autodeploy.service -n 50`, `ls /var/lib/perfectory/autodeploy/{deployed,refused,failed}` |
| 손으로 한 커밋 배포 | `bash /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-deploy.sh <sha>` |
| 실패한 커밋 다시 시도 | 원인을 고친 새 커밋을 main 에 올린다. 같은 커밋을 다시 하려면 `/var/lib/perfectory/autodeploy/failed/<sha>` 를 지운다 |

검사가 실패한 커밋(`refused`)과 배포가 실패한 커밋(`failed`)은 한 번만 Slack 으로 알리고 다시 시도하지 않는다.
배포 뒤 작업(`started_once_after_deploy`)이 실패해도 배포는 성공이고 DAG 는 다시 켜진다(루트 ADR-0173): 그 유닛의
`OnFailure` 가 Slack 에 알리고, 배포 로그에 `the post-deploy run did not succeed under <sha>: <유닛>` 한 줄이 남으며,
다음 예약 실행이 다시 한다. 이유는 `journalctl -u <유닛>` 에 있다(루트 ADR-0174). 배포가 새 릴리스로 바꾸기 전에
멈추면 DAG 는 다시 켜지고, 활성화·마이그레이션 중에 멈추면 DAG 는 멈춘 채로 남으며 배포 로그에 `THE DAGS STAY PAUSED`
줄이 남는다 — 고쳐서 다시 배포하거나, 릴리스를 확인한 뒤 켜진 작업의 DAG 를 손으로 켠다.

서버가 main 을 돌리게 되면 autodeploy 는 `foundation-worker-autodeploy.service` 를 시작해 소스가 바뀐 Cloudflare
Worker 를 따라 배포한다(루트 ADR-0175). 그 실패는 서버 배포의 결과를 바꾸지 않고, 그것이 도는 동안 다음 서버 배포는
기다린다. 토큰 두기와 켜고 끄기는 [Worker 자동 배포 런북](./worker-autodeploy.md).

## 릴리스 인증 전환 (1회)

[ADR-0134](../../../../docs/adr/0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md) 이전 운영 릴리스(비상 릴리스 — id 는 [`tools/release-retention.contract.json`](../../../../tools/release-retention.contract.json) 의 `emergency_release` 한 곳에만 있다)는 tar 로 풀린 쓰기 가능 트리이고, 안에
`bin/foundation-outbox-publisher` 와 `.foundation-floor.env` 를 가진다. 인증은 이런 트리를 추가 파일·쓰기
가능으로 거부하므로, 이 릴리스는 전환 뒤 `activate`·`rollback` 대상이 아니다. 지우지 않고 그대로 둔다(아래
"비상 복귀"가 이 디렉터리를 쓴다). 전환은 이 ADR 이 들어간 병합 커밋 `<sha>` 를 새로 설치하는 것이다. 아래는
host 관리자(root) 작업이며, 명령의 `$sha` 는 설치할 40자리 커밋이다.

### 1. 선행 조건 확인

없으면 설치가 그 이름을 대며 실패한다. 우회 스위치는 없다. 한 번에 확인한다 — 모든 줄이 `ok` 여야 한다.

```bash
sudo bash -c '
check() { if eval "$2" >/dev/null 2>&1; then echo "ok   $1"; else echo "FAIL $1"; fi; }
owned() { [[ "$(stat -c %U:%G "$1")" == root:root && $(( 0$(stat -c %a "$1") & 022 )) == 0 ]]; }
for d in /opt /opt/foundation-platform /opt/foundation-platform/releases /var/lib /var/lib/perfectory; do
  [[ -e "$d" ]] || install -d -o root -g root -m 0755 "$d"
  check "root:root, no group/other write: $d" "owned $d"
done
check "root reads repository identity, no gh" "curl -q --proto =https --silent --fail --max-time 30 -H \"Accept: application/vnd.github+json\" https://api.github.com/repos/perfectory-inc/perfectory-public | grep -qF perfectory-inc/perfectory-public"
check "root fetches canonical main"       "git ls-remote --exit-code https://github.com/perfectory-inc/perfectory-public.git refs/heads/main"
check "docker buildx is installed"        "docker buildx version"
check "python3 is /usr/bin/python3"       "[[ -x /usr/bin/python3 ]]"
'
```

`gh` 는 필요 없다. 공개 저장소 identity 는 자격증명 없이 읽는다([ADR-0136](../../../../docs/adr/0136-release-admission-reads-the-public-repository-identity-without-a-github-login.md)) — 조회가 `FAIL` 이면 root 의 HTTPS 출구(프록시·방화벽)나 같은 IP 의 익명 요청 한도(시간당 60회)를 본다. Buildx 가 없으면
`docker-buildx-plugin` 을 설치한다. 첫 빌드는 publisher 와 Spark JAR 을 받아 시간이 걸리고
`/opt/foundation-platform/artifacts` 에 쌓인다(데이터 디스크 여유를 먼저 본다).

### 2. 제어 체크아웃 설치·갱신

배포기와 검증기는 이 체크아웃에서만 실행된다. 정본 main 의 커밋 하나를 root 소유로 풀고 그 커밋을
`.perfectory-control-commit` 에 적는다. `prepare` 는 이 커밋이 설치하려는 릴리스의 조상(또는 같은 커밋)이 아니면
거부한다 — 제어 코드와 릴리스가 다른 역사에서 오면 바로 드러난다. 설치와 갱신은 같은 명령이다.

```bash
sha=<검토한 main 커밋 40자리>
sudo bash -euo pipefail -c '
sha="$1"; mirror=/var/lib/perfectory/control-source.git
install -d -o root -g root -m 0755 /opt/perfectory-control /opt/perfectory-control/releases /var/lib/perfectory
[[ -d "$mirror" ]] || git init -q --bare "$mirror"
git --git-dir="$mirror" fetch -q --no-tags https://github.com/perfectory-inc/perfectory-public.git +refs/heads/main:refs/heads/main
git --git-dir="$mirror" merge-base --is-ancestor "$sha" refs/heads/main     # main 에 병합된 커밋만
target=/opt/perfectory-control/releases/$sha
if [[ ! -e "$target" ]]; then
  staging=$(mktemp -d /opt/perfectory-control/releases/.staging.XXXXXX)
  git --git-dir="$mirror" archive "$sha" | tar -x --no-same-owner -C "$staging"
  printf "%s
" "$sha" >"$staging/.perfectory-control-commit"
  chown -R root:root "$staging"; chmod -R u+rwX,go+rX,go-w "$staging"; chmod 0755 "$staging"
  mv -T "$staging" "$target"
fi
ln -sfn "releases/$sha" /opt/perfectory-control/current.next
mv -T /opt/perfectory-control/current.next /opt/perfectory-control/current
' _ "$sha"
sudo stat -c '%U:%G %a %n' /opt/perfectory-control /opt/perfectory-control/current/scripts/deploy/foundation-release-admission.py
```

결과: 디렉터리 `root:root 0755`, 파일 `root:root 0644`(실행 파일 `0755`), 쓰기는 root 만. 갱신은 같은 명령을 새
`sha` 로 다시 실행한다. 새 릴리스를 설치하기 **전에** 제어 체크아웃을 그 릴리스와 같거나 이전의 main 커밋으로
맞춘다. 옛 `releases/<sha>` 는 `activate`·`install` 끝의 `prune` 이 지운다(아래 "릴리스 보존").

### 3. sudo 규칙 — 새 규칙을 먼저, 옛 줄은 마지막에

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh deployer-access <배포 계정>
```

정확한 경로 하나만 허용하는 `/etc/sudoers.d/foundation-release` 를 설치한다. 옛
`/opt/foundation-platform/*/scripts/deploy/foundation-release.sh` 줄이 아직 있으면 그 파일과 줄을 보여 주고
`deployer-access-installed` 로 끝난다. 그 줄은 아직 지우지 않는다(전환이 끝날 때까지 기존 경로를 남긴다).
옛 줄 삭제와 `--exclusive` 확인은 이 절의 마지막 단계(8)다.

### 4. 설치·빌드

```bash
git archive --format=tar.gz -o /tmp/foundation-$sha.tar.gz "$sha:platforms/foundation-platform"
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh prepare $sha /tmp/foundation-$sha.tar.gz
```

`prepare` 는 등록 작업(`orchestration/jobs.v1.json`)의 유닛이 하나라도 돌고 있으면 그 이름을 대며 거부한다. 빌드가
호스트의 일회성 작업으로 예산에 들어가 있기 때문이다([ADR-0137](../../../../docs/adr/0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md)).
DAG 를 먼저 멈추고 돌던 작업이 끝난 뒤 실행한다. 실행 중인 작업은 `activating` 으로 보인다(`systemctl show -p ActiveState`). 빌드하는
동안에는 잠금 때문에 등록 작업이 시작하자마자 거부되고 Airflow 가 재시도한다. 빌드는 2 CPU·`22g`(정본 `tools/release-build.contract.json`)로 약
43분 걸린다(2026-10-03 측정).

`/opt/foundation-platform/artifacts/$sha/build.json` 에 `publisher_image`(image ID)와
`publisher_tag`(`foundation-outbox-publisher:$sha`)가 기록된다. 태그가 있으므로 `docker image prune` 이 지우지
않는다. 그래도 이미지가 없어지면 `verify-current` 가 거부하므로 등록 작업이 시작되지 않는다. 복구는
`sudo mv /opt/foundation-platform/artifacts/$sha /opt/foundation-platform/artifacts/.pruned-$sha` 뒤 같은 `prepare`.

**tippecanoe 이미지를 기록하지 않은 옛 빌드.** 릴리스 빌드가 `foundation-tippecanoe:$sha` 를 함께 만들고
`tippecanoe_image`·`tippecanoe_tag` 를 기록하기 시작한 것은 PR #318 부터다. 그 전에 `prepare` 한 릴리스의
`build.json` 에는 두 칸이 없다. 그래서 제어 체크아웃을 #318 이후 커밋으로 올리면:

- 현재 릴리스가 #318 이전 빌드면 `verify-current` 가 `build output does not record this release's tippecanoe
  image tag` 로 거부하고, 등록 작업(`ExecStartPre`)이 하나도 시작하지 않는다. 새 릴리스를 `prepare` 해서
  전환하거나, 같은 릴리스를 다시 빌드한다(위 복구와 같이 artifacts 를 옆으로 옮긴 뒤 `prepare` — 단 제어
  체크아웃이 그 릴리스와 같거나 이전이어야 하므로, 옛 릴리스는 새 제어 체크아웃으로 다시 빌드할 수 없다).
  `admitted-writer-runtime.sh` 도 같은 칸이 없으면 `build manifest records no tippecanoe image` 로 거부한다.
- `rollback` 명령 자체는 영향이 없다: 링크만 바꾸고 승인 검사를 거치지 않는다. 다만 #318 이전 빌드로
  되돌린 뒤 등록 작업을 돌리려면 `verify-current` 가 그 빌드를 받아들여야 하므로, 제어 체크아웃도 그 릴리스와
  같거나 이전의 main 커밋으로 되돌린다(2절 명령, 그 커밋의 검증기는 tippecanoe 칸을 요구하지 않는다).

### 5. FLOOR 설정 이전

```bash
old=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["emergency_release"])' /opt/perfectory-control/current/tools/release-retention.contract.json)
sudo cp /opt/foundation-platform/releases/$old/.foundation-floor.env /tmp/floor-$sha.env
image=$(sudo python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["publisher_image"])' \
  /opt/foundation-platform/artifacts/$sha/build.json)
sudo sed -i -e "s|^FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=.*|FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=/opt/foundation-platform/releases/$sha|" \
  -e "s|^FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=.*|FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE=$image|" /tmp/floor-$sha.env
sudo chown root:root /tmp/floor-$sha.env && sudo chmod 0644 /tmp/floor-$sha.env
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh floor-config $sha /tmp/floor-$sha.env
```

`FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE` 는 `build.json` 의 `publisher_image` 와 같아야 한다. 다른 digest 는
`floor-config` 가 거부하고, 실행 때도 `admitted-writer-runtime.sh` 가 다시 거부한다.

### 6. Trino catalog — 옮기지 말고 복사

지금 Trino 는 `~perfectory/foundation-platform-compute` 에서 compose 프로젝트 `foundation-platform-compute` 로
돌고, 그 사본의 compose 는 짧은 bind 문법이다. 원본을 **옮기면**(mv) 그 사본으로 다시 띄우는 순간 빈 디렉터리가
생겨 2026-10-01 처럼 `r2` 가 사라진다. 원본은 그대로 두고 **복사**한다.

```bash
sudo install -d -o root -g root -m 0700 /var/lib/foundation-platform/trino/catalog
sudo cp ~perfectory/foundation-platform-compute/infra/lakehouse/trino/catalog/r2.properties \
  /var/lib/foundation-platform/trino/catalog/r2.properties
sudo chmod 0644 /var/lib/foundation-platform/trino/catalog/r2.properties
# 같은 프로젝트 이름이므로 기존 컨테이너(foundation-platform-trino, 127.0.0.1:18081)를 교체한다. 다른
# 프로젝트 이름으로 띄우면 컨테이너 이름과 18081 이 충돌한다.
FOUNDATION_PLATFORM_TRINO_CATALOG_DIR=/var/lib/foundation-platform/trino/catalog \
  docker compose -p foundation-platform-compute --project-directory /opt/foundation-platform/current \
  -f /opt/foundation-platform/current/compose.lakehouse.yml --profile lakehouse-query up -d --force-recreate trino
until docker exec foundation-platform-trino trino --execute 'SELECT 1' >/dev/null 2>&1; do sleep 2; done
docker exec foundation-platform-trino trino --execute 'SHOW CATALOGS' | tr -d '"' | grep -qx r2 && echo 'r2 catalog ok'
```

`r2 catalog ok` 가 나오지 않으면 같은 명령의 `--project-directory`/`-f` 를 `~perfectory/foundation-platform-compute`
로 바꿔 예전 사본으로 되돌린다(원본 파일이 그대로 있으므로 그대로 뜬다).

### 7. 활성화

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh activate $sha
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh migrate
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh timers ~/airflow-state/scheduler_ed25519.pub
systemctl cat foundation-map-edit-fold@complex.service | grep 'admission.py verify-current'
```

`timers` 가 등록 작업의 unit 과 `foundation-lakehouse-migrate.service` 에 `10-release-admission.conf` 를 설치한다.
템플릿 인스턴스는 템플릿(`foundation-map-edit-fold@.service.d/`)에 들어간다. 각 작업을 한 번 시작해
(`sudo systemctl start foundation-outbox-publish.service; systemctl show -p Result --value foundation-outbox-publish.service`
가 `success`) 확인한 뒤 DAG 를 켠다. 등록 작업은 더 이상 `/var/lib/foundation-platform/bin` 의 바이너리를 쓰지 않는다.
그 파일은 지우지 않는다. 수동 적재 스크립트(`scripts/load/*-handoff-export.sh`)는 새 release 안에 `bin/` 이 없으므로
`FOUNDATION_PLATFORM_PUBLISHER_BIN=/opt/foundation-platform/artifacts/<sha>/foundation-outbox-publisher` 를 명시해 돌린다.

`activate`·`install` 은 전환이 끝난 뒤 `prune` 을 돌린다(아래 "릴리스 보존"). 출력 마지막 줄이 `prune-ok`·`prune-skipped` 가 아니면
`RELEASE PRUNE FAILED` 가 함께 나오고 journal 에도 남는다 — 활성화는 그대로 유효하다. 원인을 고친 뒤 `prune` 만 다시 돌린다.

### 8. 옛 sudo 줄 삭제 (마지막)

새 경로로 7단계까지 끝난 뒤에만 한다.

```bash
sudo grep -rn 'foundation-release\.sh' /etc/sudoers /etc/sudoers.d/   # 옛 줄이 있는 파일 확인
sudo visudo -f /etc/sudoers.d/<그 파일>                                   # 옛 와일드카드 줄만 지운다
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh deployer-access <배포 계정> --exclusive                         # deployer-access-ok 여야 한다
```

### 비상 복귀 — 인증 이전 릴리스로

먼저 인증된 다른 릴리스로 `rollback`/`activate` 한다. 그것이 불가능할 때(GitHub·Buildx 장애로 새 인증 불가,
또는 인증 이전 릴리스로 돌아가야 할 때)만 root 가 아래를 그대로 실행하고 사고 기록에 시각·이유를 남긴다.
`rollback` 은 인증되지 않은 이 릴리스를 거부하므로 쓰지 않는다.

```bash
# 비상 릴리스 id 는 계약 한 곳에만 있다. prune 은 이 릴리스를 지우지 않는다.
old=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["emergency_release"])' /opt/perfectory-control/current/tools/release-retention.contract.json)
# 1) DAG 정지 (켜 둔 채 바꾸면 전환 도중 시작된 실행이 섞인다)
for id in $(python3 -c 'import json,sys; [print(j["id"]) for j in json.load(open(sys.argv[1]))["jobs"] if j["enabled"]]' \
    /opt/foundation-platform/releases/$old/orchestration/jobs.v1.json); do
  bash /opt/foundation-platform/releases/$old/scripts/deploy/airflow-runtime.sh exec airflow-scheduler airflow dags pause "foundation_$id"
done
# 2) current 를 옛 릴리스로 (원자적 교체)
sudo ln -sfn releases/$old /opt/foundation-platform/current.next
sudo mv -T /opt/foundation-platform/current.next /opt/foundation-platform/current
# 3) 인증 drop-in 제거 — 인스턴스와 템플릿 모두, 빈 디렉터리도
sudo find /etc/systemd/system -path '/etc/systemd/system/foundation-*.service.d/10-release-admission.conf' -print -delete
sudo find /etc/systemd/system -maxdepth 1 -type d -name 'foundation-*.service.d' -empty -print -delete
# 4) 옛 릴리스의 unit 재설치 (FLOOR unit 은 릴리스 안 .foundation-floor.env 를 읽는 옛 형태)
sudo install -o root -g root -m 0644 -t /etc/systemd/system \
  /opt/foundation-platform/releases/$old/infra/systemd/*.service /opt/foundation-platform/releases/$old/infra/systemd/*.timer
sudo systemctl daemon-reload
```

확인 — 모든 줄이 기대와 같아야 DAG 를 다시 켠다.

```bash
readlink /opt/foundation-platform/current                                  # releases/$old
sudo find /etc/systemd/system -name 10-release-admission.conf | wc -l      # 0
systemctl cat foundation-building-register-floor.service | grep -c foundation-floor.env   # 1
for svc in foundation-outbox-publish.service foundation-source-sweep.service \
    'foundation-map-edit-fold@admin.service' 'foundation-map-edit-fold@complex.service' \
    foundation-lineage-stewardship.service foundation-data-quality.service; do
  sudo systemctl start "$svc"
  printf '%s %s\n' "$svc" "$(systemctl show -p Result --value "$svc")"   # 각 줄 success
done
```

FLOOR(`foundation-building-register-floor.service`)는 최대 4시간 걸리므로 손으로 시작하지 않고 DAG 를 켠 뒤 Airflow 의
첫 실행이 `success` 인지(`airflow-runtime.sh exec airflow-scheduler airflow dags list-runs foundation_building_register_floor -o plain`)
본다. 모두 확인되면 1) 에서 멈춘 DAG 를 `airflow dags unpause` 로 켠다. 원인을 고친 뒤에는 이 절의 1~8단계로 다시 전환한다.

## 릴리스 보존

배포 한 번이 루트 디스크에 약 0.9GB 를 남긴다: `releases/<sha>`(대부분 15–19MB, 일부 89–241MB),
`artifacts/<sha>`(publisher 바이너리·JAR·`build.json`, 약 447MB), `config/<sha>`, 이미지 태그
`foundation-outbox-publisher:<sha>`·`foundation-tippecanoe:<sha>`(각 약 210MB). 지우는 단계가 없어서
2026-10-03 ADR-0134 전환 직후 `/opt/foundation-platform/releases` 에 49개가 쌓여 있었고, 루트 디스크가
2026-10-02 에 두 번 0 이 됐으며 2026-10-03 에는 손으로 치우기 전 82% 였다.

`foundation-release.sh prune`(root)이 남기는 것:

| 남김 | 근거 |
|---|---|
| `current`·`previous` 가 가리키는 릴리스 | `rollback` 이 `previous` 를 쓴다. `previous` 링크가 없어도 `prune` 은 돈다 |
| 비상 릴리스 | [`tools/release-retention.contract.json`](../../../../tools/release-retention.contract.json) 의 `emergency_release`. 위 "비상 복귀"가 이 디렉터리를 쓴다 |
| 설치 시각 기준 최신 `keep_newest` 개(기본 3) | 같은 계약 |
| 실행 중인 프로세스·컨테이너가 참조하는 릴리스 | 지우지 않고 `prune refused <sha>: still referenced by …` 로 이름을 댄다(종료 코드 75) |

나머지 릴리스마다 `releases/<sha>`·`artifacts/<sha>`·`config/<sha>` 를 지우고 두 이미지 태그를 지운다. 이미지는
다른 태그나 컨테이너가 쓰지 않을 때만 Docker 가 함께 지운다. 캐시된 빌드로 옛 릴리스와 남는 릴리스가 같은 image ID 를
가지면(tippecanoe 가 흔하다) 그 ID 는 남는 릴리스의 것이다: 그 이미지로 도는 컨테이너가 옛 릴리스를 붙잡지 않고, 남는
릴리스의 `build.json` 이 적은 ID 는 지우지 않는다(그 ID 의 마지막 태그면 옛 태그도 남기고 `prune kept image` 로 알린다).
Docker 가 `image inspect` 에 엉뚱한 답을 주면 그 릴리스만 `prune refused` 로 남긴다. 제어 체크아웃(`/opt/perfectory-control/releases`)도
`current` 와 최신 `keep_newest` 개만 남긴다 — 지운 제어 커밋이 다시 필요하면 위 2절 명령이 미러에서 다시 푼다.
지운 것과 확보한 바이트를 줄마다 찍고 `prune-ok removed=… freed_bytes=…` 로 끝난다. 계약을 못 읽거나
`current` 가 릴리스 링크가 아니거나 Docker 가 컨테이너 목록을 못 주면 아무것도 지우지 않는다.

`activate`·`install` 이 전환한 뒤 자동으로 돈다(별도 단계로 두지 않은 이유: 지우는 단계를 사람이 기억해야 하는 구조가
49개를 쌓았다). 릴리스 빌드가 쓰는 잠금(`foundation-release-admission.py` 의 `BUILD_LOCK`)을 끝까지 잡고 돈다. 다른
`prepare` 가 빌드 중이라 잠금을 못 잡으면 아무것도 지우지 않고 `prune-skipped: …` 를 찍고 0 으로 끝난다 — 실패 경보가
아니다. 빌드가 끝난 뒤 다음 활성화가, 또는 아래 명령이 이어서 지운다. 손으로 돌릴 때:

```bash
sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh prune
```

`prune failed <sha>: …` 가 `artifacts/<sha>` 를 지우다 멈춘 것이면 그 디렉터리가 반쯤 남는다. 그 상태로 같은 sha 를 다시
`install`/`prepare` 하면 `verify_artifacts` 가 거부한다(디렉터리가 있으면 다시 빌드하지 않고 검사만 한다). 남은 것을
손으로 지운 뒤 다시 설치한다. 읽기 전용이므로 쓰기 권한부터 돌린다:

```bash
sudo chmod -R u+w "/opt/foundation-platform/artifacts/${sha:?}" && sudo rm -rf "/opt/foundation-platform/artifacts/${sha:?}"
```

비상 릴리스를 바꾸거나 없애려면 계약의 `emergency_release` 와 위 "비상 복귀" 절을 같은 PR 에서 바꾼다. id 는 계약
밖 어디에도 적지 않는다(`foundation-release-test.sh` 가 저장소 전체에서 사본을 거부한다).

## 안전 규칙

- `gongzzang` 과 `Dawneer` 는 Trino/Spark 에 직접 붙지 않는다.
- Trino 는 운영 SQL/검증 도구이지 product request path 가 아니다.
- Spark 는 batch compute 이며 foundation-platform 의 ownership/promotion 판단을 대체하지 않는다.
- Trino catalog `*.properties` 는 secret 파일이므로 커밋하지 않고, 운영에서는 릴리스 밖에 둔다.
- 실제 table 생성과 write 는 ADR 0007 의 consumer boundary 를 지킨다.

## 다음 단계

1. R2 Data Catalog credential 로 Trino catalog smoke 를 통과시킨다.
2. `silver.industrial_complexes` 를 Trino 에서 조회한다.
3. Spark 로 Bronze sample 을 Silver Iceberg smoke table 로 쓰는 PoC 를 통과시킨다.
4. Rust `LakehouseMaintenancePolicy` plan 을 Spark rewrite job 실행과 연결한다.

## 참고

- [ADR 0007 - Netflix-style Lakehouse Compute Architecture](../adr/0007-netflix-style-lakehouse-compute-architecture.md)
- [Cloudflare R2 Data Catalog config examples](https://developers.cloudflare.com/r2/data-catalog/config-examples/)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Apache Iceberg Spark Getting Started](https://iceberg.apache.org/docs/latest/spark-getting-started/)
