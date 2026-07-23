# Remote Lakehouse Job Runner

## 목적

`foundation-outbox-publisher run-remote-lakehouse-job`는 Foundation Platform의 Rust
control-plane에서 원격 Linux Spark/Trino 작업을 실행하고, Spark가 낸
`foundation-platform.spark_run_summary.v1` 요약을 Rust 정적 계약으로 검증하는 명령이다.

이 runner는 Spark 자체를 제품 API 경로에 넣지 않는다. 역할은 세 가지다.

1. SSH로 원격 Linux compute host에 접속한다.
2. 원격 Docker Compose Spark 또는 lakehouse-control job을 실행한다.
3. Spark summary를 Rust 계약과 대조하고, 필요하면 `catalog.lakehouse_batch_run`에 audit row를 남긴다.

즉 SSH/Docker는 실행 하단이고, 성공 판정은 Foundation Platform Rust 계약이 한다.

## 지원 Job

```text
building_register_floors_smoke
building_register_floors_handoff_smoke
building_register_floors_pipeline_smoke
building_register_floors_pipeline_hub_smoke
building_register_floors_pipeline_full
building_register_units_pipeline_smoke
```

### `building_register_floors_smoke`

Spark fixture를 바로 `silver.building_register_floors` 계약으로 읽어 Iceberg smoke table에 쓴다.

```text
input:    infra/lakehouse/spark/fixtures/silver_handoff/building_register_floors.jsonl
contract: silver.building_register_floors
target:   r2.silver.building_register_floors_smoke
mode:     iceberg overwrite
rows:     2
```

### `building_register_floors_handoff_smoke`

원격 compute root에 이미 있는 handoff 파일을 읽어 같은 smoke table에 쓴다.

```text
input:    target/lakehouse/silver_handoff/building_register_floors.jsonl
contract: silver.building_register_floors
target:   r2.silver.building_register_floors_smoke
mode:     iceberg overwrite
rows:     input file row count
```

### `building_register_floors_pipeline_smoke`

원격 compute host에서 층별개요 smoke 라인을 한 번에 실행한다.

1. `target/lakehouse/bronze/source=datagokr__building_register_floor_overview` 아래의 Bronze 파일을 읽는다.
2. `lakehouse-control` 컨테이너에서 `export-building-register-floor-silver-handoff`를 실행한다.
3. 생성된 handoff를 Spark 컨테이너에 넘긴다.
4. `r2.silver.building_register_floors_smoke`에 overwrite로 쓴다.
5. Spark summary를 Rust 계약으로 검증한다.

```text
bronze:   target/lakehouse/bronze/source=datagokr__building_register_floor_overview
handoff:  target/lakehouse/silver_handoff/building_register_floors.jsonl
contract: silver.building_register_floors
target:   r2.silver.building_register_floors_smoke
mode:     iceberg overwrite
```

### `building_register_units_pipeline_smoke`

원격 compute host에서 전유부 unit smoke 라인을 한 번에 실행한다.

1. 원격 `.env.lakehouse`의 R2 credential을 읽는다.
2. `amazon/aws-cli` 컨테이너로 R2 Bronze 전유부 prefix를 `target/lakehouse/bronze/source=hubgokr__building_register_exclusive_unit`에 stage한다.
3. 같은 방식으로 R2 Bronze 표제부 witness prefix를 `target/lakehouse/bronze/source=hubgokr__building_register_main`에 stage한다.
4. `lakehouse-control` 컨테이너에서 `export-building-register-unit-silver-handoff`를 실행한다.
5. 생성된 handoff를 Spark 컨테이너에 넘긴다.
6. `r2.silver.building_register_units_smoke`에 overwrite로 쓴다.
7. Spark summary를 Rust 계약으로 검증한다.

```text
bronze:        target/lakehouse/bronze/source=hubgokr__building_register_exclusive_unit
title witness: target/lakehouse/bronze/source=hubgokr__building_register_main
handoff:       target/lakehouse/silver_handoff/building_register_units_smoke.jsonl
contract:      silver.building_register_units
target:        r2.silver.building_register_units_smoke
mode:          iceberg overwrite
max rows:      10000
```

이 job은 smoke 전용이다. non-smoke overwrite 플래그를 쓰지 않는다.

### Full / Hub Smoke

`building_register_floors_pipeline_hub_smoke`와 `building_register_floors_pipeline_full`은 층별개요
pipeline의 확장 job이다. full job은 smoke table이 아닌 canonical table을 대상으로 하므로 별도 승인 없이
실행하지 않는다.

## Fail-Fast 규칙

모든 pipeline job은 다음 조건을 먼저 확인하고, 실패하면 Spark 실행 전에 멈춘다.

- 원격 `.env.lakehouse` 파일이 있어야 한다.
- R2 credential이 있어야 한다.
- unit pipeline smoke는 Spark 실행 전 R2 Bronze prefix를 원격 staging directory로 동기화한다.
- 필요한 Bronze source directory가 staging 후 존재해야 한다.
- `lakehouse-control` Docker image build/run이 성공해야 한다.
- smoke job은 `_smoke` suffix table만 쓴다.
- full job은 non-smoke overwrite를 명시적으로 허용한 job만 가능하다.

## 환경 변수

로컬 실행자는 `.env.local` 또는 shell env에서 다음 값을 설정한다. secret 값은 remote command 문자열에 직접 넣지 않는다.

```text
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET=perfectory@<lakehouse-host>
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=/home/perfectory/foundation-platform-compute
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ENV_FILE=.env.lakehouse
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_PATH=ssh
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB=building_register_units_pipeline_smoke
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_EXECUTE=0
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT=0
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID=
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_REQUEST_ID=
```

`<lakehouse-host>`는 운영자의 내부 Linux compute host(호스트명 또는 IP)다. 실제 주소는
커밋되는 문서/코드에 넣지 않고 로컬 env에만 둔다. `FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET`과
`FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT`는 필수이며, 비어 있으면 runner가
`FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET is required` 형태의 오류로 즉시 실패한다 (fail-closed).

원격 host의 `.env.lakehouse`에는 Iceberg REST/R2 credential이 있어야 한다.

```text
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN
R2_ENDPOINT
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
```

## Dry Run

`FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_EXECUTE`가 비어 있거나 `0`이면 SSH 실행 없이 명령 계획만 출력한다.

```powershell
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET='perfectory@<lakehouse-host>'
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT='/home/perfectory/foundation-platform-compute'
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB='building_register_units_pipeline_smoke'
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_EXECUTE='0'
cargo run -p foundation-outbox-publisher -- run-remote-lakehouse-job
```

## Execute

```powershell
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_SSH_TARGET='perfectory@<lakehouse-host>'
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT='/home/perfectory/foundation-platform-compute'
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_JOB='building_register_units_pipeline_smoke'
$env:FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_EXECUTE='1'
cargo run -p foundation-outbox-publisher -- run-remote-lakehouse-job
```

성공 예시:

```text
remote-lakehouse-job-ok job=building_register_units_pipeline_smoke contract=silver.building_register_units row_count=10000 persisted_row_count=10000
```

## Audit 기록

`FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT=1`을 켜면 Spark summary 검증 후 기존
`RecordLakehouseBatchRun` use case를 통해 `catalog.lakehouse_batch_run`에 기록한다.

필수 값:

```text
DATABASE_URL
FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID
```

`FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_REQUEST_ID`는 선택 값이며 trace correlation에 사용한다. audit 기록을 켠
실행은 Spark 성공뿐 아니라 Postgres audit insert까지 성공해야 command 성공으로 본다.

## 현재 범위

이 runner는 첫 운영 자동화 단위다. 아직 전체 orchestrator가 아니다.

- Airflow, Maestro, Temporal, EMR, Fargate, Kubernetes를 대체하지 않는다.
- 나중의 orchestrator는 이 Rust command와 Spark contract를 호출하면 된다.
- `catalog.lakehouse_batch_run` audit 기록은 opt-in으로 연결되어 있다.
