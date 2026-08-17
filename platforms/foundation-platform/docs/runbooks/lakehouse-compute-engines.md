---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
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

## Trino Catalog 설정

실제 catalog 파일에는 R2 key/token 이 들어가므로 git 에 커밋하지 않는다.

ignored catalog 파일은 template 을 복사한 뒤 placeholder 를 실제 값으로 바꿔서 만든다.

```bash
cp infra/lakehouse/trino/templates/r2-iceberg.properties.template \
   infra/lakehouse/trino/catalog/r2.properties
```

그 다음 `infra/lakehouse/trino/catalog/r2.properties` 의 placeholder 를 실제 값으로 바꾼다.

필수 값:

```text
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
```

## Trino 실행

먼저 `infra/lakehouse/trino/catalog/r2.properties` 가 존재하고 template placeholder 가 남아 있지
않은지 확인한 뒤 Trino container 를 띄운다.

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
docker compose -f compose.lakehouse.yml --profile lakehouse-batch up -d spark
docker exec -it foundation-platform-spark spark-submit --version
```

Spark container 는 repo 전체를 mount 하지 않는다. 컨테이너가 보는 것은 lakehouse Spark job, lakehouse
contract, `target/lakehouse` output 뿐이다. `.env.local`, `.git`, source tree 전체를 compute container 에
넣지 않는다.

Linux bind mount 는 host directory 가 root 소유로 자동 생성될 수 있다. `lakehouse-batch` profile 은
`lakehouse-target-init` 을 먼저 실행해 `target/lakehouse` 를 Spark container uid/gid
(`FOUNDATION_PLATFORM_LAKEHOUSE_UID`, `FOUNDATION_PLATFORM_LAKEHOUSE_GID`, 기본 `185:185`) 로 맞춘 뒤 Spark 를 시작한다.

Bronze -> Silver 변환 contract smoke:

```bash
docker exec -it foundation-platform-spark spark-submit \
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
docker exec -i \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI="<catalog-uri>" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE="foundation-platform" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN="<catalog-token>" \
  foundation-platform-spark spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
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
Spark job 은 Cloudflare R2 Data Catalog 의 Iceberg REST endpoint 에 붙고, token 은 Docker exec
환경변수 전달로만 넘긴다. script 는 secret 값을 출력하지 않는다.
live write smoke 도 같은 run summary contract 를 `industrial_complexes_iceberg.json` 으로 남기고,
`foundation-platform.lakehouse_lineage_event.v1` runtime lineage artifact 를 쓴 뒤 target qualified table,
persisted row count, lineage shape 를 검증한다.

### Industrial-complex Gold projection write smoke

Silver handoff 또는 smoke 입력이 있으면 Gold `complex_catalog` projection을 전용 Iceberg
smoke 테이블에 기록한다.

```bash
docker exec -i \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI="<catalog-uri>" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE="foundation-platform" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN="<catalog-token>" \
  foundation-platform-spark spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
  /workspace/infra/lakehouse/spark/jobs/industrial_complex_silver_to_gold.py \
  --input-mode iceberg \
  --write-mode iceberg \
  --source-iceberg-table industrial_complexes_smoke \
  --target-iceberg-table complex_catalog_smoke \
  --iceberg-snapshot-id "<source-snapshot-id>" \
  --summary-output /workspace/target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg.json \
  --lineage-output /workspace/target/lakehouse/smoke/summaries/gold_complex_catalog_iceberg_lineage.json
```

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

Spark Iceberg runtime 은 `spark-submit --packages` 로 driver 시작 전에 주입한다. Docker Spark image 는
기본 Ivy cache path 가 writable 이 아니므로 script 는 `spark.jars.ivy=/tmp/.ivy2` 를 명시한다.
`FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI` 가 없으면 job 은 catalog URI 뒤에 `/v1/oauth/tokens` 를
붙여 Iceberg REST OAuth2 endpoint 를 명시한다.

Spark container 를 띄우고 batch shell 로 들어가려면:

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch up -d spark
docker exec -it foundation-platform-spark bash
```

이미 Spark container 가 떠 있으면 `docker compose ... up` 단계를 건너뛰고 바로 `docker exec` 로
들어갈 수 있다.

초기에는 Spark 를 product API path 에 넣지 않는다. Spark 는 다음 작업만 맡는다.

- Bronze raw object read
- Silver Parquet/Iceberg table write
- Gold projection write / smoke table write
- backfill/rewrite/compaction PoC

## 안전 규칙

- `gongzzang` 과 `Dawneer` 는 Trino/Spark 에 직접 붙지 않는다.
- Trino 는 운영 SQL/검증 도구이지 product request path 가 아니다.
- Spark 는 batch compute 이며 foundation-platform 의 ownership/promotion 판단을 대체하지 않는다.
- `infra/lakehouse/trino/catalog/*.properties` 는 secret 파일이므로 커밋하지 않는다.
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
