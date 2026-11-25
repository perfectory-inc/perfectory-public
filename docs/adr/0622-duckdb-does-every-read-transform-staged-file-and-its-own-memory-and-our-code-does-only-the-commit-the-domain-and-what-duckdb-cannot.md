# ADR 0622: DuckDB 가 하는 것, 우리 코드가 하는 것 — 모든 Spark 옮김이 따르는 한 장

- Status: Accepted
  - 정해진 것: 결정 1~6(사장 검토 2026-11-25 와 그 뒤 코디네이터 결정 — 결정 5·6 은 사장의 "표준 먼저" 규칙 아래 코디네이터가 정했다. 코드가 W102 의 PR 에 있다).
  - 방향만 정해졌고 재고 나서 정하는 것: "열린 질문"의 셋.
- Date: 2026-11-26
- 고치는 곳: [ADR-0127](./0127-floor-duckdb-staging-preserves-normalization-and-native-resource-contracts.md) 의 "버퍼는
  `execution_profile` 의 절반"과 `FOUNDATION_PLATFORM_SERVING_SCRATCH_MAX_BYTES`,
  [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)
  §7 의 메모리 문장. 나머지 0559 와 [ADR-0560](./0560-the-publisher-commits-duckdb-staged-parquet-with-iceberg-rust-fast-append-and-only-tables-whose-commit-is-an-append-move-off-spark.md)
  은 그대로다.
- 짝: [ADR-0616](./0616-data-jobs-run-as-dagster-assets-on-aws-ecs-and-the-host-job-layer-is-not-ported.md)(일은 AWS 의 태스크, 메모리는
  태스크의 크기 §5), [ADR-0615](./0615-a-duckdb-gold-lane-is-a-dagster-asset-partitioned-by-sido-and-the-status-of-a-part-is-dagsters-not-ours.md)(파티션과 공통 판 드라이버).

## 사장에게: 세 줄

- DuckDB 가 이미 하는 일(읽기·계산·파일 쓰기·메모리·스레드·재시도)은 DuckDB 에 맡기고, 우리 코드는 **커밋·도메인·DuckDB 가 못 하는 것**만 한다.
- 남아 있던 다른 길(레인마다 정한 메모리 숫자 7 가지, DuckDB 가 쥔 쓰기 키, 두 번째 Iceberg 독자, 재시도 없는 R2 읽기)은 지웠거나 얼렸고, 가드가 다시 생기는 것을 막는다.
- Spark 에서 옮기는 작업은 모두 아래 표를 체크리스트로 PR 에 붙인다.

## Context

**검토(2026-11-25).** 7 명의 검토자가 같은 모양을 냈다: 대부분은 이미 DuckDB 가 한다(Iceberg 는 `ATTACH`, 파일은 `COPY … FIELD_IDS`,
비교는 SQL). 값을 깎던 빈 곳은 넷이었다.

| 빈 곳 | 잰 사실 |
|---|---|
| 메모리·스레드가 태스크가 아니라 컴파일된 프로필에서 왔다 | 모든 단계가 `execution_profile` 의 절반(2 GiB)과 2 스레드. 2026-11-22 경매 Silver 의 첫 운영 run 이 3G 유닛에서 2 GiB 를 넘는 사건상세 객체에 멈췄고 읽기를 그 숫자에 맞춰 고쳐 썼다(#1646). 그 옆에 3/5 몫 둘, `limit_memory`, 고정 384·600·600 MiB, 품질 검사의 300 MiB, 판정의 `4GB`, FLOOR 의 설정 사본 |
| 디스크도 같은 꼴 | 고정 32 GiB 스필 상한 + 30 GiB 호스트 예약. 2026-11-25 그림자 Gold 레인 16 중 9 가 60.5 GB 여유에도 행 하나 읽기 전에 거부됐다(W103). ai-server 에서 단계의 작업 디렉터리와 운영 Postgres 는 같은 `/`(LVM, 여유 53.6 GB)다 |
| DuckDB 가 쓰기 자격을 쥐었다 | 쓰기 키 쌍과 쓰기 토큰으로 카탈로그를 읽기-쓰기로 붙였다. 옮긴 작업이 `INSERT INTO lakehouse.…` 를 하면 우리 요약 속성 없는 커밋이 조용히 생긴다 |
| 두 번째 Iceberg 독자 | 손으로 쓴 `lakehouse_snapshot_scan`(+`iceberg_scan`·`ranged_parquet`·`shard_plan`, 2,415 줄). 위치 삭제를 손으로 배워야 했고(#959), `double`·`binary` 를 못 읽어 산업단지 경계가 Spark 인계로 남았다 |
| DuckDB 의 R2 읽기에 재시도 숫자가 없었다 | ADR-0102 의 `r2_read` 는 Spark 와 foundation-outbox 만 썼다 |

**읽은 것(고정한 판의 소스).** DuckDB v2.0.0 (`d54cbbe3`), 그 판의 duckdb-iceberg `4c0c087`, duckdb-httpfs `77392a7`.

- 스레드: `DBConfig::GetSystemMaxThreads` → `CGroups::GetCPULimit` — 프로세스 cgroup 의 `cpu.max`(v1 은 CFS quota)를 `ceil(quota/period)`
  로 읽고, 없으면 하드웨어 스레드.
- 스필: `max_temp_directory_size` 를 정하지 않으면 temp 디렉터리 디스크 여유의 90%(`TemporaryFileManager::GetDefaultMax`).
- 읽기 전용: `ATTACH … (READ_ONLY)` 는 `AttachOptions` 가 `READ_ONLY_DATABASE` 로 두고, `ClientContext` 가 그 DB 를 바꾸는 모든 문장을
  "attached in read-only mode" 로 거부한다. duckdb-iceberg 의 `IcebergCatalog` 도 그 접근 모드를 받는다.
- 재시도: httpfs 의 `http_retries`·`http_retry_wait_ms`·`http_retry_backoff`·`http_timeout`. 전송 오류·408·429·5xx 를 새 연결로 다시 묻고,
  대기는 `wait × backoff^(n-1)` 의 전체 지터, 상한 20 s(`HTTPRetryBudget::MAX_BACKOFF_MS`, 설정 아님). 연결이 끊겨 짧아진 본문은
  전송 오류다. Iceberg REST 요청도 같은 세션 매개변수를 탄다(`APIUtils::Request`).
- 커밋: DuckDB 2.0 은 스냅샷 요약 속성과 태그를 못 쓴다(ADR-0559 의 탐침). 재시도 가드·핀·`version_seq`·지문이 모두 그것에 기댄다.

## Decision

### 1. 단위와 규칙 0

단위는 `lakehouse-engine.contract.json` 의 `engines.spark_sessions` 한 줄이다. **규칙 0: DuckDB 에 기능이 있으면 그것을 쓴다.** 아래
"우리 코드가 하는 것" 밖의 Rust 는 잰 값과 그 PR 의 예외가 있어야 한다.

### 2. DuckDB 가 하는 것

| 일 | DuckDB 의 것 | 하나뿐인 곳 / 가드 |
|---|---|---|
| Iceberg 읽기 | `ATTACH (TYPE iceberg, …, READ_ONLY)`, 판 고정은 `table_at` | `DuckDbParquetStage::open_reading_lakehouse` / `lakehouse-time-travel.yml`, `hand-written-iceberg-scan.yml`(손 독자는 새 호출자를 받지 않는다; 오늘 24 파일, 줄기만 한다) |
| R2 읽기의 재시도 | httpfs `http_retries = r2_read.num_retries`, `http_retry_wait_ms = min_wait_ms`, `http_retry_backoff = 2` | 같은 오프너 / `r2-read-paths.yml` 의 `duckdb-reads-r2-through-the-stage-opener` |
| 변환·비교·검사·집계 | SQL, 공간은 core spatial 확장 | 레인의 SQL |
| 준비 파일 | `COPY … FIELD_IDS, FILE_SIZE_BYTES` | `write`·`write_split` |
| 메모리 | `memory_limit` = `MEMORY_SHARE`(3/5) × 프로세스가 더 쓸 수 있는 몫(cgroup `memory.max`·`memory.high` − 사용량, `MemAvailable`) | `duckdb_parquet_stage::open_engine` / `duckdb-settings.yml` |
| 스레드 | 정하지 않는다 — DuckDB 가 cgroup 의 CPU 할당을 읽는다(AWS 태스크의 vCPU) | 같은 곳 / 같은 가드 |
| 스필 | `max_temp_directory_size` = `DISK_SHARE`(1/3) × 열 때 그 파일시스템의 여유. 90% 기본값은 쓰지 않는다: ai-server 에서 그 디스크가 Postgres 의 것이고, 두 단계가 동시에 상한까지 써도 1/3 이 남아야 한다 | 같은 곳 / 같은 가드 |

**디스크의 나머지.** 파일 단계의 DB(`stage.duckdb`)와 준비 파일은 스필 상한에 들지 않는다. 작업마다 정의에 `disk_max`·`disk_measured` 를
`memory_max`·`memory_measured` 옆에 두고(없으면 `declared_definitions.py` 가 거부, 잰 적이 없으면 계산한 한도와
`not measured: <까닭>`), `unit_memory.py` 가 그것을 유닛의 `FOUNDATION_PLATFORM_DUCKDB_DISK_MAX_BYTES` 로 그린다. 단계는 열 때
"여유 − 스필 상한 ≥ 그 작업의 `disk_max`" 가 아니면 거부하고, 닫을 때 `duckdb stage closed: disk_bytes=<n>` 을 남긴다. 잰 값은 그 줄에서
읽어 정의를 고치는 PR 로 들어간다(우리 코드는 run 사이에 상태를 두지 않는다). AWS 에서는 같은 값이 태스크의 임시 저장소 크기다. 고정
32 GiB 스필 상한, 30 GiB 호스트 예약, `require_room` 은 지웠다.

### 3. 우리 코드가 하는 것(이것만)

1. **Iceberg 커밋.** iceberg-rust `fast_append` 에 요약 속성, 머리 고정(head pin), 태그는 REST(ADR-0560). DuckDB 는 쓰기 자격을 갖지 않는다:
   카탈로그는 `READ_ONLY`, R2 는 읽기 전용 키 쌍(`LakehouseRead` 의 생성자 하나). 카탈로그 토큰은 읽기 토큰이 있으면 그것이고,
   ai-server 에서 그 토큰을 싣지 않은 유닛만 유닛의 토큰으로 붙는다 — 그래도 `READ_ONLY` 와 읽기 키 쌍이라 쓸 수 없다.
   `LakehouseRead::requirements` 는 읽기 토큰만 이름 대므로 AWS 태스크(`describe-requirements` 로 만듦)에서는 이 대체가 일어나지 않는다.
   시험: `duckdb_parquet_stage/tests/read_only.rs`.
2. **다시 지을 키와 붙일 행.** 공통 판 드라이버(ADR-0615 §7). 레인마다 다시 짜지 않는다.
3. **도메인.** 원천 SQL, 계약, 열 선언, 검사, 값 판정.
4. **DuckDB 가 못 하는 것.** Iceberg bucket 해시, H3(h3o), zip 풀기, ADR-0127 의 정규화기, 그리고 흘리지 못하는 연산자를 잰 곳에서만
   시군구 조각.
5. **접착.** Pipes 줄, 진입점, 비밀.

### 4. 하지 않는 것

- 새 옮김에서 Spark 의 바이트나 Python 의 값을 흉내 내기(ADR-0559 의 11-22 개정과 같다).
- 레인마다의 메모리·스레드·스필·이어 짓기·시도 루프, 호스트 모양의 디스크 예약.
- community·unsigned 확장, 실행 중 확장 설치·자동 로드.
- DuckDB 의 `INSERT`·`MERGE`·`UPDATE`·`DELETE` 를 우리 표에(이제 `READ_ONLY` 가 막는다), 손 독자가 살아 있는 동안 Iceberg v3.

### 5. 엔진·아키텍처·스레드 수가 다른 두 run 의 판정 — 출력을 결정적으로

W101 이 2026-11-25 에 쟀다: **부동소수 집계는 스레드 수에 따라 다르고, 공간 결과는 몇 ulp 다르다.** 이 PR 들로 스레드 수가 태스크를
따르게 되었으므로(ai-server 20, Fargate 는 vCPU), 같은 코드도 다른 크기의 태스크에서 `DOUBLE` 합이 마지막 자리까지 같지 않을 수 있다.

- **허용 오차가 아니라 결정적인 출력.** 옮긴 작업이 쓰는 합은 `DECIMAL` 로 계산하거나, 쓸 때 계약의 자릿수로 반올림한다. 그러면 판정은
  지금처럼 정확히 같은지를 본다(정수·decimal·글자 칸은 늘 그렇다).
- **남는 공간 칸만** 판정이 칸마다 적는 ulp 범위 안에서 같다고 본다(W101 이 잰 "몇 ulp"에서 시작). 그 범위는 판정 기록에 칸마다 남는다.

### 6. `python_values.sql` 은 얼린다

옮긴 레인들이 읽는 `python_values.sql` 은 지금 얼리고, 레인을 하나씩 옮길 때 그 레인에서 지운다. 새 옮김은 Python 의 값을 흉내 내지
않고 DuckDB 자신의 형변환을 쓴다(결정 4).

## 열린 질문

1. **손 독자 지우기.** DuckDB 의 pushdown·`iceberg_metadata()` 가 by-PNU 샤드 계획의 매니페스트 경계와 행 계산을 주는지 잰 뒤.
2. **AWS 의 표 유지보수.** R2 의 관리형 합치기·만료(태그가 사는지 미증명) 또는 iceberg-rust 의 만료. Silver 옮김(A4) 전에.
3. **새 표의 bucket 파티션.** 한 번의 `PARTITION_BY` 로 쓰기(허용) 또는 금지. 오늘 bucket 을 쓰는 표의 목록부터.

## Consequences

- 옮기는 PR 은 위 2·3·4·5·6 을 체크리스트로 붙이고, 증거로 `gold-takeover-verdict.sh <표> <시도>` 의 결과, 돈 태스크 크기와 메모리 최고치,
  디스크 최고치(`disk_measured` 의 첫 값)를 적는다.
- 같은 유닛의 메모리는 이제 그 유닛이 준 만큼 DuckDB 가 쓴다(16G 유닛에서 2 GiB → 약 9.5 GiB). 유닛의 최고치는 상한을 따른다.
- ai-server 의 DuckDB 는 유닛에 CPU 할당이 없어 20 스레드를 쓴다(코디네이터 결정: AWS 가 전제, 호스트 유닛은 바꾸지 않는다). 스레드당
  메모리가 적으면 흘리지 못하는 연산자가 먼저 OOM 날 수 있다; 시군구 조각이 그 대비다.
- DuckDB 를 올릴 때: 확장 넷 × 아키텍처 둘의 핀, 그리고 "DuckDB 가 요약 속성을 쓸 수 있는가"를 다시 탐침한다.

| 무엇 | 기본 기능 | 임시 | 운영 |
|---|---|---|---|
| 메모리·스레드 | DuckDB `memory_limit`·cgroup 스레드 | — | `MEMORY_SHARE` 하나 |
| 스필·디스크 | DuckDB `max_temp_directory_size` | 계산한 첫 `disk_max`(잰 값으로 바뀔 때까지) | `DISK_SHARE` 하나, 작업별 `disk_max` |
| 쓰기 자격 | DuckDB `READ_ONLY` | ai-server 의 토큰 대체 | 읽기 토큰 + 읽기 키 쌍 |
| R2 재시도 | httpfs `http_*` | — | `r2_read` 의 숫자 |
| 두 번째 독자 | DuckDB Iceberg 확장 | 손 독자(얼림, 24 파일) | 측정 뒤 삭제 |

## 기각한 대안

- **DuckDB 가 커밋까지.** 요약 속성과 태그를 못 쓰고, 표 속성으로 옮기면 동시 커밋에서 스냅샷마다의 상태를 잃는다.
- **ADR-0559 의 셋째 개정으로 목록을 적기.** 독자가 규칙 하나를 알려고 문서 셋을 읽어야 한다.
- **모든 사이트를 먼저 목록으로 만들고 옮기기.** 금방 낡는 큰 문서다; 표와 가드가 그 일을 한다.
- **스레드를 계약의 `cpu_slots` 로 계속 정하기.** 태스크의 vCPU 가 DuckDB 에 닿지 않는다; DuckDB 가 cgroup 을 이미 읽는다.
- **스필을 DuckDB 기본 90% 로.** ai-server 에서 그 디스크가 운영 Postgres 의 것이다.
