# ADR 0007 - Netflix-style Lakehouse Compute Architecture

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-14 |
| 상태 | Accepted |
| 범위 | `foundation-platform` lakehouse compute layer, query engine, batch engine, control-plane 경계 |
| 관련 ADR | [ADR 0006 - Lakehouse Table Format And Serving Architecture](0006-lakehouse-table-format-and-serving-architecture.md) |

## 결정

`foundation-platform` 는 Netflix-style lakehouse 원칙을 따른다. 단, Netflix 내부 플랫폼을 그대로
복제하지 않고 `R2 + Iceberg + Rust control-plane + Spark batch + Trino query` 구조로 번역한다.

```text
Cloudflare R2
  -> Apache Iceberg table
  -> R2 Data Catalog / Iceberg REST Catalog
  -> Spark batch compute
  -> Trino SQL query
  -> foundation-platform Rust control-plane
  -> Gold API / PostGIS / PMTiles / search derived serving
  -> gongzzang / Dawneer consumers
```

`foundation-platform` Rust 는 데이터를 직접 전부 변환하는 batch engine 이 아니다. Rust 는 정본 계약,
권한, lineage, promotion, rollback, quality gate, job orchestration, consumer API 를 소유한다.
대량 변환과 rewrite 는 Spark 같은 compute engine 이 수행하고, 운영 SQL 조회는 Trino 가 담당한다.

## 역할 분리

| 계층 | 선택 | 역할 |
|---|---|---|
| Storage | Cloudflare R2 | Parquet/GeoParquet object storage |
| Table format | Apache Iceberg | snapshot, manifest, schema evolution, rollback |
| Catalog | R2 Data Catalog / Iceberg REST | current metadata pointer, table identity |
| Control-plane | Rust `foundation-platform` | contract, promotion, rollback, authorization, lineage |
| Batch compute | Spark | Bronze -> Silver -> Gold 대량 변환, backfill, rewrite |
| SQL query | Trino | 운영 SQL, 검증, ad-hoc analysis |
| Local validation | DuckDB / PyIceberg | 가벼운 smoke, 개발자 검증 |
| Streaming future | Flink | CDC/streaming 필요 시 후속 도입 |
| Serving | PostGIS / PMTiles / search / cache | API latency 를 위한 derived layer |

## 왜 Spark 인가

Spark 는 대량 public-data ingest, 정규화, join, backfill, compaction/rewrite 에 적합하다.
필지, 건축물, 실거래가, 산업단지 polygon 처럼 row 수와 파일 수가 커지는 데이터를 Rust API
프로세스가 직접 모두 처리하지 않는다.

Spark 의 초기 책임:

- Bronze raw object 를 읽어 Silver Iceberg table 로 write
- Silver table 끼리 join 하여 Gold projection 생성
- 대량 backfill 과 재처리
- small file compaction 과 sort rewrite 실행

## 왜 Trino 인가

Trino 는 PrestoSQL 계열의 현대적 SQL query engine 이다. 신규 시스템에서는 PrestoDB 보다 Trino 를
기본 SQL engine 으로 둔다.

Trino 의 초기 책임:

- `silver.*`, `gold.*` Iceberg table 을 SQL 로 조회
- quality gate 검증용 SQL 실행
- 운영자가 R2/Iceberg 데이터를 직접 분석
- API serving 에 넣기 전 Gold projection 을 검증

Trino 는 product request path 에 직접 들어가지 않는다. `gongzzang` 과 `Dawneer` 의 사용자 요청은
foundation-platform API, Gold manifest, PostGIS mirror, search index 를 통해 처리한다.

## Rust 의 역할

Rust 는 compute engine 을 대체하지 않는다. 대신 compute engine 을 통제한다.

Rust 소유:

- static lakehouse table contract
- source lineage 와 `source_record`
- quality gate 결과
- publish/promotion decision
- rollback pointer
- `LakehouseMaintenancePolicy` 와 maintenance planning
- consumer API, manifest, event

Spark/Trino 소유 아님:

- business authorization
- consumer-facing write decision
- canonical ownership decision
- Gongzzang/Dawneer product boundary

## Consumer Boundary

`gongzzang` 과 `Dawneer` 는 Trino, Spark, R2 object, Iceberg commit 경로에 직접 붙지 않는다.

허용:

- foundation-platform API 호출
- foundation-platform 가 공개한 manifest 소비
- event 기반 read model/cache 갱신
- 자기 제품 소유 데이터 write

금지:

- consumer 가 Iceberg table 에 직접 commit
- consumer 가 Trino 로 공통 데이터 product query 를 직접 운영
- consumer 가 R2 object key 를 추측해서 canonical data 를 직접 읽기
- consumer 가 PostGIS mirror 를 canonical 처럼 수정

## 개발 환경 전략

기본 `docker compose up` 은 `postgres`, `redis` 만 실행한다. lakehouse compute engine 은 opt-in profile 로
추가한다.

```text
default profile:
  postgres
  redis

lakehouse-query profile:
  trino

lakehouse-batch profile:
  spark
```

이렇게 하는 이유는 Spark/Trino 가 무겁기 때문이다. 평소 API 개발자는 기본 DB/Redis 만 사용하고,
lakehouse 검증이 필요한 개발자만 profile 을 켠다.

## Rejected

- 모든 lakehouse compute 를 Rust 로 직접 재구현
- Spark 하나만으로 query, serving, control-plane 까지 처리
- Trino 를 product API request path 에 직접 배치
- PostGIS 를 Iceberg 대신 canonical public-data store 로 승격
- R2 object listing 기반 필터링을 query contract 로 사용
- `gongzzang`/`Dawneer` 가 Iceberg table 에 직접 쓰는 구조

## 완료 정의

- ADR 0007 이 Netflix-style compute 경계를 명시한다.
- Docker compose 에 opt-in lakehouse query/batch profile 이 존재한다.
- Trino catalog 설정은 secret 이 없는 template 으로 제공된다.
- Spark 는 기본 compose 실행에 포함되지 않는다.
- foundation-platform Rust control-plane 과 compute engine 역할이 분리된다.

## 참고

- [Lakehouse Industry Reference](../catalog/lakehouse-industry-reference.md)
- [Cloudflare R2 Data Catalog config examples](https://developers.cloudflare.com/r2/data-catalog/config-examples/)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Apache Iceberg Spark Getting Started](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Netflix Iceberg repository](https://github.com/Netflix/iceberg)
