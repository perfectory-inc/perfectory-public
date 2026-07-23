# ADR 0006 - Lakehouse Table Format And Serving Architecture

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-14 |
| 상태 | Accepted |
| 범위 | `foundation-platform` 공통 데이터 lakehouse table format, Iceberg catalog provider, PostGIS/PMTiles/search serving 경계 |
| 관련 ADR | [ADR 0002 - R2 Primary Object Storage](0002-r2-primary-object-storage.md), [ADR 0004 - Static Vector Tile Runtime Contract](0004-static-vector-tile-runtime-contract.md), [ADR 0005 - Object Lake Layout And Indexing](0005-object-lake-layout-and-indexing.md), [ADR 0007 - Netflix-style Lakehouse Compute Architecture](0007-netflix-style-lakehouse-compute-architecture.md) |

## 결정

`foundation-platform` 의 공통/공공/산업단지 데이터 본체는 **R2 + Apache Iceberg** lakehouse 로 관리한다.

초기 catalog provider 는 **Cloudflare R2 Data Catalog** 를 사용한다. 단, foundation-platform 의
업무 로직은 Cloudflare 전용 API 에 의존하지 않고 표준 Iceberg REST Catalog 계약에 의존한다.
R2 Data Catalog 가 비용, 기능, 안정성, beta 정책 면에서 맞지 않게 되면 Iceberg 표준은 유지한 채
catalog provider 만 교체할 수 있어야 한다.

```text
Canonical common-data store = R2 + Apache Iceberg
Initial catalog provider = Cloudflare R2 Data Catalog
Table files = Parquet / GeoParquet
Spatial hot serving = PostGIS mirror
Map runtime = static vector tile or PMTiles-derived artifact
Search serving = rebuildable search index
```

PostGIS, PMTiles/vector tiles, Redis, search engine 은 canonical source 가 아니다. 모두 Iceberg
snapshot 과 Gold artifacts 에서 재생성 가능한 **derived serving layer** 로 취급한다.

산업단지, 건축물, 필지 polygon 을 PMTiles 로만 제공할지, 편집 가능한 polygon workflow 를 둘지,
수정 geometry 를 어떤 승인/승격 절차로 canonical lakehouse 에 반영할지는 이 ADR 에서 확정하지
않는다. 이 주제는 후속 Spatial Serving And Editable Geometry ADR 에서 결정한다.

## 용어

| 용어 | 의미 |
|---|---|
| Object storage | R2 같은 대용량 객체 저장소 |
| Parquet | 컬럼형 파일 포맷. column pruning, row group statistics, 압축에 적합 |
| GeoParquet | geometry metadata 를 포함한 Parquet 공간 데이터 표준 |
| Iceberg | Parquet/GeoParquet 파일을 table, schema, snapshot, manifest, partition evolution 으로 관리하는 open table format |
| Iceberg catalog | table 이름과 최신 metadata pointer, commit 을 관리하는 계층 |
| R2 Data Catalog | Cloudflare 가 제공하는 managed Apache Iceberg REST catalog |
| Serving layer | API latency, 지도 렌더링, 검색을 위해 canonical data 에서 파생한 인덱스나 캐시 |

## 왜 Iceberg 인가

우리 데이터는 일반 서비스 DB 트랜잭션보다 공공 데이터 수집, 정규화, 버전 배포에 가깝다.

필요한 성질:

- 대용량 object storage 위에서 동작
- schema evolution 과 partition evolution
- snapshot, time travel, rollback
- manifest 기반 파일 pruning
- 여러 query engine 과의 호환성
- DB 저장 비용 최소화
- R2 와의 자연스러운 결합

Iceberg 는 이 요구에 가장 잘 맞는다. Hudi 는 upsert/change stream 이 핵심인 데이터 lake 에 더
강하고, Delta Lake 는 Databricks/Spark 중심 운영에서 장점이 크다. foundation-platform 의 기본
lakehouse 표준은 Iceberg 로 고정한다.

## Catalog Provider 정책

초기 구현은 R2 Data Catalog 를 사용한다.

이유:

- foundation-platform 의 primary object storage 가 R2 다.
- R2 Data Catalog 는 R2 bucket 과 Iceberg catalog 를 직접 연결한다.
- 표준 Iceberg REST Catalog interface 를 제공하므로 query engine 연결과 provider 교체 여지를 남긴다.
- 자체 catalog 서버, catalog DB, lock/commit 운영 부담을 초기에는 줄일 수 있다.

제약:

- R2 Data Catalog 는 현재 Cloudflare 제품 정책과 beta 성숙도 영향을 받는다.
- provider 별 운영 API 는 Cloudflare 에 종속될 수 있다.
- 비용 정책은 바뀔 수 있으므로 영구 무료를 전제로 설계하지 않는다.

규칙:

```text
Allowed:
  infra/provisioning 에서 Cloudflare R2 Data Catalog API 사용
  runtime/query 에서 Iceberg REST Catalog endpoint 사용

Forbidden:
  domain/app/business logic 이 Cloudflare 전용 catalog API 에 직접 의존
  table identity 를 Cloudflare resource id 로 저장
  R2 Data Catalog 없이는 해석 불가능한 manifest contract 생성
```

## Medallion 적용

ADR 0005 의 Bronze/Silver/Gold object layout 은 유지한다. 이 ADR 은 각 계층의 table format 과
catalog 경계를 추가한다.

### Bronze

Bronze 는 provider 원본 보존 계층이다. 모든 Bronze raw object 를 Iceberg row 로 강제하지 않는다.
공공 API 원문, zip, csv, xml, json, shp 같은 원본은 R2 object 로 immutable 저장하고, DB 는
object/batch metadata 와 lineage 만 추적한다.

필요하면 Bronze object inventory 를 Iceberg table 로 별도 구성할 수 있지만, 원본 payload 자체를
Postgres JSONB 나 row 단위 Iceberg table 로 억지 변환하지 않는다.

### Silver

Silver 는 표준화된 Iceberg table 이다.

예시 table:

```text
silver.industrial_complexes
silver.industrial_complex_boundaries
silver.parcels_current
silver.parcel_boundaries
silver.buildings_current
silver.building_footprints
silver.land_characteristics
silver.transactions
silver.public_announcements
```

일반 표형 데이터는 Parquet, geometry 를 포함하는 데이터는 GeoParquet 을 기본으로 한다.
좌표계, 면적 단위, 코드 체계, timestamp, source lineage 를 이 계층에서 표준화한다.

### Gold

Gold 는 serving 과 consumer contract 에 맞춘 Iceberg table 또는 immutable artifact 다.

예시:

```text
gold.complex_catalog
gold.parcel_lookup
gold.parcel_area_filter_projection
gold.complex_parcel_memberships
gold.spatial_locator
gold.transaction_summary_by_region
gold.search_documents
gold.vector_tile_inputs
```

Gold 는 `gongzzang`, `dawneer`, foundation-platform API 가 직접 소비할 수 있는 안정된 schema 를 가진다.
하지만 consumer 는 R2 object 를 임의로 직접 뒤지지 않고 foundation-platform API, manifest, 또는 명시된
read contract 를 통해서만 접근한다.

## Partition And Sort 정책

기본 partition 은 조회 패턴과 파일 크기를 기준으로 잡는다.

권장:

```text
region: sido, sigungu, bjdong
time: year, yyyymm, effective_date
identity: bucket(N, pnu), bucket(N, building_key)
spatial pruning: h3, tile_z/x/y, bbox columns
```

금지:

- 무한히 늘어나는 고카디널리티 값을 partition key 로 직접 사용
- 면적, 가격처럼 분포가 바뀌는 값을 무조건 top-level partition 으로 사용
- 프론트 필터 하나마다 별도 수동 JSON index 를 만드는 구조

면적 400평 이상 같은 필터는 기본적으로 다음 조합으로 처리한다.

```text
1. region/time/spatial partition 으로 object 후보 축소
2. Iceberg manifest 와 Parquet row group statistics 로 file/page 후보 축소
3. 필요한 column 만 읽어 predicate 적용
4. 매우 자주 쓰는 필터만 Gold projection 또는 hot serving index 로 승격
```

## PostGIS 사용 정책

PostGIS 는 사용한다. 다만 canonical public-data store 로 쓰지 않는다.

사용 목적:

- 산업단지 boundary, 행정구역, 필지/건축물 후보 subset 의 정확 공간질의
- `ST_Contains`, `ST_Intersects`, `ST_DWithin` 같은 hot query
- geometry 품질 검수와 운영자 QA
- 편집 가능한 polygon workflow 가 생길 경우 승인 전후 비교와 검수

규칙:

```text
PostGIS row = serving mirror or QA workspace
Iceberg snapshot = canonical source
```

PostGIS mirror 는 다음 정보를 가져야 한다.

```text
dataset
iceberg_table
iceberg_snapshot_id
foundation_platform_entity_id
geometry_checksum_sha256
loaded_at
```

mirror 가 깨지면 Iceberg snapshot 에서 다시 만들 수 있어야 한다. PostGIS 에만 존재하는
공통 데이터 fact 는 허용하지 않는다. 단, 후속 editable geometry ADR 이 승인한 운영자 수정 draft,
review, approval record 는 별도 workflow table 로 둘 수 있다.

## 지도 Artifact 정책

ADR 0004 의 `gold/manifest.json` runtime contract 는 유지한다.

지도 렌더링용 vector tile, PMTiles, TileJSON, style helper artifact 는 canonical geometry 가 아니라
렌더링 산출물이다. 산출물은 source Iceberg snapshot, source_record, file_asset lineage 를 가져야 한다.

이 ADR 은 PMTiles 를 최종 runtime 으로 강제하지 않는다. 다음 선택은 후속 ADR 에서 결정한다.

- flat vector tile object layout
- PMTiles byte-range serving
- PostGIS dynamic tile serving
- editable polygon authoring 과 tile regeneration workflow

## API Query 정책

foundation-platform API 는 대량 공공 데이터 본문을 DB row scan 으로 제공하지 않는다.

권장 흐름:

```text
단건 PNU 조회:
  DB control-plane 에서 active Gold/Iceberg snapshot pointer 확인
  pnu locator 또는 Iceberg catalog 로 대상 partition/object 확인
  필요한 Parquet/GeoParquet column 만 읽어 응답

면적/지역 필터:
  region partition + Iceberg manifest + Parquet statistics 로 후보 축소
  필요 column 만 읽어 predicate 적용

공간 질의:
  bbox/h3/tile 로 후보 축소
  hot query 는 PostGIS mirror 사용
  canonical 결과 검증은 source Iceberg snapshot lineage 로 추적

지도 렌더링:
  ADR 0004 manifest 또는 후속 spatial artifact manifest 를 소비
```

서비스 latency 가 Iceberg direct read 로 맞지 않는 query 는 Gold projection, PostGIS mirror,
search index, Redis cache 로 승격한다. 이때도 canonical source 는 Iceberg snapshot 이다.

## 품질 Gate

Silver/Gold snapshot promote 전에는 최소한 다음을 검증한다.

- source lineage 가 존재한다.
- schema evolution 이 명시적으로 승인됐다.
- required column 이 존재한다.
- row count, null ratio, duplicate natural key, checksum 검사가 통과한다.
- geometry table 은 SRID, validity, bbox, area range, geometry checksum 을 검증한다.
- Gold projection 은 source Iceberg snapshot id 를 기록한다.
- serving mirror 는 재생성 가능성을 증명하는 load manifest 를 가진다.
- rollback 대상 snapshot 이 존재한다.

## Consumer 경계

`gongzzang` 과 `dawneer` 는 Iceberg catalog, R2 object, PostGIS mirror 에 직접 write 하지 않는다.

허용:

- foundation-platform API 호출
- foundation-platform 가 공개한 read manifest 소비
- event 로 갱신되는 consumer read model/cache
- 자기 제품 소유 데이터 write

금지:

- consumer 가 공통 데이터 Iceberg table 에 직접 commit
- consumer 가 PostGIS mirror 를 canonical 처럼 수정
- consumer 가 R2 object key 를 추측해서 미공개 dataset 을 직접 읽기
- consumer 가 vector tile manifest version 을 직접 변경

## Rejected

- 공통/공공 데이터 본체를 PostgreSQL/PostGIS 에 전부 저장
- R2 에 JSON 파일만 쌓고 애플리케이션이 전체 scan
- 수동 `*-index.json` 을 foundation-platform canonical index 로 사용
- Iceberg table 을 catalog 없이 metadata file pointer 만으로 운영
- R2 Data Catalog 전용 API 를 business logic 에 직접 결합
- PostGIS 를 Iceberg 에서 재생성 불가능한 유일한 공간 원장으로 사용

## 영향

- ADR 0005 의 object lake layout 은 유지하되, Silver/Gold table format 은 Iceberg 로 수렴한다.
- Bronze raw capture 구현은 그대로 유효하다.
- PostGIS 는 더 중요해지지만 역할은 serving/QA mirror 로 제한된다.
- polygon editing 과 PMTiles runtime 방식은 후속 ADR 전까지 구현 결정으로 확정하지 않는다.
- foundation-platform 의 향후 lakehouse infra 는 Iceberg REST Catalog abstraction 을 중심으로 설계한다.

## 완료 정의

- foundation-platform 문서가 Apache Iceberg 를 canonical lakehouse table format 으로 명시한다.
- 초기 catalog provider 가 Cloudflare R2 Data Catalog 임을 명시한다.
- R2 Data Catalog 는 provider 이고 Iceberg 는 표준이라는 차이를 문서화한다.
- PostGIS, PMTiles/vector tiles, search index 가 derived serving layer 임을 명시한다.
- 산업단지/건축물/필지 polygon serving/editing 방식은 후속 ADR 로 남긴다.
- `gongzzang` 과 `dawneer` 는 foundation-platform 공통 데이터 write owner 가 아님을 유지한다.

## 참고

- [Lakehouse Industry Reference](../catalog/lakehouse-industry-reference.md)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/)
- [GeoParquet](https://geoparquet.org/)
