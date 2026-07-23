# Industrial Complex Lakehouse PoC

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-14 |
| 상태 | Draft |
| 결정 ADR | [ADR 0006 - Lakehouse Table Format And Serving Architecture](../adr/0006-lakehouse-table-format-and-serving-architecture.md) |
| SSOT 모델 | [Industrial Complex SSOT Model](industrial-complex-ssot-model.md) |
| 외부 레퍼런스 | [Lakehouse Industry Reference](lakehouse-industry-reference.md) |

## 1. 목표

산업단지를 첫 Iceberg PoC dataset 으로 사용한다. 필지 1억 건대 전체를 바로 올리지 않고,
작지만 공통성이 높은 산업단지 데이터로 R2 + Iceberg + R2 Data Catalog + PostGIS derived serving
경계를 검증한다.

PoC 가 증명해야 하는 것은 네 가지다.

- canonical data 는 R2 + Iceberg snapshot 에 있다.
- PostGIS 는 canonical 이 아니라 재생성 가능한 spatial serving mirror 다.
- 지도 artifact 는 canonical 이 아니라 Iceberg snapshot 에서 파생된다.
- `gongzzang` 과 `dawneer` 는 foundation-platform API/manifest/event 를 소비하고 공통 데이터에 write 하지 않는다.

## 2. 범위

### 포함

- `silver.industrial_complexes`
- `silver.industrial_complex_boundaries`
- `silver.complex_parcel_memberships`
- `gold.complex_catalog`
- `gold.complex_spatial_locator`
- PostGIS mirror load manifest contract
- dataset quality gate contract

### 제외

- 전국 필지 전체 적재
- 건축물 전체 적재
- 실거래/경매 전체 적재
- polygon 편집 UI 와 승인 workflow
- PMTiles 최종 runtime 방식 결정
- 검색 엔진 제품 선택

polygon 편집과 PMTiles/flat tile/PostGIS dynamic tile 선택은 후속 Spatial Serving And Editable
Geometry ADR 에서 결정한다.

## 3. 공통 규칙

### ID

Iceberg table 의 foundation-platform ID 는 UUID 문자열로 저장한다. UUID 값은 가능한 경우 자연키에서
deterministic 하게 생성한다.

| Entity | Deterministic seed |
|---|---|
| Industrial complex | `industrial_complex:{official_complex_code}` |
| Complex boundary | `spatial_layer:complex_boundary:{complex_id}:{source_record_id}` |
| Parcel membership | `complex_parcel_membership:{complex_id}:{pnu}` |

자연키가 불안정한 원천은 `source_record_id` 와 source-specific external key 를 함께 저장하고,
identity resolution 정책을 별도 문서화한다.

### 시간

모든 timestamp 는 UTC 로 정규화한다. Iceberg 에 저장하는 timestamp 값은 timezone ambiguity 가 없도록
`*_at_utc` 이름을 사용한다.

### 공간

canonical geometry 는 GeoParquet WKB encoding 을 기본으로 한다.

공통 공간 컬럼:

```text
geometry_wkb
geometry_srid
bbox_min_x
bbox_min_y
bbox_max_x
bbox_max_y
centroid_x
centroid_y
geometry_checksum_sha256
```

초기 CRS 는 `EPSG:4326` 을 기본으로 한다. 원천 CRS 가 다르면 Bronze lineage 에 원천 CRS 를 남기고,
Silver 에서 `EPSG:4326` 으로 정규화한다.

## 4. Silver Tables

### 4.1 `silver.industrial_complexes`

산업단지의 표준화된 canonical fact table 이다.

| Column | Type | Required | 설명 |
|---|---|---:|---|
| `complex_id` | string | yes | foundation-platform UUID string |
| `official_complex_code` | string | yes | 원천 공식 산업단지 코드 |
| `complex_name` | string | yes | 공식 산업단지명 |
| `complex_name_normalized` | string | yes | 검색/중복검사용 정규화명 |
| `complex_kind` | string | yes | `national`, `general`, `agricultural`, `urban_high_tech` |
| `status` | string | yes | `planned`, `developing`, `operating`, `changed`, `abolished`, `unknown` |
| `sido_code` | string | yes | 시도 코드 |
| `sigungu_code` | string | yes | 시군구 코드 |
| `primary_bjdong_code` | string | no | 대표 법정동 코드 |
| `address_text` | string | no | 공식 주소 텍스트 |
| `management_agency_name` | string | no | 관리기관명 |
| `developer_name` | string | no | 사업시행자 |
| `designated_date` | date | no | 지정일 |
| `completion_date` | date | no | 준공일 |
| `official_area_sqm` | decimal(18,2) | no | 공식 면적 |
| `source_record_id` | string | yes | foundation-platform source record id |
| `source_snapshot_id` | string | yes | 원천 snapshot/batch id |
| `valid_from_utc` | timestamp | yes | fact 유효 시작 |
| `valid_to_utc` | timestamp | no | fact 유효 종료 |
| `ingested_at_utc` | timestamp | yes | 수집/정규화 시각 |
| `row_checksum_sha256` | string | yes | geometry 제외 row checksum |

Partition:

```text
sido_code
bucket(32, complex_id)
```

Sort:

```text
sigungu_code
complex_name_normalized
official_complex_code
```

Quality gate:

- `(official_complex_code, source_snapshot_id)` unique
- `complex_id` non-null
- `complex_name` non-empty
- `complex_kind` is one of the domain wire values
- `official_area_sqm > 0` when present
- active rows for the same `complex_id` do not overlap in validity period

### 4.2 `silver.industrial_complex_boundaries`

산업단지 경계의 canonical spatial table 이다. 지도 runtime 산출물과 PostGIS mirror 는 이 table 에서
파생된다.

| Column | Type | Required | 설명 |
|---|---|---:|---|
| `boundary_id` | string | yes | deterministic boundary id |
| `complex_id` | string | yes | parent complex id |
| `boundary_kind` | string | yes | `official`, `derived`, `corrected`, `draft` |
| `geometry_wkb` | binary | yes | GeoParquet WKB geometry |
| `geometry_srid` | int | yes | 초기 기본값 4326 |
| `bbox_min_x` | double | yes | min longitude |
| `bbox_min_y` | double | yes | min latitude |
| `bbox_max_x` | double | yes | max longitude |
| `bbox_max_y` | double | yes | max latitude |
| `centroid_x` | double | yes | centroid longitude |
| `centroid_y` | double | yes | centroid latitude |
| `area_sqm_calculated` | decimal(18,2) | no | geometry 기반 계산 면적 |
| `geometry_checksum_sha256` | string | yes | WKB checksum |
| `source_record_id` | string | yes | source lineage |
| `source_snapshot_id` | string | yes | source snapshot |
| `valid_from_utc` | timestamp | yes | 유효 시작 |
| `valid_to_utc` | timestamp | no | 유효 종료 |
| `ingested_at_utc` | timestamp | yes | 정규화 시각 |

Partition:

```text
sido_code
bucket(32, complex_id)
```

`sido_code` 는 boundary table 에 물리 컬럼으로 포함한다. parent lookup 없이 partition pruning 이 가능해야 한다.

Sort:

```text
complex_id
boundary_kind
valid_from_utc
```

Quality gate:

- `geometry_srid = 4326`
- bbox min/max ordering is valid
- centroid is inside bbox
- WKB is valid polygon or multipolygon
- active `official` boundary is at most one per `complex_id`
- `geometry_checksum_sha256` is 64 lowercase hex

### 4.3 `silver.complex_parcel_memberships`

산업단지와 필지의 관계 table 이다. 전체 전국 필지 table 이 없어도 산업단지 PoC 는 이 관계를 통해
산단 중심 조회를 검증할 수 있다.

| Column | Type | Required | 설명 |
|---|---|---:|---|
| `membership_id` | string | yes | deterministic membership id |
| `complex_id` | string | yes | industrial complex id |
| `parcel_id` | string | yes | deterministic parcel id |
| `pnu` | string | yes | 19-digit PNU |
| `sido_code` | string | yes | PNU prefix |
| `sigungu_code` | string | yes | PNU prefix |
| `bjdong_code` | string | yes | PNU prefix |
| `membership_kind` | string | yes | `inside`, `intersects`, `candidate`, `excluded` |
| `source_method` | string | yes | `official_list`, `geometry_overlay`, `manual_review` |
| `area_overlap_sqm` | decimal(18,2) | no | overlay 기반 포함 면적 |
| `overlap_ratio` | decimal(9,6) | no | 필지 대비 포함 비율 |
| `source_record_id` | string | yes | source lineage |
| `source_snapshot_id` | string | yes | source snapshot |
| `valid_from_utc` | timestamp | yes | 유효 시작 |
| `valid_to_utc` | timestamp | no | 유효 종료 |
| `ingested_at_utc` | timestamp | yes | 정규화 시각 |
| `row_checksum_sha256` | string | yes | row checksum |

Partition:

```text
sigungu_code
bucket(256, pnu)
```

Sort:

```text
complex_id
pnu
membership_kind
```

Quality gate:

- `pnu` passes shared PNU validation
- one active `(complex_id, pnu)` membership for `inside` or `intersects`
- `overlap_ratio` is between 0 and 1 when present
- `membership_kind = excluded` rows must include a source method and lineage

## 5. Gold Tables And Artifacts

### 5.1 `gold.complex_catalog`

API list/detail 과 consumer read model 의 stable projection 이다.

| Column | Type | Required | 설명 |
|---|---|---:|---|
| `complex_id` | string | yes | foundation-platform id |
| `official_complex_code` | string | yes | source natural key |
| `name` | string | yes | display name |
| `kind` | string | yes | domain wire kind |
| `status` | string | yes | serving status |
| `sido_code` | string | yes | region filter |
| `sigungu_code` | string | yes | region filter |
| `address_text` | string | no | official address |
| `official_area_sqm` | decimal(18,2) | no | official area |
| `calculated_area_sqm` | decimal(18,2) | no | boundary area |
| `parcel_count` | long | yes | active membership count |
| `boundary_object_key` | string | no | derived GeoParquet object reference |
| `source_snapshot_id` | string | yes | source snapshot |
| `iceberg_snapshot_id` | string | yes | source Iceberg snapshot |
| `published_at_utc` | timestamp | yes | Gold publish time |

Partition:

```text
sido_code
```

Sort:

```text
sigungu_code
name
complex_id
```

### 5.2 `gold.complex_spatial_locator`

bbox/h3/tile 기반 후보 축소용 locator 다. 정확 판정은 GeoParquet 또는 PostGIS mirror 에서 한다.

| Column | Type | Required | 설명 |
|---|---|---:|---|
| `spatial_key` | string | yes | `tile:{z}:{x}:{y}` or `h3:{res}:{cell}` |
| `complex_id` | string | yes | foundation-platform id |
| `boundary_id` | string | yes | source boundary |
| `bbox_min_x` | double | yes | min longitude |
| `bbox_min_y` | double | yes | min latitude |
| `bbox_max_x` | double | yes | max longitude |
| `bbox_max_y` | double | yes | max latitude |
| `geometry_checksum_sha256` | string | yes | source geometry checksum |
| `object_key` | string | yes | target GeoParquet object key |
| `iceberg_snapshot_id` | string | yes | source snapshot |

Partition:

```text
spatial_key_prefix
```

초기 PoC 의 `spatial_key_prefix` 는 `tile_z=10` 수준으로 둔다. 정확한 z/h3 resolution 은 실제
데이터 분포를 측정한 뒤 후속 spatial ADR 에서 확정한다.

## 6. PostGIS Mirror Contract

PostGIS mirror 는 Iceberg snapshot 에서 로드한 serving index 다.

필수 metadata:

```text
mirror_name
iceberg_catalog_uri
iceberg_table
iceberg_snapshot_id
loaded_at_utc
row_count
geometry_checksum_sha256
load_manifest_object_key
```

권장 table:

```text
serving_postgis.complex_boundaries
serving_postgis.complex_spatial_locator
```

규칙:

- `serving_postgis` schema 는 canonical write 를 받지 않는다.
- mirror table 은 drop 후 Iceberg snapshot 에서 재생성 가능해야 한다.
- 운영자 polygon 수정 draft 는 이 mirror 에 직접 덮어쓰지 않는다.
- 수정 workflow 는 후속 editable geometry ADR 이 승인한 별도 schema 에 둔다.

## 7. PoC Success Criteria

PoC 는 다음 조건을 만족하면 성공이다.

- 세 Silver table 의 schema contract 가 문서와 Rust contract test 로 고정된다.
- `gold.complex_catalog` 와 `gold.complex_spatial_locator` 의 projection contract 가 고정된다.
- source snapshot id 와 Iceberg snapshot id 가 Gold/serving artifact 에 남는다.
- PostGIS mirror 가 canonical source 가 아님을 load manifest 로 증명한다.
- polygon 편집/PMTiles 선택을 확정하지 않고도 lakehouse 본체 검증이 가능하다.

## 8. 운영 최적화 확장 방향

Netflix AutoOptimize 같은 대형 최적화 시스템은 PoC 범위에 넣지 않는다. 대신 table contract,
partition, sort, quality gate, snapshot lineage 를 먼저 고정하고, 실제 데이터량과 쿼리 패턴이
증명되면 [Lakehouse Industry Reference](lakehouse-industry-reference.md)의 maintenance backlog 에
따라 별도 worker 로 확장한다.

초기 확장 우선순위:

1. snapshot 변경 감지
2. table/file health metrics 수집
3. small file merge 후보 산정
4. manifest rewrite 후보 산정
5. Gold/serving pointer promotion guard

현재 Rust domain contract 는 `LakehouseTableHealth`, `LakehouseMaintenancePolicy`,
`plan_lakehouse_maintenance` 로 시작한다. 이 계층은 provider-neutral 이며, 실제 R2/Iceberg
rewrite 실행은 후속 infra worker 가 담당한다.

## 9. Local Spark Contract Smoke

R2 Data Catalog credential 이 없어도 Bronze -> Silver 변환 계약은 로컬 Docker Spark 로 검증한다.

```bash
docker compose -f compose.lakehouse.yml --profile lakehouse-batch up -d spark
docker exec -it foundation-platform-spark spark-submit \
  /opt/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py
```

이 smoke 는 Bronze JSONL fixture 를 읽고 `silver.industrial_complexes` column order, required field,
domain value, deterministic UUIDv5 id, row checksum, unique source key, positive area gate 를 검증한 뒤
Parquet 으로 쓴다. 현 단계의 출력 대상은 local Parquet 이며, live R2/Iceberg credential 이 준비되면
같은 Silver frame 을 Iceberg REST Catalog writer 로 연결한다.

Spark job 의 성공 handoff 는 `foundation-platform.spark_run_summary.v1` JSON 이다. 이 summary 는
`job_name=industrial_complex_bronze_to_silver`, `contract=silver.industrial_complexes`,
write target, row count, persisted row count, source snapshot ids, quality metrics 를 포함한다.
Rust foundation-platform 는 이 summary 를 batch audit/promotion 입력으로 취급하고, Spark stdout 의
`silver-industrial-complexes-summary-json` line 또는 `--summary-output` 파일에서 읽는다.
credential/token 값은 summary 에 기록하지 않는다.

Rust contract 는 `lakehouse-domain::SparkRunSummary` 다. `validate_for_contract` 는 summary 의
schema version, target, write disposition, column contract, required columns, row count,
persisted row count, full source snapshot lineage, blocking quality metrics 를
`SILVER_INDUSTRIAL_COMPLEXES` 같은 static table contract 와 대조한다. `lakehouse-application` 은
`LakehouseBatchRunAudit` port 를 통해 검증된 summary 를 감사/승격 판단용으로 기록한다.

Audit 저장소는 `catalog.lakehouse_batch_run` 이다. 정본 데이터는 여전히 R2/Iceberg 에 있고,
이 테이블은 batch control-plane metadata 만 담는다. `summary_json` 은 원본 handoff 를 JSONB 로
보존하고, contract/target/write mode/row count/source snapshot 은 조회와 promotion guard 를 위해
정규 컬럼으로 중복 저장한다. `validate_only` 는 persisted row count 를 비워야 하며, write summary 는
persisted row count 가 candidate row count 와 같아야 한다.

Orchestration 진입점은 `lakehouse-application::RecordLakehouseBatchRun` 이다. 이 use case 는 raw summary JSON 을
받아 `SparkRunSummary` 로 파싱하고, static lakehouse table contract 를 통과한 summary 만
`LakehouseBatchRunAudit` port 로 넘긴다. 따라서 Spark worker, API, 또는 후속 scheduler 중 어느
경로에서 호출하더라도 validation 과 audit 저장 규칙은 하나로 유지된다.

승격 후보 조회는 `lakehouse-application::GetLakehousePromotionCandidate` 가 맡는다. 이 use case 는
`LakehouseBatchRunRepository` 에서 `validate_only` 가 아니고, source lineage 가 잘리지 않았으며,
`persisted_row_count = row_count` 인 최신 audit row 만 읽는다. 읽은 row 는 `summary_json` 과
정규 컬럼의 drift 를 다시 검사하고, static table contract 재검증을 통과한 경우에만 Gold/serving
promotion 입력으로 쓸 수 있다.

Live Iceberg write 는 dedicated smoke table 로 먼저 검증한다. Live R2/Iceberg credential 을 환경에
주입한 뒤 같은 Spark job 을 live write 모드로 실행한다.

```bash
docker exec -it foundation-platform-spark spark-submit \
  /opt/foundation-platform/infra/lakehouse/spark/jobs/industrial_complex_bronze_to_silver.py --live-write
```

기본 table 은 `silver.industrial_complexes_smoke` 이며, canonical
`silver.industrial_complexes` write 는 operator 가 명시적으로 table name 과 non-smoke 허용 플래그를
준 경우에만 실행한다. 이 경계는 PoC 데이터가 정본 table 을 오염시키지 않게 하기 위한 안전장치다.
