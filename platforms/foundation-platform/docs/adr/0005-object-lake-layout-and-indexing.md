# ADR 0005 - Object Lake Layout And Indexing

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-14 |
| 최종 정리 | 2026-07-14 |
| 상태 | Accepted |
| 소유자 | `foundation-platform` |
| 범위 | R2 Bronze, Iceberg Silver/Gold, immutable published artifacts |
| 관련 결정 | [ADR 0006](0006-lakehouse-table-format-and-serving-architecture.md), [ADR 0019](0019-bronze-readable-object-lake-postgres-catalog-ssot.md) |

## Decision

Foundation Platform의 대량 데이터 본체는 R2에 저장한다. PostgreSQL Catalog와 Iceberg
metadata가 identity, schema, snapshot, active pointer, checksum, lineage의 단일 출처다.

```text
R2 object key = 물리 주소
Postgres Catalog = Bronze identity/integrity/lineage SSOT
Iceberg metadata = Silver/Gold schema/snapshot/table SSOT
```

R2 경로를 사람이 읽을 수 있게 만들되, 애플리케이션은 경로 문자열을 파싱해 데이터의
버전, 최신성, 계보, 중복 여부를 판단하지 않는다.

## Layer Layout

### Bronze raw evidence

Bronze는 공급자 원본 바이트를 그대로 보존한다. 경로에는 공급자 또는 요청 범위를
식별하는 데 필요한 값만 넣는다.

```text
bronze/source={source_slug}/{request_partition...}/{provider_file_or_page}.{ext}
```

예시:

```text
bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip
bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip
bronze/source=datagokr__real_transaction_industrial_trade/period=2026-05/lawd=11680/page-000001.json
bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json
bronze/source=vworldkr__land_register/pnu=9999900601100010000/page-000001.json
```

API의 `period`, `lawd`, `sigungu`, `bjdong`, `pnu`는 어떤 요청 조각인지
구분하는 coverage identity이므로 경로에 남을 수 있다. 벌크 파일의 기준일, 갱신일,
공급자 파일 기간은 설명용 metadata이며 경로 identity로 사용하지 않는다.

모든 Bronze object는 Postgres Catalog에 다음 정보를 기록한다.

- `source_slug`
- `source_identity_key`
- `object_key`
- `checksum_sha256`
- `snapshot_period`, `snapshot_date`, `snapshot_granularity`, `snapshot_basis`
- `provider_file_id`, `provider_file_name`, `provider_updated_at`
- `request_params`, `ingestion_run_id`, `collected_at`

Bronze의 세부 identity와 date policy는 ADR 0019가 SSOT다.

### Silver and Gold Iceberg tables

Silver와 Gold의 canonical tables는 Iceberg가 관리한다.

```text
warehouse/silver/{table}/...
warehouse/gold/{table}/...
```

`metadata/`, `data/`, manifest list, manifest file, snapshot file의 실제 이름은 Iceberg가
생성한다. Foundation Platform 코드는 그 내부 object key를 조립하거나 의미를 파싱하지
않는다. 테이블 조회와 time travel은 Iceberg catalog의 table identifier와 snapshot
metadata를 사용한다.

예시:

```text
foundation.silver.building_register_unit
foundation.gold.industrial_complex
```

물리 경로의 partition field 이름은 Iceberg table spec이 결정한다. Bronze의 공급자 요청
partition과 Silver/Gold의 분석 partition은 목적이 다르므로 이름이 같을 필요가 없다.
동일 개념의 canonical field 이름은 semantic contract에서 통일한다.

### Immutable published artifacts

PBF, PMTiles, profile JSON, locator Parquet처럼 Iceberg table이 아닌 배포 산출물은 immutable
artifact id를 파일명으로 사용한다.

```text
gold/industrial-complex/profiles/{artifact_id}.json
gold/industrial-complex/spatial-locators/{artifact_id}.parquet
gold/parcel-marker-tiles/{artifact_id}/{z}/{x}/{y}.pbf
```

Catalog는 `artifact_id`, `schema_version`, `checksum_sha256`, `created_at`,
`source_snapshot_id`, `object_key`, active/previous pointer를 기록한다. 경로의
`artifact_id`는 물리 주소일 뿐이며, 데이터 버전의 의미는 Catalog metadata가 가진다.

Serving을 위한 mutable manifest가 필요하면 stable pointer를 cache로 둘 수 있다. 그
pointer는 canonical truth가 아니며 Catalog에서 재생성 가능해야 한다.

## Version Placement

R2 object key에는 다음 semantic version 표현을 넣지 않는다.

- `/v1/`, `/v2/`
- `version=...`
- `v1.json`, `gold-v2.parquet`
- timestamp를 버전 디렉터리로 직접 사용하는 수동 snapshot layout

다음 버전은 R2 경로 버전과 다른 계약이므로 유지한다.

- HTTP API major version: `/catalog/v1/...`
- event/schema contract: `*.v1.avsc`, `schema_version`
- Iceberg format/table metadata version

API와 event의 버전은 consumer compatibility 계약이다. R2 object의 데이터 버전은
Catalog/Iceberg metadata다. 둘을 같은 규칙으로 취급하지 않는다.

## Indexing

### Bronze

Postgres Catalog index가 source identity, dedupe, ingestion run, snapshot, checksum 조회를
담당한다. raw payload를 대량 JSONB row로 복제하지 않는다.

필수 조회 축:

- source + source identity
- source + snapshot date/period
- object key
- checksum
- ingestion run

### Silver and Gold

Iceberg manifest와 table statistics가 object/partition pruning을 담당한다. Trino와 Spark는
Iceberg catalog를 통해 읽는다. 애플리케이션이 R2 LIST로 최신 snapshot을 찾지 않는다.

### Serving indexes

낮은 지연이 필요한 PNU, 공간, 검색 조회는 PostGIS, search index, immutable locator artifact
같은 serving projection을 사용할 수 있다. projection은 canonical table과 snapshot에서
재생성 가능해야 하며 SSOT가 아니다.

## Stable Entity Identity

서비스 간 참조는 경로가 아니라 안정적인 canonical id를 사용한다.

- Parcel: `parcel:pnu:{pnu}`
- Building: 공급자 natural key를 포함한 deterministic building id
- Industrial complex: `industrial_complex:{official_code}`

공급자 natural key가 불안정하면 별도 entity-resolution assertion과 review workflow를
사용한다. object key를 entity id로 승격하지 않는다.

## Enforcement

- Bronze write는 `BronzeCommitter`만 수행한다.
- Bronze key는 canonical builder를 사용한다.
- Silver/Gold writer는 Iceberg catalog/table API를 사용한다.
- Published artifact는 Catalog record와 checksum 없이 활성화할 수 없다.
- R2 key에서 version/freshness/lineage를 파싱하는 production code는 허용하지 않는다.

## Consequences

- 운영자는 Bronze와 published artifact 경로를 직접 보고 대상을 식별할 수 있다.
- schema/data version과 최신성은 metadata 한 곳에서만 판단한다.
- API/event contract version은 안전하게 유지하면서 R2의 중복 version hierarchy는 없어진다.
- Silver/Gold는 수동 폴더 규칙이 아니라 Iceberg snapshot semantics를 따른다.
- R2 cleanup은 Catalog/Iceberg reference를 대조한 뒤 unreferenced object만 삭제해야 한다.
