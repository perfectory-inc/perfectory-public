# Industrial Complex SSOT Model

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-12 |
| 상태 | Draft |
| 결정 ADR | [ADR 0003 - Industrial Complex Catalog SSOT](../adr/0003-industrial-complex-catalog-ssot.md) |
| Object storage ADR | [ADR 0002 - R2 Primary Object Storage](../adr/0002-r2-primary-object-storage.md) |
| Vector tile ADR | [ADR 0004 - Static Vector Tile Runtime Contract](../adr/0004-static-vector-tile-runtime-contract.md) |
| Object lake/index ADR | [ADR 0005 - Object Lake Layout And Indexing](../adr/0005-object-lake-layout-and-indexing.md) |
| Lakehouse table ADR | [ADR 0006 - Lakehouse Table Format And Serving Architecture](../adr/0006-lakehouse-table-format-and-serving-architecture.md) |
| Lakehouse PoC | [Industrial Complex Lakehouse PoC](industrial-complex-lakehouse-poc.md) |

## 1. 목표

산업단지를 기준으로 공통 데이터가 흩어지지 않게 만든다. `foundation-platform` Catalog 는
산업단지의 단일 원장이고, `gongzzang` 과 `dawneer` 는 각자의 제품 데이터를 얹는다.

핵심 규칙은 세 가지다.

- 산업단지 자체에 붙는 사실과 운영 하위 리소스는 `foundation-platform` 가 소유한다.
- 사이트마다 다르게 보이는 표현은 `dawneer` 가 소유한다.
- 매물, 경매, 거래, 일반 사용자 행동은 `gongzzang` 이 소유한다.

## 2. 판단 기준

| 데이터 성격 | Owner | 예시 |
|---|---|---|
| 모든 제품에서 같은 값이어야 하는 산업단지 사실 | `foundation-platform` | 이름, 종류, 주소, 면적, 관리기관, 상태, 경계 |
| 산업단지에 종속된 운영 subobject | `foundation-platform` | 공지, 고시, 첨부, 도면, 필지, 건물, 공간 레이어, 3D asset |
| 산업단지의 업종 규칙 | `foundation-platform` | 유치업종, 허용업종, 필지별 업종 배정 |
| 특정 사이트의 렌더링 선택 | `dawneer` | 노출 여부, 정렬, 색상, 문구 override, 문의 채널 override |
| 부동산 상품과 시장 데이터 | `gongzzang` | 매물, 경매, 실거래, 검색 인덱스, 일반 사용자 북마크 |

애매한 경우에는 이렇게 판단한다. "이 값이 Dawneer 사이트 A 와 사이트 B 에서 달라도
되는가?" 답이 아니면 foundation-platform 다. 답이 예이면 Dawneer presentation 이다.

## 3. Canonical Catalog 모델

현재 M1 스키마에는 `industrial_complex`, `parcel`, `building`, `manufacturer`,
`outbox_event` 가 있다. 다음 모델은 M3.2 write 이관 전까지 확장해야 할 목표다.

### 3.1 Existing core

| Entity | 역할 | 주요 필드 |
|---|---|---|
| `catalog.industrial_complex` | 산업단지 aggregate root | `id`, `official_complex_code`, `name`, `kind`, `primary_bjdong_code`, `area_m2`, `version`, `updated_at` |
| `catalog.parcel` | 산업단지 내 필지 | `id`, `complex_id`, `pnu`, `kind`, `area_m2`, `version` |
| `catalog.building` | 필지에 속한 건물 | `id`, `parcel_id`, `purpose_code`, `structure_code`, `floor_area_m2` |
| `catalog.manufacturer` | 입주 제조사 | `id`, `primary_parcel_id`, `name`, `ksic_code` |
| `catalog.outbox_event` | consumer cache 갱신 이벤트 | `event_id`, `type`, `payload`, `occurred_at`, `published_at` |

### 3.2 Required expansion

| Entity | Owner rule | 주요 필드 |
|---|---|---|
| `catalog.source_record` | 모든 imported fact 의 출처 | `id`, `source`, `source_url`, `external_id`, `captured_at`, `checksum_sha256`, `raw_object_key` |
| `catalog.file_asset` | R2 객체 metadata | `id`, `object_key`, `mime_type`, `size_bytes`, `checksum_sha256`, `title`, `source_record_id`, `visibility` |
| `catalog.complex_notice` | 산업단지 공지/고시 | `id`, `complex_id`, `notice_type`, `title`, `summary`, `published_at`, `source_record_id`, `version` |
| `catalog.notice_attachment` | 공지 첨부 | `notice_id`, `file_asset_id`, `display_order` |
| `catalog.complex_attachment` | 산업단지 공식 첨부/이미지 | `complex_id`, `file_asset_id`, `asset_kind`, `display_order` |
| `catalog.blueprint` | 산업단지 도면 | `id`, `complex_id`, `file_asset_id`, `blueprint_kind`, `coordinate_system`, `scale`, `version` |
| `catalog.spatial_layer` | 지도/도면 위 공간 데이터 | `id`, `complex_id`, `parcel_id`, `blueprint_id`, `layer_kind`, `geometry_ref`, `source_record_id`, `version` |
| `catalog.vector_tile_manifest` | active static vector tile pointer | `id`, `current_version`, `previous_version`, `tiles_url_template`, `manifest_file_asset_id`, `source_record_id`, `published_at` |
| `catalog.vector_tile_artifact` | manifest `artifacts[layer]` metadata | `manifest_id`, `layer`, `source_layer`, `tile_min_zoom`, `tile_max_zoom`, `render_min_zoom`, `render_max_zoom`, `tilejson_file_asset_id`, `object_key_prefix` |
| `catalog.digital_twin_asset` | 3D/digital twin asset | `id`, `complex_id`, `parcel_id`, `building_id`, `file_asset_id`, `asset_kind`, `coordinate_transform`, `version` |
| `catalog.industry_group` | 업종 taxonomy group | `id`, `complex_id`, `name`, `description`, `version` |
| `catalog.industry_group_member` | group 에 속한 KSIC/업종 코드 | `industry_group_id`, `industry_code`, `industry_code_system` |
| `catalog.allowed_industry` | 산단 단위 허용/유치업종 규칙 | `id`, `complex_id`, `industry_group_id`, `rule_kind`, `source_record_id`, `version` |
| `catalog.parcel_industry_assignment` | 필지별 업종 배정 | `id`, `parcel_id`, `industry_group_id`, `assignment_kind`, `source_record_id`, `version` |

`geometry_ref` 는 ADR 0006 의 lakehouse 정책을 따른다. 산업단지, 필지, 건축물의
canonical spatial data 는 R2 + Iceberg/GeoParquet 계층에서 lineage 와 snapshot 을 가진다.
PostGIS, PMTiles, vector tiles 는 canonical source 가 아니라 derived serving layer 다.

폴리곤을 PMTiles 로 제공할지, 편집 가능한 polygon workflow 를 둘지, 운영자가 수정한
geometry 를 어떤 승인/승격 절차로 canonical lakehouse 에 반영할지는 후속 Spatial Serving
And Editable Geometry ADR 에서 확정한다.

Dawneer 의 `display_color_override` 같은 스타일 값은 이 모델에 들어가지 않는다.

### 3.3 Static vector tile manifest

`gold/manifest.json` 의 최종 capability owner 는 `foundation-platform` Spatial 이다.
현재 metadata table 과 실행 코드는 Spatial 물리 추출 전까지 legacy `catalog` schema/path 에
남아 있다. Gongzzang 은 manifest consumer only 이며, manifest version 이나 artifact metadata 를
write 하지 않는다.

Manifest contract 는 [ADR 0004](../adr/0004-static-vector-tile-runtime-contract.md)를 따른다.
필수 필드는 다음과 같다.

- `current_version`
- `previous_version`
- `tiles_url_template`
- `artifacts[layer]`
- `artifacts[layer].source_layer`
- `artifacts[layer].tile_min_zoom`
- `artifacts[layer].tile_max_zoom`
- `artifacts[layer].render_min_zoom`
- `artifacts[layer].render_max_zoom`
- `artifacts[layer].lineage.source_record_id`
- `artifacts[layer].lineage.manifest_file_asset_id`
- `artifacts[layer].lineage.tilejson_file_asset_id`
- `artifacts[layer].lineage.source_file_asset_ids`

`catalog.vector_tile_manifest.manifest_file_asset_id` 는 `catalog.file_asset` 의
`object_key = 'gold/manifest.json'` row 를 가리킨다. `catalog.vector_tile_artifact` 는
각 `artifacts[layer]` entry 의 SSOT 다. 개별 `.pbf` tile 마다 `file_asset` row 를 만들지
않고, `object_key_prefix`, tile count, total bytes, checksum/build metadata 로 tileset 을
대표한다.

## 4. 파일과 object storage

산업단지에 붙는 파일은 모두 `catalog.file_asset` 을 거친다.

| 파일 종류 | Canonical owner | 비고 |
|---|---|---|
| 공식 도면 PDF/CAD/image | `foundation-platform` | `blueprint.file_asset_id` |
| 공지 첨부 파일 | `foundation-platform` | `notice_attachment` |
| 산단 공식 대표 이미지 | `foundation-platform` | `complex_attachment` |
| 3D model, tileset, glTF | `foundation-platform` | `digital_twin_asset.file_asset_id` |
| `gold/manifest.json` | `foundation-platform` | `vector_tile_manifest.manifest_file_asset_id` |
| `gold/vector-tiles/artifacts/{artifact_id}/{layer}.json` | `foundation-platform` | `vector_tile_artifact.tilejson_file_asset_id` |
| `gold/vector-tiles/artifacts/{artifact_id}/{layer}/{z}/{x}/{y}.pbf` | `foundation-platform` | `vector_tile_artifact.object_key_prefix` |
| 매물 사진 | `gongzzang` | listing 상품 asset |
| 사이트 캠페인 이미지 | `dawneer` | site presentation asset |

DB/API 이름은 `object_key` 와 `objectKey` 를 쓴다. `s3_key`, `s3Key`, `S3Service`
는 신규 스키마와 DTO 에서 금지한다.

## 5. API Boundary

초기 OpenAPI 는 complexes create/get 와 parcel kind update 만 가진다. SSOT 전환에는
다음 API surface 가 필요하다.

### 5.1 Read API

| Endpoint | Consumer | 설명 |
|---|---|---|
| `GET /catalog/v1/complexes/{id}` | 모두 | 산업단지 기본 정보 |
| `GET /catalog/v1/complexes/{id}/parcels` | 모두 | 산단 필지 목록 |
| `GET /catalog/v1/parcels/{id}` | 모두 | 단일 필지 |
| `GET /catalog/v1/complexes/{id}/buildings` | 모두 | 산단 건물 목록 |
| `GET /catalog/v1/complexes/{id}/manufacturers` | 모두 | 입주 제조사 목록 |
| `GET /catalog/v1/complexes/{id}/notices` | Dawneer 중심 | 공지/고시 |
| `GET /catalog/v1/complexes/{id}/attachments` | 모두 | 공식 첨부/이미지 |
| `GET /catalog/v1/complexes/{id}/blueprints` | Dawneer 중심 | 도면 |
| `GET /catalog/v1/complexes/{id}/spatial-layers` | Dawneer 중심 | 지도/도면 레이어 |
| `GET /catalog/v1/vector-tiles/manifest` | Gongzzang 중심 | active static vector tile manifest |
| `GET /catalog/v1/complexes/{id}/digital-twin-assets` | Dawneer 중심 | 3D asset |
| `GET /catalog/v1/industry-groups` | 모두 | 업종 taxonomy |
| `GET /catalog/v1/parcels/{id}/industry-assignments` | 모두 | 필지별 업종 배정 |

### 5.2 Write API

Write API 는 Staff 권한만 허용한다. Consumer 서비스가 자기 DB 를 원장처럼 갱신하는
경로는 만들지 않는다.

| Endpoint | 권한 | 설명 |
|---|---|---|
| `POST /catalog/v1/complexes` | Catalog admin | 산단 등록 |
| `PATCH /catalog/v1/complexes/{id}` | Catalog admin | 산단 기본 정보 갱신 |
| `PUT /catalog/v1/complexes/{id}/parcels:bulk-upsert` | ETL/admin | 필지 upsert |
| `PUT /catalog/v1/complexes/{id}/notices:bulk-upsert` | ETL/admin | 공지 upsert |
| `POST /catalog/v1/file-assets` | Catalog admin | R2 object metadata 등록 |
| `PUT /catalog/v1/complexes/{id}/blueprints` | Catalog admin | 도면 등록/교체 |
| `PUT /catalog/v1/complexes/{id}/spatial-layers` | Catalog admin | 공간 레이어 등록/교체 |
| `PUT /catalog/v1/vector-tiles/manifest:promote` | Catalog admin/ETL | 검증된 static vector tile manifest pointer flip |
| `POST /catalog/v1/vector-tiles/manifest:rollback` | `MASTER_ADMIN` / `CATALOG_ADMIN` / `VECTOR_TILE_ADMIN` | Staff Identity-verified staff rollback with `expected_current_version` stale guard + audit event |
| `PUT /catalog/v1/complexes/{id}/digital-twin-assets` | Catalog admin | 3D asset 등록/교체 |
| `PUT /catalog/v1/complexes/{id}/industry-groups` | Catalog admin | 업종 group 관리 |
| `PUT /catalog/v1/parcels/{id}/industry-assignments` | Catalog admin | 필지 업종 배정 |

모든 write 는 optimistic locking 또는 idempotency key 를 가져야 한다. ETL bulk upsert 는
`source_record_id` 와 batch id 를 함께 저장한다.

## 6. Events

Consumer cache 는 outbox event 로 갱신한다.

| Event | 발생 조건 | Consumer 반응 |
|---|---|---|
| `IndustrialComplexUpdated.v1` | 산단 기본 정보 변경 | local cache/search invalidation |
| `ParcelUpserted.v1` | 필지 생성/변경 | 필지 cache 갱신 |
| `BuildingUpserted.v1` | 건물 생성/변경 | 건물 cache 갱신 |
| `ManufacturerUpserted.v1` | 제조사 생성/변경 | 제조사 cache 갱신 |
| `ComplexNoticeUpserted.v1` | 공지 생성/변경 | Dawneer 사이트 자료 갱신 |
| `FileAssetUpserted.v1` | 파일 asset 등록/변경 | CDN/cache 갱신 |
| `BlueprintUpserted.v1` | 도면 등록/변경 | Dawneer blueprint view 갱신 |
| `SpatialLayerUpserted.v1` | 공간 레이어 변경 | 지도/도면 layer cache 갱신 |
| `VectorTileManifestPromoted.v1` | `gold/manifest.json` pointer flip | Gongzzang manifest cache 갱신 |
| `DigitalTwinAssetUpserted.v1` | 3D asset 변경 | 3D viewer cache 갱신 |
| `IndustryGroupChanged.v1` | 업종 taxonomy 변경 | 검색 facet, 필지 표시 갱신 |
| `ParcelIndustryAssignmentChanged.v1` | 필지 업종 배정 변경 | 필지 상세, 검색 facet 갱신 |

Event payload 는 consumer local row id 를 담지 않는다. foundation-platform ID 와 changed fields,
source version, occurred_at 을 기준으로 한다.

## 7. Dawneer 목표 모델

Dawneer 는 산업단지 데이터를 만들지 않고 보여준다.

남는 모델:

```text
dawneer.site
dawneer.site_page
dawneer.site_deployment
dawneer.site_campaign
dawneer.site_catalog_presentation
dawneer.site_layer_presentation
```

예시:

```text
dawneer.site_catalog_presentation
  id
  site_id
  foundation_platform_complex_id
  foundation_platform_parcel_id nullable
  foundation_platform_layer_id nullable
  visible
  display_order
  title_override nullable
  description_override nullable
  image_override nullable
  contact_channel_override nullable
  display_color_override nullable
  archived_at nullable
  version
```

금지:

- Dawneer 에서 `area_sqm`, `parcel_type_id`, `industry_group_id`, `blueprint_id` 를
  canonical write source 로 유지하는 것.
- foundation-platform 값을 Dawneer form 에서 수정한 뒤 Dawneer DB 만 바꾸는 것.
- site별 presentation field 를 foundation-platform canonical table 에 저장하는 것.

허용:

- 성능을 위한 read cache. 단, 이름에 `_cache` 또는 `cached_` 를 넣고 event 로만 갱신한다.
- 사이트별 문구, 색상, 노출 여부, 정렬, 연락 채널 override.
- Dawneer 캠페인 전용 이미지. 공식 산단 이미지가 아니어야 한다.

## 8. Gongzzang 목표 모델

Gongzzang 은 foundation-platform ID 를 참조해서 부동산 상품을 만든다.

```text
gongzzang.listing
  id
  foundation_platform_complex_id nullable
  foundation_platform_parcel_id nullable
  foundation_platform_building_id nullable
  title
  listing_price_krw
  status

gongzzang.court_auction
  id
  foundation_platform_parcel_id nullable
  foundation_platform_building_id nullable
  auction_case_no

gongzzang.user
  id
  ...
```

Gongzzang 일반 사용자, 북마크, 검색 히스토리, 매물 문의는 foundation-platform Staff Identity 와
합치지 않는다.

## 9. Migration 순서

| Phase | 목표 | 산출물 |
|---|---|---|
| D0 | 모델 고정 | ADR 0003, 이 문서, ownership matrix link |
| D1 | foundation-platform schema 확장 | catalog migration, domain entities, DTO, repository tests |
| D2 | read API 확장 | complexes/parcels/notices/assets/blueprints/layers read endpoints |
| D3 | Dawneer bridge | `foundation_platform_*_id`, presentation table, shadow read diff |
| D4 | Gongzzang bridge | listing/auction catalog reference, shadow read diff |
| D5 | write owner switch | Dawneer/gongzzang 산단 write path 를 foundation-platform API 로 전환 |
| D6 | legacy cleanup | consumer canonical columns rename/drop, cache/presentation 만 유지 |

Static vector tile migration:

| Phase | 목표 |
|---|---|
| T0 | Gongzzang ADR 0036 contract 를 foundation-platform ADR 0004 로 상속 |
| T1 | `gold/manifest.json`, TileJSON, source inputs 를 `catalog.file_asset` 와 `catalog.source_record` 에 연결 |
| T2 | manifest promote 를 foundation-platform Catalog write path 로 이전 |
| T3 | Gongzzang frontend 를 manifest consumer only 로 고정 |
| T4 | Gongzzang ETL manifest write path 제거 |

## 10. 완료 정의

- 산업단지 관련 canonical write 는 foundation-platform Catalog API 하나로만 가능하다.
- Dawneer 는 사이트별 presentation override 만 write 한다.
- Gongzzang 은 매물/경매/일반 사용자 데이터를 write 하고 산업단지 ID 만 참조한다.
- 도면, 3D, 공간 레이어, 공식 첨부는 `file_asset.object_key` 기준으로 foundation-platform 에 있다.
- imported data 는 source lineage 와 version 을 가진다.
- consumer read model 은 event 로 갱신되고 직접 DB SELECT/WRITE 에 의존하지 않는다.
