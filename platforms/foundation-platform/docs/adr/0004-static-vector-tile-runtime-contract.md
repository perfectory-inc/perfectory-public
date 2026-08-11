# ADR 0004 - Foundation Vector Tile Publication Contract

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-12 |
| 상태 | Accepted |
| 최종 개정 | 2026-07-24 |
| 상속 | [`gongzzang ADR 0036`](../../../../products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md) |
| 범위 | `foundation-platform` Catalog, Martin/PostGIS/PMTiles publication, `gongzzang` map runtime |

## 결정

Foundation Catalog은 active schema-v2 vector-tile manifest와 각 publication unit의 active
release에 대한 권위다. 제한된 migration 동안 `gold/manifest.json`은 기존 parcel-anchor
runtime이 소비하는 고정 schema-v1 pointer로 남는다. Schema v2는 별도의 재생성 가능한 R2
projection `gold/vector-tiles/runtime-manifest.json`을 사용하며 두 object 모두 canonical state가
아니다.

Gongzzang ADR 0036 의 static vector tile runtime contract 를 상속하되,
foundation-platform cutover 이후에는 필지, 산업단지, 행정구역, 건물 등 Catalog spatial
layer 의 vector tile manifest 를 `foundation-platform` 가 생성, 검증, publish 한다.

`gongzzang` 은 manifest consumer only 다. Gongzzang 은 manifest 를 읽어 지도 source 를
구성할 수 있지만, manifest version, artifact metadata, lineage, file asset 연결을
직접 write 하지 않는다.

공개 불변식은 다음과 같다.

```text
(publication_unit, serving_generation)
  -> exactly one complete Martin source
  -> DynamicPostgis XOR StaticPmtiles
```

완전한 source는 해당 단위에서 현재 보이는 모든 feature를 포함한다. 같은 단위의 static과
dynamic 표현은 브라우저나 Martin에서 조합하지 않는다. feature tombstone/suppression 전송은
없다. 단위마다 서로 다른 source 종류를 독립적으로 선택할 수 있다.

이 계약은 Foundation 공개/reference 단위만 소유한다. Gongzzang listing marker, listing
visibility, `filter_hash`, marker-delta/filter-mask 동작은 Gongzzang ADR 0037/0038이 계속
소유하며 이 매니페스트의 공개 단위가 아니다.

## Runtime Pointer

legacy schema-v1 pointer는 유지한다.

```text
gold/manifest.json                                      # schema v1, frozen during migration
```

Schema v2 uses a distinct mutable runtime projection:

```text
gold/vector-tiles/runtime-manifest.json                 # schema v2, rebuildable
```

보존/rollback object key 는 immutable release id 규칙을 따른다.

```text
gold/vector-tiles/manifests/{manifest_id}.json
gold/vector-tiles/releases/{publication_unit}-{release_id}.pmtiles
gold/vector-tiles/releases/{publication_unit}-{release_id}.tilejson.json
```

> **갱신 (2026-07-30):** release 경로에서 `{release_id}/` 디렉터리 구획을 뺐다. 이 문서가
> 적었던 중첩 형태는 한 번도 배포되지 않았다 — `r2_layout.rs`는 처음부터 평평한 형태를 썼고
> 런북도 그 형태를 기록한다. 도메인 검증기가 object key의 **파일명만** 비교했기 때문에 두 형태가
> 모두 통과했고, 이 문서를 보고 URL을 만든 소비자는 404를 받았을 것이다.
>
> 이제 `catalog_domain::static_release_pmtiles_object_key`가 이 규칙의 유일한 정의이며,
> 검증기는 전체 key를 비교하고 `r2_layout.rs`는 그 함수에 위임한다. 중첩 형태는 도메인 테스트의
> 거부 사례로 고정했다.

`gold/manifest.json`을 schema-v2 byte로 덮어쓰지 않는다. 다음 source가 자체 v2
the bounded legacy anchor consumer until those sources have their own proven v2 producer/consumer
path. Every v2 Catalog manifest has the same UUID as `current_version`; its immutable R2 projection
uses that UUID as `{manifest_id}` and is written create-only. The mutable
`gold/vector-tiles/runtime-manifest.json` object is a no-cache pointer projection, and canonical
truth is the Catalog. Immutable manifest and release objects are never overwritten. Catalog records
active and retained release history; both v2 projections can be regenerated from that state.

정본 Bronze·lakehouse·recovery 객체와 제공 파생물은 서로 다른 버킷을 사용해야 한다.
derivative 버킷은 기본적으로 비공개다. Martin은 별도 버킷 범위 읽기 전용 R2 인증으로
S3 호환 API를 통해 `s3://` PMTiles를 읽는다. 객체 prefix는 discovery 경계이지 IAM 경계가
아니다. 공개 `r2.dev` URL이나 custom-domain origin은 별도 보안 결정을 거쳐야 하며 기본값이 아니다.

## Schema version dispatch

`schema_version`은 최소값이 아닌 정확한 호환성 discriminator다. producer와 consumer는 `1`과
`2`를 별도 strict DTO로 dispatch하고 그 외 값을 거부한다. `/catalog/v1`
HTTP 경로 segment는 매니페스트 스키마 버전과 독립적이다.

스키마 v1은 이미 공개된 개별 flat MVT 객체를 위한 제한된 레거시 계약으로 유지한다.
Schema v2가 Martin 단일 source 공개의 운영 계약이다. v1 필드에 v2 의미를 재사용하지 않는다.

schema-v1 `catalog.vector_tile_manifest`와 `catalog.vector_tile_artifact` table은 변경하지
않는다. Schema v2는 normalized publication-unit, release, release-layer, immutable-manifest,
manifest-unit-selection, singleton-pointer, build-job, refresh-observation table을 별도로
사용한다. 이 storage boundary가 v2가 기존 flat-MVT 제약을 약화하지 못하도록 기계적으로 막는다.

## Legacy manifest schema v1

foundation-platform 가 publish 하는 manifest 는 최소한 다음 필드를 포함한다.

```json
{
  "schema_version": 1,
  "current_version": "0196e7e0-3c20-7000-8000-000000000042",
  "previous_version": "0196e7e0-3c20-7000-8000-000000000041",
  "tiles_url_template": "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf",
  "published_at": "2026-05-12T00:00:00Z",
  "artifacts": {
    "parcels": {
      "source_layer": "parcels",
      "tile_min_zoom": 8,
      "tile_max_zoom": 16,
      "render_min_zoom": 10,
      "render_max_zoom": 22,
      "tilejson_object_key": "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels.json",
      "object_key_prefix": "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels/",
      "flat_tile_count": 123456,
      "flat_tile_total_bytes": 987654321,
      "feature_filter_properties": {
        "pnu": "pnu"
      },
      "lineage": {
        "source_record_id": "00000000-0000-0000-0000-000000000000",
        "manifest_file_asset_id": "00000000-0000-0000-0000-000000000000",
        "tilejson_file_asset_id": "00000000-0000-0000-0000-000000000000",
        "source_file_asset_ids": [
          "00000000-0000-0000-0000-000000000000"
        ]
      }
    }
  }
}
```

Required manifest fields:

- `current_version`
- `previous_version`
- `tiles_url_template`
- `artifacts`

Required `artifacts[layer]` fields:

- `source_layer`
- `tile_min_zoom`
- `tile_max_zoom`
- `render_min_zoom`
- `render_max_zoom`
- `tilejson_object_key`
- `object_key_prefix`
- `lineage.source_record_id`
- `lineage.manifest_file_asset_id`
- `lineage.tilejson_file_asset_id`
- `lineage.source_file_asset_ids`

Optional `artifacts[layer].feature_filter_properties` maps logical filter identities to concrete
feature property names inside the vector tile. foundation-platform publishes only public/reference
properties it owns. Product-owned properties such as listing price, listing status, exposure rules,
or product search filters must not appear in this manifest.

Current foundation-platform-owned reference mappings:

| Manifest artifact | Logical filter property | Vector tile feature property |
|---|---|---|
| `parcels` | `pnu` | `pnu` |
| `parcel_anchor` | `pnu` | `pnu` |
| `complex` | `official_complex_code` | `official_complex_code` |

소비자는 `feature_filter_properties`에 있을 때만 filter property가 존재한다고 가정한다.

`tiles_url_template`은 `{object_key_prefix}`, `{z}`, `{x}`, `{y}` placeholder를 포함해야 한다.
runtime은 `{object_key_prefix}`를 `artifacts[layer].object_key_prefix`로 치환한다.

새 v1 publication은 v2 producer/consumer cutover 후 중단한다. 기존 v1 manifest는 제한된
migration 기간에만 읽을 수 있다.

## Manifest schema v2

V2는 PMTiles object를 flat tile directory인 것처럼 취급하지 않고 전환되는 unit을 모델링한다.

```json
{
  "schema_version": 2,
  "current_version": "0196e7e0-3c20-7000-8000-000000000052",
  "manifest_generation": 108,
  "refresh_after_seconds": 4,
  "published_at": "2026-07-24T00:00:00Z",
  "publication_units": {
    "parcels": {
      "data_revision": "0196e7e0-3c20-7000-8000-000000000061",
      "serving_generation": 42,
      "active_release_id": "0196e7e0-3c20-7000-8000-000000000062",
      "canonical_iceberg_snapshot_id": "70000000000000001",
      "source": {
        "kind": "static_pmtiles",
        "martin_source_id": "parcels-0196e7e0-3c20-7000-8000-000000000062",
        "tiles_url_template": "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
        "pmtiles_object_key": "gold/vector-tiles/releases/parcels-0196e7e0-3c20-7000-8000-000000000062.pmtiles",
        "pmtiles_file_asset_id": "0196e7e0-3c20-7000-8000-000000000063",
        "pmtiles_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "pmtiles_bytes": 987654321
      },
      "layers": {
        "parcels": {
          "source_layer": "parcels",
          "feature_id_property": "pnu",
          "tile_min_zoom": 8,
          "tile_max_zoom": 16,
          "render_min_zoom": 10,
          "render_max_zoom": 22,
          "feature_filter_properties": {
            "pnu": "pnu"
          }
        }
      },
      "lineage": {
        "source_record_id": "0196e7e0-3c20-7000-8000-000000000064",
        "source_file_asset_ids": [
          "0196e7e0-3c20-7000-8000-000000000065"
        ]
      }
    }
  }
}
```

V2 scalar rules:

- `current_version`, `data_revision`, `active_release_id`, 모든 file/record ID는 UUID다.
- `manifest_generation`은 global poll token일 뿐 source를 선택하지 않는다.
- `manifest_generation`과 `serving_generation`은 `1..=9007199254740991` 범위의 정수여서
  JavaScript가 정밀도를 잃지 않는다.
- `canonical_iceberg_snapshot_id`는 양의 10진 **string**이다. 실제 Iceberg snapshot ID는
  JavaScript safe integer 범위를 넘으므로 JSON number로 보내지 않는다.
- schema v2의 `refresh_after_seconds`는 정확히 `4`다. schedule interval이지 source identity나
  겹치는 poll을 허용하는 값이 아니다.
- 모든 `source.tiles_url_template`는 `{z}`, `{x}`, `{y}`를 정확히 한 번 포함하는 absolute
  Martin URL이다. 운영 publication은 HTTPS가 필요하다. checked-in Docker proof에서는
  loopback literal 또는 `localhost`에 한해 HTTP parse를 허용하지만 production publish gate는
  loopback을 포함한 모든 HTTP URL을 거부한다. Dynamic URL은 안정적·query-free이며 Catalog
  runtime-manifest pointer가 commit된 PostGIS revision을 선택한다. Static URL은 release 주소의
  immutable URL이다. consumer가 `.pbf`를 덧붙이거나 route를 다시 쓰지 않는다.
- `publication_units`와 각 unit의 `layers` map은 비어 있지 않다.
- `data_revision`은 정확한 logical feature set을 식별한다. 같은 data revision에서
  dynamic→static을 포함해 완전한 다른 release를 선택할 때 `serving_generation`이 바뀐다.
- `active_release_id`는 immutable release descriptor를 식별하며 다른 source나 generation에
  재사용하지 않는다.

단위의 `source`는 닫힌 tagged union이다. 알 수 없는 종류나 필드는 검증에 실패한다.

`dynamic_postgis` contains exactly:

```json
{
  "kind": "dynamic_postgis",
  "martin_source_id": "parcels",
  "tiles_url_template": "https://tiles.example.com/parcels/{z}/{x}/{y}",
  "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000071",
  "cache_policy": "no_store"
}
```

`dynamic_postgis`는 unit에 설정한 안정적인 명시적 Martin source ID를 사용한다. tile URL은
query-free이고 URL parameter가 아닌 Catalog runtime-manifest pointer가 정확한 commit PostGIS
revision을 선택한다. Martin in-process cache는 끄고 route는 `no_store`다.
`serving_generation`은 manifest/source refresh token이며 historical PostGIS snapshot selector가
아니다.

`static_pmtiles`는 v2 예시 field를 포함한다. object key·checksum·byte size·file-asset identity가
필수다. filename은
`{publication_unit}-{release_id}.pmtiles`다. Martin이 discover한 source ID는 release 주소의
filename stem과 정확히 같고 URL은 immutable이다. Gongzzang은 logical Mapbox source ID를
안정적으로 유지하면서 검증된 tile URL만 교체한다. `object_key_prefix`, `flat_tile_count`,
`flat_tile_total_bytes` 같은 flat-object field는 v2에서 금지한다.

각 unit source는 해당 unit에 선언된 모든 `layers`를 제공하고 consumer에 필요한 미선언 layer를
제공하지 않는다. `source_layer`는 DB table이 아닌 MVT layer ID다. 각 layer는 하나의 canonical
lowercase `feature_id_property`를 선언하며 PostGIS, PMTiles, TileJSON `vector_layers[].id`,
Martin, Mapbox `promoteId`, decoded-tile test가 같은 identity를 사용한다.

첫 schema-v2 이전 단위는 의도적으로 `parcels` 하나뿐이다.

| Initial v2 publication unit | Required MVT layers |
|---|---|
| `parcels` | `parcels` |

다음 단위는 각 producer·consumer 동등성이 증명된 뒤에만 나중에 이전할 수 있다.

| Publication unit | Required MVT layers |
|---|---|
| `complex` | `complex` |
| `parcel_anchor_aggregate` | `parcel_anchor_aggregate` |
| `parcel_anchor` | `parcel_anchor` |
| `admin` | `admin` when published |
| `buildings` | `buildings` when published |

두 parcel-anchor layer는 현재 Gongzzang marker runtime에 계속 필요하다. 첫 v2 단계에서
Gongzzang은 고정 v1 manifest에서 두 source를 읽고 `parcels`는 v2 manifest에서 읽는다. v1
`parcels` artifact를 함께 등록하지 않는다. `parcels` source 변경이 legacy anchor source를
retarget하거나 제거하지 않는다. 이 제한된 dual-manifest migration은 하나의 publication unit에
두 source를 등록하지 않는다.

## Catalog ownership

매니페스트는 foundation-platform 공간 사실과 파생 runtime 산출물을 설명하므로 Catalog
데이터다.

| Resource | Owner | Catalog link |
|---|---|---|
| `gold/manifest.json` | `foundation-platform` Catalog | frozen legacy schema-v1 projection during migration |
| `gold/vector-tiles/runtime-manifest.json` | `foundation-platform` Catalog | rebuildable schema-v2 runtime projection |
| `gold/vector-tiles/manifests/{manifest_id}.json` | `foundation-platform` Catalog | create-only immutable manifest projection; ID equals `current_version` and links to its `catalog.file_asset` row |
| `gold/vector-tiles/releases/{publication_unit}-{release_id}.pmtiles` | `foundation-platform` Catalog | immutable release `pmtiles_file_asset_id` |
| `publication_units[unit]` | `foundation-platform` Catalog | active release plus layer and build metadata |
| `lineage.source_record_id` | `foundation-platform` Catalog | `catalog.source_record.id` |
| `lineage.*file_asset_id` | `foundation-platform` Catalog | `catalog.file_asset.id` |

Legacy v1 individual `.pbf` tiles do not require one `catalog.file_asset` row per object. V2 has one
file-asset row for each PMTiles archive and records release validation evidence separately.

## Spatial layer mapping

각 v1 산출물 또는 v2 공개 단위는 Foundation 공간 계층에 매핑된다. v2 unit 이름은 독립적으로
전환되는 serving boundary이고 MVT `source_layer`는 renderer contract로 남는다.

| Publication unit | Foundation Platform source |
|---|---|
| `parcels` | `catalog.parcel` + `catalog.spatial_layer(layer_kind = 'parcel_boundary')` |
| `complex` | `catalog.industrial_complex` + `catalog.spatial_layer(layer_kind = 'complex_boundary')` |
| `admin` | imported admin boundary `catalog.spatial_layer` |
| `buildings` | `catalog.building` + building footprint layer |

매니페스트 `source_layer` 값은 DB 테이블 이름이 아니라 MVT 내부 vector tile layer 이름이다.
runtime style과 click 처리에서 안정적으로 유지해야 한다.

## Gongzzang Runtime Contract

Gongzzang runtime은 다음을 해야 한다.

1. `GET /catalog/v1/vector-tiles/runtime-manifest`에서 schema v2를 가져오고 제한된 migration
   동안 기존 v1 위치에서 고정 v1 anchor manifest도 가져온다. 각 문서를 `schema_version`으로
   정확히 dispatch한다.
2. v2 `parcels`가 active인 동안 기존 v1 flat-object materialization rule은
   `parcel_anchor_aggregate`와 `parcel_anchor`에만 적용한다. v1 `parcels` artifact를 동시에
   등록하지 않는다.
3. v2에서는 각 publication unit마다 해당 unit의 tagged `source.tiles_url_template`에서 vector
   source 하나만 등록한다.
4. v2 `parcels`와 두 legacy v1 anchor source를 현재 map workflow의 core로 취급한다. 새 v2
   unit을 조용히 건너뛸 수 없다. 발행 전에 Foundation layer-registry 항목과 producer parity를
   검증하지 않으면 consumer가 fail closed하고 마지막 유효 map을 유지한다.
5. map이 보일 때 4초마다 겹치지 않는 conditional request 하나로 v2 endpoint를 poll한다. global
   `manifest_generation`이 바뀌면 전체 validation을 수행하고 `serving_generation`이 바뀐
   unit만 교체한다.
6. 새 manifest나 source가 invalid/unready면 현재 등록된 source descriptor를 유지한다. static
   retention은 정확한 immutable release를 반환한다. dynamic retention은 계속 사용할 수 있지만
   Catalog runtime-manifest pointer가 선택한 최신 commit projection을 읽을 수 있다. URL query는
   historical rollback이 아니다. 한 unit의 이전·새 source를 함께 render하지 않는다.
7. 진단·source disclosure·support report에 manifest lineage를 사용한다.

Gongzzang runtime은 다음을 해서는 안 된다.

- `gold/manifest.json` 또는 `gold/vector-tiles/runtime-manifest.json`에 쓴다.
- `current_version`, generation value, active release를 다시 쓴다.
- 누락된 publication unit·layer·lineage·source metadata를 합성한다.
- `manifest_generation`을 source selector로 사용한다.
- 한 unit에 static과 dynamic source를 조합한다.
- Naver internal tile URL을 domain data source로 사용한다.
- build-time env var를 production active version pointer로 사용한다.

## Publish Gate

필수 check가 모두 통과하지 않으면 foundation-platform promote는 Catalog active state를 바꾸거나
`gold/vector-tiles/runtime-manifest.json`을 projection하기 전에 실패해야 한다. V2 promotion은
`gold/manifest.json`을 절대 바꾸지 않는다.

- candidate manifest UUID와 release ID가 새롭고 immutable이다.
- compare-and-swap이 현재 active release와 optimistic version에 맞는다.
- 모든 required unit이 tagged source 하나를 선택하고 선언된 모든 layer가 정확한 Martin route에서
  비어 있지 않은 대표 tile로 decode된다.
- complete candidate에 stable `source_layer`, canonical feature identity, valid zoom range,
  source/geometry lineage가 있다.
- dynamic source의 complete PostGIS projection revision이 준비되고 selected Iceberg snapshot과
  audit된 publication input에서 재구성 가능하다.
- static source의 PMTiles object가 create-only로 존재하고 checksum·size가 일치하며 HTTP
  Range/S3 read가 Martin을 통해 동작하고 Martin source가 release 주소다.
- static build input이 active dynamic release/data revision과 여전히 같아야 하며 다르면 build는
  `SUPERSEDED`다.
- 모든 production URL은 HTTPS다. loopback HTTP parser 예외는 proof 전용이며 이 gate를 통과할 수 없다.
- manifest·PMTiles·source input에 필요한 Catalog `file_asset`/`source_record` row가 있다.
- 작고 mutable한 manifest projection만 expire/purge하고 immutable release path는 건드리지 않는다.

이 경계에서 add·modify·delete는 동일하다. 각각 complete candidate feature set을 만든다.
feature-level overlay·subtraction·tombstone garbage collection은 금지한다.

## API Boundary

Foundation은 Catalog API로 active manifest를 노출한다. browser는 이 endpoint를 live
conditional-poll SLO에 사용한다. R2 manifest는 비동기 재생성 가능한 distribution/boot projection이며
Catalog authority보다 최신이어서는 안 된다.

runtime endpoint는 브라우저 JavaScript가 직접 호출하므로 익명 읽기 전용 공개 계약이다.
Foundation traffic/auth registry에 제한된 canonical metric 경로, 명시적 edge 정책, CORS
allow-list, `If-None-Match` 허용, 노출된 `ETag`, service-identity middleware 제외를 기록한다.
출시 client 예산은 표시 중인 지도마다 4초에 겹치지 않는 요청 하나다. v2를 켜기 전에 배포가
선언한 동시 표시 지도 예산의 두 배로 endpoint를 검증한다.

Recommended API surfaces:

```text
GET /catalog/v1/vector-tiles/runtime-manifest
GET /catalog/v1/vector-tiles/runtime-manifests/{version}
POST /catalog/v1/vector-tiles/publication-units/{unit}:activate-dynamic
POST /catalog/v1/vector-tiles/publication-units/{unit}:promote-static
POST /catalog/v1/vector-tiles/publication-units/{unit}:rollback-serving
```

기존 `/catalog/v1/vector-tiles/manifest`, `manifest:promote`, `manifest:rollback` 표면은
schema-v1 전용으로 남긴다. route 이름·payload·저장 테이블·event·object key를 v2에 재사용하지 않는다.

API 응답과 R2 projection은 같은 엄격한 wire 계약을 사용한다. HTTP `ETag`는
`current_version`을 나타내며 `manifest_generation`은 검증된 문서 안에서 반환한다.

Promote는 Foundation Catalog admin operation이다. 하나의 transaction에서 singleton runtime
manifest pointer를 먼저 lock한 뒤 영향받은 publication unit을 lock하고 immutable release/lineage를
등록한다. 예상 active release와 optimistic version을 요구하고 candidate를 선택하며 해당 unit의
`serving_generation`을 증가시킨다. pointer가 lock된 동안 모든 unit selection을 읽고 새 immutable
manifest UUID를 만들며 global `manifest_generation`을 증가시키고 R2 projection용 outbox event를
발행한다. 모든 publication path는 `runtime_manifest_pointer -> publication_unit -> release rows`의
고정 lock 순서를 사용한다. 중복 release ID·manifest ID·generation·object key는 fail closed한다.
다른 unit의 동시 activation도 같은 pointer lock 아래 serialize해 unit selection이 유실되거나
섞인 global manifest가 commit되지 않는다.

수동 serving rollback은 **같은** `data_revision`의 보존·검증된 immutable release를 대상으로 하고
expected-active-release compare-and-swap을 요구하며 해당 release를 선택하는 새 manifest를 만든다.
기존 manifest document를 수정하거나 다시 발행하지 않는다. business data를 되돌리려면 새
canonical revision을 만들고 일반 publication flow를 따른다.

롤백 API는 변경 전에 foundation-platform Staff Identity를 통해 직원 Bearer token을 검증해야 한다.
`MASTER_ADMIN`, `CATALOG_ADMIN`, `VECTOR_TILE_ADMIN`만 벡터 타일 매니페스트를 롤백할 수 있다.
staff identity는 Zitadel token 검증에서 오고 이 결정에 쓰는 role 집합은 foundation-platform
Staff Identity DB role에서 온다. `operator_staff_id`는 request body를 신뢰하지 않고 검증된
staff session에서 온다. event에는 검증된 `operator_staff_id`, optional `request_id`, 이전/새
release ID, 예상 active release, audit와 stale-operation 진단을 위한 새 manifest/generation
값이 포함된다.

outbox publisher는 두 외부 R2 projection을 모두 담당한다. versioned published/rolled-back event를
관측하면 정확한 immutable Catalog manifest를 읽고 먼저
`gold/vector-tiles/manifests/{manifest_id}.json`에 create-only로 쓴다. retry에서 key를 발견하면
동일 byte/checksum인지 확인하고 아니면 fail closed한다. 그 뒤 active Catalog pointer를 다시
읽어 event manifest/generation이 아직 active일 때만 같은 schema-v2 byte를
`gold/vector-tiles/runtime-manifest.json`에 `Cache-Control: no-cache, max-age=0`으로 쓴다.

변경 가능한 쓰기도 R2 compare-and-swap으로 수행하며 확인 후 무조건 덮어쓰지 않는다.
publisher는 현재 pointer와 ETag를 읽고 `If-Match: <observed-etag>`를 보낸다. bootstrap은
`If-None-Match: *`를 사용한다. precondition 실패 시 Catalog authority와 R2를 모두 다시 읽는다.
현재 active manifest를 재시도하거나 stale event를 건너뛴다. publisher A가 확인한 뒤 publisher B가
더 새로운 pointer를 쓰고 A가 마지막에 쓰는 interleaving에서도 A의 stale ETag가 거부된다.
stale event는 immutable 객체를 안전하게 끝낼 수 있지만 mutable pointer를 뒤로 돌릴 수 없다.
publisher는 동결된 schema-v1 `gold/manifest.json`을 다시 쓰지 않는다.

운영 전 publisher와 Martin smoke test는 전용 derivative bucket configuration을 사용한다.
Lakehouse `FOUNDATION_PLATFORM_R2_LAKEHOUSE_*` adapter와 credential은 tile publication에서
금지한다. publisher credential은 bucket-scoped write credential이고 Martin credential은 별도의
bucket-scoped read-only credential이다.

## 기각한 대안

- 한 publication unit에서 static base와 dynamic overlay/tombstone를 조합하는 방식
- edit rate가 steady-state dynamic rendering을 정당화하지 않는 unit의 PostGIS-only runtime
- Naver internal vector/tile endpoint를 domain data source로 사용하는 방식
- PMTiles direct browser runtime이나 public R2 bucket을 production 기본값으로 사용하는 방식
- foundation-platform Catalog cutover 후 Gongzzang이 manifest를 소유하는 방식
- 모든 `.pbf` object마다 tile별 `file_asset` row를 만드는 방식
- Martin/PMTiles에 v1 flat-object field를 재사용하는 방식
- rollback 중 과거 manifest의 `previous_version`을 변경하는 방식

## 완료 기준

- legacy `gold/manifest.json`은 anchor consumer가 migration될 때까지 schema-v1 byte 호환을
  유지한다. schema v2는 Catalog runtime endpoint에서 제공하고
  `gold/vector-tiles/manifests/{manifest_id}.json`에 create-only projection하며
  `gold/vector-tiles/runtime-manifest.json`에 active no-cache pointer로 projection한다.
- strict v1/v2 dispatch가 알 수 없는 schema version을 거부하고 v1은 legacy 전용으로 남는다.
- 모든 v2 publication unit이 tagged source 하나, UUID data/release identity, JavaScript-safe
  generation, decimal-string Iceberg snapshot ID, 완전한 layer metadata, lineage를 가진다.
- 모든 static release가 immutable PMTiles `file_asset`에 연결되고 모든 dynamic release가
  준비·재구성 가능한 PostGIS projection revision에 연결된다.
- add/modify/delete, stale-build 거부, 같은 data의 serving rollback, source readiness를 기계적으로 test한다.
- Gongzzang에 manifest write path가 없고 manifest만 소비한다.
- Martin은 전용 private derivative bucket에 인증된 S3 호환 R2 접근을 사용하며
  canonical/lakehouse/recovery bucket이나 generic credential을 선택할 수 없다.
- contract가 Catalog SSOT model, implementation plan, Gongzzang ADR 0036에서 참조된다.
- live pointer write를 켜기 전에 전용 smoke command로 R2 publish/read와 decoded tile을 검증한다.
- R2 adapter와 outbox test가 두 publisher interleaving에서 fenced pointer compare-and-swap을
  증명하며 v2 pointer의 무조건 덮어쓰기를 금지한다.

## References

- [Root ADR 0006 - Object-storage-first serving](../../../../docs/adr/0006-object-storage-first-serving.md)
- [Martin file sources and remote-prefix reload](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md)
- [Apache Iceberg branching and WAP](https://iceberg.apache.org/docs/latest/branching/)
- [Cloudflare R2 S3 conditional `PutObject`](https://developers.cloudflare.com/r2/api/s3/api/)
