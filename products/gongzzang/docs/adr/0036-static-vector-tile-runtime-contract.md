# ADR 0036 - Foundation 벡터 타일 런타임 계약

| Field | Value |
|---|---|
| Date | 2026-05-12 |
| Last amended | 2026-07-24 |
| Status | Accepted |
| Owner | Foundation Platform |
| Consumer | Gongzzang |
| Upstream SSOT | [`Foundation ADR 0004`](../../../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md) |

## 결정

Foundation Platform은 public/reference vector-tile acquisition, canonical data, build, storage,
lineage, publication, rollback, active runtime manifest를 소유한다. Gongzzang은 발행된
contract를 검증하고 소비한다. Gongzzang에는 Foundation vector-tile ETL, R2 write, promotion,
rollback 경로가 없다.

브라우저는 schema-v2 runtime manifest를
`NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL/catalog/v1/vector-tiles/runtime-manifest`에서 읽는다.
기존 `NEXT_PUBLIC_TILES_MANIFEST_URL`은 schema-v1 의미를 유지하며 v2에 재사용하지 않는다.

제한된 첫 migration 단계에서 기존 schema-v1 manifest 위치는
`parcel_anchor_aggregate`와 `parcel_anchor`만을 위한 별도 입력으로 남는다. 다음 순서로
해석한다.
`NEXT_PUBLIC_TILES_MANIFEST_URL` when explicitly configured, otherwise through
`NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL/catalog/v1/vector-tiles/manifest`를 사용한다. v2
endpoint는 `parcels`를 제공한다. v2 `parcels`가 active가 되면 Gongzzang은 v1 `parcels`
artifact를 무시하므로 이 임시 dual-manifest read가 하나의 publication unit에 두 source를
만들지 않는다.

`/v1` segment는 HTTP 호환성 계약의 버전이다. Iceberg revision, manifest schema version,
R2 path component가 아니다.

각 Foundation 공개 단위와 serving generation마다 브라우저는 정확히 하나의 완전한
Martin vector source를 가진다.

```text
DynamicPostgis XOR StaticPmtiles
```

같은 unit에서 static base를 dynamic overlay·tombstone·client-side feature와 suppression으로
조합하지 않는다. Gongzzang 소유 listing marker와 `filter_hash`/delta path는 별도이며
변하지 않는다.

## 정확한 스키마 분기

`schema_version`은 정확한 discriminator다.

- `1`은 legacy flat-object DTO를 선택한다.
- `2`는 single-source publication DTO를 선택한다.
- 그 외 값은 거부한다.

소비자는 느슨한 `schema_version >= 1` parser를 사용하거나 v2 field를 조용히 v1 field로
해석하지 않는다.

## 레거시 v1

V1은 제한된 migration 동안 읽을 수 있으며 byte와 의미를 바꾸지 않는다.

- `current_version`와 `previous_version`는 UUID metadata다.
- `tiles_url_template`에는 `{object_key_prefix}`, `{z}`, `{x}`, `{y}`가 들어간다.
- `artifacts[layer].object_key_prefix`는 물리 flat-MVT object prefix다.
- `flat_tile_count`와 `flat_tile_total_bytes`는 실제 개별 tile object를 설명한다.
- `source_layer`, zoom, UUID lineage, 비어 있지 않은 tile 통계가 필수다.

V1 field를 PMTiles object나 Martin route에 재사용하지 않는다. 새 v1 production publication은
v2 producer/consumer cutover 후 중단한다. 첫 v2 단계에서는 고정된 v1 문서가 parcel artifact와
두 anchor artifact를 byte 단위로 계속 포함한다. v2 runtime은 그 문서에서 두 anchor source만
등록하고 parcel artifact는 등록하지 않는다.

## V2 계약

V2 has top-level:

```text
schema_version = 2
current_version                 # immutable manifest UUID / ETag identity
manifest_generation            # global JavaScript-safe poll token only
refresh_after_seconds
published_at
publication_units
```

Every publication unit has:

```text
data_revision                   # UUID for the exact logical feature set
serving_generation              # JavaScript-safe integer
active_release_id               # immutable release UUID
canonical_iceberg_snapshot_id   # positive base-10 decimal string, never JSON number
source                          # closed tagged union
layers                          # non-empty MVT layer metadata
lineage
```

실제 Iceberg snapshot ID는 JavaScript safe integer 범위를 넘을 수 있다. 따라서 consumer는
`canonical_iceberg_snapshot_id`를 양의 10진 string으로만 받는다.
`manifest_generation`과 모든 `serving_generation`은 다음 범위여야 한다.
`1..=9007199254740991`.
`refresh_after_seconds` must equal `4`.

`source` union은 정확히 두 variant를 가진다.

- `dynamic_postgis`: stable explicit `martin_source_id`, generation-addressed
  `tiles_url_template`, `postgis_projection_revision`, and `cache_policy`.
- `static_pmtiles`: immutable release-addressed `martin_source_id` and `tiles_url_template`,
  plus PMTiles object key, file-asset UUID, SHA-256, and byte size.

각 단위 URL은 `{z}`, `{x}`, `{y}`를 포함한 완전한 Martin template다. Gongzzang은 v2에서
extension을 붙이거나 object-key 치환을 하지 않는다. 각 layer는 stable `source_layer`,
tile/render zoom, lowercase `feature_id_property`를 선언한다. 현재 parcel identity는
`pnu`이며 proof 전용 대문자 `PNU`는 두 번째 production identity가 아니다.

Production v2 tile URL은 absolute HTTPS여야 한다. parser는 checked-in Docker proof에 한해
host가 loopback literal 또는 `localhost`일 때만 absolute HTTP를 허용한다. Foundation
production publish gate는 loopback을 포함한 모든 HTTP URL을 거부한다.

첫 v2 단위는 `parcels` 하나다. 현재 marker runtime은 두 anchor 계층을 계속 필요로 하며,
이 단계에서는 독립적인 legacy v1 source로 남는다. parcel source 전환이 이 source를 대체하지
않는다. `complex`, `parcel_anchor_aggregate`, `parcel_anchor`, `admin`, `buildings`는 각자
producer/consumer parity를 증명한 뒤에만 v2로 migration한다.

## 활성 지도 갱신

map이 mount되어 보이는 동안 Gongzzang은 다음을 수행한다.

1. 보이는 동안 4초마다 겹치지 않는 `ETag`/`If-None-Match` 요청 하나로 Catalog manifest를
   poll한다.
2. 변경을 적용하기 전에 전체 응답을 parse한다.
3. `manifest_generation`은 일부 unit이 바뀌었을 수 있다는 신호로만 사용한다.
4. unit별 `serving_generation`을 diff한다.
5. 바뀐 Mapbox vector source만 교체하면서 style-layer 순서, interaction handler, zoom,
   feature identity를 보존한다.
6. 새 manifest나 source가 invalid/unready면 현재 등록된 source descriptor를 유지한다.

한 unit의 이전 source와 새 source를 함께 등록한 상태로 두지 않는다. 이미 열린 map은
Foundation freshness SLO 안에 선택한 generation을 로드해야 한다.

이 retention 보장은 immutable static URL에 대해 정확하다. dynamic generation 값은 하나의
안정적인 Martin/PostGIS source 주변 cache만 무효화하며 historical snapshot selector가 아니다.
projection이 진행되는 동안 client가 잘못된 manifest를 거부하면 유지한 dynamic URL이 최신
commit된 완전한 geometry를 반환할 수 있다. Gongzzang은 이를 rollback으로 표시하지 않는다.

따라서 표시 중인 mount 지도 하나는 초당 최대 `0.25` manifest 요청을 만든다. client는
동시 요청 무리를 피하도록 초기 phase를 무작위화하고, hide/unmount 시 중단하며, 오류 뒤에는
제한된 exponential backoff를 적용한다. endpoint는 Foundation traffic/auth registry와
Gongzzang outbound allow policy에 명시된 anonymous public contract이며 browser JavaScript에
service credential을 넣지 않는다.

## 런타임 규칙

Gongzzang은 다음을 해야 한다.

- 모든 UUID, decimal snapshot string, generation range, tagged-union field, zoom, 필수 layer set을
  검증한다.
- 두 legacy anchor URL은 v1 규칙으로만 만들고, v2 `parcels`가 active면 v1 `parcels`를 무시하며
  v2 source URL을 직접 사용한다.
- 제한된 migration 동안 v2 `parcels`와 v1 `parcel_anchor_aggregate`·`parcel_anchor`를
  현재 map의 core로 취급한다.
- 진단과 support evidence에 manifest lineage를 사용한다.
- MVT를 canonical data로 취급하지 말고 owner API로 선택 object detail을 가져온다.

Gongzzang은 다음을 해서는 안 된다.

- Foundation artifact를 write·promote·rollback한다.
- 누락된 unit·source·layer·lineage·identity를 합성한다.
- R2 object listing에서 active state를 도출하거나 object key에서 business meaning을 parse한다.
- `manifest_generation`을 source selector로 사용한다.
- 한 unit의 static/dynamic Foundation feature를 조합한다.
- listing price·status·exposure·`filter_hash`·marker-delta semantics를 이 manifest로 옮긴다.

## 기각한 대안

- Gongzzang 소유 vector-tile ETL 또는 R2 publication.
- Naver internal tile을 canonical data로 사용하는 것.
- 허용적인 schema parsing이나 v1 flat-object field의 의미 재사용.
- direct browser PMTiles/custom transport를 production 기본값으로 사용하는 것.
- Foundation unit에 static base와 dynamic overlay/tombstone filtering을 함께 적용하는 것.
- parcel polygon unit만 바뀌었을 때 Foundation parcel-anchor unit의 target을 바꾸는 것.

## 검증

- `apps/web/tests/unit/map/vector-tile-manifest.test.ts`
- `apps/web/tests/unit/map/listing-map-runtime.test.ts`
- `apps/web/tests/unit/foundation-platform-event-contract.test.ts`
- `docs/architecture/foundation-platform-boundary.v1.json`
- `cargo xtask verify gongzzang`

상위 field-level 계약과 공개 규칙은
[Foundation ADR 0004](../../../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md).
