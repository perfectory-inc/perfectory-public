# ADR 0008 - PNU 앵커 PBF 마커 타일 계약

| Field | Value |
|---|---|
| Date | 2026-05-22 |
| Status | Accepted |
| Last amended | 2026-07-24 |
| Related | [`gongzzang ADR 0037`](../../../../products/gongzzang/docs/adr/0037-pnu-anchor-pbf-marker-tiles.md) |
| Scope | `foundation-platform` Catalog, parcel marker anchors, map marker tile serving, `gongzzang` map runtime |

## 배경

Gongzzang map runtime은 listing, 실거래가, 공시지가, auction, 향후 필지 indicator처럼
필지에 붙은 많은 map feature를 렌더링해야 한다. 이 runtime은 임의의 bounding-box JSON
endpoint에 의존하면 안 된다. 그 경로가 같은 실패를 반복해서 만들기 때문이다.

- 넓은 viewport가 한 번에 너무 많은 data를 요청할 수 있다.
- 안전한 tile 경계 전에 `ORDER BY`, deduplication, count query가 실행될 수 있다.
- 요청별 limit 때문에 marker record가 조용히 잘릴 수 있다.
- 좌표를 product별로 저장하면 marker 좌표가 필지 identity에서 벗어날 수 있다.
- frontend가 object별 API shape를 너무 많이 조정해야 한다.

플랫폼 아키텍처는 이미 다음 두 규칙을 결정했다.

- Foundation parcel·administrative geometry는 ADR 0004의 Foundation 소유 single-source
  manifest contract를 통해 표준 MVT로 제공한다.
- 필지에 붙은 business identity는 Gongzzang ADR 0018에서 PNU-first다.

이 ADR은 해당 규칙을 마커 위치와 마커 타일 응답까지 확장한다.

이 ADR은 Gongzzang listing 소유권을 foundation-platform으로 옮기지 않는다. Listing은 Gongzzang
시장 도메인 제품 데이터다. foundation-platform은 필지 anchor와 public/reference 공간 계층을
소유하고 제품 서비스는 제품 의미를 소유하며 같은 PNU-anchor 계약으로 제품 marker PBF를
제공할 수 있다.

## 결정

필지에 연결된 객체의 출시 지도 marker 트래픽은 모두 **PNU anchor 기반 PBF vector tile**을
사용해야 한다.

Contract constants:

```text
marker_tile_response_format = MVT_PBF
marker_position_source = PNU_ANCHOR
bbox_marker_runtime_forbidden = true
dropped_marker_success_forbidden = true
launch_runtime_source = FOUNDATION_VECTOR_TILE_MANIFEST
legacy_anchor_manifest_endpoint = /catalog/v1/vector-tiles/manifest
parcel_v2_runtime_manifest_endpoint = /catalog/v1/vector-tiles/runtime-manifest
db_reference_endpoint_launch_forbidden = true
db_reference_endpoint_scope = diagnostics_bounded_proof_admin
aggregate_anchor_max_zoom = 11
exact_anchor_min_zoom = 12
```

PBF는 serving projection이며 위치의 정본이 아니다.

foundation-platform 소유 static/reference marker 계층의 출시 hot path는 활성 vector tile
manifest와 각 marker publication unit에 선택된 완전한 Martin source다. 첫 schema-v2 parcel
migration 동안 두 anchor layer는 고정된 schema-v1 manifest에 남고
`/catalog/v1/vector-tiles/runtime-manifest`는 v2 parcel unit만 제공한다. DB 기반
`/map/v1/marker-tiles/...` endpoint는 진단·제한된 지역 증명·admin 검증을 위한 reference
경로이며 전국 traffic의 production launch runtime으로 사용하지 않는다.

낮은 zoom에서 개별 PNU anchor를 모두 반복하지 않는다. static/reference parcel anchor는
z11까지 aggregate artifact를 사용하고 z12부터 정확한 PNU anchor artifact를 사용한다.

marker 위치의 정본은 필지 geometry에서 도출하고 PNU로 식별하는 foundation-platform Catalog
anchor다. Gongzzang 같은 product는 marker 의미·스타일·열리는 detail panel을 결정할 수
있지만 canonical parcel marker 위치를 소유해서는 안 된다.

## 앵커 레지스트리

foundation-platform Catalog은 논리적 `parcel_marker_anchor` registry를 소유한다.

Minimum anchor fields:

| Field | Meaning |
|---|---|
| `pnu` | 필지 identity이며 기본 lookup key다. |
| `anchor_lng` | EPSG:4326 경도. |
| `anchor_lat` | EPSG:4326 위도. |
| `algorithm` | Anchor algorithm 이름. source가 제공하면 `official_label_point`를 우선하고 아니면 `polylabel`을 사용한다. |
| `algorithm_version` | 재현 가능한 anchor 생성을 위한 안정적인 version string. |
| `source_geometry_version` | anchor를 만든 parcel geometry build/version. |
| `source_geometry_checksum_sha256` | source geometry input의 checksum 또는 build checksum. |
| `computed_at_utc` | anchor 계산 시각(UTC). |

구현 중 저장소 이름은 바뀔 수 있지만 의미는 유지해야 한다.

source geometry가 허용하면 anchor는 필지 polygon 안에 있어야 한다. 필지 geometry가 잘못되거나
없으면 foundation-platform은 좌표를 만들어내지 말고 명시적인 lineage/error 상태를 내야 한다.

## 마커 타일 계약

마커 타일은 임의의 viewport bounds가 아니라 타일 좌표와 필터 식별자로 주소를 정한다.

Recommended public read shape:

```text
GET /map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}
```

`layer`는 `parcel_anchor`, `real_transaction_price`, `official_land_price`, `auction` 또는
Gongzzang `listing` 같은 안정적인 marker layer 이름이다. `filter_hash`는 검증된 filter
contract의 identity이며 자유 형식 SQL expression이 아니다.

foundation-platform은 `parcel_anchor`와 public/reference data layer처럼 소유한 layer를
제공한다. 이후 ADR이 listing business semantics를 저장하거나 해석하지 않는 중립 projection
경계를 명시적으로 만들지 않는 한 Gongzzang listing marker tile은 Gongzzang이 제공한다.

PBF tile은 geometry가 확인된 PNU anchor인 point feature를 담는다. 각 feature는 map에
필요한 최소 rendering·lookup property만 포함한다.

| Property | Meaning |
|---|---|
| `id` | Product 소유 object id 또는 aggregate id. |
| `pnu` | anchor를 해석하는 데 사용하는 필지 identity. |
| `kind` | style 선택을 위한 안정적인 marker kind. |
| `count` | feature가 여러 object를 나타낼 때의 aggregate count. |
| `rank` | label 충돌을 위한 선택적 deterministic 표시 순위. |
| `detail_ref` | detail API fetch를 위한 opaque lookup reference. |

큰 label과 풍부한 marker card는 presentation이며 data completeness가 아니다. 화면 공간이
부족하면 renderer는 label을 작은 점이나 aggregate symbol로 낮춰야 한다. 시각 공간이
부족하다는 이유만으로 성공 tile 응답에서 대상 record를 조용히 버려서는 안 된다.

## 완전성 규칙

타일 응답은 집계할 수 있지만 사실을 왜곡해서는 안 된다.

Allowed:

- 대상 record마다 point feature를 둔다.
- `count`와 `detail_ref`로 drill-down을 보존하는 deterministic aggregation을 사용한다.
- 단순화된 feature가 원래 전체 set을 나타낼 때만 zoom별 단순화를 허용한다.
- `id`, `pnu`, `detail_ref`로 별도 detail fetch를 제공한다.

Forbidden:

- `LIMIT N` as a success-path data cap for a tile without an explicit "truncated" failure state;
- 낮은 순위 marker를 버리고 tile이 완전한 것처럼 HTTP 200을 반환한다.
- 필지에 붙은 object의 marker 좌표를 product 소유 `latitude`/`longitude` column에서 도출한다.
- `bbox`, `bounds`, `south/west/north/east`, raw coordinate envelope에 기반한 public launch
  map marker request를 사용한다.

설정된 예산 안에서 tile을 표현할 수 없으면 서비스는 구조화된 budget error를 반환하거나
원본 record를 정직하게 나타내는 aggregate를 반환해야 한다.

## 정적 필지 타일과의 관계

필지 폴리곤과 마커 점은 하나의 위치 모델을 공유하는 별도 공개 단위다.

- Parcel polygon MVT: ADR 0004의 complete PostGIS 또는 immutable PMTiles 기반 Martin source.
- Marker point PBF: owner service가 business data를 foundation-platform PNU anchor와 join해
  만든 dynamic 또는 semi-static marker layer.

두 layer 모두 PNU를 join key로 사용한다. marker point PBF가 parcel polygon을 별도 위치
source로 복제해서는 안 된다.

## JSON 사용

JSON 마커 endpoint는 관리자 진단·계약 테스트·상세 조회에서만 허용한다. 출시 지도 마커 렌더링 경로는
아니다.

출시 지도는 사용자가 feature를 선택한 뒤 상세를 JSON으로 가져올 수 있지만 지도 전체 marker
표면은 PBF/MVT다.

## 영향

긍정적 효과:

- viewport 크기가 backend result size를 직접 제어하지 않는다.
- marker 위치 owner가 하나이며 parcel geometry lineage에서 재현할 수 있다.
- Gongzzang·Dawneer·향후 product가 같은 anchor semantics를 공유할 수 있다.
- data loss 없이 map rendering을 label에서 dot으로 낮출 수 있다.
- API contract가 CDN/cache 친화적인 tile addressing과 맞는다.

비용:

- foundation-platform이 anchor 생성·lineage pipeline을 소유해야 한다.
- product marker data는 tile encoding 전에 PNU로 join해야 하며, foundation-platform이 listing
  price·status·exposure·search filter·detail payload 같은 product semantics를 소유하지 않는다.
- production에 노출하기 전에 filter hashing과 tile budget error에 엄격한 contract가 필요하다.
- Gongzzang의 현재 bbox JSON map path는 과도기 경로가 되며 launch architecture로 취급하지
  않는다.

## 구현 순서

1. anchor registry schema와 anchor generation algorithm contract를 정의한다.
2. checksum과 version lineage를 포함해 parcel geometry에서 anchor를 생성한다.
3. marker tile response schema와 filter hash contract를 정의한다.
4. 먼저 위험이 낮은 marker layer 하나를 추가한다. 읽기 전용 실거래가나 공시지가 point가
   적합하다.
5. Gongzzang listing marker를 bbox JSON에서 Gongzzang 소유 PBF marker tile로 옮긴다. 이
   tile은 PNU로 foundation-platform anchor를 소비한다.
6. bbox/bounds를 사용하는 새 launch map marker path를 거부하는 CI guard를 추가한다.
7. PBF runtime 검증 후 legacy listing coordinate marker path를 deprecated하고 제거한다.

앵커 레지스트리 DB migration은 생성 전에 명시적인 migration 승인이 필요하다.

## 재검토 조건

- parcel geometry source가 바뀌어 새 anchor algorithm version이 필요해진다.
- product가 필지에 붙지 않은 자유 좌표를 first-class business object로 필요로 한다.
- marker layer가 정직한 aggregation 없이는 tile budget을 맞출 수 없다.
- 다른 service가 foundation-platform 밖에서 parcel marker 좌표를 소유하려 한다.

## 참고 문서

- [ADR 0004 - Static Vector Tile Runtime Contract](./0004-static-vector-tile-runtime-contract.md)
- [ADR 0006 - Lakehouse Table Format and Serving Architecture](./0006-lakehouse-table-format-and-serving-architecture.md)
- [Gongzzang ADR 0018 - PNU-first identity](../../../../products/gongzzang/docs/adr/0018-pnu-first-identity-no-coordinates.md)
- [Gongzzang ADR 0036 - Static vector tile runtime contract](../../../../products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md)
