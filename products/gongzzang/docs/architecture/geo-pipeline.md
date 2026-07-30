---
status: current
owner: gongzzang-제품
doc_type: architecture
last_reviewed: 2026-07-29
---

# 공간 데이터 파이프라인

이 문서는 현재 Gongzzang의 공간 데이터 책임을 설명한다.

## 1. 소유권 분리

Foundation Platform 소유:

- parcel geometry
- building/reference spatial layers
- PNU marker anchors
- public/reference vector tile lifecycle
- Catalog raw lineage

Gongzzang 소유:

- listing semantics
- listing visibility/filtering
- listing marker projection/indexes
- listing-owned marker tile/count/mask/delta/tombstone serving

## 2. Current Marker Pipeline

```text
Foundation Platform PNU anchor snapshot/event
  -> Gongzzang Foundation PNU anchor projection
  -> listing marker projection
  -> marker serving index
  -> /map/v1/marker-* routes
  -> frontend map vector source
```

주요 파일:

- `migrations/20260719000115_parcel_marker_anchor_projection.sql`
- `migrations/20260719000116_listing_marker_projection.sql`
- `migrations/20260719000117_listing_marker_filter_registry.sql`
- `migrations/20260719000119_listing_marker_overlay_and_dirty_queue.sql`
- `crates/gongzzang-persistence/src/foundation_anchor.rs`
- `crates/gongzzang-persistence/src/listing/marker_projection.rs`
- `crates/gongzzang-persistence/src/listing/marker_tile.rs`
- `services/gongzzang-api/src/listing_marker_serving`
- `apps/web/lib/map/marker-tile-contract.ts`

## 3. Public Marker Contract

공개 마커 경로는 타일 좌표와 안정적인 필터 식별자를 사용한다.

다음 입력은 사용하지 않는다.

- `bbox`
- `bounds`
- `south`
- `west`
- `north`
- `east`
- listing-owned canonical latitude/longitude columns

구조적인 이유는 지도 이동이 캐시 가능한 타일 형태 산출물을 읽어야 하고 마커 위치가 Foundation
Platform PNU 앵커에 계속 묶여 있어야 하기 때문이다.

## 4. Listing Coordinates

매물 행이 마커 좌표의 정본 소유자가 되어서는 안 된다.

허용:

- PNU identity on listing/domain records
- derived marker projection based on Foundation Platform anchor data
- overlay/delta/tombstone indexes for serving freshness

금지:

- `listing.latitude`
- `listing.longitude`
- product-owned `geom_point` as canonical marker source

## 5. Internal Spatial Queries

내부 시장 도메인 reader port는 `shared_kernel::spatial_scope::SpatialScope`를 사용한다.

지원하는 scope 형태:

- `PNU`
- `Sido`
- `Sigungu`
- `Eupmyeondong`
- validated slippy-map tile coordinates

목표는 public `bbox`/`bounds` marker 요청 형태를 다시 만들지 않고 제품 쪽 query 의도를 명시하는 것이다.
`BoundingBox`는 낮은 수준 geometry value object로 남을 수 있지만, 향후 ADR이 다른 계약을 승인하지
않는 한 시장 reader 계약은 `SpatialScope`를 우선한다.

## 6. Static Reference Tiles

Gongzzang은 Foundation Platform 추출 이후의 정적 vector tile ETL을 소유하거나 포함하지 않는다.
원천 수집·build·승격·rollback·R2 layout은 Foundation Platform 책임이다.

## 7. Guardrails

PNU-anchor PBF marker 계약과 Foundation Platform 의존성 경계를 유지한다. Foundation Platform catalog
경계는
`scripts/lefthook/foundation-ownership-boundary.sh` and the boundary contract
`docs/architecture/foundation-platform-boundary.v1.json`.
