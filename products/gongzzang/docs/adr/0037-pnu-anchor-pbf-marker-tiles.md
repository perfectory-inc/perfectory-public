# ADR 0037 - PNU 앵커 PBF 마커 타일

| Field | Value |
|---|---|
| Date | 2026-05-22 |
| Status | Accepted |
| Last amended | 2026-07-24 |
| Preceded by | [ADR 0017](./0017-listing-marker-render-canvas-bitmap-stamp.md), [ADR 0018](./0018-pnu-first-identity-no-coordinates.md), [ADR 0036](./0036-static-vector-tile-runtime-contract.md) |
| Inherits/refines | `foundation-platform` [ADR 0008 - PNU Anchor PBF Marker Tile Contract](../../../../platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md) |

## 배경

Gongzzang map runtime은 원래 빠른 MVP 형태에서 시작했다.

- browser viewport 변경이 `bounds`로 listing API를 호출한다.
- backend가 spatial envelope로 listing을 조회한다.
- frontend가 listing별 map marker를 만든다.

이 형태는 작은 demo에서는 동작할 수 있지만 SSS급 산업용 부동산 플랫폼의 출시 구조는
아니다. viewport 크기를 backend 부하 제어의 일부로 만들고 product row 안에 좌표 소유권을
넣게 하며 marker limit이 생기면 record를 숨길 수 있다.

제품 방향은 다음과 같이 확정했다.

- parcel identity는 PNU-first다.
- parcel polygon은 PBF vector tile로 제공한다.
- 필지에 붙은 marker도 PBF vector tile을 사용한다.
- marker location은 임의 listing 좌표가 아니라 foundation-platform PNU anchor에서 해석한다.

## 결정

Gongzzang launch map marker surfaces must use **PNU-anchor backed PBF marker tiles**.

Contract constants inherited from foundation-platform ADR 0008:

```text
marker_tile_response_format = MVT_PBF
marker_position_source = PNU_ANCHOR
bbox_marker_runtime_forbidden = true
dropped_marker_success_forbidden = true
```

frontend는 큰 label·compact badge·작은 dot·aggregate symbol을 렌더링할 수 있다. 이는
presentation일 뿐이다. data contract는 다음 형태 중 하나로 모든 대상 record를 보존해야 한다.

- one PBF point feature per record;
- truthful aggregation with `count` and a drill-down reference;
- zoom-dependent simplification that still represents the complete underlying set.

Gongzzang은 “label을 놓을 시각 공간이 없다”는 이유로 marker를 누락할 수 없다.

## 런타임 모델

출시 runtime은 세 관심사를 분리한다.

| Concern | Owner | Format |
|---|---|---|
 | Parcel polygon geometry | foundation-platform Catalog | ADR 0036/0004 manifest로 선택한 완전한 Martin MVT source |
 | Marker anchor position | foundation-platform Catalog | PNU anchor registry |
 | Gongzzang listing marker tiles | Gongzzang market domain | listing row를 PNU로 foundation-platform anchor와 join해 만든 dynamic PBF |
 | Public/reference marker tiles | foundation-platform Catalog | 실거래가·공시지가·parcel-anchor 등 product가 아닌 reference layer |

지도는 PNU로 시각적으로 결합한다.

1. 선택한 Foundation parcel MVT source가 선택 가능한 parcel shape과 `pnu` property를 제공한다.
2. Foundation Platform이 PNU anchor 좌표를 소유한다.
3. Gongzzang listing marker PBF는 `listing.parcel_pnu`와 anchor를 사용하며 좌표를 저장하거나
   재해석하지 않는다.
4. listing marker를 선택하면 `id`, `pnu`, `detail_ref`로 Gongzzang detail을 연다.
5. parcel을 선택하면 PNU로 product data를 조회할 수 있다.

marker tile PBF는 필지 geometry의 두 번째 원천이 되지 않는다. anchor registry에서 이미
확인한 marker point만 담는다.

Foundation Platform은 Gongzzang listing price·status·exposure rule·search filter·detail payload를
소유하지 않는다. anchor lookup/tile primitive과 public/reference spatial layer는 노출할 수
있다. Gongzzang은 listing semantics와 이를 나타내는 listing marker tile의 SSOT로 남는다.

## API 형태

Recommended marker tile path:

```text
GET /map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}
```

Stable initial `layer` candidates:

| Layer | Meaning | Freshness |
|---|---|---|
| `listing` | active Gongzzang listings, served by Gongzzang | dynamic |
| `real_transaction_price` | real transaction points, served by foundation-platform | semi-static batch |
| `auction` | court auction points, owner decided by source/domain ADR | semi-static batch |
| `official_land_price` | official land price indicators, served by foundation-platform | static or semi-static batch |

`filter_hash` is the identity of a validated server-side filter contract. It is not a raw SQL
fragment and not a free-form JSON expression.

Minimum feature properties:

| Property | Meaning |
|---|---|
| `id` | Listing id, transaction aggregate id, auction id, or aggregate id |
| `pnu` | Parcel identity |
| `kind` | Marker kind for style selection |
| `count` | Number of represented records |
| `rank` | Optional deterministic display priority |
| `detail_ref` | Opaque detail lookup reference |

상세 API는 사용자가 feature를 선택한 뒤 가져오므로 JSON으로 남겨도 된다. 지도 전체 marker
표면은 PBF다.

## 현재 코드 상태

레거시 viewport-bounds list-query 코드는 Gongzzang 출시 지도/매물 경로에서 폐기했다.
Frontend map marker 배치는 listing latitude/longitude나 listing별 Naver Marker 객체를 더 이상
사용할 수 없으며 foundation-platform PNU-anchor PBF marker 계약을 사용해야 한다.

현재 과도기 영역:

- `apps/web/components/listings/listing-map.tsx`는 foundation-platform `parcel_anchor` marker
  PBF source/layer와 Gongzzang 소유 `listing` marker PBF source/layer를 등록한다. CI가
  legacy listing별 Naver marker 배치와 viewport `bounds` request wiring을 거부한다.
- `crates/listing-domain/src/repository.rs`와 `crates/gongzzang-persistence/src/listing.rs`는
  `find_markers_in_bbox`와 `ListingMarker` lightweight marker projection을 더 이상 노출하지
  않는다. PNU anchor가 없으면 active listing 저장을 거부한다. `find_listing_marker_tile`은
  오래된 projection gap을 막기 위한 completeness check를 유지한다.
- `services/gongzzang-api/src/routes/listings.rs`는 public `bounds` query input을 더 이상 받지 않는다.
- `services/gongzzang-api/src/routes/listing_marker_tiles.rs`가 Gongzzang listing PBF endpoint를
  노출한다.
  `GET /map/v1/marker-tiles/listing/{z}/{x}/{y}.pbf?filter_hash=all-active-v1`.
- `crates/gongzzang-persistence/src/listing.rs`는 bbox 이름의 card query가 아니라
  `find_card_summaries`를 노출한다.
- `Listing`은 product coordinate를 저장하지 않으며 baseline migration도 과거 coordinate
  column이나 index를 만들지 않는다.

Gongzzang 출시 지도/매물 경로는 viewport bounds를 공개 요청 형식으로 사용하지 않는다.
product-specific listing marker PBF tile은 Gongzzang market-domain runtime surface이며
foundation-platform service가 아니다.

## 좌표 소유권

필지에 붙은 객체의 마커 좌표는 Gongzzang이 소유하지 않는다.

허용:

- listing·market-domain record에 PNU를 저장한다.
- 해당 PNU의 foundation-platform anchor 위치에 marker를 렌더링한다.
- non-canonical diagnostic 좌표는 diagnostic 또는 source raw data라고 명확히 표시할 때만 저장한다.
- marker click 후 선택 object detail에는 JSON을 사용한다.

launch marker 배치에서 금지:

- `listing.geom_point`, listing `latitude`, `longitude`, user-picked coordinate를 필지 연결
  listing의 canonical marker 위치로 사용한다.
- `bbox`, `bounds`, raw coordinate envelope로 public map marker request를 받는다.
- 대상 record를 조용히 버리고 성공 marker response를 반환한다.
- PBF tile을 anchor의 source of truth로 취급하고 foundation-platform anchor registry의
  projection으로 취급하지 않는다.

Gongzzang이 나중에 임의의 사용자 그리기 위치를 정말 필요로 하면 별도 객체 유형과 ADR로
다룬다. 필지 연결 PNU marker 의미를 약화하지 않는다.

## 렌더링 정책

renderer 구현은 Canvas, WebGL, Mapbox GL/Naver GL vector layer, bitmap stamp 또는 다른 효율적인
renderer를 선택할 수 있다. renderer 선택은 data contract를 바꾸지 않는다.

표시 단순화 순서:

1. 공간과 zoom이 허용하면 rich label marker
2. compact badge marker
3. dot marker
4. truthful aggregate marker

어떤 단계도 표현 대상 데이터에서 원본 기록을 누락해서는 안 된다.

## 전환 순서

1. 기존 bbox path는 과도기 local 동작으로만 유지한다.
2. PBF marker tile contract와 fixture test를 정의한다.
3. 실거래가나 auction 같은 read-only layer로 첫 marker tile layer를 구현한다.
4. frontend PBF source/layer loading과 canvas/vector rendering probe를 추가한다. **foundation-platform
   `parcel_anchor`와 Gongzzang `listing` marker layer에 완료.**
5. listing marker를 bbox JSON에서 PBF marker tile로 옮긴다. **현재 all-active launch filter의
   Gongzzang 소유 dynamic MVT/PBF tile에 완료.**
6. `bounds`/`bbox` request shape를 사용하는 새 launch marker code를 거부하는 CI guard를
   추가한다. **frontend marker 배치, list-query `bounds` wiring, legacy marker/card repository
   path에 완료.**
7. legacy listing coordinate·bbox marker path를 deprecated한다. **`find_markers_in_bbox`와
   public `/listings`의 `bounds` 거부에 완료.**
8. PBF marker tile이 desktop/mobile smoke check를 통과한 뒤 legacy path를 제거한다.

Gongzzang-local `parcel_marker_anchor` projection migration은 사용자가
2026-05-22. Future schema changes still require explicit migration approval before generation.

## 영향

긍정적 효과:

- backend load가 viewport area가 아니라 tile identity로 제한된다.
- marker location owner와 lineage path가 하나다.
- PBF polygon tile과 marker tile이 PNU를 join key로 공유한다.
- frontend가 data omission 없이 dense data를 dot으로 렌더링할 수 있다.
- contract를 Dawneer와 향후 service가 재사용할 수 있다.

비용:

- foundation-platform이 anchor registry와 public/reference marker tile contract를 제공해야 한다.
- listing marker가 현재 parcel-anchor layer를 넘어가면 Gongzzang이 product-specific listing
  marker tile을 제공해야 한다.
- Gongzzang frontend marker code를 `bounds` JSON에서 PBF tile 소비로 다시 작성했다.
- 새 runtime 검증 후 legacy listing coordinate/bbox code를 retire해야 한다.
- filter hashing과 truthful aggregation에 contract test가 필요하다.

## 재검토 조건

- product marker가 필지에 붙지 않아 임의 좌표가 정말 필요하다.
- PBF marker layer가 truthful aggregation 없이는 dense tile을 나타낼 수 없다.
- foundation-platform anchor generation의 algorithm이나 geometry source가 바뀐다.
- Naver GL integration이 신뢰할 수 있는 vector point layer rendering을 막아 renderer fallback이
  필요하다.

## 참고 문서

- [ADR 0018 - PNU-first identity](./0018-pnu-first-identity-no-coordinates.md)
- [ADR 0021 - Historical static vector tile decomposition](./0021-static-vector-tile-decomposition.md)
- [ADR 0036 - Static vector tile runtime contract](./0036-static-vector-tile-runtime-contract.md)
- [foundation-platform ADR 0008 - PNU Anchor PBF Marker Tile Contract](../../../../platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md)
