# ADR 0038 - 매물 마커 제공 인덱스·필터 마스크

| Field | Value |
|---|---|
| Date | 2026-05-26 |
| Status | Accepted |
| Preceded by | [ADR 0017](./0017-listing-marker-render-canvas-bitmap-stamp.md), [ADR 0018](./0018-pnu-first-identity-no-coordinates.md), [ADR 0037](./0037-pnu-anchor-pbf-marker-tiles.md) |
| Refines | [ADR 0037](./0037-pnu-anchor-pbf-marker-tiles.md) |

## 배경

ADR 0037이 launch marker contract를 고정했다. 필지 연결 listing marker는 tile coordinate로
주소를 정하고 Gongzzang 소유 marker PBF로 렌더링하며 foundation-platform PNU anchor를 통해서만
위치를 정한다.

다음 결정은 제품이 design lab과 유사한 고급 산업용 부동산 filter를 노출할 때 고카디널리티
listing filter를 처리하는 방법이다.

- asset type: factory, warehouse, land;
- deal type: sale, jeonse, monthly rent;
- numeric ranges: price, area, floor height, floor load, power, water, waste-water capacity;
- boolean and enum flags: crane, dock, drive-in, clean room, usage area, land category, court;
- namespace semantics: factory OR warehouse OR land OR auction.

모든 filter 조합을 별도 tile set으로 만드는 것은 실행할 수 없다. map pan·zoom·slider 변경마다
listing OLTP table을 직접 조회하는 것도 실행할 수 없다. cache는 도움이 되지만 price·area
범위는 가능한 값이 매우 많으므로 numeric input filter의 primary 전략이 될 수 없다.

## 결정

Gongzzang listing marker serving uses a dedicated read model and filter index. The launch table
names are `listing_marker_projection` and `listing_marker_filter_registry`:

```text
listing OLTP rows
-> listing marker projection
-> marker/filter index
-> base marker tile and optional filter mask APIs
-> browser instant filter and Canvas/GL renderer
```

map runtime은 hybrid다.

1. static polygon/reference layer는 분리해 foundation-platform contract로 제공한다.
2. Gongzzang listing marker는 listing OLTP row가 아니라 Gongzzang 소유 marker projection/index에서
   제공한다.
3. 보이는 tile의 첫 marker payload는 browser 즉시 filtering에 필요한 안전한 최소 field를 담은
   base marker tile이다.
4. 단순 visible-tile filter는 browser에서 즉시 적용한다(guardrail label: browser instant filtering).
5. 정확한 전국 count, 보이지 않는 tile, permission-sensitive result, 복잡한 filter는 server-side
   index가 처리한다.
6. base marker tile이 이미 있을 때 filter가 바뀌면 전체 tile을 다시 보내는 대신 작은 filter mask를
   반환할 수 있다.
7. cache는 보조 accelerator일 뿐 correctness나 scalability mechanism이 아니다.

## 소유권

| Fact | Owner |
|---|---|
| Listing business data, price, area, status, exposure | Gongzzang |
| Listing location identity | Gongzzang, as PNU only |
| PNU anchor coordinate and lineage | foundation-platform |
| Listing marker projection and filter index | Gongzzang |
| Listing marker tile and filter mask APIs | Gongzzang |
| Parcel, building, industrial complex, and public/reference spatial layers | foundation-platform unless a later ADR says otherwise |

Listing rows must still not own canonical marker latitude/longitude. Marker serving payloads may
include anchor coordinates, but those values are copies derived from foundation-platform anchor snapshots
and must carry anchor lineage/version.

## 기각한 대안

### A. Precompute every filter combination as marker tiles

기각한다.

가격·면적 같은 numeric range filter에서는 조합 수가 폭발하고 product filter UX가 tile
artifact lifecycle에 종속된다. listing 하나가 바뀔 때 무효화도 과도하게 발생한다.

### B. Query listing OLTP tables directly for every map request

기각한다.

write model을 고처리량 지도 제공 model로 사용해서는 안 된다. listing table에 lock/query
pressure가 생기고 map gesture traffic spike가 listing write·review·back-office 작업과
경쟁한다.

### C. Make all filters apply only after a modal "Apply" action

유일한 상호작용 모델로는 기각한다.

고급 modal filter는 draft/apply flow를 사용할 수 있지만 asset type·deal type·price·area 같은
빠른 map filter는 현재 viewport에서 즉시 반응해야 한다. system은 server 작업을 coalesce하고
cancel하지만 client visual state는 즉시 바뀐다.

### D. Put all marker domains into one combined marker tile

기각한다.

listing·auction·실거래가·parcel anchor·공시지가·industrial complex marker는 ownership,
freshness, permission, filter, invalidation이 다르다. 한 map에 시각적으로 조합하더라도
별도 layer여야 한다.

## 런타임 정책

### 계층 분리

지도에 여러 계층을 동시에 표시해도 제공은 계층별로 유지한다.

```text
parcel polygon layer          -> foundation-platform static vector tile
building polygon layer        -> foundation-platform static or batch vector tile
industrial complex layer      -> foundation-platform static or batch vector tile
listing marker layer          -> Gongzzang dynamic marker projection/index
auction marker layer          -> source owner decided by auction ADR
real transaction marker layer -> foundation-platform or data-domain reference layer
```

### 필터 실행

Browser-side instant filters:

- asset type;
- deal type;
- visible-tile price and area ranges;
- safe public flags present in the base marker tile;
- purely visual toggles.

Server-side indexed filters:

- nationwide and region counts;
- unseen tiles;
- authorization and private visibility;
- complex namespace OR logic;
- high-cardinality numeric ranges across the full corpus;
- auction dates/courts;
- industrial attributes that are not present in the base marker tile;
- exact results after draft modal filters are applied.

### 필터 마스크

filter mask는 이미 로드된 base marker tile을 위한 선택적 compact 응답이다. 정규화된 filter
contract에서 해당 tile의 어떤 marker id가 계속 보이는지를 나타낸다.

Allowed shapes:

- show list, when few markers remain;
- hide list, when most markers remain;
- compressed bitmap, when the tile has many markers and stable marker ordinal assignment.

mask의 key는 다음과 같다.

```text
layer + z + x + y + filter_hash + marker_projection_version + anchor_snapshot_id + auth_scope
```

mask는 최적화일 뿐이다. client는 mask가 없거나 오래됐거나 지원되지 않으면 같은 정규화 filter로
전체 marker tile을 요청하는 fallback을 사용할 수 있어야 한다.

### 숫자 필터

숫자 필터는 캐시 우선으로 처리하지 않는다.

price·area·floor height·floor load·power·water·date range는 다음을 사용한다.

- 필요한 값이 있으면 현재 base marker tile에서 browser-side 비교
- full-corpus count와 보이지 않는 tile에는 server-side range index
- 자주 쓰는 normalized bucket이나 반복 요청에만 선택적 cache

### 쓰기 최신성

Listing marker freshness target:

| Surface | Target |
|---|---|
| Creator's immediate edit/register UI | synchronous local UI update after write success |
| Creator's map overlay | immediate optimistic or confirmed overlay |
| Public marker projection | 1-5 seconds after publish/update/withdraw event |
| Filter/count projection | 1-10 seconds, exact when refreshed |
| Static polygon tiles | not affected by listing writes |

Listing writes must invalidate or version only affected marker projection rows, tile ids, and filter
index entries. They must not rebuild nationwide marker artifacts.

## API 방향

Existing ADR 0037 tile path remains valid:

```text
GET /map/v1/marker-tiles/listing/{z}/{x}/{y}.pbf?filter_hash={filter_hash}
```

New companion surfaces are allowed:

```text
POST /map/v1/marker-filters/listing
GET /map/v1/marker-masks/listing/{z}/{x}/{y}?filter_hash={filter_hash}&base_version={version}
GET /map/v1/marker-counts/listing?filter_hash={filter_hash}
```

이 이름은 방향을 나타낼 뿐 최종 route를 확정하지 않는다. 구현 계획에서 정확한 route와 type을
선택해야 한다.

`filter_hash` is derived from a typed, normalized filter contract. It is not raw JSON order, raw SQL,
or user-provided code.

## 가드

구현은 다음을 강제해야 한다.

- `bbox`, `bounds`, `south`, `west`, `north`, `east`라는 public launch marker request shape를
  사용하지 않는다.
- 필지 연결 listing에 listing 소유 canonical latitude/longitude를 두지 않는다.
- 대상 record를 조용히 버리는 성공 tile/mask response를 만들지 않는다.
- listing price/status/exposure를 서비스 경계를 넘어 foundation-platform으로 옮기지 않는다.
- 모든 filter 조합의 static tile을 생성하지 않는다.
- marker projection/index가 필요할 때 listing OLTP table을 직접 읽는 map request path를 만들지 않는다.

## 영향

긍정적 효과:

- map traffic가 tile id·layer·filter identity·read index로 제한된다.
- unbounded cache key에 의존하지 않고 price·area filter가 빠르게 반응한다.
- listing write가 전국 tile을 다시 만들지 않고 영향받은 projection/index entry만 갱신한다.
- auction·실거래가·listing marker가 별도로 발전해도 layer ownership이 명확하다.
- browser는 즉시 반응하고 server는 정확한 count와 보이지 않는 지역의 authority로 남는다.

비용:

- Gongzzang은 listing OLTP row 외에 marker projection과 filter index를 유지해야 한다.
- frontend가 base tile, browser instant filter, server mask, count result를 조정해야 한다.
- filter normalization이 contract surface가 되며 test가 필요하다.
- projection lag를 관찰하고 UX에서 처리해야 한다.

## 재검토 조건

- Listing marker projection lag exceeds the public freshness target.
- Base marker tile payload becomes too large for acceptable mobile map performance.
- Filter mask complexity exceeds the cost of returning filtered marker tiles.
- Numeric filters require exact global counts faster than the chosen range index can support.
- Authorization rules become too viewer-specific for shared tile or mask caching.

## 참고 문서

- [ADR 0017 - Listing marker rendering](./0017-listing-marker-render-canvas-bitmap-stamp.md)
- [ADR 0018 - PNU-first identity](./0018-pnu-first-identity-no-coordinates.md)
- [ADR 0037 - PNU Anchor PBF Marker Tiles](./0037-pnu-anchor-pbf-marker-tiles.md)
