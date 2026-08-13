---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# 매물 검색 런타임 안내

## 현재 런타임 형태

`/listings`는 PNU 우선 매물 화면이다.

```text
/login -> /listings
          -> proxy.ts auth gate
          -> GET /api/proxy/listings?pnu=&admin_code=&types=&page=
          -> backend GET /listings
          -> ListingRepository::find_card_summaries
          -> SQL filters on listing.parcel_pnu and denormalized parcel columns
```

지도 marker 위치는 매물 카드가 소유하지 않는다. 지도는 foundation-platform vector tile manifest의
PNU-anchor PBF layer를 소비하고 PNU 또는 object ID로 필지·매물 panel을 연다.

## 매물 마커 제공

매물 지도 marker는 Gongzzang `listing_marker_projection` read model을 사용한다. `listing` table은
매물 의미의 write-model SSOT로 남고, marker serving은 source lineage와 함께 foundation-platform
PNU anchor 위치를 복사한 projection row를 읽는다.

브라우저 즉시 filter는 이미 로드한 marker tile에 자산 종류·거래 종류·가격·면적 같은 빠른 filter를
적용한다. 전국 정확 건수·아직 보지 않은 tile 결과·선택적 marker mask는 viewport `bbox` 요청이 아니라
서버 marker index에서 얻는다.

Freshness is composed as:

```text
visible markers = base tile + delta overlay - tombstone overlay - unauthorized records
```

- Base tile: 일반 Gongzzang 매물 marker PBF tile
- Delta overlay: 새로 공개되거나 갱신된 marker용 단기 `listing_delta` PBF
- Tombstone overlay: cached base tile에 남았을 수 있는 판매·만료·거부·삭제·private marker의
  marker ID를 숨기는 단기 set

프론트엔드는 tombstone 응답 실패를 오래된 private·삭제 marker를 계속 보여도 된다는 뜻으로 해석하면 안 된다.
안전한 fallback은 base tile을 새로 읽거나 overlay 상태가 준비될 때까지 해당 marker layer를 숨기는 것이다.

## 환경

```text
NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID=<NCP Maps Client ID>
NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL=<foundation-platform origin>
```

## 자주 확인하는 항목

| 증상 | 예상 원인 | 확인 |
|---|---|---|
| Map is blank | Naver client ID missing or SDK blocked | Browser console for Naver SDK load errors |
| Marker layer absent | Foundation Platform vector tile manifest unavailable or missing anchor artifacts | Network request to `/catalog/v1/vector-tiles/manifest` or configured `NEXT_PUBLIC_TILES_MANIFEST_URL` |
| Listing count is zero | No active listings or filters too narrow | `SELECT count(*) FROM listing WHERE status='active'` |
| Filters ignored | URL query and store diverged | Network request query string |

## 데이터 원천

- 매물 카드: Gongzzang API와 `listing` table
- marker 위치: foundation-platform 필지 marker anchor를 PBF vector tile로 제공
- Parcel identity: PNU.
- 사진: listing-photo table 연동

## 주요 파일

| File | Role |
|---|---|
| `apps/web/components/listings/listing-map.tsx` | Naver map runtime and PBF source/layer setup |
| `apps/web/lib/map/vector-tile-manifest.ts` | Foundation Platform vector tile manifest client |
| `apps/web/lib/map/marker-tile-contract.ts` | Gongzzang listing marker tile source contract |
| `apps/web/lib/map/marker-tile-style.ts` | Foundation Platform anchor and Gongzzang listing layer registration |
| `apps/web/lib/map/listing-marker-filter.ts` | Browser-side listing marker filter plus tombstone hide predicate |
| `apps/web/lib/listings/use-listings-query.ts` | Listing card query hook |
| `apps/web/stores/listings.ts` | Listing filters and selected listing state |

## 디버그 명령

```bash
psql "$DATABASE_URL" -c "SELECT count(*) FROM listing WHERE status='active'"

curl -H "Authorization: Bearer <jwt>" \
  "http://localhost:8080/listings?pnu=9999900101100070000&page=0&size=5" | jq

cargo run -p gongzzang-api
pnpm --filter @gongzzang/web dev
```

## SSOT 규칙

매물 카드 데이터는 marker 좌표를 담으면 안 된다. 제품 marker 위치는 PNU anchor PBF tile로 해석하고,
선택 후 상세 매물 JSON을 가져온다.
