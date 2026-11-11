---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-11-05
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

지도 표식 위치는 매물 카드가 소유하지 않는다. 지도는 공짱 매물 표식 타일과 파운데이션의
벡터 타일 매니페스트(`/catalog/v1/vector-tiles/runtime-manifest`)를 소비하고 PNU 나 객체 식별자로 필지·매물 패널을 연다.
필지 기준점(PNU 앵커) 표식 층은 지금 지도에 없다 — 파운데이션의 앵커 타일 파이프라인은 은퇴했고 나중에
타일 굽기의 클러스터 단위로 돌아온다([루트 ADR-0413](../../../../docs/adr/0413-the-parcel-anchor-marker-tile-pipeline-retires-and-returns-as-a-cluster-unit-of-the-tile-bake.md)).

## 매물 표식 제공

매물 지도 표식은 공짱의 `listing_marker_projection` 읽기 모델을 쓴다. `listing` 표는
매물 의미의 쓰기 모델 정본으로 남고, 표식 서빙은 원천 계보와 함께 공짱 `parcel_marker_anchor`
사본의 PNU 기준점 위치를 담은 투영 행을 읽는다. 그 사본은 이벤트
`catalog.parcel_marker_anchor.snapshot.published.v1` 로 들어오는데, 이 이벤트의 생산자는 ADR-0413 으로 없어졌고
소비자 쪽 정리는 공짱 몫으로 남아 있다.

브라우저의 즉시 필터는 이미 받은 표식 타일에 자산 종류·거래 종류·가격·면적 같은 빠른 필터를
적용한다. 전국 정확 건수·아직 보지 않은 타일 결과·선택적 표식 가림은 화면 영역(`bbox`) 요청이 아니라
서버 표식 색인에서 얻는다.

최신성은 이렇게 합성된다:

```text
visible markers = base tile + delta overlay - tombstone overlay - unauthorized records
```

- 기본 타일: 일반 공짱 매물 표식 PBF 타일
- 변경분 겹침: 새로 공개되거나 갱신된 표식을 위한 단기 `listing_delta` PBF
- 삭제 표시 겹침: 캐시된 기본 타일에 남았을 수 있는 판매·만료·거부·삭제·비공개 표식의
  식별자를 숨기는 단기 집합

프런트엔드는 삭제 표시 응답 실패를 오래된 비공개·삭제 표식을 계속 보여도 된다는 뜻으로 읽으면 안 된다.
안전한 대안은 기본 타일을 새로 읽거나, 겹침 상태가 준비될 때까지 그 표식 층을 숨기는 것이다.

## 환경

```text
NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID=<NCP Maps Client ID>
NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL=<foundation-platform origin>
```

## 자주 확인하는 항목

| 증상 | 예상 원인 | 확인 |
|---|---|---|
| 지도가 비어 있다 | Naver 클라이언트 ID 가 없거나 SDK 가 막혔다 | 브라우저 콘솔의 Naver SDK 로드 오류 |
| 필지 기준점 표식이 없다 | 예상된 상태. 기준점 표식 층은 지도에 없다 (루트 ADR-0413; `marker-tile-style.ts` 는 층 식별자 상수만 둔다) | — |
| 매물 수가 0 이다 | 활성 매물이 없거나 필터가 너무 좁다 | `SELECT count(*) FROM listing WHERE status='active'` |
| 필터가 안 먹는다 | 주소 질의와 저장소 상태가 어긋났다 | 네트워크 요청의 질의 문자열 |

## 데이터 원천

- 매물 카드: 공짱 API 와 `listing` 표
- 표식 위치: `listing_marker_projection` (공짱 `parcel_marker_anchor` 사본의 기준점)를 공짱 매물 표식 PBF 타일로 제공
- 필지 식별: PNU.
- 사진: 매물 사진 표 연동

## 주요 파일

| 파일 | 역할 |
|---|---|
| `apps/web/components/listings/listing-map.tsx` | Naver 지도 실행과 PBF 원천·층 설정 |
| `apps/web/lib/map/vector-tile-manifest.ts` | 파운데이션 벡터 타일 매니페스트 클라이언트 |
| `apps/web/lib/map/marker-tile-contract.ts` | 공짱 매물 표식 타일 원천 계약 |
| `apps/web/lib/map/marker-tile-style.ts` | 공짱 매물 표식 층 등록 (기준점 층 식별자 상수만) |
| `apps/web/lib/map/listing-marker-filter.ts` | 브라우저 쪽 매물 표식 필터와 삭제 표시 숨김 판정 |
| `apps/web/lib/listings/use-listings-query.ts` | 매물 카드 질의 훅 |
| `apps/web/stores/listings.ts` | 매물 필터와 선택된 매물 상태 |

## 디버그 명령

```bash
psql "$DATABASE_URL" -c "SELECT count(*) FROM listing WHERE status='active'"

curl -H "Authorization: Bearer <jwt>" \
  "http://localhost:8080/listings?pnu=9999900101100070000&page=0&size=5" | jq

cargo run -p gongzzang-api
pnpm --filter @gongzzang/web dev
```

## 정본 규칙

매물 카드 데이터는 표식 좌표를 담으면 안 된다. 제품의 표식 위치는 매물 표식 PBF 타일(PNU 기준점)로 해석하고,
고른 뒤에 상세 매물 JSON 을 가져온다.
