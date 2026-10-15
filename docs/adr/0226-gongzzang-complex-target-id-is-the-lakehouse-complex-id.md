# ADR 0226: 공짱이 저장하는 산업단지 대상 id 는 lakehouse complex id(UUIDv5) 하나다

- Status: Proposed
- Date: 2026-10-14
- Area: `products/gongzzang`

## Context

산업단지에는 서로 계산되지 않는 신원이 둘 있다(Foundation 쪽 결정은 루트 ADR-0043·ADR-0048).

| 이름 | 형식 | 어디서 |
|---|---|---|
| `catalog.industrial_complex.id` | UUIDv7 (`Uuid::now_v7()`) | Catalog Postgres 의 주 키, `GET /catalog/v1/complexes/{id}` |
| lakehouse `complex_id` | UUIDv5 (Bronze→Silver 유도) | `complex` 타일 feature id, Gold 산출물 키, `GET /catalog/v1/complexes/by-lakehouse-id/{id}` |

공짱이 손에 쥐는 산업단지 id 는 이미 전부 두 번째다: 지도 클릭(타일 feature id), 패널 URL
(`apps/web/lib/identity/patterns.ts` `LAKEHOUSE_COMPLEX_ID_PATTERN`), API 가장자리
(`shared_kernel::lakehouse_complex_id::LakehouseComplexId`), `GET /api/complexes/:lakehouse_complex_id`.

그런데 대상을 문자열로 저장하는 두 표 — `bookmark_external`, `featured_content` — 의 `target_id varchar(50)` 은
어느 쪽이든 받았다. v7 로 저장된 찜·추천은 패널을 열면 조용히 404 가 된다.

## Decision

1. **정본 형식은 lakehouse complex id**: 소문자·하이픈 포함 UUIDv5
   (`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).
2. 공짱 DB 에 `DOMAIN lakehouse_complex_id` 를 하나 두고(이 DB 에서 형식이 적힌 유일한 곳), 두 표에
   `target_kind = 'industrial_complex'` 일 때만 `target_id::lakehouse_complex_id` 를 요구하는 CHECK 를 건다
   (`bookmark_external_complex_target_is_lakehouse_id`, `featured_content_complex_target_is_lakehouse_id`).
   전진 마이그레이션 `20261014101000_complex_target_id_is_lakehouse_complex_id.sql`.
3. 다른 `target_kind`(필지·경매·제조사·매물)는 건드리지 않는다.

## Rejected

- **Catalog id(v7)를 정본으로**: 타일·패널·Gold 가 모두 v5 라 매번 다리(`by-lakehouse-id` 의 역)를 건너야 하고,
  Catalog 에 없는 단지(쓰기 API 로만 등록된 행)는 v5 가 없다 — 그런 단지는 패널도 없다.
- **`target_id` 열 자체를 DOMAIN 으로**: 한 열이 여러 종류의 id 를 담으므로 종류별 조건이 필요하다.
- **`admin_action.target_id` 도 묶기**: 감사 기록용 자유 문자열이고 `target_kind` 도 열린 값이다. 대상 종류가
  닫힌 두 표만 묶는다.

## Consequences

- 런칭 전이고 두 표에 산업단지 행을 쓰는 운영 경로가 아직 없어(저장소만 있음) 기존 행 정리는 없다.
  위반 행이 있으면 마이그레이션이 실패해 배포가 멈춘다 — 그때는 행을 v5 로 고친 뒤 다시 배포한다.
- 도메인 생성자(`BookmarkExternal::try_new`, `FeaturedContent::try_new`)는 형식을 보지 않는다. 쓰기 API 가 생기면
  `LakehouseComplexId` 로 400 을 먼저 내게 한다.
