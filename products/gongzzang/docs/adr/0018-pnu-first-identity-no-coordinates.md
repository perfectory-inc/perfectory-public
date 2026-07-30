# ADR-0018: 매물 식별자는 PNU 우선이며 좌표를 소유하지 않음

| | |
|---|---|
| Date | 2026-05-06 |
| Status | Accepted, hardened 2026-05-22 |
| Decision Owner | Product/Engineering |
| Context | ADR 0016 base-layer tiles, ADR 0037 PNU-anchor PBF marker tiles |

## 배경

Gongzzang listing은 필지에 붙은 object다. listing은 PNU로 식별되는 필지에 속한다.
GPS·지오코딩·사용자 클릭 좌표는 추정치이며 지적 데이터와 다를 수 있다.

무결성이 중요한 산업용 부동산 플랫폼에서 매물 위치의 소유자는 하나여야 한다.

## 결정

`listing.parcel_pnu`가 listing location identity다. Listing row는 product coordinate를 소유하지 않는다.

지도 마커 위치는 foundation-platform 필지 마커 앵커와 PBF 마커 타일로 결정한다. 매물 카드/상세 API는
비즈니스 데이터를 노출할 수 있지만 별도 마커 좌표를 노출하지 않는다.

## 현재 코드 상태

legacy product coordinate 경로는 출시 listing flow에서 제거했다.

- `Listing` aggregate no longer has a product coordinate field.
- `POST /listings`, `PATCH /listings/:id`, and `GET /listings/:id` no longer accept or expose a product coordinate.
- `PgListingRepository` no longer reads/writes listing product coordinates.
- `migrations/10001_core_tables.sql` no longer creates a listing product coordinate column or index.
- PNU-anchor marker contract guardrails reject reintroducing listing-card coordinates, viewport bounds marker queries, or product-coordinate storage paths.

Gongzzang은 아직 출시하지 않았으므로 baseline migration에서 출시 schema가 깨끗하며 이 좌표
경로를 위한 하위 호환 local schema 이력이 필요하지 않다.

## 영향

긍정적 효과:

- 위치 owner가 하나다: foundation-platform catalog anchor.
- listing row와 parcel geometry가 충돌하지 않는다.
- listing search/card API가 business data에 집중한다.
- 대상 record를 버리지 않고 PBF tile로 marker rendering을 확장할 수 있다.

절충점:

- building 수준이나 임의 point product에는 별도 ADR과 다른 identity model이 필요하다.
- 이 ADR 강화 전에 만든 기존 local development DB는 migration chain에서 다시 만들어야 한다.

## 재검토 조건

- Gongzzang이 임의 좌표가 실제로 필요한 필지 비연결 product를 추가한다.
- Foundation Platform이 필요한 parcel class의 marker anchor를 제공하지 못한다.
- building footprint 수준 배치가 launch requirement가 된다.

## 참고 문서

- [ADR 0037: PNU Anchor PBF Marker Tiles](./0037-pnu-anchor-pbf-marker-tiles.md)
- [Foundation Platform ADR 0008](../../../../platforms/foundation-platform/docs/adr/0008-pnu-anchor-pbf-marker-tile-contract.md)
- [Core baseline migration](../../migrations/20260719000102_core_tables.sql)
