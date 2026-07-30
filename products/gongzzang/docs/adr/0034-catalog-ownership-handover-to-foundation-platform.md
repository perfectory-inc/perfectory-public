# ADR 0034 - Catalog 소유권을 Foundation Platform으로 이관

| Field | Value |
|---|---|
| Date | 2026-05-11 |
| Status | Completed; ownership reaffirmed by [ADR 0048](./0048-horizontal-platform-redefinition.md) |
| Boundary SSOT | `docs/architecture/foundation-platform-boundary.v1.json` |

## 결정

Foundation Platform은 canonical industrial-complex·parcel·building·manufacturer,
public/reference spatial, collection, lakehouse data를 소유한다. Gongzzang은 발행된
Foundation API·event·immutable artifact를 통해서만 이 사실을 소비한다.

## 완료한 분리

다음 구현 category는 Gongzzang runtime workspace에 없다.
workspace에는 다음이 없다:

- canonical Catalog domain crate
- V-World·data.go.kr source client
- raw capture·collection-control runtime
- public/reference vector-tile ETL
- Catalog API drift monitoring
- Foundation 소유 collection·raw-data DB table

Gongzzang은 listing, listing media, auction, product user, product search, product-facing
marker semantics를 영구적으로 소유한다. Foundation artifact에서 파생한 local read model은
둘 수 있지만 canonical coordinate나 Catalog source가 아니다.

## 신규 스키마 규칙

이 프로젝트는 출시하지 않았다. 따라서 Gongzzang migration chain은
최종 product 소유 schema만 둔다. 폐기된 Foundation 소유 table을 먼저 만들고 compatibility
migration으로 삭제하거나 이름을 바꾸지 않는다.

## 강제 지점

- `docs/architecture/foundation-platform-boundary.v1.json`이 소유권과 금지 dependency를
  기록한다.
- `scripts/lefthook/foundation-ownership-boundary.sh`가 활성 Gongzzang code에 Foundation
  내부 구현이 재도입되는 것을 거부한다.
- `tests/migrations/test_v001_full.sh`가 새 DB에 product table만 있고 Foundation 소유
  table은 없음을 증명한다.

## 현재 저장소 간 출처

- [ADR 0048](./0048-horizontal-platform-redefinition.md)
- `../../../../platforms/foundation-platform/docs/adr/0021-adopt-horizontal-platform-redefinition.md`
