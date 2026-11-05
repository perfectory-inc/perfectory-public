---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# parcel-lookup

PNU로 필지 정보를 조회하는 port(`ParcelInfoLookup`)와 결과 모양(`ParcelInfo` 등)만 둔 crate입니다.
매물 비정규화가 소비하며, canonical 필지 데이터는 Foundation Catalog 계약을 통해서만 읽습니다.
실제 HTTP adapter는 `services/gongzzang-api`(`foundation_parcel_lookup`)에 있고, 이 crate에는
`NoOpParcelInfoLookup`만 있습니다.

- 제품 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p parcel-lookup`
