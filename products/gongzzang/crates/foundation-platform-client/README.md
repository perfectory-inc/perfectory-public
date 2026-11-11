---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# foundation-platform-client

공짱이 Foundation 의 공개 HTTP 계약을 쓰는 클라이언트입니다. 공공데이터
제공기관을 직접 호출하지 않습니다. 주소 검증, Zitadel 작업 신원 인증, 카탈로그
조회 클라이언트(`FoundationCatalogClient`)를 제공하며 호출은 `circuit-breaker` 를 거칩니다.

- 제품 문서: [`docs/README.md`](../../docs/README.md)
- Foundation 계약(제공자 정본): [`platforms/foundation-platform/docs/openapi/catalog.v1.json`](../../../../platforms/foundation-platform/docs/openapi/catalog.v1.json)
- 소비자 사본·고정: 이 크레이트의 `openapi/catalog.v1.json`과
  [`foundation-platform-catalog-api-contract.v1.pin.json`](../../docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json)
  (`tests/catalog_contract_pin.rs`가 대조)
- 검증: `cargo test -p foundation-platform-client`
