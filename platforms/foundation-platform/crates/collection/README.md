---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-07-29
---

# Foundation collection

공공데이터 source adapter와 Bronze 수집 계약을 소유합니다. 원시 응답 보존, 요청 지문,
object manifest, commit protocol은 Foundation 수집 경계의 불변식입니다.

- 데이터 목록: [`docs/catalog/public-data-collection-catalog.md`](../../docs/catalog/public-data-collection-catalog.md)
- 실행 레인: [`docs/catalog/public-data-bronze-lane-registry.v1.json`](../../docs/catalog/public-data-bronze-lane-registry.v1.json)
- 검증: `cargo test -p foundation-collection-domain`

Postgres 원장과 R2 쓰기는 애플리케이션·인프라 계층에서 조합합니다.
