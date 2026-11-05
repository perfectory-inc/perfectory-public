---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-11-05
---

# Foundation catalog

Foundation의 필지·건물·산업단지 카탈로그 원장과 지도 편집·벡터 타일 발행 상태의 경계입니다.
`catalog-domain`·`catalog-application`·`catalog-infrastructure` 세 crate가 이 디렉터리에 함께
있으며 제품은 published HTTP 계약만 소비합니다.

- 정본 계약: [`docs/openapi/catalog.v1.json`](../../docs/openapi/catalog.v1.json)
- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p catalog-domain`

이 crate는 공공 원천을 직접 호출하지 않습니다. 수집은 `crates/collection`과
`foundation-outbox-publisher`가 담당합니다.
