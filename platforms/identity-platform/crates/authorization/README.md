---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-07-29
---

# identity authorization

서비스와 직원의 인가 정책 결정을 담당하는 Identity bounded context입니다. 다른 영역은
Identity DB를 직접 읽지 않고 published policy API를 사용합니다.

- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p authorization-domain`
