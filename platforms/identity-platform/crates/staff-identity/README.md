---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-11-05
---

# staff-identity

직원 세션 검증·폐기와 ZITADEL subject ↔ staff 계정 연결을 담당합니다. 로그인 자체는 ZITADEL이
하며, 제품 B2C 인증은 이 crate의 책임이 아닙니다.

- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p staff-identity-domain`
