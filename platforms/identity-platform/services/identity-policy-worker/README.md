---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-11-11
---

# identity-policy-worker

Identity 아웃박스 이벤트를 임차(lease)로 집어 `IDENTITY_POLICY_EVENT_ENDPOINT` 로 HTTP 발행하는
작업자입니다. 한 번 이상 전달하며, 요청마다 멱등 키 헤더를 붙입니다. 이벤트 계약은
`identity-contracts` 와 함께 판을 관리합니다.

- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p identity-policy-worker`
