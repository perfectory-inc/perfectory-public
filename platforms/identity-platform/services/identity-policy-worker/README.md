---
status: current
owner: identity-platform
doc_type: README
last_reviewed: 2026-11-05
---

# identity-policy-worker

Identity outbox 이벤트를 lease로 집어 `IDENTITY_POLICY_EVENT_ENDPOINT`로 HTTP 발행하는
at-least-once worker입니다(멱등 키 헤더 포함). 이벤트 계약은 `identity-contracts`와 함께
버전 관리합니다.

- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p identity-policy-worker`
