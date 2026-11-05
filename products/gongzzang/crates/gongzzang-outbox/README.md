---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# gongzzang-outbox

Gongzzang 제품 이벤트를 durable outbox에서 발행하는 경계입니다. `tick`이
`OutboxRepository::claim_unpublished`(lease)로 이벤트를 가져와 `Sink`로 발행하고, 기본 sink는
`LoggingSink`입니다. daemon은 [`services/gongzzang-outbox-publisher`](../../services/gongzzang-outbox-publisher/README.md)입니다.
Foundation 이벤트와 제품 이벤트는 각 소유 영역의 계약을 따릅니다.

- 제품 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p gongzzang-outbox`
