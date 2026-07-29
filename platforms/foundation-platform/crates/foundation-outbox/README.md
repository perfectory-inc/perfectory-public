---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-07-29
---

# Foundation outbox

durable outbox, R2 object writer, JobBus와 이벤트 전달 경계를 소유합니다. 메시지 발행은
원장 변경과 분리하지 않고 outbox 계약을 통해 재시도·멱등성을 보장합니다.

- 운영 문서: [`docs/runbooks/outbox-webhook-fanout.md`](../../docs/runbooks/outbox-webhook-fanout.md)
- 저장소 결정: [`docs/adr/0002-r2-primary-object-storage.md`](../../docs/adr/0002-r2-primary-object-storage.md)
- 검증: `cargo test -p foundation-outbox`
