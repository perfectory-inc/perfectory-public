---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-11-05
---

# intelligence-contracts

Intelligence 이벤트 경계의 공유 계약 타입입니다. 이벤트 envelope·topic 이름·schema subject,
dead-letter 기록, trace context를 소유합니다. 이 영역은 Foundation 원장을 직접 쓰지 않습니다.

- 설계: [`docs/architecture.md`](../../docs/architecture.md)
- Avro 스키마: [`schemas/README.md`](../../schemas/README.md)
- 검증: `cargo test -p intelligence-contracts`
