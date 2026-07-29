---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-07-29
---

# Foundation shared kernel

Foundation 내부 bounded context가 공유하는 최소 타입·오류·식별자만 제공합니다. 도메인
규칙이나 제품별 정책을 이 crate에 추가하지 않습니다.

- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p foundation-shared-kernel`
