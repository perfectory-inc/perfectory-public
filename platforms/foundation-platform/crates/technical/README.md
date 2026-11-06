---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-11-05
---

# Foundation 기술 인프라

Foundation의 범용 기술 어댑터를 모아 둔 내부 인프라 경계다. 도메인 정책이나 published 계약을 이
디렉터리에 정의하지 않습니다.

- `outbound-http-infrastructure` — outbound HTTP 재시도·circuit breaker
- `foundation-shapefile` — zip 묶음 shapefile 스트리밍 읽기
- `foundation-disposable-database` — 시험 하나용 일회용 PostgreSQL 데이터베이스
- `foundation-media-renditions` — 사진 원본에서 표시 사다리(AVIF·WebP) 만들기(루트 ADR-0427; 사다리 값은 `config/r2-connections.contract.json`)
- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p outbound-http-infrastructure`
