---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-11-05
---

# intelligence knowledge

지식 원천 등록(source registry)과 문서 청크 색인·검색을 담당하는 domain·application·infrastructure
경계입니다. 색인은 `KnowledgeIndexPort` 뒤에 있고 현재 구현은 인메모리와 Postgres 전문검색 둘입니다.
벡터 원장은 아직 소유하지 않으며, 미구현 기능을 현재 계약으로 서술하지 않습니다.

- 영역 문서: [`docs/README.md`](../../docs/README.md)
- 검증: `cargo test -p knowledge-domain`
