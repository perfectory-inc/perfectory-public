---
status: current
owner: intelligence-platform
doc_type: README
last_reviewed: 2026-07-29
---

# 아키텍처 결정 기록(ADR)

각 ADR은 되돌리기 어려운 중요한 결정의 배경·결정·영향을 기록한다. 승인된 ADR은 변경하지 않고
새 ADR로 대체한다. 번호는 순서대로 붙인다.

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [0001](0001-canonical-implementation-rust.md) | Rust를 Intelligence Platform 정본 구현으로 사용하고 Python은 폐기 | Accepted | 2026-07-08 |
| [0002](0002-canonical-release-rag-design.md) | canonical release를 읽는 RAG 설계 경계 | 제안(구현 보류) | 2026-07-08 |
| [0003](0003-korean-morphology-in-rust.md) | 한국어 형태소 분석은 Rust 색인기가 하고 사전은 mecab-ko-dic을 쓴다 | Accepted | 2026-08-14 |
