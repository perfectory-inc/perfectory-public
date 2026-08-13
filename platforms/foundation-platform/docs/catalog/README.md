---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-07-29
---

# Foundation 카탈로그

데이터셋·공급자 endpoint·수집 레인·정책·파이프라인 계약의 조회 정본이다. 사람이 읽는
목록은 기계 계약에서 생성될 수 있으며, 이 폴더의 JSON·규칙 파일을 사용처 확인 없이
삭제하거나 임의로 복제하지 않는다.

## 먼저 보는 문서

- [공공데이터 수집 카탈로그](./public-data-collection-catalog.md)
- `public-source-endpoint-catalog.v1.json` — endpoint·source slug 계약
- `public-data-bronze-lane-registry.v1.json` — 실행 레인 계약
- `pipeline-graph.v1.json` — 파이프라인 그래프 계약

## 승인 전 제안·초안

아래 문서는 현행 운영 계약이 아니라 검토 대상이다. 삭제하거나 운영 기준으로
인용하지 말고, 승인되면 상태를 `current`로 바꾸고 이 목록을 갱신한다.

- [파이프라인 그래프 제어면(제안)](./pipeline-graph-control-plane.md)
- [산업단지 레이크하우스 PoC(제안)](./industrial-complex-lakehouse-poc.md)
- [산업단지 SSOT 모델(제안)](./industrial-complex-ssot-model.md)
- [건축물대장 일관성 규칙(초안)](./building-register-consistency-rules.v1.draft.md)
- [건축물대장 필드 매핑(초안)](./building-register-field-mapping.v1.draft.md)

실행 절차는 [Foundation 런북](../runbooks/README.md), 구조는 [아키텍처](../architecture/README.md)가 소유한다.
