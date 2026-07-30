---
status: current
owner: repository-maintainers
doc_type: catalog
last_reviewed: 2026-07-28
---

# 전역 ADR 목록

모노레포 전체에서 사용하는 단일 ADR 번호 체계입니다. 영역에만 적용되는 결정도 다음
전역 번호를 사용합니다. 각 영역의 기존 `docs/adr/` 번호 체계는 마지막 번호에서
동결하며, 영역 결정은 `GZ-ADR-NNNN`, `FP-ADR-NNNN`, `IDP-ADR-NNNN`,
`ITP-ADR-NNNN`처럼 영역 접두사를 붙여 인용합니다.

- [0001 — 모노레포 거버넌스와 규칙](./0001-monorepo-governance-and-conventions.md)
- [0002 — 문서 분류와 보관](./0002-docs-taxonomy-and-archive.md)
- [0003 — 문서 물리 배치](./0003-docs-physical-taxonomy.md)
- [0004 — 검증 단일 진실 원천(`cargo xtask verify`)](./0004-verification-ssot.md)
- [0005 — 훅은 조언, CI는 권위](./0005-hooks-advisory-ci-authoritative.md)
- [0006 — 객체 저장소 우선 제공](./0006-object-storage-first-serving.md)
- [0007 — 공개 코드 단일 원천과 비공개 운영 경계](./0007-public-code-private-operations-boundary.md)
- [0008 — 수동 의존성 업데이트와 조직 브랜치](./0008-manual-dependency-updates-and-organization-branches.md)
- [0009 — 한글 정본 문서와 다국어 확장 준비](./0009-korean-first-documentation-and-multilingual-readiness.md)
- [0010 — 라이브 자원 테스트 레인 (`LiveLane`)](./0010-live-resource-test-lanes.md)
- [0011 — 테스트 실행 집합 완전성](./0011-test-execution-set-completeness.md)
- [0012 — 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md)
- [0013 — 릴리스 유일성은 두 소스 종류를 함께 허용한다](./0013-release-uniqueness-admits-both-source-kinds.md)
- [0014 — serving generation은 한 단위의 소스 선택만 추적한다](./0014-serving-generation-tracks-one-unit-source-selection.md)
