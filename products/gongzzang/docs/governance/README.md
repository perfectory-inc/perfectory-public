---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# governance/

거버넌스·문서·프로세스 SSOT.

## 책임 영역
- ADR (Architecture Decision Records)
- CODEOWNERS
- Conventional Commits (형식 규칙 — commitlint/훅 자동 강제는 미도입, [Git 컨벤션](../conventions/git-and-pr.md))
- Changesets (버전 + 릴리즈 노트)
- 의존성 업데이트 — 수동 검토 PR만 허용 (자동 Dependabot/Renovate 비활성화)
- Backstage (개발자 포털, Phase 3+)
- C4 모델 다이어그램 (Structurizr DSL)
- DORA 메트릭 (자체 수집)
- 코드 리뷰 룰
- 사고 대응 (Incident Response)
- Postmortem (No-blame)

세부 거버넌스 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## 관련 ADR
- (모든 ADR — `docs/adr/`)

## 관련 컨벤션
- [Git 컨벤션](../conventions/git-and-pr.md)
