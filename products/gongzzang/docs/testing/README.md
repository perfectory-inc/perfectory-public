---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# testing/

테스트 전략·도구·커버리지 SSOT.

## 책임 영역
- 단위 (cargo test, Vitest)
- 통합 (Testcontainers, sqlx::test)
- 계약 (Pact, Phase 3+)
- 스냅샷 (insta, Vitest snapshot)
- Property-based (proptest)
- Mutation (cargo-mutants, 주간 cron)
- E2E (Playwright)
- 부하 (k6)
- 카오스 (Chaos Mesh, Phase 4+)
- 시각 회귀 (Lost Pixel + Storybook)
- 커버리지 임계값 (도메인 90%+)

세부 테스트 문서가 필요해지면 루트 [운영 준비 작업 목록](../../../../docs/roadmap/production-readiness.md)에
작업을 먼저 등록한 뒤 이 폴더에 정본 문서를 추가한다.

## 관련 ADR
- (도입 시 ADR 작성)

## 관련 컨벤션
- [테스트 컨벤션](../conventions/testing.md)
