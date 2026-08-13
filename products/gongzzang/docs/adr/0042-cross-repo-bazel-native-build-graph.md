# ADR-0042: 영역 간 Bazel 네이티브 빌드 그래프

| | |
|---|---|
| Date | 2026-06-16 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## 원래 결정

이 ADR은 제품·플랫폼 코드 전체에 Bazel-first 방식을 확장해
service contracts, generated clients, policy checks, and release verification could share one build
graph. It required explicit migration boundaries and rollback conditions rather than permanent
wrappers around native commands.

## 대체 상태

코드베이스는 이제 하나의 모노레포와 루트 검증 계약을 공유한다. ADR-0044는 두 번째
Bazel graph: Cargo and pnpm/Turborepo execute language-native work, while
`cargo xtask verify <area>` provides the common local/CI entrypoint.

이 ADR에는 실행 가능한 migration 지침이 없다. 저장소 layout이나 workspace를 이 문서에서
state, runner support, or implementation progress from the original proposal.

## 참고 문서

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
- [Root ADR-0004](../../../../docs/adr/0004-verification-ssot.md)
