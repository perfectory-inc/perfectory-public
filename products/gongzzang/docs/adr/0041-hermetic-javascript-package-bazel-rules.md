# ADR-0041: 격리된 JavaScript 패키지 Bazel 규칙

| | |
|---|---|
| Date | 2026-06-07 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## 원래 결정

이 ADR은 ambient 환경 대신 Bazel이 `aspect_rules_js`와 `aspect_rules_ts`를 사용해
`node_modules` or a local PATH, would own JavaScript package inputs and TypeScript compilation. It
required pinned Node and pnpm toolchains and lockfile-derived dependencies.

## 대체 상태

ADR-0044 rejects the Bazel transition. pnpm owns the package graph, Turborepo owns scoped frontend
tasks, and the root verification harness owns the local/CI contract. Do not recreate the historical
이 ADR의 Bazel target이나 wrapper를 다시 도입하지 않는다.

Hermeticity remains a goal, but it is enforced through pinned package-manager inputs, lockfiles,
reproducible containers, and drift checks without a second build graph.

## 참고 문서

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
