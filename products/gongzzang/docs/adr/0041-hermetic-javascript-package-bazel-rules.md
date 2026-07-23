# ADR-0041: Hermetic JavaScript package Bazel rules

| | |
|---|---|
| Date | 2026-06-07 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## Original decision

This ADR proposed `aspect_rules_js` and `aspect_rules_ts` so Bazel, rather than ambient
`node_modules` or a local PATH, would own JavaScript package inputs and TypeScript compilation. It
required pinned Node and pnpm toolchains and lockfile-derived dependencies.

## Supersession

ADR-0044 rejects the Bazel transition. pnpm owns the package graph, Turborepo owns scoped frontend
tasks, and the root verification harness owns the local/CI contract. Do not recreate the historical
Bazel targets or wrappers from this ADR.

Hermeticity remains a goal, but it is enforced through pinned package-manager inputs, lockfiles,
reproducible containers, and drift checks without a second build graph.

## References

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
