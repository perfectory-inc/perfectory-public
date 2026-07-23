# ADR-0042: Cross-area Bazel-native build graph

| | |
|---|---|
| Date | 2026-06-16 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## Original decision

This ADR proposed extending the Bazel-first approach across the product and platform codebases so
service contracts, generated clients, policy checks, and release verification could share one build
graph. It required explicit migration boundaries and rollback conditions rather than permanent
wrappers around native commands.

## Supersession

The codebases now share one monorepo and one root verification contract. ADR-0044 rejects the second
Bazel graph: Cargo and pnpm/Turborepo execute language-native work, while
`cargo xtask verify <area>` provides the common local/CI entrypoint.

This ADR contains no executable migration instruction. Do not infer repository layout, workspace
state, runner support, or implementation progress from the original proposal.

## References

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
- [Root ADR-0004](../../../../docs/adr/0004-verification-ssot.md)
