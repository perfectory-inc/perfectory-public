# ADR 0010 - Cargo Build SSOT and Bazel Abandonment

| Field | Value |
|---|---|
| Date | 2026-06-20 |
| Status | Accepted; reaffirmed by [ADR 0012](./0012-adopt-cross-repo-bazel-reconciliation.md) |
| Scope | `foundation-platform` build, test, lint, and release artifact production |
| Related ADRs | [ADR 0001](./0001-inherit-gongzzang-adrs.md), [ADR 0011](./0011-true-bazel-build-ssot-transition.md) |

## Context

The repository temporarily carried two verification surfaces: Cargo, which actually compiled,
tested, linted, and released the Rust workspace, and Bazel wrappers around duplicated checks and
PowerShell guardrails. Bazel did not own release artifacts and the wrapper layer created two
competing definitions of build success.

The cross-repository review recorded in Gongzzang ADR-0044 reversed the attempted Bazel transition.
The Bazel and PowerShell surfaces have since been removed from this repository.

## Decision

Cargo is the permanent build, test, lint, and release-artifact SSOT for `foundation-platform`.

- Use workspace commands for full verification.
- Use `cargo build|check|test -p <package>` for package-scoped work.
- Use Rust tests or established native tools for repository-specific invariants.
- Do not add Bazel files, targets, registries, projections, or wrappers.
- Do not add PowerShell build or verification logic.
- Reconsidering Bazel requires a new ADR backed by a measured problem that Cargo package selection
  cannot solve and evidence that Bazel works on the team's supported development environments.

## Consequences

- Build evidence and release artifacts come from one toolchain.
- Package-scoped Cargo commands provide the required local fast path without a second build graph.
- Historical Bazel experiments are not active implementation guidance.
- Cross-language frontend repositories may use their own native package/build SSOT; this ADR governs
  this Rust repository.

## Verification

The decision is enforced by absence: the repository contains no `.bazelrc`, `MODULE.bazel`,
`BUILD.bazel`, Bazel rule files, or Bazel CI jobs. Active Rust verification is expressed through
Cargo and standard native tools.
