# ADR 0012 - Adopt Cross-Repo Build Reconciliation

| Field | Value |
|---|---|
| Date | 2026-06-21 |
| Status | Accepted |
| Scope | `foundation-platform` build strategy and cross-repository alignment |
| Governs | Consumer pointer to Gongzzang ADR-0044 |
| Related ADRs | [ADR 0010](./0010-cargo-build-ssot-and-bazel-freeze.md), [ADR 0011](./0011-true-bazel-build-ssot-transition.md) |

## Context

Gongzzang ADR-0044 is the cross-repository decision for the abandoned Bazel transition. This ADR
records how `foundation-platform` consumes that decision without copying its full historical
narrative.

## Decision

`foundation-platform` adopts the final, reversed state of Gongzzang ADR-0044:

- Cargo is the permanent Rust build, test, lint, and release SSOT.
- Bazel is abandoned, not paused.
- PowerShell build and verification logic is prohibited.
- Package-scoped Cargo commands are the supported affected-work fast path.
- Verification registries, projections, ratchets, and wrappers that only verify themselves are not
  part of the architecture.

ADR 0010 is reaffirmed. ADR 0011 is retained only as a rejected historical pointer.

## Consequences

- The repository has one active build direction.
- No Bazel enablers, release cutover, remote cache, or cross-repository Bazel graph remain planned.
- A future build-system change requires a new measured decision; it cannot reactivate ADR 0011.

## Authoritative Reference

[Gongzzang ADR-0044](../../../../products/gongzzang/docs/adr/0044-bazel-transition-reconciliation.md).
