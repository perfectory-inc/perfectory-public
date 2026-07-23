# ADR-0043: Bazel transition provisioning decisions

| | |
|---|---|
| Date | 2026-06-19 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## Original decision

This ADR defined prerequisites for the proposed Bazel transition: cache trust boundaries, artifact
ownership, toolchain provisioning, approval gates, and an exit condition for temporary wrappers.
Those controls were intended to keep a build-system migration from becoming an unbounded parallel
control plane.

## Supersession

ADR-0044 rejects the Bazel transition, so its provisioning plan is not executable. The general
requirements remain valid for any future build-platform proposal: immutable toolchains, explicit
cache read/write trust, reproducible release artifacts, bounded migration ownership, and removal of
the replaced path.

Any future proposal must start with a new ADR and the re-adoption bar in ADR-0044.

## References

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
