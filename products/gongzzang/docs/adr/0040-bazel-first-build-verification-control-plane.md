# ADR-0040: Bazel-first build and verification control plane

| | |
|---|---|
| Date | 2026-06-07 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## Original decision

This ADR proposed Bazel as the common build and verification entrypoint, with Cargo and
pnpm/Turborepo retained during a transition. Its goals were scoped verification, hermetic inputs,
and shared remote caching without building a custom scheduler.

The proposal also required a second target graph, transition wrappers, platform-specific toolchain
work, and remote-cache governance. Those costs duplicated knowledge already owned by native package
graphs and the repository verification harness.

## Supersession

ADR-0044 replaces this decision. Cargo is the Rust build tool, pnpm/Turborepo owns frontend tasks,
and `cargo xtask verify <area>` is the verification SSOT. Do not add Bazel files, targets, wrappers,
or registries based on this historical proposal.

The durable lesson is that a build-platform change must remove a measured bottleneck and replace an
existing SSOT. Adding another graph beside the native graphs does not satisfy that bar.

## References

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
- [Bazel remote caching](https://bazel.build/remote/caching)
