# ADR-0040: Bazel 우선 빌드·검증 제어면

| | |
|---|---|
| Date | 2026-06-07 |
| Status | **Superseded by [ADR-0044](./0044-bazel-transition-reconciliation.md)** |
| Decision owner | Platform engineering |

## 원래 결정

이 ADR은 Cargo와
pnpm/Turborepo retained during a transition. Its goals were scoped verification, hermetic inputs,
and shared remote caching without building a custom scheduler.

제안은 두 번째 target graph, transition wrapper, platform별 toolchain도 요구했다.
work, and remote-cache governance. Those costs duplicated knowledge already owned by native package
graphs and the repository verification harness.

## 대체 상태

ADR-0044 replaces this decision. Cargo is the Rust build tool, pnpm/Turborepo owns frontend tasks,
and `cargo xtask verify <area>` is the verification SSOT. Do not add Bazel files, targets, wrappers,
or registries based on this historical proposal.

지속되는 교훈은 build-platform 변경이 측정된 병목을 제거하고
existing SSOT. Adding another graph beside the native graphs does not satisfy that bar.

## 참고 문서

- [ADR-0044](./0044-bazel-transition-reconciliation.md)
- [Bazel remote caching](https://bazel.build/remote/caching)
