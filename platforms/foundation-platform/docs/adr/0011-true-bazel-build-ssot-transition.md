# ADR 0011 - True Bazel Build SSOT Transition

| Field | Value |
|---|---|
| Date | 2026-06-20 |
| Status | Rejected and superseded by [ADR 0012](./0012-adopt-cross-repo-bazel-reconciliation.md) |
| Scope | Historical Bazel transition proposal |

## Historical Decision

This ADR proposed moving Rust build, test, and release ownership from Cargo to Bazel. The proposal
was never the final production state and was reversed on 2026-06-21 after repository and supported
environment validation.

Do not implement this proposal. Cargo is the permanent build SSOT under
[ADR 0010](./0010-cargo-build-ssot-and-bazel-freeze.md) and ADR 0012.

## Why This Stub Remains

The identifier remains so old commit messages and decision references resolve to an explicit
rejection instead of a missing document. Detailed transition plans and generated evidence were
deleted because they contradicted the final decision and had no runtime value.
