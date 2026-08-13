# ADR 0011 - Bazel 빌드 SSOT 전환 기록

| Field | Value |
|---|---|
| Date | 2026-06-20 |
| Status | Rejected and superseded by [ADR 0012](./0012-adopt-cross-repo-bazel-reconciliation.md) |
| Scope | Historical Bazel transition proposal |

## 역사적 결정

이 ADR은 Rust build·test·release 소유권을 Cargo에서 Bazel로 옮기자고 제안했다. 제안은
was never the final production state and was reversed on 2026-06-21 after repository and supported
environment validation.

이 제안을 구현하지 않는다. Cargo가 다음 조건에서 영구 build SSOT다.
[ADR 0010](./0010-cargo-build-ssot-and-bazel-freeze.md) and ADR 0012.

## 이 기록을 남기는 이유

identifier는 과거 commit message와 결정 참조가 명시적인
rejection instead of a missing document. Detailed transition plans and generated evidence were
deleted because they contradicted the final decision and had no runtime value.
