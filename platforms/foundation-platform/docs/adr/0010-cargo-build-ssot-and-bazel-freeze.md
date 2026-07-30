# ADR 0010 - Cargo 빌드 SSOT와 Bazel 폐기

| Field | Value |
|---|---|
| Date | 2026-06-20 |
| Status | Accepted; reaffirmed by [ADR 0012](./0012-adopt-cross-repo-bazel-reconciliation.md) |
| Scope | `foundation-platform` build, test, lint, and release artifact production |
| Related ADRs | [ADR 0001](./0001-inherit-gongzzang-adrs.md), [ADR 0011](./0011-true-bazel-build-ssot-transition.md) |

## 배경

저장소에는 한때 실제로 compile·test·lint·release하는 Cargo와 중복 검사를 감싼 Bazel
wrapper 및 PowerShell guard가 함께 있었다. Bazel은 release artifact를 소유하지 않았고,
wrapper 계층이 빌드 성공에 대한 경쟁하는 정의 두 개를 만들었다.

Gongzzang ADR-0044에 기록된 저장소 간 검토로 시도했던 Bazel 전환을 되돌렸다. 이후 Bazel과
PowerShell 표면은 이 저장소에서 제거했다.

## 결정

Cargo를 `foundation-platform`의 영구 빌드·테스트·lint·릴리스 산출물 SSOT로 사용한다.

- 전체 검증에는 workspace command를 사용한다.
- package 단위 작업에는 `cargo build|check|test -p <package>`를 사용한다.
- repository 고유 불변식은 Rust test나 검증된 native tool로 확인한다.
- Bazel file·target·registry·projection·wrapper를 추가하지 않는다.
- PowerShell build·verification logic을 추가하지 않는다.
- Bazel을 다시 검토하려면 Cargo package 선택으로 해결할 수 없는 측정된 문제와 지원 개발
  환경에서 Bazel이 동작한다는 증거를 담은 새 ADR이 필요하다.

## 영향

- 빌드 증거와 release artifact가 하나의 toolchain에서 나온다.
- package 단위 Cargo command가 두 번째 build graph 없이 필요한 local fast path를 제공한다.
- 과거 Bazel 실험은 현재 구현 지침이 아니다.
- 다언어 frontend repository는 자체 native package/build SSOT를 사용할 수 있으며, 이 ADR은
  이 Rust repository에 적용한다.

## 검증

이 결정은 부재로 강제한다. 저장소에는 `.bazelrc`, `MODULE.bazel`,
`BUILD.bazel`, Bazel rule file, Bazel CI job이 없다. 활성 Rust 검증은 Cargo와 표준 native
tool만 사용한다.
