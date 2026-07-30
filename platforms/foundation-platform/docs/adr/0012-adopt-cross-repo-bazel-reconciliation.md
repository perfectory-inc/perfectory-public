# ADR 0012 - 저장소 간 빌드 조정 채택

| Field | Value |
|---|---|
| Date | 2026-06-21 |
| Status | Accepted |
| Scope | `foundation-platform` build strategy and cross-repository alignment |
| Governs | Consumer pointer to Gongzzang ADR-0044 |
| Related ADRs | [ADR 0010](./0010-cargo-build-ssot-and-bazel-freeze.md), [ADR 0011](./0011-true-bazel-build-ssot-transition.md) |

## 배경

Gongzzang ADR-0044는 폐기된 Bazel 전환에 대한 저장소 간 결정이다. 이 ADR은 전체 역사
서술을 복제하지 않고 `foundation-platform`이 그 결정을 소비하는 방식을 기록한다.

## 결정

`foundation-platform` adopts the final, reversed state of Gongzzang ADR-0044:

- Cargo가 영구 Rust build·test·lint·release SSOT다.
- Bazel은 일시 중지가 아니라 폐기했다.
- PowerShell build·verification logic을 금지한다.
- package 단위 Cargo command가 영향받은 작업의 지원 fast path다.
- 자기 자신만 검증하는 verification registry·projection·ratchet·wrapper는 architecture의
  일부가 아니다.

ADR 0010 is reaffirmed. ADR 0011 is retained only as a rejected historical pointer.

## 영향

- repository에는 활성 build 방향이 하나다.
- Bazel enabler, release cutover, remote cache, 저장소 간 Bazel graph를 더 이상 계획하지 않는다.
- 향후 build-system 변경에는 새로 측정한 결정이 필요하며 ADR 0011을 되살릴 수 없다.

## 정본 참고

[Gongzzang ADR-0044](../../../../products/gongzzang/docs/adr/0044-bazel-transition-reconciliation.md).
