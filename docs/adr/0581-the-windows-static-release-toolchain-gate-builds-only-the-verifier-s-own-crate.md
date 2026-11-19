# ADR 0581: Windows 정적 릴리스 도구 게이트는 검증기의 크레이트만 빌드한다

- Status: Accepted
- Date: 2026-11-19
- Builds on: [ADR-0200](./0200-the-outbox-publisher-is-split-into-lane-library-crates.md)(발행기 레인 크레이트), [ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)(일 없는 잡은 러너를 잡지 않는다), [ADR-0580](./0580-a-runner-is-taken-only-by-a-job-whose-work-outweighs-its-setup-and-the-release-dry-run-runs-once-in-the-merge-queue.md)

## Context

`Static-release executable toolchain (Windows)` 는 Foundation CI 에서 준비가 가장 긴 잡으로 보고됐다(2026-11-18 coordinator 측정 1,038 s). 단계별로 다시 쟀다(`gh api .../jobs`, 2026-11-18).

| 실행 | 이벤트 | 잡 전체 | Rust 설정 | Rust 캐시 | 검증 단계 |
|---|---|---:|---:|---:|---:|
| 44421573587 | schedule | 898 s | 22 s | 5 s(miss) | **840 s** |
| 44419648278 | pull_request | 823 s | 20 s | 5 s(miss) | **783 s** |

"준비"로 보인 것은 검증 단계 안의 `cargo run -p foundation-outbox-publisher -- verify-static-release-toolchain` 이다. 그 명령이 발행기 전체를 Windows 에서 처음부터 빌드한다. 측정 스크립트가 단계 이름의 `toolchain` 을 준비로 분류해서 준비로 보였다.

- 검증기 `static_release_toolchain::verify` 는 `foundation-publisher-tiles` 에 있다. 그 모듈이 쓰는 것은 std·anyhow·serde·sha2·tokio 뿐이다.
- 발행기는 크레이트 741개를 빌드하고 `libduckdb-sys`·`rdkafka-sys`(C/C++ 소스 빌드)를 포함한다. `foundation-publisher-tiles` 는 615개이고 둘 다 없다(`cargo tree -e normal`).
- Windows 캐시는 저장하지 않는다([ADR-0269](./0269-a-pull-request-verifies-only-the-crates-it-reaches.md): 1.5 GB 항목이 Linux 의존성 캐시를 10 GB 밖으로 밀어냈다). 그래서 이 잡은 언제나 콜드 빌드다.

## Decision

1. `foundation-publisher-tiles` 에 바이너리 `verify-static-release-toolchain` 을 둔다. 하는 일은 발행기 명령과 같은 `static_release_toolchain::verify(30 s)` 호출 하나다.
2. Windows 게이트는 `cargo run --locked -p foundation-publisher-tiles --bin verify-static-release-toolchain` 을 돌린다. 도구 설치(`static_release_toolchain_contract.py install`)와 선택 규칙은 그대로다.
3. 그 바이너리 파일은 게이트의 입력이다(`foundation_ci_scope.py` `RULES`·입력 표). 발행기의 `verify-static-release-toolchain` 명령은 남는다. 운영·Linux 단계(`rust-quality`)는 계속 그 명령을 쓴다.
4. 캐시는 늘리지 않는다. Windows Rust 캐시 저장과 R2 sccache 의 Windows 확장은 쓰지 않는다. 빌드할 것을 줄이는 쪽이 캐시 예산(ADR-0269)을 건드리지 않는다.

## Consequences

- 게이트가 골라졌을 때 잡 시간은 DuckDB·rdkafka 네이티브 빌드만큼 준다. 줄어든 양은 이 PR 의 CI 실행에서 잰다(이 PR 이 바이너리를 바꾸므로 게이트를 고른다).
- 위험: 발행기 진입점(`main.rs` 의 명령 해석)은 더 이상 Windows 에서 컴파일되지 않는다. 그 파일이 하는 Windows 특유의 일은 없다. 검증 로직은 같은 함수다.
