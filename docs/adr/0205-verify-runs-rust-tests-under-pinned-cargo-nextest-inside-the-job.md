# ADR 0205: verify 는 Rust 시험을 고정된 cargo-nextest 로, 잡 안에서 병렬로 돌린다

- Status: Accepted
- Date: 2026-10-13
- Related: [ADR-0199](./0199-verify-runs-side-suites-beside-cargo-and-pull-requests-leave-the-boundary-slice-to-the-merge-queue.md) (verify 를 두 반쪽으로), [ADR-0197](./0197-ci-downloads-pinned-release-binaries-only-through-one-cached-retrying-fetcher.md) (고정 바이너리 내려받기), [ADR-0203](./0203-pull-request-ci-has-one-wall-clock-budget.md) (PR 벽시계 예산)

## Context

ADR-0199 뒤 Foundation `Rust fmt, unit tests, clippy` 잡의 `Verify` 단계는 약 10.4분(623초)이다. 그 안에서
`cargo test --workspace` 는 시험 바이너리를 **하나 끝나면 다음 하나**로 돌린다. 바이너리 안에서는 스레드로
병렬이지만, 퍼블리셔 lib 시험 하나(약 2분)가 도는 동안 나머지 코어는 논다.

시험을 여러 잡으로 나누는 것(nextest `--partition`)도 검토했다. 2026-10-13 측정에서 그 방향은 틀렸다:
같은 시각 저장소의 실행 중 잡은 12개, 대기 중 실행은 35개였고, #449 의 Foundation 실행은 만들어진 지 20분이
지나도 시작하지 못했다. 무료 플랜의 동시 실행 한도가 PR 벽시계의 대부분을 정한다. 잡을 더 만들면 각 잡이
의존성 빌드를 다시 치르고(캐시 복원 포함), 대기열은 길어진다.

## Decision

1. **`cargo xtask verify` 의 Rust 시험은 `cargo nextest run` 으로 돈다.** 시험 하나에 프로세스 하나, 코어 수만큼
   동시에, 바이너리 경계 없이. 선택(패키지·기능·제외)은 그대로 `verify_test_commands` 한 곳에 있고, 같은
   선택으로 `cargo test --doc` 을 한 번 더 돌린다 — nextest 는 doctest 를 돌리지 않는다. 라이브러리가 없는
   선택(예: xtask)에서 cargo 가 내는 `no library targets found` 는 "doctest 없음"으로 읽고, 다른 오류는 전부
   실패다. 모든 영역의 verify 에 같은 규칙이다(ADR-0004 한 명령).
2. **nextest 는 고정된 판을 내려받기 도구로 받는다.** 판과 체크섬은 `tools/cargo-nextest.env`, URL 모양은
   `tools/pinned-artifacts.json`(대상 트리플을 위한 `target` 칸 추가). xtask 가 처음 쓸 때
   `scripts/ci/fetch_pinned.py tool cargo-nextest --member …` 로 실행 파일 하나만 꺼내
   `~/.cache/perfectory/bin/cargo-nextest-<판>/` 에 둔다. 압축 풀기는 파이썬이 한다(윈도의 셸이 먼저 찾는
   `tar` 가 zip 을 못 읽는 GNU tar 일 수 있다). CI 는 이미 모든 verify 잡에서 그 캐시를 복원한다.
3. **잡 사이 분할(`--partition`)은 하지 않는다.** 위 측정대로 병목은 잡 길이가 아니라 동시 실행 자리다.
   잡 안 병렬로 모자라면 그때 측정과 함께 다시 결정한다.

시험: `every_test_selection_runs_under_nextest_and_as_doctests`(두 반쪽이 같은 선택을 그대로 싣는지 — doctest
반쪽을 빼면 실패), `only_a_missing_library_excuses_the_doctest_run`, 내려받기 도구의
`ExtractMemberTest`(zip·tar.gz, 없는 멤버 거부).

검토한 대안:

- **`cargo test -- --test-threads`** 를 늘린다. 이미 바이너리 안에서는 병렬이다; 느린 것은 바이너리 사이의
  직렬이다.
- **`cargo install cargo-nextest`.** 잡마다 컴파일하고, 핀·체크섬 없이 crates.io 에 기댄다(ADR-0155 가
  cargo-deny 에서 없앤 바로 그 단계).
- **`taiki-e/install-action`.** 저장소의 서드파티 액션 허용 목록이 늘고, 로컬 verify 에는 쓸 수 없다.

## Consequences

- nextest 는 시험마다 새 프로세스라, 같은 프로세스의 다른 시험이 남긴 전역 상태에 기대던 시험은 드러난다
  (ADR-0199 의 파이썬 모듈 분할이 그런 둘을 찾았다). 그런 시험은 스스로 준비하도록 고친다.
- 효과는 이 변경의 PR 에서 `Verify` 단계 초 단위로 측정해 적는다.
- 라이브 레인(`cargo xtask integration`)은 그대로다: Postgres 레인은 한 데이터베이스를 공유하고 사례 사이에
  비우므로 직렬이어야 한다(`--test-threads=1`).
