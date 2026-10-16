# ADR 0210: 작업 공간 빌드 보관본은 작업 공간 단위만 가져가고, 의존성 빌드는 rust-cache 에 남긴다

- Status: Accepted
- Date: 2026-10-13
- Amends: [ADR-0201](./0201-ci-keeps-the-workspace-crates-main-built-and-restores-the-source-times-they-saw.md)
  (작업 공간 산출물을 가르는 기준, 보관본 키의 대체 복원)

## Context

2026-10-13 Foundation `Rust fmt, unit tests, clippy` 잡은 시험 실행이 약 1분인데도(ADR-0206 뒤) 13–27분이었다.
최근 실행 넷의 Verify 단계는 515–1292초였고, 그 대부분이 컴파일이었다.

| 실행 | clippy | 시험 빌드 | 시험 실행 |
|---|---|---|---|
| merge_group #479 | 8m40s | 2m31s | 59s |
| main push #463 | 11m47s | 3m26s | 4m39s(ADR-0206 이전) |

clippy 구간에서 컴파일·검사한 단위는 739개, 그중 작업 공간 것은 70개였다. 의존성을 매번 처음부터 빌드했다.

원인은 ADR-0201 의 보관본이 "작업 공간 산출물"을 가르는 방법이다. rust-cache 복원 직후 target 에 있던 파일
목록(`snapshot`)에 없는 것은 모두 작업 공간 것으로 보고 보관본으로 옮겼다. rust-cache 의 복원이 일부라도
비면, 그 실행이 새로 빌드한 의존성이 전부 보관본으로 옮겨지고 rust-cache 는 저장할 것이 없다. 다음 실행의
rust-cache 는 더 비어서 같은 일이 되풀이된다. 실측으로 확인했다.

- 모든 실행의 로그: `workspace-build-cache: 0 dependency artifacts came from rust-cache`.
- main 이 저장한 rust-quality 의 rust-cache 항목: 126–132MB(의존성 빌드 없음, 레지스트리 정도).
- 보관본: 4.64GB, `stashed 14778 workspace artifacts`(의존성 포함).

보관본 키는 Cargo.lock 해시를 담고 대체 복원(restore-keys)도 같은 해시까지만 본다. 잠금 파일이 바뀌면(크레이트
분할·의존성 갱신, 이날만 여러 번) 보관본도 rust-cache 도 비어, 그 변경이 main 에 들어가 main 실행이 끝날
때까지(약 45분) 모든 PR·병합 대기열 실행이 의존성 739단위를 처음부터 빌드했다.

## Decision

1. `scripts/ci/workspace_build_cache.py stash` 는 단위 이름이 작업 공간의 패키지·타깃 이름
   (`cargo metadata --no-deps`)인 산출물만 보관본으로 옮긴다. `<deps|.fingerprint|build>/<이름>-<16자리 해시>`
   형태에서 이름이 작업 공간 것이면 작업 공간 단위다(라이브러리는 `lib` 접두). rust-cache 가 저장 전에 작업 공간
   단위를 지우는 규칙과 같은 기준이다. 나머지 의존성 산출물은 target 에 남아 rust-cache 가 저장한다.
   `snapshot` 명령과 그 파일은 없앤다.
2. rust-cache 단계에 `shared-key: rust-quality-dependencies` 를 더한다(가드 `rust-ci-setup` 이 `key` 를 받지 않으므로 `shared-key` 로 새 이름을 준다). 이전 항목은 의존성 빌드가 없는데 캐시 키는 덮어쓸 수 없어서,
   새 이름으로 시작해야 main 이 온전한 의존성 캐시를 저장한다.
3. 보관본 키를 `…-<툴체인 해시>-<잠금 파일 해시>-<날짜>` 로 나누고, 대체 복원은 같은 잠금 파일 해시 다음에
   같은 툴체인의 가장 새 보관본을 받는다. 신선도는 cargo 가 판정하므로 잠금 파일이 달라 맞지 않는 단위는 다시
   빌드될 뿐이다. 툴체인이 다르면 받지 않는다(전부 다시 빌드되므로 받는 시간이 낭비다).
4. 시험(`scripts/ci/test_workspace_build_cache.py`, 잡이 매번 먼저 돌린다): 작업 공간 밖 경로 의존성을 둔 실제
   cargo 작업 공간에서 보관 뒤 의존성 산출물이 target 에 남고 보관본에는 없는지, 다음 실행에서 `Fresh outside`
   인지 확인한다. 분류를 "전부 작업 공간"으로 되돌리면(이전 동작) 네 개가 실패하는 것을 확인했다.

## Consequences

- 잠금 파일이 바뀌어도 의존성은 rust-cache 의 대체 복원이 대부분을 주고, 작업 공간 단위는 직전 보관본이 준다.
- 캐시 총량은 거의 같다. 4.64GB 보관본 안에 있던 의존성 약 2GB 가 rust-cache 항목으로 옮겨 갈 뿐이다.
- 병합 직후 첫 main 실행까지는 새 rust-cache 키에 항목이 없어 한 번 차갑다. 그 실행이 의존성과 보관본을
  저장한 뒤의 PR 실행으로 전/후를 잰다.
