# ADR 0200: CI 는 main 이 빌드한 작업 공간 크레이트를 보관하고, 그 빌드가 본 소스 시각을 되돌려 놓는다

- Status: Accepted
- Date: 2026-10-13
- Amends: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) §2 (rust-cache 는
  의존성만 보관한다 → 작업 공간 크레이트는 이 ADR 의 두 번째 캐시가 보관한다)

## Context

2026-10-12 PR 실행 `Rust fmt, unit tests, clippy`(Foundation) 로그를 단계별로 쟀다. rust-cache 는 키가 완전히
맞아 의존성을 복원했는데도, 작업 공간 크레이트 25개를 매번 처음부터 컴파일했다.

| 구간 | 시간 |
|---|---|
| clippy(작업 공간 전부 `Checking`) | 1분 7초 |
| 테스트 빌드(작업 공간 전부 `Compiling`, 그중 publisher 테스트 하네스 2분 13초) | 2분 25초 |
| 정적 릴리스 툴체인 확인의 publisher 빌드 | 59초 |

원인은 둘이다.

1. **rust-cache 는 저장 전에 작업 공간 크레이트의 산출물을 지운다**(`cleanTargetDir`). 의존성만 남긴다.
2. **남겨도 쓸 수 없다.** 체크아웃은 모든 소스 파일의 수정 시각을 “지금”으로 찍고, cargo 는 경로 의존성의
   신선도를 수정 시각으로 판정한다. 산출물보다 새로운 소스는 바뀐 것으로 본다.

sccache(GHA 백엔드)는 이 시간을 줄이지 못한다. 링커를 부르는 크레이트(`bin`·테스트 하네스·`proc-macro`)는
캐시하지 않는데, 비용의 대부분이 70k 줄짜리 `foundation-outbox-publisher` **바이너리**와 그 테스트 하네스다.
캐시할 수 있는 라이브러리 크레이트는 합쳐 10초 남짓이다. cargo 의 내용 기반 신선도(`-Zchecksum-freshness`)는
아직 nightly 전용이다.

## Decision

1. **작업 공간 크레이트 산출물을 따로 보관한다**(`scripts/ci/workspace_build_cache.py`).
   - rust-cache 복원 직후 `snapshot` 이 그것이 놓은 의존성 산출물 목록을 적는다. 그 밖의 `target/*/{deps,.fingerprint,build}`
     는 이 작업 공간의 것이다.
   - main 의 녹색 빌드 끝에서 `stash` 가 추적 파일마다 blob id 와 수정 시각(ns)을 기록하고, 작업 공간 산출물을
     보관 디렉터리로 **옮긴다**(같은 파일시스템의 rename, 복사 없음). `actions/cache/save` 가 그 디렉터리를 저장한다.
   - 다음 실행에서 `restore` 가 산출물을 제자리로 옮기고, **blob 이 같은** 추적 파일에만 기록된 수정 시각을
     되돌린다. 바뀐·추가된·잡이 고친 파일은 체크아웃 시각을 그대로 갖는다.
   - 그래서 cargo 가 보는 상태는 “main 의 그 빌드 직후 + 이 PR 의 변경”과 같다. 신선도 판정은 여전히 cargo 가
     하고, 스크립트는 cargo 가 마지막으로 본 상태를 되살릴 뿐이다. 내용이 같으면 신선, 다르면 다시 빌드 — 이것이
     정확성의 근거다.
2. **키**: `foundation-workspace-build-<잡>-<OS>-<hash(rust-toolchain.toml, Cargo.lock 둘)>-<sha>`, 복원은 sha 를 뺀
   접두사로 가장 최근 것을 찾는다. 툴체인이나 lockfile 이 바뀌면 다른 계보로 넘어간다(rust-cache 의 의존성과 짝이
   맞아야 쓸모가 있다). 저장은 main 만 한다(ADR-0155 와 같다). PR·머지 그룹은 읽기만 한다.
3. **퇴출을 보이게 한다.** 복원 단계가 매 실행 작업 요약에 적는다: 복원한 키와 산출물 수, 그 빌드 이후 바뀐 추적
   파일 수, 저장소 Actions 캐시 사용량/상한, 이 접두사의 보관본 수와 크기, 다음 퇴출 후보(마지막 사용이 가장 오래된
   셋). 복원 실패는 `::warning::` 이고 사용량 90% 이상도 `::warning::` 이다.
4. **첫 적용은 Foundation `rust-quality` 잡 하나다.** 측정 뒤 같은 단계를 `postgres-integration` 에 넓힌다.

### 기각한 대안

- **sccache + GHA 백엔드**: 위 Context. 바이너리·테스트 하네스를 캐시하지 않아 이득이 10초대이고, 컴파일 단위마다
  캐시 항목을 만들어 10GB 상한을 먼저 채운다. 허용 액션 목록에도 새 액션을 넣어야 한다.
- **`cache-workspace-crates: true`(rust-cache)**: 산출물은 남지만 수정 시각 문제로 다시 컴파일한다. 또 키가
  Cargo.lock 이 바뀔 때만 바뀌어 main 이 새 산출물을 저장하지 않는다(“Cache up-to-date”).
- **모든 소스를 고정된 옛 시각으로 찍기 / `git-restore-mtime`**: 바뀐 파일도 옛 시각을 받을 수 있어(커밋 시각이
  캐시 빌드보다 이를 수 있다) 오래된 산출물을 신선하다고 판정한다 — 틀린 초록이다. blob 비교는 내용이 같을 때만
  시각을 돌려준다.
- **보관본 정리(옛 키 삭제)**: 워크플로 권한은 `contents: read` 뿐이다(`check-workflow-policy.sh`). 삭제 대신
  GitHub 의 LRU 퇴출에 맡기고 위 보고로 보이게 한다. 대체된 보관본은 다시 읽히지 않으므로 가장 먼저 퇴출된다.

## Consequences

- 측정은 PR 본문에 있다(같은 PR 에서 콜드 실행과 재사용 실행).
- main 의 Foundation 실행마다 보관본이 하나 생긴다. 크기는 요약에 매번 나온다. 상한 압박이 보이면 디버그 정보를
  줄이는 것(`CARGO_PROFILE_DEV_DEBUG`)이 다음 손잡이다.
- 검사: `scripts/ci/test_workspace_build_cache.py` 가 실제 cargo 작업 공간으로 “안 바뀐 크레이트는 Fresh,
  바뀐 크레이트는 Compiling”을 확인하고, rust-quality 잡이 매번 먼저 돌린다. 시각 복원을 빼거나 blob 비교를 빼면
  실패함을 확인했다. `check-workflow-policy.sh` 는 저장 단계의 조건으로 main 전용만 허용한다(자기 테스트 포함).
