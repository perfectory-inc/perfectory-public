# ADR 0201: CI 는 main 이 빌드한 작업 공간 크레이트를 보관하고, 그 빌드가 본 소스 시각을 되돌려 놓는다

- Status: Superseded by ADR-0269 (2026-10-18: 보관분을 은퇴시켰다, 아래 Revisions)
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
   - cargo 는 `rerun-if-changed=<디렉터리>` 에서 디렉터리 **자신의** 수정 시각도 읽는다(측정: 체크아웃 시각을 가진
     디렉터리 때문에 그런 패키지가 매번 다시 빌드됐다). 그래서 디렉터리 시각도 기록하고, 그 아래 추적 항목의 이름
     집합이 같을 때만 되돌린다. 파일이 지워지거나 더해지면 디렉터리는 체크아웃 시각을 갖는다.
   - 마이그레이션 묶음을 품는 패키지(`sqlx` 의 migrate 매크로)는 stable 에서 새로 더해진 파일을 추적하지 못한다.
     API 만 갖고 있던 한 줄 build script(`rerun-if-changed=<migrations>`)를 나머지 다섯 패키지에 붙인다. 이것이
     없으면 재사용된 바이너리가 옛 마이그레이션 목록을 품는다.
2. **키**: `foundation-workspace-build-<잡>-<OS>-<hash(rust-toolchain.toml, Cargo.lock 둘)>-<UTC 날짜>`, 복원은
   날짜를 뺀 접두사로 가장 최근 것을 찾는다. 캐시 키는 바꿀 수 없고 워크플로 토큰은 항목을 지울 수 없으므로
   (`contents: read` 만 허용, `check-workflow-policy.sh`), 날짜가 보관본 수를 하루 하나로 묶는다: 그날 첫 녹색
   main 실행만 저장하고, 같은 날 뒤 실행은 이미 있는 키라 저장하지 않는다. 툴체인이나 lockfile 이 바뀌면 다른 계보로 넘어간다(rust-cache 의 의존성과 짝이
   맞아야 쓸모가 있다). 저장은 main 만 한다(ADR-0155 와 같다). PR·머지 그룹은 읽기만 한다.
3. **퇴출을 보이게 한다.** 복원 단계가 매 실행 작업 요약에 적는다: 복원한 키와 산출물 수, 그 빌드 이후 바뀐 추적
   파일 수, 저장소 Actions 캐시 사용량/상한, 이 접두사의 보관본 수와 크기, 다음 퇴출 후보(마지막 사용이 가장 오래된
   셋). 복원 실패는 `::warning::` 이고 사용량 90% 이상도 `::warning::` 이다.
4. **첫 적용은 Foundation `rust-quality` 잡 하나다.** `postgres-integration` 으로 넓히는 것은 캐시 상한 여유가 생긴 뒤다(아래).

### 기각한 대안

- **sccache + GHA 백엔드**: 위 Context. 바이너리·테스트 하네스를 캐시하지 않아 이득이 10초대이고, 컴파일 단위마다
  캐시 항목을 만들어 10GB 상한을 먼저 채운다. 허용 액션 목록에도 새 액션을 넣어야 한다.
- **`cache-workspace-crates: true`(rust-cache)**: 산출물은 남지만 수정 시각 문제로 다시 컴파일한다. 또 키가
  Cargo.lock 이 바뀔 때만 바뀌어 main 이 새 산출물을 저장하지 않는다(“Cache up-to-date”).
- **모든 소스를 고정된 옛 시각으로 찍기 / `git-restore-mtime`**: 바뀐 파일도 옛 시각을 받을 수 있어(커밋 시각이
  캐시 빌드보다 이를 수 있다) 오래된 산출물을 신선하다고 판정한다 — 틀린 초록이다. blob 비교는 내용이 같을 때만
  시각을 돌려준다.
- **커밋마다 저장(키에 sha)**: 병합마다 GB 단위 항목이 생겨 상한을 밀어낸다. 날짜 키로 바꿨다.
- **보관본 정리(옛 키 삭제)**: 워크플로 권한은 `contents: read` 뿐이다(`check-workflow-policy.sh`). 삭제 대신
  GitHub 의 LRU 퇴출에 맡기고 위 보고로 보이게 한다. 지난 날의 보관본은 다시 읽히지 않으므로 가장 먼저 퇴출된다.

## Consequences

- 측정(2026-10-12, 버리는 측정 브랜치의 workflow_dispatch, 의존성 캐시는 네 번 모두 같은 키로 적중):

  | rust-quality | 콜드(full debug) | 재사용(full debug) | 콜드(line-tables-only) | 재사용(line-tables-only) |
  |---|---|---|---|---|
  | clippy 작업 공간 | 54.8s | 1.6s | 41.8s | 1.1s |
  | 테스트 빌드 | 2m09s | 0.8s | 1m45s | 0.7s |
  | 정적 툴체인 publisher 빌드 | 53s | 0.3s | 36s | 0.3s |
  | 보관본 복원 | — | 44s | — | 42s |
  | 보관본 크기(zstd) | 3.08GB | | 2.57GB | |

  나머지 잡 시간은 테스트 실행(약 11분)이고 이 ADR 의 범위가 아니다.
- **작업 공간 크레이트는 `line-tables-only` 디버그 정보로 빌드한다**(`CARGO_PROFILE_DEV_DEBUG`, 복원 단계가 잡 환경에
  내보낸다). 역추적의 파일:줄은 남는다. 의존성은 `[profile.dev.package."*"] debug = 1` 이 이기므로 rust-cache 의 키와
  내용은 그대로다. rust-cache 단계 뒤에 내보내므로 그 키 해시에도 들어가지 않는다.
- 보관본 9.8GB(압축 전)의 거의 전부가 실행 파일 198개다. 가장 큰 넷이 publisher 의 바이너리·테스트 하네스를 두
  feature 집합(`--workspace --all-features` 와 패키지 단위)으로 만든 것(각 0.9GB)이다. 실행 파일을 빼면 그 크레이트를
  다시 컴파일하므로(publisher 하네스 2분) 뺄 수 없다. 더 줄이려면 feature 집합을 하나로 만드는 것이 손잡이다.
- 하루에 보관본이 하나(2.6GB) 생긴다. 같은 날 뒤의 main 변경은 다음 날 보관본까지 PR 에서 다시 빌드된다. 크기와
  저장소 사용량은 요약에 매번 나온다. 저장소 캐시는 이 변경 전부터 상한을 넘어 있다(측정 시 9.6–12.0GB): rust-quality 와
  postgres-integration 의 의존성 캐시를 하나로 합치는 것은 기각했다 — verify 는 `--workspace --all-features`,
  통합 레인은 패키지 단위로 빌드해 의존성 feature 집합이 달라, 먼저 저장한 잡이 키를 차지하면 다른 잡이 매번 자기 몫을
  다시 컴파일한다.
- 검사: `scripts/ci/test_workspace_build_cache.py` 가 실제 cargo 작업 공간으로 “안 바뀐 크레이트는 Fresh,
  바뀐 크레이트는 Compiling”을 확인하고, rust-quality 잡이 매번 먼저 돌린다. 시각 복원을 빼거나 blob 비교를 빼면
  실패함을 확인했다. 마이그레이션 디렉터리에 파일을 더하거나 지우면 그 패키지가 다시 빌드되는지도 실제 cargo 로
  확인한다(디렉터리 시각 비교를 빼면 지운 경우가 실패한다). `check-workflow-policy.sh` 는 저장 단계의 조건으로 main 전용만 허용한다(자기 테스트 포함).

## Revisions

- 2026-10-18 (ADR-0269 측정으로 은퇴): 풀 리퀘스트가 고른 크레이트만 검사한 뒤(ADR-0269), 2026-10-17 07:07 부터
  10-18 07:00 까지 Foundation Rust 잡 51개를 쟀다.
  - 전부 검사하는 실행(의존성 캐시 맞음): 보관분을 복원하면 clippy 15–44초·시험 빌드 33–101초, 복원하지
    않으면 44–56초·106–128초다. 그러나 복원 단계가 41–169초(중앙값 약 90초, 3–5 GB 내려받기)를 써서 잡
    전체는 7.4–10.4분 대 9.4–10.7분으로, 얻는 것과 치르는 것이 비슷하다.
  - 크레이트를 0–4개 고른 풀 리퀘스트: 검사는 65–243초인데 복원이 39–163초다 — 고른 것이 없으면 복원은
    순수한 비용이다.
  - 보관분(2.9–5.2 GB)은 무료 캐시 한도 10 GB 의 가장 큰 항목이었고, 머지 큐가 읽는 의존성 캐시를 밀어내
    큐의 Rust 잡을 18–26분으로 만들었다(ADR-0269 Measurements).
  그래서 보관·복원 단계와 `scripts/ci/workspace_build_cache.py` 를 지웠다. 작업 공간 크레이트의
  `CARGO_PROFILE_DEV_DEBUG=line-tables-only`(차가운 빌드 4분 10초 → 3분 4초)는 그 자체로 이득이라 남긴다.
