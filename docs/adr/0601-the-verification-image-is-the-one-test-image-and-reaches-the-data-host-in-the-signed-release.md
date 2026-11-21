# ADR 0601: 검증 이미지가 하나뿐인 시험 이미지이고, 서명된 릴리스에 실려 데이터 호스트에 간다

- Status: Accepted
- Date: 2026-11-21
- Builds on: ADR-0004(검증 SSOT `cargo xtask verify`), ADR-0197(고정 다운로더 하나), ADR-0276(CI 가 짓고 서명한 릴리스를 호스트가 digest 로 받는다), ADR-0294(sccache), ADR-0198(쓰지 않는 이미지 정리)

## Context

2026-11-21 코디네이터가 ai-server 에서 작업자들의 크레이트 시험용 이미지 `perfectory-rust-test:20261121b` 를 손으로 지었다
(`/data/foundation-platform/claude-scratch/shared-test-image/Dockerfile`, `Dockerfile.b`). 작업자들이 맨 `rust` 이미지에서
매번 apt 를 깔고(서버에서 약 8분, `build.log` 492 s) 콜드 컴파일을 했기 때문이다. 그런데 그 정의는 저장소 밖의 두 번째 시험
환경이었다.

| 값 | 손으로 지은 이미지 | CI 가 읽는 곳 |
|---|---|---|
| 기반 | `rust:1.99.1-bookworm`(digest 없음) | `tools/container-images.env` `RUST_TOOLCHAIN_IMAGE`(digest 고정) |
| apt | 자기 목록(clang 더함, `python3-pytest` 빠짐) | `tools/xtask/src/main.rs` `apt_deps` → `scripts/ci/apt-install.sh` |
| 도구체인 구성요소 | `rustup component add clippy rustfmt` | `rust-toolchain.toml` |
| sccache | 버전·SHA-256 을 Dockerfile 에 다시 적음 | `tools/sccache.env` → `scripts/ci/fetch_pinned.py` |

저장소에는 이미 CI 와 같은 리눅스 환경을 정의한 이미지가 있다: `tools/verify-image/Dockerfile`(`cargo-verify.sh` 가 쓴다). 기반
digest 는 `technology-version-consistency` 가 `container-images.env` 와 같은지 보고, 도구체인은 `rust-toolchain.toml` 을 복사해
`rustup toolchain install` 로 깐다. 빠진 것은 sccache 와, 그 이미지를 호스트에 올리는 길이었다.

호스트에 이미지를 올리는 저장소의 길은 하나다(ADR-0276): CI 의 `foundation-release-build.yml` 이 짓고 서명해 GHCR 에 올리고,
배포의 release admission 이 digest 로 받아 서명을 확인한 뒤 태그를 붙인다. 스스로 시작하지 않는 이미지도 이 길로 간다
(`intelligence-document-text`, `compose.document-jobs.yml` 의 `build-only`).

## Decision

1. **시험 이미지는 검증 이미지 하나.** `tools/verify-image/Dockerfile` 이 노트북의 `cargo-verify.sh`, 머지 큐의 릴리스 드라이런(`foundation_ci_scope.py` `RELEASE_EVENTS`; PR 에서는 돌지 않는다),
   데이터 호스트의 크레이트 시험이 모두 쓰는 정의다. 다른 시험 이미지 정의를 두지 않는다.
2. **값은 CI 의 파일에서만.** 기반 digest 는 `container-images.env` 와 같아야 하고(기존 검사), apt 목록은 xtask `apt_deps` 합집합과
   같아야 하며, sccache 는 `fetch_pinned.py tool sccache` 로 받는다(`tools/sccache.env` 가 버전과 SHA-256 의 유일한 집).
   `technology-version-consistency` 가 apt 목록이 다르거나 `sccache.env` 의 값이 Dockerfile 에 다시 적히면 실패한다.
3. **RUSTC_WRAPPER 는 이미지에 두지 않는다.** 이미지는 sccache 실행 파일과 `SCCACHE_DIR=/sccache`, `SCCACHE_CACHE_SIZE=20G` 만
   싣는다. `cargo-verify.sh` 는 지금처럼 증분 빌드로 돌고, 호스트 시험은 `-e RUSTC_WRAPPER=sccache -e CARGO_INCREMENTAL=0
   -v perfectory-sccache:/sccache` 로 켠다(`CARGO_INCREMENTAL=0` 은 CI 의 `scripts/ci/rust-setup.sh` 값, sccache 는 증분 단위를
   캐시하지 못한다). 호스트 캐시는 로컬 볼륨 하나다: CI 의 R2 캐시(ADR-0294)는 쓰기 키가 저장소 비밀에만 있고, 호스트에 그 키를
   두지 않는다. 컨테이너마다 sccache 가 20G 상한을 따로 세므로 동시 실행이 잠시 넘을 수 있고, 다음 실행이 줄인다.
4. **호스트에는 릴리스로.** 저장소 루트의 `compose.verify-image.yml`(`build-only`, 이미지 `perfectory-verify:local`)을 릴리스 빌드의
   `COMPOSE_FILES` 에 넣고, admission 의 `OPTIONAL_COMPOSE_IMAGES` 에 `perfectory-verify` 를 더한다. 배포의 `tag-compose-images` 가
   서명된 이미지에 `perfectory-verify:local` 을 붙이고, 그 태그를 제어 체크아웃이 이름하므로 ADR-0198 정리가 지우지 않는다.
   compose 정책이 빌드 컨텍스트를 compose 파일 아래로 요구하고 이미지가 루트의 `rust-toolchain.toml` 을 복사하므로 파일은 루트에
   둔다. 릴리스 범위(`tools/ci/change-scope.json`)에 이미지의 입력(`tools/verify-image/**`, `.dockerignore`, 그 compose 파일,
   `scripts/ci/actionlint.sh`)을 더하고, 머지 큐의 드라이런이 그 변경에서도 돌도록 `foundation-ci.yml` 범위에도 같은 넷을 더한다.
5. **층 순서.** 자주 바뀌는 Node 저장소(profile gateway 잠금 파일)를 맨 뒤로 옮긴다. 전에는 apt 위에 있어 잠금 파일 하나가 모든
   층을 다시 짓게 했다.

## 버린 대안

- **호스트에서 작업자 체크아웃으로 `docker build`.** 그 브랜치와 늘 맞지만, 호스트가 짓는 것은 ADR-0276 단계 R3(호스트는 서명된
  것만 설치한다)에 어긋나고, 작업자마다 같은 3 GB 를 다시 짓는다(apt 만 서버에서 492 s, 2026-11-21 `build.log`). 이미지 입력을
  바꾸는 브랜치만 예외로 임시 태그를 짓는다(런북).
- **CI 가 따로 GHCR 에 올리는 워크플로.** 서명·digest 확인·태그·정리의 두 번째 길이 된다. 릴리스 길에는 그것이 이미 있다.

## Consequences

- 손으로 지은 `perfectory-rust-test:*` 와 scratch Dockerfile 은 첫 릴리스 이미지가 호스트에 태그된 뒤 지운다.
- 릴리스마다 이 이미지를 BuildKit 레지스트리 캐시(`buildcache-perfectory-verify`)로 짓는다. 입력이 같으면 층이 캐시에서 오고
  digest 가 같아 GHCR 에 새 층이 올라가지 않고 호스트도 층을 다시 받지 않는다. 돈은 GitHub 공개 저장소 러너와 GHCR 라 0 원이다.
- 대가 둘. (a) 입력이 바뀐 릴리스(도구체인·apt·sccache·Node 잠금 파일)는 이 이미지를 다시 짓고, 호스트는 바뀐 층(최대 약
  0.7 GB 압축)을 받는다. (b) 이 이미지의 빌드가 실패하면(apt·rustup·sccache 다운로드) 그 커밋의 Foundation 릴리스가 실패한다.
  닫힌 쪽으로 실패하는 것이 의도다: 호스트의 시험 환경이 main 과 다른 채로 배포가 나아가면 이 ADR 이 없앤 두 번째 정의가 다시
  생긴다. 다운로드 실패는 기존 릴리스 이미지들과 같이 작업 재실행으로 푼다.
- `perfectory-verify:local` 은 배포된 main 을 따른다. 이미지 입력을 바꾸는 브랜치는 그 환경을 런북대로 임시 태그로 짓거나 머지 큐
  드라이런에 맡긴다.
- 이 PR 은 `.dockerignore` 가 `tools/pinned-artifacts.json`·`scripts/ci/fetch_pinned.py` 를 컨텍스트에서 빼던 것도 고친다.
  Dockerfile 이 둘을 복사하므로 `cargo-verify.sh` 의 이미지 빌드가 실패했을 것이다.
- CI 작업이 이 이미지를 `container:` 로 쓰는 것은 하지 않는다. 약 3 GB(압축 약 0.7 GB) 이미지를 작업마다 받아야 하고, 지금의
  apt 캐시와 `rust-setup.sh` 는 1–2분이며, `Swatinem/rust-cache` 경로와 sudo 전제가 달라진다.
