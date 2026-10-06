# ADR 0155: CI 와 릴리스 빌드는 의존성 빌드를 내용 주소로 재사용하고, 병합은 머지 큐가 판정한다

- Status: Accepted
- Date: 2026-10-06
- Amends: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md) §3 (publisher 이미지의 `--no-cache` → 내용 주소 캐시),
  [ADR-0137](./0137-the-release-build-is-sized-from-a-measurement-and-runs-alone.md) (빌드 작업 수가 실제로 Dockerfile 에 닿는다)

## Context

2026-10-06 하루에 측정한 사실이다.

1. **Foundation Rust 잡이 30분 제한에 닿았다.** PR #345 의 `Rust fmt, unit tests, clippy` 25–26분,
   `Postgres integration tests` 23분, Windows 정적 릴리스 툴체인 12–16분. 이 잡들은 캐시가 하나도 없었다
   (`gh cache list` 에 Foundation 키가 없다). 반면 Swatinem/rust-cache 를 쓰는 Gongzzang·Intelligence·Dawneer
   Rust 잡은 2–5분이었다. 시간의 대부분은 같은 의존성(번들 DuckDB C++, aws-lc, librdkafka)을 매번 다시 컴파일하는
   데 쓰였다.
2. **crates.io 내려받기 실패 두 번이 PR 전체를 떨어뜨렸다**(`curl failed [16] Error in the HTTP2 framing layer`,
   둘 다 Foundation `Supply-chain release gates` 의 `cargo install cargo-deny`). Rust 설치 방법이 다섯 벌이었고,
   dtolnay/rust-toolchain 을 쓰는 잡만 그 액션의 부수효과로 `CARGO_HTTP_MULTIPLEXING=false` 를 받았다.
   rustup 을 직접 부르는 잡(Foundation·Identity·monorepo-guard)은 받지 못했다. 같은 사실(“CI 의 cargo 는
   어떻게 네트워크를 쓰나”)이 다섯 곳에 있었고 그중 둘만 맞았다.
3. **병합 하나마다 열린 PR 전부가 체크 37개를 다시 돌렸다.** 규칙이 `strict_required_status_checks_policy: true`
   (브랜치가 main 과 같아야 병합)였기 때문이다. 같은 검증을 PR 수만큼 반복할 뿐, 병합 결과 자체를 검사하는
   것은 마지막 하나뿐이었다.
4. **릴리스 빌드(`prepare`)가 배포마다 번들 DuckDB 를 처음부터 컴파일한다.** ADR-0137 의 측정에서 2 CPU·22g 로
   2,590초, 메모리 대부분이 `libduckdb-sys` 다. 관리자 빌드는 `--no-cache` 와 임시 빌더로만 돌아 아무것도 남기지
   않았다. 덧붙여 `Dockerfile.lakehouse-control` 에 `ARG CARGO_BUILD_JOBS` 가 없어, 계약이 정한 작업 수가 빌드
   인자로 넘어가도 Dockerfile 은 그것을 읽지 않았다.

GitHub Actions 캐시는 이 저장소에서 10GB·7일(`tools/github/actions-cache-policy.json`, 결제 예산 0)이고, 측정 시점
사용량이 9.6GB 였다. PR 마다 캐시를 쓰면 그 예산이 PR 사본으로 찬다.

## Decision

1. **CI 의 Rust 설치는 `scripts/ci/rust-setup.sh` 하나다.** 루트 `rust-toolchain.toml` 이 고정한 채널을 설치하고,
   잡의 나머지 단계에 `CARGO_NET_RETRY=10`·`CARGO_HTTP_MULTIPLEXING=false`·`CARGO_HTTP_TIMEOUT=60`·sparse 색인·
   `RUSTUP_MAX_RETRIES=10`·`CARGO_INCREMENTAL=0` 과 표지 `PERFECTORY_RUST_SETUP` 을 내보낸다. dtolnay/rust-toolchain
   과 워크플로 안의 `rustup toolchain install` 은 없어졌고 허용 액션 목록에서도 뺐다.
2. **cargo 를 부르는 모든 잡은 그 다음 단계로 `Swatinem/rust-cache` 를 쓰고, main 만 캐시를 쓴다**
   (`save-if: ${{ github.ref == 'refs/heads/main' }}`). 키는 액션 기본값 그대로 Cargo.lock·툴체인·잡·위에서 내보낸
   환경이다. PR 과 머지 그룹은 main 이 쓴 캐시를 읽기만 한다. 의존성 컴파일 결과는 PR 이 아니라 lockfile 의 함수이므로
   PR 사본은 예산만 쓴다.
3. **cargo-deny 는 고정 릴리스 하나를 sha256 으로 확인해 설치한다**(`tools/cargo-deny.env`,
   `scripts/ci/install-cargo-deny.sh`). 매 실행 crates.io 에서 컴파일하던 `cargo install` 이 오늘 실패한 단계다.
4. **필수 워크플로는 모두 `merge_group` 에서도 돈다.** Foundation 범위 선택기는 머지 그룹의 base/head SHA 를 읽는다.
   규칙 정본(`tools/github/main-ruleset.json`)에 `merge_queue`(SQUASH, ALLGREEN, 동시 빌드 3, 병합 3, 응답 제한 60분)를
   넣고 `strict_required_status_checks_policy` 를 `false` 로 바꾼다. 큐가 main + 앞선 PR 들 위의 정확한 squash 결과에서
   필수 컨텍스트를 돌리므로 “브랜치가 최신이어야 한다”를 대신한다. 둘은 함께 바뀌어야 한다
   (`scripts/guard/public-github-policy.sh`). 필수 컨텍스트 12개는 그대로다.
5. **릴리스 publisher 이미지의 의존성 층은 내용 주소 캐시로 재사용한다.**
   - `Dockerfile.lakehouse-control` 은 cargo-chef 로 나눈다: `cargo chef prepare` 가 Cargo.lock 과 매니페스트만으로
     만든 recipe 를 `cargo chef cook --locked --release -p foundation-outbox-publisher` 가 먼저 빌드하고, 그 뒤에
     작업 공간 코드를 복사해 같은 명령으로 최종 바이너리를 만든다. cargo-chef 는 고정 릴리스를 `ADD --checksum` 으로
     받는다(BuildKit 이 다이제스트를 확인한 뒤에야 층이 생긴다). `ARG CARGO_BUILD_JOBS` 가 계약의 작업 수를 cargo 에
     넘긴다.
   - 관리자 빌드는 publisher 에 한해 `--no-cache` 대신 BuildKit 로컬 캐시를 `/var/lib/perfectory/foundation-release-build-cache/publisher`
     에서 가져오고(`--cache-from type=local`) 새로 내보낸다(`--cache-to type=local,mode=max`). 이 형식은 OCI 레이아웃이며
     모든 blob 이 자기 sha256 이름을 가진다.
   - 가져오기 전에 `verified_build_cache` 가 모든 blob 을 다시 해시하고, 링크·특수 파일·다른 주체의 소유/쓰기 권한·
     색인이 가리키는 blob 의 부재를 검사한다. 하나라도 어긋나면 그 캐시를 지우고 **깨끗한 빌드**로 간다. 캐시는 시간만
     아낄 수 있으므로, 손상된 캐시의 대가는 느린 빌드이지 이름 없는 바이트로 만든 릴리스가 아니다.
   - 빌드가 끝나야만 새로 내보낸 캐시가 이전 것을 대체한다. 실패한 빌드는 이전 캐시를 그대로 두고, 중단된 빌드가 남긴
     `.written-*`/`.retired-*` 는 다음 빌드가 잠금 안에서 지운다. 캐시 갱신은 ADR-0137 의 빌드 잠금 안에서만 일어난다.
   - `build.json` 은 가져온 캐시 색인의 sha256 을 `publisher_build_cache_index` 로 기록한다(깨끗한 빌드는 `null`).
     어떤 릴리스가 어떤 보관 층에서 만들어졌는지 감사할 수 있다.
   - ADR-0134 의 불변식은 그대로다: 소스는 정본 main 에서 독립 fetch 한 바이트이고, 최종 바이너리는 그 커밋의
     작업 공간 코드에서 관리자 빌드가 컴파일하며, 산출물 sha256 은 봉인된다. 재사용되는 것은 Cargo.lock 이 정한
     외부 의존성의 컴파일 결과뿐이고, 그 키는 recipe·기반 이미지 다이제스트·apt 고정판·빌드 인자다.
   - tippecanoe 는 2분 빌드라 `--no-cache` 그대로다.
6. **기계적 차단.**
   - `scripts/guard/rust-ci-setup.sh`(+ `-self-test`): cargo·rustup·cargo-deny 를 부르는 잡은 “Set up Rust” 하나와
     그 뒤의 “Rust cache” 하나를 가져야 하고, 다른 설치기·PR 쓰기 캐시·설정 전 cargo 를 거부한다. 자기 테스트는 우회
     11가지를 심고 각각 맞는 이유로 거부되는지 보며, 설정 스크립트를 가짜 rustup 으로 돌려 내보낸 값을 읽는다.
   - `tools/xtask` 는 GitHub Actions 에서 `PERFECTORY_RUST_SETUP` 이 없으면 실행을 거부한다. 스크립트를 거쳐 cargo 에
     닿는 잡은 정적 가드가 보지 못하기 때문이다.
   - `scripts/guard/check-workflow-policy.sh` 는 필터 없는 `merge_group` 을 요구하고, 범위 게이트 조건
     `env.FOUNDATION_CI_GATE_SELECTED == 'true'` 를 “Set up Rust”·“Rust cache” 에만 허용한다(선택되지 않은 잡이
     빈 target 을 진짜 키로 저장하지 못하게).
   - 릴리스 admission 시험은 publisher 만 캐시를 쓰고, 손상된 캐시 넷(blob 변조·blob 부재·링크·쓰기 권한)이 모두
     버려지며, 실패한 빌드가 이전 캐시를 보존함을 확인한다.

### 기각한 대안

- **로컬 composite action(`./.github/actions/rust-setup`)**: 저장소 정책이 로컬 액션을 허용하지 않는다
  (`check-workflow-policy.sh`, 감사된 허용 목록). 같은 SSOT 를 스크립트로 둔다.
- **sccache(GHA 백엔드)**: 컴파일 단위마다 캐시 항목을 만들어 10GB·요청 한도에 먼저 닿고, 빌드 스크립트의 C/C++
  (DuckDB)은 별도 래핑이 필요하다. rust-cache 로 측정해 부족할 때 다시 본다.
- **공식 libduckdb 정적 라이브러리를 받아 링크**: 번들 빌드와 확장 구성이 달라 운영 바이너리의 동작이 바뀔 수 있고,
  `bundled-duckdb` 기능을 끄는 변경이 된다. 의존성 층 캐시는 같은 소스·같은 플래그의 결과를 재사용할 뿐이다.
- **영속 buildx 빌더 + cache mount**: 변경 가능한 target 디렉터리를 빌더 안에 두게 되어 내용 주소 검증이 없고,
  이미지마다 자기 한도를 갖는 임시 빌더(ADR-0137)를 버려야 한다.

## Consequences

- 측정(이 PR 에서 측정, 아래 표는 PR #346 실행에서 채운다)과 운영 규칙 변경 절차는 PR 본문에 있다. 규칙은 이 PR 이
  병합된 뒤 소유자가 `tools/github/main-ruleset.json` 을 그대로 적용한다(`gh api --method PUT repos/<repo>/rulesets/<id>
  --input tools/github/main-ruleset.json`). 적용 전까지 `merge_group` 트리거는 아무 일도 하지 않는다.
- Actions 캐시 항목은 Rust 잡 21개(잡 키)만큼 생긴다. 10GB 를 넘으면 GitHub 이 오래 안 쓴 것부터 지운다 — 실패가
  아니라 그 잡이 콜드로 도는 것이다. 크기는 main 첫 실행 뒤 `gh cache list` 로 보고, 넘치면 같은 것을 빌드하는 잡을
  `shared-key` 로 묶는다.
- 릴리스 빌드 캐시는 ai-server 디스크에 publisher 한 벌(+ 갱신 중 한 벌)을 둔다. 첫 배포는 콜드이고, Cargo.lock 이나
  매니페스트가 바뀌면 그 층부터 다시 빌드한다.
- CI 안에서 Docker 로 Rust 를 빌드하는 경로(boundary slice·compose smoke)는 호스트 캐시와 설정 밖이다. 이 ADR 의
  범위가 아니며, 같은 실패가 그쪽에서 보이면 같은 환경값을 컨테이너에 넘기는 것으로 따로 다룬다.
