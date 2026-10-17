# ADR 0269: 풀 리퀘스트는 바뀐 크레이트와 그 역의존만 검사하고, 머지 큐와 야간 실행이 전부를 검사한다

- Status: Accepted
- Date: 2026-10-17
- Builds on: [ADR-0202](./0202-a-pull-request-runs-only-the-areas-it-changes.md) (영역 범위), [ADR-0205](./0205-verify-runs-rust-tests-under-pinned-cargo-nextest-inside-the-job.md) (nextest), [ADR-0201](./0201-ci-keeps-the-workspace-crates-main-built-and-restores-the-source-times-they-saw.md) (main 이 지은 작업 공간 빌드), [ADR-0203](./0203-pull-request-ci-has-one-wall-clock-budget.md) (PR 벽시계 예산)

## Context

ADR-0202 는 풀 리퀘스트가 바꾼 **영역**만 돌린다. 영역 안에서는 늘 전부였다. Foundation 작업 공간은
멤버 29개이고, `foundation-api` 한 줄을 고친 풀 리퀘스트도 29개 전부를 clippy·시험하고 Postgres 레인의
직렬 시험 전부(약 7.8분)를 돌렸다.

2026-10-16 main 에 병합된 풀 리퀘스트 5개를 쟀다(`gh api …/actions/runs`, 첫 `pull_request` 실행 시작 →
마지막 종료, 머지 큐는 같은 PR 의 `merge_group` 실행).

| PR | 바꾼 것 | PR 벽시계 | 머지 큐 | 합 |
|---|---|---|---|---|
| #662 | 잠금 파일(zip 9) | 19.9분 | 19.1분 | 39.0분 |
| #660 | 크레이트 3개 | 9.6분 | 12.1분 | 21.7분 |
| #658 | 파이썬·셸만 | 10.1분 | 11.6분 | 21.7분 |
| #657 | `foundation-outbox` 하나 | 25.3분 | 23.7분 | 49.0분 |
| #656 | 파이썬·셸만 | 18.4분 | 20.9분 | 39.3분 |

임계 경로는 늘 Foundation `Rust fmt, unit tests, clippy` 잡(9.3–25.1분)과 `Postgres integration tests`
잡(8.0–19.3분)이었다. #658·#656 은 Rust 를 한 줄도 바꾸지 않았는데 두 잡을 다 돌렸다.

검토한 사례(1차 자료, 2026-10-17 다시 읽음):

- **oxidecomputer/omicron** — `.config/nextest.toml` 의 `profile.ci`(`fail-fast = false`, `retries = 2`,
  `flaky-result = "fail"`, JUnit), `test-groups`, setup 스크립트, `rdeps()` 필터; `.config/hakari.toml` 과
  `workspace-hack/`; omicron 의 hakari 워크플로우(`hakari.yml`)가 도는 `cargo hakari generate --diff` 와
  `cargo hakari manage-deps --dry-run`. 바뀐 파일로 크레이트를 고르지는 않는다.
- **risingwavelabs/risingwave** — `.config/nextest.toml` 의 `profile.ci`(`retries = 0`,
  `fail-fast = false`, JUnit), `.config/hakari.toml`, `ci/workflows/pull-request.yml`. 역시 크레이트 선택은 없다.
- **dfinity/ic** — `ci/scripts/targets.py`: 풀 리퀘스트는 `git diff --merge-base` 로 바뀐 파일의 Bazel
  `rdeps` 만 시험하고, `ALL_TARGETS_BLOBS`(`.github/*`, `*.bazel` 등)가 바뀌면 전부를 고른다. 머지 큐와
  야간 실행은 전부를 돈다. 선언되지 않은 입력은 `PULL_REQUEST_BAZEL_TARGETS` 에 손으로 적는다.

우리에게는 Bazel 이 없다(ADR-0202 기각 그대로). `cargo metadata` 가 크레이트 그래프를 주고,
nextest 의 `rdeps()` 가 같은 그래프로 시험을 고른다. 남는 문제는 크레이트 디렉터리 밖의 입력이다.

## Decision

1. **목표(고정).** 크레이트 하나를 바꾼 풀 리퀘스트의 PR CI 는 10분 이내, 머지 큐까지 20분 이내.
   실제 풀 리퀘스트 5개의 전후 수치로만 달성을 말한다. PR 벽시계 예산 숫자는 그대로
   [`tools/ci/ci-time-budget.contract.json`](../../tools/ci/ci-time-budget.contract.json) 하나다.
2. **선택기는 하나다.** 새 범위 체계를 만들지 않는다. 영역 범위의 정본
   [`tools/ci/change-scope.json`](../../tools/ci/change-scope.json) 에 `crates` 절을 더하고, 같은
   `scripts/ci/change_scope.py` 가 `crates --workspace <dir>` 로 답한다. 비교(`CHANGE_SCOPE_EVENT`·
   `_BASE`·`_HEAD`, 머지 베이스 기준 `base...head`)도 영역 범위와 같은 함수다. 파이썬·Spark·웹은 지금처럼
   영역 범위(ADR-0202)와 Foundation 게이트 선택기를 따른다.
3. **바뀐 경로 → 크레이트.**
   - 멤버 디렉터리 안의 경로는 그 멤버(가장 깊은 것)를 고른다.
   - 멤버의 Rust 소스가 따옴표로 부르는 경로(ADR-0202 §6 의 읽기 스캐너; 저장소 루트·영역 루트·소스
     디렉터리·**그 크레이트 디렉터리** 기준 — `migrate!("../../migrations")`·`CARGO_MANIFEST_DIR`)나 그
     위 디렉터리가 바뀌면 그 멤버를 고른다. 따옴표 안의 낱말이 멤버를 담은 작업 공간 최상위 폴더
     (`"crates"`, `"services"`)이면 그 폴더 전체를 읽는다: 모든 크레이트의 소스를 걷는 시험
     (`v1_manifest_promotion_has_one_caller` 같은)이 여기 걸린다. 지운 파일도 그것을 부르는 소스로 찾는다.
   - 고른 멤버와 그 **역의존 전부**(normal·build·dev 의존)를 검사한다 — nextest `rdeps()`.
     단, 다른 크레이트의 `.rs`·`Cargo.toml` 을 텍스트로 읽는 시험의 크레이트는 자기 시험만 돈다
     (`package()`): 그 빌드는 바뀌지 않는다.
   - 어느 멤버도 담지도 읽지도 않는 경로는 Rust 입력이 아니다.
4. **전부 돌리는 입력.** `crates.run_all`(잠금 파일, 툴체인, `.cargo/config.toml`, `tools/xtask/**`,
   `.github/workflows/**`, nextest 핀, 고정 바이너리 목록, `rust-setup.sh`, 작업 공간 빌드 캐시),
   `crates.run_all_in_workspace`(작업 공간 `Cargo.toml`, `.config/**`, `clippy.toml`, `rustfmt.toml`),
   선택기와 계약 파일 자신, 멤버 밖의 Rust 소스. workspace-hack 크레이트는 모든 멤버가 의존하므로
   역의존만으로 전부가 된다.
5. **이벤트.** `pull_request` 만 좁힌다. main 푸시는 다음 풀 리퀘스트가 복원할 빌드 캐시(ADR-0201)를
   채우므로 전부, 머지 큐·`workflow_dispatch`·야간 `schedule` 도 전부다(dfinity/ic 와 같다). 크레이트를
   좁히는 워크플로우 6개(dawneer·foundation·gongzzang·gongzzang-db-migrations·identity·intelligence)는
   매일 17:41 UTC 에 main 에서 전부를 돈다. 비교할 수 없으면 전부다.
6. **한 명령.** `cargo xtask verify <area> --affected` 와 `cargo xtask integration <area> [lane] --affected`
   가 선택기에 묻는다. 고른 것이 있으면 fmt 는 그대로 전부, clippy 와 시험은 `-p <affected>…`, nextest 는
   `-E 'rdeps(=<changed>) | package(=<reader>)'` 와 `--no-tests=warn`(고른 크레이트에 시험이 없을 수
   있다), 라이브 레인은 패키지가 고른 집합에 든 대상만 돈다. 고른 것이 없으면 clippy·시험은 그렇다고
   찍고 끝난다. 선택기가 답하지 못하면 실패다 — 조용히 좁히지도 넓히지도 않는다. `--affected` 가 없으면
   지금처럼 전부다(로컬 하네스, ADR-0004).
7. **같은 프로그램의 다음 단계**(각각 별도 풀 리퀘스트, 이 ADR 에 날짜 붙은 개정으로 결과를 적는다):
   작업 공간마다 `.config/nextest.toml` 의 `ci` 프로필, cargo-hakari 와 workspace-hack 크레이트 및
   `cargo hakari generate --diff` 검사(판은 ADR-0097 의 한 버전 계약), 크레이트별 빌드 시간 측정과
   느린 크레이트의 CI 요약 출력.

증명: `scripts/ci/test_change_scope.py` 의 `CratesTest`(크레이트 A 를 바꾸면 A 와 역의존만, 전부 돌리는
입력이면 전부, 풀 리퀘스트가 아닌 이벤트는 전부, 읽는 크레이트, 모든 크레이트를 걷는 시험, 지운 파일,
낡은 `run_all` 거부)와 xtask 의 `the_crate_selection_is_read_exactly`·
`an_affected_selection_narrows_clippy_and_the_tests`·`an_affected_feature_gated_package_keeps_its_own_stage`.

2026-10-16 까지 병합된 풀 리퀘스트 30개에 선택기를 돌린 결과(Foundation 29개 중): 전부(잠금 파일·
워크플로우·범위 정의) 4건, 0개 12건(다른 영역만·파이썬·셸·문서·`deny.toml`), 1–5개 6건, 7–19개 8건.

## Consequences

- 선택기가 보지 못하는 입력이 있다: 동적으로 조립한 경로(`format!("{root}/{name}.json")`), 시험이 부르는
  스크립트가 다시 읽는 파일. 그런 누락은 머지 큐(전부)에서 빨갛게 드러나고 main 에 닿지 않는다.
  드러나면 그 경로를 따옴표로 부르게 고치거나 `crates.run_all` 에 적는다.
- Foundation 의 무거운 게이트(kafka·compose-smoke·boundary-slice·spark)는 지금처럼
  `foundation_ci_scope.py` 가 경로로 고른다. 이 결정은 Rust 크레이트 검사만 바꾼다.
- 크레이트를 좁히는 잡은 머지 베이스 비교를 위해 전체 이력(`fetch-depth: 0`, `filter: blob:none`)을 받는다.
- 효과는 이 변경 뒤 실제 풀 리퀘스트 5개로 재서 이 ADR 에 날짜 붙은 개정으로 적는다.

## Rejected alternatives

- **Bazel/Buck2 로 이전**: ADR-0202 와 같은 이유(규모 대비 비용)로 기각. ic 의 보장(역의존 시험, 머지
  큐 전부)은 `cargo metadata` 와 nextest 로 얻는다.
- **ic 의 `PULL_REQUEST_BAZEL_TARGETS` 같은 손으로 쓴 목록**: 읽는 크레이트는 소스에서 찾는다. 손 목록은
  `run_all` 하나만 둔다.
- **영역 범위와 따로 크레이트 범위 파일**: 같은 경로 판단을 두 곳에 두게 된다.
- **고른 크레이트를 잡 여러 개로 나누기**: ADR-0205 대로 병목은 동시 실행 자리다.

## Revisions

- 2026-10-17 (§7 nextest): 작업 공간 여섯 곳(gongzzang·foundation·identity·intelligence·dawneer·xtask)의
  `.config/nextest.toml` 에 omicron 의 `profile.ci`(`retries = 2`, `flaky-result = "fail"`,
  `failure-output = "immediate-final"`)를 두고, `scripts/ci/rust-setup.sh` 가 모든 CI 잡에
  `NEXTEST_PROFILE=ci` 를 건다. 실패한 시험은 두 번 더 돌아 흔들리는 시험이면 `FLAKY` 로 이름이 찍히고,
  여전히 실패다. `tools/cargo-nextest.env` 는 그대로 남는다: 판과 체크섬은 내려받기 도구의 입력이고,
  `nextest-version` 을 설정에 두면 같은 판이 두 곳에 산다. `--no-fail-fast` 는 로컬에서도 첫 실패에서
  멈추지 않는 규칙(ADR-0012 1항)이라 xtask 에 남는다. JUnit 출력은 읽는 곳이 없어 두지 않는다.
  시험: xtask `every_verified_workspace_defines_the_nextest_ci_profile`(프로필이 없는 작업 공간은
  nextest 가 시작을 거부한다), `rust-ci-setup-self-test.sh`.
- 2026-10-17 (스위트): `foundation-api` 한 줄을 바꾼 측정용 풀 리퀘스트(#670, 병합하지 않음)의 벽시계는
  11.8분이었다. Rust 잡은 4.6분(바뀐 1 + 읽는 2 크레이트, 시험 420개 44초), Postgres 레인은 2.3분이었고,
  임계 경로는 Rust 를 하나도 읽지 않는 Foundation 의 파이썬·노드 스위트(8.4분)와 러너 대기(약 3분)였다.
  그래서 같은 선택기의 `suites --workspace <dir>` 가 스위트도 고른다: 풀 리퀘스트의 바뀐 경로가 전부
  Rust 크레이트 안이거나 다른 영역이고, 그 영역의 Rust 가 아닌 소스(.py·.ts·.js·.sh …)가 어느 것도
  따옴표로 부르지 않을 때에만 `cargo xtask verify <area> --part suites --affected`(와 전체 verify)가
  스위트를 건너뛴다. 그 밖의 경로, 전부 돌리는 입력, 풀 리퀘스트가 아닌 이벤트는 그대로 돈다. 병합된
  풀 리퀘스트 30개 재현에서 Foundation 스위트를 건너뛰는 것은 9건(Rust 만 3건, 다른 영역만 6건)이다.
  시험: `SuitesTest`(심은 결함 — 스위트가 읽는 파일을 무시 — 이 실패), xtask
  `the_suite_selection_is_read_exactly`.
- 2026-10-17 (§7 빌드 시간): `cargo xtask verify` 의 clippy 와 nextest 빌드가 `--timings` 로 크레이트별
  보고서를 쓰고, 끝에서 `scripts/ci/cargo_timings.py summarize` 가 그 실행이 쓴 보고서만 읽어 가장 오래
  걸린 크레이트 15개(단위 시간의 합)를 CI 요약(`$GITHUB_STEP_SUMMARY`)과 로그에 표로 찍는다. omicron 의
  빌드 분석처럼 보고일 뿐 판정이 아니다 — 보고서가 없으면 이유를 찍고 0 으로 끝난다. 시험:
  `scripts/ci/test_cargo_timings.py`(`required/repository` 가 돌린다), xtask 의 clippy 명령 단언.
- 2026-10-17 (§7 hakari): 멤버가 둘 이상인 작업 공간 넷(foundation·gongzzang·identity·intelligence)에
  `crates/<area>-workspace-hack` 과 `.config/hakari.toml`(omicron 의 `dep-format-version = "4"`,
  `workspace-hack-line-style = "workspace-dotted"`, 리눅스·윈도 플랫폼)을 둔다. dawneer(멤버 하나)와
  xtask(의존성 없음)는 묶을 것이 없다. `cargo xtask verify <area>` 가 omicron 의 두 검사
  (`cargo hakari generate --diff`, `cargo hakari manage-deps --dry-run`)를 돌린다 — 의존성을 바꾼 뒤
  `cargo hakari generate && cargo hakari manage-deps` 를 잊으면 실패다. 판과 체크섬은
  `tools/cargo-hakari.env`(GitHub 이 공개한 자산 다이제스트와 같다)와 `tools/pinned-artifacts.json`.
  - hakari 는 `cargo build --workspace --all-targets` 의 기능 집합을 모든 멤버에 묶으므로, 운영
    이미지(`cargo build --release -p <서비스>`)의 의존성 기능도 바뀐다. 운영 바이너리 10개(foundation
    2·identity 4·gongzzang 2·intelligence 2)의 `cargo tree -e normal,build --target
    x86_64-unknown-linux-gnu` 를 main 과 비교해, 동작이 바뀌는 것은 묶음에서 뺐다(`final-excludes`):
    rustls 와 그 백엔드(`ring`·`aws-lc-rs` 가 둘 다 켜지면 rustls 가 기본 공급자를 고르지 못한다),
    hyper·hyper-util·h2(hyper-util 의 자동 연결 빌더는 `http2` 가 컴파일되면 HTTP/2 서두를 보낸 누구에게나
    평문 HTTP/2 를 열어 준다 — axum 서버 셋이 지금 거부하는 프로토콜을 받게 된다), DuckDB 번들 빌드,
    intelligence 의 한국어 사전(`build_rs` 가 받아 푼다).
  - 남는 변화는 순수 Rust 크레이트의 기능 추가뿐이다: tokio(`macros`·`signal`·`process` 등, gongzzang
    두 바이너리에 개발 의존성의 `test-util`), regex-automata 의 유니코드 기능, futures 의 async-await.
    hakari 에는 개발 의존성을 빼는 설정이 없다(omicron·RisingWave 도 같은 값을 치른다).
  - 시험: xtask `every_multi_member_workspace_keeps_a_workspace_hack`; 낡은 workspace-hack(한 줄
    지움)은 `generate --diff` 가, 의존 줄이 빠진 멤버는 `manage-deps --dry-run` 이 종료 코드 1 로 거부했다.
