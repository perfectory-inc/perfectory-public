# ADR 0294: CI 빌드는 컴파일러 캐시를 R2 에 공유한다 (sccache)

- Status: Accepted
- Date: 2026-10-22
- Amends: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (sccache 를 뒤로 미룬 결정 ①). 공개 저장소 워크플로의 비밀값 금지(`scripts/guard/check-workflow-policy.sh`)에 네 번째 좁은 예외를 더한다.

## Context

ADR-0155(2026-10-06)는 sccache 를 뒤로 미뤘다: GHA 캐시 백엔드는 10GB·요청 한도에 먼저 닿는다, rust-cache 로 측정해 부족할 때 다시 본다. 그 "부족할 때"가 왔다 —

1. `Swatinem/rust-cache` 는 target 을 한 키로 캐시한다. `Cargo.lock`·툴체인·`CARGO_*` 중 하나만 바뀌어도 캐시 전체가 무효가 되고 처음부터 컴파일한다. sccache 는 컴파일 단위마다 키를 두므로 입력이 바뀐 단위만 다시 컴파일한다.
2. **실측(2026-10-22):** main 전체 CI 의 가장 긴 작업은 Rust 컴파일 17.5분이고, PR 검사 + 병합큐 재검사 + 릴리스 빌드(~31분)가 줄줄이 더해져 변경 하나가 배포까지 ~1시간이며 그 ~48분이 컴파일이다.

R2(S3 호환, egress 무료)엔 GHA 의 10GB 한도가 없다. 단 **R2 의 S3 API 는 익명 요청을 400(InvalidArgument/"Authorization")으로 거부한다**(2026-10-22 실측). 공개 버킷의 익명 읽기는 r2.dev 웹 URL 로만 되고 그건 sccache 가 쓰는 S3 경로가 아니다. 따라서 **읽기에도 키가 필요하다.**

## Decision

1. sccache 는 핀된 다운로더(루트 ADR-0197, `tools/pinned-artifacts.json` 의 sccache 0.18.0)로 받는다 — raw `curl` 금지. 받는 일은 **전용 스텝 `scripts/ci/sccache-setup.sh`** 가 하고, `scripts/ci/rust-setup.sh`(모든 Rust CI 잡의 단일 설치 지점, 루트 ADR-0155) 는 건드리지 않는다 — rust-setup 이 다운로더를 품으면 모든 Rust 잡이 핀 캐시 복원을 강요당하기 때문(`scripts/guard/check-pinned-downloads.py`). 그래서 **캐시를 쓰는 잡만** 이 스텝을 돌리고, 그 잡만 복원 스텝을 진다. 버킷은 `perfectory-sccache`, 키 접두사는 툴체인 채널(바뀌면 새 네임스페이스).
2. **(선택지 A) 하나의 읽기·쓰기 R2 키를 저장소 비밀값으로 두고 캐시를 쓰는 Rust 잡이 읽고 쓴다.** 같은 저장소 브랜치의 PR 과 main 둘 다 캐시를 읽고 채운다. 포크 PR 은 비밀값을 못 받으므로 sccache 없이 예전처럼 빌드한다. 엔드포인트도 비밀값이다 — 키라서가 아니라, `scripts/guard/public-repository-safety.sh` 가 공개 코드에 계정별 R2 바인딩(계정 식별자가 든 평문 엔드포인트)을 금지하기 때문.
3. **가드의 네 번째 좁은 예외.** `scripts/guard/check-workflow-policy.sh` 는 `foundation-ci.yml` 에서 정확히 `SCCACHE_R2_ENDPOINT`·`SCCACHE_R2_ACCESS_KEY_ID`·`SCCACHE_R2_SECRET_ACCESS_KEY` 세 비밀값만 통과시킨다. 다른 이름도, 다른 파일의 이 이름들도 여전히 거부하며 `scripts/guard/workflow-policy-self-test.sh` 가 그 경계를 고정한다(루트 ADR-0001 가드 정책: 실패가 막는 사고 = 공개 CI 에서의 비밀값 유출).
4. `Swatinem/rust-cache` 는 그대로(레지스트리·산출물, 둘은 안 겹친다). DuckDB 의 C/C++ 는 아직 캐시 안 함(`RUSTC_WRAPPER` 는 rustc 만 감싼다). 나쁜/닿지 않는 백엔드는 빌드를 깨지 않는다 — probe 컴파일이 실패하면 캐시 없이 빌드한다.
5. **먼저 `foundation-ci.yml` 하나에만.** 다른 Rust 워크플로는 `rust-setup.sh` 공유로 켤 수 있으나 측정 뒤에 본다.

## Consequences

- 입력이 바뀐 단위만 다시 컴파일된다. 전/후 빌드 시간을 실측해 효과를 기록한다 — **배포 전까지 속도 이득은 가설이다.**
- **받아들인 위험(선택지 A):** 읽기·쓰기 키가 같은 저장소 PR 전부에 보이므로, 협력자(또는 탈취된 협력자 계정)가 캐시에 오염된 산출물을 심으면 그것이 릴리스 빌드에 섞일 수 있다(캐시 포이즈닝). 소수 신뢰 팀이라 감수한다. **업그레이드 경로:** 팀이 커지거나 위험이 오르면 PR 엔 읽기 전용 키, main 전용 GitHub 환경에만 쓰기 키(선택지 B)로 바꾼다 — 가드 예외는 그대로 쓰인다.
- 소유자가 저장소 밖에서 한 일: R2 `perfectory-sccache` 버킷 + 읽기·쓰기 R2 토큰, 저장소 비밀값 `SCCACHE_R2_ENDPOINT`·`SCCACHE_R2_ACCESS_KEY_ID`·`SCCACHE_R2_SECRET_ACCESS_KEY` 등록(이미 완료). 엔드포인트도 평문이 아니라 비밀값으로 둔다(계정 식별자 노출 금지).
- 비용: R2 저장 몇 GB(LRU) + egress 무료 → CI 돈 0 원칙 유지.
