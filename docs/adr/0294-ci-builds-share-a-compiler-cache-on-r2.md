# ADR 0294: CI 빌드는 컴파일러 캐시를 R2 에 공유한다 (sccache)

- Status: Accepted
- Date: 2026-10-22
- Amends: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (sccache 를 뒤로 미룬 결정 ①). 공개 저장소 워크플로의 비밀값 금지(`scripts/guard/check-workflow-policy.sh`)에 네 번째 좁은 예외를 더한다.

## Context

ADR-0155(2026-10-06)는 sccache 를 검토하고 **뒤로 미뤘다**: GHA 캐시 백엔드는 컴파일 단위마다 항목을 만들어 10GB·요청 한도에 먼저 닿는다, rust-cache 로 측정해 부족할 때 다시 본다.

그 "부족할 때"가 왔다:

1. `Swatinem/rust-cache` 는 target 을 한 키로 캐시한다. `Cargo.lock`·툴체인·`CARGO_*` 중 하나만 바뀌어도 캐시 전체가 무효가 되고 처음부터 컴파일한다. 2026-10 툴체인 승급(#828, 1.96→1.99)이 모든 rust-cache 를 한 번에 무효화했다 — 변경당 ~1시간.
2. 미룬 이유였던 10GB·요청 한도는 R2(S3 호환, egress 무료)엔 없다. sccache 는 컴파일 단위마다 키를 두므로 바뀐 단위만 다시 컴파일한다.

남은 문제: 공개 저장소라 CI 워크플로는 비밀값을 쓸 수 없는데(위 가드) R2 **쓰기**엔 키가 필요하다. 업계 표준이 이를 푼다 — mozilla/sccache 문서의 `SCCACHE_S3_NO_CREDENTIALS`("PR 용 읽기전용 캐시에 쓰라"), MapLibre·hyperb1iss/hypercolor 의 R2 설정, 그리고 GitHub 이 2026-06 부터 기본값으로 삼은 "믿을 수 없는 트리거는 캐시 읽기 전용": **PR 은 익명으로 읽고, 병합된 코드만 쓴다.**

## Decision

1. sccache 는 핀된 다운로더(루트 ADR-0197, `tools/pinned-artifacts.json` 의 sccache 0.18.0)로 받는다 — raw `curl` 금지. `scripts/ci/rust-setup.sh`(모든 Rust CI 잡의 단일 설치 지점, 루트 ADR-0155)가 `SCCACHE_R2_ENDPOINT` 가 있을 때 sccache 를 켠다.
2. **읽기는 익명.** R2 쓰기 키가 환경에 없으면 `SCCACHE_S3_NO_CREDENTIALS=true` 로 공개-읽기 버킷을 읽는다. 포크 PR 도 캐시를 읽는다(끄지 않는다).
3. **쓰기는 main 만.** R2 쓰기 키는 GitHub 환경 `sccache-writer` 가 `refs/heads/main` 잡에만 준다. `foundation-ci.yml` 의 Rust 잡은 `environment` 이름을 main 에서만 `sccache-writer`, 아니면 빈 값으로 둔다. 병합(검토 통과)된 코드만 캐시를 채운다.
4. **엔드포인트는 공개(비밀값 아님).** 버킷의 계정을 가리킬 뿐 키가 아니므로 평문으로 두어 PR 이 읽을 수 있다.
5. **가드의 네 번째 좁은 예외.** `scripts/guard/check-workflow-policy.sh` 는 `foundation-ci.yml` 에서 정확히 `SCCACHE_R2_ACCESS_KEY_ID`·`SCCACHE_R2_SECRET_ACCESS_KEY` 두 비밀값만 통과시킨다. 다른 이름도, 다른 파일의 이 이름들도 여전히 거부하며, `scripts/guard/workflow-policy-self-test.sh` 가 그 경계를 고정한다(루트 ADR-0001 가드 정책: 실패가 막는 사고 = 공개 CI 에서의 비밀값 유출).
6. `Swatinem/rust-cache` 는 그대로(레지스트리·산출물, 둘은 안 겹친다). DuckDB 의 C/C++ 는 아직 캐시 안 함(`RUSTC_WRAPPER` 는 rustc 만 감싼다). 나쁜/닿지 않는 백엔드는 빌드를 깨지 않는다 — probe 컴파일이 실패하면 캐시 없이 빌드한다.
7. **먼저 `foundation-ci.yml` 하나에만.** 다른 Rust 워크플로는 `rust-setup.sh` 공유로 읽기는 받고, 쓰기 확장은 측정 뒤에 본다.

## Consequences

- 변경 하나에 드는 컴파일이 "거의 처음부터 여러 번"에서 "바뀐 단위만"으로 줄어든다. 전/후 빌드 시간을 `foundation-ci.yml` 의 Rust 잡에서 실측해 효과를 기록한다(ADR-0155 의 "측정해 부족할 때"에 답하는 수치). **배포 전까지 속도 이득은 가설이다.**
- 소유자가 저장소 밖에서 할 일: ① R2 의 `sccache` 버킷을 **공개 읽기**로 켠다. ② GitHub 에 **main 브랜치 보호**를 건 `sccache-writer` 환경을 만들고, 그 버킷만 Object Read & Write 하는 키 2개를 **환경 비밀값**(`SCCACHE_R2_ACCESS_KEY_ID`, `SCCACHE_R2_SECRET_ACCESS_KEY`)으로 넣는다. ③ `foundation-ci.yml` 의 평문 `SCCACHE_R2_ENDPOINT` 를 버킷의 S3 API 엔드포인트로 채운다(비면 sccache 는 꺼진 채).
- 보안: 쓰기 키는 환경이 main 잡에만 주므로 PR·포크엔 전달되지 않는다. 가드 예외는 두 이름·한 파일로 좁고 자가시험으로 지켜진다. 공개되는 엔드포인트는 계정 식별자일 뿐 자격증명이 아니다. 캐시에는 공개 코드의 컴파일 산출물만 담긴다.
- 비용: R2 저장 몇 GB(LRU) + egress 무료 → CI 돈 0 원칙 유지.
