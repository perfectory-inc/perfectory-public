# ADR 0293: CI 빌드는 컴파일러 캐시를 R2 에 공유한다 (sccache)

- Status: Accepted
- Date: 2026-10-22
- Amends: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (sccache 를 뒤로 미룬 결정 ①)

## Context

ADR-0155(2026-10-06)는 sccache 를 검토하고 **뒤로 미뤘다**: "sccache(GHA 백엔드)는 컴파일 단위마다 캐시 항목을 만들어 10GB·요청 한도에 먼저 닿고, 빌드 스크립트의 C/C++(DuckDB)은 별도 래핑이 필요하다. rust-cache 로 측정해 부족할 때 다시 본다."

그 "부족할 때"가 왔다:

1. **`Swatinem/rust-cache` 는 target 디렉터리 전체를 한 키로 캐시한다.** `Cargo.lock`·툴체인·`CARGO_*` 가 키에 들어가서, 그 중 하나만 바뀌면 캐시 전체가 무효가 되고 처음부터 다시 컴파일한다. 2026-10 툴체인 승급(#828, 1.96→1.99)이 모든 rust-cache 를 한 번에 무효화했다. 변경 하나가 PR 검사·병합 큐 재검사·릴리스 빌드(~31분)에서 **거의 처음부터, 여러 번** 컴파일돼 변경당 ~1시간이 든다.
2. **ADR-0155 가 미룬 이유는 GHA 캐시 백엔드의 10GB·요청 한도였다.** R2(S3 호환 API)는 그 한도가 없고 egress 가 무료다. 즉 **미뤘던 바로 그 이유가 R2 로 사라진다.** sccache 는 컴파일 단위(크레이트의 코드젠 단위)마다 키를 두므로, 툴체인이 바뀌어도 바뀐 단위만 다시 컴파일하고 나머지는 R2 에서 그대로 꺼낸다 — rust-cache 의 "전부 아니면 전무"와 다르다.

## Decision

1. **CI 의 Rust 컴파일은 sccache 를 거쳐 R2 에 캐시를 공유한다.** `scripts/ci/rust-setup.sh`(모든 Rust CI 잡의 단일 설치 지점, 루트 ADR-0155)가, R2 자격증명이 환경에 있을 때만, 핀된 **sccache v0.18.0**(발행자의 `.sha256` 로 내려받은 즉시 검증)을 설치하고 `RUSTC_WRAPPER=sccache` 와 S3 백엔드 설정을 `GITHUB_ENV` 로 내보낸다. 버킷은 `sccache`, 엔드포인트·키는 저장소 비밀값(`SCCACHE_R2_ENDPOINT`, `SCCACHE_R2_ACCESS_KEY_ID`, `SCCACHE_R2_SECRET_ACCESS_KEY`)에서 온다.
2. **`Swatinem/rust-cache` 는 그대로 둔다.** 레지스트리·인덱스·sccache 가 캐시하지 않는 것을 여전히 캐시하고, 자격증명이 없는 포크 PR 의 유일한 캐시다. 둘은 겹치지 않는다: rust-cache 는 다운로드·산출물, sccache 는 컴파일러 호출.
3. **포크 PR 은 비밀값을 받지 못하므로 sccache 가 꺼진 채 예전처럼 빌드한다.** `rust-setup.sh` 는 자격증명이 없으면 `RUSTC_WRAPPER` 를 설정하지 않고 sccache 를 설치하지도 않는다 — 실패하지 않는다.
4. **DuckDB 의 C/C++ 는 아직 캐시하지 않는다.** `RUSTC_WRAPPER` 는 rustc 만 감싸고, 빌드 스크립트의 C/C++ 는 예전처럼 컴파일한다(ADR-0155 가 지목한 별도 래핑). Rust 쪽 이득을 먼저 실측하고, 부족하면 `CC`/`CXX` 래핑을 다시 본다.
5. **먼저 `foundation-ci.yml` 하나에만 비밀값을 넣어 실측한다.** 다른 Rust 워크플로는 `rust-setup.sh` 를 공유하므로, 그 워크플로의 env 에 같은 비밀값을 넣으면 켜진다 — 측정 뒤 확장한다.

## Consequences

- 변경 하나에 드는 컴파일이 "거의 처음부터 여러 번"에서 "바뀐 단위만"으로 줄어든다. 전/후 빌드 시간을 `foundation-ci.yml` 의 Rust 잡에서 실측해 효과를 기록한다(ADR-0155 의 "측정해 부족할 때"에 답하는 수치).
- 소유자가 저장소 밖에서 한 번 한 일: Cloudflare R2 에 `sccache` 버킷 생성, 그 버킷만 Object Read & Write 하는 R2 API 토큰 생성, 저장소 Actions 비밀값 셋(`SCCACHE_R2_ACCESS_KEY_ID`, `SCCACHE_R2_SECRET_ACCESS_KEY`, `SCCACHE_R2_ENDPOINT`) 등록. 환경변수 접두사 `SCCACHE_` 는 이미 계약에 예약돼 있었다(`config/environment-variable-naming.contract.json`).
- 비용: R2 저장 몇 GB(캐시 blob, LRU 로 관리) + **egress 무료** → CI 돈 0 원칙(루트 ADR 기조) 유지.
- 보안: 키는 비밀값으로만 들어오고 포크 PR 엔 전달되지 않는다. sccache 바이너리는 버전 핀 + 발행자 체크섬 검증으로 내려받는다. 캐시에는 컴파일 산출물만 담긴다.
