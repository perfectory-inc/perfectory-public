# ADR 0200: outbox-publisher 는 레인별 라이브러리 크레이트로 나뉘고, 실행 파일은 명령 해석과 배선만 남긴다

- Status: Accepted
- Date: 2026-10-13
- Related: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (CI 캐시는 의존성만 재사용한다)

## Context

2026-10-13 측정이다.

1. **`foundation-outbox-publisher` 는 하나의 크레이트에 소스 371개·약 16만 줄이 있다.** `main.rs` 1,515줄이
   169개 모듈을 선언하고, 그중 by-PNU 서빙 묶음(`building_by_pnu_serving_export` 등 14개 모듈과 이들이 쓰는 공용
   모듈 7개, 약 2만5천 줄)이 8월 이후 가장 많이 고친 곳이다(`building_by_pnu_serving_export` 114회).
2. **한 줄을 고치면 크레이트 전체가 다시 컴파일된다.** rustc 의 컴파일 단위는 크레이트다. 노트북(8코어, 공유
   `CARGO_TARGET_DIR`, 증분 끔)에서 `by_pnu_pack.rs` 하나를 건드린 뒤 `cargo test -p foundation-outbox-publisher
   --no-run` 이 312초 걸렸고, 다시 컴파일된 것은 이 크레이트 하나였다.
3. **CI 에서 이 크레이트가 임계 경로다.** ADR-0155 의 캐시는 의존성만 담고 작업공간 크레이트는 매번 새로
   컴파일한다. main 머지 큐 실행(run 38267452690)에서 clippy 의 publisher 검사 52초, 테스트 빌드의 publisher
   컴파일 2분 43초(다른 작업공간 크레이트는 그 전 15초 안에 끝남), 정적 릴리스 단계의 dev 빌드 51초였다. 크레이트
   하나의 프런트엔드는 대부분 직렬이라 4코어 러너가 이 시간 동안 대부분 놀았다.

## Decision

1. **크레이트 지도.** publisher 의 라이브러리 크레이트는 `platforms/foundation-platform/crates/publisher/` 아래에
   둔다.

   | 크레이트 | 담는 것 |
   | --- | --- |
   | `foundation-publisher-core` | 여러 레인이 같이 쓰는 것: 환경·제어 지원(`public_data_control_support`), R2 키 배치(`r2_layout`), 게이트웨이 계약(`by_pnu_gateway_contract`, `profile_gateway_contract`), 핸드오프 manifest 지원, 레이크하우스 스냅숏 스캔, Gold 프로필 저장소, 건물 연결 증거 |
   | `foundation-publisher-by-pnu` | by-PNU 서빙 레인: 건물·필지 서빙 내보내기, 묶음(pack), manifest 발행, 상태·재기준·핀·패치 |
   | 이후 레인 | 같은 규칙으로 하나씩: HUB Silver 내보내기, 국가 수집(VWorld·공공데이터), 공간 발행(경계·정적 릴리스·타일), 산업단지 |

2. **의존 방향은 한쪽뿐이다.** 실행 파일 → 레인 크레이트 → `foundation-publisher-core` → 기존 도메인·응용·인프라
   크레이트. 레인 크레이트끼리는 서로 의존하지 않고, core 는 어떤 레인에도 의존하지 않는다. 두 레인이 같은 것을
   필요로 하면 그것은 core 로 내려간다. 이 방향은 `foundation-outbox-publisher/tests/publisher_crate_direction.rs`
   가 각 크레이트의 `Cargo.toml` 을 읽어 검사한다(레인 사이 의존이 생기면 컴파일 그래프가 다시 직렬이 되어 이
   결정의 효과가 조용히 사라진다).
3. **실행 파일에 남는 것.** `main.rs` 의 명령 해석(`Command`)과 디스패치, 런타임 환경 배선, outbox 발행 루프.
   명령 이름·인자·출력·종료 코드는 바뀌지 않는다. 레인 모듈의 실행 함수(`run`)는 레인 크레이트에서 공개되고
   실행 파일은 그것을 부를 뿐이다.
4. **옮기기는 동작을 바꾸지 않는다.** 모듈은 `git mv` 로 옮기고 경로(`crate::` → 크레이트 이름, 고정 파일의
   상대 경로)와 가시성만 고친다. 옮긴 모듈의 테스트는 함께 옮겨 같은 이름으로 돌고, 골든 파일은 그대로 둔다.
   옮겨진 코드는 실행 파일과 같은 lint 자세(`main.rs` 의 crate 수준 허용 목록)를 그대로 가져간다.
5. **가장 많이 고치는 것부터 작은 PR 로.** 첫 PR 은 core 와 by-PNU 레인이다. 다음 레인은 각자 독립 PR 이다.

## Consequences

- by-PNU 레인을 고치면 `cargo test -p foundation-publisher-by-pnu` 는 그 크레이트만 컴파일한다. 작업공간 전체 테스트는
  여전히 의존하는 실행 파일을 다시 컴파일하지만, 실행 파일이 작아지는 만큼 짧아지고, 레인 크레이트의 코드 생성과
  테스트 하네스가 실행 파일 컴파일과 병렬로 돈다. 효과는 PR 마다 before/after 로 잰다.
- CI 범위(`scripts/ci/foundation_ci_scope.py`)와 데이터 파이프라인 그래프의 증거 경로가 새 위치를 가리켜야 한다.
- 옮긴 모듈의 공개 표면에 문서가 없는 항목이 많다. 레인 크레이트는 `missing_docs` 를 crate 수준에서 풀고 시작하며,
  이것은 실행 파일 안에 있을 때와 같은 상태다. 공개 표면을 줄이고 문서화하는 것은 레인별 후속 작업이다.
