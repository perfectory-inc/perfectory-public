# ADR 0010: 라이브 자원 테스트 레인 (`LiveLane`)

- Status: Accepted
- Date: 2026-07-28
- 관련: [ADR-0004 검증 SSOT](./0004-verification-ssot.md) (Phase 2 종결)

## Context

ADR-0004는 fmt/clippy/test 명령을 `cargo xtask verify`로 통합했지만, 외부 백엔드가
필요한 테스트는 **Phase 2로 명시적으로 미뤘다**(0004 §Scope: "DB-integration/compose/
frontend jobs keep their commands and are exempt from the guard"). 그 미뤄둔 자리에서
다음이 자라났다.

`cargo xtask integration foundation`은 `--workspace --all-features -- --ignored`였다.
`--ignored`는 어떤 백엔드가 필요한지 구분하지 못하므로, Postgres만 프로비저닝하는 잡이
Kafka·R2·lakehouse·공공API 테스트까지 함께 실행했다. 각 테스트는 자기 자원이 없음을
발견하고 "자원 없음" 분기로 조용히 빠져나갔고, cargo는 이를 **통과로 집계**했다.

전수 조사 결과 이런 지점이 **39곳**이었다. 대표적으로:

- `complex_anchor_summary_reads.rs`·`marker_tile_reads.rs`·`parcel_marker_anchor_rebuild.rs`
  는 `#[ignore]`조차 없어, DB 없는 `verify`에서 매번 실행되고 통과했다. `--ignored`는
  ignore된 것만 돌리므로 **CI 어디에도 DB에 도달할 경로가 없었다**(ADR-0008 PNU-앵커
  경로의 DB 커버리지가 0이었다).
- intelligence의 Redis 토큰버킷 계약 테스트는 필요한 환경변수가 저장소 전체에
  **한 번도 존재한 적이 없었다**(git 이력 포함).
- `Option<Integration>`의 `None`은 탈출구였다. 4개 영역 중 3개가 이를 달고 있었고,
  그 영역들의 라이브 스위트는 아무 데서도 실행되지 않으면서 CI는 초록이었다.

원인은 능력 부족이 아니라 **누락된 훑기**다. 결함은 전부 `569ec151`
("publish audited source snapshot", 1,938파일 / 361,056줄 일괄 임포트)로 들어왔고, 이
저장소 안에서 새로 만든 Kafka 작업(07-26)은 fail-loud 게이트와 전용 가드
(`foundation-kafka-contract.sh`)를 갖추고 있었다. 규율은 작동했으나 임포트된 코드에
적용된 적이 없었다.

## Decision

**자원별 레인이 자기 타깃을 이름으로 지정한다. 쓸어담지 않는다.**

`tools/xtask`에 `LiveLane { name, required_env, targets }`를 둔다. `integration`은
`--workspace --ignored` 대신 레인의 타깃만 `-p <pkg> --test <target>`으로 실행하며,
`required_env`가 없으면 **거부한다**. `Option<Integration>`은 삭제했다.

타깃은 **제외가 아니라 열거**한다. 제외 목록은 누군가 테스트를 추가하면 조용히
넓어지지만, 열거 목록은 완전성 가드와 짝지으면 같은 누락이 실패가 된다.

### 게이팅 방식도 레인이 선언한다 (`LaneGating`)

`#[ignore]`는 `-- --ignored`로, `#![cfg(feature = "…")]`는 `--features …`로 선택되며
**둘은 서로의 반대**다. 첫 레인 표는 네 영역 모두에 `-- --ignored`를 붙였고, 그래서
gongzzang 레인은 (1) `--features integration`이 없어 스위트가 빈 파일로 컴파일되고
(2) `--ignored`가 그 안의 아무것도 고르지 못해 0개를 실행한 뒤 (3) cargo와 함께
exit 0으로 끝났다. **이 ADR이 없애려던 결함이 레인 계층에서 그대로 재현돼 있었다.**
게이팅을 필드로 만들면 플래그가 가정이 아니라 선언에서 따라 나온다.

### 레인은 실행 개수를 되읽는다

cargo는 필터가 아무것도 고르지 못해도 exit 0이다. 즉 **종료 코드는 "검증됨"과
"선택되지 않음"을 구분하지 못한다.** 그래서 xtask가 stdout을 캡처해 libtest의
`test result: ok. N passed`를 되읽고, 어떤 타깃이든 `N == 0`이면 레인을 실패시킨다
(`executed_test_count` / `lane_target_verdict`). stderr는 그대로 흘려보내 컴파일
진행 로그는 살아 있다.

**"0개 선택 = 실패"는 우리 발명이 아니라 업계 기본값이다.**

| 러너 | 장치 | 기본값 |
| --- | --- | --- |
| cargo-nextest | `--no-tests=fail` (exit 4 `NO_TESTS_RUN`) | 0.9.75(2024-08)에 도입, **0.9.85(2024-11-26)부터 기본** |
| pytest | exit 5 `EXIT_NOTESTSCOLLECTED` | PR #817, **2015년부터 기본** |
| Gradle | `failOnNoMatchingTests` / `failOnNoDiscoveredTests` | 둘 다 **기본 true** (후자는 9.0.0) |
| Maven Surefire | `failIfNoSpecifiedTests` | **기본 true** (2.12~) |
| CTest | `--no-tests=error` | CMake 3.26~, 스크립트 모드 기본 |
| Go | 없음 | golang/go#64500 미해결 |

Surefire의 이분법이 우리 사정과 정확히 겹친다: 필터 없는 전체 실행은 관대하게
(`failIfNoTests` 기본 false), **명시적 필터가 아무것도 못 맞히면 하드 실패**
(`failIfNoSpecifiedTests` 기본 true). 레인은 항상 `-p <pkg> --test <target>`이라는
명시적 필터다.

**stdout 파싱을 고른 것은 취향이 아니라 유일한 선택지다.** cargo에 이 기능이 없는
이유가 rust-lang/cargo#6151·#11875에 적혀 있고 둘 다 `S-blocked-external`로 열려
있다. ehuss: *"libtest에 구조화된 출력이 없어서 cargo는 필터가 아무것도 못 맞혔다는
사실을 알 수 없다"*. epage는 제안된 `--fail-if-noop` PR을 *"libtest 소관인데 libtest는
soft feature freeze"*라며 기각했다. 구조화 출력(rust-lang/rust#49359)은 아직 unstable
이라 `--locked` 안정 툴체인에서 쓸 수 없다. 즉 **막힌 쪽이 cargo이고, 사람이 읽는
요약을 되읽는 것이 현재 stable에서 가능한 유일한 방법이다.**

ehuss가 #11875에서 든 반대 논거(한 타깃에서만 매칭되고 나머지 타깃은 0개를
보고하는 경우)는 우리에게 적용되지 않는다. 레인은 타깃을 **하나씩** 지정해 호출하므로
0개는 언제나 명확한 실패다.

### `verify`의 2단계도 이 표에서 파생된다

`--all-features`는 feature 게이팅된 스위트를 켜 버리므로, 그 패키지는 DB 없는 기본
실행에서 제외되어야 한다. 이 사실은 이제 `Feature` 레인의 존재에서 계산된다
(`feature_gated_packages`). 손으로 맞추던 `two_stage_test: bool`은 삭제했다 —
**동작을 지운 것이 아니라 같은 사실의 두 번째 진술을 지운 것이다.** 그냥 지웠다면
`verify gongzzang`이 DB 없이 라이브 스위트 20개를 실행하고 첫 연결에서 죽는다.

재발 차단 가드 2종:

- `scripts/guard/live-lane-completeness.sh` — 두 가지를 증명한다. (1) **소속**: 백엔드
  게이팅된 모든 테스트 타깃이 정확히 하나의 레인에 속한다. (2) **일치**: 레인이 선언한
  `LaneGating`이 그 타깃 소스의 실제 게이팅과 같다. 소속만으로는 부족하다 — gongzzang
  20개는 완전하게 선언돼 있었고 아무것에도 선택되지 않았다. 실행 개수 되읽기가 더 강한
  검사지만 **레인이 실제로 돌 때만** 발동하고, 5개 레인(foundation r2·lakehouse·
  data-go-kr, intelligence kafka·redis)은 어느 CI 잡에서도 돌지 않는다. 그 레인들에겐
  이 정적 검사가 유일한 방어다. 선례: rust-lang/rust PR #108905가 오타난 compiletest
  디렉티브(`//@ ignore-<typo>`)를 조용한 무효에서 하드 에러로 바꾸자 **선언한 게이팅이
  한 번도 적용된 적 없는 테스트 79개**가 드러났다.
- `scripts/guard/no-silent-test-skip.sh` — 자원 부재를 통과로 바꾸는 **네 가지** 형태를
  금지한다. 시끄러운 형태(`eprintln!("skipping …")`), 조용한 형태
  (`env::var(..).ok()`), let-else 형태, 그리고 env 프로브가 이른 **성공** 반환을
  감싸는 형태. 마지막 둘은 앞의 두 규칙을 모두 우회했고 실제 결함 2건을 숨기고 있었다.
  `Err` 반환은 정상이므로 걸리지 않는다 — 실행 거부는 옳은 동작이다.

## 기각한 대안: 별도 `live-tests` 패키지

Oxide omicron이 `live-tests/`를 별도 패키지로 두고 있어 유력한 후보였고, 그 README는
*"cargo에는 빌드는 하되 기본 실행은 안 하게 할 방법이 없다"*고 적고 있다. 그러나 조사
결과 우리에겐 맞지 않는다.

1. **문제가 풀리지 않는다.** `tools/xtask`의 실행 명령은 모두 `--workspace`다. 새 패키지를
   `members`에 넣는 한 그대로 쓸려 들어가므로 `--exclude`가 **여전히 필요하다**. 비용을
   전부 치르고 얻는 것이 없다.
2. **커버리지가 깎인다.** `r2_smoke_contract.rs`는 10개 중 8개가, `data_go_kr_…rs`는 2개 중
   1개가 `#[ignore]`가 아닌 상시 계약 테스트다. 파일을 통째로 옮기면 기본 경로에서 9개가
   사라진다.
3. **전례가 대응하지 않는다.** omicron의 live-tests는 **배포된 실제 시스템**을 대상으로
   하지만, 우리 Kafka 레인은 일회용 compose 스택이다.
4. 부수 비용: `Cargo.lock` 재생성(`--locked`), cargo-deny/SBOM, `unique-package-names`
   가드, CI 경로 필터, SQLx 오프라인 메타데이터, `foundation-kafka-contract.sh`
   즉시 실패, 런북 2건.

물리 경계만으로는 fail-closed가 보장되지 않는다. 보장하는 것은 완전성 가드다.

## 기각한 대안: Bazel

`rules_rust`로 샌드박스 네트워크 차단을 얻는 안을 재검토했으나 기각을 유지한다. DFINITY
(Rust 60만 줄)는 성공했지만 **rust-analyzer가 전체 코드베이스에서 깨졌고 Cargo 파일을
이중 유지**해야 했다. 우리는 26만 줄, 1인 운영, 호스트에 cargo가 없어 모든 검증이 Docker
경유이며, 워크스페이스 4개를 의도적으로 분리해 두었다(Bazel이 가장 까다로워하는 지점).
얻으려는 것이 "테스트 네트워크 차단" 하나인데 비율이 맞지 않는다.

> **갱신 (2026-07-30):** 이 문단의 근거 하나가 낡았다. rust-analyzer의 비-Cargo 프로젝트
> 자동 탐지(`rust-analyzer.workspace.discoverConfig`)가 머지되어(rules_rust #2755, PR #3073)
> Bazel 프로젝트에서도 Cargo와 비슷하게 동작한다. 공식 문서가 여전히 "형식은 잠정적"이라
> 적고 flycheck이 기본 비활성이므로 Cargo 수준은 아니지만, **"IDE가 깨진다"는 더 이상 주된
> 기각 사유가 아니다.**
>
> 기각 자체는 유지한다. 유효한 근거는 1인 운영, 원격 캐시 부재, 검증 계층 전면 재작업이며
> 재검토 트리거와 함께
> [운영 준비 로드맵 우선순위 4](../roadmap/production-readiness.md)에
> 정리돼 있다. 기각을 유지하되 근거를 갱신하지 않으면, 언젠가 낡은 근거로 옳은 결정을
> 방어하게 된다.

## Consequences

- 새 영역·새 백엔드 추가는 `tools/xtask` 한 곳의 데이터 편집이다.
- 레인에 배정되지 않은 백엔드 게이팅 테스트는 CI에서 실패한다. 깜빡할 수 없다.
- `cargo xtask integration <area>`는 이제 postgres 레인을 뜻한다. 두 호출자
  (`foundation-ci.yml`, `scripts/verify/integration.sh`) 모두 Postgres만 프로비저닝한다.
- `gongzzang-db-migrations.yml`이 raw `cargo test -p gongzzang-persistence --features
  integration` 대신 gongzzang postgres 레인을 실행한다. 그 잡은 이미 Postgres와 전체
  마이그레이션 체인을 세우므로 추가 비용이 없고, `tools/xtask/**`를 paths에 넣어
  레인 정의 변경이 그 잡을 재실행시킨다(`xtask-path-coverage`가 `integration`까지
  강제하도록 확장).

### 남은 부채 (이 ADR로 닫히지 **않는** 것)

0. **초판의 커버리지 집계는 틀렸다 (정정).** 초판은 "55타깃 중 실행 28개(foundation
   postgres 25 + kafka 3)"이며 "gongzzang 20개는 CI 잡이 없다"고 적었다. 실측하면
   **49개가 어딘가에서 실행된다**:

   | 타깃 | 실행 주체 | 상태 |
   | --- | --- | --- |
   | gongzzang postgres 20 | `gongzzang-db-migrations.yml` (`required/gongzzang-migrations`) | 계속 실행돼 왔다 |
   | foundation postgres 25 | `foundation-ci.yml` postgres-integration (레인) | 실행 |
   | foundation kafka 3 | `foundation-ci.yml` kafka-integration | 실행 |
   | identity `role_grant_postgres` 1 | `identity-ci.yml` live-contracts (raw 명령) | 실행 |
   | identity `live_provisioning` 1 | 없음 | **미실행** |
   | intelligence kafka·redis 2 | 없음 | **미실행** |
   | foundation r2·lakehouse·data-go-kr 3 | 자격증명 없음 | **미실행** |

   오진의 원인은 **"레인에서 안 돈다"를 "어디서도 안 돈다"로 읽은 것**이다. 이 ADR이
   경고한 바로 그 혼동("확인 안 함"을 "없음"으로 기록)을 ADR 자신이 저질렀다.
   gongzzang 20개가 레인을 거치지 않고 raw 명령으로 돌고 있었을 뿐이다. 그
   raw 명령은 이제 `cargo xtask integration gongzzang postgres`로 교체돼,
   완전성 가드가 증명하는 집합과 CI가 실행하는 집합이 같아졌다.

1. **walking-skeleton의 중복 스윕.** `scripts/ci/walking-skeleton-e2e.sh`가
   `cargo test --workspace --features integration`으로 같은 20개를 한 번 더 돌린다.
   이 ADR이 기각한 "쓸어담기" 형태이며, PR마다 중복 비용을 낸다. 레인 위임으로
   대체 가능하지만 그 워크플로는 이번 변경 범위 밖이라 손대지 않았다.

   > **해소 (2026-08-05):** 레인에 위임했다. 경로는 여기 적힌
   > `scripts/ci/walking-skeleton-e2e.sh`가 아니라
   > `products/gongzzang/scripts/ci/walking-skeleton-e2e.sh`다. 스윕이 덮던 것은 **양쪽 다 이미
   > 다른 워크플로가 돌리고 있었다** — 단위 테스트는 `gongzzang-ci.yml`의
   > `cargo xtask verify gongzzang`이, 20개 DB 타깃은 `gongzzang-db-migrations.yml`의
   > `cargo xtask integration gongzzang postgres`가. 별칭이 `--manifest-path`를 호출자 디렉터리
   > 기준으로 푸는데 이 스크립트는 `products/gongzzang`에서 돌므로, `foundation-kafka-live.sh`와
   > 같은 `(cd "$monorepo_root" && …)` 형태를 쓴다.
   >
   > 남은 판단: 이 잡이 DB 타깃을 **아예 돌리지 않아도 되는가**. 지금은 돌린 직후 `truncate`하고
   > API를 띄우므로 그 실행이 E2E 단언에 기여하지 않는다. 다만 스크립트가 선언한 순서
   > ("migrate, run integration tests, boot the API")를 바꾸는 일이라 위임까지만 했다.
2. **전수 감사 결과: 게이팅 선언은 55개 모두 실물과 일치한다.** 55타깃을 서브모듈
   (`mod`/`#[path]`)까지 펼쳐 실제 속성을 세어 확인했다. `Ignored` 35개는 모두 진짜
   `#[ignore]` 속성을 1개 이상 갖고, gongzzang 20개는 모두 문자 그대로
   `#![cfg(feature = "integration")]`이며 그 크레이트 `tests/` 전체에 `#[ignore]`가
   0개다. 주석 속 `#[ignore]` 언급을 속성으로 오독한 파일도 없고, `[[test]]`·
   `required-features`·`harness = false` 같은 매니페스트 함정도 없다. 다만 이 사실은
   **지금** 참일 뿐이라 위의 정적 일치 검사로 고정했다.
3. **`required_env`가 짧은 레인 3개를 찾아 고쳤다.** 레인의 거부 계약은 타깃이 *읽는*
   변수를 전부 이름 대야 하는데, 연결 문자열이 아닌 **스위치**가 빠져 있었다.
   `live_kafka_outage`는 `FOUNDATION_TEST_KAFKA_REQUIRED` 없이 `Ok(())`를 반환했고
   (브로커 없이 통과), 나머지 kafka 2개는 `FOUNDATION_PLATFORM_KAFKA_ENABLED` 없이
   `Err`를 반환하며, `r2_smoke_contract`의 두 번째 ignored 테스트는 저장소 어디서도
   export하지 않는 `FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE`를 단언한다. 셋 중
   조용한 것은 첫 번째뿐이지만, 셋 다 "약속한 거부가 발동할 수 없다"는 같은 결함이다.
   r2는 **한 번도 돌지 않았으므로** 아무것도 알려주지 않았을 것이다.

   > **해소 (확인 2026-08-05):** 마지막 문장이 지목한 미export 변수는 이제 존재한다.
   > `scripts/verify/foundation-r2-lakehouse-live.sh:114`가
   > `FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE`를 export하고 레인 선언도 그것을 요구한다.
   > r2 타깃 자체는 5번의 이유(자격증명)로 여전히 돌지 않는다.
4. **가드는 여전히 정적이다.** 다섯 번째 삼킴 방식이 나오면 또 통과한다. Bazel
   샌드박스 같은 물리 차단이 아니다. Bazel조차 새는 것이 알려져 있으나
   (bazelbuild/bazel#10068·#11325 — darwin-sandbox가 네트워크를 실제로 막지 못함),
   방향은 맞다. Docker `--network none`이 보강 후보다.
5. **여전히 실행되지 않는 6타깃.** identity `live_provisioning`(레인 자체가 어느
   워크플로에서도 호출되지 않는다 — identity-ci는 raw 명령으로 `role_grant_postgres`
   하나만 돌린다), intelligence kafka·redis, foundation r2·lakehouse·data-go-kr
   (자격증명 없음). 이제 "안 돎"이 정직하게 보이고 게이팅도 검증되지만, 커버리지는
   그대로다.

   > **부분 해소 (확인 2026-08-05):** 6타깃이 아니라 **5타깃**이다.
   > `identity-ci.yml`이 이제 `cargo xtask integration identity postgres`로 레인을 부르고
   > (100행) 레인이 요구하는 두 URL을 모두 설정하므로(75~81행), `live_provisioning`은 실행된다.
   > 이것이 자격증명 없이 닫을 수 있던 유일한 항목이었다.
   >
   > 남은 5개(intelligence kafka·redis, foundation r2·lakehouse·data-go-kr)는 전부
   > **자격증명이 없어서** 안 돈다. 코드로 닫히지 않는다 — 누가 어느 계정으로 그 자원을 준비할
   > 것인가의 결정이며, 우선순위 0의 자격증명 분리 항목과 같은 결정에 묶인다.
6. **`foundation-kafka-live.sh`가 레인을 우회한다.** raw `cargo test --locked … --
   --ignored` 루프로 같은 3타깃을 돌린다. gongzzang에서 없앤 것과 같은 사설 사본이다.

   > **해소 (확인 2026-08-05):** 더 이상 우회하지 않는다.
   > `scripts/verify/foundation-kafka-live.sh`는 88·126행에서
   > `cargo xtask integration foundation kafka`를 부른다. 이 항목이 언제 닫혔는지는 이 기록이
   > 말하지 않는다 — 닫힐 때 여기에 적히지 않았다는 사실 자체가 아래 정정의 근거다.
7. **임포트 게이트가 없다.** 이 결함군이 들어온 경로(대량 임포트)는 지금도 검사 없이 열려
   있다.
8. **rustfmt 한 줄이 가드를 통째로 무력화했다 (발견·수정).** 4번의 "정적이라 샌다"는
   가정보다 훨씬 싸게 뚫렸다. 새 삼킴 방식이 필요하지도 않았고, 이미 잡던 방식을
   **줄바꿈만** 하면 됐다. intelligence의 `knowledge_source_registry_contract`가
   그랬다 — rustfmt가 `eprintln!(`를 `"skipping …"` 문자열에서, `env::var("…")`를
   `.ok()`에서 떼어 놓자 두 규칙이 나란히 지나갔다. 이 파일은 `#[ignore]`가 아니라서
   `cargo xtask verify intelligence`가 돌 때마다 DB 없이 **통과했다**. 가드는 이제
   `.`·`"`로 시작하는 연속 줄을 앞 줄에 접어 붙인 뒤(원래 줄 번호는 보존) 매칭하고,
   자기-테스트가 접힌 세 형태(`eprintln!` 거부, `.ok()…?` 거부, `.ok()…expect()` 수용)를
   고정한다. 테스트 자체는 형제인 `workflow_state_contract_suite`와 같은 fail-loud
   헬퍼(`.ok().filter(..).expect(..)`)로 바꿨다. 교훈: 정적 가드의 실질 적용범위는
   **정규식이 보는 텍스트 형태**에 달려 있고, 포매터가 그 형태를 바꾼다.
9. **개수 파서는 `--nocapture`에 잠재적으로 취약하다.** `test result:`로 시작하는 줄을
   전부 합산하므로, 테스트가 stdout에 그 접두사를 직접 찍으면 개수가 부풀 수 있다.
   레인은 `--nocapture`를 붙이지 않아 지금은 닿지 않는 경로이고, 실행 0개일 때는 찍을
   테스트 자체가 없으므로 **거짓 통과 방향으로는 새지 않는다**(부풀리기만 가능). 반대로
   libtest 문법이 바뀌어 파싱이 전부 실패하면 합계가 0이 돼 레인이 빨갛게 죽는다 —
   fail-closed 쪽이다.
10. **0-테스트 규칙에 옵트아웃이 없다.** 정당하게 0개인 레인 타깃을 예외 처리할 수단을
    일부러 만들지 않았다. Bazel 수준의 엄격함이며, 그런 타깃이 생기면 레인 표에서
    빼는 것이 옳은 답이라고 본다. 필요해지는 시점에 `expected_zero` 같은 플래그가 아니라
    이 판단부터 다시 검토해야 한다.
