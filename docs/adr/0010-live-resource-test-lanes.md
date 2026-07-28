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
"선택되지 않음"을 구분하지 못한다.** nextest의 `--no-tests=fail`에 해당하는 장치가
cargo test에는 없으므로, xtask가 stdout을 캡처해 libtest의
`test result: ok. N passed`를 되읽고 어떤 타깃이든 `N == 0`이면 레인을 실패시킨다
(`executed_test_count` / `lane_target_verdict`). stderr는 그대로 흘려보내 컴파일
진행 로그는 살아 있다. 이것이 없으면 다음 플래그 실수도 같은 방식으로 숨는다.

### `verify`의 2단계도 이 표에서 파생된다

`--all-features`는 feature 게이팅된 스위트를 켜 버리므로, 그 패키지는 DB 없는 기본
실행에서 제외되어야 한다. 이 사실은 이제 `Feature` 레인의 존재에서 계산된다
(`feature_gated_packages`). 손으로 맞추던 `two_stage_test: bool`은 삭제했다 —
**동작을 지운 것이 아니라 같은 사실의 두 번째 진술을 지운 것이다.** 그냥 지웠다면
`verify gongzzang`이 DB 없이 라이브 스위트 20개를 실행하고 첫 연결에서 죽는다.

재발 차단 가드 2종:

- `scripts/guard/live-lane-completeness.sh` — 백엔드 게이팅된 모든 테스트 타깃이 정확히
  하나의 레인에 속함을 증명한다. `#[ignore]`와 `#![cfg(feature = ...)]` **두 게이팅
  방식을 모두** 인식하고, `platforms/`와 `products/`를 **모두** 스캔한다.
- `scripts/guard/no-silent-test-skip.sh` — 자원 부재를 통과로 바꾸는 형태를 금지한다.
  시끄러운 형태(`eprintln!("skipping …")`)와 조용한 형태(`env::var(..).ok()`) 둘 다.

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
2. **`no-silent-test-skip.sh`가 놓치는 형태가 남아 있다.**
   `let Ok(x) = env::var(..) else { return Ok(()) }`는 두 정규식(`…skip` 출력,
   `env::var(..).ok()`) 어디에도 걸리지 않는다. identity `live_provisioning`은
   이번에 fail-loud로 고쳤지만, `foundation-outbox/tests/publish_roundtrip.rs`의
   `pool()`은 그대로다 — 게다가 `.map_or_else(|_| Ok(None), …)`로 **연결 실패까지**
   "백엔드 없음"으로 강등한다. 가드 정규식 확장 + 그 파일 수정이 남은 일이다.
3. **가드는 정적이다.** 세 번째 삼킴 방식이 나오면 또 통과한다. Bazel 샌드박스 같은
   물리 차단이 아니다. Docker `--network none`이 보강 후보다.
4. **레인의 게이팅 선언이 실물과 일치하는지는 정적으로 검사되지 않는다.**
   `live-lane-completeness.sh`는 "모든 백엔드 게이팅 타깃이 어떤 레인에 속하는가"만
   증명하고, 그 레인의 `LaneGating`이 파일의 실제 게이팅과 맞는지는 보지 않는다.
   실행 개수 되읽기가 **실행될 때** 이를 잡지만, 한 번도 실행되지 않는 레인
   (r2·lakehouse·data-go-kr)은 여전히 틀린 채로 있을 수 있다.
5. **임포트 게이트가 없다.** 이 결함군이 들어온 경로(대량 임포트)는 지금도 검사 없이 열려
   있다.
