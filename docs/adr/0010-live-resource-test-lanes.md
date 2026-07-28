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

### 남은 부채 (이 ADR로 닫히지 **않는** 것)

1. **커버리지는 늘지 않았다.** 55타깃 중 실제 실행은 28개(foundation postgres 25 + kafka 3).
   나머지 27개는 이제 "안 돎"이 보일 뿐 여전히 아무것도 검증하지 않는다. gongzzang 20개와
   intelligence 2개는 CI 잡이 없고, R2·lakehouse는 자격증명이 없다.
2. **gongzzang 중복.** `two_stage_test`와 `integration` feature가 레인과 공존해 같은 지식이
   3곳에 있다. 레인이 생긴 지금 `two_stage_test`는 삭제 가능하다.
3. **가드는 정적이다.** 두 정규식에 걸리지 않는 새로운 삼킴 방식은 통과한다. Bazel
   샌드박스 같은 물리 차단이 아니다. Docker `--network none`이 보강 후보다.
4. **임포트 게이트가 없다.** 이 결함군이 들어온 경로(대량 임포트)는 지금도 검사 없이 열려
   있다.
