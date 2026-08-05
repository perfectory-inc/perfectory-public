---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-07-30
---

# ADR 0011: 테스트 실행 집합 완전성

- Status: Accepted
- Date: 2026-07-29
- 관련: [ADR-0004 검증 SSOT](./0004-verification-ssot.md), [ADR-0010 라이브 자원 테스트 레인](./0010-live-resource-test-lanes.md) (적용 범위 확장)

## Context

ADR-0004는 fmt/clippy/test **명령**을 `cargo xtask verify` 하나로 모았다. ADR-0010은 백엔드가
필요한 Rust 테스트의 **실행 집합**을 레인으로 열거하고, 열거가 완전함을 가드로 증명했다.

그 불변식은 옳았지만 **적용 범위가 Rust로 한정돼 있었다.** ADR-0010의 완전성 가드
(`scripts/guard/live-lane-completeness.sh`)는 `platforms`·`products` 아래의 `*/tests/*.rs` 중
`#[ignore]` 또는 `#![cfg(feature = …)]`로 게이팅된 것만 발견 대상으로 삼는다. 그 경계 밖에서
같은 결함이 그대로 자라 있었다.

원인은 우연이 아니라 **선언의 모양**이다.

| 선언 | 모양 | 대조 가능? |
| --- | --- | --- |
| `LaneTarget { package, test }` | **타깃**을 열거 | 가능 — 발견 집합과 이름으로 맞출 수 있다 |
| `PythonTests { dir, python_path, args }` | **명령**을 서술 | 불가능 — 무엇을 책임지는지 말하지 않는다 |
| `pnpm turbo test` | **쓸어담기** | 불가능 — `test` 스크립트를 가진 패키지만 우연히 걸린다 |

명령은 "어떻게 도는가"만 말한다. 무엇을 책임지는지 말하지 않는 선언은 검사할 수 없고,
검사할 수 없는 선언은 조용히 불완전해진다.

2026-07-29 전수 조사로 확인된 미실행 집합:

| 언어 | 대상 | 개수 | 확인 방법 |
| --- | --- | ---: | --- |
| Python | `platforms/foundation-platform/infra/lakehouse/dbt/tests/` 3파일 | 25 | xtask `python_tests`·전 워크플로·`lefthook.yml` 어디에도 없음 |
| Python | `platforms/foundation-platform/services/foundation-api/tests/test_api_exchange_direction_contract.py` | 1 | 동일 |
| TypeScript | `products/gongzzang/apps/web` 의 `probe:naver` 스크립트 | 1파일 | 어느 워크플로도 이 스크립트를 부르지 않음 |

직접 실행하니 26개 중 25개가 통과하고 1개가 실패했다. 실패한 것은
`test_trino_catalog_template_contract.py`의 `test_foundation_jdbc_catalog_uses_foundation_bucket_name`으로,
`assertIn(X)` 직후 `assertNotIn(S)`를 단언하는데 `S ⊂ X`다. **두 단언은 동시에 참일 수 없어
구조적으로 통과가 불가능하다.** 한 번도 실행된 적이 없었으므로 한 번도 드러나지 않았다.

### 가장 안쪽 사례: 하네스 자신

같은 누락이 한 층 더 안쪽에도 있다. 이 저장소에는 Cargo 워크스페이스가 **다섯 개**인데
`AREAS`에 선언된 것은 **네 개**다.

| 워크스페이스 | 선언된 area |
| --- | --- |
| `platforms/foundation-platform` | `foundation` |
| `platforms/identity-platform` | `identity` |
| `platforms/intelligence-platform` | `intelligence` |
| `products/gongzzang` | `gongzzang` |
| `tools/xtask` | **없음** |

`verify(area)`는 `repo_root().join(area.dir)` 안에서 `cargo test --workspace`를 돈다. `tools/xtask`가
어느 area도 아니므로 **그 안의 단위 테스트 10개는 어디서도 실행되지 않는다.** 워크플로에서
`tools/xtask/**`는 일곱 곳에 나오지만 전부 **경로 필터**다 — 변경되면 area CI가 재실행되지만,
재실행되는 것은 area의 테스트이지 xtask의 테스트가 아니다. `cargo-verify.sh`가
`perfectory-target-xtask` 볼륨을 마운트하는 데서 보이듯 xtask는 매번 **빌드**되지만 그 테스트가
**실행**된 적은 없다.

돌지 않는 10개가 하필 이것들이다.

```
executed_test_count_reads_what_libtest_actually_ran
a_lane_target_that_executed_nothing_is_a_failure_not_a_pass
live_lane_runs_only_its_declared_targets_and_never_sweeps_the_workspace
feature_gated_lane_compiles_its_suite_and_never_filters_on_ignored
ignore_gated_lane_keeps_ignored_and_enables_no_feature
foundation_lanes_require_every_variable_their_targets_read
identity_lane_requires_the_urls_its_targets_actually_read
a_feature_gated_suite_is_excluded_from_the_backendless_default_run
foundation_python_plan_preserves_provider_and_discovers_spark_tests
```

**"0개 실행 = 실패"가 작동함을 증명하는 테스트가 한 번도 실행되지 않았다.** libtest의 요약 형식이
바뀌거나 파서가 깨져도 아무것도 잡지 못하고, ADR-0010의 백스톱은 조용히 무력해진다.

ADR-0010은 "xtask 수정이 어떤 area CI도 트리거하지 못한다"는 허점을 찾아
`scripts/guard/xtask-path-coverage.sh`를 만들었다. **트리거는 고쳤고 테스트는 고치지 않았다.**
경로 필터의 존재가 커버리지의 존재처럼 보였기 때문이다.

### 우회 사본

같은 경계에 **우회 사본**도 두 개 있다. `scripts/guard/no-adhoc-cargo-lint.sh`는 워크플로의 raw
`cargo clippy`/`cargo fmt`만 금지하고 `cargo test`는 보지 않으며, 스캔 범위도 `.github/workflows/`
뿐이다.

- `.github/workflows/identity-ci.yml`은 raw `cargo test -p authorization-infrastructure --test
  role_grant_postgres <테스트명> -- --ignored --exact`로 **레인이 선언한 2개 타깃 중 1개의, 그
  안에서도 테스트 1개만** 돌린다. 그 잡은 Postgres를 띄우지만 레인이 요구하는 두 환경변수 중
  `IDENTITY_ROLE_GRANT_TEST_DATABASE_URL`만 설정한다 — 그래서 레인을 부를 수 **없었고**, 그
  대신 raw 명령이 자랐다.
- `scripts/verify/foundation-kafka-live.sh`는 레인이 선언한 것과 똑같은 3개 타깃을 raw
  `cargo test … -- --ignored --nocapture` 루프로 돌린다. ADR-0010이 부채 #6으로 기록한 사본이며,
  `.github/` 밖이라 가드의 스캔 범위에도 들어오지 않는다.

## Decision

**저장소에서 발견되는 모든 테스트 타깃은 정확히 하나의 선언된 실행 경로에 속하고, 그 경로는
실제로 호출되며, 선택된 테스트가 0개면 실패한다.** 언어는 이 불변식의 매개변수일 뿐이다.

> **보강 (2026-07-30):** 이 불변식에는 짝이 있다. **테스트가 읽는 모든 입력은 그 테스트를 돌리는
> CI를 다시 트리거해야 한다.** 실행 경로가 선언되고 실제로 호출되더라도, 입력 변경이 그 경로를
> 깨우지 않으면 결과는 같다 — 테스트는 자기가 지키는 변경에 대해 돌지 않는다.
>
> 실제 사례가 나왔다. `administrative_boundary_contract`는 `docs/architecture/` 아래 문서 두 개를
> 읽는데 `foundation-ci.yml`의 push 경로 필터에 그 디렉터리가 없었다. 한글 우선 문서 마이그레이션이
> 그 문서를 바꿨고, 그 푸시로는 해당 테스트가 돌 수 없었다. 가드·문서 감사는 모두 통과했다.
>
> `scripts/guard/area-ci-input-coverage.sh`가 이 짝을 집행한다. `xtask-path-coverage`가 알려진
> 입력 하나(`tools/xtask`)를 고정한 것을, 소스가 실제로 읽는 것에서 파생하도록 일반화한다.
> 기준점 판정은 Rust를 파싱하지 않고 파일시스템에 묻는다 — 영역 루트에서 존재하면 영역 글롭이
> 이미 커버하고, 레포 루트에서만 존재하면 필터에 있어야 하며, 어느 쪽에도 없으면 경로가 아니다.
> 두 루트를 모두 만족하는 경로는 현재 저장소에 없다.

이를 위해 세 가지를 정한다.

### 1. 비-Rust 선언도 타깃 모양이어야 한다

`PythonTests`에 `covers` 필드를 더한다. `dir`/`args`는 **어떻게** 도는지를, `covers`는
**무엇을 책임지는지**를 말한다.

```rust
struct PythonTests {
    dir: &'static str,
    python_path: Option<&'static str>,
    args: &'static [&'static str],
    /// Discovery roots (area-relative) this suite is responsible for.
    covers: &'static [&'static str],
}
```

ADR-0010이 `two_stage_test: bool`을 지우고 `LaneGating`을 필드로 만든 것과 같은 이동이다.
암묵적 가정을 검사 가능한 선언으로 바꾼다. 발견된 모든 Python 테스트 파일은 정확히 하나의
`covers` 루트 아래 있어야 하며, 0개면 누락이고 2개 이상이면 소유가 모호하다.

### 2. 가드는 하나, 발견기만 언어별로 둔다

`live-lane-completeness.sh`가 Rust에 더해 Python과 TypeScript 발견기를 갖는다. 언어마다 별도
가드를 만들지 않는다 — 같은 불변식의 세 번째 진술은 드리프트 지점이 될 뿐이다.

발견 규약은 명시적이고 좁게 둔다. Python은 `test_*.py`/`*_test.py`, TypeScript는 각
`package.json`의 이름에 `test` 또는 `probe`가 들어간 스크립트다. **규약 밖 이름에 예외를 주지
않는다** — 예외 목록은 조용히 넓어지고, 그러면 발견기가 다시 불완전해진다. 규약에 맞지 않는
기존 파일은 이름을 바꾼다.

### 3. 실행 집합의 SSOT는 레인이고, 우회는 금지한다

`no-adhoc-cargo-lint.sh`의 금지 대상에 `cargo test`를 더하고, 스캔 범위를
`.github/workflows/`에서 `scripts/`까지 넓힌다. `cargo xtask` 경유는 허용이므로
`cargo-verify.sh`·`integration.sh`는 걸리지 않는다.

이 규칙이 켜지면 위의 우회 사본 두 개가 즉시 빨개진다. 둘 다 레인 호출로 교체한다. 스택
프로비저닝(compose, 준비 대기, 자격증명 주입)은 스크립트의 책임으로 남는다 — 레인의 책임은
무엇을 돌릴지 정하는 것뿐이다.

### 4. 0개 실행은 실패다 — 러너와 무관하게

xtask는 이미 Rust 레인에서 libtest의 `test result: ok. N passed`를 되읽어 `N == 0`이면
실패시킨다(`executed_test_count`). 같은 판정을 Python에도 둔다. **unittest는 `Ran N tests`
요약을 stderr에, pytest는 `N passed`를 stdout에 쓰므로 두 스트림을 모두 캡처해야 한다** —
기존 `cargo_capturing_stdout`이 stderr를 흘려보낼 수 있는 것은 libtest 요약이 stdout에 있기
때문이고, Python에 같은 선택을 하면 유일한 신호를 버린다. 파싱이 전부 실패하면 합계가 0이 되어
실패 쪽으로 떨어진다(fail-closed).

TypeScript는 vitest의 `--passWithNoTests=false`를 스크립트에 **명시**한다. 이미 기본값이지만,
명시하면 설정 변경으로 조용히 뒤집히지 않는다.

## 선례

"0개 선택 = 실패"는 우리 발명이 아니다. ADR-0010이 정리한 표를 이 ADR의 범위로 넓힌다.

| 러너 | 장치 | 기본값 |
| --- | --- | --- |
| cargo-nextest | `--no-tests=fail` (exit 4 `NO_TESTS_RUN`) | 0.9.75(2024-08) 도입, **0.9.85(2024-11-26)부터 기본** |
| pytest | exit 5 `EXIT_NOTESTSCOLLECTED` | PR #817, **2015년부터 기본** |
| Gradle | `failOnNoMatchingTests` / `failOnNoDiscoveredTests` | 둘 다 **기본 true** |
| Maven Surefire | `failIfNoSpecifiedTests` | **기본 true** (2.12~) |
| CTest | `--no-tests=error` | CMake 3.26~, 스크립트 모드 기본 |
| **vitest** | `--passWithNoTests` | **기본 false**(=0개면 실패). 4.1.7에서 실측: `No test files found, exiting with code 1`. 명시해 고정한다 |
| **Python unittest** | 없음 | `Ran 0 tests` 후 exit 0. 되읽기로 보완한다 |
| Go | 없음 | golang/go#64500 미해결 |

**발견-선언 대조**의 직접 선례는 rust-lang/rust PR #108905다. 오타난 compiletest 디렉티브
(`//@ ignore-<typo>`)를 조용한 무효에서 하드 에러로 바꾸자 **선언한 게이팅이 한 번도 적용된 적
없는 테스트 79개**가 드러났다. 우리 상황과 형태가 같다 — 선언은 있었고, 아무것도 선택하지
않았으며, 아무도 몰랐다.

## 기각한 대안

### Bazel/rules_rust로 빌드 그래프에서 실행 집합을 파생

불변식은 정확히 우리가 원하는 것이다. 빌드 그래프 밖에 테스트가 존재할 수 없으므로 발견과
선언이 애초에 갈라지지 않는다. 그러나 도구는 ADR-0010에서 이미 기각했다.

**2026-07-30 재조사에서 기각 근거를 갱신했다.** 1차 자료를 다시 읽은 결과는 다음과 같다.

- **낡은 근거:** rust-analyzer 파손. 비-Cargo 자동 탐지(`discoverConfig`)가 머지되어
  (rules_rust #2755, PR #3073) Bazel 프로젝트도 Cargo와 비슷하게 동작한다. 공식 문서가
  여전히 "형식은 잠정적"이라 적고 flycheck이 기본 비활성이므로 Cargo 수준은 아니지만,
  이것을 주된 사유로 계속 인용하는 것은 정확하지 않다.
- **유효한 근거 셋.** ① **1인 운영** — DFINITY는 60만 줄을 여러 작업 스트림으로 수 개월에
  걸쳐 옮겼고, 그 기록은 빌드 시간 개선 수치를 제시하지 않으며 Cargo 파일 이중 유지가
  남았다고 적는다. ② **원격 캐시 부재** — GitHub 호스트 러너에는 영속 캐시가 없다. Bazel
  도입으로 빌드 시간을 크게 줄인 공개 사례(50분→5분)는 캐시 없이 재현되지 않는다.
  ③ **검증 계층 전면 재작업** — 가드 60개와 `cargo xtask`가 전부 Cargo·셸 모양이고,
  **이 ADR의 구현이 거기에 더 얹었다.** 품질을 올리는 방식이 이사 비용을 함께 올린다는
  긴장은 기록해 둔다.

이 ADR은 **도구가 아니라 불변식만** 가져온다. 얻는 보장은 같고 비용은 가드 하나의 확장이다.

재검토 트리거(두 번째 엔지니어, CI 병목, 원격 캐시 확보)와 그때까지 지킬 것은
[운영 준비 로드맵 우선순위 4](../roadmap/production-readiness.md)에 있다.
Buck2는 Rust 네이티브이고 rust-analyzer와 호환되어 위 ①의 IDE 문제를 태생적으로 갖지 않지만,
외부 채택 사례와 생태계가 얕다. 준비사항이 Bazel과 동일하므로 지금 고르지 않는다.

이사 걸림돌은 `scripts/guard/build-coupling-baseline.sh`가 양방향 래칫으로 동결한다
(2026-07-29 실측: `build.rs` 1개, 컴파일 타임 파일 읽기 79곳). 영역 간 Cargo path 의존은
ADR-0001이 이미 금지하므로, 이사에서 가장 비싼 준비는 이미 끝나 있다.

### 발견을 자동화해 선언 자체를 없애기

"발견된 것을 전부 돌린다"면 대조할 필요가 없다. 그러나 그러면 **의도적으로 안 도는 것과 잊어서
안 도는 것을 구분할 수 없다.** 자격증명이 없어 못 도는 레인은 전자이고, dbt 스위트는 후자였다.
ADR-0010이 `Option<Integration>`의 `None`을 지운 이유와 같다 — 탈출구가 아니라 검사 가능한
주장이 필요하다.

### 언어별로 별도 가드 3개

읽기는 쉬워지지만 같은 불변식이 세 곳에 진술되고, 그중 하나만 고쳐지는 날이 온다. ADR-0004가
없앤 5벌 복제와 같은 형태다. 가드는 하나로 두고 발견기만 언어별로 나눈다.

### 가드가 Rust 소스를 grep 하는 대신 xtask가 선언을 JSON으로 내보내기

가장 엄격한 형태다. `cargo xtask manifest --json`이 `AREAS`를 기계가 읽는 데이터로 찍고 가드가
그것을 소비하면, 정규식이 소스 텍스트 형태에 의존하는 문제(ADR-0010 부채 #8: rustfmt 한 줄이
가드를 무력화했다)가 원리적으로 사라진다. 선언은 Rust가 소유하고 가드는 파생물만 읽는다.

**그러나 가드가 cargo에 의존하게 된다.** 지금 이 가드들은 툴체인 없이 도는 것이 설계다 —
ADR-0005가 "host 도구가 없으면 훅은 실패가 아니라 skip"을 정한 이유이고, `monorepo-guard.sh`가
pre-push에서 몇 초 만에 끝나는 이유다. JSON 경로를 택하면 가드 한 벌을 돌리는 데 xtask 빌드가
선행되어야 하고, 툴체인이 없는 환경에서는 가드 자체가 skip 되어 **검사가 없는 것과 같아진다.**
얻는 것(포매터 내성)보다 잃는 것(무조건 도는 저비용 검사)이 크다.

이 저장소 규모에서는 grep 기반 검사 + 접힌 줄 처리(ADR-0010이 부채 #8에서 추가한 방어) +
자기-테스트의 음성 사례가 실용적 균형점이다. 포매터가 형태를 또 바꾸면 자기-테스트가 먼저
빨개지도록 음성 사례를 계속 늘리는 쪽을 택한다. 규모가 커져 가드가 이미 cargo를 요구하는
지점(예: 모든 CI 잡이 툴체인을 설치하는 상태)에 도달하면 이 판단을 다시 본다.

### 규약 밖 이름에 예외 목록 두기

`no_dbt_forbidden_responsibilities.py`처럼 `test_` 접두가 없는 파일을 예외로 등록하면 발견기가
그 순간부터 불완전해진다. 파일 이름을 규약에 맞추는 쪽이 싸고 되돌릴 수 없는 결함을 남기지
않는다. 실제로 그 파일은 발견기에도 `unittest discover -p 'test_*.py'`에도 잡히지 않아
**테스트 3개가 여전히 아무 데서도 실행되지 않는 상태**였고, 이 ADR의 적용에서
`test_no_dbt_forbidden_responsibilities.py`로 개명했다. 규약을 넓히는 대신 이름을 맞추는 선택이
어떤 결과를 내는지 보여주는 사례다 — dbt 스위트의 실행 개수가 22에서 25로 늘었다.

## Consequences

- 새 테스트 파일을 추가하고 선언을 잊으면 CI가 실패한다. 깜빡할 수 없다.
- 새 언어를 들이면 발견기 하나를 더한다. 불변식과 가드는 그대로다.
- `cargo test`를 워크플로나 `scripts/`에 직접 쓸 수 없다. 레인을 통해야 한다.
- **이 결정을 적용하는 즉시 두 가지가 빨개진다.** 하나는 위의 모순 테스트이고, 다른 하나는
  identity의 `live_provisioning`이다 — 후자는 한 번도 실행된 적이 없으므로 처음 도는 순간의
  결과를 아무도 모른다. 둘 다 이 결정이 작동한다는 증거이지 부작용이 아니다.
- 실행 경로로 들어오는 테스트(실측):

  | 대상 | 개수 | 이전 |
  | --- | ---: | --- |
  | dbt 계약 테스트 | 25 | 어디서도 미실행 |
  | `foundation-api` 계약 테스트 | 1 | 어디서도 미실행 |
  | xtask 하네스 자기 테스트 | 10 | 어디서도 미실행 |
  | **합계** | **36** | |

  dbt 25개 중 3개는 파일 이름이 `test_*` 규약 밖이라 발견기에도 러너에도 잡히지 않던 것이다.
  이름을 규약에 맞춘 뒤 22 → 25가 됐다.
- `monorepo-guard.yml`은 `bash scripts/guard/monorepo-guard.sh`를 `cargo xtask verify tooling`으로
  교체한다. `verify`가 저장소 가드를 먼저 부르므로 둘을 나란히 두면 57개 가드가 한 잡에서 두 번
  돈다. 이 워크플로는 `paths:` 필터가 없어 xtask 변경이 트리거를 놓칠 수 없다.

## 남은 부채

1. **가드는 여전히 정적이다.** 정규식이 보는 텍스트 형태에 의존하므로 포매터가 그 형태를 바꾸면
   샌다. ADR-0010 부채 #8이 실제 사례다 — rustfmt가 `eprintln!(`를 문자열에서, `env::var("…")`를
   `.ok()`에서 떼어 놓자 두 규칙이 나란히 통과했다. 새 발견기도 같은 방식으로 뚫릴 수 있다.
   Docker `--network none`이 물리적 보강 후보로 남아 있다.

   > **부분 해소 (2026-08-05):** 보강이 들어갔다 —
   > [ADR-0010 남은 부채 4](./0010-live-resource-test-lanes.md)에 경위를 적었다. 가드는 여전히
   > 정적이고 이 항목이 예고한 대로 한 번 더 뚫렸다(`judgment-position-exit-codes`가 여러 줄 문자열
   > 내부를 코드로 읽었다). 달라진 것은 **뚫려도 나갈 곳이 없다는 것**이고, CI는 아직 이 차단 밖이다.
2. **TypeScript 발견 단위는 파일이 아니라 스크립트다.** 스크립트가 전부 호출되는 것은 증명하지만,
   그 스크립트의 설정이 모든 파일을 수집하는지는 증명하지 못한다. 0개 실행 판정은 스위트에 파일이
   하나라도 남아 있으면 발동하지 않는다.

   현재 이 저장소는 그 위험을 잘 다루고 있다 — `vitest.config.ts`가 5개 파일을 `exclude` 하고,
   `vitest.integration.config.ts`의 `include`가 그 5개를 이름으로 정확히 되받으며, 두 레인 모두
   `gongzzang-frontend.yml`에서 실행된다. 분할은 완전하다. 다만 **그 완전성을 증명하는 것은 없다.**
   두 설정의 합집합이 발견된 테스트 파일 전체와 같은지 대조하는 검사가 다음 단계다.

   > **해소 (2026-08-05):** 검사를 붙이기 전에 **거울을 만든 원인을 없앴다.**
   >
   > 두 설정이 같은 5개 파일을 이름으로 주고받은 이유는 그 파일들이 실제 Redis를 쓰면서
   > `tests/unit/`에 있었기 때문이다(다섯 개 모두 `getRedis()`로 `select`·`flushdb` 한다).
   > `tests/integration/`으로 옮기니 양쪽이 디렉터리 glob만 남고 **맞춰 둘 목록 자체가 사라졌다.**
   > 두 레인이 공유하던 `setup.ts`도 `tests/unit/` 아래 있었고 `tests/`로 옮겼다.
   >
   > 설정은 하나로 합쳤다. `test.projects`는 Vitest 자신의 다중 프로젝트 메커니즘이며
   > (`test.workspace`를 대체했다) 플러그인·별칭·setup을 한 번만 적게 한다. 두 번째 설정 파일이
   > 첫 번째와 어긋나는 경로가 없어졌다. 실행은 `--project unit` / `--project integration`으로
   > 나뉘므로 유닛 레인이 Redis를 건드리지 않는 성질은 그대로다.
   >
   > 이동 뒤에도 한 가지가 남는다 — **세 glob 밖에 놓인 `*.test.ts`**. 어느 프로젝트도 수집하지
   > 않고 두 레인 다 초록이다. `products/gongzzang/scripts/ci/vitest-lane-completeness.sh`가
   > `vitest list --filesOnly --json --static-parse`로 **Vitest의 수집기가 실제로 모은 집합**을
   > 받아 디스크의 테스트 파일 전체와 대조한다. include/exclude 의미를 여기서 다시 구현하면
   > 방금 없앤 거울을 되살리는 것이므로 하지 않는다. `--static-parse`(Vitest 4.1)는 파일을
   > 임포트하지 않고 파싱하므로 수집이 Redis에 붙지 않는다.
   >
   > 통과만이 아니라 **거부도 증명했다.** 규칙 밖에 테스트 파일을 하나 심으면 검사가 그 파일을
   > 이름으로 지목하며 실패하고, 지우면 다시 통과한다. 첫 실행에서는 통과하면서 개수를
   > `collected=49, discovered=50`으로 찍었다 — 마지막 개행이 없어 `wc -l`이 하나 적게 셌다.
   > 통과한 검사가 실패처럼 읽히는 것은 [ADR-0012](./0012-verification-results-must-mean-what-they-say.md)가
   > 다루는 결함이라 함께 고쳤고, `comm`이 로케일 차이로 조용히 틀린 차집합을 내지 않도록 양쪽을
   > `LC_ALL=C`로 정렬한다.
3. **자격증명이 없어 돌지 않는 레인은 이 ADR이 다루지 않는다.** ADR-0010 부채 #5의 대상이며,
   그 레인들에게는 정적 대조가 여전히 유일한 방어다.
4. **임포트 게이트는 여전히 없다.** 이 결함군이 들어온 경로 — 감사된 소스 스냅샷의 일괄 임포트,
   ADR-0010 Context가 지목한 그 유입 지점 — 는 지금도 검사 없이 열려 있다. ADR-0010 부채 #7 그대로다.
   이 ADR이 고친 모순 단언도 그 임포트 시점의 모습 그대로였다.
