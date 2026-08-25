---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-25
---

# ADR 0056: Foundation의 무거운 게이트는 소유 입력이 바뀔 때만 일을 한다

- Status: Accepted
- Date: 2026-08-25
- 관련: [ADR-0004 검증 SSOT](./0004-verification-ssot.md),
  [ADR-0011 테스트 실행 집합 완전성](./0011-test-execution-set-completeness.md),
  [ADR-0012 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md),
  [ADR-0054 정적 릴리스 도구 신원 계약](./0054-static-release-tools-have-one-executable-identity-contract.md)

## Context

`foundation-ci.yml`의 `boundary-slice`, `kafka-integration`, `compose-smoke`,
`static-release-toolchain-windows`는 실제 변경과 무관하게 모든 PR에서 컨테이너·도구 설치를 시작했다.
네 잡은 각각 약 5분 11초, 4분 38초, 3분 37초, 6분 5초가 들었고 병렬 실행과 터미널
`required/foundation` 판정을 포함한 한 실행은 약 11분이었다. 병목은 Rust 컴파일 캐시가 아니라
Postgres·Martin·Kafka·Schema Registry·Compose 기동과 Windows 도구 다운로드였다.

워크플로 전체에 `pull_request.paths`를 거는 것은 해결이 아니다. GitHub의
[필수 검사 문제 해결 문서](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks)는
경로 필터 때문에 워크플로가 생략되면 연결된 필수 검사가 `Pending`에 남아 병합을 막는다고 명시한다.
반면 [잡 조건 문서](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/control-jobs-with-conditions)는
조건으로 생략된 잡을 GitHub UI에서 성공으로 보고한다고 설명한다. 그러나 이 저장소의 더 강한
터미널 계약은 `needs.<job>.result`의 `success`만 받는다. `scripts/ci/require-successful-needs.sh`는
`skipped`, `failure`, `cancelled`를 모두 실패시킨다. GitHub 표시와 이 저장소 집계기의 입력을 같은
사실로 간주하면 필수 검사를 무력화하거나 정상 PR을 막을 수 있다.

검토한 재사용 후보는 GitHub 기본 `paths`/잡 조건과
[`dorny/paths-filter`](https://github.com/dorny/paths-filter)다. 전자는 필수 워크플로 전체를 생략하는
경우를 해결하지 못하고, 후자는 변경 경로 계산을 제공하지만 어떤 Foundation 구성 요소가 어느
게이트를 소유하는지와 필터 누락을 잡는 독립 witness를 제공하지 않는다. 저장소의 Action 허용 목록에
새 제3자 실행 코드를 추가해도 이 도메인 계약은 별도로 작성해야 한다. 따라서 기존
`actions/checkout`, Git, Python 표준 라이브러리 위에 분류 제어 계층만 둔다.

## Decision

1. PR의 `pull_request` 트리거는 경로 필터 없이 항상 실행한다. `push.paths`는 기존대로 유지한다.
2. `scripts/ci/foundation_ci_scope.py`가 네 무거운 게이트의 경로 분류 SSOT다. Git diff는 rename을
   감지하지 않게 하여 이전 경로와 새 경로를 삭제·추가 두 입력으로 읽는다. 비교 기준이 없거나 Git
   비교가 실패하면 최적화를 포기하고 네 게이트를 전부 실행한다.
3. 기존 네 잡 ID를 유지하고 잡 수준 `if`를 쓰지 않는다. 각 잡은 전체 이력을 checkout한 뒤 분류기를
   호출한다. 해당 없는 잡의 나머지 모든 step은 성공 코드 0으로 즉시 끝난다. 따라서 터미널 잡이 보는
   결과는 `skipped`가 아니라 실제 `success`이며 `required/foundation`의 일곱 `needs`와 일곱
   `REQUIRED_RESULT_*` 매핑은 바뀌지 않는다.
4. `Cargo.lock`, `pnpm-lock.yaml`, `rust-toolchain.toml`, `.cargo/config.toml`, Foundation workspace
   `Cargo.toml`, 워크플로 자신, 분류기 자신은 영향 범위를 좁힐 수 없으므로 네 게이트를 모두 실행한다.
5. `boundary-slice`는 `scripts/tiles/**`, `serving_postgis`를 포함한 모든 Foundation migration,
   Foundation API migration runner, 경계·타일 발행기, 발행기가 직접 사용하는 object storage와 그
   catalog/lakehouse/normalization 계약을 감시한다.
   새 파일이 자동으로 포함되도록 파일 열거 대신 소유 디렉터리 prefix를 쓴다.
6. `kafka-integration`은 실제 Kafka harness, Redpanda/Karapace Compose, `foundation-outbox`, Avro schema,
   migration runner와 해당 outbox의 catalog/lakehouse/shared 계약, xtask live lane을 감시한다. 해당 없는
   변경에서 Postgres조차 시작하지 않도록 GitHub job service를 분류 뒤의 digest-pinned `docker run`으로
   옮긴다.
7. `compose-smoke`는 루트 Compose와 포함 파일, `.dockerignore`, compose 제어 셸, bootstrap/grant/finalize SQL,
   migration, Foundation API Dockerfile·소스와 그 Rust 의존 구성 요소를 감시한다.
8. Windows 게이트는 필수 집계에서 빼지 않는다. ADR-0054가 Linux와 Windows의 서로 다른 upstream
   archive·실행 파일 hash·배너를 실제 배포물로 검증하도록 요구하기 때문이다. 운영자 플랫폼 증거를
   비필수로 낮추는 대신, 정적 릴리스 계약·설치기·embedded verifier·명령 진입점·toolchain 입력이
   바뀌지 않으면 도구 다운로드와 Rust 빌드만 생략한다.
9. `scripts/ci/test_foundation_ci_scope.py`는 각 소유 범주의 독립 witness, lock/workflow 전부 실행,
   무관 경로 전부 생략, rename 양쪽 경로, 수동 실행의 전부 실행, 워크플로의 일곱 집계를 검사한다.
   분류 route 하나를 의도적으로 제거한 복사본이 `validate_rules`에서 실패하는 mutation test를 네
   게이트 각각에 둔다. `rust-quality`가 이 테스트를 직접 호출하고
   `scripts/guard/foundation-ci-scope-self-test.sh`도 monorepo guard에 연결해 이
   증명이 다른 PR에서도 항상 실행되게 한다.
10. `scripts/guard/workflow-policy-self-test.sh`는 터미널 집계기가 `skipped`와 `failure`를 모두
    거부하는지 검사한다. 기존 workflow policy는 터미널 잡이 모든 비터미널 잡을 정확히 한 번 `needs`와
    결과 환경변수에 매핑하는지 계속 검사한다.

## Rejected alternatives

- `on.pull_request.paths`는 필수 context 자체를 만들지 않아 병합을 무기한 막는다.
- 무거운 잡에 잡 수준 `if`를 걸고 집계기에서 모든 `skipped`를 성공으로 바꾸면, 경로 판정에 의한
  의도된 생략과 upstream 실패 때문에 생긴 생략을 터미널 경계에서 구분하지 못한다.
- 네 실행 잡과 네 성공 wrapper 잡을 따로 만들면 현행 workflow policy의 “모든 비터미널 잡을 집계”
  계약과 일곱 결과 표면을 불필요하게 넓힌다.
- 개별 파일 목록은 새 발행 모듈·migration·Dockerfile이 추가될 때 기본값이 미감시이므로 쓰지 않는다.
- Windows 게이트를 `required/foundation`에서 빼면 ADR-0054의 지원 플랫폼 실물 증거가 운영자 선택
  검사로 약해진다. 6분 비용의 원인은 필수 여부가 아니라 무관 변경에서도 설치·빌드한 것이었다.

## Consequences

Foundation과 무관하거나 네 무거운 구성 요소와 무관한 PR에서는 각 잡이 checkout과 Python 분류만
수행하고 `success`로 끝난다. 기존 11분 실행의 임계 경로는 약 2분 19초인 supply-chain·Rust·Postgres
게이트 쪽으로 이동하므로 GitHub runner queue 변동을 제외하면 3분 안팎, 약 8분 단축을 예상한다.
관련 경로, lockfile, CI 정의 변경에서는 기존 네 실물 검증을 모두 실행하므로 최악 실행 시간은 줄지
않는다.

로컬에서는 GitHub-hosted Windows runner와 Actions service lifecycle을 그대로 실행할 수 없다.
따라서 분류·mutation·집계·workflow wiring은 단위 테스트와 actionlint/workflow policy로 검증하고,
실제 Windows archive와 네 잡의 Actions 상태·소요 시간은 이 변경의 PR 실행에서 확인한다.
