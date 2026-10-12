# ADR 0199: verify 는 파이썬·노드 시험을 cargo 옆에서 돌리고, PR 은 경계 슬라이스를 머지 큐에 맡긴다

- Status: Accepted
- Date: 2026-10-13
- Amends: [ADR-0004](./0004-verification-ssot.md) (verify 의 단계 순서), [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) (머지 큐가 판정)

## Context

2026-10-12 Foundation Platform CI 실측: `pull_request` 중앙 21.4분(최대 25.6), `merge_group` 22.4분 —
PR 하나가 병합까지 약 45분을 기다렸다. 같은 날 성공한 PR 실행 하나의 단계별 시간:

| 잡 / 단계 | 시간 |
| --- | --- |
| `Boundary vertical slice` — 컨테이너 안에서 퍼블리셔를 캐시 없이 컴파일 | 884초 |
| `Rust fmt, unit tests, clippy` — `cargo xtask verify foundation` | 895초 |
| 　└ 가드 85초 · clippy 62초 · 시험 빌드 143초 · Rust 시험 약 210초 | 약 500초 |
| 　└ 그 **뒤에** 파이썬 묶음 4개(그중 `orchestration/tests` 394개가 277초) | 약 300초 |
| 　└ 그 **뒤에** 노드 묶음 4개(pnpm install·typecheck·vitest·build) | 약 90초 |
| `Postgres integration tests` | 407초 |
| `Spark and Iceberg runtime tests` | 447초 |

`orchestration/tests` 의 277초는 한 시험이 아니라 2–30초짜리 하위 프로세스 시험의 긴 꼬리였다(서버에서
시험별 측정). 파이썬·노드 묶음은 cargo 가 만든 것을 읽지도, cargo 가 읽는 것을 쓰지도 않는데 cargo 가 끝난
뒤에야 시작했다.


## Decision

1. **CI 는 `verify <area>` 를 두 반쪽으로 나눠 두 러너에서 동시에 돌린다:
   `cargo xtask verify foundation --part cargo`(가드·fmt·clippy·Rust 시험)와 `--part suites`(파이썬·노드
   묶음).** 같은 러너 안에서 동시에 돌리는 것부터 시험했고 실패했다(아래 측정). 4 vCPU 를 나눠 쓰자
   시험 빌드가 143초 → 209초, Rust 시험이 약 3.5분 → 5분으로 늘어 verify 는 727초에 머물렀다. 일이 CPU
   에 묶여 있으니 기계를 늘려야 줄어든다. 묶음 목록·명령은 여전히 xtask 한 곳에 있고, 저장소 가드는 cargo
   반쪽에서 한 번만 돈다. `required/foundation` 은 새 잡 `suites` 까지 아홉 개를 요구한다. xtask 시험
   `every_workflow_that_splits_verify_runs_both_halves` 가 한쪽만 부르는 워크플로를 거부한다(일부러 한쪽을
   지워 실패를 확인했다).
2. **로컬의 `verify <area>`(인자 없음)는 그대로 한 명령이고, 두 반쪽을 한 기계에서 동시에 돌린다**
   (`run_beside`). 실패는 하나의 `FailureLog` 에 모이고(ADR-0012 규칙 1), 반쪽 스레드가 패닉하면 그 자체가
   실패다. 시험 `the_side_suites_run_beside_cargo_and_report_into_one_log` 는 두 쪽이 서로의 신호를 기다리게
   해서 순서대로 돌리면 10초 시간 초과로 실패한다.
3. **`unittest discover` 묶음은 시험 모듈마다 프로세스 하나로, 네 개씩 동시에 돈다.** `orchestration/tests`
   한 프로세스가 277초였다 — 2–30초짜리 하위 프로세스 시험의 긴 꼬리. 나누는 단위는 discover 가 실제로
   읽는 파일(시작 디렉터리 바로 아래 패턴 일치 파일)과 같고, 시작 디렉터리에 패키지(`__init__.py`)가 있으면
   한 프로세스로 둔다. 모듈마다 실패를 기록하고 `Ran N tests` 합계로 0개 검사를 그대로 한다(시험
   `a_discover_suite_splits_into_the_modules_discover_would_load`). 이 분할이 숨은 순서 의존 둘을 드러냈다:
   Spark 시험 두 모듈이 같은 프로세스에서 먼저 실린 다른 모듈의 PySpark 대역에 기대 import 되고 있었다. 이제
   스스로 대역을 싣는다.
4. **PR(`pull_request`)은 `boundary-slice` 를 머지 큐에 맡긴다.** `scripts/ci/foundation_ci_scope.py` 의
   `MERGE_QUEUE_ONLY` 가 그 목록이다. `merge_group`·main 푸시·수동 실행은 지금과 똑같이 경로로 고른다 —
   경계 슬라이스 없이 main 에 들어가는 변경은 없다. 잡 이름과 `required/foundation` 은 그대로이며, PR 에서
   그 잡은 이유를 출력하고 성공한다.

검토한 대안:

- **경계 슬라이스의 컨테이너 빌드를 캐시한다**(target 볼륨을 `actions/cache` 로, 또는 sccache). 저장소
  캐시 한도 10GB 에서 퍼블리셔 디버그 산출물 몇 GB 를 따로 들고 있어야 하고, 컨테이너와 러너의 경로·glibc
  가 달라 러너의 rust-cache 를 공유하지 못한다. 원격 컴파일 캐시(sccache+R2)는 비용 판단이 필요한 다음 단계다.
- **같은 러너 안 동시 실행만.** 위 1항의 측정으로 기각.
- **pytest-xdist.** 대부분의 묶음은 unittest 이고 러너에 xdist 가 없다. 모듈 단위 프로세스가 표준
  라이브러리만으로 같은 효과를 낸다.

## Consequences

- 측정(전/후)은 이 변경의 PR 본문에 적는다.
- `suites` 잡은 러너 하나를 더 쓴다(공개 저장소라 비용 없음, 동시 실행 한도 안).
- 머지 큐는 여전히 경계 슬라이스(약 15분)를 기다린다. 그것을 줄이는 일(컨테이너 빌드 캐시, 퍼블리셔 분할)은
  다음 단계다.
- 경계 슬라이스만 깨뜨리는 PR 은 이제 머지 큐에서 처음 실패한다. 그때 드는 비용은 큐 한 바퀴다.
