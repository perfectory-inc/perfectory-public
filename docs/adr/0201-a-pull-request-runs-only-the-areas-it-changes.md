# ADR 0201: 풀 리퀘스트는 바꾼 영역의 검사만 돌린다

- Status: Accepted
- Date: 2026-10-13
- Builds on: [ADR-0004](./0004-verification-ssot.md), [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md)

## Context

2026-10-12~13 풀 리퀘스트 실행을 `gh run list --event pull_request`로 쟀다.

- 워크플로우 12개 가운데 11개가 `pull_request`에 경로 조건 없이 매번 전부 돈다. 경로 조건은 `push`에만 있었다.
  필수 검사(`tools/github/main-ruleset.json`)는 워크플로우가 아예 시작하지 않으면 영원히 대기하므로
  `pull_request: paths:`를 쓸 수 없었다.
- 풀 리퀘스트 한 건의 벽시계(첫 워크플로우 시작→마지막 종료)는 p50 23.4분, p90 31.3분이다.
  워크플로우별 p50은 Foundation 24.5, intelligence 11.5, identity 10.6, gongzzang CI 8.0,
  frontend 6.0, walking-skeleton 5.8, dawneer 5.8, db-migrations 5.5, sqlx-prepare 5.3분이다.
- 영역의 입력 경로는 각 워크플로우의 `push: paths:`에 흩어져 있었고, 두 가드
  (`xtask-path-coverage`, `area-ci-input-coverage`)가 그 YAML 목록을 직접 읽었다.
- `gongzzang-walking-skeleton.yml`이 실행하는 `products/gongzzang/scripts/ci/walking-skeleton-e2e.sh`처럼
  워크플로우가 이름으로 부르는 파일이 그 목록에 빠져 있어도 아무것도 막지 않았다.

## Decision

1. **영역→경로의 정본은 [`tools/ci/change-scope.json`](../../tools/ci/change-scope.json) 하나다.**
   범위가 있는 워크플로우는 `paths:` 필터를 갖지 않는다. 범위 없이 늘 도는 워크플로우는 `unscoped`에 이유와 함께 적는다.
2. **공통 형태 하나.** 범위가 있는 워크플로우는 늘 시작하고 첫 잡 `scope`가
   `python3 scripts/ci/change_scope.py select --workflow <자기 파일>`로 `affected`를 낸다. 다른 잡은
   `needs: [scope]`, `if: ${{ needs.scope.outputs.affected == 'true' }}`로만 돈다. 필수 잡은
   `CHANGE_SCOPE_RUN`을 받아 `scripts/ci/require-successful-needs.sh`에 넘긴다.
   그 스크립트는 `scope` 잡이 성공했고 답이 정확히 `false`일 때에만 `skipped`를 통과시킨다.
   그래서 영역을 건드리지 않은 풀 리퀘스트에서도 `required/*`는 몇 초 만에 성공으로 보고된다.
3. **암묵 입력.** 워크플로우 파일 자신, 워크플로우가 이름으로 부르는 저장소 파일(스크립트·계약),
   그리고 선택기와 계약 파일(바뀌면 전부 돈다)은 목록에 적지 않아도 입력이다.
4. **이벤트.** `workflow_dispatch`·`schedule`은 전부 돈다. `push`·`pull_request`는 바뀐 경로로 고른다
   (`base...head`, 머지 베이스 기준). 비교할 수 없으면(첫 푸시, git 실패) 전부 돈다.
   `merge_group`은 워크플로우마다 `full` 또는 `scoped`를 계약에 적는다. 서비스를 실제로 띄우는
   통합 레인(identity, db-migrations, walking-skeleton)은 목록이 놓친 입력에 대한 마지막 안전망으로 `full`이다.
5. **기계적 거부.** `required/repository`(monorepo-guard.yml)의 `python3 scripts/ci/test_change_scope.py`가 다음을 거부한다.
   계약에 없는 새 풀 리퀘스트 워크플로우, 범위 워크플로우에 남은 `paths:`, `scope`에 묶이지 않은 잡,
   `CHANGE_SCOPE_RUN`을 넘기지 않는 필수 잡, 추적 파일을 하나도 가리키지 않는 낡은 경로,
   `cargo xtask verify|integration`을 돌리면서 `tools/xtask/**`·`.cargo/config.toml`·`rust-toolchain.toml`을
   입력으로 받지 않는 워크플로우. `check-workflow-policy.sh`는 필수 잡의 그 한 줄 매핑만 추가로 허용한다.
6. `area-ci-input-coverage`는 이제 계약에서 입력을 읽는다. 범위 밖 워크플로우가 아직 `push: paths:`를
   가지면(Foundation) 그 목록을 읽는다.

## Consequences

- 한 영역만 바꾼 풀 리퀘스트는 다른 영역 워크플로우를 `scope` 잡 하나(체크아웃+diff, 수십 초)로 끝낸다.
- Foundation CI는 이번 결정에서 `unscoped`이다. 잡마다 자체 선택기(`scripts/ci/foundation_ci_scope.py`)가 있고,
  그 잡들은 다른 변경이 고치는 중이다. Foundation이 이 형태를 따르면 그때 `push: paths:`와
  `xtask-path-coverage.sh`를 지운다. 그 전까지는 Foundation이 풀 리퀘스트 벽시계를 결정한다.
- 워크플로우가 부르는 스크립트가 다시 부르는 파일(2단계 이상)은 암묵 입력이 아니다. 그런 입력은 계약에 적는다.

## Rejected alternatives

- **`pull_request: paths:`**: 필터에 걸리면 워크플로우가 시작하지 않아 필수 검사가 대기 상태로 남는다.
- **재사용 워크플로우·로컬 액션으로 `scope` 공유**: 워크플로우 정책이 로컬 액션을 허용하지 않는다(감사된 허용 목록만).
- **서드파티 경로 필터 액션(`dorny/paths-filter` 등)**: 결정 규칙이 YAML 안에 다시 흩어지고, 새 서드파티 의존이 생긴다.
  표준 라이브러리 파이썬 하나와 `git diff`로 충분하다.
- **각 잡의 단계마다 건너뛰기(Foundation 방식)**: 잡마다 러너가 뜨고 단계별 `if:` 허용 목록이 늘어난다.
