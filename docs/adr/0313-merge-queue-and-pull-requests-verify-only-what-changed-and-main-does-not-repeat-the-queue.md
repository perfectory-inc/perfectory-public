# ADR 0313: 머지 큐와 PR 은 바뀐 것만 검증하고, main 푸시는 큐가 통과시킨 트리를 다시 돌리지 않는다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0202](./0202-a-pull-request-runs-only-the-areas-it-changes.md)(영역 scope), [ADR-0207](./0207-a-pull-request-runs-only-the-guards-whose-inputs-it-changes.md)(가드 scope), [ADR-0269](./0269-a-pull-request-verifies-only-the-crates-it-reaches.md)(크레이트 선택), [ADR-0167](./0167-the-host-deploys-the-tree-the-merge-queue-passed.md)(큐 판정으로 배포)
- Amends: ADR-0202·0207·0269 의 "머지 큐는 언제나 전부 돌린다" 한 줄. 나머지는 그대로.

## Context

사장 원칙은 "모든 처리는 바뀐 것만, 자기 몫만"이다. CI 는 PR 에서만 그 원칙을 지켰고, 머지 큐·main
푸시·영역 안의 공용 단계는 매번 전부를 돌렸다. 2026-10-28 실측(최근 성공 15회, 벽시계 분):

| 무엇 | 측정 |
|---|---|
| 저장소 가드(`monorepo-guard.sh`) | `cargo xtask verify <영역>` 마다 실행 → 머지 그룹 하나에 6번. 크레이트 0개를 고른 문서 PR #870 에서 rust-quality 10.7분 중 9.3분(566 s) |
| 머지 큐 | `change_scope.py` 의 `ALWAYS_RUN_EVENTS` 에 `merge_group` → 문서 한 줄도 전 영역·전 크레이트·전 가드. Foundation CI 중앙값 16분(12–26), monorepo-guard 11분 |
| main 푸시 | 큐가 방금 통과시킨 **같은 커밋**(트리 `90f8ff5d…` 가 큐·푸시 양쪽 실행에 동일)을 다시 검증. Foundation CI 중앙값 17분(12–32), monorepo-guard 10분 |
| Postgres 통합 | 게이트 없음. #870(ADR 한 장)에서 크레이트 0개 선택 **뒤에** `apt-get` 4분 7초 + 마이그레이션 빌드 18초, 테스트 0개 |
| `always` 가드 | 이유만 적으면 됐다. 메모리 상한 가드 하나가 8.5분(느린 러너 16분) × 머지마다 6번(#895 가 0.05초로 고침) |

PR 쪽도 목표(1크레이트 PR ≤ 10분, 큐 포함 ≤ 20분)에 못 미쳤다: 1크레이트 PR
#867 14.8분, #852 14.3분, 문서 PR #870 10.8분.

### 큰 프로젝트는 머지 큐에서 무엇을 돌리나

| 프로젝트 | 커밋 전 게이트 | 전체 안전망 |
|---|---|---|
| rust-lang bors/homu | 큐에서 전체 스위트(단일 롤업) — 대신 롤업으로 PR 여럿을 한 번에 | 없음(큐가 곧 전체) |
| Kubernetes prow | `run_if_changed` / `skip_if_only_changed` 로 presubmit 잡을 경로로 고르고, tide 머지 풀도 같은 선택 | periodic 잡(주기 전체) |
| Bazel/Buck (Google TAP, Meta) | 타깃 결정(target determination): 바뀐 파일 → 역의존 타깃만 | TAP 포스트서브밋·주기 전체 빌드 |
| Chromium CQ | `analyze` 로 영향 받은 타깃만 빌드·테스트 | CI(포스트서브밋) 빌더가 전체 |

공통점: 커밋 전 게이트(큐 포함)는 **영향 범위만** 돌리고, 선언되지 않은 결합은 **주기적 전체 실행**이 잡는다.
bors 의 "큐에서 전부"는 저장소 하나가 한 빌드인 Rust 의 형태에서 나온 것이고 우리처럼 영역·크레이트 선택기가
이미 있는 모노레포에는 맞지 않는다.

## Decision

1. **저장소 가드는 CI 실행 하나에 한 번.** `cargo xtask verify <영역>` 은 가드를 돌리지 않는다. 가드는
   `required/repository`(`monorepo-guard.yml` 의 `cargo xtask verify tooling`) 와 `verify all` 에서만 돈다.
   그 잡은 PR·큐·푸시·일일 실행 모두에서 돈다.
2. **머지 큐는 PR 처럼 좁힌다.** `merge_group` 은 `ALWAYS_RUN_EVENTS` 에서 빠지고, 영역(ADR-0202)·크레이트
   (ADR-0269)·가드(ADR-0207) 선택이 모두 `merge_group.base_sha...head_sha` 로 비교한다. 큐의 head 는 그 base
   (큐가 올라선 main 끝)에 앞선 큐 항목들과 이 PR 을 더한 것이므로, 이 비교는 main 에 아직 없는 모든 변경을
   담는다. 비교할 수 없으면(이력 없음, git 실패) 지금처럼 전부 돈다.
3. **안전망은 매일 밤 전체 실행.** 수동 실행과 `schedule` 은 계속 전부를 돈다. 큐가 좁히는 scoped 워크플로는
   모두 `schedule:` 을 가진다 — `change_scope.py check` 가 없는 것을 거부한다(이번에 frontend·sqlx-prepare·
   walking-skeleton 에 매일 17:41 UTC 를 더함). 저장소 가드는 매일 21:17 UTC 에 전부 돈다.
4. **main 푸시는 큐가 같은 트리에서 통과시킨 일을 다시 하지 않는다.** 푸시에서 scope 선택기는 그 워크플로의
   최근 성공한 `merge_group` 실행 중 `head_commit.tree_id` 가 이 커밋의 트리와 같은 것을 찾는다(읽기 전용
   `github.token`, 공개 데이터). 있으면 `affected=false` 로 모든 게이트 잡이 건너뛰고 `required/*` 는 초록,
   가드 선택기는 가드 0개를 고른다. 없거나 읽을 수 없으면 지금처럼 경로로 고른다(안전한 쪽). 배포는 이미
   큐의 판정으로 나간다(ADR-0167) — 푸시 실행은 판정을 바꾸지 못했고, 실패했다면 배포를 막을 뿐이었다.
   - 남는 것: monorepo-guard 푸시의 캐시 채우기(고정 바이너리·xtask rust-cache)와 xtask 자체 검사 1–2분.
   - 대가: main 의 rust-cache 는 이제 매일 밤 전체 실행이 저장한다(전에는 푸시마다). sccache(R2,
     [ADR-0294](./0294-ci-builds-share-a-compiler-cache-on-r2.md))가 워크스페이스 크레이트를 덮으므로 PR 은 의존성 캐시만 하루치 늦다.
5. **Foundation CI 도 영역 scope 를 쓴다.** `foundation-ci.yml` 은 `unscoped` 에서 `workflows` 로 옮기고
   `scope` 잡을 가진다. 입력은 지난 push `paths:` 목록에, `change_scope.py check` 가 소스에서 찾아낸 빠진
   입력 14개(`scripts/github/**`, `tools/github/**`, `tools/technology-versions.contract.json` 등)를 더한 것.
   영역 안에서는 크레이트 선택이 게이트다:
   - Postgres 통합: 크레이트 0개면 rust-cache·DB 클라이언트·마이그레이션·시드·통합 테스트를 하지 않는다.
   - Catalog OpenAPI 내보내기: `foundation-api` 가 검증 대상일 때만(그 내보내기가 그것을 컴파일한다).
   - xtask 의 `apt-get` 은 크레이트 선택 **뒤**, 고른 것이 있을 때만(`verify --part cargo`, 통합 레인).
6. **`always` 가드는 시간 예산을 선언한다.** `tools/ci/change-scope.json` 의 `always` 항목은 이유와 함께
   `budget_seconds`(1–60, CI 러너 기준)를 가진다. `guard_scope.py check` 가 없거나 범위 밖이면 거부하고,
   `monorepo-guard.sh` 는 CI 에서 가드마다 걸린 시간을 찍고 예산을 넘으면 그 실행을 실패시킨다. 60초가 넘게
   필요한 가드는 `paths` 를 선언해 바뀐 때만 돈다. 막는 사고: 메모리 상한 가드 8.5분 × 머지마다 6번(2026-10-28).

## Consequences

- 필수 체크 이름(`required/foundation`, `required/repository` 등)은 그대로 보고된다. 건너뛴 게이트는
  `require-successful-needs.sh` 가 scope 의 명시적 `false` 일 때만 받아들인다(ADR-0202 그대로).
- 위험: 어떤 계약에도 선언되지 않은 결합(경로 A 의 변경이 영역 B 를 깨뜨림)은 큐가 더는 잡지 못하고 그날 밤
  전체 실행이 잡는다. 그 사이 main 에 들어간 커밋은 배포될 수 있다. 완화: 어떤 영역도 주장하지 않는 경로는
  여전히 전부를 돌리고(ADR-0202), 영역 소스가 따옴표로 읽는 파일은 입력이어야 한다(`change_scope.py check`).
  밤 실행이 실패하면 그 회귀를 만든 입력을 계약에 더하는 것이 고침이다.
- 위험: 읽기 전용 토큰이 Actions 실행 목록을 못 읽으면(403) 푸시는 지금처럼 돈다 — 느릴 뿐 틀리지 않는다.
- 측정: PR #(이 변경)의 실측 전후 표를 PR 본문에 둔다.
