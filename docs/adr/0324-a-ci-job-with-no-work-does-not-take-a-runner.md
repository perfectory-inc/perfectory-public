# ADR 0324: 할 일이 없는 CI 잡은 러너를 잡지 않는다

- Status: Accepted
- Date: 2026-10-29
- Builds on: [ADR-0313](./0313-merge-queue-and-pull-requests-verify-only-what-changed-and-main-does-not-repeat-the-queue.md)(큐·PR 은 바뀐 것만), [ADR-0202](./0202-a-pull-request-runs-only-the-areas-it-changes.md)(영역 scope), [ADR-0269](./0269-a-pull-request-verifies-only-the-crates-it-reaches.md)(크레이트 선택), [ADR-0199](./0199-verify-runs-side-suites-beside-cargo-and-pull-requests-leave-the-boundary-slice-to-the-merge-queue.md)(Foundation 게이트 선택)
- Amends: ADR-0202 의 "건드리지 않은 영역도 `required/*` 잡이 돌아 성공을 보고한다"와 ADR-0199 의 "고르지 않은 게이트 잡도 러너에서 끝까지 돌아 성공한다". 선택 규칙 자체는 그대로.

## Context

ADR-0313 뒤 1크레이트 PR(#907)의 **일**(임계경로)은 약 6분인데 벽시계는 12.3분이었다. 잡마다 러너를
20–250 s 기다렸다. 조직은 GitHub Free(동시 잡 20개, 표준 러너)이고 사장 원칙은 CI 비용 0 이다 — 러너를
늘릴 수 없으니 **잡 수**를 줄여야 한다.

Foundation 쪽 1크레이트 PR 하나가 러너를 잡는 잡(계산, #881 의 체크 목록 + ADR-0313 의 Foundation scope 잡):

| 묶음 | 잡 | 그중 할 일이 없는 것 |
|---|---|---|
| Foundation CI | scope + 게이트 9 + required = 11 | 고르지 않은 게이트 5–6개(Windows·Spark·Compose·Boundary·Suites·Supply-chain): 러너를 받아 체크아웃하고 "선택 안 됨"을 찍고 끝 |
| 다른 scope 워크플로 8개 | scope + required = 16 | `required/*` 8개: 게이트가 모두 건너뛰었다는 것을 확인하려고 러너 하나씩 |
| 영역 없는 워크플로 | docs·repository·secrets = 3 | — |
| 합 | **30** (동시 한도 20 초과) | **13–14** |

#907 실측: `required/foundation` 은 게이트가 끝난 뒤 러너를 **3분 13초** 기다렸고 일은 6초였다. 할 일 없는
게이트(Boundary 9 s·Compose 8 s·Windows 21 s 의 일)도 각각 0.3–4분을 기다렸다 — 그 대기 동안 진짜 일
(Postgres·Rust)의 러너를 빼앗았다.

ADR-0202 는 이미 "각 잡의 단계마다 건너뛰기(Foundation 방식): 잡마다 러너가 뜬다"를 기각했다. 그 결정이
Foundation 게이트와 `required/*` 에는 아직 적용되지 않았다.

### 큰 프로젝트는

| 프로젝트 | 할 일이 없는 잡 |
|---|---|
| Kubernetes prow | `run_if_changed` 로 고르지 않은 presubmit 은 **트리거되지 않고**, tide 는 필수 컨텍스트 중 트리거되지 않은 것을 통과로 본다 |
| GitHub Actions 문서 | 잡 단위 `if:` 로 건너뛴 잡은 체크가 "Skipped" 이고 **필수 체크여도 성공으로 친다** — 경로 필터로 워크플로를 통째로 건너뛰는 것(체크가 영원히 대기)과 다르다 |
| rust-lang (bors → GitHub merge queue) | 잡 계산(`citool calculate-job-matrix`)이 먼저 돌아 이번 실행의 잡 목록을 만들고, 목록에 없는 잡은 러너를 받지 않는다 |
| Chromium CQ | `analyze` 가 영향 없다고 한 빌더는 트리거하지 않는다 |

공통점: **고르는 일은 한 번, 고르지 않은 잡은 스케줄되지 않는다.**

## Decision

1. **고르는 일은 scope 잡이 한 번 한다.** Foundation 의 `scope` 잡은 `foundation_ci_scope.py --skipped-jobs` 로
   할 일이 없는 잡 목록을 JSON 출력 `skipped` 로 낸다. 기준은 이미 있는 선택기 그대로다:
   - 게이트 5개(boundary-slice·kafka-integration·compose-smoke·spark-runtime·static-release-toolchain-windows):
     `RULES`·`MERGE_QUEUE_ONLY`(ADR-0199).
   - rust-quality·postgres-integration: `change_scope.py crates` 가 `some` 이고 `affected` 줄이 없을 때(ADR-0269).
   - suites: `change_scope.py suites` 가 `some` 이고 테스트·Node 줄이 없을 때.
   - supply-chain: 워크스페이스의 `Cargo.toml`·잠금 파일·`deny.toml`·`tools/cargo-deny.env`·`tools/syft.env`·
     `tools/xtask/**` 가 바뀌지 않았고 크레이트 선택이 `all` 이 아닐 때(`SUPPLY_CHAIN_RULE`). 권고(advisory)는
     커밋 없이 바뀌므로 밤 전체 실행과 `dependency-advisories.yml` 이 맡는다.
   비교할 수 없거나 선택기 답을 못 읽으면 그 잡은 돈다(안전한 쪽). 각 잡은 `if:` 에서
   `!contains(fromJSON(needs.scope.outputs.skipped), '<잡 id>')` 를 읽는다. 잡 안의 단계별 선택·가드는 그대로
   남아 같은 답을 다시 확인한다.
2. **선택기의 자기 시험은 고르는 곳에서 돈다.** `test_foundation_ci_scope.py`·
   `test_foundation_postgres_production_shape.py` 는 rust-quality·postgres 잡에서 scope 잡으로 옮긴다 —
   그 잡들이 건너뛰어도 선택기와 production-shape 게이트의 시험은 돈다.
3. **건드리지 않은 영역의 `required/*` 는 건너뛴다.** scope 잡이 있는 워크플로의 터미널 잡은 정확히
   `if: ${{ always() && (needs.scope.result != 'success' || needs.scope.outputs.affected == 'true') }}` 다.
   scope 가 성공하고 `false` 라고 답했을 때만 건너뛰고(GitHub 은 필수 체크로 통과), scope 가 실패·취소되면
   돌아서 실패한다. 영역이 바뀌면 지금처럼 `always()` 로 모든 결과를 모은다.
4. **결과 검사는 scope 가 이름 댄 잡만 건너뛰었다고 받아들인다.** `require-successful-needs.sh` 는
   `CHANGE_SCOPE_SKIPPED`(scope 잡의 목록)를 받는다: 형식이 `["job-id", ...]` 가 아니거나, scope 답 없이
   왔거나, scope 잡 자신을 이름 대면 실패. 목록에 없는 잡이 `skipped` 면 지금처럼 실패.
5. **검사기**: `check-workflow-policy.sh` 는 scope 잡이 있는 워크플로의 터미널 잡에 3번의 정확한 `if:` 를
   요구하고 `CHANGE_SCOPE_SKIPPED` 매핑을 한 번만 허용한다. `change_scope.py check` 는 scope 잡이 `skipped`
   를 내면 모든 게이트 잡이 **자기 id** 로 그것을 읽고 터미널이 그것을 넘기는지 확인한다. 막는 사고: #907 의
   `required/foundation` 6초 일에 3분 13초 대기, 할 일 없는 게이트들이 진짜 일의 러너를 빼앗음.
6. `configure-public-repository.sh` 의 "main 에서 필수 컨텍스트가 초록인가" 확인은 `skipped` 도 초록으로 본다
   (GitHub 판정과 같게). 자동 배포(`release_checks.py`)와 `service-checks.py` 는 이미 그렇다.

## Consequences

- 1크레이트 Foundation PR(#907 과 같은 변경)이 러너를 잡는 잡: 30 → 16(계산). 동시 한도 20 아래로 내려온다.
  문서만 바꾼 Foundation PR 은 Foundation 잡 11 → 2(scope·required) — rust-quality·postgres 도 건너뛴다.
- main 푸시가 큐 판정으로 건너뛸 때(ADR-0313 §4) 모든 `required/*` 는 러너 없이 Skipped 로 남는다. 배포
  판정은 Skipped 를 통과로 본다(`PASSING`).
- Windows 잡의 "고정 바이너리 채우기"는 이제 그 잡이 골라졌을 때나 밤 전체 실행에서만 돈다(전에는 영역이
  바뀐 모든 푸시). 키가 고정 목록의 해시라 하루 늦어질 뿐이다.
- 위험: 잡 단위 `if:` 의 표현식이 틀리면 잡이 조용히 건너뛸 수 있다. 완화: 결과 검사(4)가 목록에 없는
  건너뜀을 실패로 만들고, `change_scope.py check` 가 표현식을 정확한 문자열로 고정한다.
- 측정(탐침 #916 전·#917 후, #907 과 같은 1크레이트 변경, 같은 시각에 나란히): 러너를 잡은 잡 30 → 16,
  러너 대기 합 8,609 s → 4,074 s, 벽시계 17.8 → 14.2분. 두 탐침 모두 다른 PR 25개가 동시에 돌던 때라 잡마다
  중앙값 4–5분을 기다렸다 — 남은 벽시계는 조직 전체의 러너 경합이고, Foundation 임계경로의 일은
  scope 23 s + Rust 325 s + required 8 s ≈ 6분이다.
