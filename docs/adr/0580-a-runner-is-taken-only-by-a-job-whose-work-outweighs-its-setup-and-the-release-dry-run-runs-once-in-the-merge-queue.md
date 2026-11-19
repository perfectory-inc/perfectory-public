# ADR 0580: 러너는 준비보다 일이 큰 잡만 잡는다 — 작은 정책 검사는 영역의 Rust 잡 단계이고, 릴리스 빌드 시험은 머지 큐에서 한 번 돈다

- Status: Accepted
- Date: 2026-11-19
- Builds on: [ADR-0324](./0324-a-ci-job-with-no-work-does-not-take-a-runner.md)(일 없는 잡은 러너를 잡지 않는다),
  [ADR-0199](./0199-verify-runs-side-suites-beside-cargo-and-pull-requests-leave-the-boundary-slice-to-the-merge-queue.md)(PR 은 경계 슬라이스를 큐에 맡긴다),
  [ADR-0420](./0420-ci-capacity-is-not-taken-from-the-production-host-and-a-self-hosted-runner-runs-only-trusted-events-on-a-dedicated-machine.md)(동시 상한 20, 유료·자체 러너 없음)
- Amends: [ADR-0345](./0345-the-release-image-is-built-before-merge-and-a-host-behind-main-alerts-with-its-reason.md) 의 "PR 과 머지 큐가 릴리스 빌드 시험을 돈다" 한 줄 — 이제 머지 큐만. 선택 규칙(릴리스 워크플로의 scope)은 그대로.

## Context

소유자는 GitHub 요금제를 바꾸지 않는다(Free, 표준 러너 동시 20). 그러니 대기를 줄이는 길은 **러너를 잡는 잡의 수와 시간**뿐이다.

2026-11-18 22:17Z ~ 11-19 00:19Z 의 완료 실행 286개(작업 661개)를 쟀다(`gh api .../runs/{id}/jobs`, 읽기 전용).

| 지표 | 값 |
|---|---:|
| 작업 시간 비율 | 대기 42% · 준비 11% · 일 46% |
| 러너 대기 p50 / p90 | 42 s / 279 s |
| 러너를 잡은 작업 중 일이 60 s 미만인 것 | 501 / 661 |
| PR 시도 하나(같은 head) 러너·분 p50 | 18.0 |
| 머지 큐 항목 하나 러너·분 p50 / 작업 수 p50 | 92.8 / 32 |

할 일이 있는데도 러너 하나를 혼자 잡는 작은 잡이 영역마다 남아 있었다. 모두 그 영역의 Rust 잡과 **같은 준비**(체크아웃·고정 바이너리·Rust 설정·같은 의존성 캐시 읽기)를 되풀이했다.

| 워크플로 | 잡 | 일(평균) | 준비(평균) | 러너 대기 p50 |
|---|---|---:|---:|---:|
| identity-ci | Identity supply-chain policy | 4 s | 37 s | 154 s |
| identity-ci | Identity OpenAPI determinism | 8 s | 28 s | 161 s |
| dawneer-ci | Dawneer supply-chain policy | 4 s | 28 s | 156 s |
| intelligence-ci | Intelligence supply-chain policy | 6 s | 41 s | 45 s |
| CI (gongzzang) | cargo-deny | 5 s | 47 s | 42 s |
| CI (gongzzang) | Repo guardrails | 12 s | 40 s | 69 s |

이 여섯 잡은 창 안에서 러너를 62번 잡았다. 그 러너·분 48.8분 가운데 일은 7.0분이었고, 러너를 기다린 시간은 145.7분이었다.

릴리스 빌드 시험(`release-image`, 일 645~725 s)은 PR 에서 한 번, 머지 큐에서 다시 한 번 돌았다. 큐 실행이 PR 이 증명한 트리를 그대로 다시 돌린 것이 아니다. 머지 큐 head 트리가 그 워크플로의 초록 PR 실행 트리와 같았던 것은 큐 head 91개 중 2개였다(2026-11-13~19, `head_commit.tree_id` 대조). 그러니 판정은 큐의 것이 정본이고, PR 의 시험은 이른 알림일 뿐이다. 창 안에서 PR 시험 4번이 러너 33분을 썼다.

동시 실행 취소(`cancel-in-progress`)는 이미 모든 워크플로가 `pull_request` 에만 켜고 `merge_group`·main 에는 끈다. `foundation-release-build.yml` 은 main 푸시 전용이라 커밋마다 그룹을 따로 둔다(ADR-0276 §9). 바꿀 것이 없다.

### 큰 프로젝트는

| 프로젝트 | 작은 검사 |
|---|---|
| rust-lang CI | `citool` 이 잡 목록을 만들고, tidy·라이선스 같은 짧은 검사는 따로 러너를 받지 않고 `mingw-check`/`x86_64-gnu-llvm` 같은 큰 잡 안의 단계로 돈다 |
| Kubernetes prow | `verify-*` 들은 presubmit 하나(`pull-kubernetes-verify`)의 단계다 |
| Chromium | 정적 검사(`presubmit`)는 빌더 하나에서 묶어 돈다 |

공통점: **러너(기계) 하나의 고정 비용이 검사보다 크면 검사를 묶는다.**

## Decision

1. **준비가 일보다 큰 검사는 그 영역의 Rust 잡 단계다.** 영역의 Rust 잡이 이미 하는 준비 위에 단계로 둔다. 선택은 그대로다(영역이 바뀌면 돈다, ADR-0202).
   - identity: OpenAPI 결정성·cargo-deny → `rust-quality` 의 단계.
   - dawneer·intelligence: cargo-deny → `rust` 의 단계.
   - gongzzang: cargo-deny·repo-guard 셋·트래픽/인증 정책 drift → 잡 하나 `policy`. Rust 잡(임계경로, 일 약 200 s)은 늘리지 않는다.
   - 검사 내용·명령·required 컨텍스트 이름은 바뀌지 않는다. `required/*` 의 결과 검사는 남은 잡을 그대로 모은다.
2. **릴리스 빌드 시험은 머지 큐에서 돈다.** `foundation_ci_scope.py` 의 `RELEASE_EVENTS` 는 `merge_group`·`workflow_dispatch` 다. PR 은 ADR-0199 의 경계 슬라이스처럼 `release-image` 를 건너뛴다.
   - 큐 실행이 같은 선택(릴리스 워크플로의 scope)으로 고르므로, 시험이 빌드하지 않은 커밋은 여전히 main 에 들어가지 않는다.
   - 큐에서라면 골랐을 PR 에는 `::warning title=release-image deferred to the merge queue::` 를 남긴다.
   - 시험: `test_foundation_ci_scope.py` 가 PR·push·schedule 에서 고르지 않고 merge_group 에서 고르는 것을 고정한다.
3. **유지하는 것.** required 검사 12개, 보안 게이트(cargo-deny·SBOM·secret-scan·저장소 가드), 유료·자체 러너 금지(ADR-0420)는 그대로다. 새 가드는 없다. 기존 `check-workflow-policy.sh`·`change_scope.py check` 가 고친 워크플로를 그대로 검사한다.

## Consequences

- 영역이 바뀐 이벤트 하나에서 러너를 잡는 잡이 최대 6개 준다(identity 2·dawneer 1·intelligence 1·gongzzang 2). 기준 창에 넣어 계산하면 러너 배정 62번(작업의 9.4%), 준비·정리 러너·분 약 42분이 사라진다. 일 7분은 남은 잡으로 옮겨 간다.
- PR 시도가 쓰는 릴리스 시험 러너·분이 0 이 된다(창 안 33분). 대가: 릴리스 이미지를 깨는 PR 은 큐에서야 실패하고 ALLGREEN 에 따라 큐에서 빠진다. 2026-10-29 #939 같은 사고는 여전히 main 전에 잡힌다.
- identity `rust-quality` 는 OpenAPI 내보내기와 cargo-deny 만큼(측정 약 12 s + 컴파일) 길어진다. gongzzang `policy` 는 cargo-deny·repo-guard 만큼(약 17 s) 길어진다. 둘 다 그 워크플로의 임계경로가 아니거나 임계경로의 10% 미만이다.
- 측정: 같은 스크립트와 같은 길이의 창으로 PR 본문에 전후를 적는다.
