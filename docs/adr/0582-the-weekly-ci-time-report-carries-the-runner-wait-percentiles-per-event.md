# ADR 0582: 주간 CI 시간 보고는 이벤트별 러너 대기 분위를 함께 싣는다

- Status: Accepted
- Date: 2026-11-19
- Builds on: [ADR-0203](./0203-pull-request-ci-has-one-wall-clock-budget.md)(PR 벽시계 예산·주간 보고),
  [ADR-0420](./0420-ci-capacity-is-not-taken-from-the-production-host-and-a-self-hosted-runner-runs-only-trusted-events-on-a-dedicated-machine.md)
  Consequences("주간 `ci-time-budget` 이 이 대기 분위를 함께 보고하게 넓히는 것이 다음 변경")

## Context

PR 의 벽시계는 일·준비·러너 대기의 합이다. 2026-11-18 측정에서 작업 시간의 74%(coordinator, 150 실행)와 42%(W90, 286 실행)가 러너 대기였다. 주간 보고(ADR-0203)는 벽시계만 보여서, 벽시계가 늘 때 그것이 일 때문인지 러너 경합 때문인지 말하지 못했다. ADR-0420 의 측정(`started_at − created_at`)은 손으로 한 번 잰 것이었다.

## Decision

1. `scripts/ci/ci_time_budget.py measure` 는 벽시계 표 옆에 **러너 대기 표**를 쓴다. 주(UTC 월요일)마다, 이벤트(`pull_request`·`merge_group`·`push`)마다 한 줄이다. 칸은 실행 수·작업 수·p50·p90·p99·최대(초)다. 분위는 벽시계와 같은 nearest-rank 다.
2. 대기는 작업마다 `started_at − created_at` 이다(ADR-0420 측정 1 과 같은 정의). 러너를 받지 않은 작업(건너뜀, 시작 전 취소)은 넣지 않는다.
3. **표본.** 주·이벤트마다 완료 실행 25개를 시작 시각 순으로 고르게 고른다. 최신 페이지만 읽으면 한 저녁만 본다. 표본 수는 요청 예산에서 나온다. jobs API 는 실행마다 한 번이고 `GITHUB_TOKEN` 은 저장소 전체에 시간당 1,000 요청이다. 2026-11-09 주에는 실행 5,704개, 목록 약 60쪽이었다. 그러면 4주는 약 540 요청이다. 읽지 못한 실행은 `::warning::` 과 함께 빼고 계속한다.
4. **판정하지 않는다.** 예산은 벽시계 하나다(ADR-0203 1항). 대기는 원인을 읽는 숫자이고, `judge` 는 그대로다. JSON 아티팩트 `ci-time-trend` 에 `runner_wait` 가 더해진다.
5. 주간 잡의 `timeout-minutes` 는 15 → 45 다. 한 주 측정이 4분 50초였다(2026-11-19, 같은 스크립트).

## Consequences

- 벽시계가 예산을 넘는 주에, 같은 표에서 러너 대기의 몫이 보인다. 동시 상한(ADR-0420)이 바뀌거나 잡 수를 줄인 변경(ADR-0580)의 효과를 주마다 같은 방법으로 비교한다.
- 첫 실측(2026-11-09 주, 이 스크립트): pull_request 대기 p50 124 s·p90 480 s·p99 1,954 s, merge_group p50 3 s·p90 168 s, push p50 32 s·p90 315 s. 같은 주 PR 벽시계 p50 27.6분(예산 10분).
- 위험: 같은 시간에 다른 워크플로가 `GITHUB_TOKEN` 요청을 많이 쓰면 목록 읽기가 403 으로 실패할 수 있다. 실행은 월요일 01:23 UTC 한 번이고, 실패하면 그 주 보고만 붉다(PR 을 막지 않는다).
