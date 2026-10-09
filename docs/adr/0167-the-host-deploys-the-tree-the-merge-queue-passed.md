# ADR 0167: 서버는 머지 큐가 통과시킨 트리를 main 의 두 번째 검사를 기다리지 않고 배포한다

- Status: Accepted
- Date: 2026-10-09
- Amends: [ADR-0159](./0159-the-host-deploys-main-by-itself-once-its-checks-pass.md) §2(배포 판정)
- Builds on: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md)(머지 큐·main 전용 캐시)

## Context

ADR-0159 의 서버는 main 머리 커밋의 check run 이 **모두** 끝나야 배포한다. 그 커밋에는 검사가 두 벌 붙는다.

- 머지 큐(`merge_group`)가 그 커밋을 만들어 검사하고, 통과하면 main 을 그 커밋으로 빨리 감기 한다. 그래서 큐가 검사한
  커밋과 main 의 커밋은 sha 가 같다.
- main 에 들어온 뒤 `push` 로 같은 검사가 한 번 더 돈다(main 전용 캐시를 채우는 실행, ADR-0155).

2026-10-09 실측: #376 의 커밋 6f987755 에는 큐 검사 47개가 모두 성공한 뒤에도 main 의 `Rust fmt, unit tests, clippy`
등이 돌고 있었다. 서버는 "still running" 으로 기다렸고, 병합에서 배포까지 약 20분이 더 걸렸다. 그 20분 동안 이미 검사를
통과한 같은 트리를 기다렸다.

## Decision

1. **큐가 통과시킨 커밋은 바로 배포한다.** `release_checks.py` 는 커밋의 check run 과 함께 그 커밋의 `merge_group`
   워크플로 실행(`/actions/runs?head_sha=<sha>&event=merge_group`)을 익명으로 읽는다. 큐 실행이 모두 끝나 성공·건너뜀·중립이고,
   그 실행들의 check run 이 하나 이상 있으며 모두 통과했으면, 나머지(main 의 두 번째 실행)가 돌고 있어도 `deploy` 다.
2. **실패는 어디서든 거부다.** main 의 두 번째 실행이든 큐의 실행이든, 끝난 check run 하나라도 실패면 `refuse` 다(ADR-0159
   그대로).
3. **큐를 거치지 않은 커밋은 전과 같다.** `merge_group` 실행이 없으면 모든 check run 이 끝나야 배포한다.

## Consequences

- 병합에서 서버 배포까지 main 의 두 번째 검사 시간(실측 약 20분)이 빠진다.
- main 의 두 번째 실행이 배포 **뒤에** 실패하면, 그 커밋은 이미 배포됐다. 같은 트리가 큐에서는 통과했으므로 그 실패는
  불안정한 검사이거나 바깥(미러·네트워크) 실패다. Slack 의 CI 실패 알림으로 보이고, 다음 커밋이 그 다음 배포다.
- 시험: `orchestration/tests/test_foundation_autodeploy.py` 의 `TheMergeQueueRunDecides` (큐 통과면 배포, 큐 진행 중이면
  대기, 두 번째 실행 실패면 거부, 큐 없음이면 전부 기다림)와 API 경로 시험.
