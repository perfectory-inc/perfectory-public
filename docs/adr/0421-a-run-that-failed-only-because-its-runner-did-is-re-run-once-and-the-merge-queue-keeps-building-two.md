# ADR 0421: 러너 때문에만 실패한 실행은 한 번 다시 돌리고, merge queue 는 동시에 둘을 짓는 설정을 유지한다

- Status: Accepted
- Date: 2026-11-05
- Builds on: [ADR-0269](./0269-a-pull-request-verifies-only-the-crates-it-reaches.md)(일정 전용 캐시 정리 작업의 `actions: write` 예외),
  [ADR-0313](./0313-merge-queue-and-pull-requests-verify-only-what-changed-and-main-does-not-repeat-the-queue.md)(merge queue 검증 범위),
  ADR-0420(대기의 원인 측정, #1079)

## Context

ADR-0420 의 측정(2026-11-05)에 따르면 PR 의 체감 대기는 둘에서 온다.
- 호스트 러너의 동시 작업 상한: Free 조직은 20개이고, 몰리는 시간대에 PR 작업 대기 p90 이 1,231 s 다.
- merge queue: 마지막 투입부터 병합까지 p90 136분이다.

같은 날 사람이 단계 없이 실패한 작업(러너 사망·BlobNotFound·디스크 부족)을 손으로 여러 번 다시 돌렸다(#1059·#1068·#1071 등). 이런
작업은 아무 판정도 내지 않았다. 그런데 PR 은 빨강이 되고, 사람이나 에이전트가 볼 때까지 멈춘다.

merge queue 설정은 `tools/github/main-ruleset.json` 이 정본이다. 지금 값은 `grouping_strategy: HEADGREEN`,
`max_entries_to_build: 2`, `max_entries_to_merge: 5`, `check_response_timeout_minutes: 120` 이다. 그룹 크기(`max_entries_to_merge`)는
이미 1 보다 크다.

2026-11-05 merge queue 측정(merge_group 실행 1,000개 = 커밋 84개, 11-03 17:47 ~ 11-05 08:09 UTC):

| 지표 | 값 |
|---|---:|
| 커밋 하나의 벽시계(그 커밋의 workflow 실행들 중 가장 늦은 끝 − 가장 이른 시작) | p50 29.3분, p90 61.9분, p99·최대 87.2분 |
| 커밋 하나의 workflow 수 | p50 12 |
| 실패한 커밋 | 84 중 26(31%) |

## Decision

1. **러너 때문에만 실패한 실행은 한 번 다시 돌린다.** 일정 전용 workflow `rerun-runner-failures.yml` 이 10분마다 지난 2시간의 실패한
   실행을 본다(`scripts/ci/rerun_runner_failures.py`). 다음을 모두 만족할 때만 `rerun-failed-jobs` 를 부른다.
   - 실패한 작업이 하나 이상이고, 그 모두가 실패한 단계 없이 끝났다.
   - 첫 시도다(`run_attempt == 1`). 그래서 망가진 러너가 무한히 되풀이되지 않는다.
   - `merge_group` 이 아니다. 큐는 체크가 실패한 순간 그 항목을 이미 뺐으므로, 다시 돌려도 병합되지 않는 커밋에 러너만 쓴다.
   - 실패한 단계가 하나라도 있으면 진짜 실패다. 손대지 않는다.
2. **권한**: 그 workflow 의 작업 하나만 작업 수준 `actions: write` 를 가진다. 트리거는 schedule·workflow_dispatch 뿐이다.
   - ADR-0269 의 캐시 정리 작업과 같은 예외이고, 같은 규칙이다.
   - `scripts/guard/check-workflow-policy.sh` 의 허용 파일 목록에 이 파일 하나를 더했다.
   - 자기 시험은 이 파일의 승인과, push 트리거를 붙이면 거부되는지를 확인한다.
   - 다른 파일·최상위 권한·contents write 는 여전히 거부된다.
   - 실패가 막는 사고: 단계 없는 실패를 사람이 찾아 손으로 다시 돌리던 일이다(위 Context, 2026-11-05 여러 번).
3. **merge queue 는 `max_entries_to_build: 2` 를 유지한다(계산).**
   - 커밋 하나가 workflow 12개를 돌린다. 둘을 동시에 지으면 이미 수십 개의 작업이 동시 상한 20 을 두고 PR 작업과 다툰다.
   - 셋 이상으로 올리면 처리량이 늘지 않는다. 같은 20개 슬롯을 더 많은 작업이 나눠 쓰게 되어 PR 대기만 늘어난다.
   - 그룹 크기(`max_entries_to_merge: 5`)는 이미 1 보다 크다.
   - `check_response_timeout_minutes: 120` 은 측정된 최대 87분보다 크므로 그대로 둔다.
   - 소유자가 ADR-0420 의 A(동시 60)를 고르면, 같은 측정을 다시 재고 `max_entries_to_build` 를 올린다. 계산상 5까지는 상한 60 안이다.

## Consequences

- 단계 없는 실패가 사람 손 없이 최대 10분 안에 한 번 다시 돈다. 두 번째 실패는 진짜 장애로 남아 사람이 본다.
- 일정 작업 하나가 10분마다 러너를 잠깐 쓴다. 하루 144번, 각 1분 미만이고 공개 저장소 표준 러너라 비용은 0 이다(계산).
- merge queue 에서 러너 때문에 실패한 항목은 다시 큐에 넣어야 한다. 이 ADR 은 그것을 하지 않는다. keeper 가 PR 이 초록이면 다시
  넣는다.
