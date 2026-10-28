# ADR 0304: 할 일이 없다고 답한 확인은 그 작업의 차례로 친다 — takes_turns 무한 미룸 해소

- Status: Accepted
- Date: 2026-10-28
- Amends: [ADR-0277](./0277-a-job-checks-for-work-before-it-waits-for-its-pool.md) §3("확인은 작업의 시작 기록에도
  남지 않는다"), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md) §2(차례 지키기)
- 관련 사건: 2026-10-19 무렵부터 10-28 까지 by-PNU 굽기(`by_pnu_serving_bake`)의 10:15 시각 실행이 매일 몇 초 만에
  `deferred: silver_refresh_building_register_..., silver_refresh_land_...` 와 `foundation-job-outcome unchanged` 로 끝났다.

## Context

ADR-0138 §2 의 차례 지키기: `takes_turns` 작업(굽기)의 시각 실행은 같은 풀에서 옆에서 돌 수 없는 작업이 굽기의 마지막
시작 뒤에 한 번씩 시작하기 전에는 미뤄진다. "시작했다"는 `start-scheduled-job.sh` 가 실행을 시작할 때 남기는
`/var/lib/foundation-scheduler/starts/<job>` 이다.

ADR-0277 은 12개 `silver_refresh_*` 레인(spark 3칸)에 칸 없는 확인(`check_service`)을 붙였다. 확인이
`foundation-job-check unchanged` 라고 답하면 DAG 가 `run` 을 skip 한다. §3 은 확인을 작업의 시작으로 적지 않는다고 정했다.
그래서 새 판이 없는 날이 이어지면 레인은 실행도 시작 기록도 없다. 굽기는 그 레인들이 "아직 차례를 못 가졌다"고 보고
매일 미뤘다. 굽기의 입력 트리거(`triggered`)는 미뤄지지 않지만, 그 경로도 Gold 재생성 작업이 출력 줄 없이 끝나
`failed no outcome reported` 가 되어 막혀 있었다(같은 PR 에서 고친다). 결과: 10-16 이후 by-PNU 서빙이 멈췄다.

차례 지키기의 뜻은 "여러 날 굽기가 **할 일이 있는** FLOOR·계보·레인을 굶기지 않는다"이다. 할 일이 없다고 답한 작업을
기다리는 것은 그 뜻에 들지 않는다.

## Decision

1. `start-scheduled-job.sh` 는 systemd 가 성공이라 한 확인의 답이 `foundation-job-check unchanged` 이면, 그 작업의
   시작 기록(`starts/<job>`)에 지금 시각을 적는다. 할 일이 없어 skip 되는 실행이 그 작업의 차례였다.
2. 그 밖의 답(`would_change` 등), 답 없음, 실패한 확인, 빌드 중이라 시작하지 않은 확인은 아무것도 적지 않는다. 그 작업은
   실행이 시작될 때 지금처럼 기록되고, 그 전까지 굽기는 그 작업에 차례를 양보한다.
3. 확인은 여전히 미뤄지지 않고, 풀을 기다리지 않으며, 결과 줄(`foundation-job-outcome`)을 내지 않는다(ADR-0277 §3 의 나머지).
4. 시험: `orchestration/tests/test_start_scheduled_job.py` `TheCheckBeforeThePool` — 확인이 `would_change` 인 동안 굽기는
   매번 미뤄지고, `unchanged` 한 번 뒤에는 시작한다. 실패한 확인의 `unchanged` 는 차례가 아니다.

## Consequences

- 할 일 없는 레인 12개는 07:00 확인 때 차례를 가진 것으로 적힌다. 그 뒤 10:15 굽기 시각 실행은 그 레인 때문에 미뤄지지 않는다.
- 할 일이 있는 레인은 확인이 `would_change` 라 기록되지 않고, 실행이 시작돼야 기록된다. 굽기가 그들을 앞지르지 않는 것은 전과 같다.
- 확인과 굽기 사이에 새 판이 들어와도 레인의 다음 확인(또는 입력 이벤트)이 그것을 잡는다. 그 하루 동안 굽기가 한 번 먼저
  돌 수 있는 것이 전부이고, 레인은 굽기 옆에서 못 도는 동안 기다린다(ADR-0138 상한 계산의 "같거나 큰 무게는 한 번 앞설
  수 있다"와 같은 크기).
- 배포 직후 첫 10:15 시각 실행(또는 첫 입력 트리거)은 실제로 굽기를 시작한다. 10-16 이후 쌓인 Gold 변경을 한 번에 굽는다.
