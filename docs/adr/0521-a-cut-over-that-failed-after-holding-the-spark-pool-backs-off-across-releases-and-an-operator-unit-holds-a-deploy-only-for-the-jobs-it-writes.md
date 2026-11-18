# ADR 0521: 풀을 오래 쥐고 실패한 cut-over 는 배포가 바뀌어도 물러서고, 운영자 유닛은 자기가 쓰는 작업이 보류될 때만 배포를 멈춘다

- Status: Accepted
- Date: 2026-11-11
- Amends: [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(자동 승격의 백오프),
  [ADR-0441](./0441-a-deploy-never-waits-for-a-running-job-or-an-operators-run-each-keeps-the-release-it-started-with.md)(배포가 운영자 유닛을 기다리는 조건)
- Builds on: [ADR-0462](./0462-a-start-its-pool-refused-waits-in-a-queue-that-ages-it-and-a-claim-holds-the-pool-only-for-a-spark-step.md)(claim 은 Spark 걸음 동안만),
  [ADR-0499](./0499-a-failed-job-runs-again-once-when-a-release-changes-the-code-it-reaches.md)(코드 버전)

## Context

2026-11-11 의 일(잰 값, ai-server 읽기 전용):

| 시도(자동 승격) | 레인 | gold 걸음 | 결과 |
|---|---|---|---|
| 11-10 21:42Z · 23:12Z · 11-11 00:57Z | parcel-real-transaction | 1,913 · 1,845 · 4,540 s | 실패 |
| 03:42Z · 08:12Z · 10:12Z · 11:27Z | parcel-real-transaction | 8,839 · 5,807 · 2,815 · 2,568 s | 실패(`Java heap space`, 드라이버 하트비트 끊김; `real_transaction_silver_to_gold.py:620`) |
| 사이사이 | parcel-nearby | `inputs` 3–4 s `waiting` | claim 없음 |

이 레인의 시도는 26번이다. 첫 전체 Gold 가 매번 메모리로 죽는데, 백오프(1·2·4…24시간)는 **지금 릴리스의 시도만** 셌다(ADR-0435,
11-08 의 "한 릴리스가 gold 걸음을 1초 만에 다 깨뜨렸다" 사고 때문). 호스트는 몇 시간마다 배포하므로 한 번 실패하면 늦어도 한 시간 뒤
같은 Gold 가 다시 spark 풀 전체를 잡았다. claim 은 규칙대로 gold 걸음 동안만 걸렸지만(ADR-0462) 그 걸음이 47–147분이었고, 그동안
기본 등급 시작 18개가 큐에서 최대 14,434 s(admin_boundary_release, 08:15Z→12:15Z) 기다렸다. "몇 초 만에 끝나는 실행"은
parcel-nearby 의 입력 대기였고 claim 을 걸지 않았다.

같은 날 W18h 가 본 두 번째 문제: 배포가 보류하는 작업이 하나라도 있으면 돌고 있는 운영자(transient) 유닛 **모두**를 기다린다(ADR-0441).
이 cut-over 는 `gold.parcel_real_transaction` 과 그 굽기만 쓰는데도, 아무 표나 바꾸는 릴리스를 몇 시간씩 붙잡았다. 이 ADR 의 고침도
그 대기에 걸린다.

## 비교

| | A. 비싼 실패는 모든 릴리스에 걸쳐 센다 | B. 레인 코드 버전이 바뀌면 다시(ADR-0499 식) | C. 지금대로(릴리스마다 초기화) |
|---|---|---|---|
| 11-11 실거래 | 26번째 시도 뒤 24시간 | `orchestration/jobs.v1.json`(생성물, 11-09 이후 64 커밋 중 23 개가 정의를 바꿈)이 버전에 들어가 몇 시간마다 다시 | 한 시간마다 다시 |
| 11-08 사고(1 s 실패) | 빠른 실패라 릴리스마다 초기화(지금과 같음) | 고친 코드가 버전을 바꾸면 다시 | 다시 |
| 고친 뒤 기다림 | 최대 24시간(고침이 그 레인 Gold 에 닿아도) | 바로 | 바로 |

Kubernetes 의 CrashLoopBackOff 는 파드가 실행하는 것이 바뀌지 않는 한 재시작을 거듭해도 물러섬을 유지하고, Argo Rollouts 는 새
리비전을 새로 분석한다. 우리에게 "새 리비전"은 그 레인이 실행하는 코드인데, 지금 그 범위를 좁게 추론할 방법(B)은 생성물 JSON 때문에
릴리스와 거의 같다. 그래서 A: **실패한 방식**으로 가른다. 몇 초 만의 실패는 릴리스 탓일 가능성이 크고(11-08), 풀을 10분 넘게 쥐고
돈 뒤의 실패는 자원·데이터 탓이라 다른 릴리스도 같은 결과를 낸다.

## Decision

1. **자동 승격의 백오프는 이 릴리스의 시도와, 어느 릴리스든 "비싸게" 실패한 시도를 센다.** 비싼 실패 = 그 시도의 실행에서 `gold` 또는
   `bake` 걸음이 600 s(`COSTLY_SECONDS`) 이상 돈 뒤 `failed`. 기다림은 레인의 마지막 시도(어느 릴리스든)부터 잰다. 시간 상한으로
   멈춘 gold(재개됨, 한 시간)와 `waiting`(0) 규칙은 그대로다. 순서(시도가 적은 레인 먼저)는 지금처럼 모든 시도를 센다.
   (`scripts/ops/by-pnu-auto-promote.sh`)
2. **claim 이 Spark 일 없이 풀을 쥐면 알린다.** capacity claimant 는 자기 Spark 걸음이 도는 유닛을 `work_units` 로 선언한다
   (`definitions/platform.definition.json`, cut-over 러너는 이 목록으로 Spark 를 기다린다). 자산 조정기는 살아 있는 claim 마다
   `foundation_capacity_claim_age_seconds` 와 `foundation_capacity_claim_work_units`(그 풀의 작업 + `work_units` 중 도는 수)를 쓰고,
   `FoundationCapacityClaimHeldWithoutSpark` 는 그 값이 15분 동안 0 이면 울린다.
3. **ADR-0441 개정: 운영자 유닛은 자기가 쓰는 작업을 선언하고, 배포는 그 집합이 보류 집합과 만날 때만 기다린다.**
   `by-pnu-pack-operator.sh` 는 묶음 전용 레인(하나·목록·`lanes`)의 실행이 시작하는 모든 유닛에 `FOUNDATION_RUN_WRITES=<작업 id,...>` 를
   붙인다. 값은 레인 Gold 표를 자산으로 쓰거나 그것으로 만드는 작업들로, 배포의 보류 집합을 만드는 `release_changes.writers` 를 그대로
   쓴다(실거래: `by_pnu_parcel_real_transaction_bake,gold_parcel_real_transaction_rebuild`). `foundation-deploy.sh` 는 선언이 있고 모든
   작업을 보류하지 않는 릴리스라면, 선언된 작업이 하나도 보류되지 않을 때 그 유닛을 기다리지 않는다. 선언이 없는 유닛(패널 레인, 그래프가
   못 놓는 레인)이나 모든 작업을 보류하는 릴리스는 전처럼 기다린다.

## Consequences

- 11-11 기록을 새 코드로 다시 돌리면(재생, 잰 값의 계산) parcel-real-transaction 의 다음 시도는 11-12 11:27Z 다. 옛 코드는 같은
  릴리스면 2시간 뒤(13:27Z), 새 릴리스면 곧바로 다시 잡는다.
- 실거래 첫 Gold 의 OOM 자체는 이 ADR 이 고치지 않는다. 고친 PR 이 배포돼도 이 레인은 최대 24시간 기다린다(빠른 재시도는 run-now 로
  Gold 작업 `gold_parcel_real_transaction_rebuild` 를 돌리면 된다: Gold 가 생기면 cut-over 의 gold 걸음은 "nothing to do" 다).
- 선언은 `release_changes.writers` 와 같은 그래프 해석을 쓰므로, 보류 규칙이 바뀌면 선언도 함께 바뀐다(SSOT 하나).
