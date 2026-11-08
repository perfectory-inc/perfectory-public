# ADR 0435: by-PNU 레인은 자동 분석을 거쳐 스스로 발행하고, 일이 언제 시작하는지는 시계가 아니라 용량과 우선순위가 정한다

- Status: Accepted
- Date: 2026-11-07
- Supersedes: [ADR-0425](./0425-clock-started-batch-never-runs-into-the-by-pnu-operator-window-and-a-late-start-waits-for-its-next-schedule.md)(운영자 창),
  [ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md) §2b 의 "by-PNU 발행은 운영자가 시작한다" 부분(운영자 시작의
  즉시·한정 규칙 자체는 남는다), [ADR-0433](./0433-a-by-pnu-cut-over-and-a-full-bake-are-approved-in-the-console-and-the-host-runs-them-in-the-operator-window.md) §3(창에 맞춘 시작)
- Amends: ADR-0433 — 승인 단추는 정말 예외인 동작(재발행 강제, 중단)에만 남는다. 정상 경로의 첫 발행·세대 전환은 승인을 기다리지 않는다
- Builds on: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(spark 풀 슬롯),
  [ADR-0190](./0190-unit-prices-are-their-own-gold-panel-and-by-pnu-lane.md)·ADR-0193 §6(묶음 전용 레인의 첫 cut-over 단계),
  [ADR-0317](./0317-unit-price-vintages-are-stored-as-versions-and-any-vintage-is-rebuilt-exactly.md)(걸음마다 커밋하는 이행),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md) §7(수준 기반 조정자), ADR-0431(Dagster 이행), ADR-0432(차단 asset check)

## Context

2026-11-06 하루의 parcel-ownership 첫 발행(호스트 journal, `by-pnu-bake/cutover/*.jsonl`, 잰 값).

| 시각(UTC) | 무슨 일 | 원인 |
|---|---|---|
| 11-05 01:54 | 레인 Gold 완성(gold 단계 1,602 s, 39,396,906 행), 굽기 636 s, equality 통과 | — |
| 11-05 02:42 | latency 단계 실패: R2 `service error` 한 번 | 일시 오류, 다시 돌릴 사람 필요 |
| 11-06 00:24 | 사장 `sudo` 비밀번호 실패, 손으로 drop-in(`FORCE_FULL`) 쓰고 지움 | 사람 단계 |
| 00:37 | 사장이 cut-over 시작 | 사람 단계 |
| 01:22 | gold 단계가 unit-price 이행(backfill)을 45분 기다리다 실패 | 같은 풀을 두고 순서가 없음 |
| 03:46 | 다시 시작 → "would run into the scheduled window 04:30-21:50 UTC" 로 거부 | 시계 창 |
| 11-07 | 다섯 레인(소유·멸실·에너지·부속지번·규제구역) 미서빙 | 사람이 다시 깨어 있어야 함 |

정상 경로에 사람 손이 셋(셸·sudo·창 시각)이었고, 사고는 셋 모두에서 났다. ADR-0425 의 운영자 창과 ADR-0433 의 승인 단추는
그 손을 줄였지만 없애지 못했다: 창은 여전히 시계이고, 승인은 여전히 사람이다. 이미 있는 것은 충분하다:

- cut-over 러너(`by-pnu-lane-cutover.sh`)가 history·preview·gold·parity·bake·equality·latency·publish·monitor-sample 을 차례로 돌리고
  첫 실패에서 멈춘다 — 이것이 자동 분석이다.
- 매니페스트는 바꿀 때마다 이전 것을 manifest history 에 create-only 로 남긴다(`by_pnu_serving_manifest_publish.rs`).
- 엣지 탐침(`by-pnu-edge-probe.py`, 15분마다)이 사용자처럼 GET 두 번으로 레인을 읽고, cut-over 된 레인부터 스스로 잡는다.
- spark 풀(슬롯 3)과 `pool-full` 판정(`job_units.py`)이 누가 시작했든 도는 것을 센다.

빠진 것은 (1) 순서: 풀을 기다리는 일 사이에 우선순위가 없고 cut-over 는 풀 밖에 있었다, (2) 시작: Gold 가 준비된 레인을 아무도
자동으로 승격하지 않는다, (3) 시계: 창이 용량 대신 시각으로 막는다.

## 업계 비교

| 방식 | 승격 | 실패 시 | 자원 순서 |
|---|---|---|---|
| Argo Rollouts / Flagger | AnalysisRun 이 지표로 통과하면 **자동 promote** | 자동 abort·이전 ReplicaSet 으로 롤백 | — |
| Spinnaker + Kayenta | 자동 canary 판정 → 다음 단계 | 판정 실패 = 파이프라인 중단·알림 | — |
| Google SRE(canarying) | 사람이 아니라 판정이 승격, 사람은 알림으로만 | 자동 롤백이 기본 | — |
| Kubernetes PriorityClass | — | — | 높은 클래스가 대기하면 낮은 것을 선점 |
| Slurm QOS / priority | — | — | 높은 우선순위 대기 작업이 먼저, 같은 우선순위는 제출 순서 |
| Dagster run queue | — | — | `dagster/priority` 태그 + `tag_concurrency_limits`(풀) |

공통점: 정상 경로에 사람이 없다. 시계는 자원 순서의 도구가 아니다. 선점된 쪽이 잃는 것이 없게 단계 경계에서 멈춘다(Slurm
REQUEUE·K8s graceful termination).

## Decision

1. **시작은 용량과 우선순위가 정한다(이 PR, 단계 a).** `orchestration/jobs.v1.json` 에 `priority_classes`(정본:
   `definitions/platform.definition.json`)를 둔다: `publish`(1000, by-PNU 발행 — cut-over 러너와 굽기 작업), `default`(100, 이름 없는
   작업 전부), `backfill`(10, `yields_at_step_boundary`). 작업은 `priority_class` 를 적고(없으면 `default`), 작업 목록 밖에서 풀을
   쓰는 일은 `capacity_claimants` 에 풀·슬롯·클래스·유닛 패턴으로 선언한다(지금은 `by_pnu_lane_cutover`, spark 3슬롯, publish).
   `job_specs.py` 가 없는 클래스, 걸음마다 커밋하지 않는 작업의 `backfill`, 선언 안 된 claimant 를 거부한다.
2. **대기는 큐의 claim 이다.** claimant 는 `/var/lib/foundation-run-now/capacity/<claimant>.json`(root 만 씀)에 자기가 도는 systemd
   유닛을 적는다(`job_units.py claim`). claim 은 **그 유닛이 도는 동안만** 센다 — 죽은 claimant 는 아무것도 잡지 않는다.
   `job_units.py pool-full` 은 누가 시작했든, 살아 있는 claim 가운데 **더 높은 클래스이거나 같은 클래스로 먼저 줄 선 것**에게 자리를
   남기지 못하는 시작을 막는다(Slurm 의 우선순위 + 제출 순서). 막힌 시작은 지금처럼 `foundation-job-outcome waiting <이유>` 다.
3. **backfill 은 단계 경계에서 양보한다.** `job_units.py preempted` 는 `yields_at_step_boundary` 클래스의 도는 작업에게 "더 높은
   claim 이 기다리고 지금 도는 것으로는 자리가 없다" 를 알려 준다. unit-price 이행(`unit-price-versions-migrate.sh`)은 30초마다 묻고,
   답이 있으면 run 폴더의 `yield` 파일을 쓴다. `vintage_versions.Budget.allows` 는 그 파일이 있으면 다음 걸음을 시작하지 않는다.
   걸음은 이미 커밋되므로 잃는 것이 없고, 다음 시작이 이어 간다. 걸음 하나도 못 뗀 양보 run 은 `stalled` 가 아니라 `waiting` 이다.
4. **cut-over 러너는 run 동안 풀을 claim 하고 용량을 기다린다.** 45분 고정 대기는 없어진다. claim 아래에서는 새 시작이 없으므로,
   대기 상한은 대기를 시작할 때 돌던 단위들의 실행 한도(`time_limit_minutes + stop_grace_minutes`) 가운데 가장 긴 것이다. 그것을
   넘으면 호스트가 멈춘 것이라 실패하고 알린다(run 의 유닛 실패 → `foundation-unit-failed@`).
5. **Gold 가 준비된 레인은 스스로 발행한다(단계 b).** root 조정자(타이머)가 cut-over 되지 않은 레인마다 러너의 분석 단계를
   그대로 돌린다: 통과하면 발행하고, 실패하면 발행 전에 멈추고 알린다. 같은 증거(같은 Gold 스냅샷)로 실패한 레인은 지수 백오프로만
   다시 돈다(대기 루프가 아니라 수준 기반 조정, ADR-0333 §7). 발행 뒤 엣지 GET 두 번(by-pnu-edge-probe.py)이 사후 SLO 검사이고,
   이전 매니페스트가 있는 레인은 실패 시 그 history 로 자동 되돌린다. 첫 발행이 사후 검사에 실패하면 되돌릴 이전 상태가 없으므로
   알린다.
6. **시계 창을 지운다(단계 c).** `job_units.py scheduled_window`·`operator_window`·`operator_start`, `start-scheduled-job.sh` 의 창
   거부, 러너의 `window_verdict`, `operator_window_means` 를 지운다. ADR-0433 의 승인 단추는 정말 예외인 동작에만 남는다: 같은 세대를
   다시 굽기(`force_full`), 진행 중인 run 의 중단(`stop`). 승인된 cut-over 는 창을 기다리지 않고 바로 큐에 선다.
7. **설명 안 된 max_delta 는 자동 검증으로(단계 d).** 사람의 `FORCE_FULL` 대신, 레인 Gold 의 차단 asset check(ADR-0432, 계약에서
   생성)와 Silver 대비 행 수·스키마 개정·표본 비교를 기록하고, 통과하면 자동 재기준(full)하고 실패하면 그때만 알린다.

## 버린 대안

- **창을 넓히거나 예산을 줄이기.** 시계는 남는다. 11-06 03:46 은 예산 60분짜리 단계가 창 끝 44분 전이라 거부됐다.
- **K8s 식 강제 선점(kill).** 이행의 한 걸음은 최대 53분(11-06 00:45 조각 1/4 = 3,175.6 s)이고 죽이면 그 걸음을 버린다. 걸음 경계 양보는
  버리는 것이 없다.
- **Airflow `priority_weight` 만 쓰기.** Airflow 큐 안의 순서일 뿐이고 cut-over 는 Airflow 작업이 아니다(11-06 의 원인). 가중치는 Airflow
  큐의 순서로 남고, 클래스는 호스트의 순서다.
- **claim 을 만료 시각으로 지우기.** 시각은 다시 시계다. 유닛이 도는 동안만 세면 죽은 claimant 가 남기는 것이 없다.

## Consequences

- 사장 손(발행당): 11-06 parcel-ownership 하나에 ssh·sudo·drop-in 쓰기·지우기·cut-over 명령·재시작 = **6 동작**(잰 값, journal),
  ADR-0433 설계로도 단추 1 + 창 대기. 이 ADR 뒤: **0**(계산값). 사람은 실패 알림을 받을 때만.
- Gold 준비 → 서빙: parcel-ownership 은 11-05 01:54Z 에 Gold·굽기·equality 가 끝났는데 11-07 까지 미서빙 = **53시간 이상**(잰 값).
  이 ADR 뒤: 조정자 주기(15분) + latency(지난 측정 660 s) + publish + monitor-sample ≈ **30–40분**(계산값, 풀이 비어 있을 때. backfill 이
  돌면 그 걸음 하나만큼 더).
- 사고 재현 시험: `test_job_units.py::CapacityGoesByPriorityNotByClock`(11-06 00:37 의 이행이 양보하고 낮은 시작이 막힘, 죽은 claim 은
  아무것도 안 잡음), `test_vintage_versions_spark.py::RunBudgetTest`(yield 파일 뒤 걸음 없음, 양보 run 은 `stalled` 아님),
  `test_by_pnu_lane_cutover.py`(러너가 claim 하고 끝에 지움).
- 단계: (a) 우선순위·claim·양보 — 이 PR. (b) 자동 승격 조정자와 사후 검사. (c) 창 제거, ADR-0425·0433 상태 줄. (d) max_delta 자동 검증.
  뒤 단계 PR 은 앞 PR 이 병합될 때까지 draft.
- 비용: 돈 0. 이행 run 마다 30초에 파이썬 한 번(`systemctl list-units` 한 번).
- Dagster 로 옮겨 가면(ADR-0431 `proxied`·`dagster`) 클래스는 `dagster/priority` 태그, 풀은 Dagster pool 의 동시성 한도로 그대로 옮긴다.
  지금은 Dagster 가 작업을 돌리지 않으므로 호스트의 claim 큐가 정본이다.

---

개정(2026-11-07): 백오프의 두 갈래. ① 한 번도 시도하지 않은 레인이 실패한 레인보다 먼저 간다(#1135). ② gold 단계가 패널
Gold 재생성의 시간 상한(`orchestration/jobs.v1.json` `gold_panel_rebuild.time_limit_minutes`)까지 돌다 멈춘 시도는
백오프를 두 배로 늘리지 않고 한 시간 뒤 다시 간다. 재생성은 쌓아 둔 단계부터 잇기 때문이다(ADR-0438, ADR-0440).
parcel-regulation-zones 의 첫 전국 빌드가 상한보다 길다(2026-11-07).

개정(2026-11-09): ①의 "한 번도 시도하지 않은 레인"은 어느 배포에서도 시도하지 않은 레인이다. 새 배포가 지우는 것은 백오프뿐이고
순서는 아니다. 같은 배포의 시도 수가 같으면, 예전 배포에서 시도한 레인보다 한 번도 시도하지 않은 레인이 먼저 간다. 2026-11-08
20:27Z·23:12Z 에 새 배포가 나올 때마다 parcel-regulation-zones(160분 상한의 전국 Gold)가 계약 순서로 다시 맨 앞에 섰다. 그 뒤에서
parcel-building-permits·parcel-court-auction·parcel-register-masters 는 한 번도 시도되지 않았다.
