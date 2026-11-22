# ADR 0605: 작업의 풀은 지키는 자원으로 나누고 Dagster 의 풀이 센다 — 한 run 은 한 칸, 기다림은 목록에서 계산하지 않고 잰다

- Status: Accepted
- Date: 2026-11-23
- Supersedes: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md) 의 슬롯 무게(`pool_slots`),
  슬롯 조합을 다 세는 메모리 셈, 굶김 상한(`STARVATION_CYCLES`).
- Amends: [ADR-0563](./0563-the-model-claims-the-memory-it-takes-not-the-spark-pool-and-a-complex-s-profile-is-exported-when-its-facts-or-its-row-change.md)
  (claim 은 한 칸짜리 풀에만 둔다 → 두 칸 풀에도 둔다, §3),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md) §5(입력 시작 작업의 주기는 굶김 셈에만 쓰였다;
  서로를 시작하는 원의 거부는 남는다),
  [ADR-0583](./0583-jobs-move-to-dagster-in-four-groups-one-switch-each-and-the-per-job-dual-run-gate-retires-with-the-first.md)
  §7(풀을 메모리 종류로 나눠 굶주림을 막는다 — 이 ADR 이 자원으로 나눈다; 노화·선점과 호스트 허가의 끝 조건은 그대로다).
- Builds on: [ADR-0574](./0574-every-limit-is-set-from-measurement-and-a-unit-near-its-limit-is-named-before-it-fails.md)(한도는 측정으로, 작업마다의 메모리),
  [ADR-0599](./0599-a-silver-release-is-compared-by-duckdb-from-a-local-copy-and-appended-in-one-commit-so-the-lanes-need-no-branch-and-no-spark.md)(Silver 레인이 Spark 를 떠남)

## Context

풀 두 개(`spark` 3칸, `documents` 1칸)와 작업마다의 슬롯 무게(`pool_slots` 1~3)는 Airflow 의 풀을 본뜬 것이다(ADR-0138).
`job_specs.pool_starvation` 은 그 무게로 작업마다 최악의 기다림을 계산해, 자기 주기 20번(`STARVATION_CYCLES`)을 넘으면 작업
목록을 거부했다. 2026-11-22 의 실물:

- **풀의 이름이 자원과 맞지 않는다.** `spark` 48개 가운데 Spark 를 쓰지 않는 발행기 안의 DuckDB 작업이 21개다(Gold 판 빌드
  `*_versions_build` 8G, by-PNU 굽기 14개). `documents` 는 문서 작업(GPU) 둘에 Silver 레인 33개·FLOOR·Gold 판 빌드 셋이 끼어
  40개다. 이름은 "documents" 인데 한 칸을 Silver 레인이 나눠 쓴다.
- **계산된 상한이 작업을 시계에 묶는다.** 입력으로 시작하는 작업은 먹이는 작업의 주기를 물려받는다. 시간마다 오는 입력이면
  상한은 1,200분이다. W86 의 `gold_parcel_boundary_served_versions_build`(#1609)는 1,200분에 걸려 매일 11:30 schedule 로,
  W81e 의 실거래 Gold 판 빌드(#1629)는 documents 1,310분·spark 3,355~4,240분으로 두 시간마다 schedule 로 옮겨졌다. 기다림은
  계산이 아니라 줄에서 일어나는 일인데, 계산이 줄보다 먼저 막았다.
- **Dagster 는 이미 같은 줄을 센다.** `dagster.yaml` 의 `tag_concurrency_limits` 가 `foundation/pool` 태그로 spark 3·documents 1
  run 을 세고, 호스트의 `job_units.py pool-full` 이 무게로 한 번 더 센다. 태그 한도는 run 수를 세므로 3칸짜리 작업도 1이다.

Dagster 1.13.27 에서 확인한 것(배포판 휠의 소스와 공식 문서):

- 풀은 op 의 `pool=` 이고, `concurrency.pools.granularity: run` 이면 그 op 를 가진 **run 하나가 한 칸**을 쥔다
  (`claim_concurrency_slot(key, run_id, step_key, priority)`, 무게 없음).
- 풀마다의 한도는 `dagster.yaml` 이 아니라 인스턴스 DB 에 있다(`set_concurrency_slots`, CLI `dagster instance concurrency set`,
  GraphQL `setConcurrencyLimit`·`deleteConcurrencyLimit`). yaml 은 `default_limit`·`granularity`·`op_granularity_run_buffer` 만 받는다.
- 취소되거나 실패한 run 의 칸은 스스로 풀리지 않는다: `run_monitoring.free_slots_after_run_end_seconds` 가 없으면 한 칸 풀이 영영 막힌다.
- 줄은 `dagster/priority` 높은 것부터, 같으면 들어온 순서다. 노화는 없다. 센서는 `QUEUED` run 을 보므로 한 작업의 줄 선 run 은 하나다.
- 메모리를 보고 꺼내는 기능은 없다.

메모리(잰 값, `every-container-has-a-memory-cap.sh` 의 출력, 2026-11-22 main): 늘 도는 서비스 32.32g + 예약 5g, 정적 자리
24.68g. 그때 예약된 작업 합은 23.25g(spark: 접기 8g + 굽기 14g; documents: 사실 작업 1.25g), 전체 60.57g/62g.
Silver 레인 16G·Gold 판 빌드 8G·FLOOR 5G 는 이 합에 없다: `memory_claim` 이 시작할 때 호스트의 MemAvailable 로 허가한다(ADR-0563).
정적인 run 수 한도만으로는 지금의 동시성이 맞지 않는다: spark 20g + 레인 16G = 36g > 24.68g.

## Decision

1. **풀은 지키는 자원으로 나눈다.** 이름이 곧 자원이다.

   | 풀 | 칸 | 무엇 | 메모리 |
   |---|---|---|---|
   | `spark` | 1 | `spark`(20g)·`spark-small`(2,560m)·타일 굽기 컨테이너를 띄우고 자기 유닛 상한이 없는 작업 27개 | 정적: 가장 큰 구성원 |
   | `duckdb` | 2 | 발행기 안의 DuckDB: Silver 레인 33개·FLOOR·산업단지 Silver·Gold 판 빌드 10개·by-PNU 굽기 14개 | 자기 유닛의 `memory_claim` |
   | `gpu` | 1 | 문서 작업 둘(OCR·HWP, 사실 작업의 로컬 모델) — GPU 하나 | 정적 + 모델의 `memory_claim` |
   | `default_pool` | — | 작은 수집·검사 | `host_reserved` |

   `spark` 의 20g run(계보·Gold 재빌드)은 이미 3칸을 다 쥐고 혼자 돌았고, 접기 둘은 init 컨테이너 이름을 나눠 써 겹치면 안 된다:
   한 칸이 둘 다 지킨다. 구성원은 정의 파일의 `host_memory` 가 정한다: 유닛의 MemoryMax 를 쓰는 작업은 `duckdb`, 컨테이너만 쓰는 작업은 `spark`.

2. **한 run 은 한 칸이다.** `pool_slots` 는 지운다. 작업이나 claimant 가 다시 적으면 `job_specs.no_slot_count` 가 거부한다(읽히지 않고
   무시되는 숫자를 남기지 않는다). Dagster 의 run 수 세기와 호스트의 `pool_full` 이 같은 수를 센다.

3. **`duckdb` 의 run 은 자기 유닛의 메모리로 허가된다.** 모든 `duckdb` 작업은 자기 유닛에 `memory_claim` 을 두고, 정의의
   `host_memory` 에는 없다(굽기의 `spark-small` 단계 2,560m 는 `keep_available` 6g 안이다 — FLOOR 의 내보내기 컨테이너와 같은 셈).
   claim 은 두 칸 풀에도 둔다. 그래서 `job_units.py memory_short` 는 자기 몫과 함께 **도는 다른 claim 이 아직 더 가질 수 있는 몫**
   (MemoryMax − MemoryCurrent)을 뺀다: 차례로 시작한 두 claim 이 같은 메모리로 허가되지 않는다.

4. **메모리 가드는 풀마다 `slots` 개의 가장 큰 정적 구성원을 더한다.** 슬롯 조합을 모두 세던 셈은 지운다. claim 으로 허가되는
   구성원은 더하지 않는다(그대로). 잰 값: 예약된 작업 21.25g(spark 20g + gpu 1.25g), 더 큰 일회성 작업 22g 이 세어져 **59.32g/62g**.

5. **기다림은 잰다.** `pool_starvation`·`longest_wait_minutes`·`hold_minutes`·`cycle_minutes`·`STARVATION_CYCLES` 를 지운다. 자산
   조정자(`asset_reconcile.py`)가 Dagster 의 `QUEUED` run 가운데 작업마다 가장 오래된 것의 나이(`foundation_dagster_run_queued_seconds`)를
   등급 예산(`priority_classes.queue_wait_budget_minutes`, `foundation_dagster_run_queue_budget_seconds`)과 함께 쓰고,
   `FoundationDagsterRunQueuedOverBudget` 이 넘으면 울린다. 호스트 줄의 `FoundationJobQueueWaitOverBudget` 은 그대로다.
   입력 시작 작업이 서로를 시작하는 원은 `job_specs.start_circles` 가 여전히 거부한다.
   그래서 시간마다 오는 입력으로 시작하는 작업도 목록이 받는다. `gold_parcel_boundary_served_versions_build` 는 이 PR 에서 `inputs` 로 돌아간다.

6. **Dagster 가 선언한다.**
   - 작업의 op 가 자기 풀을 갖는다(`run_jobs.py`: `pool=`; `default_pool` 은 풀 없음). `foundation/pool` 태그와 그 태그 한도는 지운다.
   - `dagster.yaml`: `concurrency.pools.granularity: run`(op 단위면 run 프로세스 170 MiB 가 칸을 기다리며 떠 있다),
     `default_limit: 1`(배포가 아직 쓰지 않은 풀은 한 번에 하나), `run_monitoring.free_slots_after_run_end_seconds: 300`.
   - 배포(`airflow-runtime.sh sync_dagster`)가 코드 위치를 다시 읽기 전에 `writer_gate.py ask pools` 로 목록의 칸을 쓰고, 목록에 없는 풀을
     지우고, **다시 읽어** 목록과 다르면 말한다. 쓰기 게이트는 세 문서(`POOL_LIMITS`·`SET_POOL_LIMIT`·`DELETE_POOL_LIMIT`)만, 한도 1~16 만 받는다.
     Airflow 의 풀도 같은 목록에서 쓰고 목록에 없는 풀은 지운다.
   - `dagster/priority` 는 모든 시작에서 등급 값 × 1000 + 큐 무게다. 배포의 한 번 시작·운영자의 run-now(`job_orchestrator.py start`)는
     등급 값만 보내 같은 등급의 센서 run 뒤에 섰고 `decided-by` 태그도 없었다: 이제 작업의 `JobSpec.dagster_tags` 를 보낸다.
   - `foundation/decided-by=dagster` 한도 5 는 풀 칸 합 + 1 이다(1 + 2 + 1 + 1, 전과 같은 수; ADR-0583 §7 의 셈).

7. **무엇이 Dagster 의 것이고 무엇이 우리 것으로 언제까지 남나.**

   | 것 | 누구의 것 | 언제까지 |
   |---|---|---|
   | 풀의 칸 세기, 줄, `dagster/priority` 순서, 끝난 run 의 칸 풀기, Deployment → Concurrency 화면 | Dagster 기본 기능 | 계속 |
   | 풀 한도를 목록에서 쓰고 다시 읽는 배포 단계(`writer_gate.py ask pools`) | 우리 것, 계속 | OSS 1.13 은 풀마다의 한도를 yaml 로 받지 않는다. 받는 판이 오면 yaml 로 옮기고 지운다 |
   | QUEUED run 나이와 경보 | 우리 것(조정자 한 함수), 계속 | Dagster 가 줄의 나이를 지표로 내면 그것으로 바꾼다 |
   | 호스트 `pool_full`(Dagster 밖의 시작: 운영자 일회성·cut-over·연습 인스턴스·손 `systemctl start`) | 우리 것, 한시 | ADR-0583 의 마지막 PR(운영 묶음 스위치 뒤, 그 시작들이 Dagster run 이 된 뒤) |
   | 용량 claim(`capacity_claimants`, cut-over) | 우리 것, 한시 | 같은 PR |
   | `memory_claim`·`memory_short` | 우리 것, 한시 | 레인마다 잰 상한(ADR-0574 §8 인스턴스 드롭인; 지금 33개가 템플릿의 16G)으로 정적 `duckdb` 한도가 24.68g 안에 들어올 때. 그 PR 이 표를 싣는다 |
   | 대기 노화(`queue_aging`)·선점(`preempted`) | 우리 것, 한시 | ADR-0583 §7 대로 마지막 묶음과 함께 지운다(W82b) |

8. **되돌리기**는 이 PR 의 revert 하나다. 되돌린 릴리스의 op 에는 풀이 없으므로 DB 에 남은 풀 한도는 아무 run 도 세지 않는다.
   배포가 쓰는 Airflow 풀은 목록대로 돌아간다(`documents` 가 다시 생기고 `duckdb`·`gpu` 는 지워진다).

## Consequences

- W81e(#1629)의 실거래 Gold 판 빌드는 rebase 뒤 `inputs` 로 둘 수 있다. 굶김 계산 때문에 시계에 있는 다른 작업
  (`gold_public_auction_rebuild` 21:35, 공매 kind Gold, VWorld 판 수집의 Spark 없는 이유 등)은 주인 PR 이 옮긴다: 이 PR 은 이유 문장만 고친다.
- `spark` 1칸은 접기(옛 2칸) 옆의 `rt_molit` Silver(옛 1칸) 동시 실행을 잃는다. 둘 다 짧다.
- `duckdb` 2칸은 두 레인이 동시에 claim 을 묻게 한다. 두 번째는 첫째가 자라는 몫까지 빼고 메모리를 기다린다(§3). 지금 문서 풀에서 하나씩 기다리던 것보다 덜 기다린다.
- 굽기와 Gold 판 빌드는 Spark 작업 뒤에서 기다리지 않는다. 그 대신 Silver 레인과 같은 줄에 선다. 순서는 큐 무게가 정한다(읽히는 쪽이 먼저, ADR-0558).
- 모든 작업의 코드 버전이 바뀐다(`job_specs.py` 를 모두가 읽는다). 이전 코드에서 실패한 작업만 한 번 다시 시작된다(ADR-0499).
- `takes_turns`(서빙 굽기)는 한 칸 풀에서만 뜻이 있다. 굽기가 2칸 `duckdb` 로 가서 이제 미뤄지지 않는다: FLOOR 와 계보가 굽기 옆에서 돈다.

## 기각한 대안

- **태그 한도를 그대로 쓴다.** 이미 run 수를 세지만 화면도 op 의 풀도 없고, 무게 있는 호스트 셈과 두 수가 된다.
- **op 단위 풀과 무게.** 한 작업을 칸 수만큼의 op 로 쪼개 무게를 흉내 낸다. 재발명이고, op 단위는 run 프로세스를 칸 없이 띄운다.
- **메모리를 보고 풀 한도를 바꾸는 제어기.** Dagster 에 없는 기능을 우리가 만든다. claim 은 이미 있고, 끝 조건(§7)이 있다.
- **지금 정적 메모리 풀로 간다.** 36g > 24.68g: 레인 하나와 Spark 하나가 함께 돌 수 없게 된다. 레인 상한을 재는 것이 먼저다.
- **굶김 상한을 Dagster 쪽으로 옮긴다.** 계산은 Airflow 의 폴링 기다림(`wait_limit_minutes`·재시도)을 본뜬 것이다. Dagster 의 줄은 run 을
  잃지 않고 기다리게 하므로, 넘는지는 재서 알린다.
