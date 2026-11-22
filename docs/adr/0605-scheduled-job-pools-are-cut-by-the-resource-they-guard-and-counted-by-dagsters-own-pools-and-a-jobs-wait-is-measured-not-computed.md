# ADR 0605: 작업의 풀은 지키는 자원으로 나누고 Dagster 의 풀이 센다 — 한 run 은 한 칸, 기다림은 목록에서 계산하지 않고 잰다

- Status: Accepted
- Date: 2026-11-23
- Supersedes: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md) 의 슬롯 무게(`pool_slots`),
  슬롯 조합을 다 세는 메모리 셈, 굶김 상한(`STARVATION_CYCLES`).
- Amends: [ADR-0563](./0563-the-model-claims-the-memory-it-takes-not-the-spark-pool-and-a-complex-s-profile-is-exported-when-its-facts-or-its-row-change.md)
  (claim 은 한 칸짜리 풀에만 둔다 → 두 칸 풀에도 두고, claim 끼리와 정적 풀을 서로 예약한다, §3),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md) §5(입력 시작 작업의 주기는 굶김 셈에만 쓰였다;
  서로를 시작하는 원의 거부는 남는다),
  [ADR-0583](./0583-jobs-move-to-dagster-in-four-groups-one-switch-each-and-the-per-job-dual-run-gate-retires-with-the-first.md)
  §7 과 그 2026-11-20 각주: 풀은 메모리 종류가 아니라 자원으로 나눈다(§1). "`job_units.py` 에 새 규칙을 더하지 않는다" 의 예외로
  메모리 허가에 §3 의 셈을 더한다. 메모리 허가의 끝 조건은 §3 의 부등식으로 바꾼다. `pool_full`·용량 claim 의 끝 조건(마지막 PR)과
  노화·선점(W82b)은 그대로다.
- Builds on: [ADR-0574](./0574-every-limit-is-set-from-measurement-and-a-unit-near-its-limit-is-named-before-it-fails.md)(한도는 측정으로, 작업마다의 메모리),
  [ADR-0599](./0599-a-silver-release-is-compared-by-duckdb-from-a-local-copy-and-appended-in-one-commit-so-the-lanes-need-no-branch-and-no-spark.md)(Silver 레인이 Spark 를 떠남)

용어: `default_pool` 은 Airflow 의 기본 풀 이름이고 Dagster 에서는 풀 없음이다. `host_reserved`(5g)는 메모리 계약이 OS·Docker·발행기
명령 몫으로 떼어 둔 것이다. "정적" 메모리는 가드가 상한(cap)으로 더하는 것, "claim" 은 시작할 때 MemAvailable 로 허가하는 것이다.

## Context

풀 두 개(`spark` 3칸, `documents` 1칸)와 작업마다의 슬롯 무게(`pool_slots` 1~3)는 Airflow 의 풀을 본뜬 것이다(ADR-0138).
`job_specs.pool_starvation` 은 그 무게로 작업마다 최악의 기다림을 계산해, 자기 주기 20번(`STARVATION_CYCLES`)을 넘으면 작업
목록을 거부했다. 2026-11-22 의 실물:

- **풀의 이름이 자원과 맞지 않는다.** `spark` 48개 가운데 21개는 Spark 를 쓰지 않는 발행기 안의 DuckDB 작업이다(Gold 판 빌드
  `*_versions_build` 7개, 유닛 상한 8G; by-PNU 굽기 14개). `documents` 는 39개다. 문서 작업(GPU) 둘에 Silver 레인 33개, FLOOR,
  산업단지 Silver, Gold 판 빌드 셋이 끼어 한 칸을 나눠 쓴다.
- **계산된 상한이 작업을 시계에 묶는다.** 입력으로 시작하는 작업은 먹이는 작업의 주기를 물려받는다. 시간마다 오는 입력이면
  상한은 1,200분이다. 그래서 W86 의 `gold_parcel_boundary_served_versions_build`(#1609)는 매일 11:30 schedule 로 옮겨졌고,
  W81e 의 실거래 Gold 판 빌드(#1629)는 두 시간마다 schedule 로 옮겨졌다(documents 1,310분, spark 3,355~4,240분). 기다림은
  줄에서 일어나는 일인데, 계산이 줄보다 먼저 막았다.
- **같은 줄을 두 곳이 센다.** `dagster.yaml` 의 `tag_concurrency_limits` 가 `foundation/pool` 태그로 spark 3·documents 1 run 을
  세고, 호스트의 `job_units.py pool-full` 이 무게로 한 번 더 센다. 태그 한도는 run 수를 세므로 3칸짜리 작업도 1이다.

Dagster 1.13.27 에서 확인한 것(배포판 휠의 소스와 공식 문서):

- 풀은 op 의 `pool=` 이다. `concurrency.pools.granularity: run` 이면 그 op 를 가진 **run 하나가 한 칸**을 쥔다
  (`claim_concurrency_slot(key, run_id, step_key, priority)`, 무게 없음).
- 풀마다의 한도는 `dagster.yaml` 이 아니라 인스턴스 DB 에 있다. `set_concurrency_slots`, CLI `dagster instance concurrency set`,
  GraphQL `setConcurrencyLimit`·`deleteConcurrencyLimit` 로 바꾼다. yaml 은 `default_limit`·`granularity`·`op_granularity_run_buffer` 만 받는다.
- 취소되거나 실패한 run 의 칸은 스스로 풀리지 않는다. `run_monitoring.free_slots_after_run_end_seconds` 가 없으면 한 칸 풀이 영영 막힌다.
- 줄은 `dagster/priority` 가 높은 것부터, 같으면 들어온 순서다. 노화는 없다. 센서는 `QUEUED` run 을 보므로 한 작업이 줄에 세우는 run 은 하나다.
- 메모리를 보고 run 을 꺼내는 기능은 없다.

메모리(잰 값, `every-container-has-a-memory-cap.sh` 의 출력, 2026-11-22 main):

- 늘 도는 서비스 32.32g 과 예약 5g 을 빼면 정적 자리는 24.68g 이다.
- 예약된 작업 합은 23.25g 이었다(spark: 접기 8g + 굽기 14g; documents: 사실 작업 1.25g). 전체 60.57g/62g.
- Silver 레인 16G, Gold 판 빌드 8G, FLOOR 5G 는 이 합에 없다. `memory_claim` 이 시작할 때 호스트의 MemAvailable 로 허가한다(ADR-0563).
  claim 이 걸린 유닛은 batch 등급이라, 메모리가 모자라면 커널이 이 유닛을 먼저 죽인다.
- 정적인 run 수 한도만으로는 지금의 동시성이 맞지 않는다: spark 20g + 레인 16G = 36g > 24.68g.

## Decision

1. **풀은 지키는 자원으로 나눈다.** 이름이 곧 자원이다.

   | 풀 | 칸 | 무엇 | 메모리 |
   |---|---|---|---|
   | `spark` | 1 | `spark`(20g)·`spark-small`(2,560m)·타일 굽기 컨테이너를 띄우고 자기 유닛 상한이 없는 작업 27개 | 정적: 가장 큰 구성원 20g |
   | `duckdb` | 2 | 발행기 안의 DuckDB 61개: Silver 레인 33개, FLOOR, 산업단지 Silver, Gold 판 빌드(`foundation-gold-versions@*`) 11개와 `gold_parcel_nearby_rebuild`, by-PNU 굽기 14개 | 자기 유닛의 `memory_claim`(§3) |
   | `gpu` | 1 | 문서 작업 둘(OCR·HWP, 사실 작업의 로컬 모델). GPU 는 하나다 | 정적 1.25g + 모델의 `memory_claim` |
   | `default_pool` | — | 작은 수집·검사. Dagster 에서는 풀 없음 | `host_reserved` |

   `spark` 의 20g run(계보·Gold 재빌드)은 이미 3칸을 다 쥐고 혼자 돌았다. 접기 둘은 init 컨테이너 이름을 나눠 써 겹치면 안 된다.
   한 칸이면 둘 다 지켜진다. 구성원은 무엇을 띄우는지로 정했다(이 PR 이전 정의의 `host_memory`): 자기 유닛의 MemoryMax 로 도는
   발행기 작업은 `duckdb`(그래서 이제 `host_memory` 가 없고 claim 이 있다), 컨테이너를 띄우고 유닛 상한이 없는 작업은 `spark`,
   문서 작업 둘은 `gpu` 다.

2. **한 run 은 한 칸이다.** `pool_slots` 는 지운다. 작업이나 claimant 가 다시 적으면 `job_specs.no_slot_count` 가 거부한다(읽히지 않고
   무시되는 숫자를 남기지 않는다). Dagster 는 자기가 꺼내는 run 을 센다. 호스트의 `pool_full` 은 같은 칸으로, Dagster 밖의
   시작까지 함께 센다.

3. **`duckdb` 의 run 은 자기 유닛의 메모리로 허가된다. 그 허가는 우리 것이고 한시다.**
   - 모든 `duckdb` 작업은 자기 유닛에 `memory_claim` 을 둔다. 그래서 정의의 `host_memory` 에 없고, 가드의 정적 합에 들지 않는다.
   - 굽기 14개는 `memory_claim.beside: 2560m`(자기 `spark-small` 단계)을 적는다.
   - `job_units.py memory_short` 는 MemAvailable 에서 다음을 뺀 나머지가 `keep_available`(6g) 이상일 때만 허가한다:
     1. 자기 유닛이 아직 더 가질 몫(MemoryMax − MemoryCurrent)과 자기 `beside`.
     2. 도는 다른 claim 유닛이 아직 더 가질 몫. 차례로 시작한 두 claim 이 같은 메모리로 허가되지 않는다.
     3. 도는 claimant 작업 유닛(`capacity_claimants.work_units`: cut-over 의 일시 pack 굽기와
        `gold-panel-rebuild-unconditional`)이 아직 더 가질 몫. 그 유닛들은 작업도 claim 도 아니다.
     4. 도는 claim 마다 그 `beside`.
     5. **다른 claim 이 돌 때만**, 자기 풀이 아니고 run 이 하나도 돌지 않는 정적 풀마다의 몫(`memory_claims.static_reserve`:
        spark 20,480m, gpu 1,280m). 첫 claim 은 ADR-0563 처럼 허가된다(1~4 만 뺀다; batch 등급). 둘째 claim 은 아직 시작하지 않은
        Spark 가 들어올 자리를 남긴다. 그래서 두 claim 이 함께 돌 때 OOM 킬러에 기대지 않는다. 이미 도는 정적 run 은 제 메모리가
        MemAvailable 에 들어 있으므로 다시 빼지 않는다(그 run 이 더 자랄 몫은 빼지 않는다).
   - 가드는 풀마다 `static_reserve` 가 그 풀의 정적 구성원이 함께 쥘 수 있는 가장 큰 합(§4) 이상인지 본다.
   - **끝 조건.** `duckdb` 를 claim 없이 정적으로 세어도 정적 자리에 들어오면 `duckdb` 작업의 claim 과 이 §3 의 셈(1~5)을 지운다.
     식으로는 `20 + 1.25 + 2 × (duckdb 의 가장 큰 상한 + 그 beside) ≤ 24.68g` 이고, 가장 큰 것이 1.7g 이하여야 한다. 굽기의
     `beside` 2,560m 하나만으로 넘으므로, 레인마다 잰 상한(ADR-0574 §8 인스턴스 드롭인; 지금 33개가 템플릿의 16G)만으로는 닿지
     않는다. 20g Spark 작업이 떠나거나(DuckDB 전환) 정적 자리가 커져야 한다. 그 PR 이 표를 싣는다. `gpu` 의 모델 claim(ADR-0563)은 이 끝 조건과 상관없이 남는다.

4. **메모리 가드는 풀마다 `slots` 개의 가장 큰 정적 구성원을 더한다.** 슬롯 조합을 모두 세던 셈은 지운다. claim 으로 허가되는
   구성원은 더하지 않는다(그대로). 잰 값: 예약된 작업 21.25g(spark 20g + gpu 1.25g). 전체는 32.32 + 5 + max(21.25, 일회성 22) = **59.32g/62g** 이다.

5. **기다림은 잰다.** `pool_starvation`·`longest_wait_minutes`·`hold_minutes`·`cycle_minutes`·`STARVATION_CYCLES` 를 지운다.
   - 자산 조정자(`asset_reconcile.py`)가 Dagster 의 `QUEUED` run 가운데 작업마다 가장 오래된 것의 나이와 등급 예산을 쓴다
     (`foundation_dagster_run_queued_seconds`·`foundation_dagster_run_queue_budget_seconds`).
   - 넘으면 `FoundationDagsterRunQueuedOverBudget` 이 울린다. 레이블은 `job_id` 다. node-exporter 수집이 자기 `job` 레이블을 지키므로
     그래서 파일에 `job` 이라 쓰면 Prometheus 에는 `exported_job` 으로 들어오고, 모든 시계열의 `job` 은 `node-exporter` 가 된다.
   - 호스트 줄의 `FoundationJobQueueWaitOverBudget` 은 그대로다.
   - 입력 시작 작업이 서로를 시작하는 원은 `job_specs.start_circles` 가 여전히 거부한다.
   - 그래서 시간마다 오는 입력으로 시작하는 작업도 목록이 받는다. `gold_parcel_boundary_served_versions_build` 는 이 PR 에서 `inputs` 로 돌아간다.

6. **Dagster 가 선언한다.**
   - 작업의 op 가 자기 풀을 갖는다(`run_jobs.py`: `pool=`; `default_pool` 은 풀 없음). `foundation/pool` 태그와 그 태그 한도는 지운다.
   - `dagster.yaml`:
     - `concurrency.pools.granularity: run` — op 단위면 run 프로세스(170 MiB)가 칸을 기다리며 떠 있다.
     - `default_limit: 1` — 배포가 아직 쓰지 않은 풀은 한 번에 하나만 돈다.
     - `run_monitoring.free_slots_after_run_end_seconds: 300`.
   - 배포(`airflow-runtime.sh sync_dagster`)는 쓰기 웹서버가 답하자마자 `writer_gate.py ask pools` 로 목록의 칸을 쓰고 다시 읽는다.
     그 앞의 `compose up -d` 가 이미 새 op 를 가진 코드 서버를 만들 수 있다. 그 사이에도 `default_limit: 1` 이라 더 많이 돌지는 않는다.
   - 목록에 없는 풀은 reload 뒤(`ask pools --prune`)에 지운다. 그때는 그 풀을 부르는 op 가 없다.
   - 쓰기 게이트는 세 문서(`POOL_LIMITS`·`SET_POOL_LIMIT`·`DELETE_POOL_LIMIT`)와 한도 1~16 만 받는다. 목록의 칸도 1~16 이다(`MAX_POOL_SLOTS`).
   - 조정자가 풀마다 Dagster 의 한도가 목록과 같은지 쓴다(`foundation_dagster_pool_limit_matches`). 다르면
     `FoundationDagsterPoolLimitDrift` 가 울린다. 배포의 경고만이 신호가 아니다.
   - Airflow 의 풀은 목록대로 쓴다. 옛 `documents` 는 지우지 않는다: 그 풀을 단 태스크 인스턴스가 남아 있을 수 있고, 그 풀을 부르는 새 태스크는 없다.
   - `dagster/priority` 는 모든 시작에서 등급 값 × 1000 + 큐 무게다. 배포의 한 번 시작과 운영자의 run-now(`job_orchestrator.py start`)는
     지금까지 등급 값만 보냈다. 그래서 같은 등급의 센서 run 뒤에 섰고 `decided-by` 태그도 없었다. 이제 작업의 `JobSpec.dagster_tags` 를 보낸다.
   - `foundation/decided-by=dagster` 한도 5 는 풀 칸 합에 기본 풀 적재 한 자리를 더한 값이다(1 + 2 + 1, + 1). 전과 같은 수이고 ADR-0583 §7 의 셈이다.

7. **무엇이 Dagster 의 것이고 무엇이 우리 것으로 언제까지 남나.**

   | 것 | 누구의 것 | 언제까지 |
   |---|---|---|
   | 풀의 칸 세기, 줄, `dagster/priority` 순서, 끝난 run 의 칸 풀기, Deployment → Concurrency 화면 | Dagster 기본 기능 | 계속 |
   | 풀 한도를 목록에서 쓰고 다시 읽는 배포 단계(`writer_gate.py ask pools [--prune]`)와 그 어긋남 경보 | 우리 것, 계속 | OSS 1.13 은 풀마다의 한도를 yaml 로 받지 않는다. 받는 판이 오면 yaml 로 옮기고 지운다 |
   | QUEUED run 나이와 경보 | 우리 것(조정자 한 함수), 계속 | Dagster 가 줄 나이를 지표로 내면 그것으로 바꾼다 |
   | 호스트 `pool_full`(Dagster 밖의 시작: 운영자 일회성·cut-over·연습 인스턴스·손 `systemctl start`) | 우리 것, 한시 | ADR-0583 의 마지막 PR(운영 묶음 스위치 뒤, 그 시작들이 Dagster run 이 된 뒤) |
   | 용량 claim(`capacity_claimants`, cut-over) | 우리 것, 한시 | 같은 PR |
   | `memory_claim`·`memory_short` 와 §3 의 셈(`beside`·`static_reserve`·작업 유닛 예약) | 우리 것, 한시 | §3 의 부등식이 맞을 때. 그 PR 이 표를 싣는다 |
   | `takes_turns` 와 그 시작 기록(`start-scheduled-job.sh`) | 우리 것, 한시. 이제 쓰는 작업이 없다(서빙 굽기는 두 칸 `duckdb`) | 호스트 시작 경로와 함께 ADR-0583 의 마지막 PR |
   | 대기 노화(`queue_aging`)·선점(`preempted`) | 우리 것, 한시 | ADR-0583 §7 대로 마지막 묶음과 함께 지운다(W82b) |

8. **되돌리기.** 이 PR 의 revert 하나에, revert 를 배포하기 전 한 줄을 더한다.
   - revert 를 배포하기 전에, 지금 릴리스에서 `echo '{}' | scripts/deploy/airflow-runtime.sh exec -T dagster-webserver python -I -B
     /opt/dagster/gate/writer_gate.py ask pools --prune` 로 Dagster 의 풀 한도를 모두 지운다(0 으로 끝나면 남은 한도가 없다).
     이 릴리스에서 만들어진 `QUEUED` run 은 op 의 풀을 지니고 있다. 되돌린 yaml 에는 `free_slots_after_run_end_seconds` 도 없다.
     그래서 한도를 남기면 칸이 풀리지 않은 run 이 줄을 막을 수 있다.
   - Airflow 에는 `duckdb`·`gpu` 풀이 남는다. 쓰는 태스크가 없어 해가 없다. `documents` 는 옛 `ensure_pools` 가 다시 쓴다.

## Consequences

- W81e(#1629)의 실거래 Gold 판 빌드는 rebase 뒤 `inputs` 로 둘 수 있다. 굶김 계산 때문에 시계에 있는 다른 작업
  (`gold_public_auction_rebuild` 21:35, 공매 kind Gold, VWorld 판 수집의 Spark 없는 이유 등)은 주인 PR 이 옮긴다. 이 PR 은 이유 문장만 고친다.
- `spark` 1칸이면 접기(옛 2칸)와 `rt_molit` Silver(옛 1칸)가 함께 돌던 것을 잃는다. 둘 다 짧다.
- `duckdb` 의 둘째 칸은 메모리가 허락할 때만 쓰인다. Spark 가 쉬고 있으면 둘째 16G 레인은 16 + (첫 레인이 더 가질 몫) + 21.28 + 6 이 남아 있어야 한다: 첫 레인이 다 자란 뒤 약 43g, 막 시작했으면 약 59g.
  레인 claim 하나일 때는 지금과 같다.
- **서빙 굽기는 지금과 다르다.** 전에는 정적 합에 14g 로 들어 20g Spark run 과 겹칠 수 없었다. 이제는 첫 claim 이라 Spark 몫을
  남기지 않고, 14 + 2.56 + 20 = 36.56g 이 정적 자리를 넘을 수 있다. 메모리가 모자라면 커널이 batch 등급인 굽기를 먼저 죽인다.
  굽기는 끊겨도 되는 작업이고(`interruptible`, 한 걸음씩 커밋), 다음 입력이나 10:15 fallback 틱에 이어 간다. 배포 뒤 첫 주에
  `unit-limits.prom` 의 oom-kill run 과 서빙 굽기의 재시작 수를 본다. 잦으면 굽기 claim 에도 정적 몫을 늘 남긴다.
- `duckdb` 안의 순서는 등급이 먼저다(값 × 1000). by-PNU 굽기는 `publish` 등급이라 레인(`default`)보다 먼저 꺼내진다. 같은 등급
  안에서는 큐 무게(읽히는 쪽이 먼저, ADR-0558)를 따른다.
- **가장 낮은 등급(실례: `unit_price_versions_migration`).** backfill 등급, 옛 3칸 작업이다. 11-21 16:30Z 무렵부터 27.7시간 동안
  호스트가 38번 넘게 거절했다(`the spark pool has no room (N of 3 slots)`; 잡고 있던 것: 타일 굽기 40, 패널 Gold 27, 실거래 굽기 16,
  계보 14, kind Gold 9). 3칸이 한꺼번에 비어야 했고, 거절된 시작은 호스트 줄의 backoff 로만 다시 왔다.
  - 이제 한 run 은 한 칸이고 Dagster 줄에 `QUEUED` 로 선다. 거절·재시도 고리도, 사람의 시작도 없다.
  - 꺼내는 순서는 등급이 먼저다(값 10). spark 칸이 빌 때 더 높은 등급의 spark run 이 줄에 없으면 이 run 이 꺼내진다.
  - 한 작업이 줄에 세우는 run 은 하나다(센서). 그래서 어느 때든 그 앞에는 spark 의 다른 작업 26개의 run 이 하나씩만 설 수 있다.
  - 그래도 높은 등급이 끊이지 않아 1,440분(backfill 예산)을 넘기면 `FoundationDagsterRunQueuedOverBudget` 이 울린다.
    노화로 앞당기지 않는다(ADR-0583 §7).
  - 시작되면 160분 상한 안에서 한 걸음씩 커밋한다. 끊기면 다음 시작이 다음 걸음부터 이어 간다.
  - 배포 때 호스트 줄에 이미 서 있는 시작은 조정자가 다시 묻는다. 새 셈으로는 spark 칸이 처음 빌 때 들어간다.
- **타일 굽기(실례: `parcel_tile_bake` 13.4시간 10번 거절, `parcel_served_gold` 37시간 15번 거절).** 둘 다 옛 3칸 작업이었고,
  spark 를 잡고 있던 것은 `lakehouse_maintenance` 와 2칸씩 적힌 Gold 판 빌드(`foundation-gold-versions@*`)였다.
  - 판 빌드는 Spark 를 띄우지 않는다. 열두 개(W96 #1616 의 공매 레인 포함) 모두 `duckdb` 로 옮긴다.
  - 이제 `spark` 에는 Spark 나 타일 굽기 컨테이너를 띄우는 27개만 남는다.
  - `lakehouse_maintenance` 는 `proxied` 단계다. Airflow 가 정하지만 run 은 Dagster op 라 Dagster 의 `spark` 칸으로 센다.
  - 두 굽기는 `default` 등급의 한 칸 run 이다. 앞에 설 수 있는 것은 지금 도는 spark run 하나(상한 165분 이하)와,
    더 높은 우선순위의 spark 작업 run 이 작업마다 하나씩이다. `publish` 등급의 패널·kind Gold 둘과, `default` 안에서 무게가
    더 큰 `rt_molit` Silver·접기 둘·필지 등록부·경매 Silver 다. 거절·재시도 고리는 없다. 그래도 360분(`default` 예산)을
    넘기면 `FoundationDagsterRunQueuedOverBudget` 이 울린다.
- 메모리가 모자라 호스트에서 거절된 큰 claim(16G 레인, 24G 사실 작업)은 호스트 줄의 backoff 로 기다린다. 그 사이 작은 claim
  (5~8G)이 들어갈 수 있다. 이 굶주림은 계산하지 않고 잰다(`FoundationJobQueueWaitOverBudget`). 기다리는 시간이 얼마일지는 배포 뒤 첫 주의 지표로 본다.
- 모든 작업의 코드 버전이 바뀐다(`job_specs.py` 를 모두가 읽는다). 이전 코드에서 실패한 작업만 한 번 다시 시작된다(ADR-0499).
- 서빙 굽기는 이제 차례를 기다리지 않는다(`takes_turns` 없음). FLOOR 와 계보는 굽기 옆에서 돈다. 조정자도 다른 작업처럼 굽기를 다시 시작한다.

## 기각한 대안

- **태그 한도를 그대로 쓴다.** 이미 run 수를 세지만 화면도 op 의 풀도 없다. 무게로 세는 호스트 셈과 숫자도 둘이 된다.
- **op 단위 풀과 무게.** 한 작업을 칸 수만큼의 op 로 쪼개 무게를 흉내 낸다. 재발명이고, op 단위면 run 프로세스가 칸 없이 떠 있다.
- **메모리를 보고 풀 한도를 바꾸는 제어기.** Dagster 에 없는 기능을 우리가 만드는 것이다. claim 이 이미 있고, 끝 조건(§3)도 있다.
- **지금 정적 메모리 풀로 간다.** 36g > 24.68g 이라 레인 하나와 Spark 하나가 함께 돌 수 없게 된다.
- **`duckdb` 1칸.** 위험은 지금과 같지만, 레인 33개가 20시간 굽기와 한 칸을 나눠 쓴다.
- **한도를 CLI 로 쓴다**(`dagster instance concurrency set`, 데몬 컨테이너 안). 삭제가 없고, 다시 읽어 견주려면 출력을 파싱해야 한다.
  게이트 문서 셋은 실제 Dagster 의 GraphQL 로 CI 에서 시험된다(`test_dagster_job_orchestrator.py`).
- **굶김 상한을 Dagster 쪽으로 옮긴다.** 그 계산은 Airflow 의 폴링 기다림(`wait_limit_minutes`·재시도)을 본뜬 것이다. Dagster 의 줄은
  run 을 잃지 않고 기다리게 하므로, 넘었는지는 재서 알린다.
