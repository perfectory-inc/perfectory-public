# ADR 0558: 작업은 같은 풀에서 자기 출력을 읽는 모든 작업보다 앞선다 — 순서는 파이프라인 그래프에서 계산한다

- Status: Accepted
- Date: 2026-11-12
- Amends: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md) §2 의 "무게: FLOOR·계보 10, 접기 5, 굽기 1" 한 줄(손으로 적는 절대 무게 → 단계 + 계산). 나머지는 그대로다.
- Builds on: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(한 풀과 `priority_weight`),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(작업 사이 의존은 그래프의 간선에서 읽는다),
  [ADR-0462](./0462-a-start-its-pool-refused-waits-in-a-queue-that-ages-it-and-a-claim-holds-the-pool-only-for-a-spark-step.md)(호스트 대기열의 나이)

## Context

spark 풀(3칸)에서 기다리는 작업의 순서는 Airflow 가 정한다. Airflow 3.3.2 의 스케줄러는 풀을 기다리는 과제를
`-priority_weight, logical_date` 로 줄 세우고(`scheduler_job_runner.py`), 무게는 과제 인스턴스가 만들어질 때 한 번
계산된다(`TaskInstance.insert_mapping`). 기다린 시간은 순서에 들어가지 않는다. 무게는 지금까지 작업마다 손으로 적었다.

### 측정 (2026-11-12 04:25Z, ai-server Airflow DB, 읽기 전용)

| 풀을 기다리는 작업 | 무게 | 기다리기 시작 | 비고 |
|---|---:|---|---|
| `gold_parcel_nearby_rebuild` 외 Gold 재구축 7건, `zoning_tile_bake` | 2 | 11-11 12:17Z ~ 11-12 03:23Z | 먼저 돈다 |
| `data_go_kr_file_data`, `registered_factory`, `parcel_registry` | 1 | 11-11 12:15Z | 16시간째 대기 |
| `parcel_served_gold`, `parcel_tile_bake`, `court_auction_silver` 외 2건 | 1 | 11-11 15:55Z ~ 22:41Z | |

- `gold_parcel_nearby_rebuild` 는 `data_go_kr_file_data` 만 적재하는 Silver 표 일곱을 읽는다(그래프 간선). 소비자가
  무게 2, 생산자가 무게 1 이라 소비자가 먼저 돌았고, 아직 없는 표를 읽다 실패했다(11-11 17:12Z).
- 같은 뒤집힘이 spark 풀에 열한 쌍 더 있다. 예: `registered_factory`→`gold_parcel_factories_rebuild`,
  `parcel_registry`→Gold 재구축 넷. 생산자가 소비자보다 앞선다는 원칙은 ADR-0138·0171 의 단계(Silver 레인 4 > 패널
  Gold 3 > Gold 2 > 굽기 1)에 이미 있었다. 새 작업이 무게를 안 적으면 1 이 되고, 그것을 막는 검사가 없었다.
- 호스트 대기열의 나이(ADR-0462)는 Airflow 가 과제를 시작시킨 뒤에만 보인다. `foundation_job_queue_wait_seconds` 는
  세 작업 모두 60,000초를 넘겼고(예산 21,600초) `FoundationJobQueueWaitOverBudget` 가 그것을 알리지만, Airflow 줄의
  순서는 바꾸지 못한다.
- 같은 무게 안의 순서는 `logical_date` 다. 입력이 시작시킨 실행과 배포가 시작시킨 실행은 `logical_date` 가 없어
  (2일간 41건 모두 NULL) 날짜가 있는 실행 뒤로 간다. 같은 무게끼리는 먼저 온 순서가 보장되지 않는다. 그래서
  생산자와 소비자는 무게가 같아서는 안 되고 달라야 한다.

### 다른 곳은 어떻게 하나

- Airflow 자신의 기본 규칙 `weight_rule="downstream"` 은 한 DAG 안에서 과제의 무게에 그것을 기다리는 과제들의 무게를
  더한다. 앞 과제가 뒤 과제보다 항상 무겁다. 우리는 작업마다 DAG 가 하나라서 이 규칙이 닿지 않았다(`absolute`).
- 우선순위 상속(Sha·Rajkumar·Lehoczky 1990)은 높은 작업이 기다리는 자원을 쥔 낮은 작업을 그 높이로 올린다.
  ADR-0439 의 `priority_class_from` 이 호스트 대기열에서 같은 일을 한다.
- Dagster 는 자산 그래프의 위상 순서로 실행을 세운다. 이행(ADR-0431)이 끝나면 이 계산은 그쪽 것이 된다.

## Decision

1. **Airflow 가 쓰는 무게는 계산한 값이다.** `job_specs.queue_weights` 가 작업마다 무게를 낸다: 작업이 적은
   `priority_weight`(단계)에서 시작해, 같은 풀에서 그 작업의 출력을 읽는 모든 작업의 무게보다 1 클 때까지 올린다.
   읽는 관계는 `producers()` 가 파이프라인 그래프의 간선에서 읽은 것 그대로다(ADR-0171 §1). `JobSpec.priority_weight`
   와 DAG 가 이 값을 쓴다. 손으로 맞추는 값이 없으므로 그래프와 어긋날 수 없다.
2. **서로의 출력을 읽는 작업들은 한 묶음이고 무게가 같다.** 지도 편집 접기 둘은 원장 하나를 함께 쓰고 읽는다.
   묶음 밖에서 묶음을 읽는 작업만 묶음을 올린다.
3. **작업이 적는 `priority_weight` 는 단계이고 열 칸씩 띄운다.** FLOOR·매시 Silver 적재 100, 접기 50, Silver 레인
   40, 패널 Gold 30, 그 밖의 Gold 20, 굽기 10, 안 적으면 1. 읽는 사슬이 두 단계 사이에 들어갈 자리다. 지금 가장 긴
   사슬은 `data_go_kr_file_data` 33 → `registered_factory` 32 → `lineage_stewardship` 31 → 패널 Gold 30 이다.
4. **`lineage_stewardship` 는 단계를 적지 않는다.** 10 이던 무게는 "혼자 도는 큰 작업이 매시 접기에 밀리지 않게"
   적은 값이었다. 그런데 계보 검토는 `court_auction_silver`·`registered_factory`·`data_go_kr_file_data`·토지이동
   레인의 표를 읽으므로, 그 무게를 두면 네 작업이 모두 접기 위로 올라가 접기의 대기 상한을 넘긴다(계산: 1,645분,
   한도 1,200분). 계보 검토의 자리는 그것이 쓰는 법정동 변경표를 읽는 Gold 재구축들 바로 위(31)다. 하루 한 번
   도는 작업이라 접기 뒤에 서도 자기 상한(20일) 안이다.
5. **대기 상한(`pool_starvation`)은 계산한 무게로 센다.** Airflow 가 실제로 세우는 순서와 같은 값이다.
6. 검사: `test_job_specs` 가 실제 작업 목록에서 같은 풀의 모든 생산자·소비자 쌍에 대해 생산자의 무게가 더 큰지
   본다. 막는 사고: 2026-11-11 `gold_parcel_nearby_rebuild` 가 자기 입력을 적재하는 `data_go_kr_file_data` 보다
   먼저 돌아 실패했고, 생산자는 16시간 넘게 기다렸다.

### 계산 결과 (실제 작업 목록, 2026-11-12)

| 작업 | 적은 단계 | 전(실효) | 후(계산) |
|---|---:|---:|---:|
| `data_go_kr_file_data` | 10 | 1 | 33 |
| `registered_factory`, `court_auction_silver` | 10 | 1 | 32 |
| `lineage_stewardship` | — | 10 | 31 |
| `parcel_registry` | 10 | 1 | 21 |
| `admin_boundary_release` | 20 | 2 | 21 |
| `parcel_served_gold`, `intelligence_document_fact`, `unit_price_versions_migration` | 10 | 1 | 11 |
| 그 밖의 spark 작업 71건 | ×10 | 같음 | 같음(상대 순서 그대로) |

spark 풀의 생산자·소비자 162쌍 가운데 생산자가 소비자보다 가볍거나 같은 쌍: 전 31쌍(가벼움 12, 같음 19) → 후 2쌍(한 묶음인 접기 둘이 서로를 읽는 것뿐).

## Consequences

- `data_go_kr_file_data` 는 Gold 재구축들보다 먼저 돈다. 주변 kind 의 Silver 표가 적재된 뒤에야 주변 Gold 가 돈다.
- 계보 검토는 FLOOR·접기·Silver 레인 뒤에 선다. 06:50Z 에 바로 돌던 것이 07:00Z 레인들 뒤로 갈 수 있다. 레인이
  바뀐 것이 없으면 Spark 없이 끝나므로 지연은 작다고 본다(측정하지 않았다).
- Airflow 줄에는 여전히 나이가 없다. 가장 낮은 단계(굽기, 유지보수)는 위 단계가 풀을 계속 채우면 기다린다. 그것은
  용량 문제이고 `FoundationJobQueueWaitOverBudget`(호스트 대기열, 예산 6시간)와
  `foundation_job_missed_start_seconds` 가 알린다. Airflow 의 무게 전략은 과제 인스턴스가 생길 때만 불려서 나이를
  줄 수 없다(위 측정). 줄 자체를 나이로 세우는 것은 Dagster 실행 대기열로 옮길 때의 일이다(ADR-0431).
- 같은 무게 안에서 `logical_date` 없는 실행이 뒤로 가는 것은 그대로다. 의존 관계가 있는 쌍은 이제 무게가 달라
  영향을 받지 않는다. 관계없는 같은 단계 작업끼리는 먼저 온 순서가 보장되지 않는다.
- 새 spark 작업은 단계만 고른다. 생산자·소비자 순서는 간선을 적는 것으로 끝난다.
- 그래프에 간선이 빠진 의존은 이 계산이 모른다. 간선의 완전함은 ADR-0171 의 검사(계보 보고와 대조)가 맡는다.
