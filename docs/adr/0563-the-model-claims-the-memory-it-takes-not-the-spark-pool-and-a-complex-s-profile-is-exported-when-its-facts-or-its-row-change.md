# ADR 0563: 모델은 spark 풀이 아니라 자기가 쓰는 메모리를 claim 하고, 단지 프로필은 그 단지의 사실이나 행이 바뀔 때 나간다

- Status: Accepted
- Date: 2026-11-13
- Amends: [ADR-0478](./0478-the-intelligence-model-is-strata-with-qwen3-8-flash-next-on-the-upgraded-host-and-until-then-qwen3-6-time-shares-the-spark-pool.md)
  §2 의 "그것을 `Requires=` 하는 작업이 spark 풀의 슬롯을 전부 잡고 도는 동안만 뜨고"(풀 전부 → 메모리 claim), 그리고
  [ADR-0456](./0456-industrial-complexes-are-served-from-the-catalog-edge-as-one-index-and-per-complex-profiles.md) §5 의
  일정(계보 순환의 한 단계 → 자기 작업). 두 ADR 의 나머지는 그대로다.
- Builds on: [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(풀과 칸),
  [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)(유닛마다 메모리 상한, batch 가 먼저 죽는다),
  [ADR-0435](./0435-a-by-pnu-lane-publishes-itself-through-automated-analysis-and-capacity-and-priority-not-the-clock-decide-when-work-starts.md)(시계가 아니라 용량과 우선순위),
  [ADR-0462](./0462-a-start-its-pool-refused-waits-in-a-queue-that-ages-it-and-a-claim-holds-the-pool-only-for-a-spark-step.md)(거부된 시작의 대기열),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(작업은 바뀐 데이터로 이어진다),
  [ADR-0477](./0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md)(문서 사실과 정밀도 관문)

## Context

문서 텍스트는 다 읽혔다(1,688 문서, 44,929 쪽). 사실은 한 문서에서 여섯 개, 단지 하나의 `gold.complex_document_facts` 한 줄까지만
갔고, 그 단지의 패널에는 아직 보이지 않는다. 막힌 곳은 둘이다.

### 1. 사실 작업은 빈 spark 풀을 기다린다

ADR-0478 §2 는 24G 모델이 Spark 옆에서 메모리를 쥐지 않게 하려고 사실 작업이 spark 풀 3칸을 전부 잡게 했다. 3칸짜리 작업은 풀이
비어야 시작한다.

| 측정 (ai-server, 2026-11-13 11:00Z, 읽기 전용) | 값 |
|---|---|
| 사실 작업의 run | 한 번(11-12 20:49Z, 문서 1건, 규칙만으로 닫혀 모델을 부르지 않았다; 런타임 CPU 3.5 초) |
| 그 run 이 시작하기까지 기다린 시간 | 26,931 초(대기열 기록) |
| 지금 기다리는 시작 | 11-13 04:04Z 부터, 거부 이유 "by_pnu_lane_cutover 의 claim(first_publish, 3칸)이 먼저" |
| 호스트 메모리 | 62.6 GiB 중 사용 17.4, `MemAvailable` 46.7 GiB(작은 Spark 세 개가 도는 중) |
| GPU | 8 GiB 중 9 MiB 사용(아무 모델도 올라와 있지 않다) |
| `MemAvailable`, 7일(Prometheus, 15초 표본) | 최소 8.68 · 5% 30.32 · 중앙 43.54 · 최대 53.41 GiB |
| 같은 7일에서 30 GiB 미만이던 시간 | 4% (26 GiB 미만 1%) |
| 모델이 실제로 쥐는 메모리 | RAM 21.4 GB + VRAM 5.7 GB(ADR-0478 의 11-11 실측, 매핑한 가중치의 페이지 캐시 포함; 상한 24G) |

- 풀의 칸은 **상한**의 합을 묶는다(ADR-0138): 가드는 상시 서비스의 상한 33.3 G + 예비 5 G + 풀이 허용하는 최악 조합을 62 G 안에
  맞춘다(지금 61.34 G). 상한으로 세면 24G 모델은 무엇 옆에도 들어가지 않는다. 그래서 풀 전부를 잡았고, 그 모델의 24G 는 가드의 합에
  들어가 있지도 않았다(작업의 메모리 원천에 런타임 유닛이 없다).
- 실제로는 상시 서비스가 상한의 3분의 1 을 쓴다(Trino 는 상한 16 G 에 2 G). 호스트는 일주일의 95% 동안 30 GiB 넘게 비어 있었다.
  모델이 정말 충돌하는 것은 "Spark 가 있다"가 아니라 "지금 남은 메모리가 24G 보다 적다"이다.
- 사실 작업의 Spark 단계(계획·쌓기·접기)는 `spark-small`(2.5 G) 세션이다. 큰 Spark 를 쓰지 않는다.

### 2. 프로필 내보내기는 계보 순환 안에 있다

단지 프로필과 목록을 엣지에 쓰는 `export-industrial-complex-gold-profiles` 는 Spark 를 쓰지 않는 발행기 명령이다. 그런데 ADR-0456 §5 가
그것을 `lineage_stewardship`(spark 3칸, 하루 한 번)의 한 단계로 두었다. 11-12 20:55Z 에 접힌 사실은 11-13 06:50Z 의 계보 순환을
기다렸고, 그 순환은 같은 claim 뒤에서 거부되어 11:00Z 에도 돌지 않았다. 사실이 바뀌어도 그것을 알리는 간선이 없었다(그래프에 엣지 문서
노드가 없다).

### 다른 곳은 어떻게 하나

- **Borg**(Verma 외, EuroSys 2015 §5.5 "resource reclamation"): 작업은 한도(limit)를 선언하지만 스케줄러는 실제 사용량에 여유를 더한
  예약(reservation)을 재고, 그 차이(회수된 자원)에 일괄 작업을 넣는다. 한도의 합으로만 채우면 기계의 절반이 논다. 회수된 자원에 든
  일괄 작업은 자원이 모자라면 먼저 죽는다.
- **Kubernetes**: 스케줄링은 `requests` 로, 상한은 `limits` 로 한다. 둘이 다른 Pod(Burstable)는 노드가 압박을 받으면 Guaranteed 보다
  먼저 쫓겨난다. 우리의 batch 등급(`OOMScoreAdjust=500`, ADR-0455)이 같은 순서다.
- **Slurm** 의 GRES·`--mem`: GPU 와 메모리는 작업이 따로 요청하는 자원이고, 노드의 CPU 칸 수와 별개로 센다.
- **Airflow asset / Dagster 자산 그래프**: 하류는 상류가 바뀌었다는 사건으로 시작한다(ADR-0171 이 이미 그 방식이다).

### 이미 있는 것 (새 메커니즘 전에 증명)

전 트리(`platforms/foundation-platform/{orchestration,scripts,definitions,infra}`, `scripts/guard`, `tools`, `docs/adr`)를 찾았다.

| 필요 | 이미 있는 것 | 판정 |
|---|---|---|
| 시작 승인 한 곳 | `scripts/deploy/job_units.py pool-full`(모든 시작과 자산 조정기가 묻는다) | 그대로 쓰고 거부 사유 하나를 더한다 |
| 거부된 시작의 대기·나이 | 시작 대기열(ADR-0462) | 그대로 |
| spark 칸을 안 잡는 문서 작업의 풀 | `documents` 풀 1칸(텍스트 작업) | 사실 작업을 같은 풀에 둔다 |
| 풀 밖 작업의 claim | `capacity_claimants`(ADR-0435) | 맞지 않는다: 풀의 **칸**을 잡는 장치라 메모리 양을 말하지 못한다 |
| 메모리 양으로 승인하는 장치 | 없다(가드는 CI 에서 상한 합만 본다) | 승인에 추가한다(아래 1) |
| 상류가 바뀌면 시작 | `started_by: inputs`(ADR-0171) | 그대로 |
| 바뀐 단지만 쓰는 내보내기 | 내용 해시 비교(ADR-0456 §2) | 그대로 |
| 포인터 발행과 `catalog.industrial_complex.gold_pointer.published.v1` | 있다(ADR-0036·0051) | 맞지 않는다: 웹은 ADR-0456 부터 엣지 목록에서 프로필 키를 읽고, 그 사건의 소비자는 건드리지 않는 AWS 제품 API 의 Postgres 사본뿐이다 |

## Decision

1. **작업은 풀의 칸이 대신하지 못하는 메모리를 `memory_claim` 으로 말한다.** `jobs.v1.json` 의 작업이
   `memory_claim.units`(그 run 이 채울 수 있는 systemd 유닛들)를 적는다. 양은 그 유닛들의 `MemoryMax` 이고(ADR-0455 가 렌더링한 값,
   숫자를 다시 적지 않는다), 시작 승인(`job_units.py pool-full` 의 `memory_short`)이 호스트의 `MemAvailable` 에서 그 양을 빼고도
   `memory_claims.keep_available`(6g)이 남을 때만 승인한다. claim 한 유닛이 이미 쥔 몫은 다시 세지 않는다. 안 맞으면 풀이 찼을 때와
   같은 답(`foundation-job-outcome waiting …`)이고 같은 대기열에 선다. 시계 창은 없다.
   - `keep_available` 6g = 사실 작업과 텍스트 작업의 `spark-small` 세션(2,560m 씩)과 1 G.
   - claim 은 상한의 합이 아니라 **지금 남은 메모리**에 대해 승인한다. 상한 합 검사(ADR-0138)는 그대로이고, claim 한 유닛은 그 합의
     선언된 예외다. claim 한 유닛은 batch 등급이어야 한다: 승인 뒤 다른 것이 자라면 커널이 모델의 매핑된 쪽을 먼저 회수하고,
     그래도 모자라면 batch 를 먼저 죽인다. 사실 작업은 문서 단위로 쌓으므로 죽은 run 의 문서는 다음 run 이 다시 읽는다.
2. **사실 작업은 spark 칸을 잡지 않는다.** `intelligence_document_fact` 는 `documents` 풀 1칸에서 돌고(문서 작업은 한 번에 하나라서
   GPU 도 한 작업만 쓴다), 모델 런타임 유닛을 `memory_claim` 으로 적는다. 텍스트 작업이 쪽을 쌓았다고 알리면 시작하고
   (`started_by: inputs`), 매시가 대체 일정이다. 대체 일정은 모델 예산 때문에 다음으로 넘긴 문서도 다시 읽는다.
3. **단지 프로필 내보내기는 자기 작업이다.** `industrial_complex_edge_export`(풀 없음, 발행기만): `gold.complex_catalog` 를 쓰는
   계보 순환이나 `gold.complex_document_facts` 를 쓰는 사실 작업이 `changed` 로 끝나면 시작하고, 매일 한 번이 대체 일정이다
   (손으로 다시 지은 Gold). 내용 해시가 바뀐 단지의 프로필만 쓰고 목록이 바뀌었을 때만 새 세대를 낸다(ADR-0456 §2 그대로).
   배포 뒤에도 한 번 돈다. 그래프에 노드 `industrial-complex-edge-documents` 와 두 간선을 더한다. 계보 순환은 더는 내보내지 않는다.
4. **검사.**
   - 가드 `every-container-has-a-memory-cap.sh`: claim 한 유닛들의 상한 + `keep_available` 이
     `scheduled_jobs.memory_claims.available.p05`(실측한 기간의 95% 동안 호스트에 남아 있던 메모리) 안에 들어야 한다. 넘는 claim 은
     굶는 작업이다. claim 한 유닛은 batch 이고 실측이 계약에 적혀 있어야 한다. 막는 사고: 2026-11-13 사실 작업의 24,600 초 대기.
   - `job_specs.py`: claim 하는 작업은 칸이 하나인 한 풀에 있어야 한다(두 claim 이 같은 남은 메모리에 대해 함께 승인되지 않게).
   - `test_job_units.py`: 11-13 의 상황(spark 작업이 돌고 cut-over 가 풀 전부를 claim) 에서 메모리가 있으면 시작하고, 없으면 거부한다.
   - `test_industrial_complex_edge_export.py`: 내보내기 작업은 어느 Spark 풀에도 없고, 두 표를 쓰는 작업이 그것을 시작시킨다.

## 화면에 싣는 것은 그대로다

ADR-0477 §5 의 관문은 바뀌지 않는다. 화면에 실리는 것은 손 확인을 넘긴 (종류, 방법, 쪽 방법)뿐이다:

| 사실 종류 | 방법 · 쪽 | 손 확인(2026-11-11) | 화면 |
|---|---|---:|---|
| `land_use_area` | 규칙 · 텍스트 층 | 30/30 | 싣는다 |
| `invited_industry` | 규칙 · 텍스트 층 | 30/30 | 싣는다 |
| `parcel_lot` | 규칙 · 텍스트 층 | 30/30 | 싣는다 |
| `sale_price` | 규칙 · 텍스트 층 | 재지 않음 | 싣지 않는다 |
| 모든 종류 | 규칙 · OCR 쪽 | 재지 않음 | 싣지 않는다 |
| `land_use_area` | 로컬 모델 | 재지 않음 | 싣지 않는다 |

세 종류의 정밀도는 30건 중 30건이다(관문은 30건 이상, 0.98 이상). 30건 표본에서 오류 0 건이면 실제 정밀도의 95% 신뢰 하한은 약
0.905 다(3의 법칙). 사실이 쌓이면 표본을 넓혀 다시 잰다. OCR 쪽과 모델 사실은 표에 쌓이되 접기가 화면에서 뺀다.

## Consequences

- 사실 작업은 호스트에 30 GiB 가 남아 있으면 시작한다. 지난 7일 기준으로 시간의 96% 다. 남은 4% 는 큰 Spark 가 메모리를 실제로
  쓰고 있을 때이고, 그때는 기다리는 것이 옳다.
- 모델이 큰 Spark 와 함께 도는 시간이 생긴다. 승인은 시작 시점의 측정이라, 뒤에 시작한 20g Spark 가 상한까지 자라면 남는 메모리가
  부족해질 수 있다(7일 최소 8.68 GiB 에 모델 21 GB 를 더한 경우). 그때 일어나는 일은 순서대로: 모델의 매핑된 가중치 쪽 회수(모델이
  느려진다, 문서 하나의 한도 600 초) → OOM 이 batch 를 죽임(모델이나 Spark; 둘 다 다시 돈다) → 상시 서비스와 백업은 보호 등급이라
  남는다. 이 위험을 없애려면 뒤에 시작하는 작업도 자기 상한을 남은 메모리에 대해 승인받아야 하고, 그것은 작업마다의 상한을 호스트가
  읽을 수 있게 내보내는 일이다. 측정이 필요를 보이면 그때 한다: `FoundationHostMemoryLow`(ADR-0385)와 OOM 기록이 그 신호다.
- 프로덕션에서 모델이 문서를 읽은 적은 아직 없다(유일한 run 이 규칙만으로 닫혔다). 21.4 GB 는 11-11 시험의 값이다. 첫 모델 run 의
  유닛 `memory peak` 가 계약의 `claimed_measured` 를 갱신한다.
- 사실 작업과 텍스트 작업은 한 칸을 나눠 쓴다. 텍스트가 밀린 문서를 읽는 한 시간 동안 사실 작업은 기다린다(그 반대도 같다).
- 사실이 접히면 다음 Airflow 사건에서 내보내기가 돌고(수십 초), 패널은 다음 목록 요청에서 새 프로필을 읽는다. 계보 순환이 밀려도
  프로필은 밀리지 않는다.
- ADR-0478 §3 의 증설(RAM +64 GB) 뒤에는 `available` 을 다시 재서 계약에 적는다. claim 의 방식은 그대로다.
