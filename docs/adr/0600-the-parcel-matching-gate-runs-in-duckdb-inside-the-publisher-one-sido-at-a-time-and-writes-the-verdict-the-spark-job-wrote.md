# ADR 0600: 필지 대조 관문은 발행기 안의 DuckDB 에서 시도마다 한 번씩 돌고, Spark job 이 쓰던 판정을 바이트까지 그대로 쓴다

- Status: Accepted
- Date: 2026-11-21
- Builds on: [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md)(변환은 발행기 안 DuckDB 에서, 엔진 동등 판정이 있어야 옮긴다),
  [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §7(굽기 전 대조 관문),
  [ADR-0265](./0265-the-matching-gate-waits-for-the-parcel-registry-it-reads.md)(등록부가 없으면 관문은 기다린다),
  [ADR-0490](./0490-a-new-cadastral-edition-becomes-the-served-edition-with-no-person-and-a-lagging-served-edition-is-said.md) §4a(등록부는 판 기준으로 읽는다)

## Context

필지 타일 굽기의 Gold 쪽(`scripts/ops/parcel-tile-bake.sh`)은 판을 굽기 전에 대조 관문 `parcel_matching_gate.py` 를
Spark 컨테이너로 돌렸다. 관문은 `foundation-platform.parcel_matching_verdict.v1` 판정 파일을 쓰고, 굽기는 그 판정에
자기 개정을 묶는다(`lakehouse_bake_verdict.rs`: 통과, `registry == "checked"`, `parcels.checked ==
snapshot_parcel_count`). 사장 우선순위(2026-11-20)는 DuckDB·Dagster 가 Spark·Airflow 를 대신하는 전환이다.
ADR-0559 는 변환을 엔진 동등 판정이 있을 때만 옮긴다고 정했다.

측정(202609 판, 39,876,546 필지, 입력 다섯 표는 Spark run 과 같은 스냅숏에 고정, ai-server, 2 스레드, 2 GiB):

| 실행 | 시간 | 정점 메모리 | 결과 |
| --- | --- | --- | --- |
| Spark 관문, 운영 연습 2026-11-20 | 2,584초 | Spark 컨테이너 | 통과 |
| DuckDB, 판 전체를 한 번에 | 2시간 넘게 돌다 멈춤 | 1.2 GiB 한도에서 15 GB 넘침 | 판정 없음 |
| DuckDB, 시도 41 하나(5,211,798 필지) | 97초 | 1.4 GiB | 등록부 Iceberg 읽기 61초·마지막 현재 행 14.6초 |
| DuckDB, 시도마다 한 번(이 결정) | 1,895초 | 1,475 MiB | Spark 판정과 바이트까지 같음 |

판 전체를 한 번에 돌리면 4천만 행이 해시 표 하나에 몰려 1.2 GiB 한도에서 끝없이 넘쳐 쓴다. Spark 는 같은 일을
파티션마다 나눠 셔플한다. 시도 하나는 2 GiB 안에 넉넉히 들고, Iceberg 는 PNU 접두 조건으로 그 시도의 파일만
읽는다(시도 41: 행 묶음 12개 중 10개).

## Decision

1. 관문은 발행기 명령 `run-parcel-matching-gate` 이다(`foundation-outbox-publisher`, 안의 계산은
   `foundation-publisher-silver::parcel_matching_gate_stage`). 굽기 스크립트는 Spark 대신 이 명령을 부르고, 판정
   파일 경로·종료 코드(통과 0, 거부 1, 등록부 없음 3)·판정 v1 의 키와 바이트는 Spark job 그대로다(Python
   `json.dumps(…, ensure_ascii=False, indent=2, sort_keys=True)` 와 같게 쓴다). `parcel_matching_gate.py` 와 그
   Spark 시험은 지운다.
2. 관문의 검사는 `gate.sql` 하나에 SQL 로 있고, 검사하는 시도마다 한 번 돈다. 그 시도 접두를 판과 등록부 읽기
   양쪽에 글자로 넣어 Iceberg 가 그 파일만 읽게 한다. `collect.sql` 이 각 run 의 셈·이유마다 첫 20개 표본·쥔 id 를
   모으고, 시도를 건너갈 수 있는 유일한 검사인 "두 필지가 나눈 id" 는 모은 id 위에서 한 번 묶는다. 검사하지
   않는 접두(다른 시도·잘못된 PNU 의 앞자리·NULL PNU)는 세기만 해 `snapshot_parcel_count` 는 판 전체의 서로 다른
   PNU 다.
3. 입력은 다섯 표의 머리 스냅숏에 고정해 읽는다(`AT (VERSION => …)`). 등록부는 Spark 처럼 판까지의
   `to_snapshot_id` 와 검사하는 시도로 거른다. 필지별 마지막 현재 행은 창 함수가 아니라 해시 집계 두 번
   (`max(k)`, 그 키의 `max(parcel_id)`)이라 넘쳐 쓸 수 있다.
4. 잘 생긴 PNU(19자리)는 UBIGINT 로 들고, `regexp_full_match('[0-9]{19}')` 로 판단한다. 202609 에서는 잘못된
   PNU 가 0건이라 판정이 같다.
5. 굽기 unit 은 그대로다(2 GiB, Gold 쪽 115분). DuckDB 메모리 한도는 unit 의 3/5, 스레드는 엔진 계약의
   `cpu_slots`(2)이고, 넘친 파일은 run 의 작업 디렉터리(`/data`)에 쓴다. Gold 쪽 시간은 관문 약 32분과 서빙본
   Gold 약 58분으로 115분 안이다(Spark 관문은 43분이었다). unit 은 읽기 전용 키
   `/etc/foundation-platform/lakehouse-reader.env` 를 받는다.
6. Spark job 의 `--attribute`·`--lineage-from-snapshot-id` 검사는 옮기지 않는다. 그 플래그를 넘기던 호출이 없었고
   `attributes` 는 `{}` 그대로다.

## Consequences

- 굽기 Gold 쪽에서 관문을 위한 Spark 컨테이너가 사라진다. 서빙본 Gold(`parcel_boundary_served_gold.py`)와
  fold 는 아직 Spark 이고, 다음 단계에서 같은 동등 판정을 거쳐 옮긴다.
- 가장 긴 단계는 이제 모은 id 4천만 개를 묶는 마지막 단계다(678초). 시간이 모자라면 id 의 해시로 먼저 묶고
  후보만 글자로 다시 보는 길이 있다. 판정을 바꾸지 않는 최적화라 그때 재서 바꾼다.
- 공개 저장소 고정 자료는 `99999` PNU 만 쓸 수 있어(공개 고정 자료 안전 검사), 시도를 건너가는 id 공유는 단위
  시험이 한 시도 안의 공유로만 확인한다. 같은 집계를 모든 시도의 id 위에서 돌릴 뿐이다.
- 판정 비교는 운영 판정 파일과 같은 입력 스냅숏에서 했다. 판정이 다르면 바꾸지 않는다는 ADR-0559 의 조건을
  이 관문은 채웠다.
