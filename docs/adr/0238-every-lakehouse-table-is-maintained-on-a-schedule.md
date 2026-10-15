# ADR 0238: 모든 레이크하우스 표를 정해진 시각에 유지보수한다

- Status: Accepted
- Date: 2026-10-14

## Context

`lakehouse_maintenance.py`(합치기·스냅숏 만료·고아 청소)는 `scripts/load/lakehouse-batch-load.sh` 가 silver 표를
적재한 직후에만 불렀다. 2026-10-14 REST 카탈로그 메타데이터로 표 49개를 전수 측정했더니 gold 표와 다수 표에 30~96일
된 스냅숏이 남아, 지워졌어야 할 데이터 파일 약 80.6GB(현재 데이터 66.6GB 대비)를 붙들고 있었다. 보존 규칙(루트
ADR-0237)은 작업이 그 표를 찾아갈 때만 의미가 있다.

같은 조사에서 무조건 만료하면 깨지는 것 셋을 확인했다.

1. Silver 적재(Rust `silver_refresh`, Python `lakehouse_ingest`)는 "이미 적재했는가"를 `main` 계보의 스냅숏 요약
   `foundation.ingest-batch-objects` 의 합집합으로 판단한다. 부분 레인은 모든 부분의 기록이 남아 있어야 적재됐다고 본다
   (`decide_parts`). 기록이 담긴 스냅숏 하나가 만료되면 그 부분을 다시 적재한다. 추가(append) 표에서는 행이 두 번 쌓인다.
   배치 적재 뒤의 7일 만료가 이미 이 위험을 안고 있었다.
2. Gold 재구축은 `main` 계보에서 가장 새 `foundation.source-iceberg-snapshots`(source pin)를 읽어 계획한다. 합치기가
   그 위에 `replace` 스냅숏을 얹은 뒤 창이 지나면 pin 스냅숏이 만료되고, 재구축이 계획을 거부한다.
3. FLOOR 작업은 머리에서 증인 스냅숏까지 `append` 커밋만으로 이어지는지 검사한다(`verify_append_descendant`).
   합치기의 `replace` 하나로 다음 FLOOR 실행이 거부된다.

## Decision

1. 정기 작업 `lakehouse_maintenance`(orchestration/jobs.v1.json): 매일 04:30 UTC, `spark` 풀 3칸, Airflow 165분,
   systemd `TimeoutStartSec=160m`. 이 시각 spark 풀에는 매시 fold 만 돈다. 마감(140분) 뒤엔 새 표를 시작하지 않아
   06:50 lineage 전에 손을 뗀다. 01:10 은 기각했다: 일일 spark 작업의 첫 시작이 운영자 by-PNU 전환(`by-pnu-lane-cutover.sh`)이
   피하는 일일 창의 시작이라, 밤 전환 시간을 01:10 까지 줄인다. 04:30 은 그 창의 시작을 06:50 에서 04:30 으로 당길 뿐
   밤 전환 시간(창 끝 ~ 04:30)은 남는다.
2. 한 번의 Spark 실행(`lakehouse_maintenance.py --all-tables`)이 레이크하우스 계약 산출물(`platform_contracts.
   load_lakehouse_artifact()["contracts"]`, 표 목록의 정본)에 있고 카탈로그에 존재하는 표 전부를 돈다. 가장 오래전에
   유지보수한 표부터(상태 디렉터리의 `rotation.json`). 유지보수 계약의 `scheduled.deadline_minutes`(유닛 상한보다 작다)
   뒤에는 새 표를 시작하지 않는다. 못 간 표는 다음 실행에서 맨 먼저다. 한 표의 실패는 나머지를 멈추지 않고, 하나라도
   실패하면 실행이 실패한다.
3. 만료는 이 레이크하우스가 메타데이터에서 다시 읽는 스냅숏을 지우지 않는다(`protected_snapshots`).
   - `main` 계보의 모든 적재 기록 스냅숏(`lakehouse_ingest.INGEST_BATCH_OBJECTS_KEY`).
   - `main` 계보에서 가장 새 Gold pin 스냅숏(`gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY`).
   - `older_than` 을 보호 스냅숏 중 가장 오래된 것의 커밋 시각(밀리초)까지 당긴다. 키 이름은 그것을 쓰는 모듈의 상수를
     import 한다.
   이 규칙은 배치 적재 뒤의 호출에도 같이 적용된다.
4. 표별 예외는 유지보수 계약 `tables` 한 곳: `silver.building_register_floors` 는 합치기를 건너뛴다(위 3).
5. 고아 청소는 정기 실행에서 빠진다(`scheduled.skip`). 서버에서 R2 를 상대로 돌려 본 적이 없다. 운영자 단계로 남는다.
6. 작업은 행을 바꾸지 않으므로 `foundation-job-outcome unchanged` 로 끝나고, 아래 작업을 깨우지 않는다(루트 ADR-0171).
7. 시간 초과로 죽은 실행의 Spark 컨테이너는 `ExecStopPost` → publisher `stop-lakehouse-maintenance` 가 이 실행의
   Compose 프로젝트(`foundation-lakehouse-maintenance-<INVOCATION_ID>`)만 지운다(루트 ADR-0128).

## Consequences

- gold 표와 적재 기록이 없는 표는 30일 창과 태그 규칙대로 줄어든다.
- 적재 기록이 계보 전체에 걸친 silver 표는 지금처럼 거의 만료되지 않는다. 기록이 스냅숏 요약에만 있는 한 만료가 곧
  "적재를 잊음"이기 때문이다. 기록을 스냅숏 밖(예: 적재 원장)으로 옮기는 일은 별도 결정이다.
- 배치 적재 뒤 7일 만료가 적재 기록을 지우던 잠재 결함이 닫힌다.
- 실행 중에는 릴리스 빌드가 기다린다(ADR-0137). 04:30~07:15 UTC 사이다.

## 기각한 대안

- **표마다 정기 작업 하나**: 49개 작업이 spark 풀을 나눠 쓰면 기아 한도(STARVATION_CYCLES)를 넘는다. 한 실행이 차례로 돈다.
- **보호 스냅숏에 태그를 단다**: 영구 참조가 계속 늘고, 적재가 끝난 뒤 누가 지울지가 또 하나의 규칙이 된다. 실행마다
  계산하는 바닥선은 남는 것이 없다.
- **적재 기록이 있는 표는 만료를 통째로 건너뛴다**: 계보 밖 스냅숏(되돌린 커밋·지난 브랜치)까지 남는다. 바닥선은 다시
  읽히는 것만 지킨다.
