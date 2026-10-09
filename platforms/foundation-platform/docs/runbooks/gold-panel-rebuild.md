---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-10
---

# 필지·건물 패널 Gold 재생성 — 예약 작업 런북

[루트 ADR-0139](../../../../docs/adr/0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md) 의 운영 절차다.
`gold.parcel_panel`·`gold.building_panel` 은 by-PNU 서빙 굽기([parcel-by-pnu-serving-bake.md](./parcel-by-pnu-serving-bake.md))의
원천이다. 이 작업은 Silver 입력이 행을 바꿨을 때만 기존 생산자로 Gold 를 다시 만든다.

| 무엇 | 정본 |
| --- | --- |
| 작업 목록·일정·풀·켜짐 | `orchestration/jobs.v1.json` 의 `gold_panel_rebuild` |
| 실행 계정·환경·시간 상한·쓰기 경로 | `infra/systemd/foundation-gold-panel-rebuild.service` |
| 한 번의 실행 | `scripts/ops/gold-panel-rebuild.sh all` (필지 다음 건물; 표 하나만은 `parcel`·`building`, 시험은 `--dry-run`, 감독 첫 실행은 `--unconditional <이유>`) |
| 끊긴 실행의 Spark 정리 | 단위의 `ExecStopPost` = `gold-panel-rebuild.sh cleanup` → 릴리스 publisher `stop-gold-panel-rebuild` |
| 표·생산자·행 손실 허용치·Spark 크기·측정 안 된 입력 | `infra/lakehouse/contracts/gold-panel-rebuild.contract.json` |
| 할 일이 있는지(계획) | `infra/lakehouse/spark/jobs/gold_rebuild.py` |
| 입력 표 목록 | 생산자의 상수(`ALL_SOURCES` 등, ADR-0130) |
| 메모리 | `tools/host-memory-budget.contract.json` 의 `scheduled_jobs.memory.gold_panel_rebuild` (compose `spark`, 20g) |

## 1. 끝에서 끝까지

```text
Silver 적재(FLOOR·계보·Silver 레인·수동 적재·백필)  행을 바꾼 새 Silver 스냅숏
  → gold_panel_rebuild (입력 이벤트 또는 08:45, spark 3슬롯)  계획: 새 Silver 있음 → 생산자 실행 → 새 Gold 스냅숏(판 기록 포함)
  → by_pnu_serving_bake (Gold 이벤트 또는 10:15, spark 1슬롯) 서빙하지 않은 Gold 스냅숏 → 새 세대로 굽기
  → manifest 이동                           샤드 행 수 합 = Gold 행 수일 때만
```

- 둘 다 `started_by: inputs` 다([루트 ADR-0171](../../../../docs/adr/0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)).
  Silver 를 쓰는 작업의 실행이 `foundation-job-outcome changed` 로 끝나면 Gold 작업이 곧 시작하고, 표 하나라도
  커밋하면 `changed` 로 끝나 굽기를 시작한다. 시각(08:45·10:15)은 대체 경로다. 손 적재는 이벤트를 남기지 않으므로
  그날 08:45 에 반영된다. 급하면 Airflow 화면에서 그 Silver 자산(`iceberg://silver.<표>`)에 이벤트를 만들거나
  `airflow-runtime.sh trigger gold_panel_rebuild` 로 시작한다.
- Gold 작업은 무게가 굽기보다 크다(3 > 1): 둘이 함께 기다리면 Gold 가 먼저 돈다.
- 굽기는 `takes_turns` 다. 이 작업이 켜진 뒤에는 Gold 작업이 한 번 시작한 뒤에야 굽기가 다시 시작한다.
- 할 일이 없으면 둘 다 `nothing to do` 로 성공한다.

## 2. 한 번의 실행

표마다(필지, 그다음 건물):

1. **계획** — compose `spark` 컨테이너에서 `gold_rebuild.py --gold-table <표>` 가 카탈로그의 `.snapshots`·`.refs`
   만 읽는다. 결과는 `/var/lib/foundation-gold-panel-rebuild/<표 단위>/runs/<UTC 시각>/plan.json`·`pins.json`.
   - Gold 의 `main` 조상 중 `foundation.source-iceberg-snapshots` 요약 속성이 있는 가장 새 스냅숏의 판이 기준이다.
     백필(`lakehouse_schema_migrate`)이 만든 기록 없는 스냅숏은 건너뛴다.
   - 입력마다 기준 판 뒤의 조상 중 행을 더하거나 지운 스냅숏이 있으면 바뀐 것이다. 압축(`replace`)·행 0 개 커밋은
     아니다. 만료로 기준 판이 지워졌어도 남은 가장 오래된 스냅숏의 `parent_id` 가 그 판이면 거기서 멈춘다.
   - 기록이 없는 Gold(이 작업 이전의 손 생성)는 행의 `published_at_utc` 최댓값 뒤의 변경을 본다. 이 시각은 생산자가
     **시작할 때** 찍히므로 그 사이의 변경을 놓칠 수 있다. 그래서 이 대체 판단은 작업이 꺼져 있을 때(감독 첫 실행)만
     쓴다. `jobs.v1.json` 에서 켜진 뒤에는(파일을 못 읽어도 켜진 것으로 본다) 계획기가 `--no-time-fallback` 으로 돌아
     기록 없는 Gold 를 거부한다 — 그 Gold 는 `--unconditional` 실행 한 번으로 기록을 갖게 한다(4절).
   - `--unconditional <이유>` 는 역사와 상관없이 다시 만든다. 이유는 계획의 `reasons` 와 로그에 남고, 행 하한과
     품질 검사는 그대로다.
   - 계약의 `unmeasured_inputs` 에 적힌 입력(지금은 `silver.parcel_lineage`)이 카탈로그에 생기면 계획이 거부한다.
     그 입력으로 잰 실행이 없어 20g 상한 안에 드는지 모르기 때문이다. `--dry-run` 만 그 입력을 읽을 수 있다(6절).
   - 아무것도 바뀌지 않았으면 `nothing to do` 로 그 표를 끝낸다.
2. **생산** — 모든 입력을 현재 판으로 고정해(`pins.json`) 생산자를 `--minimum-count` 와 함께 돌린다. 하한은 현재
   Gold 행 수 × (1 − `max_row_loss_fraction`) 의 올림이다. 생산자는 품질 검사(열·필수값·PNU·중복)와 하한을
   **쓰기 전에** 확인하고, 하나라도 어기면 아무것도 커밋하지 않고 실패한다.
3. **커밋** — 통과하면 새 스냅숏 하나(`overwrite`)를 커밋하고 요약에 판을 적는다. 이전 스냅숏은 역사에 남는다.
   실행 요약·계보 이벤트는 같은 run 폴더의 `summary.json`·`lineage.json`.

한 표가 실패해도 다른 표는 돈다. 단위는 실패하고 `foundation-unit-failed@` 가 슬랙으로 알린다.

**한 번에 하나.** 스크립트는 `/var/lib/foundation-gold-panel-rebuild/rebuild.lock` 을 기다리지 않고 잡는다. 예약 단위,
운영자의 무조건 재생성(`by-pnu-pack-operator.sh <레인> gold-rebuild`, 루트 ADR-0166: 이 단위 파일을 그대로 옮긴 임시 단위
`foundation-gold-panel-rebuild-unconditional`), 감독 실행이 모두 같은 잠금을 쓴다. 잡혀 있으면 75 로 끝나고 아무것도
계획하지 않는다.

**Spark 컨테이너의 정리.** Spark 는 compose 프로젝트 `foundation-gold-rebuild-<INVOCATION_ID>` 에서 돈다. 시간 상한
(160분)이 스크립트와 compose 클라이언트를 죽여도 컨테이너는 Docker 데몬의 것이라 20g 를 쥔 채 남는다. 그러면
Airflow 는 풀 3슬롯을 풀어 다음 작업을 시작하고 호스트 예산을 넘는다. 단위의 `ExecStopPost` 가 성공·실패·시간 초과
모두에서 `gold-panel-rebuild.sh cleanup` 을 부르고, 릴리스 publisher 의 `stop-gold-panel-rebuild` 가 그 프로젝트의
일회성 컨테이너와 네트워크만 멈추고 지운 뒤 남은 것이 없는지 확인한다(FLOOR 와 같은 구현, 루트 ADR-0128). 다른
작업·다른 실행의 컨테이너는 목록에 오르지 않는다. systemd 밖에서 손으로 돌리면 스크립트가 새 ID 를 만들고 정리
명령(`INVOCATION_ID=<id> gold-panel-rebuild.sh cleanup`)을 stderr 에 적는다.

## 3. 실패했을 때

| 로그 | 뜻 | 할 일 |
| --- | --- | --- |
| `fewer than the N the previous snapshot allows` | 행 손실이 허용치를 넘음, 커밋 없음 | 입력 중 어느 것이 지역·행을 잃었는지 run 폴더의 요약과 Silver 스냅숏을 본다. 진짜 감소면 감독 아래 생산자를 손으로 `--expected-count` 와 함께 돌린다(아래 5) |
| `Gold quality gates failed` · `duplicate` | 생산자 품질 검사 거부, 커밋 없음 | 생산자 문서의 거부 사유대로. 필지 중복은 [parcel-by-pnu-serving-bake.md](./parcel-by-pnu-serving-bake.md) 1절 |
| `required Silver inputs do not exist` · `no longer exist` | 입력 표가 없음 | 계획이 거부. 표를 되살리거나 생산자 입력을 바꾸는 결정이 먼저 |
| `records no source pins` | 켜진 작업이 판 기록 없는 Gold 를 만남 | 감독 아래 `gold-panel-rebuild.sh <표> --unconditional "<이유>"` 한 번(4절 3단계와 같음) |
| `no run was measured with them` | 계약의 `unmeasured_inputs` 입력이 생김 | 6절대로 `--dry-run` 을 재고, 상한 안이면 계약에서 그 항목을 지우는 배포 |
| `could not plan` | 카탈로그 읽기 실패 | 로그 `gold_rebuild.py.log` 확인 후 다음 날 실행이 다시 계획한다 |

굽기와의 상호작용: 굽기가 여러 실행에 걸치는 동안(필지 전국 약 4일) Gold 가 새 스냅숏을 커밋하면 굽기는
"moved during the bake" 로 멈추고 다음 실행이 새 세대로 처음부터 굽는다(ADR-0138). 필지 입력은 월 단위로 바뀌므로
보통은 닿지 않는다. 같은 달에 여러 번 바뀌면 그 사이 Gold 작업을 일시정지(DAG pause)해 굽기를 끝낸다.

## 4. 켜는 순서 (Turning it on)

작업은 `enabled: false` 로 등록됐고, 아래 순서를 2026-10-07 에 마쳐 켰다(필지 dry run 39,861,511행·1,435초·컨테이너
최대 18.5GiB, OOM 없음; 무조건 재생성 한 번). 호스트는 꺼진 작업의 시작을 거부한다.

1. 감독 실행: FLOOR·계보·접기 둘·굽기 DAG 를 일시정지하고 `systemctl show -p ActiveState` 로 모두 inactive 확인.
2. 같은 스크립트를 `--dry-run` 으로 먼저 돌려 `dry run passed` 와 행 수를 확인한다
   (`/opt/foundation-platform/current/scripts/ops/gold-panel-rebuild.sh all --dry-run`, unit 과 같은 환경 파일).
   이때 `silver.parcel_lineage` 가 있으면 필지 dry run 의 컨테이너 최대 메모리를 재서 6절에 적는다(그 전에는 켜지 않는다).
3. **무조건 재생성 한 번**: `gold-panel-rebuild.sh all --unconditional "first supervised run: every Gold records its pins"`.
   이 작업 이전에 만든 Gold 는 판 기록이 없고, 기록 없는 Gold 의 유일한 근거인 `published_at_utc` 는 생산자 시작
   시각이라 변경을 놓칠 수 있다. 그래서 첫 실행은 역사를 보지 않고 두 표를 다 다시 만든다(행 하한·품질 검사는 그대로).
   이유는 계획과 로그에 남는다. Spark 는 컨테이너 안에서 돌므로 메모리는 단위의 `MemoryPeak` 가 아니라 컨테이너
   (20g 상한)의 OOM 여부로 본다.
4. 두 Gold 의 현재 스냅숏 요약에 `foundation.source-iceberg-snapshots` 가 있는지 확인한다(카탈로그
   `SELECT summary FROM <표>.snapshots`). 둘 다 있어야 켠다. 켠 뒤에는 계획기가 기록 없는 Gold 를 거부한다.
5. 굽기 한 번(그 런북 8절)이 새 Gold 를 구워 발행했는지 확인. 필지도 새 스냅숏이므로 굽기가 새로 굽는다
   (ADR-0138 전량이면 약 81시간, ADR-0141 패치 세대가 운영에 들어간 뒤라면 바뀐 문서만).
6. 배포로 `jobs.v1.json` 의 `enabled` 를 `true` 로, `disabled_reason` 을 지운다.

## 5. 손으로 다시 만들 때

예약 작업과 같은 경로를 쓴다: `gold-panel-rebuild.sh <표>` 는 계획이 `nothing to do` 라고 하면 아무것도 하지
않는다. 그래도 다시 만들어야 하면(생산자 코드가 바뀐 경우 — 이 작업은 코드 변경을 보지 않는다) 생산자를 직접
돌린다. 생산자가 커밋할 때 판 기록을 남기므로 다음 예약 계획은 그 스냅숏에서 시작한다.

## 6. 측정 (2026-10-03, ai-server scratch)

`building_panel_silver_to_gold.py --validate-only`, 현재 Silver 판, 20g 컨테이너, `local[8,8]`·driver 16g:
1,946초(32분), 익명 메모리 최대 19,333,795,840 B, `memory.peak` 20GiB(상한, OOM 없음), 5,268,380행.
같은 조건의 필지(`--no-carry-lineage`): 1,394초(23분), 익명 메모리 최대 19,367,006,208 B, 39,861,511행.
행 손실 게이트는 이 판을 거부한다(현재 Gold 5,939,794행의 99% 미만): 표제부 09-27 판의 PNU 빈 행이
965,150개로 늘어난 Silver 회귀 때문이다(ADR-0139 측정 절). 그 회귀를 고치기 전에는 4절 3단계가 `fewer than`
으로 끝나는 것이 맞는 결과다.

**측정하지 못한 것: 필지의 계보 이어붙이기.** 필지 생산자는 `silver.parcel_lineage` 가 있으면 번호가 바뀐 필지의
속성을 계보로 이어 붙인다(조인 하나 더). 2026-10-03 측정 때도, 2026-10-04 다시 읽었을 때도 카탈로그에 그 표가
없어서 `--no-carry-lineage` 로만 쟀다. 그 측정이 이미 익명 메모리 18.0GiB 로 상한 20g 에 가깝다. 그래서 계약의
`gold.parcel_panel.unmeasured_inputs` 에 그 표를 적어 두었고, 표가 생기면 계획이 거부한다. 켜기 전(4절 2단계)이나
표가 생긴 뒤에 `--dry-run` 으로 재고, 최대치가 상한 − 여유(약 2GiB)를 넘으면 ADR-0138 의 메모리 가드
(`scripts/guard/every-container-has-a-memory-cap.sh`)를 통과하는 범위에서 compose `spark` 상한을 올리거나 driver 를
줄이는 결정을 먼저 한 뒤 계약에서 그 항목을 지운다.

Airflow 시간 상한 165분(systemd 는 160분)은 매시 접기의 굶김 상한이 한도(1,200분 = 주기 20번)에 닿는 값이다(`job_specs.pool_starvation`).
