# ADR 0139: 필지·건물 패널 Gold 는 Silver 입력이 바뀌면 등록 작업이 다시 만든다

- Status: Accepted
- Date: 2026-10-03
- Builds on: [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md) (작업 = systemd 단위 + ops 스크립트 + `jobs.v1.json`),
  [ADR-0130](./0130-panel-input-snapshots-are-complete-and-bound-before-spark.md) (입력 판 고정),
  [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md) (풀·메모리 예산)

## Context

by-PNU 서빙 굽기(PR #318)는 Gold 에 서빙하지 않은 스냅숏이 있을 때만 굽는다. 그런데 그 Gold 를 만드는 것은
아무도 예약하지 않았다. `gold.parcel_panel`·`gold.building_panel` 의 생산자(`parcel_panel_silver_to_gold.py`,
`building_panel_silver_to_gold.py`)를 부르는 곳은 저장소 안에 없었고, 사람이 손으로 돌렸다.

2026-10-03 ai-server 카탈로그를 읽어 확인한 결과:

- `gold.building_panel` 의 현재 스냅숏은 2026-09-12 에 커밋됐다. 그 뒤 Silver 입력 다섯 중 넷이 행을 바꿨다
  (표제부 09-27, 층 09-30, 호 09-26·09-30 — 호의 09-30 은 ADR-0125 의 `unit_pnu` 백필, 면적 09-30).
  그래서 Gold 행 5,939,794 개 중 `unit_pnu` 를 가진 것이 0 개이고, 새 건물 굽기가 그 Gold 를 거부한다
  (`Gold unit is missing its source unit_pnu`). 데이터 결함이 아니라 낡은 Gold 다.
- `gold.parcel_panel` 의 현재 스냅숏(10-01)은 생산자가 아니라 lakehouse 마이그레이션의 `row_digest` 백필이
  다시 쓴 것이다. 행이 만들어진 것은 09-09 이고, 입력 Silver 아홉의 마지막 변경은 09-07 이다. 다시 만들 일이 없다.
- Silver 표에는 행을 바꾸지 않는 스냅숏도 쌓인다: 압축(`replace`), 행 0 개의 `append`(스키마 마이그레이션).
  유지보수는 7일이 지난 스냅숏을 지운다(`lakehouse-maintenance.contract.json`). 이런 것에 Gold 를 다시
  만들면 새 Gold 스냅숏 하나가 전국 굽기 며칠(ADR-0138: 필지 약 81시간)을 부른다.

## Decision

1. **등록 작업 하나가 두 표를 차례로 다시 만든다.** `orchestration/jobs.v1.json` 의 `gold_panel_rebuild`
   → `foundation-gold-panel-rebuild.service` → `scripts/ops/gold-panel-rebuild.sh all`. 필지, 그다음 건물
   (굽기와 같은 순서). 표마다 기존 생산자를 그대로 쓴다. 한 표가 실패해도 다른 표는 돌고, 단위는 실패한다.
   어떤 표를 누가 무엇으로 만드는지, 행 손실 허용치, Spark 크기는
   `infra/lakehouse/contracts/gold-panel-rebuild.contract.json` 한 곳에 있다. 입력 표 목록은 ADR-0130 대로
   생산자의 상수가 소유한다(계획기가 생산자 모듈에서 읽는다).
2. **Gold 스냅숏이 자기가 읽은 Silver 판을 적는다.** 생산자가 커밋할 때 ADR-0130 의 입력 판 매핑을 Iceberg
   스냅숏 요약 속성 `foundation.source-iceberg-snapshots` 에 남긴다(쓰기 옵션 `snapshot-property.*`; SQL
   `INSERT OVERWRITE` 대신 같은 의미의 `writeTo(...).overwrite(true)`). 실행 요약 파일은 서버 디스크에만 남지만,
   이 기록은 표 자체와 함께 간다.
3. **"새 Silver" 의 정의.** 계획기(`infra/lakehouse/spark/jobs/gold_rebuild.py`)는 Gold 의 `main` 조상을 따라
   기록이 있는 가장 새 스냅숏을 찾는다(백필이 만든 기록 없는 스냅숏은 건너뛴다). 입력마다 그 판 뒤의 `main`
   조상 스냅숏 중 행을 더하거나 지운 것(`added-records`·`deleted-records`·삭제 파일 > 0, `replace` 제외)이
   있으면 바뀐 것이다. 기록된 판이 만료로 지워졌으면 가장 오래 남은 스냅숏의 `parent_id` 가 그 판을 가리키므로
   거기서 멈춘다. 남은 기록으로 판까지 닿지 못하면 바뀐 것으로 본다. 기록이 아예 없는 Gold(이 결정 이전에
   만든 것)는 행의 `published_at_utc` 최댓값 — 생산자가 돈 시각 — 뒤에 커밋된 변경을 찾는다. 새로 생긴 선택
   입력(필지의 `silver.parcel_lineage`)은 변경이고, 없으면 생산자에 `--no-carry-lineage` 를 넘긴다.
4. **아무것도 바뀌지 않았으면 아무것도 하지 않는다.** 바뀌었으면 모든 입력을 현재 판으로 고정해 생산자를 돌린다.
5. **행을 잃은 Gold 는 쓰기 전에 거부한다.** 계획이 현재 Gold 의 `total-records` × (1 − `max_row_loss_fraction`)
   (지금 두 표 0.01)을 올림해 `--minimum-count` 로 넘기고, 생산자는 품질 검사 직후·쓰기 전에 그보다 적으면
   거부한다. 거부된 실행은 아무것도 커밋하지 않는다. 기존 품질 검사(열·필수값·PNU 형식·중복)도 그대로 쓰기 전이다.
6. **덮어쓰지 않고 쌓는다.** 커밋은 새 스냅숏 하나다. 이전 스냅숏은 표의 역사에 남고, 굽기가 서빙하는 세대는
   그 스냅숏을 계속 가리킨다(유지보수의 만료 정책은 그대로).
7. **풀과 메모리(ADR-0138).** `spark` 풀 3슬롯을 다 쓴다(혼자 돈다). 생산자는 compose `spark`(20g) 에서
   `local[8,8]`·driver 16g 로 돈다. 메모리 예산 계약의 `scheduled_jobs` 에 `spark` 로 적었고, 3슬롯이라 다른
   예약 작업과 겹치는 조합이 없으므로 합계는 60.94g / 62g 그대로다. `retries` 0, 시간 상한 165분(systemd `TimeoutStartSec` 은 그보다 짧은 160분): 매시 접기의
   굶김 상한이 1,035분 + 165분 = 1,200분 = 주기 20번으로 한도에 닿는다. 그보다 길게 잡으려면 이 표를 나누거나
   다른 작업의 상한을 줄이는 결정이 먼저다.
8. **굽기 앞에 선다.** 일정 08:45(굽기 10:15), `priority_weight` 3(FLOOR·계보 10, 접기 5, 굽기 1): 둘이 함께
   기다리면 Gold 가 먼저 시작한다. 굽기는 `takes_turns` 라서, 이 작업이 켜지면 Gold 작업이 한 번 시작한 뒤에야 다시
   시작한다. 그래서 경로는 Silver 변경 → Gold 작업(새 스냅숏) → 굽기(그 스냅숏을 새 세대로) → manifest 이동이다.
9. **켜는 것은 따로.** `enabled: false` 로 등록한다. 감독 아래 한 번 돌려 `gold.building_panel` 을 다시 만들고
   굽기가 그것을 구운 뒤에 배포로 켠다(런북 `gold-panel-rebuild.md`).

## 측정 (2026-10-03, ai-server scratch, 커밋 없음)

`building_panel_silver_to_gold.py --validate-only`(전 변환·전 검사, 쓰기 없음), 현재 Silver 판 다섯, 컨테이너
상한 20g(compose `spark` 와 같음), `local[8,8]`, driver 16g.

| 무엇 | 값 |
|---|---:|
| 걸린 시간 | 1,946초 (32분 26초) |
| 컨테이너 익명 메모리 최대 (`memory.stat` anon) | 19,333,795,840 B (18.0GiB) |
| 컨테이너 `memory.peak` (페이지 캐시 포함) | 21,476,700,160 B (= 20GiB 상한, OOM 없음) |
| 만든 행 | 5,268,380 (현재 Gold 5,939,794) |
| R2 읽기 일시 오류 | 1회(`Premature end of Content-Length`), 태스크 재시도로 통과 |

- 같은 조건의 `parcel_panel_silver_to_gold.py --validate-only --no-carry-lineage`(현재 Silver 판 8개):
  1,394초(23분 14초), 익명 메모리 최대 19,367,006,208 B(18.0GiB), `memory.peak` 20GiB(OOM 없음),
  39,861,511행(현재 Gold 와 같음, 중복 거부 없음).
- 상한·슬롯: 두 표 모두 anon 18.0GiB 로 compose `spark` 20g 안이고 driver 16g 가 그 대부분이다. 더 작은 Spark
  로는 들어가지 않으므로 3슬롯(혼자)이다. 두 표를 차례로 돌리면 약 56분으로, 시간 상한 160분(systemd)·165분
  (Airflow) 안이다. 상한은 위 7항의 굶김 한도가 정한다.
- **행 손실 게이트가 실제로 막을 것이다.** 현재 Silver 로 만든 건물 Gold 는 11.3% 적다. 원인은 Silver 다:
  `silver.building_register_titles` 의 09-27 덮어쓰기 판은 PNU 가 빈 행이 965,150 개(09-03 판은 37,386 개)라서
  PNU 수가 5,939,178 → 5,267,811 로 줄었다. 이 작업을 켜도 그 판으로는 건물 Gold 를 커밋하지 않는다(그래야 한다).
  표제부 PNU 회귀를 먼저 고쳐야 한다.
- 실시간 계획(같은 날, 읽기 전용): 필지 `nothing_to_do`, 건물 `rebuild`(표제부·층·호가 09-12 뒤 변경, 면적의 행 0 개
  `append` 는 변경 아님).

## 검토한 대안

- **Silver 변경 시각만 비교**: 판을 고르는 시점과 생산자 시작 사이에 들어온 커밋을 영영 놓칠 수 있고, 압축·만료를
  변경으로 읽는다. 기록이 없는 옛 Gold 에만 쓴다.
- **Gold 행의 논리 `source_snapshot_id`(수집 번호 해시) 비교**: ADR-0125 의 `unit_pnu` 백필은 수집 번호를
  바꾸지 않고 행 내용을 바꿨다. 이 방법은 오늘의 낡은 건물 Gold 를 "최신" 으로 판정한다.
- **Silver 적재 작업이 끝에서 Gold 를 부르기**: Silver 를 쓰는 곳이 FLOOR·수동 적재·마이그레이션 등 여럿이라
  같은 호출을 여러 곳에 복제해야 하고, 수동 적재는 예약 밖이다. 상태를 보고 판단하는 작업 하나가 낫다.
- **표마다 작업 하나**: 굶김 상한은 작업마다 한 번씩 더해지므로 둘로 나눠도 접기가 기다리는 시간은 같고, 단위·예산
  항목만 둘이 된다. 굽기와 같은 "한 단위가 차례로" 를 따른다.

## Consequences

- 이 작업이 켜지면 손으로 Gold 를 만들 일이 없다. 손으로 만든 Gold 에도 판 기록이 남는다(생산자가 남긴다).
- 생산자 코드가 바뀌어도(입력은 그대로) 이 작업은 다시 만들지 않는다. 그런 변경은 지금처럼 감독 실행으로
  다시 만든다(후속: 생산자 판 번호를 기록에 더하는 것).
- 측정은 `--validate-only` 다. 커밋(Iceberg 쓰기)이 더하는 시간은 감독 실행에서 잰다. 두 표 합이 160분을 넘으면
  Spark 크기나 작업 배치를 다시 정한다.
