# ADR 0182: Gold 재생성이 세대 공시가격을 전국 한 묶음으로 조합하고, 건물 패널은 가장 새 판의 묶음을 읽는다

- Status: Accepted
- Date: 2026-10-10
- Builds on: [ADR-0095](./0095-unit-official-prices-are-reindexed-through-the-exclusive-register.md)(세대 공시가격 재색인),
  [ADR-0101](./0101-unit-prices-preserve-every-reference-date.md)(모든 기준일 보존),
  [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md)(Gold 재생성),
  [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)(Silver 레인),
  [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(데이터 기반 연결)

## Context

공동주택가격·전유부 Silver 레인(ADR-0169)은 새 월을 스스로 싣는다. 그러나 건물 패널이 읽는 `silver.unit_official_price`
는 그 둘을 조합한 표이고, 조합(`unit_official_price.py`)은 시도마다 사람이 돌렸다(런북 silver-refresh.md). 소스부터
서비스까지의 경로에서 사람 손이 남은 자리였다. 2026-10-10 운영 표의 상태: 한 묶음(202608), 1억 8,874만 행, 16개 시도,
기준일 2008–2026. 한 판이 기준일 전체 이력을 싣는다.

이 조합을 그대로 예약 작업으로 옮길 수는 없었다.

1. **표는 쌓기만 한다**(`append_only`). 새 월을 붙이면 묶음이 둘이 된다. 건물 패널 생산자는 그 표에서
   `source_snapshot_id` 가 정확히 하나여야 하고(`assert_single_snapshot`), 예약 Gold 는 묶음을 지정하지 않는다. 새 월을
   붙인 다음 날 Gold 재생성이 거부된다.
2. **시도마다 한 묶음씩 쌓는다.** 중간에 실패하면 새 월이 몇 개 시도만 가진 채 남고, "가장 새 묶음"은 불완전하다.
3. **같은 월이 두 번 쌓일 수 있다.** 묶음 이름에 두 Silver 표의 스냅숏 id 가 들어 있다. 압축이나 재적재로 스냅숏 id
   가 바뀌면 같은 월이 새 묶음으로 다시 붙는다.
4. **spark 풀에 새 작업 자리가 없다.** 슬롯 셋을 다 쥐는 작업을 하나 더 넣으면 시간 단위 fold 의 굶김 상한
   (ADR-0138, 1,200분 중 1,200분)을 넘는다.

## Decision

1. **조합은 Gold 재생성이 건물 패널 앞에서 한다.** `gold-panel-rebuild.sh` 의 건물 단계는 계획 전에
   `unit_official_price.py` 를 돌린다. 그 표를 읽는 것은 이 작업뿐이고, 이미 spark 풀의 슬롯을 쥔 작업이므로 굶김 상한은
   그대로다. dry-run 은 쓰지 않으므로 건너뛴다. 조합이 실패하면 건물 패널은 그 실행에서 만들지 않는다(실패 알림).
   작업 목록(`jobs.v1.json` gold_panel_rebuild)은 두 가격 표 → `silver.unit_official_price` 간선을 함께 수행한다고
   적는다. 그래서 두 가격 레인의 출력이 Gold 재생성을 시작시킨다(ADR-0171).
2. **한 번에 전국, 한 월에 한 묶음.** 조합은 두 표가 함께 가진 가장 새 월을 전국 한 번에 조합해 한 번의 append 로
   쓴다. 시도는 행의 PNU 에서 나온다. 표가 그 월의 묶음을 이미 가지면 아무것도 쓰지 않는다(스냅숏 id 와 무관). 한 표만
   새 월이면 둘이 함께 가진 월을 쓰고(대개 이미 있음), 느린 레인이 따라오면 다음 실행이 새 월을 조합한다. 다른 월끼리
   섞지 않는다. 시도 하나만 조합하는 경로(`--sido`)는 없앤다.
3. **패널은 가장 새 판의 묶음을 읽는다.** 생산자는 고정된 스냅숏 안에서 `newest_batch`(조합 모듈의 한 함수)로
   가장 새 월의 묶음만 읽는다. 조합이 쓰지 않은 id 가 섞여 있으면 고르지 않고 지금처럼 거부한다. 같은 월의 묶음이 둘이면
   이름을 대고 거부한다. 운영자가 묶음을 지정하면(`--price-source-snapshot-id`) 그것이 이긴다.
4. **자기가 쓴 것으로 자기를 시작하지 않는다.** 입력으로 시작하는 작업의 시작 조건은 그 작업의 입력 중 자기가 쓰지
   않는 것이다(`JobSpec.starting_inputs`). "자기 입력을 쓰는 작업은 거부" 규칙을 이 구조로 대신한다.

## Consequences

- 원천 → 가격 Silver 레인 → (같은 Gold 실행 안의) 조합 → 건물 패널 → 굽기 → 서비스가 사람 손 없이 이어진다.
- 표는 월마다 한 묶음(약 1.9억 행)씩 자란다. 옛 묶음은 그 달에 서빙한 것의 기록으로 남는다(append-only).
- 전국 조합은 시도별보다 큰 shuffle 이다(shuffle partition 400). 첫 실행의 시간과 Spark 메모리를 재서 Gold 재생성의
  시간 상한(165분) 안인지 확인한다.
- 시험: 조합 함수(`test_unit_official_price`), 패널의 묶음 선택(`test_unit_price_history_spark`), 작업 순서와 실패
  (`test_gold_panel_rebuild`), 시작 조건(`test_job_specs`, `test_job_dags`).
