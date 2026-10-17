# ADR 0272: 서빙되는 섹션은 어느 시도도 조용히 잃을 수 없다

- Status: Accepted
- Date: 2026-10-17
- Builds on: [ADR-0139](./0139-panel-gold-is-rebuilt-when-a-silver-input-changes.md) (행 하한 `max_row_loss_fraction`),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) (증분 병합),
  [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md) (종류 = 패널 섹션),
  [ADR-0145](./0145-one-source-of-truth-for-region-code-changes.md) (코드 변경표 하나)

## Context

2026-10-16 `gold.parcel_panel` 이 시도 29·46 의 섹션을 비운 채 만들어졌다(원인은 #660 이 고쳤다). 패널의
행 수는 그대로였으므로 행 하한(ADR-0139)은 아무것도 보지 못했다. 종류 표 하나만 거부됐는데, 종류 표의 행이 곧
그 섹션이라 행이 줄었기 때문이다. 섹션의 손실은 행의 손실이 아니라 **덮임(coverage)의 손실**이고, 지역 단위다.

2026-10-17 운영 Gold 를 Trino 로 읽어(읽기 전용) 잰 값:

| 스냅숏 | 행 | 시도 29·46 의 산림대장·이동연혁·토지권리 | 시도 28 산림대장 | 시도 28 토지권리 |
|---|---|---|---|---|
| 10-14 (그때 서빙) | 39,861,511 | 99.50·99.79% · 99.78·99.84% · 4,303·5,973 PNU | 99.87% | 35,241 |
| 10-16 오전 | 39,861,511 | **0 · 0 · 0** | **77.59%** | **27,543** |
| 오늘(고친 뒤) | 39,861,511 | 99.43·99.59% · 99.86·99.92% · 4,306·5,987 PNU | 98.94% | 34,787 |

10-14 와 오늘 사이 어느 시도의 어느 섹션도 1.3% 넘게 줄지 않았다(시도 28 토지권리 −1.3%). 늘어난 것은
있다: 10-14 판은 시도 41 이동연혁의 10% 를 빠뜨리고 있었고 오늘 판이 채웠다.

## Decision

1. **섹션마다·시도마다 덮임을 재고, 서빙 중인 스냅숏과 비교한다.** 서빙되는 섹션은
   `config/r2-connections.contract.json` 의 종류 줄(섹션 이름·내용 열)이 정하고, 그 종류의 호스트 패널과 종류 표
   자신이 대상이다(현재 `gold.parcel_panel` 과 여섯 종류). 패널은 섹션이 비어 있지 않은 PNU 의 **비율**을,
   종류 표는 행이 곧 그 PNU 이므로 그 **수**를 비교한다. 시도는 PNU 앞 두 자리이고, 표 전체도 같이 본다.
   비교 대상은 계획기의 직전 Gold head(`previous_gold_snapshot_id`), 즉 굽기가 서빙하는 판이다.
2. **한도는 계약 한 곳에 사유와 함께 한 번 적는다.** `infra/lakehouse/contracts/gold-panel-rebuild.contract.json`
   의 `section_coverage`: `max_share_drop_fraction` 0.05, `min_region_pnus` 1,000, `reason`(위 측정).
   `gold_rebuild.load_contract` 가 섹션을 가진 모든 표에 같은 값을 붙이고, 섹션을 가진 표가 있는데 블록이 없거나
   사유가 비면 계획을 거부한다. 서빙 판의 수가 `min_region_pnus` 미만인 시도(예: 토지권리 시도 36, 865)는
   표 전체 안에서만 판정한다 — 한 번의 갱신이 그 비율을 5% 넘게 흔들 수 있다.
3. **넘으면 쓰기 전에 거부한다.** 계획이 생산자에게 `--section-coverage-snapshot <직전 head>
   --max-section-coverage-drop --section-coverage-min-pnus` 를 넘기고, 생산자는 `gold_incremental.build_or_merge`
   안에서 행 하한 바로 뒤에 `section_coverage.Guard` 를 검사한다. 전체 재생성은 새 Gold 를, 증분 병합은 서빙
   판의 수에서 영향받은 PNU 의 옛 행을 빼고 다시 계산한 행을 더한 값을 잰다. 거부 메시지는 섹션과 시도마다
   "비율(수) → 비율(수)" 를 적고 `nothing was written` 으로 끝난다.
4. **개편은 계산에 넣지, 한도를 풀지 않는다.** `reference.legal_dong_code_change` 의 짝
   (`legal_dong_code_change_views.region_pairs`)이 잇는 시도 코드들은, 한쪽은 서빙 판에만 있고 다른 쪽이 새
   판에 있을 때(29·46 서빙, 12 생성 — 또는 그 반대) 한 지역으로 합쳐 비교한다. 코드만 옮긴 PNU 는 손실이
   아니고, 합친 지역에서 섹션을 비우면 거부된다. 양쪽 코드가 두 판 모두에 있으면 합치지 않는다(흐리지 않는다).

## Consequences

- 10-16 오전 판은 이 한도에서 10곳(시도 28·29·46 과 표 전체, 세 섹션)으로 거부된다. 10-14 → 오늘 판은 통과하고,
  오늘 판을 다시 만들면 통과한다(위 측정에 `section_coverage.breaches` 를 돌린 결과).
- 생산자는 실행마다 서빙 판을 한 번 더 읽어 시도별로 센다(필지 패널 약 4천만 행의 섹션 열 여섯 개).
- 판정 시험: `spark/tests/test_section_coverage.py`(한 시도의 섹션을 비우면 거부, 그대로면 통과, 코드 변경표로
  이름이 바뀐 시도는 통과, 바뀐 시도를 비우면 거부, 계약·계획 배선), `test_section_coverage_spark.py`(Spark 로
  잰 수와 전체 재생성의 거부).
