# ADR 0458: 작업은 달력의 날 수가 아니라 증거로 Dagster 로 옮긴다 — 자기 결정 N개가 같고, 자기가 하는 모든 종류의 결정이 한 번씩 같을 때

- Status: Accepted
- Date: 2026-11-09
- Supersedes: [ADR-0431](./0431-scheduled-jobs-move-from-airflow-to-dagster-one-family-at-a-time-after-a-dual-run-in-which-both-decide.md) §4
  (옮기는 문턱 "연속 7일 매일 통과 + 정지 구간 결정을 사람이 확인"). 나머지(단계 필드, 이중 실행의 비교 규칙, 하류 먼저, 가족과 순서)는 그대로다.
- Builds on: ADR-0431 §2(이중 실행)·개정 각주(증거 원장 `comparisons.jsonl`), ADR-0435(정상 경로는 승인을 기다리지 않는다)

## Context

ADR-0431 §4 는 가족마다 "연속 7일" 을 문턱으로 정했다. 잰 값으로 보면 그 문턱은 증거의 양이 아니라 달력을 센다.

| 무엇 | 잰 값 (2026-11-09, 운영 읽기 전용) |
|---|---|
| 이중 실행에 든 작업 | 64개(가족 1–5 를 넣은 PR 의 54개 + 그 뒤 `shadow` 로 태어난 원천 Silver 10개) |
| 그 작업들의 시계 주기 | 하루보다 짧은 주기(매시·30분·5분) 8개, 매일 56개; 매주·매달 0개. 입력으로 시작하는 38개는 매일 fallback 틱도 있다 |
| 증거 원장 `/var/lib/dagster/dual-run/comparisons.jsonl` | **없음 — 비교 0줄.** 코드 위치가 2026-11-06 21:16Z 부터 모든 로드에서 실패했다(`No module named 'publish_checks'`, 데몬 로그 6,222번). 배포는 그동안 "observing" 이라 했다(#1204 가 고침) |

- **7일은 작업마다 다른 양이다.** 5분 주기 작업은 하루에 288번 결정하고, 매일 작업은 한 번 결정한다. 같은 7일이 2,016개의 결정과 7개의
  결정을 같은 증거로 친다. 반대로 매주·매달 작업이 생기면 7일 안에 그 종류의 결정이 하나도 없을 수 있다.
- **사람의 확인은 문턱이 아니다.** 정지·일시정지 구간의 결정은 비교가 이미 따로 남긴다(`airflow_out`·`paused`): 결정 규칙이 아니라 가용성이다.
  사람이 확인해야 옮기는 문턱은 사람이 없으면 영원히 서 있다(소유자 11-07: 사람 단계·고정 시간창 금지).
- **원장의 한 줄은 하루 창이다.** 검사는 Airflow run 마다 지난 하루를 비교하므로, 한 결정이 여러 줄에 거듭 나온다. 줄의 `decisions` 를
  더하면 같은 결정을 여러 번 센다. 같은 결정을 한 번 세려면 줄이 같은 결정을 이름으로 남겨야 한다.

대기업·오픈소스가 섀도에서 옮기는 문턱:
- **GitHub Scientist**: 실험은 "일치한 관측 수"와 불일치 0 을 보고, 불일치 하나는 조사 대상이다(시간이 아니라 관측).
- **Google SRE 의 카나리 판정(Kayenta, Spinnaker)**: 기간이 아니라 표본으로 판정하고, 실패한 판정은 처음부터 다시 한다.
- **Stripe·Shopify 의 섀도 읽기**: 코드 경로마다(여기서는 결정의 종류마다) 한 번 이상 일치를 본 뒤에 바꾼다.

## Decision

1. **문턱은 작업마다 증거다.** 작업은 원장이 그 작업의 **서로 다른 일치 결정 `GATE_AGREEING`(=3)개**를 마지막 차이 뒤로 가지고, 그 작업이
   하는 **모든 종류의 결정이 그 안에 하나 이상** 있을 때 `shadow` 를 지난다(`orchestration/dagster/dual_run.py gate`).
   - 결정의 종류(`decision_kinds`): 시계 틱은 그 cron 의 주기(`monthly` — 날짜를 정한 cron, `weekly` — 요일을 정한 cron, 그 밖은 `daily`;
     매시 틱도 하루 안의 결정이다), 입력이 시작하는 작업은 `event`. 입력으로 시작하면서 매일 fallback 이 있는 작업은 `daily`·`event` 둘 다다.
   - 일치 결정은 둘 다 한 결정이다: 같은 시계 틱, 또는 입력 사건 뒤의 같은 `asset_triggered` run. 원장의 줄이 그것을 `agreed` 로 이름(종류,
     틱 또는 run 시작)째 남기고, 문턱은 이름으로 중복을 지운다.
   - 정지·일시정지 구간의 결정(`airflow_out`·`paused`)은 어느 쪽으로도 세지 않고 보고만 한다(`apart`). 사람의 확인은 문턱이 아니다.
2. **가족은 그 작업이 모두 통과할 때 옮긴다.** 가족의 작업 목록은 ADR-0431 §7 의 표와 각 가족을 `shadow` 로 넣은 PR 이다(아래 표).
   `gate --family 이름=작업,작업` 이 그 가족의 통과를 한 줄로 말한다. 단계의 정본은 여전히 작업의 `dagster_stage` 다.
3. **차이 하나는 그 작업의 셈을 처음으로 돌리고 호출한다.** 차이가 난 비교의 가장 늦은 차이 결정까지는 증거가 아니다(뒤의 창이 같다고
   하는 결정도). 검사는 그 자리에서 Alertmanager 에 `FoundationDualRunDisagreed`(service `dual-run`, 작업마다 하나, 마지막 차이 뒤 26시간
   유지)를 보내고, 경로는 작업마다 처음 한 번과 하루 한 번 다시 알린다(`infra/observability/alertmanager/alertmanager.yml`). 보내지 못한
   호출은 검사의 metadata `paged: false` 로 남고, 비교는 이미 원장에 있다. Dagster 데몬은 그래서 runtime 프로젝트의 네트워크에 붙는다.
4. **`proxied` → `dagster` 도 같은 규칙이다.** `proxied` 에서도 Airflow 가 결정하므로 비교는 계속된다. 셈은 그 작업이 `proxied` 로 간
   릴리스의 줄부터 다시 하고, 센 결정의 run 은 Dagster 의 실행기(ADR-0417)로 성공해야 한다(`foundation-job-outcome`). 되돌리기는 ADR-0431 §4
   그대로 그 작업의 `dagster_stage` 를 한 단계 내리는 PR 하나다.
5. **이행이 끝나면 함께 지운다.** 이 문턱은 한 번의 이행을 위한 것이라 원장 밖에 영구 장치를 두지 않는다: 셈은 원장을 읽는 CLI 이고 호출은
   이미 있는 Alertmanager 다. 마지막 작업이 `dagster` 로 가면 `dual_run.py`·그 검사·`dual-run` 경로를 Airflow DAG 생성·배포 단계·유닛과 함께
   지운다(ADR-0431 §7 의 5단계). 원장 파일은 추가 전용 기록으로 남는다.

가족과 그 작업(이중 실행에 넣은 PR 의 정의 변경에서 센 값):

| 가족 | PR | 작업 수 | 작업 |
|---|---|---|---|
| 1 단가 | #1111 | 2 | `gold_unit_price_rebuild`, `by_pnu_unit_price_bake` |
| 2 공유 독자 | #1117 | 5 | `data_quality`, `shape_ledger_check`, `asset_reconcile`, `media_reconcile`, `lakehouse_maintenance` |
| 3 원천 수집·Silver | #1118 (+그 뒤 `shadow` 로 태어난 10) | 34 + 10 | 수집기·Silver 갱신(공매·경매·실거래·건축물대장·건축인허가·토지·VWorld·법정동 변경 …) |
| 4 Gold·by-PNU 레인 | #1119 | 5 | `gold_panel_rebuild`, `gold_kind_rebuild`, `gold_public_auction_rebuild`, `by_pnu_serving_bake`, `by_pnu_public_auction_bake` |
| 5 나머지 | #1120 | 8 | 경계 2·지도 편집 접기 2·`outbox_publish`·`lineage_stewardship`·`zoning_tile_bake`·`unit_price_versions_migration` |

## Consequences

- 문턱에 닿는 데 드는 시간은 작업의 주기가 정한다: 매시·분 주기 작업은 몇 시간, 매일 작업은 사흘, 입력으로 시작하는 작업은 그 입력이 한 번
  바뀌고 사흘. 매주·매달 작업이 생기면 그 주기의 틱 한 번을 기다린다. 2026-11-09 의 원장은 비어 있으므로(#1204 전에는 증거가 남지 않았다)
  모든 가족이 0 에서 시작한다.
- 문턱을 지난 뒤의 이동은 `proxied` 단계의 구현(ADR-0431 §5: Airflow 이미지의 `dagster-airlift[in-airflow]`, 프록시 과제)이 있어야 한다.
  그 전까지 `job_specs.py` 는 `proxied` 를 거부하고, 이 문턱은 어느 작업이 준비됐는지를 말한다.
- 비용: 원장 줄마다 `agreed` 목록(하루 창의 결정 수, 5분 주기 작업이 가장 긴 288개 ≈ 13 KB)이 더해진다. 64개 작업의 하루 원장은 수 MB 다.
  Alertmanager 호출은 차이가 날 때만 한 번이다. 0원.
- 시험(CI `dagster-observe`): 창이 겹쳐도 결정을 한 번 세는지, 차이 뒤로만 세는지(뒤 창이 같다고 한 옛 결정 포함), 종류가 빠지면 통과하지 않는지,
  cron 의 주기, 호출의 모양과 Alertmanager 가 없을 때 예외 없이 `False`.

## 기각한 대안

- **연속 N일(ADR-0431 §4 그대로)**: 위 표 — 날은 작업마다 다른 양이고, 사람의 확인에 묶인다.
- **원장 줄의 `decisions` 를 더하기**: 하루 창이 겹쳐 같은 결정을 여러 번 센다.
- **가족 필드를 작업 목록에 더하기**: 가족은 한 번의 이행 계획이고 단계는 작업마다 적혀 있다(ADR-0431 §7). 이행이 끝나면 쓸모가 없는 필드를
  정본에 두지 않는다.
- **Dagster 의 검사를 ERROR 로 바꿔 호출을 대신하기**: Dagster 의 검사 결과는 사람에게 가지 않는다. 운영의 호출 경로는 Alertmanager 하나다.

## 개정 각주 — 2026-11-14, 가족 2 에서 자산 조정자를 뗀다

[ADR-0565](./0565-a-job-dagster-decides-is-reached-through-dagster-by-everything-that-reached-it-through-its-airflow-dag.md) §4 에 따라
`asset_reconcile` 은 가족 2 와 함께 `dagster` 로 가지 않는다: 그것이 시작하는 작업(spark·documents 풀)이 `dagster` 인 동안 `proxied` 에 남고, 그
마지막 작업 뒤에 간다. 가족 2 의 나머지 넷은 §2 그대로 함께 옮긴다. 문턱(§1)은 바뀌지 않는다.
