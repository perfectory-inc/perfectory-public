# ADR 0497: 필지 화면의 "인입 가능성" 은 공개 기록 신호를 모으고, 하수 기록은 자기 by-PNU 종류 parcel-utility 다

- Status: Accepted
- Date: 2026-11-11
- Builds on: ADR-0496(오수정화시설 Silver), ADR-0398(건물 에너지 종류), ADR-0450(총괄표제부 종류 — 같은 틀),
  ADR-0287 §9(자기 생산자), ADR-0312 §2(바뀐 키만), ADR-0329 §3, ADR-0336(스키마 레지스트리), ADR-0435(자동 승격),
  ADR-0465 §5(지역 단위 값은 필지 문서에 넣지 않는다)

## Context

공장·창고 매수인과 임차인이 필지에서 묻는 것 하나가 "상하수도·가스·전기·도로를 끌어올 수 있나" 다. 배관·선로 도형은
공개제한 정보라 받을 수 없다. W63 조사(`platforms/foundation-platform/docs/catalog/parcel-utility-connectivity-sources.md`)는
공개 기록에서 필지 단위 신호 셋을 골랐다: 도시가스(건물 가스 사용 행), 하수(건축물대장 오수정화시설), 도로(토지특성 도로접면).

**측정(2026-11-11, 운영 Trino·Bronze 읽기 전용).**

| 신호 | 이미 서빙되는 곳 | PNU |
|---|---|---:|
| 도시가스: 가스 사용 행이 있음 | `parcel-energy` 문서(ADR-0398) | 396,237 |
| 도로접면 값 | 필지 문서 `characteristics.road_contact` | 35,784,044 |
| 오수정화방식이 채워진 지번 | 없음(ADR-0496 Silver) | 2,826,917 → 서빙 약 251만(연결률 88.9% 적용, 계산) |

가스 Silver 는 202602–202606 다섯 달뿐이라 에너지 문서의 12개월 창과 Silver 전체가 같은 답을 준다.
도로접면은 필지 문서에 이미 있다. 둘을 새 Gold 에 다시 실으면 같은 사실의 두 번째 사본이다.
총괄표제부 종류(ADR-0450)는 문서가 약 62만 필지뿐이라 일반건축물의 시설(583만 행)을 실을 수 없다.

### 실사례

- 미국 매물 정보망(MLS) 필드(RESO Data Dictionary 의 `Sewer`·`WaterSource`·`Utilities`)와 그것을 싣는 Zillow·Redfin 은
  "Public Sewer / Septic Tank", "Natural Gas Available" 을 기록의 낱말로 보이고, 공급 확인은 매수인 몫이라는 단서를 붙인다.
  배관 도형이 아니라 기록이 근거다.
- RESO 는 신호마다 따로 칸을 두고 한 화면 묶음("Utilities")으로 보인다. 우리도 원천마다 그 원천의 문서에 두고 화면이 모은다.

## Decision

1. **화면 구획 하나, 신호마다 그 원천의 문서.** 필지 패널 필지정보에 "인입 가능성" 구획을 둔다(`lib/api/utility.ts` 가 모은다).
   - 도시가스: 에너지 문서에서 가스 행이 있는 가장 새 달 → "사용 실적 있음 · YYYY.MM 사용분". 사용량 0 인 행도 계량기가 있다는 뜻이라 센다.
   - 오수정화방식: 이 ADR 의 종류에서, 시설이 많은 방식부터 원천 낱말 셋까지(하수종말처리장연결·부패탱크방법 …).
   - 도로접면: 필지 문서의 값 그대로.
   - 신호가 없는 칸은 보이지 않는다. "없음" 이 아니다(가스는 제공처가 주택·산업용을 빼고, 오수정화방식은 절반이 빈칸이다).
   - 출처 문구는 두지 않는다. 공개 기록의 신호이고 실제 인입은 공급 기관 신청 뒤 정해진다는 단서 한 줄만 둔다.
2. **종류 한 줄.** `definitions/parcel-utility.definition.json` `by_pnu_lanes.parcel-utility`: 호스트 필지, 종류·섹션 `utility`,
   열 `utility_json`, 생산자 `parcel_utility_silver_to_gold`. Gold `gold.parcel_utility` 와 경로 `/parcels/utility/by-pnu/` 는
   그 한 줄에서 유도된다(ADR-0193).
3. **문서(`utility.sewage`).** 오수정화방식코드가 채워진 현재 행만 센다(빈 행은 아무것도 말하지 않는다).
   - `methods`: 방식마다 코드·원천 이름·시설 수, 많은 것부터 최대 20.
   - `facility_total`: 센 시설 수.
   - `treatment_plant_connection`: 방식 300 하수종말처리장연결이나 400 마을하수처리장연결이 있으면 참. AI·조인을 위한 계산 칸이고,
     화면은 판정 대신 원천 낱말을 보인다.
4. **서빙 필지판의 PNU 만, 바뀐 필지만.** Silver 의 키는 행 SHA-256 이라, 고정한 Silver 와 지금 Silver 의 현재 행이 다른 PNU(양방향)와
   필지판에 들고 난 PNU 만 다시 계산한다(ADR-0312 §2). 개편으로 바뀐 코드의 행은 `renumbered`(`views.pair_rows`, ADR-0329 §3)로
   서빙 코드에 읽고, 코드 변경표가 바뀌면 통째로 짓는다(ADR-0450 과 같은 규칙과 이유).
5. **작업.** `gold_parcel_utility_rebuild` 와 `by_pnu_parcel_utility_bake` 는 총괄표제부 종류와 같은 단위·풀·메모리다. 둘 다
   켜진 채 들어가 첫 run 추정(각 900초·1,500초, 계산)을 적고, 자산 조정기(ADR-0333)가 맡는다. 첫 발행은 자동 승격(ADR-0435)이다.
6. **스키마는 레지스트리에 있다(ADR-0336).** 주제 `foundation-platform.parcel_utility_by_pnu` 판 1 리비전 1, `FULL_TRANSITIVE`.
   소비자는 `pack_lane_document` 와 웹(`utility.generated.ts`)이다. Spark 시험(`test_parcel_utility_gold.py`)이 쓰인 JSON 경로를 레지스트리와 같게 묶는다.
7. **지역 단위 신호는 이 문서에 넣지 않는다.** 법정동 안 이웃 필지의 가스 사용 비율, 한전 분산전원연계정보의 변전소·배전선로 여유
   용량은 법정동·시군구가 단위라, 필지 문서에 넣으면 값 하나가 바뀔 때 그 지역의 모든 필지 문서가 바뀐다(ADR-0465 §5 와 같다).
   지역 단위 문서로 따로 서빙할 일이고 이 ADR 은 정하지 않는다.

## Consequences

- **비용(계산).** Gold 약 251만 행, 문서 하나 약 150 바이트(JSON) → 원문 약 0.4 GB, 묶음 압축 뒤 더 작다. R2 저장 월 10원 아래.
  한 달 변화는 Silver 실측 새 행 22,165·사라진 행 11,289 라 그 PNU 의 패치뿐이다.
- **위험.** 정화조(2xx)는 합류식 하수도 지역에서 하수처리구역 안에도 있다(하수도법 제34조). 그래서 판정 대신 방식 이름을 보인다.
  산업용 가스는 원천에 없어 공장 필지 자신은 가스 신호가 비기 쉽다 — 이웃 비율(§7)이 그 빈칸을 채울 몫이다.
- 실패가 막는 사고: 빈 오수정화방식을 "없음" 으로 보이면 하수도가 들어온 필지를 미인입처럼 보인다.
  `test_methods_most_first_ties_once_silence_and_unplaced_left_out` 과 웹 시험 "shows no 인입 가능성 …" 이 지킨다.
