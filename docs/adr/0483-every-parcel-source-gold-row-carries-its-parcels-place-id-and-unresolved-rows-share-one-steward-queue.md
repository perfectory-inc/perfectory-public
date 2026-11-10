# ADR 0483: 모든 필지 원천의 Gold 행은 그 필지의 place_id 를 싣고, 못 놓인 행은 원천을 가리지 않고 하나의 스튜어드 큐에 선다

- Status: Accepted
- Date: 2026-11-11
- Builds on: [ADR-0288](./0288-every-parcel-source-converges-on-one-place-identity.md) §4·§5(신뢰도 등급·스튜어드 큐, place_id 가 조인 열쇠),
  [ADR-0405](./0405-a-masked-deal-links-to-its-main-lot-group-when-one-parcel-is-not-measured-enough.md)(가린 거래는 본번 묶음에),
  [ADR-0370](./0370-the-ownership-change-level-is-measured-and-a-level-is-judged-on-its-lower-bound.md)(수준은 하한으로 판정),
  [ADR-0329](./0329-official-evidence-supersedes-a-recorded-inference-automatically.md) §3(`views.changed_codes`),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)(Gold 는 부분 빌드),
  [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md) §10·ADR-0115(검토 큐와 스튜어드 API)
- 관련: ADR-0481(필지 place_id 발행), ADR-0482(서빙판의 코드 형태·공매 주소), ADR-0477(intelligence 는 place_id 로 받는다), ADR-0001

## Context

ADR-0288 §5 는 "place_id 가 모든 하위의 유일한 조인 열쇠" 라고 정했다. 2026-11-11 운영을 읽기 전용으로 쟀다.

| 측정 | 값 |
|---|---:|
| place_id 열을 가진 Gold 표 | 0 |
| `gold.place_id_registry` 의 필지(unit=parcel) id | 0 (읍면동 5,067 뿐) |
| `gold.parcel_public_auction` 행 / 그 PNU 가 `silver.parcel_registry` 현재 id 를 가진 행 | 4,078 / 4,078 |
| `gold.parcel_factories` 행 / 같은 기준 | 123,451 / 123,451 |
| `gold.parcel_court_auction`·`gold.parcel_real_transaction`·`gold.parcel_nearby` | 아직 운영에 없음(첫 빌드 전) |

- 필지 place_id 발행은 ADR-0481(#1245)이 연다. 그러나 발행만으로는 원천이 place_id 로 조인되지 않는다.
  원천의 Gold 행이 그 열을 가져야 한다.
- 이를 지키는 가드가 없었다. ADR-0288 §1 의 그래프 가드는 "필지 원천이 해석기를 거친다" 만 본다.
- 못 놓인 행의 스튜어드 큐는 원천마다 따로였다.
  - 필지 계보는 `gold.lineage_review_queue`(unit `parcel`, ADR-0113 §10)에 선다.
  - 짝 없는 폐지 동은 같은 표의 unit `legal_dong`(ADR-0263)에 선다.
  - 공매·경매·공장·실거래의 미해결 행은 Silver 의 `pnu_unresolved_reason` 에만 있다. 사람이 볼 줄이 없다.
- 실거래 가린 지번(W53b 계산, 2026-11-11):
  - 가린 행 12.81M 중 토지가 12.43M 이다.
  - 승인된 범주(공장창고·상업업무·단독다가구)는 약 27.8만 행이다. 그중 약 10.7만 행이 본번 묶음에 이어진다(ADR-0405).
  - 토지의 같은 본번 측정은 9,824 중 9,378(95.46%, Wilson 하한 95.03%)이다. 기준 99% 에 못 미친다.
    같은 필지면 같은 묶음이므로, 이 값은 묶음 정밀도의 하한이다.

## Decision

1. **필지 원천의 by-PNU 종류 Gold 는 조인 열을 싣는다.**
   - 종류 줄(`definitions/*.definition.json` `by_pnu_lanes`)이 `place_identity` 로 열을 이름한다.
     - `place_id`: 그 행 PNU 의 필지 id.
     - `main_lot_group_place_ids`: 본번 묶음으로 이은 거래가 있는 행에서, 묶음 필지들의 place_id(정렬한 JSON 배열).
       가린 실거래의 **묶음 수준 place_id** 다(ADR-0405).
   - Gold 계약(`by_pnu_kind.rs`)이 그 열을 `attached_via` 뒤에 둔다.
     서빙 문서에는 싣지 않는다(`PACK_LANE_JOIN_COLUMNS`). 조인 열쇠이지 화면 내용이 아니다.
     `row_digest` 는 그 열을 덮는다. 그래서 id 가 바뀐 행은 병합된다.
   - 지금 다섯 종류가 싣는다: 공매·경매·등록공장·실거래·주변.
2. **id 는 다리에서 읽는다(`place_id_join`).**
   - `gold.place_id_bridge` 의 필지 단위 행에서, 그 코드의 열린 기간의 id 를 쓴다. 없으면 마지막으로 닫힌 기간의 id 다.
   - 코드쌍 줄(`legal-dong-code-change`)은 읽지 않는다. 새 코드의 형태이지, 서빙 PNU 의 기간이 아니다.
   - 발행 전인 지역의 PNU 는 null 이다. 그 지역이 발행되면 채워진다.
3. **부분 빌드다(ADR-0312).**
   - 빌드는 짓는 PNU 의 다리 행만 받는다. `restrict` 가 다른 입력처럼 `pnu` 로 자른다.
   - 바뀐 id 의 PNU 만 다시 계산한다. 고정한 스냅숏 뒤에 `gold.place_id_changelog` 에 덧붙은 행의 옛·새 코드를 읽는다.
     이력은 발행마다 append 이므로 Iceberg 증분 append 스캔이다.
     그 사이에 append 아닌 커밋(압축)이 끼면, 두 다리 스냅숏의 id 를 비교한다.
   - 실거래는 그 PNU 의 법정동을 다시 짓는다. 묶음의 필지는 한 법정동 안에 있다(ADR-0405 §5).
   - 열이 생기는 첫 빌드는 선언한 전량 예외다. 계약 열이 바뀌어 증분이 스스로 물러난다. 첫 필지 발행(모든 id 가 `added`)도 같다.
4. **가드: 해석기를 거친 필지 원천이 들어가는 PNU 단위 Gold 는 place_id 를 싣고 다리를 읽는다.**
   - `scripts/catalog/pipeline_graph.py` `place_id_problems` 가 그래프와 계약 산출물로 거부한다.
     필지 원천의 정의는 ADR-0288 §1 가드와 같다(원천 간선이 `RESOLVER_VIA` 를 거침).
   - `pnu` 가 없는 Gold(지역 통계)는 이 규칙 밖이다.
   - 막는 사고(ADR-0001): 2026-11-11, place_id 열을 가진 Gold 가 하나도 없었다.
     그래서 개편을 가로지르는 조인이 PNU 로 떨어졌고, 아무도 알리지 않았다.
     시험이 열을 뺀 계약과 다리 간선을 뺀 그래프를 거부한다.
5. **못 놓인 행은 하나의 스튜어드 큐에 선다.**
   - 큐는 이미 있는 `gold.lineage_review_queue` 다. 새 unit `place_reference` 로 들어간다. 스튜어드 API 의 load 파일도 같다.
   - 항목 = (원천 표, `pnu_unresolved_reason`, 법정동 코드)다. 행마다가 아니다. 스튜어드가 정하는 것은 코드의 짝, 지번의 꼴이다.
     항목은 행 수와 표본 행 키를 싣는다.
   - 원천이 주지 않은 것(`jibun_masked` 등)은 `awaiting_data` 로 선다. 스튜어드가 승인할 수 없다.
   - 부분: 지난 큐 이후 Silver 스냅숏이 움직인 원천만, 그 원천의 항목만 다시 쓴다(MERGE).
   - 이 결정의 구현은 다음 PR 이다(§6 의 스위치와 별개).
6. **기준 아래 데이터셋의 묶음 연결은 계약 스위치 하나 뒤에 있다(실거래 `place_inference.below_bar`).**
   - `shown: false`(기본, 사장 결정 전): 지금과 같다. 토지의 가린 거래는 잇지 않는다(`jibun_masked`).
   - `shown: true`: `inferred_same_main_lot_group` 수준의 증거로 잇는다. 등급은 `inferred_same_main_lot_group_below_bar` 다.
     화면은 `label_ko`("추정 · 정확도 약 95%")를 쓴다.
   - `measured` 에 적힌 데이터셋만, `floor`(0.95) 이상, `min_measured_rows` 이상일 때다.
     기준을 넘는 데이터셋은 원래 등급을 지킨다.
   - 스위치를 켜면 그 데이터셋의 가린 거래가 모두 바뀐다. 실거래 Gold 는 한 번 전량 다시 짓는다(선언한 예외).

## Consequences

| 원천 | Gold 행 (운영, 실측) | place_id 있는 행: 전 (실측) | 후 (계산) |
|---|---:|---:|---:|
| 공매 | 4,078 | 0 | 4,078 (필지 발행 뒤; 모든 PNU 가 원장 현재 id 를 가짐, 실측) |
| 등록공장 | 123,451 | 0 | 123,451 (같음) |
| 경매·실거래·주변 | 첫 빌드 전 | — | 첫 빌드의 모든 행 |
| 실거래 묶음(승인 범주) | — | 0 | 묶음으로 이은 약 10.7만 행이 `main_lot_group_place_ids` 를 가짐 |
| 실거래 토지(가린 12.43M) | — | 0 | 스위치 꺼짐: 0 / 켜짐: 측정상 약 55.7% 가 이어짐 |

- 비용(계산): 종류마다 빌드가 필지 다리(약 3,986만 기간)를 두 열로 한 번 읽는다. 증분은 짓는 PNU 의 다리 행만 받는다.
  바뀐 id 는 변경 이력의 덧붙은 행만 읽는다. 첫 채움은 공매 4,078·공장 123,451 행의 bake 를 한 번 다시 돈다.
  문서 바이트는 같다(조인 열은 서빙하지 않음). 자체 서버, 과금 없음.
- 위험: 필지 발행(ADR-0481)이 배포되기 전에는 열이 null 이다. 발행되는 지역부터 채워진다.
- 선례: Overture GERS 는 모든 데이터셋 행이 GERS id 를 싣고 bridge 로 옛 id 를 잇는다.
  Kimball 의 conformed dimension 은 사실 표가 대리 키를 싣는다.
  미해결 해석을 원천을 가리지 않고 한 데이터 스튜어드 큐에 두는 것은 Informatica MDM·Reltio 의 match/merge 검토 큐와 같다.

## Rejected

- **읽을 때마다 PNU 로 다리를 조인하기:** 소비자마다 다르게 푼다. ADR-0288 의 "적재 시 한 번 확정" 과 어긋난다.
- **place_id 를 서빙 문서에 싣기:** 다섯 종류의 문서 스키마와 레지스트리 개정이 필요하다. 화면은 아직 읽지 않는다.
- **원천마다 따로 큐:** ADR-0288 §4 가 통일을 요구한다. 콘솔이 여러 줄을 따로 봐야 한다.
- **토지를 기준 99% 아래로 그냥 켜기:** 사장 승인(ADR-0405 §3)이 99% 다. 스위치로 준비만 하고 결정을 기다린다.
