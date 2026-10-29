# ADR 0346: 델타 상한을 넘는 변경 집합은 기록이 설명하면 전량으로 굽고, 설명이 없을 때만 거부한다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md) §3("변경이 전체의 절반을 넘으면 거부한다"),
  [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md) §2(`max_delta_fraction` 0.5 는 그대로)
- Builds on: [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md) §5(지문 표류·잘못된 기준의 경보),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md) §1(바뀐 키 기록),
  [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md)(코드 판 기록), [ADR-0320](./0320-a-document-schema-bump-bakes-only-the-documents-it-changes.md)(문서 판),
  [ADR-0310](./0310-a-long-bake-finishes-at-its-pin-then-patches-forward.md)(핀에서 끝내고 앞으로 패치)

## Context

**2026-10-29 두 레인이 같은 이유로 멈췄다**(운영 journal, 배포 02528d1 뒤):

| 레인 | 반영 → 새 Gold | 다른 PNU / 새 표 | 기록 | 결과 |
|---|---|---:|---|---|
| building | 7009263748812461350 → 6612081582327553001 | 5,946,006 / 5,945,309 | 두 커밋 모두 통째 덮어쓰기, `__changes`·`foundation.keys-changed`·`foundation.build-source-sha256` 없음 | `not_a_delta` 거부, 발행 0 |
| parcel-forest-ledger | 3701134578441783806 → 1029537232158793275 | 39,715,931 / 39,703,956 | 문서 판 v1 → v2(ADR-0320 의 확장) | `not_a_delta` 거부, 발행 0 |

building 이 멈추자 그 문서를 따르는 unit-price 레인도 "따르는 building 레인이 Gold 6311420005557193586 의 입력을 서빙하지
않는다"로 기다렸다(ADR-0333 §7).

**building 의 99.8% 는 원천 파일 줄 번호였다**(운영 Trino 읽기 전용, 두 스냅숏을 `pnu` 로 맞대어):

- 바뀜 5,933,490 · 새 6,029 · 삭제 6,487 · 같음 5,790.
- 시도 41(952,543 PNU 의 `row_digest` 다름)에서 문서 안의 `floor_row_id`·`units[].building_link_evidence.unit_row_id`·
  `building_link_source_record_id`·`building_link_input_sha256` 네 필드를 지우고 비교하면 다른 PNU 는 **1,628**(0.17%)이다.
- 그 네 값은 `bronze/source=hubgokr__…/OPN<배치>.zip#line-N` — 원천 파일 이름과 줄 번호다. 10-20 판(`OPN20260920…`)에서
  10-28 판(`OPN20261020…`)으로 다섯 Silver 입력이 모두 새 배치가 되어 모든 문서가 바뀌었다.
- 층 행 id 는 [ADR-0331](./0331-building-floor-and-unit-area-rows-are-named-by-their-natural-key-not-their-file-line.md)(#932, 10-29 05:40Z 병합)이
  자연 키로 고쳤다. 6612 는 그 전(10-28 19:59Z)에 만들어졌다. 호(`unit_row_id`)와 연결 근거 두 필드는 아직 줄 번호이고
  건물 문서의 **5.95%**(353,978 / 5,945,309, 6612 측정)가 그것을 싣는다. 그래서 다음 달부터는 이 몫만 매달 바뀐다(상한 아래).

즉 building 거부는 **경보가 맞게 울린 것이다**: 같은 코드·같은 문서 판에서 내용이 거의 그대로인데 지문이 전부 바뀌었다 —
ADR-0099 §5 가 말한 "지문 정의 표류"(계보가 지문에 들어감)다. 반면 forest-ledger 는 기록된 문서 판 변경이 낳은 진짜 변경인데
같은 문으로 막혔다.

**상한 0.5 의 근거가 바뀌었다.** 건물 레인 전량 굽기는 19분 20초다(10-29 07:17:52 → 07:37:12, 5,945,767 문서, 측정). ADR-0274 §2 의
손익분기는 기본 세대의 29–52% 이고 계약의 `max_cumulative_change_ratio` 는 0.25 다. 0.5 를 넘는 변경은 언제나 손익분기 위,
곧 전량이 더 싸다. 그러니 0.5 를 넘는 변경에서 남은 질문은 비용이 아니라 **믿어도 되는가** 하나다.

### 실사례

- **Netflix Iceberg WAP · Great Expectations/dbt 볼륨 이상 검사**: 크기 이상만으로 멈추지 않고, 원인이 기록된 변경(배포·스키마
  이전·의도된 백필)이면 통과시키고 원인 없는 이상만 막고 알린다.
- **Dagster 코드 판·데이터 판, Bazel action key**: 출력이 바뀐 이유를 입력·코드 판의 기록으로 설명한다. 기록이 같은데 출력이
  다르면 비결정성(또는 손상)이다.
- **Google SRE "경보는 행동을 요구할 때만"**: 사람이 할 일이 정해져 있는 경보(매번 "전량 강제")는 자동화한다.

## Decision

1. **상한을 넘는 변경 집합은 설명을 찾는다.** `by_pnu_panel_delta.py` 가 `max_delta_fraction` 을 넘는 변경을 셀 때, 계약
   `by_pnu_serving_patches.over_max_delta_fraction.explained_by` 가 받는 기록만 본다(크기·모양은 설명이 아니다):
   - `recorded_change_set`: 두 스냅숏 사이 모든 Gold 커밋이 바뀐 키를 기록했다(ADR-0312 `gold.<t>__changes`; 델타의
     `change_source recorded`). 생산자가 자기 비교로 그 키가 바뀌었다고 적은 것이다.
   - `code_version`: 두 스냅숏이 각자 최신 핀 옆에 기록한 생산자 코드 판(`foundation.build-source-sha256`, ADR-0321)이
     **둘 다 있고** 다르다. 한쪽이 기록이 없으면(ADR-0321 이전) 아무것도 말하지 않는다.
   - `document_schema`: 굽기가 이 실행에서 문서 판을 바꿔 다시 그린다고 말한다(`--document-schema-revision
     "<서빙 판> -> <굽는 판>"`, ADR-0320 의 `expands`). 깨는 판(`breaks`)은 이미 변경 집합 없이 전량이다.
2. **설명되면 종료 6, 굽기는 전량이다.** 이유는 "max_delta_fraction 초과, 설명: <기록들>; 손익분기 위라 전량이 싸다"이고
   실행 요약 `explained_by` 에 남는다. 새 세대이고 ADR-0310 의 핀·이어 하기를 그대로 탄다(같은 스냅숏의 반쯤 구운 전량
   세대는 이어서 끝낸다).
3. **설명이 없으면 종료 4, 거부한다.** 발행 0, 레인 실패(기존 `OnFailure`·운영 지도 경보). 로그와 실행 요약이 증거를 단다:
   바뀜·새·삭제·같음 수와 "확인한 기록: recorded_change_set, code_version, document_schema — 하나도 없음", 그리고 Gold 를
   확인한 뒤의 조치(`FOUNDATION_BY_PNU_BAKE_FORCE_FULL` + 이유)다. 빈 현재 표는 여전히 설명할 수 없는 거부다.
4. **규칙은 계약에 있다.** `config/r2-connections.contract.json` `by_pnu_serving_patches.over_max_delta_fraction`
   (`decision`·`explained_by`·`reason`). 모르는 설명 이름이나 빈 이유는 델타 작업이 거부한다. `max_delta_fraction` 0.5 는
   그대로다 — 이제 비용 문턱이 아니라 "설명이 필요한 크기" 문턱이다.
5. **시험이 이 사고를 이름으로 지킨다.** `test_by_pnu_panel_delta.py` `LargeChange`(building 10-29 는 거부로 남고,
   forest-ledger 10-29 의 판 변경·기록된 키·양쪽에 기록된 다른 코드 판은 설명), `test_by_pnu_serving_bake.py`(거부는 증거를 말하고
   발행 0, 설명된 변경은 전량 새 세대, 판 확장 레인은 굽기가 판 변경을 델타에 넘기고 전량).

## Consequences

| 항목 | 전 | 뒤 | 근거 |
|---|---|---|---|
| forest-ledger v1 → v2(39.7M 의 99.99%) | 거부, 사람이 전량 강제 | 자동 전량(10-28 에 반쯤 구운 g3 을 이어 끝냄) | 설계(§2), 시험 |
| 생산자 코드 변경의 대량 변경 | 거부 | 자동 전량 | §1 `code_version` / `recorded_change_set` |
| building 10-29(기록 없는 통째 덮어쓰기 두 번) | 거부, 증거 없음 | 거부, 수·확인한 기록·조치를 말함 | §3 |
| 거부 하나를 푸는 사람 일 | 원인 조사 + 전량 강제 | 설명 없는 경우만 | — |

- 남은 일(이 ADR 밖): 호의 `unit_row_id` 와 연결 근거 두 필드도 자연 키로(ADR-0331 의 층과 같은 길). 그 전까지 매달 건물 문서
  약 6% 가 줄 번호만으로 다시 구워진다(상한 아래라 패치). 층 자연 키가 처음 실리는 Gold 재구축은 ADR-0331 대로 모든 층 행이
  한 번 다르다 — 그 커밋이 바뀐 키를 기록하지 않으면 이 결정의 §3 대로 거부되고 사람이 전량을 강제한다(그날 한 번).
- 통째 덮어쓰기(ADR-0326 의 비용 선택)도 바뀐 키를 기록하면 그런 날도 §1 이 설명한다. 그 기록은 ADR-0312 의 몫이다.
- 필지 레인의 검증 재기준(ADR-0146, 운영자가 켜는 것)은 그대로 거부한다: 운영자가 이미 지켜보는 경로다.
