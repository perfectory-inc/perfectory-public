# ADR 0350: 필지 화면은 그 필지의 실거래와 같은 법정동의 공장·창고 거래를 보인다

- Status: Accepted
- Date: 2026-10-30
- Builds on: [ADR-0349](./0349-a-real-transaction-row-is-named-by-its-content-and-a-cancellation-is-a-new-version.md)
  (`silver.real_transaction`, 법정동·PNU 연결), [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md)
  (데이터 종류 하나 = Gold 표 하나 = by-PNU 레인 하나), [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md)
  (자기 Silver 로 만드는 종류), [ADR-0292](./0292-public-sale-servable-gold-carries-only-host-panel-pnus.md)
  (서빙 Gold 는 주인 패널에 있는 PNU 만), [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)
  (Gold 커밋은 바뀐 PNU 를 남긴다), [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)
  (문서 스키마는 레지스트리 하나)

## Context

산업용 부동산을 사고 빌리는 사람이 필지 화면에서 묻는 것은 "이 땅은 얼마에 거래됐나" 와 "근처 공장·창고는 얼마에
거래되나" 다. ADR-0349 의 실측이 둘의 무게를 정한다.

| 사실 | 값 |
|---|---|
| 공장창고 거래 중 PNU 까지 이어지는 것 | 약 30–70%(일반건물은 지번 가림), 법정동은 100% |
| 토지 거래 중 PNU 까지 | 0%(모두 가림), 법정동은 100% |
| 공장창고 거래, 전국 한 달 | 988(2025-09, 실측) |
| 지목 공장용지 / 창고용지 필지 | 325,437 필지(10,916 법정동) / 152,890(16,102 법정동) — `silver.land_characteristic` 가장 새 판, Trino 읽기만 |

즉 "내 필지 거래" 만 보이면 공장·창고 필지 대부분은 빈 화면이다. 비교 거래는 법정동 단위로 이어진 거래에서 나와야
한다. 그런데 by-PNU 레인은 PNU 로만 열쇠를 쓴다(`pnu_pattern`). 법정동 열쇠 레인을 새로 두려면 게이트웨이 Worker 의
새 열쇠 체계가 필요하다.

## Decision

1. **종류 하나: `parcel-real-transaction`(Gold `gold.parcel_real_transaction`, 절 `real_transaction`, 열
   `real_transaction_json`).** 자기 생산자(`real_transaction_silver_to_gold`)가 `silver.real_transaction` 의 현재
   행(삭제 표지가 아닌 것)으로 만든다. 주인 패널에 묶이지 않는다(ADR-0287). 서빙 Gold 는 서빙 중인 필지판에 있는 PNU
   만 싣는다(ADR-0292).
   - 거래가 어느 PNU·법정동에 붙는지는 생산자가 매 빌드 원천 열(`address_raw`, 지번, 속성)에서 정본 해석기로 다시
     정한다. 해석기는 공식 연결과 가린 지번의 추정 연결(ADR-0351)을 함께 맡는다.
   - Silver 의 `pnu`·`legal_dong_code` 는 적재 시점의 증거일 뿐이다. 생산자는 그것을 읽지 않는다.
   - 계약 뒤 분할·합병된 필지는 공매 Gold 의 `placed` 처럼 계보로 현재 필지에 옮겨진다(ADR-0113).

2. **한 PNU 의 문서.**
   - `transactions`: 그 PNU 로 이어진 거래 전부(12종), 계약일 새 순. 해제된 거래도 `cancelled`·`cancel_date` 와 함께
     싣는다(숨기지 않는다: 화면이 표시한다). 금액은 원천의 만원 그대로, 면적은 m².
   - `comparables`: 그 PNU 의 지목이 공장용지·창고용지이거나 자기 공장창고 거래가 있을 때만, 같은 **현재** 법정동의
     공장창고 거래 중 해제되지 않은 것, 계약일이 빌드일로부터 36개월 안, 새 순 20건. 가린 지번 거래도 들어간다:
     이 목록의 몫이 그것이다. 각 거래는 원천의 지번 표기(`3**`)를 그대로 싣는다. 추정으로 이어진 필지가 있으면
     그 PNU 와 등급(`inferred_*`)도 싣고, 화면은 "추정" 으로 표시한다(ADR-0351).
   - `region`: 그 법정동과 시군구의 공장창고·토지 지난 12개월 건수와 m²당 금액의 중앙값·사분위(4번 표에서).
   - 문서는 `transactions` 나 `comparables` 가 하나라도 있을 때만 생긴다. 256 KiB 를 넘으면 오래된
     `transactions` 부터 덜어낸다(ADR-0193 의 한도, 공매와 같다).

3. **바뀐 것만 다시 굽는다(ADR-0312·0314).** 바뀐 열쇠는 다음 다섯 가지의 (PNU, 법정동)이다. 바뀐 PNU 의 문서와,
   바뀐 법정동의 공장·창고 필지 문서만 다시 만든다.
   - 적재가 붙인 행.
   - 법정동 코드 변경: `reference.legal_dong_code_change` 의 바뀐 행은 `views.changed_codes` 로 찾는다(ADR-0273,
     ADR-0329 §3). 그 두 코드 아래의 거래가 다시 해석된다. 사장 규칙: 모든 데이터는 법정동 코드 변경을 따라간다.
   - 추정 입력: 토지특성·건축물대장·토지소유의 바뀐 PNU 의 법정동(ADR-0351 §3).
   - 계보의 바뀐 간선.
   - 빌드일이 넘어가 36개월 창 밖으로 나간 거래가 있는 법정동. 계산(2번의 규모로): 공장창고 거래 월 약 1,000건 × 법정동당 공장·창고 필지 평균 약 20 → 월 약 2만 문서.

4. **지역 통계 `gold.real_transaction_region_stats`.** (데이터셋, 지역 단위 `sigungu`|`legal_dong`, 지역 코드,
   분기) 마다 거래 수·해제 수·m²당 금액(토지는 거래면적, 공장창고·상업업무는 대지면적과 건물면적 각각)의 p25·중앙값·
   p75. 해제된 거래는 건수에만 넣고 금액에서 뺀다. 가치평가 모형과 2번 `region` 이 읽는다. 바뀐 묶음의 (데이터셋,
   시군구, 분기)만 다시 계산한다.

5. **화면.** 공짱 필지 패널에 `실거래` 탭: "이 필지의 실거래"(계약일·거래금액·건물/대지면적·층·해제) 와 "같은
   법정동 공장·창고 거래"(계약일·거래금액·대지면적·건물면적·용도지역·지번 표기, 법정동 중앙값 한 줄). 낱말은
   원천의 낱말이다(거래금액, 건축물면적, 대지면적, 용도지역, 해제여부; 원천 대응표). 금액은 만원을 억·만원으로만
   바꿔 쓴다.

6. **스키마와 연결 순서.** 문서 스키마는 `contracts/by-pnu-documents/foundation-platform.parcel_real_transaction_by_pnu.v1.json`
   과 레지스트리(ADR-0336)에 먼저 두고, 웹은 생성된 타입만 쓴다. 순서: Gold 생산자·종류 줄·문서 기록·레지스트리 →
   재빌드 작업과 서빙 bake 작업(공매와 같은 모양, `gold_real_transaction_rebuild`, `by_pnu_real_transaction_bake`)
   → 웹. Silver 백필(ADR-0348 §5: 산업용 세 종 약 23일, 전부 약 86일)의 산업용 세 종이 끝나기 전에 서빙을 켜지 않는다: 반쯤 찬 이력은 "거래 없음" 으로 읽힌다.

## Consequences

- 법정동 열쇠 레인(게이트웨이의 새 열쇠)은 두지 않는다: 비교 거래를 공장·창고 필지 문서에 싣는 비용(위 계산, 월 약
  2만 문서)이 새 Worker 경로보다 작다. 공장·창고가 아닌 필지(예: 계획관리지역 나대지)도 비교 거래를 보여야 하면
  그때 법정동 레인을 다시 저울질한다.
- 가린 지번의 필지 추정은 ADR-0351 이 정한다(사장 결정 2026-10-31): 잰 정밀도가 99% 이상인 증거 수준만 쓴다.
- 이 ADR 의 구현은 Silver 수집이 돈 뒤의 별도 PR 들이다(6번의 순서).
