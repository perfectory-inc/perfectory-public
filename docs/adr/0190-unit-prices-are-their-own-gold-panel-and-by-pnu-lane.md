# ADR 0190: 호 공시가격은 건물 패널에서 떼어 자기 Gold 패널과 묶음 전용 레인으로 서빙한다

- Status: Accepted
- Date: 2026-10-12
- Amends: [ADR-0182](./0182-the-gold-rebuild-joins-the-unit-prices-and-the-panel-reads-the-newest-vintage.md)
  (가격을 건물 패널에 조합하던 자리), [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md)
  (첫 묶음 발행은 객체 레인 위에서만 하던 것)
- Builds on: [ADR-0101](./0101-unit-prices-preserve-every-reference-date.md), [ADR-0151](./0151-building-packs-serve-each-document-whole.md),
  [ADR-0164](./0164-panel-gold-is-written-in-pnu-order-and-a-shard-reads-only-its-files.md),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md),
  [ADR-0185](./0185-a-unit-price-batch-is-its-snapshots-and-the-panel-reads-the-last-one-of-a-vintage.md)

## Context

`gold.building_panel` 은 PNU 하나에 한 행이고, `buildings_json` 이 모든 호와 각 호의 `official_price_history`(기준일
2008년부터 전부)를 품는다. 행의 대부분이 가격 이력이다.

| 증상 (2026-10-11/12 운영) | 원인 |
|---|---|
| 벡터 읽기에서 Arrow 2 GiB 넘침 (ADR-0185 가 벡터화를 끔) | 한 행이 최대 약 1,270만 자 |
| PNU 굽기 디코드 OOM 6회 (PR #423 이 스트리밍으로 고침) | 같은 행 |
| Gold 증분이 1시간 뒤 OOM | 가격 판이 바뀌면 아파트 PNU 거의 전부의 JSON 을 다시 만든다 |

가격은 건물·호와 다른 주기로 바뀌는 시계열이다(판은 매월, 기준일은 해마다). 시계열을 실체 문서 안에 넣으면 시계열의
작은 변화가 실체 문서 전체의 재계산·재굽기가 된다.

업계의 형태:

- 차원 모델링은 천천히 바뀌는 실체와 시점마다 쌓이는 값을 다른 표에 둔다
  ([Kimball, fact table grains](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/timespan-fact-table/)).
  우리 시계열의 정본은 이미 그 모양이다: `silver.unit_official_price` 는 (호, 기준일) 한 행이고 판마다 쌓인다(ADR-0101, 0182).
- 부동산 상세 화면은 실체와 가격 이력을 다른 요청으로 받는다. Redfin 은 집 정보(`aboveTheFold`)와 가격·세금
  이력(`belowTheFold`)을 따로 낸다([예시 정리](https://www.scrapingbee.com/blog/how-to-scrape-redfin/)).

## Decision

1. **Gold: `gold.unit_price_panel` — PNU 하나에 한 행, `units_json` = 그 PNU 건물 문서가 싣는 호들의 가격 이력.**
   - 호를 PNU 에 매다는 함수는 건물 패널 생산자의 `unit_placement` 하나다(연결된 호는 건물의 PNU, 미연결 호는 자기 PNU).
     그래서 웹은 두 문서를 호 `id` 로 합친다. 가격이 있는 호가 하나라도 있는 PNU 만 행이 있다.
   - 생산자 `unit_price_panel_silver_to_gold.py` 는 다른 패널과 같은 증분 기계(ADR-0180 `gold_incremental.Lane`, 고치지
     않고 재사용)를 쓴다: 판 사이에 실제로 바뀐 가격·호·표제부 행이 닿는 PNU 만 다시 계산해 MERGE 한다. 판마다
     바뀐 가격이 적으면 바뀐 PNU 만 쓰고, 해마다 새 기준일이 오면 거의 모든 PNU 가 바뀌어 전량 재생성이 된다.
   - PNU 한 행이라 지금의 굽기·변경 집합(`by_pnu_panel_delta`, row_digest)·관문이 그대로 맞는다. 기각한 대안:
     (호, 기준일) 한 행의 Gold. 시계열 정본은 Silver 가 이미 그 모양이고, 서빙 기계(증분 MERGE 키, 변경 집합,
     굽기의 행 상한)가 전부 "Gold 한 행 = PNU 한 문서"라 PNU 묶기를 새로 짜야 한다.
   - 예약 Gold 재생성(`gold-panel-rebuild.sh all`)이 parcel·building 다음에 `unit-price` 를 만든다.
2. **`gold.building_panel` 은 가격을 읽지 않는다**(전환 3단계에서). 웹은 가격을 접힌 `<details>` 안에서만 그리므로
   첫 화면에 최신가도 필요 없다. 그 뒤로 가격 판은 건물 Gold·건물 묶음을 건드리지 않는다.
3. **서빙: 계약이 이름 짓는 묶음 전용 레인.** `config/r2-connections.contract.json` 의 `by_pnu_lanes` 가 레인 목록의
   정본이다. 다른 자료 종류도 같은 모양으로 더한다(코드 수정 없이 Gold 패널 + 계약 항목).

   | 계약 항목 | 뜻 |
   |---|---|
   | 키 (`unit-price`) | 레인 이름. manifest `unit` = `<이름>-by-pnu`, 환경변수 접두사 = `FOUNDATION_PLATFORM_<이름_대문자>_BY_PNU_SERVING`, 명령 = `export-<이름>-by-pnu-section-packs` 등 — 모두 규칙으로 유도 |
   | `pack_only: true` | 객체 레인이 없다. Worker 는 묶음으로만 답하고, 묶음 없는 manifest 는 503 |
   | `gold_table` | 그 레인의 Gold 패널. 내용 칸은 전부 `*_json` 이어야 한다(계약을 읽을 때 검사) |
   | `document_schema_version` | 문서 스키마 |
   | `latency_baseline_lane` | 첫 발행 관문 (나)가 비교할 운영 레인 |
   | 나머지 | `building_by_pnu_gateway` 와 같은 모양(Worker 이름·주소·경로·캐시·`section_packs`) |

   - 문서는 범용 렌더러(`pack_lane_document.rs`)가 규칙으로 만든다:
     `{schema_version, pnu, source{table, iceberg_snapshot_id}, <칸 이름에서 _json 을 뗀 것>: 그 JSON}`.
   - Worker 는 같은 소스(`foundation-by-pnu-gateway`)의 레인별 빌드다(`wrangler.unit-price.jsonc`, 계약에서 생성).
     ADR-0151 처럼 gzip 덩어리를 그대로 낸다. 굽기·패치·발행·감시·카나리아 명령과 예약 굽기(`by-pnu-serving-bake.sh all`)는
     계약의 레인을 모두 돈다. 아직 첫 발행 전인 레인은 예약 굽기에서 "할 일 없음"이다.
4. **묶음 전용 첫 발행 관문**(모든 묶음 전용 레인의 공용 기반, ADR-0147 §6 의 대체가 아닌 확장):
   - (가) 그대로: 굽는 곳에서 Gold 행이 그리는 문서 바이트 = 묶음에서 다시 읽은 바이트, 전수.
   - (나) 비교 대상이 다르다: 객체 경로가 없으므로, 이 레인의 미리보기를 `latency_baseline_lane` 의 미리보기
     (같은 PNU, 같은 Worker 코드, 같은 배치)와 번갈아 읽어 기존 SLO(차가운·따뜻한 읽기 증가 한도, 가용성, CPU,
     no-gzip, 부하 단계)를 그대로 단언한다. 내용은 기준 레인의 답이 아니라, 발행기가 R2 의 미발행 묶음에서
     직접 꺼낸 그 레인 문서와 견준다. 증거는 `baseline_lane` 을 적고, 발행은 그것이 계약의 기준 레인과 같아야 받는다.
   - 첫 발행은 manifest 가 없을 때 v2 봉투(`object_count` 0)에 `section_packs` 블록을 실어 처음 쓴다(생성만, CAS).
   - 카나리아·롤백은 기존 Worker 기계(버전 비율) 그대로다. 첫 배포는 트래픽이 없는 새 Worker 다.
5. **웹이 읽을 때 합친다**(별도 PR). 호 가격 `<details>` 를 처음 열 때 그 PNU 의 가격 문서를 한 번 받는다
   (PNU 당 한 번, 같은 PNU 의 다른 호는 캐시). 404 는 "가격 없음"이다.
6. **전환 순서**(운영자가 실행, 단계마다 되돌릴 수 있다):
   1. 이 변경 배포 → 예약 Gold 재생성이 `gold.unit_price_panel` 을 처음 만든다.
   2. 새 Worker(`foundation-unit-price-gateway` 와 미리보기) 배포 → g1 전량 굽기 → 관문 (가)·(나) → 첫 발행.
   3. 웹 배포(가격은 가격 레인에서, 건물 문서의 가격은 있어도 무시).
   4. 건물 패널에서 가격 제거 → 건물 Gold 재생성 → 건물 레인 새 세대 전량 굽기·발행.
   - 옛 세대·옛 스냅숏은 지우지 않는다(ADR-0072). 묶음 레인의 manifest 세대 교체는 타일의 "전량 교체" 와 같은
     모양이고 툼스톤은 레인 안 패치에서만 쓴다.
7. **측정**(각 PR 에 재는 법을 적는다): 가격 Gold 전량 ≤ 30분(gold-panel-rebuild 로그의 `in Ns`), 가격 입력이 빠진
   건물 Gold 증분 ≤ 10분(같은 로그), 월 가격 판의 패치 굽기 ≤ 10분(by-pnu-serving-bake 런 요약의 시작·끝).
   건물 Gold 행 최대 크기는 측정 후 벡터 읽기 끄기(ADR-0185)를 되돌릴지 정하는 근거다.

## Consequences

- 매월 가격 판은 가격 Gold 의 바뀐 PNU 와 가격 레인의 바뀐 PNU 패치만 만든다. 건물 쪽은 영향이 없다.
- 해마다 새 기준일이 오면 가격 레인만 거의 전량 다시 굽는다(법정동 수만큼의 묶음).
- 다음 자료 종류는 Gold 패널 + 계약 항목만으로 레인이 된다. parcel·building 블록을 `by_pnu_lanes` 로 옮기는 것은 후속이다.
- 패널을 여는 사용자는 가격을 펼칠 때 요청 하나를 더 낸다.
- 시험: Spark(가격 패널 증분 = 전량, `test_unit_price_panel_iceberg`), Rust(계약 레인의 명령·규칙 이름, 렌더러, 객체 없는
  첫 발행과 기준 레인 대조, `pack_lane_tests`), Worker(묶음 없는 manifest 는 503, `unit-price-gateway.test.ts`),
  예약 작업(`test_gold_rebuild`).
