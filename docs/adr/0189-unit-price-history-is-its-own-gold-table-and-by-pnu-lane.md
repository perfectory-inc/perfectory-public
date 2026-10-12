# ADR 0189: 호 공시가격 이력은 건물 패널에서 떼어 자기 Gold 표와 자기 by-PNU 레인으로 서빙한다

- Status: Proposed
- Date: 2026-10-12
- Amends: [ADR-0182](./0182-the-gold-rebuild-joins-the-unit-prices-and-the-panel-reads-the-newest-vintage.md)
  (가격을 건물 패널에 조합하던 자리), [ADR-0151](./0151-building-packs-serve-each-document-whole.md) (건물 문서의 내용)
- Builds on: [ADR-0101](./0101-unit-prices-preserve-every-reference-date.md), [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md),
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

- 차원 모델링은 천천히 바뀌는 실체(차원)와 시점마다 쌓이는 값(사실 표)을 나누고, 사실 표는 (실체, 시점) 한 행이다
  ([Kimball, fact table grains](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/timespan-fact-table/)).
- 부동산 상세 화면은 실체와 가격 이력을 다른 요청으로 받는다. Redfin 은 집 정보(`aboveTheFold`)와 가격·세금
  이력(`belowTheFold`)을 따로 낸다([예시 정리](https://www.scrapingbee.com/blog/how-to-scrape-redfin/)).
- Iceberg 의 행 단위 `MERGE INTO`(copy-on-write)는 바뀐 행이 든 파일만 다시 쓰고, Spark 3.5 의
  `WHEN NOT MATCHED BY SOURCE` 로 사라진 행을 지운다([Iceberg Spark writes](https://iceberg.apache.org/docs/1.8.0/spark-writes)).

## Decision

1. **Gold: `gold.unit_price_history` — (호, 기준일) 한 행.**
   - 칸: `pnu`(그 호가 서빙되는 PNU — 건물 패널이 호를 매다는 PNU 와 같은 함수), `unit_id`, `register_pk`,
     `base_date`, `price_won`, `source_snapshot_id`, `row_digest`, `published_at_utc`. 키는 (`register_pk`, `base_date`).
   - PNU 순서로 쓴다(ADR-0164 와 같은 정렬), 그래서 굽기 샤드는 자기 파일만 읽는다.
   - 생산자 `unit_price_history_silver_to_gold.py` 는 가장 새 판의 묶음(ADR-0185 `newest_batch`)을 전부 투영하고
     `MERGE INTO … ON (register_pk, base_date)` 한 번으로 반영한다: 바뀐 값만 갱신, 새 기준일은 삽입, 판에서 사라진
     행은 삭제. 행이 작아 전량 투영이 싸고, 쓰기는 바뀐 파일만이다. 입력 스냅숏이 직전 실행과 같으면 빈 커밋.
   - 호를 PNU 에 매다는 함수(`unit_placement`)는 건물 패널 생산자에서 꺼내 두 생산자가 같이 부른다(한 곳).
2. **`gold.building_panel` 은 가격을 읽지 않는다.** `silver.unit_official_price` 를 입력에서 빼고, 호 객체에서
   `official_price_history` 를 뺀다. 첫 화면에 최신가가 필요 없다(웹은 가격을 접힌 `<details>` 안에서만 그린다).
   가격 판이 바뀌어도 건물 패널 Gold·건물 묶음은 한 바이트도 바뀌지 않는다.
3. **서빙: 새 by-PNU 레인 `unit-price-by-pnu`.** 건물·필지 레인과 같은 묶음 기계(형식 v1, 세대·패치·manifest,
   관문 가·나·다, 카나리아, 매시 감시)를 그대로 쓰고, 레인 하나를 더한다.
   - 문서: `{schema_version: "foundation-platform.unit_price_by_pnu.v1", pnu, source, units: [{id, official_price_history}]}`,
     호 id 오름차순, 가격이 하나라도 있는 PNU 만. 없으면 404(가격 없음).
   - Worker: 같은 소스(`foundation-by-pnu-gateway`)의 세 번째 빌드(`wrangler.unit-price.jsonc`), 자기 이름·주소,
     gzip 덩어리 그대로 통과(ADR-0151 의 CPU 근거 유지 — Worker 가 합치지 않는다).
   - 변경 집합: `by_pnu_panel_delta` 를 (PNU 별 행 지문 집계)로 넓혀, 가격 레인은 바뀐 행의 PNU 만 패치한다.
   - 기각: 건물 레인 안의 두 번째 항목 — 한 레인이 Gold 표 둘의 반영 스냅숏을 가져야 하고, 패치 번호가 레인
     하나라서 가격 판이 건물 레인의 패치를 만든다.
4. **웹이 읽을 때 합친다.** 건물 문서는 지금처럼 받고, 호 가격 `<details>` 를 처음 열 때 그 PNU 의 가격 문서를
   한 번 받는다(`useQuery`, 키 = PNU, 같은 PNU 의 다른 호는 캐시). 캐시 머리는 건물 레인과 같다
   (`public, max-age=3600`, 묶음 `ETag`).
5. **전환 순서(코드로, 각 단계 되돌릴 수 있음)**:
   1. 코드 배포. 가격 Gold 첫 생성, 가격 레인 g1 굽기 → 관문 → 발행 → Worker 카나리아.
   2. 웹 배포: 가격은 가격 레인에서, 건물 문서의 `official_price_history` 는 있어도 무시(옛 문서와 새 문서 둘 다 받음).
   3. 건물 패널 Gold 전량 재생성(가격 없음) → 건물 레인 새 세대 전량 굽기 → 관문 → 발행.
      건물 문서 `schema_version` 은 `building_by_pnu_profile.v3`(호에 가격 칸 없음). 웹은 v2·v3 를 받는다.
   4. 옛 세대·옛 Gold 스냅숏은 지우지 않는다(ADR-0072 append-only; 지도 타일 규칙 — 전량 교체, 툼스톤 없음 — 은
     묶음 레인의 manifest 세대 교체와 같은 모양이다).
6. **측정**(첫 운영 실행, run summary 와 굽기 요약에서): 가격 Gold 전량 반영 ≤ 30분, 건물 패널 Gold 증분(가격
   입력 없음) ≤ 10분, 가격 판 하나의 패치 굽기 ≤ 10분, 건물 Gold 최대 행 크기 ≤ 1 MB. 벡터 읽기 끄기(ADR-0185
   의 `read.parquet.vectorization.enabled=false`)는 측정 뒤 별도 결정으로 되돌린다.

## Consequences

- 매월 가격 판은 가격 Gold 의 바뀐 행과 가격 레인의 바뀐 PNU 패치만 만든다. 해마다 새 기준일은 행을 붙이는 일이다.
- 패널을 여는 사용자는 가격을 펼칠 때 요청 하나를 더 낸다.
- Worker·계약 블록·카나리아·감시가 레인 하나씩 늘어난다(코드는 같고 설정만 늘어난다).
- 시험: 생산자(Spark 로컬, MERGE 의 갱신·삽입·삭제), 건물 패널에 가격 칸 없음, 레인 열거와 굽기(Rust `--bins`),
  Worker 레인 빌드(vitest), 웹 합치기(vitest).
