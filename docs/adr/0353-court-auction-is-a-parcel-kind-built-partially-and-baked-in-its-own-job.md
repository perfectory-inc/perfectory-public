# ADR 0353: 법원경매는 필지의 종류 레인이다 — 바뀐 물건만 다시 만들고, 자기 굽기 작업으로 서빙한다

- Status: Accepted
- Date: 2026-10-30
- Builds on: [ADR-0287](./0287-public-sale-is-a-parcel-kind-built-from-its-own-silver.md)(자기 생산자 종류),
  [ADR-0292](./0292-public-sale-servable-gold-carries-only-host-panel-pnus.md)(서빙 가능한 PNU 만),
  [ADR-0303](./0303-each-by-pnu-lane-is-its-own-schedule-pipeline.md)(레인마다 자기 굽기 작업),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)(부분 재생성),
  [ADR-0327](./0327-a-worker-deploy-and-a-quarter-hourly-probe-read-what-users-read-twice.md)(엣지 탐침),
  [ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)(자산 조정기),
  [ADR-0336](./0336-every-published-schema-lives-in-one-registry-and-ci-holds-every-consumer-to-it.md)(스키마 레지스트리)
- 대체한 초안: PR #880(공매를 본뜬 화면 골격, 그 뒤 main 의 구조 전환 전에 만들어졌다)

## Context

법원경매 목록 수집과 Silver 적재는 2026-10-30 사장 승인("모든 데이터 수집을 승인")으로 켜진다. `silver.court_auction_item`
은 2026-10-19 밤 한 번 적재로 43,821 행, 그중 PNU 가 있는 행 37,922(86.5%)이다(Trino 읽기 전용 실측). 산업용 부동산을
사는 사람에게 경매 물건은 공매와 같은 자리, 필지 패널에 보여야 한다.

초안 #880 은 공매 레인을 본떠 이 화면을 만들었지만, 그 뒤 main 이 바뀌었다:

| 그 뒤 생긴 규칙 | 초안 #880 |
|---|---|
| 모든 처리는 부분, 전량은 이유가 있는 예외(ADR-0312·0314) | 매일 전량 덮어쓰기, 물건 표를 통째 입력(whole)으로 선언 |
| 레인마다 자기 굽기 작업(ADR-0303) | 마지막 커밋에서 따로 붙였다 |
| 발행 스키마는 레지스트리 한 곳(ADR-0336), 손으로 쓴 버전 문자열 금지 | `contracts/by-pnu-documents` 기록 + 웹 `z.literal("…v1")` |
| 자산 조정기가 잃어버린 이벤트를 상태로 메운다(ADR-0333 §7) | 없음 |
| 서빙 엣지를 두 번 읽는 탐침(ADR-0327) | 없음 |

공매 Gold 가 매일 전량인 까닭은 그 물건 표가 매시간 바뀌고 문서 블록 표 일곱 개를 통째로 읽기 때문이다(jobs.v1.json
`gold_public_auction_rebuild.full_reason`). 경매 물건 표는 밤에 한 번, 바뀐 행만 쌓이므로(ADR-0280) 그 이유가 없다.

## Decision

1. **종류 한 줄, 자기 생산자.** `config/r2-connections.contract.json` 의 `parcel-court-auction` 줄이 섹션
   `court_auction`·열 `court_auction_json`·생산자 `court_auction_silver_to_gold` 를 이름 짓는다. 물건의 자리(현재 행의
   PNU, 필지 이력·합병 따라가기, 길 잃은 PNU 는 세되 서빙하지 않음)는 공매의 `placed`·`servable`·`parcels_named` 를
   **그대로 불러 쓴다**(복사하지 않는다). 문서는 진행 중이고 매각기일이 오늘 이후인 물건을 기일 순서로 담고, 출처 표시
   (`source_attribution` = "대한민국법원 법원경매정보", 저작권법 제24조의2, ADR-0281 §6)를 싣는다.
2. **부분 재생성(ADR-0312 §2).** 물건 표는 통째 입력이 아니다: `affected_pnus` 가 물건 표의 바뀐 키
   (`gold_incremental.changed_rows`)와 지난 빌드 뒤 매각기일이 지난 물건을, 지금 놓이는 PNU 와 지금 Gold 가 싣고 있는
   PNU 양쪽으로 옮긴다. 서빙 필지 판(`silver.parcel_boundaries`)과 필지 이력이 바뀌면 물건 행이 이름 짓지 않은 자리가
   움직이므로 그 둘만 통째 입력이다(새 판은 한 달에 한 번이고, 그때는 전량이 맞다). 작업 `gold_court_auction_rebuild`
   의 `scope` 는 `changed-keys` 이다. **시험:** 옮겨 간 물건은 옛 PNU 와 새 PNU 둘 다, 기일이 지난 물건은 옛 PNU 만,
   손대지 않은 물건의 PNU 는 다시 만들지 않는다(`test_court_auction_gold.AffectedSparkTest`).
3. **자기 굽기 작업과 자산(ADR-0303, ADR-0333).** `by_pnu_court_auction_bake` 가 Gold 커밋마다 이 레인만 굽는다
   (1칸, MemoryMax 3G, 60분). Gold 작업과 굽기 작업은 둘 다 `started_by: inputs` 이고 `asset_reconcile.reconciles`
   에 들어가 이벤트를 잃어도 5분 안에 상태로 다시 시작된다. 시계 시작은 22:05(공매 21:35 와 겹치지 않게)와
   11:05(10:15·10:45·10:55 굽기와 겹치지 않게)다.
4. **스키마 레지스트리 주제(ADR-0336).** `foundation-platform.parcel_court_auction_by_pnu` 를 판 1 부터 레지스트리에
   올린다(legacy 아님). 웹은 `pnpm gen:schemas` 가 만든 zod 를 `extend` 만 하고, 소비자 계약(web·발행기·Worker)이
   읽는 경로를 적는다. `contracts/by-pnu-documents` 기록은 만들지 않는다.
5. **엣지 탐침(ADR-0327)은 손대지 않고 따라온다.** 탐침은 `by_pnu_lanes` 의 모든 레인을 돌고, 레인의 표본은 운영자의
   첫 전환 때 호스트에 쓰인다. 이 레인에는 사진이 없으므로 미디어 읽기 대상(`edge_probe.media.documents_lane`)은 공매
   그대로다.
6. **사진은 이 결정의 범위 밖이다.** 목록 검색 응답에는 사진이 없다. 사진은 물건상세(`gdsDspslObjctLst` 의 사진
   목록)에만 있고, 상세 수집은 ADR-0289/0290 의 2단계(세션 풀·상세·사진 CAS)로 아직 만들어지지 않았다. 그때 문서에
   선택 필드 `rounds[].photos[]` 를 레지스트리 판 2 로 더한다(FULL_TRANSITIVE 안의 확장). 매각결과 표
   (`silver.court_auction_sale_result`)도 아직 문서에 싣지 않는다.
7. **웹은 지난 기일을 거른다.** Gold 는 하루 한 번 만들어지므로, 그 사이 기일이 지난 물건은 웹이 한국 날짜로 거른다
   (`court-auction.ts upcoming`).

## Consequences

- 매일 밤 물건이 바뀐 PNU 만 다시 만든다. 첫 빌드와 판이 바뀐 날만 전량이다. 전량도 약 4만 행 크기라 공매(4,713 행,
  143초)의 몇 배 안이다.
- 첫 발행은 운영자의 레인 전환(`by-pnu-pack-operator.sh parcel-court-auction cutover 1`)이다. 그 전까지 굽기 작업은
  `nothing to do` 로 끝난다. 전환은 창(04:30–21:50 UTC)을 피해야 한다.
- 비용: 서빙 묶음은 약 4만 문서로 R2 저장 수십 MB, 읽기는 사용자 조회만큼이다(월 수십 원 수준, 계산). Spark 는 자체 서버다.
- 사진·매각결과는 다음 결정에서 더한다(6항).

## Rejected

- **공매처럼 매일 전량:** 물건 표가 밤에 한 번 바뀐 행만 쌓이는데 매일 4만 행을 다시 쓴다. 부분이 기본이라는 원칙
  (ADR-0314)에 이유가 없다.
- **`by_pnu_serving_bake all` 에 넣기:** 전국 필지 재굽기가 몇 시간씩 이 레인을 붙잡는다(2026-10-28 공매가 겪은 일,
  ADR-0303).
- **기록 파일(`contracts/by-pnu-documents`)로 시작하고 나중에 옮기기:** 새 주제는 레지스트리로 시작하는 것이
  ADR-0336 이행 표의 목표 상태다. 옮길 일을 만들 이유가 없다.
