# ADR 0362: 공장 부지를 고르는 사람이 보는 규제 구역은 층마다 Silver 로 올리고, 지도는 바뀐 층만 굽는다

- Status: Accepted
- Date: 2026-10-30
- Builds on: ADR-0359(PR #968)(용도지역지구 도형을 매일 훑기가 받는다, §4 D154 대신 이 도형),
  [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)(Silver 레인은 장부에서 판을 고른다),
  [ADR-0275](./0275-silver-release-lanes-append-the-changed-keys-instead-of-overwriting.md)·[ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)·[ADR-0318](./0318-the-remaining-silver-lanes-append-their-changed-keys-compared-bucket-by-bucket.md)(바뀐 키만 덧붙이고 묶음마다 커밋),
  [ADR-0316](./0316-parcel-boundaries-are-partitioned-by-edition-and-range-ordered-by-pnu.md)(타일은 접두사로 나눠 바뀐 몫만 굽는다),
  [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)(지도 편집과 굽기)
- Related: ADR-0020(산업단지 경계의 정본은 ILIS DAM_DAN), FP-ADR-0004(지도 타일은 통째 교체뿐)

## Context

브이월드 용도지역지구 도형(`vworldkr__land_use_zone`, NA/24)은 고시된 규제 구역 그 자체다 — 개발제한구역, 도시계획시설,
공장설립제한·승인지역, 농업진흥지역, 용도지역 등 187개 층. 필지마다 어느 구역에 걸리는지는 이미 `silver.land_use_plan`(D155)이
가진다. 그러나 땅을 고르는 사람은 지도에서 구역의 **경계**를 본다: 이 부지 옆이 그린벨트인지, 도로 예정선이 어디로 지나는지.
그 경계는 이 데이터셋에만 있다(ADR-0359 §4).

**측정 (2026-10-30, 운영 원장 `catalog.bronze_object`·`bronze_object_member` 읽기 전용, R2 범위 읽기).**

- 층마다 한 달에 전체데이터 ZIP 하나(`AL_D<층>_00_<YYYYMMDD>.shp/.shx/.dbf/.prj/.fix`, `.cpg` 없음)와 변동데이터
  여러 개(`CH_D<층>_…`)를 낸다. 모든 층의 판이 같은 날짜다(2026-06 판은 모두 `20260609`).
- `.prj` 는 모든 층이 Korea 2000 Central Belt 2010(EPSG:5186), DBF 는 CP949 다.
- DBF 열은 측정한 다섯 층(D032 개발제한구역, D143 공간시설, D124 도시지역, D335 공장설립제한지역, D060 산업단지)에서 같다:
  `A0` N9 행 번호, `A1` C33 구역 번호(고시기관·시군구·고시·코드·일련으로 이뤄진 33자), `A2` 층 이름, `A3` D8 데이터기준일자,
  `A4` 시군구 코드, `A5` 용도지역지구 코드(`UDV100`), `A6` 그 이름, `A7` C8 이름 없는 날짜, `A8`·`A9` 이름 없는 글.
  빈 칸은 공백만이 아니라 NUL 로도 채워진다.
- `A1` 은 통째로 읽은 네 층에서 층 안에 겹치지 않는다(D032 758, D335 730, D336 660, D327 5,544 구역).
- 2026-06 판 압축 크기(MB): 개발제한구역 9, 농업진흥지역 141, 도시지역 132, 관리지역 600, 농림지역 342,
  자연환경보전지역 14, 교통시설 385, 공간시설 80, 유통및공급시설 9, 공공문화체육시설 15, 방재시설 66, 보건위생시설 0.4,
  환경기초시설 2, 산업집적활성화 용도구역 10, 공장설립제한지역 4, 공장설립승인지역 7, 산업단지 5, 보전·준보전산지 984
  (도형이 둘로 나뉜 ZIP).

## Decision

1. **한 표, 층마다 한 묶음.** `silver.land_use_zone_area`(GeoParquet): 키 `(layer_code, zone_feature_id)`(`D032`,
   `A1`), 층 이름·시군구·코드·이름, `A7`–`A9` 는 원천이 이름을 지웠으므로 `field_a7`–`field_a9` 로 원문 그대로, 도형은
   EPSG:4326 WKB 와 bbox·SHA-256. `A0`(행 번호)과 `A3`(판마다 바뀌는 기준일)은 싣지 않는다. 모든 층이 같은 열이라 층마다 표를
   만들지 않는다.
2. **처음 서빙하는 층은 15개다** — 계약의 `completeness.regions`.
   - 개발제한구역(032).
   - 도시계획시설 7종: 교통(142)·공간(143)·유통및공급(144)·공공문화체육(145)·방재(146)·보건위생(147)·환경기초(148).
   - 공장 입지 규제: 공장설립제한지역(335)·공장설립승인지역(336)·산업집적활성화 용도구역(327).
   - 농업진흥지역(036).
   - 용도지역: 도시(124)·농림(126)·자연환경보전(127).

   빼는 것과 이유:
   - **관리지역(125, 600MB)·보전·준보전산지(054, 984MB):** 500MB 를 넘어 RAON 레인으로만 온다. 그 레인의 예산이 0이라
     새 판이 오지 않고(ADR-0359 §2), 054 는 도형이 두 shapefile 로 나뉘어 있다. RAON 감독 실행 뒤 다음 단계에서 넣는다.
   - **산업입지/산업단지(060):** 산업단지 경계의 정본은 ILIS `DAM_DAN` 이고 이미 타일로 서빙된다. 같은 사실을 두 곳에 두지 않는다.
   - **나머지 169개 층**(학교환경위생정화구역·가축사육제한구역·소하천 등): 산업용 부동산 매수·임차인이 먼저 묻는 규제가 아니다.
     Bronze 에 남는다. 필지별 저촉은 이미 `silver.land_use_plan` 이 화면에 낸다.
3. **판 고르기·적재·비교는 기존 Silver refresh 레인 그대로다(새 장치 없음).**
   - 레인 `land-use-zone-area`, 작업 `silver_refresh_land_use_zone_area`, `scope: changed-keys`, 켜진 채로 병합된다.
   - 장부에서 목록의 층을 모두 덮는 가장 새 판을 고른다. 완결성 `listed` 는 시도 대신 계약이 적은 층 목록이다.
   - 층마다 export 한다(`export-land-use-zone-area-silver-handoff`, `land_use_silver_export` 와 같은 환경 이름·요약).
   - load branch 에 올리고, compare 가 층(`layer_code` 네 글자) 단위 묶음마다 바뀐 구역의 행과 사라진 구역의 삭제 표시만
     덧붙인 뒤 한 번에 발행한다. 비교 열은 이름·코드·`field_a*`·도형 체크섬이다.
   - 시간이 다하면 남은 층은 다음 실행이 잇는다(실행 상한 150분, ADR-0300 H).
   - 변동데이터(CH_)는 읽지 않는다. 달마다 오는 전체데이터와 키 비교가 같은 바뀐 키를 내고, 변동분을 차례대로 덮는 장치를
     따로 두지 않는다.
4. **도형 열은 기존 적재기로 싣는다.** `silver_scalar_handoff_to_lakehouse.py` 가 `binary` 열을 넘김에서 소문자 16진 글로
   받아 `unhex` 한다. 다른 열 형식은 그대로이고, 다룰 수 없는 형식은 Spark 를 띄우기 전에 거부한다. export 는 SHP 와 DBF 를
   ZIP 에서 나란히 흘려 읽어(`SequentialShapefileReader`) 가장 큰 층(교통시설 385MB)도 메모리에 통째로 두지 않는다. 좌표는
   공유 리더가 EPSG:4326 으로 바꾼다.
5. **지도(후속, 이 PR 밖).** 서빙은 지도 편집 굽기 경로(ADR-0112, `bake-lakehouse-tiles`)를 새 유닛 `zoning` 으로 쓴다.
   - 피처 id 는 `D032:<A1>`. `tile-bake-containers.contract.json` 의 `partition_by_feature_id_prefix` 를 4로 두어(ADR-0316)
     바뀐 층의 archive 만 다시 굽고 나머지는 재사용한다.
   - 남은 일:
     - ① served-Gold 작업(`served_gold_common.py`): Silver 현재 행 → 피처;
     - ② `catalog.vector_tile_publication_unit` 의 `zoning` 유닛과 첫 정적 릴리스를 만드는 마이그레이션(굽기는 활성 정적
       릴리스가 있어야 교체한다);
     - ③ 타일 Worker 경로와 공짱 지도의 층 토글.
   - 그래프는 이 서빙을 `planned` 로만 적는다 — 구현되지 않은 서빙 간선을 `implemented` 로 쓰지 않는다.

## Consequences

- 병합 뒤 다음 매일 훑기가 새 파일을 받으면, 아니면 매일 07:00 UTC 에 레인이 원장의 가장 새 완결 판(지금은 2026-06)을 올린다.
  첫 실행은 판 전체를 처음 덧붙이고, 그 뒤는 달마다 바뀐 구역만 덧붙인다.
- **크기·비용 (계산, 측정 아님; R2 $0.015/GB-월, 1달러 1,400원).** 15개 층의 한 판은 압축 약 1.2GB 다.
  - Silver: 처음 약 1.5–3GB(WKB 를 zstd Parquet 로 쓴 크기, shp 크기에서 추정).
  - 달마다 쌓이는 것:
    - 바뀐 구역 행 수백 MB 미만(전체 187개 층의 변동데이터가 달마다 약 0.1GB 다);
    - export 넘김(gzip JSONL, 증거로 남는다) 약 2–3GB.
  - 한 해 뒤 R2 약 40GB → 월 약 850원. 쓰기 요청은 층마다 수십 번이다.
  - Spark 는 이미 있는 ai-server spark 풀 몫이라 추가 비용이 없다.
  - 타일(후속): 15개 층 PMTiles 수백 MB 로 추정 → 월 100원 미만.
  - 원천 하나가 월 5만 원에 한참 못 미친다.
- 위험:
  - 공유 리더(`foundation_shapefile`)는 Polygon 이 아닌 도형(빈 도형 포함)을 만나면 실행 전체를 거부한다. 측정한 층에서는
    보지 못했지만, 만나면 그 판은 적재되지 않고 실패로 알려진다.
  - 장부 읽기 상한(`MAX_MEMBER_ROWS` 200,000)은 이 원천이 달마다 약 700개 객체를 더해 약 3년 뒤에 닿는다.
- 시험:
  - Rust: 합성 층 ZIP(99999 코드) → 계약 열과 같은 행, NUL 칸, id 없는 구역 거부, CH_·둘로 나뉜 층 거부;
  - 레인 계약(완결성 `listed`, 비교 묶음 4, 멤버 규칙);
  - Python: 적재기의 binary 운반.
