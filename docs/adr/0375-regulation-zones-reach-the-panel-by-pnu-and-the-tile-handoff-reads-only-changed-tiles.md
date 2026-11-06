# ADR 0375: 규제 구역은 필지 패널에 by-PNU 로 가고, 타일 넘김은 바뀐 타일만 읽고, 빽빽한 계열은 한 줌 늦게 나온다

- Status: Accepted
- Date: 2026-11-04
- Builds on: [ADR-0373](./0373-regulation-zones-are-served-as-map-tiles-patched-where-a-zone-changed.md)(규제 구역 지도 타일),
  [ADR-0374](./0374-a-tile-unit-may-be-born-from-its-first-lakehouse-bake.md)(유닛의 탄생),
  [ADR-0323](./0323-parcel-tiles-patch-only-the-tiles-changed-parcels-touch.md)(바뀐 타일만 패치),
  [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)(Gold 변경 기록),
  [ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)(비교 커밋이 변경 기록),
  ADR-0329 §3(`views.changed_codes`), ADR-0361(자기 생산자 종류 — 템플릿)
- Removes: ADR-0373 이 남긴 두 지름길. 하나는 패널이 지도가 불러 둔 타일만 본 것이고, 다른 하나는 넘김이 바뀔 때마다 현재 행 전부를 읽은 것이다.

## Context

ADR-0373 은 지도 타일을 냈다. 그러나 두 곳이 지름길이었다.

1. **패널.** 필지 패널의 "지도 규제 구역" 목록은 지도가 이미 불러 둔 타일에서 필지와 겹치는 구역을 골랐다(웹 `shortcut:`).
   그래서 링크로 바로 연 필지는 지도가 그 자리를 그릴 때까지 아무 구역도 보이지 않았다.
2. **넘김.** Silver 스냅숏이 바뀔 때마다 넘김이 `silver.land_use_zone_area` 를 통째로 읽었다(측정 아래).
   패치는 바뀐 타일만 냈지만, 읽기는 전량이었다.

또 z10 의 가장 붐비는 타일이 tippecanoe 기본 한도(500KB)를 넘는지 재지 않았다.

**측정 (2026-11-04, 운영 Trino 읽기 전용).**

- Silver 행: 판 20260609 이 231,579 행, 판 20261009 가 648,845 행(그중 삭제 표시 43,742)이다. 합계 **880,424 행**,
  현재 구역 655,266 이다. 지금 넘김은 실행마다 이 전부를 읽는다.
- 바뀐 구역이 닿는 z10 타일과 그 안의 이웃 구역(구역 상자 기준):

| 바뀐 것 | 바뀐 구역 | z10 타일 | 이웃 구역 |
|---|---:|---:|---:|
| 시군구 26350 전체 | 1,418 | 2 | 17,021 |
| 시군구 27140 전체 | 2,457 | 2 | 25,180 |
| 시군구 43110 전체 | 15,688 | 4 | 37,549 |
| 전국에 흩어진 0.1%(무작위 649) | 649 | 121 / 196 | 633,855 |

- 가장 붐비는 z10 타일(x=873, y=396, 35,973 구역)의 실제 MVT 크기. extent 4096, 단순화 1 단위(tippecanoe 의 문서화된
  허용 오차), 버퍼 5px, gzip 으로 인코딩했다:

| 계열 | z10 | z11(4 장 중 최대) | z12 |
|---|---:|---:|---:|
| 도시계획시설 D142–D148 | 497 KB | 226 KB | — |
| 용도지역 | 281 | 138 | 62 |
| 개발제한구역 | 30 | 17 | 8 |
| 공장입지 | 10 | 10 | 6 |
| 농업진흥 | 0 | 0 | 1 |
| 모두 | ≈ 818 | ≈ 391 | |

## Decision

### §1 필지마다 겹치는 규제 구역은 by-PNU 종류다

1. 자기 생산자 종류 한 줄을 `config/r2-connections.contract.json` `by_pnu_lanes` 에 더한다(ADR-0361 소유 종류가 템플릿).
   - 레인 `parcel-regulation-zones`, 종류 `regulation-zones`.
   - Gold 는 `gold.parcel_regulation_zones`, 스키마 레지스트리 주제는 `foundation-platform.parcel_regulation_zones_by_pnu`,
     경로는 `/parcels/regulation-zones/by-pnu/<pnu>` 다. 모두 그 한 줄에서 유도된다.
   - 문서는 PNU 마다 구역 목록이다: `zone_id`, `layer_code`, `layer_name`, `zone_code`, `zone_name`(원천의 말 그대로)과 기준일(판 날짜).
2. **관계는 내부가 겹치는 것이다.** 경계만 닿는 것은 넣지 않는다.
   - 후보는 격자 칸 등가 조인으로 고른다. 그다음 표준 라이브러리만 쓰는 WKB 다각형 판정으로 확정한다.
   - 이 방식은 `parcel_land.py` 의 선례를 따른다. Spark 이미지에는 Sedona·shapely 가 없다.
3. **Gold 는 `gold_incremental` 로 바뀐 것만 다시 만든다**(ADR-0312). 영향받는 PNU 는 셋이다.
   - 바뀐 구역의 옛 도형과 새 도형에 겹치는 필지;
   - 바뀐 필지;
   - 법정동 코드가 바뀐 필지. ADR-0329 §3 `views.changed_codes` 를 필지 패널의 `code_change_pnus` 와 같은 방식으로 따른다.
     코드 변경 표를 통째 입력(`WHOLE_TABLE_INPUTS`)으로 두지 않는다.
4. **패널은 이 레인을 읽는다.** 지도가 그 자리를 그렸는지와 무관하다. 지도의 계열 토글은 그대로 타일을 쓴다.
   수집 대기 층(ADR-0373 §4)의 안내는 그대로다.
5. 첫 생성은 선언된 전량 예외다. by-PNU 첫 발행은 운영자의 묶음 전용 관문을 거친다. 그래서 두 작업은 꺼진 채 병합된다.

### §2 규제 구역 넘김은 바뀐 타일만 읽는다

1. **타일 색인 표 `gold.land_use_zone_area_by_tile`.** 덧붙이기만 하는 Iceberg 표다.
   - 한 줄이 (z10 타일, 구역) 하나다: `tile_key`(`x * 1024 + y`), `zone_id`, 타일 속성 넷, 도형, 상자,
     `content_sha256`, Silver 판, 반영한 Silver 스냅숏, `row_state`(삭제 표시), `ingested_at_utc`.
   - `tile_key` 로 파티션한다. 현재 행은 (tile_key, zone_id) 마다 가장 새 줄이다.
     구역이 타일을 떠나거나 사라지면 그 타일에 삭제 표시를 덧붙인다.
   - 상자가 z10 타일에 닿으면 소속이다. 굽기 패치 버퍼(`patch_buffer_pixels` 6px, Rust `BUFFER_PIXELS`)보다 1px 넓혀 본다.
     넘치는 쪽은 안전하다. Python 과 Rust 의 타일 계산은 같은 고정값(`tile_range_golden.json`)을 푼다.
2. **유지는 덧붙은 행만 읽는다.**
   - 유지된 Silver 스냅숏 이후 비교 레인이 기록한 변경 커밋만 읽는다. 읽는 길은 `changelog.changes_between` 이다
     (ADR-0311, 압축은 건너뜀).
   - 옛 소속은 좁은 키 열(도형 없음)에서 찾는다.
   - 전량 예외는 둘이다. 표가 없을 때와, 기록된 변경만으로 이어지지 않을 때(만료·되돌리기)다. 그때 이유가 요약
     `full_reason` 에 남는다.
3. **넘김은 부분이다.**
   - 서빙 타일을 만든 뒤 색인에 덧붙은 줄이 바뀐 구역과 더러운 z10 타일을 정한다.
     옛 소속의 삭제 표시와 새 소속의 줄이 모두 들어 있으므로 옛 상자와 새 상자의 타일이 모두 든다.
   - 넘기는 것은 그 타일 파티션의 현재 구역(이웃)과 변경 목록(`upsert`/`delete`)이다.
   - 요약은 `handoff_scope`·`rows_read` 를 적고, 작업 로그는 `land-use-zone-area-tile-handoff-read` 줄을 남긴다.
4. **굽기는 부분 넘김을 패치로만 받는다.**
   - 새 지문 색인은 서빙 스냅숏의 옛 `.idx`(ADR-0323 §1)에 변경 목록을 합친 것이다.
   - 더러운 타일은 지금처럼 옛 상자와 새 상자로 낸다. 게이트는 합친 색인으로 돈다.
   - 패치가 거절되면 부분 넘김은 기본판이 되지 않는다. 거절 사유는 옛 색인 없음, 한도 초과, 불일치, 탄생 전이다.
     - 굽기는 `lakehouse-tile-bake-partial-declined` 를 남기고 65 로 끝난다.
     - 작업은 `--full` 로 다시 넘기고 다시 굽는다. 이것이 선언된 전량 예외이고, 층 archive 재사용(ADR-0316)은 그대로다.
   - parcels·admin·complex 는 그대로다.

### §3 계열별 최소 줌은 타일 크기로 정하고 타일 계약에 적는다

- **예산.** 타일당 500 KB gzip 이다. tippecanoe 의 기본 최대 타일 크기이자 Mapbox Tiling Service 의 한도다.
- **결정.** z10 에서 모든 계열을 합치면 818 KB 로 넘는다. 도시계획시설 하나(497 KB)를 빼면 321 KB 로 맞는다. z11 은 모두
  합쳐 391 KB 로 맞는다. 그래서 도시계획시설(D142–D148)은 z11 부터, 나머지는 z10 부터 나온다.
- **기록.** `config/tile-bake-containers.contract.json` `feature_min_zoom.zoning` 에 적고, 이유는
  `feature_min_zoom_reason` 에 적는다.
- **강제.** 굽기가 이 유닛에만 tippecanoe `-j` 피처 필터(`$zoom`, 고정 커밋 2.79.0 이 지원)를 붙인다.
  - 이 인자는 bake inputs 에 든다. 그래서 다른 인자로 만든 색인과 archive 는 재사용되지 않는다.
  - 모든 최소 줌은 레이어 줌(10–14) 안이어야 한다. 그래서 maxzoom id 게이트는 그대로 통과한다.

## Consequences

- **링크로 연 필지.** 지도가 아직 그리지 않은 자리의 필지도 패널이 규제 구역을 보인다(웹 합성 시험).
- **넘김이 읽는 행 (측정).** 지금은 실행마다 880,424 행이다. 시군구 하나가 통째로 바뀌는 달에는 이웃 17,021–37,549 구역과
  바뀐 행만 읽는다. 전국에 흩어진 변경은 거의 전부다: 낮은 줌 타일은 그 안의 모든 구역에 기댄다.
  옛 소속 찾기는 색인 표의 좁은 키 열을 행 수로는 전부 훑는다(`rows_read.by_tile_key_scan`).
  첫 운영 실행의 `rows_read` 가 첫 실측이다.
- **비용 (계산).** 타일 색인 표는 도형을 한 번 더 가진다. 약 1.6GB 이고 R2 에서 월 약 35원이다.
  패널 레인의 Gold 와 묶음은 필지 수에 비례한다. 첫 생성 비용은 첫 운영 실행에서 잰다.
- **위험.**
  - Iceberg 스냅숏 만료(약 30일) 뒤에는 첫 실행이 전량으로 돌아간다. 이유는 기록된다.
  - 196 개 타일 파티션에 작은 파일이 쌓인다. 매일 압축이 정리한다.
  - 표준 라이브러리 다각형 판정의 경계 허용 오차는 시험으로 고정한다.
