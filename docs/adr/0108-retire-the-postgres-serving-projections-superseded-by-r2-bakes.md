# ADR 0108: R2 굽기로 대체된 PostgreSQL 서빙 투영을 순차 폐기한다

- Status: Accepted
- Date: 2026-09-26

## Context

[ADR-0096](./0096-parcel-attributes-are-served-from-pre-baked-r2-objects.md)이
"서빙 정본은 R2 미리구운 객체, PostgreSQL 은 소형 운영 데이터로 축소, 대형
투영 표는 후속 ADR 로 순차 대체"를 결정했다. 그 후속 ADR 이 이 문서다.

**실측 (2026-09-26, ai-server `foundation` DB):**
- postgres 총 크기 **197 GB**. 대형 표는 전부 by-pnu 상세 서빙용 투영이다.
- 필지 상세 R2 엣지 `https://catalog.perfectory.io/parcels/by-pnu/{pnu}` = **HTTP 200**,
  40 KB JSON, `source: gold.parcel_panel`, 안에 토지특성·임야·대지권·공시지가·
  이동이력·용도지역 전부 포함.
- 건물 상세 R2 엣지 `buildings.perfectory.io` / `buildings.gongzzang.app` = **200**,
  건물·층·호·세대가격 포함.
- gongzzang 웹은 필지/건물 상세를 **R2 엣지에서 직접** 읽는다(코드 주석 + ADR-0096).
  foundation-api(postgres)는 공개 호스트에 노출되지 않는다(`/api/...` = 404).
- 즉 대형 표들은 **이미 R2 로 서빙이 넘어갔고, postgres 사본만 안 지운 채 남아** 있다.
  같은 사실을 두 곳이 서빙하는 중복이며, reverse-ETL 안티패턴(ADR-0096)이 그대로다.

R2 굽기가 덮는 표는 코드로 확인했다: `parcel_document.rs` 가 characteristic·forest·
land_right·price·transfer·zoning 을, `building_document.rs` 가 building·floor·unit·
official price 를 합성 객체에 담는다. 지도 타일·마커도 R2(`gold/vector-tiles/releases`)
에서 서빙된다.

## 판정표 (실측 근거)

| postgres 표 | 크기 | R2 굽기가 덮나 | postgres 소비자 | 판정 |
|---|---|---|---|---|
| parcel_transfer_event | 28 GB | ✅ parcel_panel(이동이력) | 상세 API뿐(R2로 이전) | **삭제(1차)** |
| parcel_zoning | 8.7 GB | ✅ parcel_panel(용도지역) | 상동 | **삭제(1차)** |
| parcel_characteristic | 6.8 GB | ✅ parcel_panel(토지특성) | 상동 | **삭제(1차)** |
| parcel_forest_ledger | 6.5 GB | ✅ parcel_panel(임야) | 상동 | **삭제(1차)** |
| parcel_land_right | 6.2 GB | ✅ parcel_panel(대지권) | 상동 | **삭제(1차)** |
| parcel_price | 5.7 GB | ✅ parcel_panel(공시지가) | 상동 | **삭제(1차)** |
| unit_official_price_legacy_year | 5.3 GB | ✅ building_panel(세대가격) | 상동 | **삭제(1차)** |
| unit_official_price | (소) | ✅ building_panel(세대가격) | 상동 | **삭제(1차)** |
| building_unit | 7.5 GB | ✅ building_panel(호) | 상세 API + 정규화 검증 | **삭제(2차)** — 조인 의존 확인 후 |
| building | 1.8 GB | ✅ building_panel(건물) | 상동 | **삭제(2차)** |
| parcel | 9.4 GB | ✅ parcel_panel(기본) | tile/marker/identifier_lookup 조인 | **삭제(2차)** — 지도 경로 확인 후 |
| parcel_identifier_lookup | (중) | — | by-pnu 조인 키 | **삭제(2차)** — parcel 과 함께 |
| vector_tile_*, parcel_marker_anchor | 합계 2 MB 미만 | 타일·마커 **실물은 R2**(gold/vector-tiles/releases); postgres 는 40 kB 제어 포인터만 | foundation-api 타일 경로(catalog.rs)가 런타임에 manifest 를 읽음(실측) | **3차(저우선)** — 비용 무의미. 완전 순수 원하면 제어 포인터를 R2 manifest 로 옮긴 뒤 폐기 |
| industrial_complex (목록 1,442행) | 1.7 MB | 고정 전체 목록이라 R2 파일 하나로 구울 수 있음 | list_complexes 전체 조회 | **R2 이전 가능** — 읽기 전용 고정 목록, 대량 아님(저우선) |
| source_catalog, source_record, ingestion_run, collection_job, bronze_object, outbox_event, outbox_quarantine, publication_revision, catalog_edit, catalog_mutation_idempotency, lakehouse_*(batch_run/registry), schema_profile | (소, 활동량 비례) | ✕ (서빙 데이터 아님) | 수집·발행 워커가 **계속 씀(write)** | **유지** — 파이프라인 제어 상태 |
| normalization_application, normalization_proposal(+review/audit) | (소) | ✕ | AI 제안·직원 승인이 **계속 씀** | **유지** — 승인 워크플로 상태 |
| administrative_unit*, allowed_industry, industry_group* | (소) | 고정 참조라 R2 가능하나 처리 조인에 쓰임 | 정제 조인 | **유지(또는 R2)** — 소형 참조 |
| serving_postgis.parcel_boundary_mirror/publication (지도 경계) | **112 GB** (전국 3,986만, 세대 1개) | ✗ (성격이 다름 — 아래) | Martin 이 Dynamic 렌더로 **실시간 읽음** | **삭제 아님 — 전국 타일의 유일 Dynamic 소스.** 전국 Static R2 PMTiles 완성 시 warm delta 만 남겨 축소(§postgis) |

1차 삭제만으로 **약 67 GB 즉시 회수**(의존 위험 0 — 순수 상세 표). 2차까지 하면
기본 엔티티·식별자까지 약 86 GB+ 회수. 3차(tile/marker 제어 포인터)는 2 MB
미만이라 회수량이 아니라 "완전 무-DB" 순수성 목적이며 저우선이다.

## serving_postgis 의 역할 (catalog 상세와 다르다)

`serving_postgis`(112 GB, parcel_boundary_mirror/publication)는 catalog 상세 표와
**성격이 다르다**. catalog 상세는 R2 구운 JSON 이 이미 대체한 순수 중복이라 삭제
대상이지만, postgis 경계는 **지도 타일의 살아있는 렌더 소스**다.

지도 타일 서빙 모델(runbook `tiles-object-storage-first-slice`, ADR-0004/0006/0042):
- **Dynamic:** PostGIS view → Martin → MVT. **최신 변경(추가·수정·삭제, tombstone
  포함)이 먼저 들어가 즉시 서빙되는 warm 경로.**
- **Static:** 같은 PostGIS generation 을 고정해 불변 PMTiles 로 구워 R2(serving-
  derivative 버킷)에 올리고 Martin 이 거기서 서빙하는 cold 경로. 승격은 하루 한 번
  (`Asia/Seoul`) 스케줄 또는 관리자 반영으로 일어난다.
- 정본(canonical geometry)은 R2 Iceberg(lakehouse silver)다. PostGIS 는 그 스냅숏 +
  감사된 공개 입력에서 **재구축 가능한 warm projection 이며 유일 정본은 아니다**.

즉 흐름은 **경계 변경 → PostGIS(즉시 서빙) → (하루/관리자 반영) → R2 PMTiles 확정**
이고, catalog 처럼 "중복이라 삭제"가 아니라 "최신을 받는 warm 층이라 유지"다.
runbook 명시: "Static serving 은 PostGIS 렌더 부하만 줄이지 warm projection 자체를
제거하지 않는다."

**실측(2026-09-26):** parcel_boundary_mirror = 62 GB / **39,861,511 행**(전국 필지
전량, ADR-0082 의 3,986만과 일치), publication ≈ 50 GB, **data_revision 세대 = 1개**.
즉 옛 세대가 안 지워져 쌓인 블로트가 **아니다** — 딱 한 세대, 전국 전량이다.

**왜 전량인가:** Martin 의 Dynamic 경로가 이 PostGIS 에서 타일을 실시간 렌더하므로,
지도의 어느 필지든 그리려면 전국 도형이 다 있어야 한다("최근 변경분만"이 아니라
"렌더 가능한 전부"). Static(R2 PMTiles) 경로는 runbook 기준 아직 "한 산업단지 3필지"
슬라이스만 증명됐고 전국 미완이라, 전국 타일 부담을 Dynamic(PostGIS)이 통째로 지고
있다. 그래서 112 GB 다.

**따라서 지금은 삭제 불가**(유일한 전국 타일 소스 — 지우면 지도가 깨진다). 다만
종착 아키텍처에서는 이 층이 작아야 한다: **전국 Static R2 PMTiles 발행을 완성하면
PostGIS 는 "아직 확정 안 된 최근 변경분(warm delta)"만 남기고 대폭 축소된다.** 이것이
serving_postgis 축소의 올바른 경로이며, 블로트 정리가 아니라 **Static 타일 전국
롤아웃**이 전제다(후속 작업 [[the-parcel-pipe-reaches-the-map]] 계열).

## Decision

1. **R2 굽기가 완전히 덮고 postgres 소비자가 없는 서빙 투영 표는 폐기한다.**
   각 폐기는 forward-only 마이그레이션(`DROP TABLE`)이며, 되돌리려면 R2 원천에서
   재적재하면 된다(브론즈·실버 정본 불변).

2. **폐기 전 게이트(표마다 확인, 순서 고정):**
   - (a) 해당 사실이 R2 by-pnu 객체에 실려 있음을 실물 GET 으로 확인.
   - (b) postgres 그 표를 읽는 코드가 상세 서빙 경로(R2 로 이전됨)뿐이고,
     검색·조인·발행에 남은 소비자가 없음을 확인.
   - (c) 확인 후에만 DROP 마이그레이션을 병합한다.

3. **1차 (의존 없는 순수 상세 표, 즉시):** parcel_transfer_event, parcel_zoning,
   parcel_characteristic, parcel_forest_ledger, parcel_land_right, parcel_price,
   unit_official_price, unit_official_price_legacy_year. 이들은 다른 카탈로그 표가
   조인하지 않으므로 게이트 (a)(b) 확인 즉시 DROP.

4. **2차 (기본 엔티티·식별자, 지도 경로 확인 후):** building_unit, building,
   parcel, parcel_identifier_lookup. by-pnu 상세는 R2 가 덮지만, tile/marker 발행과
   parcel_identifier_lookup 조인이 postgres 를 런타임에 읽지 않는지 먼저 증명한 뒤
   DROP. 지도 마커가 postgres 를 런타임에 읽으면 그 경로부터 R2 로 옮긴 뒤 폐기.

5. **3차 (저우선, 완전 순수용):** vector_tile_*·parcel_marker_anchor. 타일·마커
   실물은 이미 R2 이고 postgres 에는 40 kB 제어 포인터만 남아 있으나,
   foundation-api 타일 경로가 런타임에 이를 읽는다(실측). 비용이 0 에 가까워
   시급하지 않다. 완전한 무-DB 서빙을 원하면 이 제어 포인터를 R2 manifest 로
   옮겨 타일 경로가 R2 를 읽게 한 뒤 폐기한다.

6. **postgres 에 남는 것은 "쓰기가 계속 일어나는 제어 상태"뿐이다.** 서빙되는
   데이터는 하나도 남기지 않는다. R2 로 못 하는 것은 두 종류뿐이며 이것만 남긴다:
   - (가) **계속 쓰는(write) 운영 상태** — 수집 작업·발행 큐·브론즈 원장·정규화
     제안/승인·레이크하우스 배치 원장·멱등/편집 원장. R2 객체는 "통째 교체"만
     되고 초당 소량 수정에 부적합하므로 이 상태는 DB 가 맞다.
   - (나) **조건이 무한한 즉석 검색** — 미리 구울 수 없는 임의 필터 조회. (단
     gongzzang 매물 검색은 제품 소관이며 이 카탈로그 밖이다.)
   그 외 읽기 전용 고정 목록(industrial_complex, administrative_unit* 등)은
   대량이 아니라 급하지 않으나 원칙상 R2 로 옮길 수 있다.

7. **앞으로의 규칙(중요): 새 대량·서빙 데이터는 postgres 에 적재하지 않는다.**
   수집·정제하는 모든 대량 데이터의 정본은 R2 레이크하우스이고, 서빙은 R2
   미리구운 객체에서 한다. postgres 로의 대량 투영(reverse-ETL)은 다시 만들지
   않는다. 따라서 **postgres 는 데이터 양(필지 수·행 수)에 따라 커지지 않고,
   활동량(진행 중인 작업 수)에만 비례하는 작은 크기로 고정된다.** 이것이 이
   폐기의 종착점이자 [[the-modernization-program]]·ADR-0096 의 완성이다.

8. **적재기도 함께 끈다.** 폐기하는 표의 postgres 투영 적재
   (outbox-publisher 의 `*_catalog_projection_load`)는 더 이상 돌리지 않는다 —
   죽은 표에 다시 붓지 않도록 스케줄/호출에서 제거하고, 그 코드는 폐기 표와 같은
   변경에서 비활성화한다.

## Consequences

- 1차 즉시 ~67 GB, 2차까지 ~86 GB+ 회수. AWS(RDS/Aurora) 이전 시 서빙 DB 인스턴스가
  대폭 작아진다(ADR-0096 비용 근거 실현). ai-server 디스크 압박(240 GB SSD 98% 사고
  이력)도 해소.
- 서빙 정본이 R2 로 단일화되어 reverse-ETL 중복이 사라진다(SSOT 회복).
- 리스크: R2 굽기 배치가 상세의 유일 경로가 되므로, **매일 재굽기 파이프라인의
  안정성**이 곧 상세 가용성이다 — [[execution-ssot-and-scheduling]]의 스케줄 상시화가
  이 폐기의 전제 안전망이다. 굽기가 멈추면 상세가 낡으므로, 폐기는 재굽기 배치가
  안정적으로 도는 것을 확인하며 단계적으로 진행한다.
- 후속: 각 삭제는 개별 PR(마이그레이션 + 적재기 비활성화 + 소비자 제거)로 나간다.
  이 ADR 은 순서와 게이트를 고정하고, 실제 DROP 은 게이트 통과 후 별도 변경이다.
