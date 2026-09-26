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
| vector_tile_*, parcel_marker_anchor | (소) | 타일=R2 서빙 | 발행기 빌드타임 | **유지/검증** — 런타임 미독 확인까지 |
| industrial_complex, source_catalog, ingestion_run, collection_job, bronze_object, outbox_event, administrative_unit* | (소) | — | 검색·수집·발행 운영 | **유지** (ADR-0096 §5 소형 운영) |

1차 삭제만으로 **약 67 GB 즉시 회수**(의존 위험 0 — 순수 상세 표). 2차까지 하면
기본 엔티티·식별자까지 약 86 GB+ 회수.

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

5. **유지:** 검색·수정·조인·수집·발행이 필요한 소형 운영 데이터(ADR-0096 §5) —
   industrial_complex, source/ingestion/collection/bronze/outbox, administrative_unit*,
   그리고 (검증 전까지) vector_tile_*·parcel_marker_anchor.

6. **적재기도 함께 끈다.** 폐기하는 표의 postgres 투영 적재
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
