# ADR 0109: 공짱 백엔드는 필지·건물 상세를 R2 엣지에서 직접 읽고 foundation-api 는 by-pnu 상세 서빙에서 손을 뗀다

- Status: Accepted
- Date: 2026-09-27

## Context

[ADR-0108](./0108-retire-the-postgres-serving-projections-superseded-by-r2-bakes.md)
은 필지 상세 6표(+`unit_official_price`)를 "다른 카탈로그 표가 조인하지 않는
순수 상세 표"로 보고 1차 즉시 삭제로 분류했다. 그 게이트 (b)는 "그 표를 읽는
코드가 상세 서빙 경로(R2 로 이전됨)뿐"이라고 전제했다.

**코드 실측(2026-09-27)으로 이 전제가 불완전했음을 확인했다:**
- ADR-0108 은 "공짱 **웹**이 R2 엣지에서 직접 읽는다"만 봤다. 그러나 공짱
  **백엔드(gongzzang-api)**는 R2 엣지가 아니라 foundation-api(postgres) 를 거쳐
  상세를 읽는다.
- `FoundationPlatformParcelInfoLookup`(`foundation_parcel_lookup.rs`)가
  `GET {FOUNDATION_PLATFORM_API_BASE_URL}/catalog/v1/parcels/by-pnu/{pnu}` 를
  호출해 characteristics·zonings·price·forest_ledger·transfer_history·land_rights
  6종을 전부 소비한다.
- 죽은 코드가 아니라 살아있는 라우트에 물려 있다: 매물 생성
  (`listings/mutation/create.rs`)의 필지정보 부착, 필지 조회(`parcels.rs`).
  `startup.rs` 기준 운영=foundation-api 읽기, dev=NoOp.
- 건물/호/층도 마찬가지: `building_reader.rs` 가 같은 base_url 로 건물·호·
  `official_price_history` 를 읽는다.

따라서 ADR-0108 의 "소비자 없음" 게이트를 이 표들은 통과하지 못한다. **표를
지우려면 먼저 공짱 백엔드를 R2 엣지로 옮겨야 한다.**

**R2 엣지가 같은 사실을 이미 서빙함을 실측:**
- 필지: `GET https://catalog.perfectory.io/parcels/by-pnu/{pnu}` = 200, 40KB,
  `source.table = gold.parcel_panel`(Iceberg). 객체 스키마가 공짱의
  `CatalogParcelResponse` DTO 와 **필드명·타입이 그대로 맞는다**(zonings.anchor_code·
  price·characteristics·forest_ledger·transfer_history·land_rights·
  land_right_total 전부 존재, R2 의 여분 필드 schema_version·source 는 serde 가
  무시). 즉 **DTO 그대로 역직렬화된다.**
- 건물: `GET https://buildings.perfectory.io/buildings/by-pnu/{pnu}` = 200, 93KB.
  단, 모양이 다르다 — pnu 당 **복합 객체**(`buildings[].floors[]`·
  `buildings[].units[]`·`unlinked_units[]`)이고, 공짱은 건물목록 조회 + 건물별 호
  페이지네이션(cursor) 두 호출로 쓴다. 또 `CatalogBuildingResponse.updated_at` 이
  필수인데 R2 객체엔 없다.
- 엣지는 공개 CDN(무인증, `catalog/v1` 프리픽스 없음). 공짱 클라의
  `FoundationCatalogClient` 는 auth bearer 와 `catalog/v1` 프리픽스를 붙이므로
  그대로는 못 쓴다.

## Decision

1. **공짱 백엔드는 필지·건물 상세를 R2 엣지에서 직접 읽는다.** foundation-api 는
   by-pnu 상세 서빙에서 손을 뗀다 — 웹과 백엔드가 같은 서빙면(R2 엣지) 하나를
   쓰고, postgres 역-ETL 상세 사본과 그 라우트·적재기는 폐기한다(ADR-0096·0108
   종착).

2. **모양 차이 때문에 단계로 나눈다:**
   - **1단계 (필지, 깨끗함, ~62GB 회수):** 공짱 필지 조회를 R2 엣지
     `catalog.perfectory.io/parcels/by-pnu/{pnu}` 로 옮긴다(DTO 그대로 맞음 —
     전용 무인증 R2 리더 + 기존 `parcel_info_from_response` 재사용). foundation-api
     의 필지 by-pnu 상세 라우트와 필지 6표 적재기·repo·port·domain·DTO 를
     은퇴시킨다. 그 뒤 6표 DROP: `parcel_transfer_event`·`parcel_zoning`·
     `parcel_characteristic`·`parcel_forest_ledger`·`parcel_land_right`·
     `parcel_price`.
   - **2단계 (건물·호, 재구성 필요, 회수 0):** 공짱 건물 리더를 R2 복합 객체
     (`buildings.perfectory.io/buildings/by-pnu/{pnu}`)로 옮긴다 — `updated_at`
     부재와 "페이지네이션 → 복합 객체" 전환을 흡수. foundation-api 건물·호 상세
     라우트와 unit 적재기를 은퇴. `unit_official_price`(+`_publication`·트리거·
     함수) DROP. (`unit_official_price` 는 현재 0행이라 디스크 회수는 없고 정리
     목적.)

3. **폐기 순서·게이트는 ADR-0108 을 따른다:** (a) R2 객체가 사실을 실은 것을
   실물 GET 으로 확인(완료), (b) 공짱 소비자를 R2 로 옮겨 postgres 소비자가 0 이
   됨을 확인한 뒤에만 (c) DROP.

4. **적재기도 함께 끈다(ADR-0108 §8):** 폐기 표의 `*_catalog_projection_load` 를
   CLI/dispatch/모듈에서 제거한다.

5. **foundation-api 는 by-pnu 상세를 다시 서빙하지 않는다.** 상세 서빙 정본은 R2
   엣지 하나다. foundation-api 에 남는 카탈로그 읽기는 상세가 아닌 것(예: 산업단지
   목록·complex-by-lakehouse-id)뿐이며, 그 역시 원칙상 R2 로 옮길 수 있으나 이 ADR
   범위 밖이다.

## Consequences

- 1단계로 ai-server 루트 디스크에서 ~62GB 즉시 회수(6표). 2단계는 회수 0(정리).
- 서빙면이 R2 엣지 하나로 단일화 — 웹·백엔드가 같은 객체를 읽어 역-ETL 중복과
  SSOT 위반이 사라진다.
- 공짱 상세 가용성 = R2 엣지(= 매일 재굽기 배치) 안정성. 웹은 이미 그 의존을 갖고
  있었고, 백엔드도 같은 의존으로 통일된다. 재굽기 스케줄 상시화
  ([[execution-ssot-and-scheduling]])가 안전망이다.
- 리스크·순서: 공짱 운영(AWS)이 지금 foundation-api 상세 계약에 의존하므로, 코드
  계약 은퇴(라우트·DTO 필드 제거)는 공짱 R2 리포인트와 같은 릴리스에서 맞춰
  내보낸다. dev(ai-server)는 공짱이 NoOp 이라 DROP 자체로는 안 깨지지만, 배포와
  마이그레이션(DROP)을 한 번에 올려 낡은 코드가 사라진 표를 읽는 창을 없앤다.
- 후속: 각 단계는 개별 PR. 이 ADR 은 방향·단계·게이트를 고정하고, 실제 DROP 은
  게이트 통과 후의 변경이다.
