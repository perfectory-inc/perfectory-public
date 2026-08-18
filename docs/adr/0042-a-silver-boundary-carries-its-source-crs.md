# ADR 0042: Silver 경계는 원천 CRS 를 그대로 싣는다

- Status: Accepted
- Date: 2026-08-18
- 관련: [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md), [ADR-0020 기하는 어떤 사실의 증거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md)

## Context

`silver.industrial_complex_boundaries` 는 19칸이 정의된 채로 소비자만 있고 생산자가 없었다.
`gold.complex_catalog.boundary_object_key` 와 `calculated_area_sqm` 이 1,442곳 전부 NULL 인 것이
그 결과다. 이 결정은 그 생산자를 붙이면서 마주친 하나의 갈림길에 대한 것이다.

원천은 `sandan_boundary.zip` 의 `DAM_DAN.shp` 이고, 그 `.prj` 는 다음을 선언한다.

```
PROJCS["Korea_2000_Korea_Central_Belt_2010", ... Transverse_Mercator,
       False_Easting 200000.0, False_Northing 600000.0,
       Central_Meridian 127.0, Latitude_Of_Origin 38.0, UNIT["Meter",1.0]]
```

즉 **EPSG:5186, 미터 단위**다. 좌표 범위도 x 123368..428422, y 73849..642792 로 미터다.

계약의 품질 게이트는 `geometry_srid = 4326` 이었고, 같은 문장이
`docs/catalog/industrial-complex-lakehouse-poc.md` 에도 "초기 기본값 4326",
"Silver 에서 EPSG:4326 으로 정규화한다"로 두 번 더 적혀 있었다. 그 문장을 지키려면 적재 경로
어딘가에서 TM 역투영을 해야 한다.

**할 수 있는 곳이 없다.** 이 저장소의 Rust 워크스페이스에도, Spark job 이 도는 이미지에도
투영 라이브러리가 없다(저장소 루트 `docs/technology-stack.md` §1.1 채택 명단에 없다). 남는 선택은 손으로 짠
역투영뿐이고, 그것이 틀리면 결과는 검출되지 않는다 — 좌표는 여전히 유효한 위경도 범위 안에
있고, bbox 정렬 검사도 체크섬 검사도 통과하며, 산업단지만 엉뚱한 자리에 놓인다.

미터라는 사실은 그 자체로 값이 있다. `area_sqm_calculated` 를 신발끈 공식으로 그 자리에서
정확히 낼 수 있고, 그것이 지금 Gold 에서 NULL 인 `calculated_area_sqm` 을 채운다. 4326 으로
먼저 바꾸면 면적을 재기 위해 다시 투영해야 한다.

## Decision

1. **Silver 경계 표는 원천 CRS 를 변환 없이 싣고 `geometry_srid` 로 선언한다.**
   `silver.industrial_complex_boundaries` 의 품질 게이트를 `geometry_srid = 5186` 으로 바꾼다.
   계약 JSON(`infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`)과
   `lakehouse-domain/src/lakehouse.rs` 의 정적 거울을 함께 고친다 —
   `lakehouse_contract_artifact` 테스트가 둘이 어긋나면 실패한다.

2. **CRS 는 표마다 선언한다. 레이크하우스 전역 기본값은 없다.**
   `silver.parcel_boundaries` 는 `geometry_srid = 4326` 그대로 둔다. 그 표의 원천은 V-World
   GeoJSON 이고 이미 4326 이므로, 지킬 수 있는 게이트다. 게이트가 표마다 다른 값을 말하는 것은
   불일치가 아니라 원천이 다르다는 사실이다. `geometry_srid` 컬럼이 존재하는 이유가 이것이다.

3. **재투영은 서빙 가장자리에서 PostGIS `ST_Transform` 이 한다.**
   거기에는 proj 가 있고, `serving_postgis.administrative_unit_boundary_publication` 이 이미
   4326 을 요구하는 형태다. 정규화 계층은 투영을 하지 않는다.

4. **`docs/catalog/industrial-complex-lakehouse-poc.md` 의 "Silver 에서 4326 으로 정규화한다"를
   철회한다.** 그 문서는 이제 표별 `quality_gates` 를 정본으로 가리킨다.

## Consequences

- 산업단지 경계를 지도에 그리는 소비자는 4326 을 그냥 받지 못한다. PostGIS mirror 를 거치거나
  스스로 `ST_Transform(geom, 4326)` 을 불러야 한다. 이것은 비용이지만, 틀린 좌표를 조용히
  받는 것보다 싸다.
- `area_sqm_calculated` 는 미터 좌표에서 직접 나오므로 재투영 오차가 없다. 공식 면적
  (`official_area_sqm`) 과 다를 수 있고 **다른 것을 재는 값이다** — 지정 면적과 도형 면적은
  같아야 할 이유가 없다. 어느 쪽도 다른 쪽으로 맞추지 않는다.
- 남은 일: 서빙 경로에서 이 표를 4326 으로 내보내는 발행기는 아직 없다.
  `gold.complex_catalog.boundary_object_key` 를 채우는 단계에서 다뤄야 한다.
- 이 결정은 "정규화 계층은 자기가 못 하는 변환을 계약으로 약속하지 않는다"의 한 사례다.
  게이트가 코드가 할 수 없는 것을 말하고 있으면, 고칠 것은 코드가 아니라 게이트다.
