# ADR 0045: 서빙 투영의 행은 리비전이 아니라 적재를 이름한다

- Status: Accepted
- Date: 2026-08-19

## Context

산업단지 경계 폴리곤 1,343행이 `silver.industrial_complex_boundaries` 에 실물로 있고,
`serving_postgis` 에는 그 표가 없었다. 서빙으로 나가는 길을 낼 때 기준으로 삼을 선례는
`serving_postgis.administrative_unit_boundary_publication`(마이그레이션 `20260727000002`)이다.

그런데 그 선례의 칸 구성은 이후 두 마이그레이션이 바꿔 놓은 세계와 어긋난 부분이 있다.

1. `20260731000001` 이 `serving_postgis.spatial_projection_load` 를 만들면서 발행 표의 키를
   리비전에서 **적재(load)** 로 옮겼다. 그 적재 원장이 `data_revision` 과
   `canonical_iceberg_snapshot_id` 를 싣는다. 행정경계 표가 같은 두 칸을 여전히 들고 있는 것은
   그 표가 원장보다 먼저 만들어졌기 때문이지, 행마다 그 값이 필요해서가 아니다.
   `20260731000002` 는 같은 이유로 `spatial_projection_load.publication_unit_key`(텍스트 사본)를
   지우고 외래키로 바꾸면서, 두 철자가 어긋나지 않게 하라는 주석은 외래키의 일이라고 적었다.
2. `administrative_unit_boundary_publication.properties` (jsonb) 를 읽는 것이 없다. 자기
   `_current` 뷰도, 슬라이스 증명도 읽지 않는다. 타일 속성의 정의는
   `catalog_domain::vector_tile_feature_filter_properties` 하나다.
3. `catalog.promote_vector_tile_runtime_manifest` 는 매니페스트가 **모든** 발행 단위를 고르지
   않으면 승격을 거부한다. 따라서 릴리스가 생기기 전에 단위 행이 존재하면, 이미 승격을 마친
   배포에서 *다른* 단위의 다음 승격이 실패한다.

## Decision

1. `serving_postgis.industrial_complex_boundary_publication` 은 `projection_load_id` 를
   `NOT NULL` 외래키로 싣고, `data_revision` 과 `canonical_iceberg_snapshot_id` 는 **싣지
   않는다.** 두 값은 적재 원장이 소유하고, 원장은 `catalog.publication_revision` 로 외래키가
   걸려 있다. 기하 행마다 사본을 두면 같은 지식이 두 곳에 살고, 그 사본에는 외래키를 걸 수 없다.
2. 발행 표에 `properties` jsonb 를 두지 않는다. 뷰는 타입이 있는 칸을 읽고, 공개 속성의 정의는
   위의 한 곳이다.
3. `complex` 발행 단위는 마이그레이션이 시드하지 않는다. `ensure_administrative_unit` 이
   `admin` 을 만드는 자리와 같이, 발행 커맨드가 적재를 여는 트랜잭션 안에서 만든다.
4. `complex_id` 는 UUIDv5 형태 CHECK 를 갖는다(ADR-0043 과 같은 근거). 외래키는 걸지 않는다 —
   `catalog.industrial_complex.lakehouse_complex_id` 의 유일성은 **부분** 유니크 인덱스이고,
   PostgreSQL 은 부분 인덱스를 참조할 수 없다.
5. `boundary_kind` 는 `'official'` 하나만 허용한다. 나머지 세 값은 생산자가 없다.

### 기각한 대안

- **행정경계 표를 칸 단위로 복제**: 선례를 그대로 베끼면 리뷰가 쉬워지지만, `20260731000002`
  헤더가 길게 논증하고 실제로 제거한 중복을 새 표에 다시 들여온다.
- **마이그레이션에서 단위 시드**: 결정 3의 반대. 실제로 시도하면
  `administrative_boundary_publication` 의 승격 테스트가 빨개진다 — 승격 게이트가 단위 개수를
  비교하기 때문이다. 이는 이 결정의 기계적 증거다.
- **`geom` 의 다이제스트를 저장**: `geom` 은 PostGIS 가 만든 4326 재투영 결과라 비교할 짝이
  아무 데도 없다. 대신 Silver 가 5186 WKB 에 대해 계산한 체크섬을 싣되, 발행기가 디코드한
  바이트로 **다시 계산해 대조한 뒤** 쓴다.

## Consequences

- `publish-industrial-complex-boundary-postgis` 가 이 표를 쓰는 유일한 생산자다. 적재를 지나
  리비전·스냅샷을 찾으려면 조인 한 번이 더 필요하다. 그 조인은 `_current` 뷰가 이미 하고 있다.
- `complex` 단위가 생기는 순간부터 모든 런타임 매니페스트가 그 단위를 골라야 한다. 즉 T14 가
  Martin 레이어와 승격을 붙이기 전까지, 이 커맨드를 돌린 데이터베이스에서는 다른 단위의 승격도
  `complex` 를 포함해야 한다. 이는 게이트의 의도된 성질이며, 커맨드를 돌리지 않은 배포는 영향이
  없다.
- 폴리곤이 없는 단지 98곳은 이 표에 행이 없다. `gold.complex_catalog.calculated_area_sqm` 를
  채우는 쪽은 이 표의 `area_sqm_calculated` 를 읽되, 없는 단지를 0 으로 읽어서는 안 된다.
