# ADR 0374: 지도 유닛은 첫 lakehouse 굽기로 태어날 수 있다 — 유닛·첫 릴리스·매니페스트는 한 트랜잭션

- Status: Accepted
- Date: 2026-11-04
- Amends: ADR-0112(lakehouse 굽기는 활성 정적 릴리스를 갈아 끼운다)의 전제 "유닛의 첫 공개는 PostGIS 동적 릴리스"
- Related: [ADR-0108](./0108-retire-the-postgres-serving-projections-superseded-by-r2-bakes.md)(PostGIS 서빙 투영 퇴역),
  [ADR-0323](./0323-parcel-tiles-patch-only-the-tiles-changed-parcels-touch.md)(패치), ADR-0373(규제 구역 타일, 이 결정의 첫 사용자)

## Context

지도 타일 유닛(`catalog.vector_tile_publication_unit`)의 첫 공개는 지금까지 한 길뿐이었다.

1. PostGIS 투영 적재;
2. Martin 동적 릴리스 승격;
3. `publish-*-static-release` 가 Martin 에서 정적 릴리스를 굽는다;
4. 그 뒤에야 `bake-lakehouse-tiles` 가 정적 릴리스를 갈아 끼운다.

이 순서를 승격 함수 `catalog.promote_vector_tile_runtime_manifest` 가 강제한다
(`the first runtime publication must be dynamic PostGIS`, 마이그레이션 일곱 곳이 같은 문장을 다시 쓴다).
같은 함수의 두 규칙이 새 유닛을 더 어렵게 한다.

- **매니페스트는 모든 유닛을 덮어야 한다.** 릴리스 없는 유닛 줄이 하나라도 커밋되면 다른 모든 유닛의 승격이
  `is not a complete publication` 으로 막힌다. 매시 도는 지도 편집 접기(admin·complex)도 함께 멈춘다.
- **정적 릴리스는 릴리스 주소여야 한다**(`<unit>-<release>`, `gold/vector-tiles/releases/<id>.pmtiles`).

새 유닛 `zoning`(규제 구역, ADR-0373)은 Silver 에만 있고 PostGIS 투영이 없다. ADR-0108 은 PostGIS 서빙 투영을
걷어내는 중이다. ADR-0362 §5② 는 "마이그레이션이 유닛과 첫 정적 릴리스를 만든다"고 적었지만, 그 길은 위 두
규칙에 막힌다. 또 서빙 상태를 마이그레이션에 쓰면 환경마다 다른 데이터가 스키마 이력에 들어간다(시험 DB 에는
현재 매니페스트가 없다).

검토한 길:

| 길 | 기각 이유 |
|---|---|
| zoning 에 PostGIS 투영·Martin 경로를 하나 더 만든다 | 퇴역 중인 계층을 늘린다(ADR-0108). 유닛마다 투영 적재 코드가 하나씩 있다(admin·complex·parcels 각자) |
| 빈 PMTiles 를 "씨앗" 릴리스로 올리고 그 위에 첫 굽기를 얹는다 | 빈 지도가 한 번 공개된다. 씨앗의 스냅숏 id 를 지어내야 한다(`^[1-9][0-9]*$`, "이미 그 스냅숏을 서빙한다" 검사와 부딪힌다) |
| 마이그레이션이 유닛·릴리스·매니페스트 줄을 쓴다 | 위 이유. R2 객체 없이 Catalog 만 앞서 간다 |
| 빌드 원장에 "입력 없는 굽기"를 넣는다 | `vector_tile_build_job.input_release_id NOT NULL` 과 그것을 읽는 트리거 셋·승격 코드가 모두 "릴리스는 릴리스를 갈아 끼운다"를 전제한다. 바꿀 곳이 가장 넓다 |

선례: Iceberg·Delta 는 표를 만드는 커밋 자체가 첫 스냅숏이다(빈 표를 먼저 따로 공개하지 않는다).
Mapbox Tiling Service 는 tileset 을 첫 publish 로 만든다(create 와 첫 publish 사이에 빈 tileset 을 내보내지 않는다).

## Decision

1. **유닛 줄에 태어난 길을 적는다.** `catalog.vector_tile_publication_unit.born_from`: `NULL`(첫 공개가 PostGIS
   동적이었다) 또는 `'lakehouse_bake'`. CHECK 가 다른 값을 거부한다.
2. **승격 함수의 규칙 하나만 바뀐다.** `born_from = 'lakehouse_bake'` 인 유닛은 첫 공개를 정적 PMTiles 로 받는다.
   나머지 규칙은 그대로다(함수 본문은 `20260928120000_lakehouse_bake.sql` 의 것을 통째로 다시 쓰고, 그 한 줄만
   다르다): 모든 유닛을 덮는 매니페스트, 릴리스 주소, 새 유닛의 `serving_generation = 1`, 세대 증가.
3. **새 명령 `register_lakehouse_tile_unit`**(멱등 원장의 매니페스트 응답 명령). 한 트랜잭션에서 다른 승격과 같은
   잠금 순서로 다음을 쓴다.
   - 잠금: 포인터 → 모든 유닛.
   - 유닛 줄(`born_from = 'lakehouse_bake'`)을 쓴다. 같은 키의 유닛이 이미 있으면 거부한다. 다음 릴리스는 보통의
     lakehouse 굽기다.
   - `source_record`(`lakehouse:<표>`, 스냅숏 id)와 첫 `publication_revision` 을 쓴다.
   - 재해시까지 끝난 정적 릴리스, 그 레이어, 파일 자산을 쓴다.
   - 나머지 유닛의 선택을 그대로 옮기고 새 유닛을 더한 매니페스트를 쓰고 승격 함수를 부른다.
   - v2 이벤트를 낸다.
   - 같은 키로 다시 부르면 처음 매니페스트로 답한다.
4. **누가 태어날 수 있는지는 계약이 정한다.** `config/tile-bake-containers.contract.json`
   `units_born_from_a_lakehouse_bake.<unit>` 이 그 유닛의 레이어를 적는다: `source_layer`, `feature_id_property`,
   타일·렌더 줌, 정렬된 `feature_filter_properties`, `source_record_source`.
   - `bake-lakehouse-tiles` 는 Catalog 에 그 유닛이 없을 때만 이 레이어로 굽고, 게이트를 돌리고, create-only 로
     올려 재해시한 뒤, 3번 명령으로 등록한다.
   - 처음엔 비교할 이전 릴리스가 없다. 그래서 첫 릴리스 동등성 게이트는 건너뛰고, 증거에 그 이유를 적는다.
     maxzoom id 게이트는 그대로 돈다.
   - 유닛이 생긴 뒤에는 레이어를 Catalog 에서 읽는다. 계약 항목을 고쳐도 태어난 유닛은 바뀌지 않는다.
   - 계약에 없는 유닛이 Catalog 에도 없으면 굽기는 시작 전에 거부한다.
5. **패치 대상 유닛이면 첫 굽기도 지문 색인을 남긴다**(ADR-0323 §1). 그래서 둘째 굽기부터 바로 패치가 된다.

## Consequences

- 새 지도 유닛은 PostGIS 투영 없이 lakehouse 에서 바로 태어난다. 릴리스 없는 유닛 줄이 커밋되는 순간이 없으므로
  매시 접기를 막지 않는다.
- 시험: `catalog-infrastructure` DB 시험 `unit_birth.rs`.
  - 다른 유닛을 그대로 옮긴 매니페스트에 새 유닛이 세대 1로 실린다.
  - 같은 키를 다시 부르면 같은 답이다. 같은 유닛을 다시 등록하면 거부된다.
  - 태어난 릴리스 위에서 보통의 lakehouse 굽기가 시작된다.
  - `born_from` 은 `lakehouse_bake` 만 받는다.
- 어휘 시험(`a_database_vocabulary_is_spelled_the_same_way_in_both_languages`)이 새 명령의 철자를 DB 제약과 맞춘다.
- 위험: 승격 함수 본문을 통째로 다시 쓴다(이 저장소의 선례대로). 앞 판과 다른 줄은 `born_from` 조건 한 줄이다.
