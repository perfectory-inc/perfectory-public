# ADR 0194: R2가 이미 가진 PostgreSQL 사본 다섯 표를 지운다

- Status: Accepted
- Date: 2026-10-12
- Builds on: [ADR-0108](./0108-retire-the-postgres-serving-projections-superseded-by-r2-bakes.md),
  [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §8·§9,
  [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md) §10

## Context

레이크하우스(R2·Iceberg)가 정본이고 PostgreSQL은 다시 만들 수 있는 투영이다. 2026-10-12 운영 DB `foundation`을 쟀다.

- **DB 전체는 130GB**이다. 아래 다섯 표가 거의 전부를 차지한다.

  | 표 | 크기 | 같은 사실을 서비스하는 곳 |
  |---|---|---|
  | `serving_postgis.parcel_boundary_mirror` | 62GB | 필지 지도 R2 정적 PMTiles(`static_pmtiles`) |
  | `serving_postgis.parcel_boundary_publication` | 50GB | 상동 |
  | `catalog.parcel` | 9.4GB | 필지 엣지 묶음(by-PNU, R2) |
  | `catalog.building_unit` | 7.5GB | 건물 엣지 묶음(by-PNU, R2) |
  | `catalog.building` | 1.8GB | 상동 |

- 컨테이너가 10일째 떠 있는 동안 다섯 표 모두 `pg_stat_user_tables`의 seq/idx scan이 0이고, insert·update도 0이다.
- 서버 루트 디스크는 85%를 쓰고 있다.
- `catalog.parcel`을 가리키는 FK를 가진 표 10개는 운영에서 모두 0행이다. 대상은 `building`, `building_unit`,
  `parcel_identifier`, `parcel_administrative_unit`, `parcel_complex_membership`, `parcel_marker_anchor`,
  `manufacturer`, `parcel_industry_assignment`, `spatial_layer`, `digital_twin_asset`이다.
- 매일 밤 pgBackRest가 R2에 백업한다(암호화). 마지막 전체 백업 2026-10-11은 139.7GB로 다섯 표를 담고 있다.
  보존 기간은 35일이다.

코드에서 이 표들을 읽고 쓰는 곳은 다음과 같다. Airflow DAG, systemd 유닛, ops 스크립트 가운데 이 표들을
부르는 것은 없다.

- **필지 경계 두 표.**
  - 쓰기: 손으로 돌리는 publisher 명령이다.
    - `rebuild-postgis-parcel-boundary-mirror*`
    - `publish-parcel-boundary-postgis`
    - `publish-parcel-boundary-static-release`
    - `promote-parcel-boundary-runtime`
    - `seal-parcel-publication-evidence`
  - 읽기: `rebuild-parcel-marker-anchors` 명령, `POST /catalog/v1/parcel-marker-anchors:rebuild` 경로, Martin `parcels`
    소스.
  - 막는 것: 새 필지판을 굽는 유일한 길이 아직 이 왕복이다. ADR-0133의 레이크하우스 오븐은 코드가 병합됐다
    (§2–§5). 그러나 전국으로 돌린 적이 없고, 예약 작업 `parcel_tile_bake`(§7)도 없다.
- **catalog 세 표.**
  - 쓰기: `load-parcel-catalog-projection`, `load-building-catalog-projection`,
    `load-building-unit-catalog-projection`, `load-building-unit-building-link`. 그리고 이들에게 넘기는 Spark
    handoff 작업.
  - 읽기: foundation-api 경로 여덟 개다(아래 2번).
    - 이 중 둘(`parcels/by-pnu/{pnu}/buildings`, `buildings/{id}/units`)은 gongzzang-api `/api/buildings`,
      `/api/buildings/{id}/units`가 중계한다.
    - 그러나 웹은 이 중계를 부르지 않는다. 건물·호를 R2 엣지(`building-edge`)에서 읽는다.
    - 나머지 여섯 경로는 부르는 클라이언트가 없다.

## Decision

1. **다섯 표를 지운다.** 정본은 R2이고, 지운 뒤 다시 필요하면 레이크하우스에서 다시 만든다. 이력 원장
   `spatial_projection_load`·`parcel_boundary_mirror_rebuild_run`은 남긴다(ADR-0133 §9).
2. **catalog 세 표의 읽기 경로를 지운다.** 대체는 R2 엣지 묶음이고, 지금 이미 그렇게 서비스한다.
   - foundation-api에서 지우는 경로:
     - `GET /catalog/v1/parcels/{id}`
     - `GET /catalog/v1/parcels/by-pnu/{pnu}/buildings`
     - `GET /catalog/v1/parcels/by-pnu/{pnu}/units`
     - `GET /catalog/v1/buildings/{id}`
     - `GET /catalog/v1/buildings/{id}/units`
     - `GET /catalog/v1/buildings/by-register-pk/{register_pk}`
     - `GET /catalog/v1/complexes/{id}/anchor-summary`
     - `PATCH /catalog/v1/parcels/{id}/kind`
   - gongzzang-api에서 지우는 중계: `/api/buildings`, `/api/buildings/{id}/units`.
   - 다른 표를 읽는 경로(디지털 트윈·공간 레이어·산업 배정 등)는 이 ADR의 범위가 아니다.
3. **catalog 세 표의 쓰기 경로를 지운다.**
   - 적재 명령 넷: `load-parcel-catalog-projection`, `load-building-catalog-projection`,
     `load-building-unit-catalog-projection`, `load-building-unit-building-link`.
   - 그 명령에만 넘기는 Spark handoff 작업.
   - pipeline graph의 `serving-parcel`·`serving-building`·`serving-building-unit` 노드와 간선.
4. **필지 경계 두 표는 ADR-0133 순서를 따른다.**
   1. 필지 서빙본과 굽기를 예약 작업으로 만든다(§7). 이 작업은 `bake-lakehouse-tiles`를 `parcels` 유닛으로 돌린다.
   2. 첫 전국판을 굽는다. 동등성 검사(ADR-0135)를 통과하면 승격한다.
   3. 마커 앵커는 R2 Silver에서 다시 만드는 `rebuild-parcel-marker-anchors-streaming`만 남긴다.
   4. 미러를 읽는 `rebuild-parcel-marker-anchors` 명령과 HTTP 경로를 지운다(§8).
   5. PostGIS 게시·정적판·승격 명령과 Martin `parcels` 소스 설정을 지운다(§9).
5. **표를 지우는 마이그레이션은 마지막에, 그리고 증명 뒤에만 병합한다.** 아래가 모두 참이어야 병합한다.
   - 읽기·쓰기 코드를 지운 release가 운영에 깔려 있다.
   - 그 release 이후 운영에서 다섯 표의 scan·insert·update 증가가 0이다. `pg_stat_user_tables`의 값을 PR에 적는다.
   - 표를 지우기 전 마지막 pgBackRest 전체 백업이 성공했고, 그 백업에 다섯 표가 들어 있다. 백업 크기를 PR에
     적는다.
   - 필지 경계 두 표는 여기에 더해 4번이 운영에서 확인돼야 한다. 즉 필지 활성 release가 `lakehouse_bake`이고
     fallback이 manifest에서 빠졌어야 한다.
   - 실행 시점에 사장(coordinator) 확인을 받는다(ADR-0133 §9).

   catalog 세 표와 필지 경계 두 표는 준비되는 시점이 다르다. 그래서 마이그레이션을 둘로 나눌 수 있다.
6. **FK와 뷰.**
   - `catalog.parcel`·`catalog.building`을 가리키는 FK 제약은 표와 함께 지운다. 참조하던 칸은 그대로 둔다
     (모두 0행).
   - 이 표들만 읽는 뷰·함수·트리거도 함께 지운다. 대상은 `parcel_identifier_lookup`, `parcel_current_identifier`,
     `parcels_missing_temporal_identifier`, `publish_parcel_identifier`, `parcel_boundary_current`, 그리고 가드
     트리거다.
7. **되돌리기.**
   - **코드 단계(2–4번):** PR을 되돌리면 된다. 표와 데이터는 아직 있다.
   - **표를 지운 뒤:**
     - 첫째 길: 35일 안이면 pgBackRest 백업에서 옆 인스턴스로 복원하고, 필요한 표만 옮긴다.
     - 둘째 길: 언제든 레이크하우스 Silver에서 다시 만든다. 지운 적재 명령은 git 이력에 있다.
     - 서비스는 어느 쪽도 기다리지 않는다. 손님 경로는 처음부터 R2만 읽기 때문이다.

## Consequences

- DB가 130GB에서 10GB 아래로 준다. 서버 루트 디스크가 약 130GB 비고, 매주 전체 백업도 그만큼 준다.
- 필지 지도는 PostGIS 손 왕복(약 2.5시간) 대신 예약된 레이크하우스 굽기로 새로워진다.
- foundation-api의 catalog 읽기 경로가 줄어든다. gongzzang-api의 건물 중계 둘이 사라진다. 웹은 바뀌지 않는다.
- 첫 전국 필지 굽기는 서버 자원을 쓴다. Spark 서빙본과 tippecanoe를 돌린다. 시간·메모리·디스크를 런북에 남긴다
  (ADR-0133 §7).
