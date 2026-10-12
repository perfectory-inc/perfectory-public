# ADR 0194: R2가 이미 가진 PostgreSQL 사본 다섯 표를 지운다

- Status: Accepted
- Date: 2026-10-12
- Builds on: [ADR-0108](./0108-retire-the-postgres-serving-projections-superseded-by-r2-bakes.md),
  [ADR-0133](./0133-parcel-tiles-are-rebaked-nationally-from-the-lakehouse.md) §7·§8·§9,
  [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md) §10

## Context

레이크하우스(R2·Iceberg)가 정본이고 PostgreSQL은 다시 만들 수 있는 투영이다. 2026-10-12 운영 DB `foundation`을 쟀다.

- **DB 전체는 130GB**이다. 아래 다섯 표가 거의 전부를 차지한다. 다섯 표는 서로 다른 디스크에 있다.

  | 표 | 크기 | 놓인 곳 | 같은 사실을 서비스하는 곳 |
  |---|---|---|---|
  | `serving_postgis.parcel_boundary_mirror` | 62GB | tablespace `parcel_bulk`(`/data`) | 필지 지도 R2 정적 PMTiles(`static_pmtiles`) |
  | `serving_postgis.parcel_boundary_publication` | 50GB | tablespace `parcel_bulk`(`/data`) | 상동 |
  | `catalog.parcel` | 9.4GB | `pg_default`(루트 디스크) | 필지 엣지 묶음(by-PNU, R2) |
  | `catalog.building_unit` | 7.5GB | `pg_default`(루트 디스크) | 건물 엣지 묶음(by-PNU, R2) |
  | `catalog.building` | 1.8GB | `pg_default`(루트 디스크) | 상동 |

  `pg_default` 전체가 18GB이므로 catalog 세 표를 빼면 남는 DB는 1GB 안팎이다.
- 통계가 쌓이기 시작한 2026-10-02 이후 10일 동안 다섯 표 모두 `pg_stat_user_tables`의 seq/idx scan이 0이고,
  insert·update도 0이다.
- 디스크: 루트는 86%(31GB 남음), `/data`는 33%(1.3TB 남음)이다. 필지 경계 두 표를 지우면 `/data`가 112GB 빈다.
  루트를 비우는 것은 catalog 세 표(약 19GB)뿐이다.
- `catalog.parcel`을 가리키는 FK를 가진 표 10개는 운영에서 모두 0행이다. 대상은 `building`, `building_unit`,
  `parcel_identifier`, `parcel_administrative_unit`, `parcel_complex_membership`, `parcel_marker_anchor`,
  `manufacturer`, `parcel_industry_assignment`, `spatial_layer`, `digital_twin_asset`이다.
- 매일 밤 pgBackRest가 R2에 백업한다(암호화). 마지막 전체 백업 2026-10-11은 139.7GB로 다섯 표를 담고 있다.
  보존 기간은 35일이다. **이 백업에서 복원해 본 적은 아직 없다.**

### 필지 경계 두 표를 읽고 쓰는 곳

Airflow DAG와 systemd 유닛 가운데 두 표를 부르는 것은 없다. 나머지는 모두 손으로 돌리는 명령·스크립트다.

- 쓰기(publisher 명령):
  - `rebuild-postgis-parcel-boundary-mirror*` (`postgis_parcel_boundary_mirror_rebuild.rs`,
    `postgis_parcel_boundary_mirror_national_rebuild.rs`)
  - `publish-parcel-boundary-postgis`, `publish-parcel-boundary-static-release`,
    `promote-parcel-boundary-runtime`, `seal-parcel-publication-evidence`
    (`parcel_publication_evidence_sealer.rs`·`_writer.rs`)
- 읽기:
  - 마커 앵커: `rebuild-parcel-marker-anchors` 명령과 `POST /catalog/v1/parcel-marker-anchors:rebuild` 경로
    (`catalog-infrastructure` `parcel_marker_anchor_rebuild.rs`).
  - 검증·증거 명령: `parcel_projection_digest.rs`(두 표를 직접 SELECT), `postgis_mirror_dlq_cutover_evidence.rs`,
    `postgis_anchor_pbf_regional_proof_check.rs`, `parcel_catalog_projection_load.rs`.
  - Martin `parcels` 소스와 `parcel_boundary_current` 뷰.
  - 시험·로컬 도구: `scripts/tiles/tiles-slice-proof.sh`·`fixture.sql`·`martin-dynamic.yaml`
    (`parcel_boundary_current`), `tools/xtask/src/main.rs`의 시험 `parcel_boundary_publication`,
    `foundation-outbox-publisher/tests/parcel_boundary_publication.rs`·`parcel_publication_source_evidence.rs`,
    `foundation-api/tests/tiles_slice_contract.rs`·`local_vector_tile_seed_contract.rs`·`deploy_contract.rs`,
    `infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql`, `infra/compose/grant-foundation-runtime.sql`,
    `scripts/guard/build-coupling-baseline.sh`.
  - 데이터 지도: pipeline graph의 `serving-parcel-boundary` 노드와 그 간선.
- **남길 대체 경로가 지울 모듈에 묶여 있다.** R2 Silver에서 앵커를 만드는
  `parcel_marker_anchor_streaming_rebuild.rs`와 `parcel_marker_anchor_artifact_export.rs`가 지울
  `postgis_parcel_boundary_mirror_national_rebuild.rs`의 `ExecutionEvidence`·`HandoffObject`·`parse_stage_row`·
  `push_copy_csv_row`·`read_execution_evidence`·`validate_source_snapshot_id`를 가져다 쓴다.
- **대체 경로는 운영에서 돈 적이 없다.** 운영 `parcel_marker_anchor`는 0행이다. ADR-0133의 레이크하우스 오븐은
  코드가 병합됐지만(§2–§5) 전국으로 돌린 적이 없고, 예약 작업 `parcel_tile_bake`(§7)도 없다. 그래서 새 필지판을
  굽는 길은 아직 PostGIS 왕복뿐이다.

### catalog 세 표를 읽고 쓰는 곳

- 쓰기: `load-parcel-catalog-projection`, `load-building-catalog-projection`,
  `load-building-unit-catalog-projection`, `load-building-unit-building-link`. 그리고 이들에게 넘기는 Spark
  handoff 작업.
- 읽기: foundation-api 경로 여덟 개다(Decision 2번).
  - 이 중 둘(`parcels/by-pnu/{pnu}/buildings`, `buildings/{id}/units`)은 gongzzang-api `/api/buildings`,
    `/api/buildings/{id}/units`가 중계한다.
  - 그러나 웹은 이 중계를 부르지 않는다. 건물·호를 R2 엣지(`building-edge`)에서 읽는다.
  - 나머지 여섯 경로는 부르는 클라이언트가 없다.
- 문서: 웹 `preview-floors/page.tsx` 각주가 `catalog.building / building_unit`을 출처로 적는다.

## Decision

1. **다섯 표를 지운다.** 정본은 R2이고, 지운 뒤 다시 필요하면 레이크하우스에서 다시 만든다. 이력 원장
   `spatial_projection_load`·`parcel_boundary_mirror_rebuild_run`은 남긴다(ADR-0133 §9).
2. **순서는 셋이다. 앞 단계가 운영에서 확인되기 전에는 다음으로 가지 않는다.**
   1. **공용 부분 분리.** 앵커 두 모듈이 쓰는 핸드오프·증거 타입과 함수(`ExecutionEvidence`, `HandoffObject`,
      `parse_stage_row`, `push_copy_csv_row`, `read_execution_evidence`, `validate_source_snapshot_id`)를 미러
      재구축과 무관한 공용 모듈로 옮긴다. 동작은 바꾸지 않는다. 이후 미러 모듈을 지워도 대체 경로가 남는다.
   2. **catalog 세 표 먼저.** 필지 경계와 독립이고, 대체(R2 엣지 묶음)가 이미 서비스 중이며, 루트 디스크를 비우는
      것은 이쪽뿐이다. 읽기 경로 → 쓰기 경로 → DROP 마이그레이션 순서다(3·4·6번).
   3. **필지 경계 두 표는 대체 굽기가 운영에서 한 번 돈 뒤에만.** 5번 순서를 따른다.
3. **catalog 세 표의 읽기 경로를 지운다.** 대체는 R2 엣지 묶음이고, 지금 이미 그렇게 서비스한다.
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
   - 웹 `preview-floors/page.tsx` 각주의 출처를 R2 엣지 묶음으로 고친다.
   - 다른 표를 읽는 경로(디지털 트윈·공간 레이어·산업 배정 등)는 이 ADR의 범위가 아니다.
4. **catalog 세 표의 쓰기 경로를 지운다.**
   - 적재 명령 넷: `load-parcel-catalog-projection`, `load-building-catalog-projection`,
     `load-building-unit-catalog-projection`, `load-building-unit-building-link`.
   - 그 명령에만 넘기는 Spark handoff 작업.
   - pipeline graph의 `serving-parcel`·`serving-building`·`serving-building-unit` 노드와 간선.
5. **필지 경계 두 표는 ADR-0133 순서를 따른다.**
   1. 필지 서빙본과 굽기를 예약 작업으로 만든다(§7). 이 작업은 `bake-lakehouse-tiles`를 `parcels` 유닛으로 돌린다.
      작업 폴더는 계약(`config/tile-bake-containers.contract.json`의 `work_disk.production_root`)대로 `/data`에
      두고, Spark 서빙본의 `spark.local.dir`도 `/data` 아래에 둔다(`gold-panel-rebuild.sh`와 같은 방식). 루트
      디스크에는 쓰지 않는다.
   2. **첫 전국판을 운영에서 굽고 디스크를 잰다.** 오븐은 시작 전에 작업 디스크 여유가
      `max(2GiB, 핸드오프 바이트 × 8)` 이상인지 확인한다. 서울 시험(898,741건, 내보내기 355MB)을 단순 비례하면
      전국 핸드오프는 약 16GB, 요구 여유는 약 128GB, 결과 판은 약 3.8GB다. `/data`의 여유(1.3TB)는 이것의 10배
      이상이다. 실제로 쓴 최대 디스크·시간·메모리를 런북에 적고, 배수 8을 실측으로 고친다. 동등성
      검사(ADR-0135)를 통과하면 승격한다.
   3. 마커 앵커는 R2 Silver에서 다시 만드는 `rebuild-parcel-marker-anchors-streaming`만 남기고, 운영에서 한 번
      돌려 `parcel_marker_anchor`가 채워진 것을 확인한다.
   4. 미러를 읽는 `rebuild-parcel-marker-anchors` 명령과 HTTP 경로를 지운다(§8).
   5. PostGIS 게시·정적판·승격·증거 명령, 위 Context의 검증 명령 셋, Martin `parcels` 소스,
      `scripts/tiles`의 동적 PostGIS 시험, xtask 시험 `parcel_boundary_publication`, pipeline graph의
      `serving-parcel-boundary` 노드를 지운다(§9).
6. **표를 지우는 마이그레이션은 증명 뒤에만 병합한다.** catalog 세 표와 필지 경계 두 표는 준비되는 시점이
   달라서 마이그레이션을 둘로 나눈다. 각 마이그레이션은 아래가 모두 참이어야 병합한다.
   - 읽기·쓰기 코드를 지운 release가 운영에 깔려 있다.
   - 그 release 이후 운영에서 지울 표의 scan·insert·update 증가가 0이다. `pg_stat_user_tables`의 값을 PR에 적는다.
   - 표를 지우기 전 마지막 pgBackRest 전체 백업이 성공했고, 그 백업에 지울 표가 들어 있다. 백업 크기를 PR에
     적는다.
   - **그 백업에서 지울 표 하나를 옆 인스턴스로 복원해 행 수를 운영과 맞춰 본 증거**를 PR에 적는다. 되돌리기의
     첫째 길(7번)이 실제로 되는지 확인한 적이 없기 때문이다.
   - 필지 경계 두 표는 여기에 더해 5번이 운영에서 확인돼야 한다. 즉 필지 활성 release가 `lakehouse_bake`이고
     fallback이 manifest에서 빠졌어야 한다.
   - 같은 PR에서 `scripts/catalog/render-foundation-baseline.py`로 `foundation-baseline.md`를 다시 만든다.
   - 실행 시점에 사장(coordinator) 확인을 받는다(ADR-0133 §9).
7. **FK와 뷰.**
   - `catalog.parcel`·`catalog.building`을 가리키는 FK 제약은 표와 함께 지운다. 참조하던 칸은 그대로 둔다
     (모두 0행).
   - 이 표들만 읽는 뷰·함수·트리거도 함께 지운다. 대상은 `parcel_identifier_lookup`, `parcel_current_identifier`,
     `parcels_missing_temporal_identifier`, `publish_parcel_identifier`, `parcel_boundary_current`, 그리고 가드
     트리거다.
8. **되돌리기.**
   - **코드 단계(2–5번):** PR을 되돌리면 된다. 표와 데이터는 아직 있다.
   - **표를 지운 뒤:**
     - 첫째 길: 35일 안이면 pgBackRest 백업에서 옆 인스턴스로 복원하고, 필요한 표만 옮긴다. 6번의 복원 증거가
       이 길을 보증한다.
     - 둘째 길: 언제든 레이크하우스 Silver에서 다시 만든다. 지운 적재 명령은 git 이력에 있다.
     - 서비스는 어느 쪽도 기다리지 않는다. 손님 경로는 처음부터 R2만 읽기 때문이다.

## Consequences

- DB가 130GB에서 1GB 안팎으로 준다. 매주 전체 백업도 그만큼 준다.
- 디스크: catalog 세 표를 지우면 루트가 약 19GB 빈다(86% → 약 77%). 필지 경계 두 표를 지우면 `/data`가
  112GB 빈다. 루트 디스크의 나머지 압박은 이 ADR로 풀리지 않는다.
- 필지 지도는 PostGIS 손 왕복(약 2.5시간) 대신 예약된 레이크하우스 굽기로 새로워진다.
- foundation-api의 catalog 읽기 경로가 줄어든다. gongzzang-api의 건물 중계 둘이 사라진다. 웹 화면은 바뀌지
  않는다.
- 첫 전국 필지 굽기는 서버 자원을 쓴다. Spark 서빙본과 tippecanoe를 돌린다. 디스크는 `/data`만 쓴다.
