# ADR 0571: PostGIS 필지 경계 사본은 필지가 레이크하우스 경로로 접히고 백업이 그 표를 담은 뒤에 은퇴한다

- Status: Accepted
- Date: 2026-11-19
- Supersedes: [ADR-0025](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md),
  [ADR-0029](./0029-parcel-publication-evidence-is-written-from-the-terminal-run.md),
  [ADR-0031](./0031-parcel-mirror-run-seals-publication-scope.md),
  [ADR-0082](./0082-the-parcel-mirror-takes-its-national-claim-from-the-contract.md);
  [ADR-0194](./0194-retire-the-five-postgres-copies-that-r2-already-holds.md) §6 의 "동등성·복원 증거" 항
- Builds on: [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md) §10·§12,
  ADR-0194 §5, [ADR-0375](./0375-regulation-zones-reach-the-panel-by-pnu-and-the-tile-handoff-reads-only-changed-tiles.md) §2

## Context

운영 DB `foundation` 은 112 GB 이고 거의 전부가 `serving_postgis.parcel_boundary_mirror`(62 GB)와
`parcel_boundary_publication`(50 GB)이다(tablespace `parcel_bulk`, `/data`). 사장이 2026-11-18 삭제를 승인했다.
같은 날 읽기 전용으로 쟀다.

- **읽는 곳.** 두 표의 `idx_scan` 0, `seq_scan` 5, 마지막 2026-11-18 12:33:38Z. 모든 표가 매일 12:30–12:34 에
  이름 순으로 한 번씩 훑인다: `foundation-shape-ledger-check.service` 의
  `scripts/ops/postgres-production-shape.sh` 가 표마다 `select exists (select 1 from t)` 를 부른다(재시작
  2026-11-14 뒤 다섯 번 = 5). 데이터를 읽는 예약 작업·Airflow/Dagster 작업·API 경로·Worker 는 없다.
  뷰 `parcel_boundary_current` 를 읽는 것은 손으로 띄운 `martin-complex` 의 `parcels` 원천뿐이고 요청은 0 이다.
- **쓰는 곳.** publisher 명령 여덟(`publish-parcel-boundary-postgis`·`-static-release`,
  `promote-parcel-boundary-runtime`, `seal-`·`write-parcel-publication-evidence`,
  `rebuild-postgis-parcel-boundary-mirror{,-national,-national-from-contract}`)은 유닛·스크립트 호출자가 없다.
  마지막 쓰기는 2026-09-05 전국 미러(39,861,511 행)다.
- **필지는 아직 ADR-0112 경로에 없었다.** 산업단지·행정경계의 활성 release 는 `lakehouse_bake` 가 만들었고
  오버레이가 200 이다. 필지 활성 release 는 PostGIS `bake`(09-27)의 `readdress` 이고, fallback 은
  `http://127.0.0.1:3110` 의 `dynamic_postgis` release 이며(손님이 닿지 못한다), `overlay/parcels` 는 404 였다.
  레이크하우스 필지 굽기는 11-13 부터 매 run 115분 시간 초과였다(유닛 1 GiB 상한 안에서 1.6 GB 색인).
- **ADR-0194 §6 의 동등성 증명**(`scripts/ops/parcel-boundary-equality.py`)은 두 표 112 GB 를 다시 훑는다.
  운영에서 무한정 스캔을 하지 않는다는 규칙과 맞지 않고, 지우는 목적(되돌릴 수 있음)은 백업이 보증한다.
  최신 전체 백업 `20261115-022325F`(DB 112 GB, 저장소 21.7 GB)를 매주 복원 드릴(ADR-0300 K)이 같은 날 복원해
  운영과 견줬다(`postgres-restore-drill-ok backup=20261115-022325F tables=89`).

## Decision

1. **순서.** 지우기 전에 필지가 ADR-0112 경로에 선다. 굽기가 상한 안에서 돈다(색인 디스크 병합, 패치 경로의
   흐름 비교, 유닛 상한을 발행기까지 잰 2G), 필지 편집이 오버레이에 보이고 Gold 반쪽이 부분 handoff 로 접는다
   (ADR-0112 개정 각주 2026-11-19), 첫 레이크하우스 필지 기본판이 승격되어 `lakehouse_bake` 승격이 fallback 을
   비운다. 그 뒤에만 표를 지우는 마이그레이션을 병합한다.
2. **코드.** 위 여덟 명령과 그 모듈·시험(publisher `tests/parcel_boundary_publication.rs`,
   `parcel_publication_source_evidence*`), 그 명령에만 쓰이던 publisher-core 의 증거 자격·경로 함수와 계약의
   증거 쓰기 키, xtask 의 postgres 레인 대상 셋과 `LaneTarget::Binary`, 동등성 스크립트와 그 시험, pipeline
   graph 의 `silver-parcel-boundaries-to-serving-parcel-boundary`·`serving-parcel-boundary-to-parcel-tiles`
   간선을 지운다. `serving-parcel-boundary` 노드는 표와 함께(마이그레이션 PR) 지운다: graph 의 서빙 표 목록은
   마이그레이션에서 읽기 때문이다. 손으로 띄운 Martin 둘을 내리는 것은 ADR-0112 §10 대로 사장 확인 뒤다.
3. **ADR-0194 §6 의 증거를 바꾼다.** 동등성 스크립트 대신, 지우기 전에 (가) 최신 pgBackRest 전체 백업의
   이름과 크기, (나) 그 백업을 복원한 매주 드릴의 결과, (다) `/data` 여유를 PR 에 적는다. 동등성은 굽기 자신의
   관문(ADR-0133 §4, ADR-0135 첫 판 동등성)이 레이크하우스 판과 지금 서빙판 사이에서 본다.
4. **지우는 것과 남기는 것.** 마이그레이션은 뷰 `parcel_boundary_current`, 두 표, 두 표에만 걸린 가드 함수
   (`protect_sealed_parcel_boundary_mirror_row`, `reject_sealed_parcel_boundary_mirror_truncate`,
   `reject_parcel_boundary_publication_mutation`)를 지운다. 남기는 것과 이유:
   - `serving_postgis.spatial_projection_load`: 산업단지·행정경계 load 도 담는다(4행 중 2행).
   - `parcel_boundary_mirror_rebuild_run`(2행)·`catalog.parcel_publication_source_evidence`(1행): 이력 원장이고
     공유 원장 `spatial_projection_load.source_evidence_id` 가 가리킨다(ADR-0194 §1, 모든 데이터 append-only).
     증거 표의 INSERT 검증 함수 `validate_parcel_publication_source_evidence` 는 지울 미러를 읽으므로 지우고,
     증거 표는 INSERT 도 거부하는 닫힌 원장이 된다(기존 `reject_parcel_publication_source_evidence_mutation`).
   - `catalog.vector_tile_release` 의 `dynamic_postgis` 필지 release 행: 이력이다. 활성·fallback 이 가리키지
     않게 되는 것은 1번의 승격이 한다.
5. **되돌리기.** 표를 지운 뒤 첫 길은 35일 보존 안의 pgBackRest 백업을 옆 인스턴스로 복원해 필요한 표만
   옮기는 것이다(매주 드릴이 그 길을 연다). 서비스는 기다리지 않는다: 손님 경로는 R2 만 읽는다.

## Consequences

- DB 112 GB → 1 GB 안팎, `/data` 112 GB 가 빈다. 매주 전체 백업도 그만큼 준다.
- 필지 지도는 PostGIS 왕복 없이 레이크하우스에서만 새로워진다. 굽기가 다시 멈추면 지도는 마지막 판에
  머무른다(R2 가 단일 경로라는 ADR-0112 의 선택).
- 동등성 스크립트 없이 지운다: 레이크하우스 판과 PostGIS 판을 행 단위로 견준 기록은 남지 않는다. 대신
  첫 레이크하우스 기본판의 동등성 관문이 서빙되는 타일 기준으로 본다.
