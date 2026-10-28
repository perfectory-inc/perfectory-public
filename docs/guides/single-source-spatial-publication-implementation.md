---
status: proposed
owner: foundation
doc_type: guide
last_reviewed: 2026-07-28
---

<!-- public-repository-safety: reviewed-public-contract -->

# 단일 출처 공간 데이터 공개 구현 안내서

**상태:** 승인된 전달 순서. 진행 중 — **87단계 중 53단계 완료(2026-07-29 저장소 실측 대조)**.
체크박스(`- [ ]`)가 감사 가능한 진행 추적 수단이다.
**아키텍처 계약:** [단일 출처 공간 데이터 공개](../architecture/single-source-spatial-publication.md)

## 진행 현황 (2026-07-29 정합 검토)

각 단계가 지목한 산출물을 저장소에서 직접 확인해 대조했다. 코드가 계획과 다른 이름으로
구현된 경우도 실물을 기준으로 판정했다.

| Task | 완료 | 상태 | 근거 |
|---|---:|---|---|
| 1 네이버 벡터소스 리로드 | 2/4 | 프로브 존재 | `naver-sdk.probe.ts:23,467` — `setTiles` 전략·5초 상한 단언. 실행 증거는 런타임 첨부라 미커밋 |
| 2 Iceberg WAP | 7/8 | 실 R2만 미검증 | `spatial_tile_publication_wap.py`, `spatial_tile_wap_command.rs`, `spatial_tile_wap_evidence_contract.rs`(390줄+JSON 스키마), `main.rs:151,538` |
| 3 ADR 정합 | 5/5 | 완료 | 루트 ADR-0006, FP-ADR-0004, GZ-ADR-0036에 단일 출처·동일 데이터 롤백·prefix reload 반영 |
| 4 Manifest v2 | 7/7 | 완료 | `serving_publication.rs`, `catalog_v1.rs:48` (`catalog.vector_tile_runtime_manifest.published.v2`) |
| 5 발행 원장·DB 제약 | 6/6 | 완료 | `20260724000001_spatial_tile_publication.sql` — 3개 표 + CHECK 제약, `sqlx_repository.rs` 배선 |
| 6 원자적 활성화·승격·롤백 | 3/8 | **부분** | `promote_vector_tile_manifest.rs`·`rollback_vector_tile_manifest.rs`만 존재. **빌드 생명주기 부재** |
| 7 WAP 후보·동적 투영 | 1/6 | **부분** | `SpatialTileWapCandidate` 포트는 있음(`lakehouse-application/src/ports.rs:141`). `spatial_tile_projection.rs` 부재 |
| 8 PMTiles 릴리스·Martin | 5/9 | **부분** | 빌드 체인은 `tiles-slice-proof.sh`(martin-cp→MBTiles→PMTiles)에 있음. **격리 빌드 DB와 Rust 명령 3종 부재** |
| 9 런타임 매니페스트·ETag | 5/5 | 완료 | `catalog.rs:615` `/catalog/v1/vector-tiles/runtime-manifest`, OpenAPI 반영 |
| 10 Gongzzang v2 소비자 | 8/8 | 완료 | `foundation-vector-source-refresh.ts:66` 4초 폴링+ETag. 계약 핀 `d916cddc…` 양쪽 바이트 동일 |
| 11 상태기계 E2E | 0/8 | **미착수** | `foundation-vector-source-publication.probe.ts` 부재 |
| 12 스케줄·레디니스·런북 | 0/7 | **미착수** | reconcile 스크립트·systemd 유닛·`spatial_tile_refresh_observation.rs` 전부 부재 |
| 13 최종 검증·리뷰 | 4/6 | 부분 | CI에서 foundation·gongzzang 검증 완료. 전체 완료 전이라 최종 리뷰는 미완 |

**부재가 확인된 CLI 명령:** `plan-spatial-tile-build`, `record-spatial-tile-build-result`,
`promote-spatial-tile-build`, `mark-tile-layer-dynamic`, `start-vector-tile-build`,
`rollback-tile-layer-source`, `reconcile-spatial-tile-publication`.
존재하는 것은 `probe-spatial-tile-wap` 하나뿐이다.

**남은 작업의 실질:** Task 6의 빌드 생명주기(dynamic 표시 → 빌드 시작 → 결과 기록)가 빠져 있어
Task 8의 정지 릴리스가 원장에 기록될 경로가 없고, 그 때문에 Task 11의 E2E 증명이 성립하지 않는다.
**Task 6 → 8 → 11 → 12 순서가 강제된다.**

**목표:** Foundation이 R2/Iceberg에 정본 geometry를 보관하고, Martin이 한 번에 하나의 완전한 PostGIS 또는
PMTiles source만 제공하며, 오래된 build가 승격되지 않고, 이미 열린 Gongzzang 지도가 커밋된 source
변경을 5초 안에 감지하는 운영 형태의 `parcels` vertical slice를 만든다.

**아키텍처:** Foundation Catalog는 불변 release 원장, 단위별 active release, 하나의 전역 runtime-manifest
generation을 소유한다. 공개 변경은 격리된 Iceberg WAP branch에서 준비하고 Catalog 한 트랜잭션에서
완전한 PostGIS projection으로 활성화한 뒤 Martin이 dynamic으로 제공한다. 정적 build는 정확히 하나의
release를 위한 격리 PostGIS build DB를 사용하고 불변 PMTiles를 create-only로 업로드한다. Martin의
R2 prefix hot reload를 확인한 뒤 완전한 static release를 CAS 승격한다. Gongzzang은 ETag로 Catalog
manifest를 poll해 완전한 vector source 하나를 교체하며 static base와 feature tombstone을 조합하지 않는다.

**기술 스택:** 고정 Docker image의 Rust(`tools/technology-versions.contract.json` 의 `rust_toolchain`), PostgreSQL 17/PostGIS 3.5, Apache Iceberg/Spark WAP,
계약에 고정된 Martin/MBTiles/PMTiles, Cloudflare R2 S3 API, Axum/SQLx, TypeScript/Zod, Naver Maps의
bundled mapbox-gl, Vitest, Playwright.

---

## 파일·책임 지도

### 결정과 운영자 계약

- Modify `docs/adr/0006-object-storage-first-serving.md` — root serving decision and single-source invariant.
- Modify `platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md` — Foundation manifest v2 and publication ownership.
- Modify `platforms/foundation-platform/docs/adr/0006-lakehouse-table-format-and-serving-architecture.md` — WAP-selected canonical snapshot and derived serving roles.
- Modify `products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md` — v1/v2 consumer migration and five-second refresh.
- Modify `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md` — exact build, discovery, promotion, rollback, and R2 procedures.

### 브라우저 capability와 Gongzzang 소비자

- 실제 bundle SDK가 vector tile을 reload할 수 있음을 증명하도록 `products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts`를 수정한다.
- Modify `products/gongzzang/apps/web/lib/map/vector-tile-manifest.ts` — strict v1/v2 schemas and conditional fetch.
- Create `products/gongzzang/apps/web/lib/map/foundation-vector-layer-registry.ts` — one registry for source IDs, style dependencies, and `promoteId`.
- Modify `products/gongzzang/apps/web/lib/config/layer-ids.ts` — remove the dead duplicate or re-export the registry.
- Create `products/gongzzang/apps/web/lib/map/foundation-vector-source-refresh.ts` — one selected reload strategy and atomic group update.
- Modify `products/gongzzang/apps/web/lib/map/listing-map-runtime.ts` — register through the registry and expose refresh cleanup.
- Modify `products/gongzzang/apps/web/components/listings/listing-map.tsx` — own poll timer, abort controller, and visibility handling.
- Modify `products/gongzzang/apps/web/proxy.ts` — keep manifest and tile origins in the explicit CSP contract.
- Modify `products/gongzzang/apps/web/tests/unit/map/vector-tile-manifest.test.ts`.
- Create `products/gongzzang/apps/web/tests/unit/map/foundation-vector-source-refresh.test.ts`.
- Modify `products/gongzzang/crates/foundation-platform-client/openapi/catalog.v1.json` — pinned
  provider-contract snapshot.
- Modify `products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`
  for the SHA-256 pin of the exact provider contract accepted by Gongzzang.
- Modify
  `products/gongzzang/docs/architecture/platform-integration/{allowed-call-matrix.v1.json,route-exposure-policy.v1.json}`
  for the browser-direct Catalog route, exposure, and no-credential policy.

### Foundation 계약과 공개 상태

- Modify `platforms/foundation-platform/crates/catalog/catalog-domain/src/vector_tile.rs` — preserve v1 flat-layout types.
- Create `platforms/foundation-platform/crates/catalog/catalog-domain/src/serving_publication.rs` — publication aggregate and transition invariants.
- Modify `platforms/foundation-platform/crates/catalog/catalog-domain/src/{errors.rs,lib.rs}`.
- Modify `platforms/foundation-platform/crates/foundation-contracts/src/catalog.rs` — manifest v2 and command/response DTOs.
- Modify `platforms/foundation-platform/crates/foundation-shared-kernel/src/{ids.rs,events/catalog_v1.rs}` — typed IDs, additive v2 event, and byte-compatibility tests.
- Modify tests:
  - `platforms/foundation-platform/crates/catalog/catalog-domain/tests/vector_tile_manifest.rs`
  - `platforms/foundation-platform/crates/foundation-contracts/tests/vector_tile_manifest_dto.rs`

### DB와 application transaction

- Create `platforms/foundation-platform/migrations/20260724000001_spatial_tile_publication.sql`.
- Modify `platforms/foundation-platform/services/foundation-api/tests/deploy_contract.rs` — keep the original four-file baseline immutable while permitting additive migrations.
- Modify `platforms/foundation-platform/crates/catalog/catalog-application/src/ports.rs`.
- Create application use cases:
  - `mark_tile_layer_dynamic.rs`
  - `start_vector_tile_build.rs`
  - `record_vector_tile_build_result.rs`
  - `promote_tile_layer_static.rs`
  - `rollback_tile_layer_source.rs`
- Modify `platforms/foundation-platform/crates/catalog/catalog-application/src/lib.rs`.
- Modify `platforms/foundation-platform/crates/catalog/catalog-infrastructure/src/{unit_of_work.rs,sqlx_repository.rs,row_map.rs,lib.rs}`.
- Create `platforms/foundation-platform/crates/catalog/catalog-infrastructure/tests/spatial_tile_publication.rs`.
- Task 6에서 지목한 두 `CatalogUnitOfWork` test fake를 갱신한다.

### Iceberg WAP와 serving projection

- Modify `platforms/foundation-platform/crates/lakehouse/lakehouse-domain/src/lakehouse.rs` and tests
  — 기존 SCD2 필지 계약의 current-row predicate에 대한 Rust SSOT.
- Regenerate `platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`
  — Spark-facing derived artifact, never an independent authority.
- Modify `platforms/foundation-platform/infra/lakehouse/spark/jobs/platform_contracts.py` and its
  contract tests — expose that predicate without copying it.
- Create `platforms/foundation-platform/infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py`.
- Create `platforms/foundation-platform/infra/lakehouse/spark/tests/test_spatial_tile_publication_wap.py`.
- Modify `platforms/foundation-platform/crates/lakehouse/lakehouse-application/src/ports.rs`.
- Create `platforms/foundation-platform/crates/lakehouse/lakehouse-infrastructure/src/spatial_tile_wap.rs`.
- Modify `platforms/foundation-platform/crates/lakehouse/lakehouse-infrastructure/src/lib.rs`.
- Create `platforms/foundation-platform/crates/lakehouse/lakehouse-infrastructure/tests/spatial_tile_wap.rs`.
- Create `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_wap_command.rs`.
- Modify `platforms/foundation-platform/services/foundation-outbox-publisher/src/{main.rs,main_command_tests.rs}`.
- 위 migration에 로그가 남는 완전한 필지 공개 projection을 추가한다.
- Create `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_projection.rs`.

### Static builder, R2, and Martin

- Create `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_build.rs`.
- Create `platforms/foundation-platform/crates/publisher/foundation-publisher-tiles/src/tile_derivative_object_storage.rs`.
- Modify `platforms/foundation-platform/crates/publisher/foundation-publisher-core/src/r2_layout.rs`.
- Reuse `platforms/foundation-platform/crates/foundation-outbox/src/object_storage/r2.rs`; do not create another S3 client.
- Modify `scripts/tiles/{compose.yaml,martin-dynamic.yaml,martin-static.yaml,tiles-slice-proof.sh,fixture.sql,vector-tile-manifest.local.json}`.
- Modify `platforms/foundation-platform/services/foundation-api/tests/{tiles_slice_contract.rs,tiles_slice_harness_contract.rs}`.

### Runtime API, pointer, and operations

- Modify `platforms/foundation-platform/services/foundation-api/src/routes/{catalog.rs,catalog_openapi.rs,catalog_tests.rs,mod.rs}`.
- Modify `platforms/foundation-platform/services/foundation-api/src/state.rs`.
- Modify `platforms/foundation-platform/services/foundation-api/src/routes/tests/health_and_metrics.rs`.
- Regenerate `platforms/foundation-platform/docs/openapi/catalog.v1.json`.
- Modify `platforms/foundation-platform/crates/foundation-outbox/src/vector_tile_manifest.rs`.
- Modify `platforms/foundation-platform/crates/foundation-outbox/tests/{vector_tile_manifest_pointer.rs,publish_roundtrip.rs}`.
- Modify `platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs` for build/reconcile commands.
- Create `platforms/foundation-platform/scripts/tiles/reconcile-spatial-tile-publication.sh`.
- Create `platforms/foundation-platform/infra/systemd/foundation-spatial-tile-publication.{service,timer}`.
- Modify `platforms/foundation-platform/infra/observability/prometheus/foundation-api.rules.yml`.

## Task 1: Prove the Naver Vector-Source Reload Capability

이 단계는 중단 게이트다. 실제 Naver SDK 번들이 지원되는 갱신 경로 하나를 증명하기
전에는 Foundation 백엔드를 구현하지 않는다.

**Files:**
- Modify: `products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts`
- Test: `products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts`

- [x] **Step 1: Add a failing probe for the preferred `setTiles` path**

작은 벡터 소스를 등록한 뒤 실제 소스 객체를 검사하고, 호출 가능한 `setTiles`가 있을
때만 시도한다.

```ts
type ReloadableVectorSource = {
  setTiles?: (tiles: string[]) => void;
};

const source = mapbox.getSource(sourceId) as ReloadableVectorSource | undefined;
expect(source).toBeDefined();
```

probe는 첫 타일 URL을 기록하고 `setTiles([secondUrl])`을 호출할 수 있으면 호출한 뒤
`secondUrl`에 대한 네트워크 요청을 관측해야 한다. 메서드 이름만 발견한 것은 증명이 아니다.

- [x] **Step 2: Add bounded fallback probes**

`setTiles`가 없거나 동작하지 않으면 다음 순서로 시험한다.

1. `removeLayer` + `removeSource` + center/zoom을 보존하는 deterministic re-add;
2. center/zoom과 click registration을 보존하는 제어된 Naver map 재초기화.

선택된 전략은 정확히 하나만 기록한다. service worker를 추가하거나 실제 객체에 있는
메서드 밖에서 mapbox 내부를 조작하지 않는다.

- [ ] **Step 3: Run the real browser probe**

다음을 실행한다.

```bash
pnpm -C products/gongzzang/apps/web probe:naver --grep "vector source reload"
```

기대 결과는 `setTiles`, `remove-and-add`, `map-reinitialize` 중 선택된 전략과 5초 안에
두 번째 타일 URL 요청이 있었다는 증거를 포함한 PASS다.

인증 정보나 Naver 테스트 페이지가 없으면 중단하고 확보한다. 세 전략이 모두 실패하면
중단하고 아키텍처 검토로 돌아간다.

- [ ] **Step 4: Commit the capability evidence**

Commit only the probe, not screenshots, API keys, or generated traces:

```bash
git add products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts
git commit -m "test(gongzzang): prove vector source reload capability"
```

## Task 2: Prove Iceberg WAP on the Selected REST Catalog

이 단계는 두 번째 중단 게이트다. 표준 Iceberg 계약을 구현하지 않는 제공자 위에
사용자 정의 branch/pointer 시스템을 만들지 않게 한다.

**Files:**
- Modify: `platforms/foundation-platform/crates/lakehouse/lakehouse-domain/src/lakehouse.rs`
- Modify: `platforms/foundation-platform/crates/lakehouse/lakehouse-domain/tests/industrial_complex_lakehouse_contract.rs`
- Modify: `platforms/foundation-platform/crates/lakehouse/lakehouse-domain/tests/lakehouse_contract_artifact.rs`
- Regenerate: `platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json`
- Modify: `platforms/foundation-platform/infra/lakehouse/spark/jobs/platform_contracts.py`
- Create: `platforms/foundation-platform/infra/lakehouse/spark/tests/test_platform_contracts.py`
- Create: `platforms/foundation-platform/infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py`
- Create: `platforms/foundation-platform/infra/lakehouse/spark/tests/test_spatial_tile_publication_wap.py`
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_wap_command.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/main_command_tests.rs`

- [x] **Step 1: Write failing Rust-SSOT and Spark job contract tests**

먼저 Rust contract test를 확장한다. `LakehouseTableContract`에 machine-readable
`current_row_predicate`; every existing contract is `None` except `SILVER_PARCEL_BOUNDARIES`, whose
value is exactly `valid_to_utc IS NULL`. Update the artifact drift test so the derived JSON field must
match the Rust value.

Then require the Spark job to:

- 두 번째 parcel geometry table을 만들지 말고 기존 canonical `silver.parcel_boundaries` contract를 대상으로 한다.
- contract에서 정확한 current-row predicate `valid_to_utc IS NULL`을 읽으며 producer나 projection이 자체 SCD2 selector를 작성하지 않는다.
- create `tile_<release_uuid>` at an exact base snapshot;
- write one add, one geometry replacement, and one logical delete only to that branch, preserving
  the table's `valid_from_utc`/`valid_to_utc` history and one-active-row-per-`pnu` invariant;
- prove `main` is unchanged;
- read the branch and validate the change;
- fast-forward `main` only when explicitly requested;
- emit JSON containing table, base snapshot, branch snapshot, branch name, and result;
- set bounded retention and never print credentials.
- reject zero or multiple current rows for a `pnu`, and prove superseded historical rows are absent
  from the candidate's current-row read.

- [x] **Step 2: Run the tests and observe both missing-contract and missing-job failures**

Run:

```bash
python platforms/foundation-platform/infra/lakehouse/spark/tests/test_platform_contracts.py
python platforms/foundation-platform/infra/lakehouse/spark/tests/test_spatial_tile_publication_wap.py
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p lakehouse-domain --test industrial_complex_lakehouse_contract
```

Expected: FAIL because the Rust contract lacks the selector and
`spatial_tile_publication_wap.py` does not exist.

- [x] **Step 3: Extend the Rust SSOT and regenerate its Spark artifact**

`LakehouseTableContract`에 선택 필드를 추가하고 모든 정적 계약에서 명시적으로 초기화한다.
필지 계약의 값은 Rust에서 한 번만 설정한 뒤 JSON 파생 산출물을 갱신한다. 산출물 드리프트
테스트가 기계적 가드다. Python은 JSON 값을 읽을 수 있지만 어떤 Python·SQL 파일도 이
조건식을 다시 정의할 수 없다.

- [x] **Step 4: Implement the thin Spark WAP job**

메타데이터 파일을 직접 수정하지 말고 Iceberg SQL branch 연산을 사용한다.

```sql
ALTER TABLE <table> CREATE BRANCH `<branch>` AS OF VERSION <snapshot> RETAIN 7 DAYS;
-- write MERGE/DELETE operations to <table>.branch_<branch>
CALL <catalog>.system.fast_forward('<namespace.table>', 'main', '<branch>');
```

보간하기 전에 식별자를 검증한다. 실패한 후보가 스스로 발행하지 못하도록 `prepare`,
`validate`, `fast-forward`를 별도 명령으로 유지한다.

- [x] **Step 5: Add the Rust command wrapper**

Add:

```text
foundation-outbox-publisher probe-spatial-tile-wap
```

기존 `remote_lakehouse_job` 경계를 따른다. Rust command가 입력을 검증하고 pinned
`compose.lakehouse.yml` Spark service용 secret-free execution plan을 낸다. host/runner가 이
계획을 실행하며 Rust container에는 Docker socket을 주지 않는다. 두 번째 validation 단계가
결과 evidence JSON을 읽고 예상하지 않은 table/snapshot/branch/result를 거부한 뒤
`target/spatial-tile-publication/`에 기록한다. plan과 evidence 어느 쪽에도 catalog token을 넣지 않는다.

- [x] **Step 6: Run offline contract tests**

이 테스트는 식별자 검증, SQL 생성, 증거 파싱, 비밀값 제거를 증명한다. 제공자 기능을
증명했다고 주장해서는 안 된다.

```bash
python platforms/foundation-platform/infra/lakehouse/spark/tests/test_spatial_tile_publication_wap.py
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p foundation-outbox-publisher spatial_tile_wap
```

Expected: PASS. Output must say `provider_capability=not_proven_offline`.

- [ ] **Step 7: Prove the real R2 Data Catalog provider**

버킷 범위 테스트 카탈로그 인증 정보를 환경에서만 받아 전용 테스트 namespace/table에
명령을 실행한다. 운영 Gold 테이블은 절대 사용하지 않는다. 실제 제공자 probe만 통합
게이트이며 mock, JDBC catalog, 로컬 메타데이터 디렉터리로 대체하지 않는다.

```bash
cd platforms/foundation-platform
# `MSYS2_ARG_CONV_EXCL` is inert on Linux and prevents Git Bash on the
# supported Windows host from rewriting the container-only `/tmp` path.
MSYS2_ARG_CONV_EXCL='*' docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN \
  spark spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
  /workspace/infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py \
  probe --namespace tiles_slice_proof --table parcel_boundaries_wap_probe
```

Expected: `provider=cloudflare-r2-data-catalog branch_isolation=ok fast_forward=ok`.

호환성 pin: 이 slice는 Cloudflare가 문서화한 static catalog token과 Iceberg vended-credentials
mode를 사용하며 이 mode에 별도 endpoint가 없으므로 `oauth2-server-uri`를 의도적으로 설정하지
않는다. Spark credential redaction regex와 Iceberg `1.6.1` package를 명시적 contract test로
고정한다. 제공자가 새 mode를 문서화하고 새 real-provider probe가 schema-valid evidence를 만들기
전에는 Iceberg version·token exchange mode·OAuth endpoint 변경을 막는다.

Cloudflare beta 제공자가 실패하면 중단하고 실패를 기록한다. Parquet/Iceberg 데이터는
R2에 유지하면서 규격을 준수하는 Iceberg REST Catalog 제공자를 선택한다. 임의 객체 키로
WAP을 흉내 내지 않는다.

- [x] **Step 8: Commit the provider-neutral capability slice**

```bash
git add platforms/foundation-platform/infra/lakehouse/spark \
  platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json \
  platforms/foundation-platform/crates/lakehouse/lakehouse-domain \
  platforms/foundation-platform/services/foundation-outbox-publisher/src
git commit -m "test(foundation): prove Iceberg WAP publication"
```

## Task 3: Reconcile the Accepted ADRs

이 작업은 1·2단계를 통과한 뒤에만 수행한다.

**Files:**
- Modify: `docs/adr/0006-object-storage-first-serving.md`
- Modify: `platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md`
- Modify: `platforms/foundation-platform/docs/adr/0006-lakehouse-table-format-and-serving-architecture.md`
- Modify: `products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md`
- Modify: `products/gongzzang/docs/architecture/platform-integration/route-exposure-policy.v1.json`
- Modify: `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md`

- [x] **Step 1: Replace feature overlay language with the single-source invariant**

Document:

```text
(publication_unit, serving_generation) -> exactly one complete Martin source
```

Martin composite·client feature tombstone·custom MVT decode/filter/re-encode가 Foundation polygon
경로가 아님을 명시한다.

- [x] **Step 2: Define the three independent versions**

- `data_revision`: canonical feature content;
- per-unit `serving_generation`: selected release/source;
- global `manifest_generation` and immutable `current_version`: polling/ETag.

- [x] **Step 3: Record WAP, isolated build DB, prefix hot reload, and same-data rollback**

공식 Iceberg branching 및 Martin PMTiles hot-reload 문서를 연결한다. data revert는 새 canonical
revision을 만들고 serving rollback은 business data를 바꾸지 않는다고 기록한다.

- [x] **Step 4: Run documentation and monorepo guards**

Run:

```bash
git diff --check
"C:/Program Files/Git/bin/bash.exe" scripts/guard/monorepo-guard.sh
```

Expected: PASS. On Windows linked worktrees, do not invoke WSL's `/usr/bin/git` against a Windows
`.git` pointer.

- [x] **Step 5: Commit the reconciled decision**

```bash
git add docs/adr/0006-object-storage-first-serving.md \
  platforms/foundation-platform/docs/adr \
  products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md \
  products/gongzzang/docs/architecture/platform-integration/route-exposure-policy.v1.json \
  platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md
git commit -m "docs: adopt single-source spatial publication"
```

## Task 4: Add Manifest v2 Without Repurposing v1

**Files:**
- Modify: `platforms/foundation-platform/crates/catalog/catalog-domain/src/vector_tile.rs`
- Create: `platforms/foundation-platform/crates/catalog/catalog-domain/src/serving_publication.rs`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-domain/src/{errors.rs,lib.rs}`
- Modify: `platforms/foundation-platform/crates/foundation-contracts/src/catalog.rs`
- Modify: `platforms/foundation-platform/crates/foundation-shared-kernel/src/{ids.rs,events/catalog_v1.rs}`
- Test: `platforms/foundation-platform/crates/catalog/catalog-domain/tests/vector_tile_manifest.rs`
- Test: `platforms/foundation-platform/crates/foundation-contracts/tests/vector_tile_manifest_dto.rs`

- [x] **Step 1: Write failing v2 domain and DTO tests**

Test that:

- v1은 여전히 `{object_key_prefix}/{z}/{x}/{y}`와 flat statistic을 요구한다.
- v2 has one top-level UUID `current_version`, JavaScript-safe `manifest_generation`, exact
  `refresh_after_seconds: 4`, publish timestamp, and non-empty `publication_units`;
- each publication-unit value has UUID `data_revision`, JavaScript-safe `serving_generation`, UUID
  `active_release_id`, immutable positive-decimal-string `canonical_iceberg_snapshot_id`, one closed
  tagged `source`, non-empty `layers`, and lineage;
- the launch fixture/enablement guard, not the reusable DTO type, contains only the `parcels` unit
  and exactly the `parcels` MVT layer;
- `static_pmtiles` requires immutable object key, bytes, checksum, and Martin source ID;
- `dynamic_postgis` rejects PMTiles-only fields;
- v2 tile URLs accept HTTPS and proof-only HTTP loopback, while a separate production publication
  policy rejects every HTTP URL;
- unknown schema versions are rejected.

- [x] **Step 2: Run the tests and verify the missing-v2 failure**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p catalog-domain -p foundation-contracts vector_tile
```

기대 결과: v2 타입이 아직 없어 실패한다.

- [x] **Step 3: Implement the domain types**

Use explicit types:

```rust
pub enum ActiveTileSource {
    DynamicPostgis,
    StaticPmtiles,
}

pub struct ServingGeneration(u64);
pub struct ManifestGeneration(u64);
pub struct FeatureIdProperty(String);
```

`TilesUrlTemplate`은 v1 flat-layout 타입으로 유지한다. `{z}`, `{x}`, `{y}`는 요구하지만
`{object_key_prefix}`는 받지 않는다. absolute HTTPS와 Docker proof에서 사용하는 `localhost` 또는
loopback IP literal에 한해 absolute HTTP를 허용한다. 운영 HTTPS-only 규칙을 wire parser에 넣지
말고 production publish gate가 더 엄격한 정책을 소유하고 테스트한다.

- [x] **Step 4: Implement additive v2 DTOs**

미래 버전을 `z.number().min(1)`처럼 느슨하게 역직렬화하지 않는다. Rust와 TypeScript는
스키마 버전 `1` 또는 `2`에서만 정확히 분기해야 한다.

- [x] **Step 5: Define the additive v2 Catalog event before any application uses it**

불변 매니페스트 ID, 전역 generation, 선택된 release와 각 정본 Iceberg snapshot ID를 담는
`VectorTileRuntimeManifestPublishedV2` payload를 추가한다. 고정 포인터 키
`gold/vector-tiles/runtime-manifest.json`은 타입이 있는 공용 상수로 한 번만 정의하고,
생성 전용 키 `gold/vector-tiles/manifests/{manifest_id}.json`은 이벤트의 manifest ID에서
타입 안전하게 파생한다. 어느 키도 임의의 이벤트 문자열로 받지 않는다. 기존 v1 enum 태그와
직렬화 바이트 fixture는 모두 그대로 유지한다. 기존 이벤트가 동일하게 역직렬화되고 v2
이벤트가 generation·release·snapshot 집합을 생략할 수 없음을 왕복 테스트와 golden-byte
테스트로 증명한다. Task 9는 이 이벤트를 소비하며 두 번째 이벤트 형태를 정의하지 않는다.

- [x] **Step 6: Run the package tests**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p catalog-domain -p foundation-contracts -p foundation-shared-kernel vector_tile
```

기대 결과: v1 호환성과 v2 검증이 모두 통과한다.

- [x] **Step 7: Commit**

```bash
git add platforms/foundation-platform/crates/catalog/catalog-domain \
  platforms/foundation-platform/crates/foundation-contracts \
  platforms/foundation-platform/crates/foundation-shared-kernel
git commit -m "feat(foundation): define tile publication manifest v2"
```

## Task 5: Add the Publication Ledger and Database Constraints

**Files:**
- Create: `platforms/foundation-platform/migrations/20260724000001_spatial_tile_publication.sql`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/deploy_contract.rs`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-domain/src/serving_publication.rs`
- Test: `platforms/foundation-platform/crates/catalog/catalog-domain/tests/vector_tile_manifest.rs`

- [x] **Step 1: Fix the additive-migration guard test first**

deploy contract가 지정된 2026-07-19 baseline file 네 개를 hash하고 각각 계속 존재하는지 별도로
검증하게 바꾼다. directory에 migration이 정확히 네 개여야 한다는 assertion만 제거한다. 이름이
올바른 다섯 번째 migration을 허용한다는 test를 추가한다.

- [x] **Step 2: Write the failing state-machine tests**

Cover:

```text
legacy/no active release -> dynamic release 1
dynamic data revision A -> static release A
static data revision A -> dynamic data revision B
static release A -> same-data dynamic fallback A
static data revision A -> old data revision Z  (reject)
```

- [x] **Step 3: Add normalized publication tables**

마이그레이션은 다음을 생성해야 한다.

```sql
catalog.vector_tile_publication_unit
catalog.vector_tile_release
catalog.vector_tile_release_layer
catalog.vector_tile_runtime_manifest
catalog.vector_tile_runtime_manifest_unit
catalog.vector_tile_runtime_manifest_pointer
catalog.vector_tile_build_job
catalog.vector_tile_refresh_observation
```

필수 데이터베이스 제약:

- unique lowercase logical `layer_id`;
- positive per-unit `serving_generation` and global manifest generation;
- the runtime pointer is a single checked row and references one immutable v2 manifest;
- each immutable v2 manifest has a unique generation and one release selection per publication unit;
- manifest-unit selections reference a release from that same unit and snapshot the serving
  generation used by that selection;
- every v2 release has immutable `canonical_iceberg_snapshot_id`; every build job has immutable
  `input_release_id`, `input_data_revision`, and `frozen_source_snapshot_id`, with a constraint tying
  all three to the input release;
- active and fallback release belong to the same unit;
- fallback release has the same `data_revision`;
- `static_pmtiles` release requires a PMTiles file asset and versioned Martin source;
- `dynamic_postgis` release requires a projection generation and forbids PMTiles fields;
- a build result whose reported snapshot differs from `frozen_source_snapshot_id` cannot validate;
- `(publication_unit_id, idempotency_key)` is unique;
- static promotion cannot be represented without validation evidence.
- refresh observations contain only manifest/release generations, commit/first-tile timestamps,
  outcome, probe environment와 evidence checksum만 기록하며 user·PNU·camera·IP data는 절대
  기록하지 않는다. idempotency key는 unique해야 한다.

release의 `(id, data_revision, canonical_iceberg_snapshot_id)`에 복합 unique key를 사용하고,
빌드 입력 세 값에서 복합 foreign key를 만들어 snapshot 결합을 Rust만이 아니라
PostgreSQL이 강제하게 한다.

`catalog.vector_tile_manifest`, `catalog.vector_tile_artifact` 또는 flat-MVT 제약을 바꾸지
않는다. 이것들은 동결된 schema-v1 저장 모델이다. v2는 위의 새 정규화 테이블만 사용한다.
v2 마이그레이션이 어느 legacy 테이블을 바꾸거나 기존 v1 route/event/object-key 바이트를
변경하면 실패하는 마이그레이션 계약 테스트를 추가한다.

- [x] **Step 4: Add the logged complete parcel projection**

Create `serving_postgis.parcel_boundary_publication` as a logged serving projection with canonical
lowercase `pnu`, `official_complex_code`, `data_revision`, and `geometry(MultiPolygon,5179)`, plus
primary/GiST indexes. Do not remove or mutate `serving_postgis.parcel_boundary_mirror`.

- [x] **Step 5: Run migration and domain tests**

```bash
scripts/verify/integration.sh foundation
```

예상 결과: 모든 migration이 least-privilege migrator로 적용되고 state constraint가 잘못된
fixture를 거부하며 기존 catalog/listing path가 그대로 유지된다.

- [x] **Step 6: Commit**

```bash
git add platforms/foundation-platform/migrations \
  platforms/foundation-platform/services/foundation-api/tests/deploy_contract.rs \
  platforms/foundation-platform/crates/catalog/catalog-domain
git commit -m "feat(foundation): persist spatial tile publication state"
```

## Task 6: Implement Atomic Activation, Promotion, and Same-Data Rollback

**Files:**
- Modify: `platforms/foundation-platform/crates/catalog/catalog-application/src/ports.rs`
- Create: `platforms/foundation-platform/crates/catalog/catalog-application/src/{mark_tile_layer_dynamic.rs,start_vector_tile_build.rs,record_vector_tile_build_result.rs,promote_tile_layer_static.rs,rollback_tile_layer_source.rs}`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-application/src/lib.rs`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-infrastructure/src/{unit_of_work.rs,sqlx_repository.rs,row_map.rs,lib.rs}`
- Create: `platforms/foundation-platform/crates/catalog/catalog-infrastructure/tests/spatial_tile_publication.rs`
- Modify test fakes:
  - `platforms/foundation-platform/crates/catalog/catalog-application/tests/industrial_complex_catalog_import_use_case.rs`
  - `platforms/foundation-platform/crates/catalog/catalog-application/tests/industrial_complex_mutation_use_case.rs`

- [ ] **Step 1: Write the concurrent activation integration test**

같은 expected release에서 두 edit을 시작한다. activation 하나만 commit되고 패자는 typed version
conflict를 받는지 확인한다. 승리한 transaction이 다음을 갱신하는지 확인한다.

- complete PostGIS projection rows;
- immutable dynamic release;
- unit active pointer and serving generation;
- global immutable manifest and generation;
- preallocated immutable-manifest `file_asset` identity whose deterministic object key is
  `gold/vector-tiles/manifests/{manifest_id}.json`;
- additive v2 outbox event.

insert 하나라도 실패하면 부분 상태가 남아서는 안 된다.

두 단위(`parcels`, `complex`)를 동시에 시작하는 두 번째 interleaving도 실행한다.
두 commit 모두 성공할 수 있지만 각 결과 manifest에는 두 selection이 정확히 한 번씩 들어가고
전역 `manifest_generation`은 순서대로 두 번 증가해야 한다. selection이 사라지거나 half-committed
unit을 합친 manifest가 나오면 안 된다. database serialization failure를 충분한 증거로 보지 말고
고정된 lock order 자체를 test로 증명한다.

- [ ] **Step 2: Write the stale-build promotion test**

Sequence:

```text
release R10 active
build B10 starts from R10
edit activates R11
B10 validates
B10 promotion -> conflict and SUPERSEDED
build B11 -> promotion succeeds
```

- [ ] **Step 3: Write snapshot-binding and publication-capability tests**

claimed frozen snapshot이 input release의 immutable snapshot과 다르면 build를 거부한다. v2
capability가 꺼진 경우 같은 activation은 내부 publication state만 갱신하고 v2 public event는
내보내지 않는다. 기존 v1 event/projection 동작은 byte-identical이어야 한다. capability가 켜진
경우 같은 transaction에 v2 event 정확히 하나를 기록한다.

- [ ] **Step 4: Write the serving-rollback test**

정적 릴리스 S11을 승격한 뒤 같은 데이터 리비전의 보존된 동적 릴리스 R11로 롤백한다.
다른 데이터 리비전의 fallback은 거부되는지 확인한다.

> **정정 (2026-07-30):** 이 항목은 `[x]`였는데 근거가 잘못됐다. 존재하는
> `catalog-infrastructure/tests/vector_tile_manifest_rollback.rs`는 **v1 flat manifest** 롤백
> 테스트이고, 이 단계가 요구하는 v2 serving-source 롤백 테스트는 없다. 이름이 비슷해서
> 대조에서 통과했다.

- [x] **Step 5: Implement application commands and ports**

모든 mutation command에는 `expected_active_release_id`, `expected_version`,
`canonical_iceberg_snapshot_id`와 idempotency key가 들어간다. Promotion에는
`input_release_id`, `frozen_source_snapshot_id`, candidate validation digest도 추가한다.
명시적으로 타입이 있는 `RuntimeManifestPublicationCapability`를 주입하며
domain/application code가 environment variable을 직접 읽지 못하게 한다.

구현 결정: `expected_version`은 전역 manifest version이 아니라 단위별
`expected_serving_generation`으로 둔다. 전역 version을 CAS 키로 쓰면 서로 다른 단위를 바꾸는
두 edit이 충돌로 판정되어 Step 1이 요구하는 "둘 다 commit되고 전역 generation이 순서대로 두 번
증가"를 만족할 수 없다. 전역 manifest는 pointer를 잠근 뒤 다시 만든다. 단위별 release id와
generation은 서로를 대체하지 않는다 — 같은 데이터 리비전으로 rollback하면 보존된 release가
새 generation에서 다시 활성화되므로 release id 하나로는 두 상태를 구분할 수 없다.

environment variable 금지는 서술이 아니라 `scripts/guard/no-env-access-in-domain-layers.sh`가
집행한다. 레포의 `*-domain`·`*-application` 크레이트 36개 모두가 현재 0건이므로 예외 목록 없이
그 상태를 고정했다.

`RuntimeManifestPublicationCapability`가 무엇을 막는지도 못 박아 둔다. **admission이 아니라
v2 outbox event를 막는다.** Step 3이 요구하는 대로 capability가 꺼진 배포도 activation을 내부
원장에 기록하고 public v2 event만 내보내지 않으며, 그래야 v1 동작이 byte-identical하게 남는다.
따라서 use case는 이 capability를 보지 않는다 — use case에서 거부하면 그 요구를 만족할 수 없고,
하나의 배포 결정을 두 곳에서 답하게 되어 주입 타입을 도입한 이유가 사라진다. 결정은 event를
쓰는 곳, 즉 transaction이 소유한다. 정적 빌드 원장은 내부 기록이므로 애초에 대상이 아니다.
API의 v2 매니페스트 조회 라우트가 같은 capability로 404를 내는 것은 별개의 읽기 게이트다.

- [ ] **Step 6: Implement one SQLx transaction boundary**

기존 `catalog-infrastructure/src/unit_of_work.rs`의 `FOR UPDATE`/CAS/outbox pattern을 재사용한다.
먼저 singleton `vector_tile_runtime_manifest_pointer` row, 그 다음 영향을 받은 publication unit,
마지막으로 release row를 lock한다. pointer와 unit row가 잠긴 동안 완전한 전역 manifest를 만들며
모든 code path는 `runtime_manifest_pointer -> publication_unit -> release rows` 순서를 지켜야 한다.
typed serialization/CAS conflict가 나면 최신 pointer부터 retry하고 다른 unit의 selection을 조용히
버리지 않는다. database transaction 안에서 R2 pointer를 갱신하지도 않는다.

> **정정 (2026-07-30):** 이 항목도 `[x]`였는데 근거가 잘못됐다. `unit_of_work.rs`가 구현한
> v2 메서드는 `promote_vector_tile_runtime_manifest` 하나이며, 이는 singleton pointer의 CAS를
> SQL 함수에 위임할 뿐이다. 이 단계가 요구하는
> `runtime_manifest_pointer -> publication_unit -> release rows` 3단 lock 트랜잭션은 없다.
> 다섯 개 application 명령의 port 메서드는 모두 **기본 구현이 에러**인 상태이며, 그것이
> 미구현을 조용한 성공으로 바꾸지 않는 이유다.

<!-- 두 인용은 별개다 (MD028) -->

> **구현 제약 (2026-07-30 확인):** SQL 함수를 읽어 보면 이 단계의 일이 예상보다 적고, 대신
> 예상하지 못한 제약이 하나 있다.
>
> `catalog.promote_vector_tile_runtime_manifest`가 이미 pointer CAS, 완전성
> (`next_unit_count = publication_unit_count`), 첫 발행은 dynamic, static은 현재 리비전,
> serving generation ±1, 전역 generation 증가, 그리고 unit pointer 갱신(+fallback 보존/삭제)을
> 모두 수행한다. Rust 트랜잭션이 할 일은 release·layer·manifest·manifest_unit 행을 쓰고 이
> 함수를 호출한 뒤 outbox event를 넣는 것이다.
>
> 제약: **한 단위만 바꿔도 매니페스트는 모든 publication unit을 선택해야 한다.** 함수가
> `next_unit_count <> publication_unit_count`를 거부하므로, 활성화 트랜잭션은 현재 pointer의
> manifest_unit 행을 읽어 나머지 단위의 선택을 그대로 이어받아야 한다. Step 1이 요구하는
> "selection이 사라지거나 half-committed unit을 합친 manifest가 나오면 안 된다"가 이 제약이다.
>
> `fallback_release_id`는 함수가 **보존하거나 지울 뿐 설정하지 않는다.** 정적 승격은 함수 호출
> *전에* 이전 release id를 읽어 두고, 호출 *후에* fallback을 직접 써야 한다 — 호출 후에는
> `active_release_id`가 이미 새 release다.

<!-- 두 인용은 별개다 (MD028) -->

> **착수 전 조사 결과 (2026-07-30):** 필요한 부품은 모두 존재한다. 없는 것은 트랜잭션 본문뿐이다.
>
> - **v2 outbox 이벤트 존재.** `foundation_shared_kernel::events::catalog_v1::CatalogEvent`의
>   `VectorTileRuntimeManifestPublished(VectorTileRuntimeManifestPublishedV2)`. 페이로드는
>   `manifest_id`, `manifest_generation`, `publication_units: BTreeMap<String,
>   VectorTileRuntimeUnitSelectionV2>`, `published_at`이며 단위 선택은 `active_release_id`,
>   `data_revision`, `serving_generation`, `canonical_iceberg_snapshot_id`를 담는다.
>   (`foundation-contracts`가 아니라 `foundation-shared-kernel`에 있다.)
> - **capability 주입 지점.** `PgCatalogUnitOfWork::new(pool)` 프로덕션 호출처가 4곳이다.
>   생성자를 바꾸면 v2와 무관한 3곳이 함께 바뀌므로, `AppState`에서 쓴 것과 같은
>   `with_runtime_manifest_publication(capability)` 빌더를 두고 기본값은 **비활성**으로 둔다
>   (fail-closed). v2를 발행하는 호출처만 켠다.
> - **publication unit은 시드된다.** `infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql`이
>   INSERT 한다. 활성화 트랜잭션은 단위를 만들지 않고 **없으면 실패**한다.
> - **CAS 판정에서 `serving_generation` 컬럼을 그대로 믿지 말 것.** 기본값이 1이므로
>   `active_release_id IS NULL`인 단위도 1을 들고 있다. 첫 발행은 `expected_*` 둘 다 `None`이어야
>   하고 다음 세대는 1이다. 그 외에는 관찰값 + 1이다.
> - **반환값을 위한 리더가 트랜잭션을 받지 못한다.** `sqlx_repository.rs`의
>   `get_active_vector_tile_runtime_manifest`(193줄)가 `&self.pool`에 묶여 있다. 커밋 뒤 pool로
>   다시 읽으면 그 사이 다른 승격이 끼어들 수 있으므로, 본문을 `&mut sqlx::PgConnection`을 받는
>   함수로 먼저 추출해 두 호출처가 같은 정의를 쓰게 한다. **이것이 트랜잭션보다 앞선 작업이다.**

<!-- 두 인용은 별개다 (MD028) -->

> **Increment C·D 결과 (2026-07-30):** `mark_tile_layer_dynamic` 하나를 세로로 뚫고, 뚫는 과정에서
> 드러난 게이트 결함을 고쳤다. 리더 추출, 트랜잭션,
> `catalog-infrastructure/tests/spatial_tile_publication.rs`(단위별 disposable DB 10개)까지 완료.
> 남은 네 명령(`start_vector_tile_build`, `record_vector_tile_build_result`,
> `promote_tile_layer_static`, `rollback_tile_layer_source`)은 여전히 port 기본 구현이 에러다.
>
> 뚫어 보고 드러난 것 세 가지 — 계획이 예상한 것과 다르다.
>
> 1. **게이트가 `serving_generation`의 의미를 어기고 있었고, 고쳤다.** gap 검사가 매니페스트에
>    선택된 **모든** 단위에 `unit.serving_generation + 1`을 요구했으므로, 한 단위만 바꾸는 발행이
>    이어받은 단위들의 세대까지 올렸다. 그러면 FP-ADR-0004의 단위별 폴링이 정보를 잃고, Step 1의
>    "서로 다른 두 단위가 둘 다 commit"이 성립하지 않는다 — 나중 writer의
>    `expected_serving_generation`이 자기와 무관한 발행에 의해 낡기 때문이다.
>    `20260730000003_serving_generation_tracks_one_unit_source_selection.sql`이 규칙을 전이별로
>    좁혔다: 첫 발행은 `1`, release가 바뀌면 `+1`, release를 재선택하면 **유지**.
>    근거: [ADR-0014](../adr/0014-serving-generation-tracks-one-unit-source-selection.md).
>    마이그레이션을 빼면 `activating_one_unit_carries_every_other_unit_into_the_new_manifest`와
>    `two_edits_to_different_units_both_commit_and_advance_the_global_generation_in_order`가
>    `has a serving-generation gap`으로 실패한다 — 음성 대조로 확인했다.
> 2. **두 번째 publication unit은 "시드 후 즉시 활성화" 순서만 가능하다.** 단위를 추가하고 발행하지
>    않은 채 두면 다른 어떤 단위의 활성화도 완전성 검사에서 실패한다. 시드가 단위를 만들고 활성화가
>    첫 발행을 하는 순서가 강제된다.
> 3. **CAS 충돌에 retry loop를 두지 않았다.** Step 6 본문은 "충돌하면 최신 pointer부터 retry"라고
>    쓰지만, 트랜잭션이 pointer 관계에 `SHARE ROW EXCLUSIVE`를 **먼저** 잡으면 동시성으로 인한 CAS
>    충돌 자체가 생길 수 없다(같은 락을 SQL 함수도 잡으므로 모든 pointer 이동이 이 락에서 직렬화된다).
>    남는 충돌은 호출자의 관찰이 낡은 경우뿐이고, 그것은 retry하면 안 된다 — 호출자가 보지 못한 상태
>    위에 덮어쓰게 된다. retry를 대기로 바꾼 것이므로 lost-update 창이 없다.
>
> **Step 1의 두 interleaving은 이제 테스트가 있다.** 같은 단위 경합은
> `two_edits_to_one_unit_from_one_observed_state_leave_exactly_one_winner`(정확히 하나 commit, 패자는
> typed `VectorTileServingStateConflict`), 서로 다른 단위 경합은
> `two_edits_to_different_units_both_commit_and_advance_the_global_generation_in_order`(둘 다 commit,
> 각 매니페스트에 두 selection이 정확히 한 번, 전역 generation 순서대로 두 번 증가)가 담당한다.
> 후자는 database serialization failure가 아니라 **나중 매니페스트가 앞선 writer의 새 release를
> 이어받았다는 사실**로 lock order를 증명한다. Step 1이 여전히 미완인 이유는 완전한 PostGIS
> projection 행 단정이 남았기 때문이며, 그 행을 쓰는 것은 Task 7 Step 4다.
>
> 미해결(범위 밖): **`idempotency_key`를 저장할 자리가 스키마에 없다.** `vector_tile_build_job`은
> `(publication_unit_id, idempotency_key)`를 갖지만 활성화용 원장은 없다. Step 5가 요구한 "재시도가
> 첫 결과를 돌려준다"는 아직 아니며, 추가 마이그레이션 + ADR이 필요하다.
>
> > **정정 (2026-07-30):** 이 단락은 처음에 "release 유니크 키가 재시도를 거부한다"고 썼고 근거로
> > `a_replayed_activation_leaves_no_partial_state`를 들었다. 둘 다 틀렸다. 동일한 본문의 재시도는
> > `expected_active_release_id`가 여전히 `None`이므로 **CAS**가 거부한다 —
> > `a_stale_observation_loses_the_compare_and_swap_and_moves_nothing`의 첫 분기가 그것이다. 그
> > 테스트는 첫 호출에 `None`, 두 번째에 `Some((release, 1))`을 보내므로 **본문이 다른 요청**이었고,
> > 재시도를 시험한 적이 없다. 지금은
> > `re_publishing_one_revision_after_observing_it_leaves_no_partial_state`로 이름을 고쳤고, 그것이
> > 실제로 덮는 것은 "관찰을 갱신해 같은 리비전을 다시 발행하려는 시도"다. 이중 발행이 불가능하다는
> > 결론 자체는 유지되지만 막는 주체가 다르다.

- [ ] **Step 7: Run unit and database integration tests**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p catalog-application -p catalog-infrastructure spatial_tile_publication
scripts/verify/integration.sh foundation
```

예상 결과: 같은 unit의 동시 writer/promoter 중 정확히 하나가 성공하고 서로 다른 unit의 writer는
어느 selection도 잃지 않고 serialize되며 rollback은 data revision을 바꾸지 않는다.

- [x] **Step 8: Commit**

```bash
git add platforms/foundation-platform/crates/catalog/catalog-application \
  platforms/foundation-platform/crates/catalog/catalog-infrastructure
git commit -m "feat(foundation): atomically switch complete tile sources"
```

## Task 7: Connect WAP Candidates and the Complete Dynamic Projection

**Files:**
- Modify: `platforms/foundation-platform/crates/lakehouse/lakehouse-application/src/ports.rs`
- Create: `platforms/foundation-platform/crates/lakehouse/lakehouse-infrastructure/src/spatial_tile_wap.rs`
- Modify: `platforms/foundation-platform/crates/lakehouse/lakehouse-infrastructure/src/lib.rs`
- Create: `platforms/foundation-platform/crates/lakehouse/lakehouse-infrastructure/tests/spatial_tile_wap.rs`
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_projection.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs`
- Test: `platforms/foundation-platform/services/foundation-outbox-publisher/src/main_command_tests.rs`

- [ ] **Step 1: Write the failed-candidate isolation test**

branch C10을 준비하고 Catalog 활성화를 강제로 실패시킨 뒤 C11을 준비한다. C11은 C10이
아니라 아직 선택된 Catalog snapshot에서 분기되는지 확인한다. 선택되지 않은 branch는
제한된 만료 대상이어야 한다. 선택된 snapshot ID를 결과 release에 저장하고 다른 snapshot을
지정한 활성화 증거는 거부한다.

- [ ] **Step 2: Write the projection readiness test**

공개 명령은 후보 snapshot의 모든 예상 필지가 존재하고 geometry가 유효하며 projection이
같은 data revision을 보고할 때만 serving generation을 커밋해야 한다.

- [x] **Step 3: Implement the provider-neutral WAP port**

Expose only:

```rust
prepare_candidate(base_snapshot, change_set, release_id)
validate_candidate(candidate)
retain_selected(candidate)
expire_unselected(candidate)
fast_forward_main(selected_snapshot)
```

어댑터는 증명된 Spark 작업을 호출한다. 제공자별 Cloudflare API 호출을 추가하지 않는다.

- [ ] **Step 4: Implement projection activation**

후보 branch의 필지 행을 staging에 넣고 count/geometry/SRID를 검증한 뒤
영향을 받은 완전한 projection을 만들어 Task 6 transaction에서 release를 활성화한다. 전국
rebuild는 staging과 atomic replacement를 사용해야 하며 active table을 먼저 `TRUNCATE`하지 않는다.
canonical Iceberg geometry는 WKB/SRID 4326이고 serving projection은 SRID 5179다. 다음으로 decode한다.
`ST_GeomFromWKB(..., 4326)`, reject invalid/non-polygonal input, transform with `ST_Transform(...,
5179)`, normalize to `MultiPolygon`, and assert the transformed geometry's SRID before the swap. Read
contract가 소유한 current-row predicate로 선택된 row만 읽고 어떤 `pnu`도 current row가 0개 또는
2개 이상이면 실패한다. Integration fixture에는 superseded SCD2 row를 넣고 그것이
`serving_postgis.parcel_boundary_publication`에 절대 들어가지 않음을 증명한다.

자동화와 종단 간 proof가 같은 Rust use case를 호출하도록 이 경계를
`foundation-outbox-publisher activate-spatial-tile-candidate --evidence-json <path>`로 노출한다.
임의 SQL이나 geometry argument가 아니라 검증된 evidence만 받는다.

- [ ] **Step 5: Add reconciliation**

Catalog 활성화 후 Iceberg `main`은 선택한 snapshot의 ancestry를 따라갈 때만 fast-forward한다.
`main`과 보존 릴리스에서 더 이상 필요하지 않을 때까지 선택된 branch를 유지한다.

- [ ] **Step 6: Run tests and commit**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p lakehouse-infrastructure -p foundation-outbox-publisher spatial_tile
git add platforms/foundation-platform/crates/lakehouse \
  platforms/foundation-platform/services/foundation-outbox-publisher
git commit -m "feat(foundation): activate WAP spatial tile revisions"
```

## Task 8–13

Task 8부터 13까지는 [뒷부분](./single-source-spatial-publication-implementation-tasks-8-13.md)에 있다.

