<!-- public-repository-safety: reviewed-public-contract -->

# Single-Source Spatial Publication Implementation Guide

**Status:** Approved delivery sequence; implementation pending. Steps use checkbox (`- [ ]`) syntax
for auditable task tracking.
**Architecture contract:** [Single-source spatial publication](../architecture/single-source-spatial-publication.md)

**Goal:** Deliver one production-shaped `parcels` vertical slice in which Foundation keeps canonical geometry on R2/Iceberg, Martin serves one complete PostGIS or PMTiles source at a time, stale builds cannot promote, and an already-open Gongzzang map observes a committed source change within five seconds.

**Architecture:** Foundation Catalog owns an immutable release ledger, per-unit active release, and one global runtime-manifest generation. Public edits are prepared on an isolated Iceberg WAP branch, activated with the complete PostGIS projection in one Catalog transaction, and served dynamically by Martin. Static builds use an isolated PostGIS build database for one exact release, upload an immutable PMTiles object create-only, wait for Martin's R2-prefix hot reload, and CAS-promote the complete static release. Gongzzang polls the Catalog manifest with ETag and changes one complete vector source; it never composes a static base with feature tombstones.

**Tech Stack:** Rust 1.96.0 in the pinned Docker image, PostgreSQL 17/PostGIS 3.5, Apache Iceberg/Spark WAP, Martin 1.12.0, MBTiles/PMTiles, Cloudflare R2 S3 API, Axum/SQLx, TypeScript/Zod, Naver Maps' bundled mapbox-gl, Vitest, and Playwright.

---

## File and Responsibility Map

### Decisions and operator contract

- Modify `docs/adr/0006-object-storage-first-serving.md` — root serving decision and single-source invariant.
- Modify `platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md` — Foundation manifest v2 and publication ownership.
- Modify `platforms/foundation-platform/docs/adr/0006-lakehouse-table-format-and-serving-architecture.md` — WAP-selected canonical snapshot and derived serving roles.
- Modify `products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md` — v1/v2 consumer migration and five-second refresh.
- Modify `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md` — exact build, discovery, promotion, rollback, and R2 procedures.

### Browser capability and Gongzzang consumer

- Modify `products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts` — prove the actual bundled SDK can reload vector tiles.
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
  — SHA-256 pin for the exact provider contract accepted by Gongzzang.

### Foundation contract and publication state

- Modify `platforms/foundation-platform/crates/catalog/catalog-domain/src/vector_tile.rs` — preserve v1 flat-layout types.
- Create `platforms/foundation-platform/crates/catalog/catalog-domain/src/serving_publication.rs` — publication aggregate and transition invariants.
- Modify `platforms/foundation-platform/crates/catalog/catalog-domain/src/{errors.rs,lib.rs}`.
- Modify `platforms/foundation-platform/crates/foundation-contracts/src/catalog.rs` — manifest v2 and command/response DTOs.
- Modify `platforms/foundation-platform/crates/foundation-shared-kernel/src/{ids.rs,events/catalog_v1.rs}` — typed IDs, additive v2 event, and byte-compatibility tests.
- Modify tests:
  - `platforms/foundation-platform/crates/catalog/catalog-domain/tests/vector_tile_manifest.rs`
  - `platforms/foundation-platform/crates/foundation-contracts/tests/vector_tile_manifest_dto.rs`

### Database and application transaction

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
- Update the two `CatalogUnitOfWork` test fakes named in Task 6.

### Iceberg WAP and serving projections

- Modify `platforms/foundation-platform/crates/lakehouse/lakehouse-domain/src/lakehouse.rs` and tests
  — Rust SSOT for the existing SCD2 parcel contract's current-row predicate.
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
- Add the logged, complete parcel publication projection in the migration above.
- Create `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_projection.rs`.

### Static builder, R2, and Martin

- Create `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_build.rs`.
- Create `platforms/foundation-platform/services/foundation-outbox-publisher/src/tile_public_object_storage.rs`.
- Modify `platforms/foundation-platform/services/foundation-outbox-publisher/src/r2_layout.rs`.
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

This is a hard stop gate. Do not implement the Foundation backend until the actual Naver SDK bundle
proves one supported reload path.

**Files:**
- Modify: `products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts`
- Test: `products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts`

- [ ] **Step 1: Add a failing probe for the preferred `setTiles` path**

After registering a small vector source, inspect the actual source object and require a callable
`setTiles` before attempting it:

```ts
type ReloadableVectorSource = {
  setTiles?: (tiles: string[]) => void;
};

const source = mapbox.getSource(sourceId) as ReloadableVectorSource | undefined;
expect(source).toBeDefined();
```

The probe must record the first tile URL, call `setTiles([secondUrl])` when available, and observe a
network request for `secondUrl`. Merely finding a method name is not sufficient.

- [ ] **Step 2: Add bounded fallback probes**

If `setTiles` is absent or ineffective, test in order:

1. `removeLayer` + `removeSource` + deterministic re-add while preserving center/zoom;
2. controlled Naver map reinitialization while preserving center/zoom and click registration.

Record exactly one selected strategy. Do not add a service worker or manipulate mapbox internals
beyond methods present on the live object.

- [ ] **Step 3: Run the real browser probe**

Run:

```bash
pnpm -C products/gongzzang/apps/web probe:naver --grep "vector source reload"
```

Expected: PASS with evidence naming `setTiles`, `remove-and-add`, or `map-reinitialize`, plus a request
for the second tile URL within five seconds.

If credentials or the Naver test page are unavailable, stop and obtain them. If all three strategies
fail, stop and return to architecture review.

- [ ] **Step 4: Commit the capability evidence**

Commit only the probe, not screenshots, API keys, or generated traces:

```bash
git add products/gongzzang/apps/web/tests/probes/naver-sdk.probe.ts
git commit -m "test(gongzzang): prove vector source reload capability"
```

## Task 2: Prove Iceberg WAP on the Selected REST Catalog

This is the second hard stop gate. It prevents building a custom branch/pointer system around a
provider that does not implement the standard Iceberg contract.

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

- [ ] **Step 1: Write failing Rust-SSOT and Spark job contract tests**

Extend the Rust contract test first: `LakehouseTableContract` has an optional machine-readable
`current_row_predicate`; every existing contract is `None` except `SILVER_PARCEL_BOUNDARIES`, whose
value is exactly `valid_to_utc IS NULL`. Update the artifact drift test so the derived JSON field must
match the Rust value.

Then require the Spark job to:

- target the existing canonical `silver.parcel_boundaries` contract rather than inventing a second
  parcel-geometry table;
- load the exact current-row predicate `valid_to_utc IS NULL` from that contract; no producer or
  projection may carry its own handwritten SCD2 selector;
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

- [ ] **Step 2: Run the tests and observe both missing-contract and missing-job failures**

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

- [ ] **Step 3: Extend the Rust SSOT and regenerate its Spark artifact**

Add the optional field to `LakehouseTableContract`, initialize it explicitly on every static contract,
set the parcel value once in Rust, then update the JSON derived artifact. The artifact drift test is the
mechanical guard: Python may read the JSON value but no Python or SQL file may redefine the predicate.

- [ ] **Step 4: Implement the thin Spark WAP job**

Use Iceberg SQL branch operations rather than editing metadata files:

```sql
ALTER TABLE <table> CREATE BRANCH `<branch>` AS OF VERSION <snapshot> RETAIN 7 DAYS;
-- write MERGE/DELETE operations to <table>.branch_<branch>
CALL <catalog>.system.fast_forward('<namespace.table>', 'main', '<branch>');
```

Validate identifiers before interpolation. Keep `prepare`, `validate`, and `fast-forward` as separate
commands so a failed candidate cannot publish itself.

- [ ] **Step 5: Add the Rust command wrapper**

Add:

```text
foundation-outbox-publisher probe-spatial-tile-wap
```

Follow the existing `remote_lakehouse_job` boundary: the Rust command validates inputs and emits a
secret-free execution plan for the pinned `compose.lakehouse.yml` Spark service. The host/runner
executes that plan; the Rust container never receives a Docker socket. A second validation step reads
the resulting evidence JSON, rejects an unexpected table/snapshot/branch/result, and records it below
`target/spatial-tile-publication/`. Neither plan nor evidence may contain a catalog token.

- [ ] **Step 6: Run offline contract tests**

These tests prove identifier validation, SQL construction, evidence parsing, and secret redaction. They
are not allowed to claim provider capability:

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

With bucket-scoped test catalog credentials from environment only, run the command against a dedicated
test namespace/table. Never use production Gold tables. The live provider probe is the sole integration
gate; do not substitute a mock, JDBC catalog, or local metadata directory for this step:

```bash
cd platforms/foundation-platform
docker compose -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \
  spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
  /workspace/infra/lakehouse/spark/jobs/spatial_tile_publication_wap.py \
  probe --namespace tiles_slice_proof --table parcel_boundaries_wap_probe
```

Expected: `provider=cloudflare-r2-data-catalog branch_isolation=ok fast_forward=ok`.

If Cloudflare's beta provider fails, stop. Record the failure and choose a conforming Iceberg REST
Catalog provider while keeping Parquet/Iceberg data on R2; do not emulate WAP with ad-hoc object keys.

- [ ] **Step 8: Commit the provider-neutral capability slice**

```bash
git add platforms/foundation-platform/infra/lakehouse/spark \
  platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json \
  platforms/foundation-platform/crates/lakehouse/lakehouse-domain \
  platforms/foundation-platform/services/foundation-outbox-publisher/src
git commit -m "test(foundation): prove Iceberg WAP publication"
```

## Task 3: Reconcile the Accepted ADRs

Only perform this task after Tasks 1 and 2 pass.

**Files:**
- Modify: `docs/adr/0006-object-storage-first-serving.md`
- Modify: `platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md`
- Modify: `platforms/foundation-platform/docs/adr/0006-lakehouse-table-format-and-serving-architecture.md`
- Modify: `products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md`
- Modify: `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md`

- [ ] **Step 1: Replace feature overlay language with the single-source invariant**

Document:

```text
one publication unit + one manifest generation -> exactly one complete Martin source
```

State explicitly that Martin composite, client feature tombstones, and custom MVT
decode/filter/re-encode are not the Foundation polygon path.

- [ ] **Step 2: Define the three independent versions**

- `data_revision`: canonical feature content;
- per-unit `serving_generation`: selected release/source;
- global `manifest_generation` and immutable `current_version`: polling/ETag.

- [ ] **Step 3: Record WAP, isolated build DB, prefix hot reload, and same-data rollback**

Link the official Iceberg branching and Martin PMTiles hot-reload documentation. Document that data
revert creates a new canonical revision; serving rollback never changes business data.

- [ ] **Step 4: Run documentation and monorepo guards**

Run:

```bash
git diff --check
"C:/Program Files/Git/bin/bash.exe" scripts/guard/monorepo-guard.sh
```

Expected: PASS. On Windows linked worktrees, do not invoke WSL's `/usr/bin/git` against a Windows
`.git` pointer.

- [ ] **Step 5: Commit the reconciled decision**

```bash
git add docs/adr/0006-object-storage-first-serving.md \
  platforms/foundation-platform/docs/adr \
  products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md \
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

- [ ] **Step 1: Write failing v2 domain and DTO tests**

Test that:

- v1 still requires `{object_key_prefix}/{z}/{x}/{y}` and flat statistics;
- v2 has one top-level `manifest_generation`;
- each artifact has `data_revision`, `serving_generation`, `publication_unit`,
  `active_source`, immutable `canonical_iceberg_snapshot_id`, per-artifact
  `tiles_url_template`, and lowercase `feature_id_property`;
- `static_pmtiles` requires immutable object key, bytes, checksum, and Martin source ID;
- `dynamic_postgis` rejects PMTiles-only fields;
- unknown schema versions are rejected.

- [ ] **Step 2: Run the tests and verify the missing-v2 failure**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p catalog-domain -p foundation-contracts vector_tile
```

Expected: FAIL because the v2 types do not exist.

- [ ] **Step 3: Implement the domain types**

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

Keep `TilesUrlTemplate` as the v1 flat-layout type. Add a separate v2 tile URL type that requires
`{z}`, `{x}`, and `{y}` but not `{object_key_prefix}`.

- [ ] **Step 4: Implement additive v2 DTOs**

Do not deserialize future versions through `z.number().min(1)`-style permissiveness. Rust and
TypeScript must both dispatch exactly on schema version `1` or `2`.

- [ ] **Step 5: Define the additive v2 Catalog event before any application uses it**

Add a `VectorTileRuntimeManifestPublishedV2` payload carrying the immutable manifest ID, global
generation, selected releases with their canonical Iceberg snapshot IDs, and projection key. Keep
every existing v1 enum tag and serialized byte fixture unchanged. Add round-trip and golden-byte tests
proving old events deserialize identically and the v2 event cannot omit its generation/release/snapshot
set. Task 9 will consume this event; it must not define a second event shape.

- [ ] **Step 6: Run the package tests**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p catalog-domain -p foundation-contracts -p foundation-shared-kernel vector_tile
```

Expected: PASS for both v1 compatibility and v2 validation.

- [ ] **Step 7: Commit**

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

- [ ] **Step 1: Fix the additive-migration guard test first**

Change the deploy contract so it hashes the four named 2026-07-19 baseline files and separately
asserts they remain present. Remove only the assertion that the directory contains exactly four
migrations. Add a test proving a fifth correctly named migration is permitted.

- [ ] **Step 2: Write the failing state-machine tests**

Cover:

```text
legacy/no active release -> dynamic release 1
dynamic data revision A -> static release A
static data revision A -> dynamic data revision B
static release A -> same-data dynamic fallback A
static data revision A -> old data revision Z  (reject)
```

- [ ] **Step 3: Add normalized publication tables**

The migration must create:

```sql
catalog.vector_tile_publication_unit
catalog.vector_tile_release
catalog.vector_tile_build_job
catalog.vector_tile_refresh_observation
```

Required database constraints:

- unique lowercase logical `layer_id`;
- positive per-unit `serving_generation` and global manifest generation;
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
  outcome, probe environment, and evidence checksum—never user, PNU, camera, or IP data; their
  idempotency key is unique.

Use a composite unique key on release `(id, data_revision, canonical_iceberg_snapshot_id)` and a
composite foreign key from the build input triple so the snapshot binding is enforced by PostgreSQL,
not only by Rust.

Extend `catalog.vector_tile_manifest` and `catalog.vector_tile_artifact` additively with
`schema_version`, global generation, release reference, v2 transport fields, and conditional checks.
Make flat-only columns nullable only behind a check that requires them for schema v1/`flat_mvt`.

- [ ] **Step 4: Add the logged complete parcel projection**

Create `serving_postgis.parcel_boundary_publication` as a logged serving projection with canonical
lowercase `pnu`, `official_complex_code`, `data_revision`, and `geometry(MultiPolygon,5179)`, plus
primary/GiST indexes. Do not remove or mutate `serving_postgis.parcel_boundary_mirror`.

- [ ] **Step 5: Run migration and domain tests**

```bash
scripts/verify/integration.sh foundation
```

Expected: all migrations apply as the least-privilege migrator, state constraints reject invalid
fixtures, and the existing catalog/listing paths remain intact.

- [ ] **Step 6: Commit**

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

Start two edits from the same expected release. Assert exactly one activation commits and the loser
receives a typed version conflict. Assert the winning transaction updates:

- complete PostGIS projection rows;
- immutable dynamic release;
- unit active pointer and serving generation;
- global immutable manifest and generation;
- additive v2 outbox event.

No partial state may remain if any insert fails.

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

Reject a build whose claimed frozen snapshot does not equal the immutable snapshot on its input
release. With the v2 capability disabled, the same activation must update internal publication state
but emit no v2 public event; existing v1 event/projection behavior remains byte-identical. With the
capability enabled, exactly one v2 event is recorded in the same transaction.

- [ ] **Step 4: Write the serving-rollback test**

Promote static release S11 and roll back to retained dynamic release R11 with the same data revision.
Assert a fallback with a different data revision is rejected.

- [ ] **Step 5: Implement application commands and ports**

Every mutation command includes `expected_active_release_id`, `expected_version`,
`canonical_iceberg_snapshot_id`, and idempotency key. Promotion additionally includes
`input_release_id`, `frozen_source_snapshot_id`, and candidate validation digest. Inject an explicit
typed `RuntimeManifestPublicationCapability`; domain/application code must not read environment
variables directly.

- [ ] **Step 6: Implement one SQLx transaction boundary**

Reuse the existing `FOR UPDATE`/CAS/outbox pattern in
`catalog-infrastructure/src/unit_of_work.rs`. Build the complete global manifest while the rows are
locked; never update an R2 pointer inside the transaction.

- [ ] **Step 7: Run unit and database integration tests**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p catalog-application -p catalog-infrastructure spatial_tile_publication
scripts/verify/integration.sh foundation
```

Expected: exactly one concurrent writer/promoter succeeds; rollback cannot change data revision.

- [ ] **Step 8: Commit**

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

Prepare branch C10, force the Catalog activation to fail, then prepare C11. Assert C11 branches from
the still-selected Catalog snapshot, not C10. The unselected branch must be eligible for bounded
expiry. Persist the selected snapshot ID on the resulting release and reject any activation evidence
that names a different snapshot.

- [ ] **Step 2: Write the projection readiness test**

The public command must not commit a serving generation unless every expected parcel for the
candidate snapshot is present, geometry is valid, and the projection reports the same data revision.

- [ ] **Step 3: Implement the provider-neutral WAP port**

Expose only:

```rust
prepare_candidate(base_snapshot, change_set, release_id)
validate_candidate(candidate)
retain_selected(candidate)
expire_unselected(candidate)
fast_forward_main(selected_snapshot)
```

The adapter invokes the proven Spark job. Do not add provider-specific Cloudflare API calls.

- [ ] **Step 4: Implement projection activation**

Load the candidate branch's parcel rows into staging, validate counts/geometry/SRID, then replace the
affected complete projection and activate the release in the Task 6 transaction. A national rebuild
must use staging and atomic replacement; never `TRUNCATE` the active table first. The canonical
Iceberg geometry is WKB/SRID 4326, while the serving projection is SRID 5179: decode with
`ST_GeomFromWKB(..., 4326)`, reject invalid/non-polygonal input, transform with `ST_Transform(...,
5179)`, normalize to `MultiPolygon`, and assert the transformed geometry's SRID before the swap. Read
only rows selected by the contract-owned current-row predicate and fail if any `pnu` has zero or more
than one current row. Integration fixtures must contain superseded SCD2 rows and prove they never
enter `serving_postgis.parcel_boundary_publication`.

Expose this boundary as
`foundation-outbox-publisher activate-spatial-tile-candidate --evidence-json <path>` so automation
and the end-to-end proof invoke the same Rust use case. It accepts validated evidence, not arbitrary
SQL or geometry arguments.

- [ ] **Step 5: Add reconciliation**

After Catalog activation, fast-forward Iceberg `main` only along the selected snapshot ancestry.
Retain selected branches until `main` and retained releases no longer need them.

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

## Task 8: Build One Frozen PMTiles Release and Discover It Through Martin

Static builds must not read the mutable public PostGIS mirror.

**Files:**
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_build.rs`
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/tile_public_object_storage.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/{main.rs,main_command_tests.rs,r2_layout.rs}`
- Modify: `scripts/tiles/{compose.yaml,martin-dynamic.yaml,martin-static.yaml,tiles-slice-proof.sh}`
- Test: `platforms/foundation-platform/services/foundation-api/tests/tiles_slice_harness_contract.rs`

- [ ] **Step 1: Write the frozen-build concurrency test**

Start an isolated build PostGIS database from release R20, begin `martin-cp`, then activate edit R21
in the live database. Decode the resulting archive and assert it contains exactly R20 and its persisted
`canonical_iceberg_snapshot_id`, never a mixture. Include superseded SCD2 rows in the frozen input and
prove none enter the archive.

- [ ] **Step 2: Write the R2 storage-boundary tests**

Require:

- `FOUNDATION_TILE_PUBLIC_R2_*` configuration separate from generic/lakehouse/recovery R2 variables;
- bucket/prefix allow-list;
- create-only `If-None-Match: *`;
- immutable UUID filename;
- no delete or overwrite command;
- read-only Martin credentials scoped to the same public tile prefix.

- [ ] **Step 3: Implement the isolated build database**

Follow the same safe control/data boundary as Task 2. The Rust command emits a checksum-addressed,
secret-free build plan containing the input release, data revision, exact frozen Iceberg snapshot,
expected current-row contract digest, pinned tool image digests, bounded zooms, and output paths.
The host `tiles-slice-proof.sh`/Compose runner executes that plan in disposable containers; the
publisher container never invokes Docker and never receives a Docker socket. A separate Rust command
validates the execution receipt and artifacts before recording the build result.

The executable command surface is:

```text
foundation-outbox-publisher plan-spatial-tile-build --unit parcels
foundation-outbox-publisher record-spatial-tile-build-result --receipt-json <path>
foundation-outbox-publisher promote-spatial-tile-build --build-id <uuid> --expected-release <uuid>
```

The Compose runner creates a disposable PostGIS database, imports only rows selected by the
contract-owned current-row predicate from the exact frozen snapshot, applies explicit Martin views,
and runs all zoom passes there. Reuse the proof's pinned PostGIS/Martin images. Do not grant the
builder DDL rights on the live serving database.

- [ ] **Step 4: Implement the standard OSS build chain**

```text
frozen PostGIS
  -> martin-cp
  -> MBTiles validate
  -> go-pmtiles convert
  -> go-pmtiles verify
  -> MVT identity/feature validation
```

The host runner executes these pinned OSS tools. Rust owns plan/receipt validation and state
transitions; it does not implement MVT encoding or process control through a Docker socket.

For this slice every build input and route is the single `parcels` source. Remove comma-separated
`parcels,parcel_anchor_aggregate,parcel_anchor` `martin-cp` inputs and composite polygon URLs. Existing
anchor sources may remain direct legacy sources, but they are not members of the `parcels` publication
unit or archive. Add a guard rejecting `,` in a Foundation polygon artifact's source URL and asserting
the PMTiles TileJSON contains exactly the `parcels` source layer.

- [ ] **Step 5: Make mutable dynamic tiles impossible to serve from an old cache key**

Set `cache: disable` on `martin-dynamic`; Martin 1.12 otherwise enables an in-process tile cache by
default. Require every dynamic v2 URL to include the exact serving generation in its query string or
path, and add a contract test that a generation change changes the full tile URL. At a CDN boundary,
either return `Cache-Control: no-store` for the dynamic route or mechanically prove the cache key
includes the generation parameter. The proof must fetch the same z/x/y before and after an add/modify/
delete transition and observe the new bytes without a purge. Static Martin keeps its immutable cache.

- [ ] **Step 6: Configure Martin remote-prefix discovery**

Replace the named static source with:

```yaml
pmtiles:
  paths:
    - ${FOUNDATION_TILE_PUBLIC_PMTILES_PREFIX}
  reload_interval: 5s
```

Configure R2's S3-compatible endpoint and read-only credentials through environment. In local mode,
use a watched local directory. Keep the pinned Martin 1.12.0 digest.

- [ ] **Step 7: Upload, discover, decode, then mark validated**

After create-only upload, poll Martin's catalog with a bounded timeout until the expected source ID
appears. Fetch representative z/x/y tiles, decode lowercase `pnu`, compare feature IDs/counts with the
dynamic release, and only then record the static candidate as validated.

- [ ] **Step 8: Run local and real-R2 proof modes**

```bash
scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
scripts/tiles/tiles-slice-proof.sh
```

Expected local output includes `DYNAMIC cache isolation OK` and `STATIC prefix hot reload OK`. With
complete test credentials, expected output includes `REAL R2`, the unique object key, discovery
evidence, and decoded matching features. Production/lakehouse/recovery buckets must be rejected before
upload.

- [ ] **Step 9: Commit**

```bash
git add platforms/foundation-platform/services/foundation-outbox-publisher \
  scripts/tiles platforms/foundation-platform/services/foundation-api/tests
git commit -m "feat(foundation): publish immutable PMTiles releases"
```

## Task 9: Expose the Atomic Runtime Manifest and ETag

**Files:**
- Modify: `platforms/foundation-platform/services/foundation-api/src/routes/{catalog.rs,catalog_openapi.rs,catalog_tests.rs,mod.rs}`
- Modify: `platforms/foundation-platform/services/foundation-api/src/state.rs`
- Modify: `platforms/foundation-platform/crates/foundation-contracts/src/catalog.rs`
- Modify: `platforms/foundation-platform/docs/openapi/catalog.v1.json`
- Modify: `platforms/foundation-platform/crates/foundation-outbox/src/vector_tile_manifest.rs`
- Test: `platforms/foundation-platform/crates/foundation-outbox/tests/{vector_tile_manifest_pointer.rs,publish_roundtrip.rs}`

- [ ] **Step 1: Write failing API tests**

Assert:

- `GET /catalog/v1/vector-tiles/runtime-manifest` returns one complete v2 manifest;
- `ETag` is a standards-compliant quoted entity tag containing the immutable `current_version`;
- matching `If-None-Match` returns 304 with no body;
- a per-unit activation changes global `manifest_generation`, `current_version`, and ETag;
- response uses `Cache-Control: no-cache, must-revalidate`;
- database manifest is visible immediately even if R2 pointer projection is delayed.
- the default-disabled v2 capability gate cannot serve v2, emit a v2 public event, or project v2 to
  R2 until explicitly enabled;
- CORS preflight accepts `If-None-Match`, and the response exposes `ETag` to browser JavaScript.

- [ ] **Step 2: Implement the atomic read endpoint**

Read the active global manifest and all artifact release descriptors from one database snapshot.
Do not assemble part of the response from R2. Add
`FOUNDATION_TILE_RUNTIME_MANIFEST_V2_ENABLED=false` as a fail-closed default: while disabled, the new
runtime endpoint does not publish v2 and the accepted v1 endpoint remains unchanged. Enable it only
after Task 10's Gongzzang parser, provider-contract snapshot, and SHA pin have shipped.

Update the existing CORS layer to allow `header::IF_NONE_MATCH` and expose `header::ETAG`; add route
tests using a non-default allowed origin so this cannot regress silently.

- [ ] **Step 3: Add the additive v2 outbox projection**

Consume the v2 event defined and byte-tested in Task 4; do not define a second payload here. Keep the
v1 projection bytes unchanged and make the publisher skip stale v2 events by active manifest
ID/generation before writing `gold/manifest.json`. Apply the same typed, fail-closed publication
capability used by the HTTP endpoint at both transaction-time event emission and publisher-time
projection (defense in depth). Tests must prove that, while disabled, both HTTP and R2 keep their v1
bytes and no v2 event exists; enabling after Task 10 allows the next transition/reconcile to publish
the current v2 state.

- [ ] **Step 4: Regenerate and verify OpenAPI**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  "$RUST_TOOLCHAIN_IMAGE" \
  cargo run --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  --locked --quiet -p foundation-api --bin export-catalog-openapi -- \
  /workspace/platforms/foundation-platform/docs/openapi/catalog.v1.json
```

Expected: generated artifact matches the committed OpenAPI contract test.

- [ ] **Step 5: Run tests and commit**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p foundation-api -p foundation-outbox vector_tile
git add platforms/foundation-platform/services/foundation-api \
  platforms/foundation-platform/crates/foundation-contracts \
  platforms/foundation-platform/crates/foundation-outbox \
  platforms/foundation-platform/docs/openapi/catalog.v1.json
git commit -m "feat(foundation): publish atomic tile runtime manifest"
```

## Task 10: Implement the Gongzzang v2 Consumer and Four-Second Poll

**Files:**
- Modify: `products/gongzzang/apps/web/lib/map/vector-tile-manifest.ts`
- Create: `products/gongzzang/apps/web/lib/map/foundation-vector-layer-registry.ts`
- Modify: `products/gongzzang/apps/web/lib/config/layer-ids.ts`
- Create: `products/gongzzang/apps/web/lib/map/foundation-vector-source-refresh.ts`
- Modify: `products/gongzzang/apps/web/lib/map/listing-map-runtime.ts`
- Modify: `products/gongzzang/apps/web/components/listings/listing-map.tsx`
- Modify: `products/gongzzang/apps/web/proxy.ts`
- Modify: `products/gongzzang/apps/web/tests/unit/map/vector-tile-manifest.test.ts`
- Create: `products/gongzzang/apps/web/tests/unit/map/foundation-vector-source-refresh.test.ts`
- Modify: `products/gongzzang/crates/foundation-platform-client/openapi/catalog.v1.json`
- Modify: `products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json`

- [ ] **Step 1: Write strict manifest fetch tests**

Cover:

- exact schema versions 1 and 2 accepted; version 3 rejected;
- ETag retained and sent as `If-None-Match`;
- 304 returns the existing manifest without reparsing;
- v2 materializes each artifact's own tile URL;
- `//` cannot be produced;
- invalid UUID/current version, source layer, identity, or generation rejects the update.

- [ ] **Step 2: Write atomic source-refresh tests**

Use a fake mapbox bridge matching the capability selected in Task 1. Assert:

- only artifacts whose per-unit generation changed are retargeted;
- parcel source retains lowercase `promoteId: "pnu"`;
- the `parcels` v2 switch does not retarget or duplicate the existing direct legacy anchor sources;
- no old and new source remain together;
- a failed new source retains the last validated complete source;
- cleanup stops timers and aborts fetches.

- [ ] **Step 3: Consolidate the layer registry**

Move source IDs, source-layer expectations, `promoteId`, and style dependency groups into
`foundation-vector-layer-registry.ts`. Remove runtime string duplication. Do not add a building style
until product design exists; make adding a future registry entry require no publication-state copy.

- [ ] **Step 4: Implement conditional polling**

Poll at most every four seconds while mounted and visible, check immediately on visibility restore,
and stop on cleanup. Use the Catalog endpoint directly for freshness; R2 remains boot/distribution
projection only.

- [ ] **Step 5: Implement the proven reload strategy**

Use only the strategy proven by Task 1. Preserve style metadata across routine static/dynamic switches;
a manifest that changes `source_layer`, zoom, or `feature_id_property` requires full validated
re-registration.

- [ ] **Step 6: Run unit and full web tests**

```bash
pnpm -C products/gongzzang/apps/web test
pnpm -C products/gongzzang/apps/web probe:naver --grep "vector source reload"
```

Expected: all Vitest tests pass and the live probe observes the second tile URL within five seconds.

- [ ] **Step 7: Advance the Gongzzang provider-contract pin**

Copy the exact generated Foundation OpenAPI bytes from
`platforms/foundation-platform/docs/openapi/catalog.v1.json` into Gongzzang's
`crates/foundation-platform-client/openapi/catalog.v1.json`, update the pin's lowercase SHA-256, and
run `catalog_contract_pin`. The snapshot, pin, strict Zod parser, and source-refresh tests must land
before any environment enables Foundation's v2 capability gate. The existing contract-pin test is the
mechanical checksum guard; the Foundation flag is the fail-closed rollout gate. No intermediate task
commit is independently deployable with v2 enabled.

- [ ] **Step 8: Commit**

```bash
git add products/gongzzang/apps/web \
  products/gongzzang/crates/foundation-platform-client/openapi/catalog.v1.json \
  products/gongzzang/docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json
git commit -m "feat(gongzzang): refresh complete Foundation tile sources"
```

## Task 11: Prove the Complete State Machine End to End

**Files:**
- Modify: `scripts/tiles/{tiles-slice-proof.sh,fixture.sql,vector-tile-manifest.local.json,compose.yaml}`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/{tiles_slice_contract.rs,tiles_slice_harness_contract.rs}`
- Modify: `platforms/foundation-platform/infra/db/seeds/local_vector_tile_manifest.sql`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/local_vector_tile_seed_contract.rs`
- Create: `products/gongzzang/apps/web/tests/probes/foundation-vector-source-publication.probe.ts`
- Modify: `products/gongzzang/apps/web/playwright.probes.config.ts`

- [ ] **Step 1: Make the contract test fail on mixed sources**

Parse manifest v2 and assert each publication unit selects exactly one release/source. Cross-check
manifest artifact, Martin catalog source, source layer, feature ID, canonical data revision, and exact
Iceberg snapshot. Reject comma-separated/composite URLs for Foundation polygon units and require the
`parcels` PMTiles archive to expose exactly one `parcels` vector layer.
The harness must first prove the fail-closed default does not publish v2, then opt in with
`FOUNDATION_TILE_RUNTIME_MANIFEST_V2_ENABLED=true` only after the Task 10 consumer/pin tests pass.

- [ ] **Step 2: Add add/modify/delete fixtures**

Use three stable parcel IDs:

- add a new parcel;
- modify an existing parcel so old and new footprints differ;
- delete an existing parcel.

Expected dynamic response contains exactly the new desired set and never the old geometry.

- [ ] **Step 3: Prove stale-build rejection**

Start static build at R30, activate R31 before promotion, and assert:

```text
STATIC promote rejected expected_release=R30 current_release=R31
DYNAMIC tile OK generation=R31
```

Decode the R30 archive to prove it is internally consistent rather than mixed.

- [ ] **Step 4: Build and promote the current release**

Build R31, upload create-only, wait for Martin prefix discovery, decode it, then CAS-promote. Assert
dynamic R31 and static R31 have matching feature IDs and expected geometry hashes.

- [ ] **Step 5: Prove same-data serving rollback**

Roll back from static R31 to retained dynamic R31 and assert the data revision and feature set do not
change while global manifest generation advances.

- [ ] **Step 6: Prove the five-second client path**

Extend the Compose harness with the migrated Foundation API connected to the same Catalog/PostGIS
database; use Playwright's existing `webServer` boundary to run Gongzzang against that API and the two
Martin services. Pass the local Foundation base URL, v2 manifest URL, and Naver client ID explicitly
through `playwright.probes.config.ts`; never synthesize a production credential. The new Playwright
spec must:

1. open the real Naver-backed map with dynamic R31 and wait for a generation-addressed parcel tile;
2. invoke `promote-spatial-tile-build` through the harness's Rust control container while the page
   remains open;
3. read the Catalog commit timestamp/generation from the API response or persisted evidence;
4. observe the first network request for the static R31 source URL and its successful source-data
   event;
5. fail if the old and new parcel sources coexist or if elapsed time exceeds five seconds.

Do not satisfy this test with a mocked mapbox bridge; Task 10 unit tests own mocks. Run repeatedly;
every prelaunch proof run must complete within five seconds. Emit one redacted, checksum-addressed
`target/spatial-tile-publication/refresh-observations/<id>.json` record containing only generation,
commit time, first-tile time, duration, outcome, and test environment. Task 12 ingests the same schema;
screenshots, credentials, feature IDs, and camera coordinates are forbidden.

- [ ] **Step 7: Run twice for idempotency**

```bash
scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh
pnpm -C products/gongzzang/apps/web exec playwright test \
  --config playwright.probes.config.ts foundation-vector-source-publication.probe.ts
```

Expected both runs exit 0 with unique artifacts and no leaked containers. With R2 credentials, run
real mode twice and retain evidence; never overwrite or delete previous evidence.

- [ ] **Step 8: Commit**

```bash
git add scripts/tiles platforms/foundation-platform/infra/db/seeds \
  platforms/foundation-platform/services/foundation-api/tests \
  products/gongzzang/apps/web/tests/probes/foundation-vector-source-publication.probe.ts \
  products/gongzzang/apps/web/playwright.probes.config.ts
git commit -m "test: prove single-source tile publication lifecycle"
```

## Task 12: Add Scheduling, Readiness, Metrics, and Operator Runbook

**Files:**
- Create: `platforms/foundation-platform/scripts/tiles/reconcile-spatial-tile-publication.sh`
- Create: `platforms/foundation-platform/infra/systemd/foundation-spatial-tile-publication.service`
- Create: `platforms/foundation-platform/infra/systemd/foundation-spatial-tile-publication.timer`
- Create: `platforms/foundation-platform/services/foundation-outbox-publisher/src/spatial_tile_refresh_observation.rs`
- Modify: `platforms/foundation-platform/services/foundation-outbox-publisher/src/{main.rs,main_command_tests.rs}`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-application/src/ports.rs`
- Modify: `platforms/foundation-platform/crates/catalog/catalog-infrastructure/src/{sqlx_repository.rs,lib.rs}`
- Modify: `platforms/foundation-platform/services/foundation-api/src/{state.rs,routes/mod.rs}`
- Modify: `platforms/foundation-platform/services/foundation-api/src/routes/tests/health_and_metrics.rs`
- Modify: `platforms/foundation-platform/infra/observability/prometheus/foundation-api.rules.yml`
- Modify: `platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md`
- Modify: `platforms/foundation-platform/services/foundation-api/tests/deploy_contract.rs`
- Create: `scripts/tiles/active-map-refresh-soak.sh`

- [ ] **Step 1: Write command and deployment contract tests**

Require one Rust command SSOT:

```text
foundation-outbox-publisher reconcile-spatial-tile-publication
foundation-outbox-publisher publish-spatial-tiles-now --unit parcels
foundation-outbox-publisher record-spatial-tile-refresh-observation --evidence-json <path>
```

The shell/systemd wrapper invokes those commands and the exact pinned Compose argv emitted by
`plan-spatial-tile-build`; it contains no publication decisions and rejects any unrecognized service,
image digest, mount, or output path. The deployed Rust publisher remains Docker-socket-free.

- [ ] **Step 2: Implement debounce, nightly reconcile, and publish-now**

The command:

- keeps serving dynamic while building;
- coalesces queued changes;
- marks stale jobs superseded;
- waits for Martin discovery/validation;
- promotes only through Task 6 CAS;
- retries safely by idempotency key.

- [ ] **Step 3: Persist and export operational evidence**

The observation command validates the Task 11 evidence schema/checksum, rejects future timestamps or
negative durations, and inserts idempotently into `catalog.vector_tile_refresh_observation`. Build,
discovery, promotion, and projection timings remain persisted in the publication/build ledger; the
command-style publisher does not pretend to expose an in-process Prometheus endpoint.

Extend Foundation API's existing `/metrics` query path to derive cumulative counters and histogram
buckets from those database ledgers, including build result/duration, superseded builds, projection
lag, Martin discovery lag, promotion conflicts, manifest projection lag, and synthetic active-map
refresh duration/outcome. This database-to-API path is the single scrape boundary.

- [ ] **Step 4: Add readiness without making `/readyz` perform remote tile I/O**

Readiness fails when:

- dynamic is active and projection generation lags;
- static is active and the reconciler's bounded background probe has not recently decoded the exact
  Martin route;
- Catalog runtime manifest and active release disagree.

Add repository/state methods plus readiness tests for all three cases. `/readyz` reads the transactionally
recorded readiness evidence; it must not fetch R2/Martin or decode MVT on each health request.

- [ ] **Step 5: Add the prelaunch rolling SLO guard**

`active-map-refresh-soak.sh` repeatedly invokes the disposable browser proof, records each observation
through the Rust command, and computes a rolling 24-hour result. A test fixture must make the script
fail below 99% success or when any prelaunch sample exceeds five seconds. Add Prometheus rules over the
exported observation counters/histogram for `success_ratio_24h < 0.99` and five-second violations.

This is synthetic prelaunch/deployment evidence, not a false claim of real-user monitoring. The
runbook must block a production SLO claim until the same metric is fed by the standard Gongzzang
OpenTelemetry/Sentry RUM boundary; building that general frontend observability platform is outside
this spatial slice.

- [ ] **Step 6: Update the runbook**

Document:

- local and real-R2 commands;
- dedicated public static-tile bucket and read/write credential separation;
- WAP candidate retention/reconciliation;
- dynamic edit, nightly schedule, publish-now, static promotion, and same-data rollback;
- failure recovery without tombstones;
- exact stop conditions for future partitioning;
- truthful statement that real R2 is unproven when credentials/evidence are absent.
- v2 rollout order: deploy the dual-version Gongzzang consumer and exact OpenAPI pin first, then enable
  the Foundation capability gate;
- the dynamic cache invariant (`martin-dynamic cache: disable` plus generation-addressed CDN key);
- the difference between prelaunch synthetic evidence and a later production RUM SLO.

- [ ] **Step 7: Run deployment contract tests and commit**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo test --manifest-path /workspace/platforms/foundation-platform/Cargo.toml \
  -p foundation-api deploy_contract
git add platforms/foundation-platform/scripts/tiles \
  platforms/foundation-platform/infra \
  platforms/foundation-platform/crates/catalog \
  platforms/foundation-platform/services/foundation-api \
  platforms/foundation-platform/services/foundation-outbox-publisher \
  platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md \
  scripts/tiles/active-map-refresh-soak.sh
git commit -m "feat(foundation): operate spatial tile publication"
```

## Task 13: Run the Authoritative Verification and Review

**Files:**
- Verify all changed files.

- [ ] **Step 1: Run formatting/diff/secret checks**

```bash
git diff --check
scripts/ci/gitleaks-scan.sh
```

Expected: no whitespace errors or committed secrets.

- [ ] **Step 2: Run Foundation verification in the pinned Rust container**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo xtask verify foundation
scripts/verify/integration.sh foundation
```

Expected: PASS.

- [ ] **Step 3: Run Gongzzang verification in the pinned Rust container**

```bash
. tools/container-images.env
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e SQLX_OFFLINE=true "$RUST_TOOLCHAIN_IMAGE" \
  cargo xtask verify gongzzang
pnpm -C products/gongzzang/apps/web test
```

Expected: PASS.

- [ ] **Step 4: Run the local proof twice and real R2 when configured**

```bash
scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh
```

When dedicated test credentials are present:

```bash
scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
scripts/tiles/tiles-slice-proof.sh
```

Expected: dynamic add/modify/delete, stale-build rejection, static R2 prefix discovery, matching
features, same-data rollback, and five-second active-map refresh all pass.

- [ ] **Step 5: Request code review**

Require reviewers to check the architecture invariants, not just green test status.

- [ ] **Step 6: Confirm a clean branch**

```bash
git status --short --branch
git log --oneline --decorate -15
```

Expected: clean `feat/spatial-publication-state-machine`, with no commit on `main`.
