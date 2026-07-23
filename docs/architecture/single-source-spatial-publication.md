<!-- public-repository-safety: reviewed-public-contract -->

# Single-Source Spatial Publication Architecture

**Status:** Approved direction; implementation pending
**Date:** 2026-07-24
**Scope:** Foundation-owned public/reference polygon layers consumed by Gongzzang
**Delivery sequence:** [Single-source spatial publication implementation guide](../guides/single-source-spatial-publication-implementation.md)
**Freshness SLO:** A public change is reflected on an already-open map within 5 seconds after its
active release and serving generation are committed.

## 1. Decision

Each publication unit has exactly one complete active tile source at a time:

- `DYNAMIC`: Martin renders the complete current unit from the Foundation PostGIS serving mirror.
- `STATIC`: Martin serves one immutable, version-addressed PMTiles artifact from the dedicated public static-tile R2 bucket.

At launch, a publication unit is one logical layer such as `parcels`, `complex`, or `buildings`.
Static and dynamic representations of the same unit are never rendered together. A future ADR may
reduce the unit to non-overlapping Web Mercator partitions only after measurements justify it.

This supersedes the proposed Foundation-wide `static base + feature delta - tombstone` composition.
Martin composite sources combine tile payloads; they do not provide feature subtraction,
deduplication, or replacement precedence. Implementing those semantics in Gongzzang would duplicate
Foundation visibility policy in a consumer. Implementing them in a custom gateway would create a
custom tile engine.

## 2. Problem and Root Invariant

The root problem is not how to hide one polygon already baked into PMTiles. It is how to expose one
logically current map while preserving an immutable R2-first canonical history and allowing immediate
edits.

For every `(publication_unit, serving_generation)`:

1. A tile response contains zero or one current representation of each logical feature.
2. Every tile in the unit is read from one complete active source.
3. A source transition is visible only after the candidate source has been validated.
4. A stale static build cannot replace a newer dynamic revision.
5. Rollback selects a previously validated complete source; it does not reconstruct a mixture.
6. PMTiles, PostGIS, Martin caches, and the public manifest are projections, never independent
   authorities for canonical geometry or visibility.

These invariants eliminate double drawing, stale resurrection, feature-level suppression garbage
collection, and change loss during a static build.

## 3. Authority and SSOT

SSOT means one authority for each fact, not one physical storage system.

| Fact | Authority | Derived copies |
|---|---|---|
| Canonical geometry revision, feature identity, public lifecycle, and lineage | Foundation R2 + Apache Iceberg, mutated only through the Foundation Catalog contract | PostGIS mirror, PMTiles, MVT properties |
| Per-unit active release, serving generation, and serving rollback history | Foundation Catalog transaction/CAS state | Published manifest, Martin routes |
| Global runtime-manifest generation and ETag | Foundation Catalog global manifest pointer | R2 manifest projection, Gongzzang poll state |
| Tile bytes while dynamic | Complete PostGIS serving mirror through Martin | Martin/CDN caches |
| Tile bytes while static | Immutable PMTiles artifact selected by Catalog | Martin/CDN range cache |
| Listing visibility and marker state | Gongzzang listing write model | Gongzzang marker projections |

An approved public edit must be durably registered in the Foundation canonical revision and projected
into PostGIS before its serving generation becomes public. PostGIS remains reconstructible from the
canonical Foundation data and must not become the only copy of an edit. An unapproved preview may use
a staff-only workspace but is outside the public tile contract.

The public R2 manifest is a projection of Catalog state. It is not the place where operators manually
choose the active source.

## 4. Publication State Model

The active source and build job are separate facts:

```text
publication_unit
  data_revision                 # canonical data revision selected for public use
  serving_generation            # increments for every active-release change
  active_release_id            # immutable complete-source descriptor
  fallback_release_id?         # validated source for the same data_revision
  optimistic_version

runtime_manifest
  manifest_generation           # global, increments when any unit changes
  current_version               # immutable manifest UUID used as ETag

tile_build_job
  input_release_id
  input_data_revision
  frozen_source_snapshot_id
  status = QUEUED | BUILDING | VALIDATED | FAILED | SUPERSEDED | PROMOTED
  candidate_artifact_id
```

`BUILDING` is not a third serving source. While a static build runs, the unit continues serving its
complete dynamic source.

Each immutable release records its `data_revision`, canonical Iceberg snapshot, PostGIS projection
generation, source kind, versioned tile URL, validation evidence, and optional PMTiles artifact. A
dynamic release does not make mutable PostGIS rows into history; its canonical revision remains
reconstructible from Iceberg.

`data_revision` changes when public feature content changes. `serving_generation` changes whenever
Catalog selects another complete release, including a dynamic-to-static switch for the same data.
They are deliberately different values. `manifest_generation` is global and changes whenever any
publication unit changes, so one poll token can unambiguously detect independently updated units.

### 4.1 Public edit

1. Read the expected active release, Catalog-selected Iceberg snapshot, data revision, and optimistic
   version. New edits must branch from this selected snapshot, never from an unselected Iceberg
   `main` head.
2. Create a release-scoped Iceberg WAP branch from the selected snapshot, write the candidate there,
   and validate it. The branch has an explicit retention period and remains isolated from `main`.
3. In one Foundation database transaction, lock the publication unit; verify the expected active
   release/version; apply the candidate revision to the complete PostGIS serving projection; record a
   new immutable dynamic release; select it; increment `serving_generation`; and record the outbox
   event for the R2 manifest projection. The same transaction creates the new immutable global
   manifest and increments `manifest_generation`.
4. The Catalog runtime-manifest endpoint reads the new complete release directly from that committed
   transaction.
5. The outbox publisher asynchronously projects the same manifest to R2 for boot/distribution use.
6. Return public success only after steps 1-4 are durable and the dynamic source is ready.

Every public edit creates a new release, even when the unit was already dynamic. The per-unit row
lock, expected active release, and branch base snapshot serialize concurrent edits. The selected WAP
snapshot becomes the Catalog-authoritative canonical revision only when step 3 commits. A failed
activation leaves an unselected branch; later edits still branch from the Catalog-selected snapshot,
so failed candidates cannot leak into later public revisions.

A reconciler fast-forwards Iceberg `main` only along the ancestry of the currently Catalog-selected
snapshot. It retains every selected branch until `main` catches up and every retained release no
longer needs it; unselected branches expire after their bounded audit retention. This is Apache
Iceberg's standard
[Write-Audit-Publish/branch mechanism](https://iceberg.apache.org/docs/latest/branching/), not a
custom table format.

Before the publication backend is implemented, a bounded live capability probe must prove that the
selected Iceberg REST Catalog provider supports creating a branch at an exact snapshot, writing and
reading the branch, retaining it, and fast-forwarding `main`. Cloudflare R2 Data Catalog is a provider,
not the table-format SSOT. If its current beta implementation fails this standard Iceberg contract,
the implementation stops for a provider decision; it must not emulate branches with ad-hoc R2
pointers. Canonical Parquet/Iceberg data can remain on R2 behind another conforming Iceberg REST
Catalog.

If PostGIS is unavailable or not caught up, the edit may be accepted as pending but must not be
reported as publicly visible.

The dynamic tile URL is serving-generation-addressed or explicitly non-cacheable. Martin's mutable tile cache
and the CDN must not retain a deleted geometry beyond the 5-second SLO.

### 4.2 Scheduled or operator-requested static publication

1. Capture active dynamic release `R`, its `data_revision`, canonical Iceberg snapshot, and projection
   generation.
2. Materialize a build-scoped frozen PostGIS snapshot for exactly `R`; never run multiple
   `martin-cp` passes against a mutating live mirror.
3. Create a build job bound to `R` and the frozen snapshot; continue serving dynamic tiles.
4. Bulk render with `martin-cp` to MBTiles.
5. Validate source layers, stable identities, zoom coverage, feature counts, and expected omissions.
6. Convert and verify one immutable PMTiles artifact.
7. Upload it create-only to a versioned key in the dedicated public static-tile R2 bucket.
8. Wait for `martin-static` to discover the new immutable object from its configured R2 PMTiles
   prefix, then verify the version-addressed source route, HTTP Range reads, and decoded MVT through
   the production-shaped URL.
9. Compare and swap only if the unit still selects input release `R`, its `data_revision` is
   unchanged, and its optimistic version matches the build input.
10. Create and select a static release with the same `data_revision`, retain `R` as the same-data
    fallback, increment the unit `serving_generation` and global `manifest_generation`, and expose the
    complete manifest from Catalog.

If any edit replaces `R` after step 1, step 9 marks the build `SUPERSEDED`; it can never
be promoted. The scheduler may debounce and retry from the newer revision. No edit is paused or lost.

Martin uses two independently configured deployments of the same pinned Martin image:

- `martin-dynamic` has stable, explicit PostGIS sources and is not restarted for a static release.
- `martin-static` uses Martin 1.12's `pmtiles.paths` remote-prefix discovery against only the dedicated
  public static-tile R2 release prefix. A bucket-scoped, read-only R2 credential grants only list/read
  access to that prefix.

Every archive has a unique filename/object key, so discovery only adds immutable routes; files are
never overwritten in place. The publisher polls Martin's catalog until the expected source appears,
decodes representative tiles, and only then invokes the CAS promotion. The checked-in config fixes a
bounded reload interval and the proof mechanically verifies the pinned Martin source-ID convention.
Named `pmtiles.sources` URLs are not used because they are snapshotted at startup.

This reuses Martin's documented
[remote PMTiles prefix hot reload](https://maplibre.org/martin/sources-pmtiles/) and avoids a custom
watcher, Docker socket, or service restart in the publication path. Local fallback uses the same
contract with a watched local directory.

### 4.3 Next edit after static publication

The PostGIS mirror remains warm and caught up even while static tiles handle public reads. The edit
flow creates a new data revision and dynamic release, then atomically selects that complete source
before public success. The client never displays the static and dynamic forms together.

### 4.4 Rollback

Serving rollback and data rollback are different operations:

- **Serving rollback** recovers from a bad tile source by selecting a retained, validated complete
  release for the same `data_revision`, using an expected-active-release compare and swap. The first
  slice proves static-to-dynamic rollback because the warm dynamic mirror still represents the same
  data revision.
- **Data revert** does not point at a mutable historical PostGIS state. Foundation creates a new
  canonical revision whose content intentionally reverts selected prior changes, projects it into
  PostGIS, and follows the normal public-edit flow. History remains append-only.

Rollback never pairs an old archive with newer feature tombstones and never silently changes
business data as a side effect of infrastructure recovery.

## 5. Manifest v2 Contract

The accepted v1 contract describes individual flat MVT objects using a global
`tiles_url_template`, physical `object_key_prefix`, `flat_tile_count`, and
`flat_tile_total_bytes`. Those fields must not be repurposed for a Martin PMTiles route.

A Foundation-owned manifest v2 must make the serving transport explicit per artifact. The exact Rust
DTO remains the contract SSOT and generates OpenAPI/TypeScript consumers, but it must express at least:

```json
{
  "schema_version": 2,
  "current_version": "uuid",
  "manifest_generation": 108,
  "refresh_after_seconds": 4,
  "artifacts": {
    "parcels": {
      "publication_unit": "parcels",
      "active_source": "dynamic",
      "data_revision": "iceberg-snapshot-or-catalog-revision",
      "serving_generation": 42,
      "tiles_url_template": "https://tiles.example.com/parcels-dynamic/{z}/{x}/{y}.pbf?generation=42",
      "source_layer": "parcels",
      "feature_id_property": "pnu",
      "tile_min_zoom": 11,
      "tile_max_zoom": 16,
      "render_min_zoom": 11,
      "render_max_zoom": 18
    }
  }
}
```

For `STATIC`, the artifact additionally identifies the immutable PMTiles artifact, checksum, byte
size, object key, source Iceberg snapshot/revision, and version-addressed Martin source. For
`DYNAMIC`, it identifies the PostGIS projection revision and cache policy. Flat-object statistics
exist only for a flat-object layout; PMTiles statistics use archive-specific fields.

Each artifact declares one canonical lowercase `feature_id_property`. The PostGIS view, PMTiles
producer, TileJSON `vector_layers[].id`, Martin source, Mapbox `promoteId`, and contract tests must use
that same value. Proof-only aliases such as uppercase `PNU` are not production identities.

Manifest v1 remains supported during a bounded consumer migration. Catalog must never publish a v2
manifest until the pinned Gongzzang consumer contract accepts it.

## 6. Five-Second Active-Map Refresh

Gongzzang performs a conditional Catalog runtime-manifest check at most every 4 seconds while the map
is mounted and visible. This reserves one second of the 5-second SLO for manifest retrieval, source
replacement, and the first new tile:

1. Use `ETag`/`If-None-Match` or an equivalent revision response so unchanged polls are small.
2. On a changed global `manifest_generation`, validate the complete manifest from the same Catalog
   response; do not wait for the asynchronous R2 manifest projection. Diff per-artifact
   `serving_generation` values to find the affected sources.
3. Replace or retarget only affected vector sources using the supported Naver internal mapbox-gl
   source API, then force tile reload.
4. Re-register dependent style layers in deterministic order if source replacement requires it.
5. Stop polling when the component is unmounted or the page is hidden; check immediately when it
   becomes visible.

Before backend state-machine implementation begins, the existing Naver SDK browser probe must prove
one of these source-reload paths:

1. Preferred: `getSource(id).setTiles(...)` changes a vector source URL and causes fresh tile requests
   while source-layer, zoom, and `promoteId` remain unchanged.
2. Fallback: `removeLayer`/`removeSource` followed by deterministic re-registration preserves camera
   and interaction state and meets the SLO.
3. Last bounded fallback: controlled Naver map reinitialization preserves camera/selection state and
   meets the SLO.

If none is supported by the actual bundled SDK, this design is blocked and must return to architecture
review; a service worker or custom MVT compositor must not be introduced as a hidden workaround.

The client must retain the last validated complete source if a new manifest is malformed or its
source is not ready. A source transition failure must be observable and must not combine old static
tiles with new dynamic tiles.

The 5-second SLO starts when Foundation commits the active release and complete runtime manifest in
the Catalog transaction, not when an operator begins drawing or when a background canonicalization
job starts. The target is at least 99% of transitions within 5 seconds over a rolling 24-hour window;
before launch, a repeated browser integration probe must meet the limit on every run. The measurement
ends when the already-open map successfully loads a tile for each changed artifact's new
`serving_generation`.

## 7. Cache Contract

- Static PMTiles object keys and Martin source URLs are immutable and version-addressed.
- Static tile and HTTP Range responses may use long-lived immutable caching.
- The lightweight Catalog runtime-manifest response uses `no-cache, must-revalidate` and supports conditional
  requests, so every poll observes the current revision while unchanged responses remain small.
  A polling interval of at most four seconds reserves one second for source reload and the first new
  tile; measurements must shorten the interval if that budget is missed.
- Dynamic tiles use `no-store` at launch, or a measured cache configuration whose total origin,
  Martin, CDN, browser, and polling delay remains within 5 seconds.
- A PMTiles object is never overwritten in place.
- Promotion purges or expires only the small mutable manifest/revision pointer, not immutable tile
  objects.

## 8. Ownership and Boundaries

- Foundation owns canonical public/reference geometry, feature identity, publication state, Martin
  source readiness, PMTiles builds, R2 upload, validation, promotion, rollback, and the manifest.
- Gongzzang validates and consumes the published manifest. It owns only active-map refresh and
  product presentation.
- Dawneer may later provide staff controls for edit, approval, publish-now, and rollback, but calls
  Foundation APIs. Its UI state is not publication authority.
- Gongzzang listing markers retain their separate Gongzzang-owned dynamic contract. This design does
  not reuse listing tables or tombstone state.
- Cross-area integration remains published HTTP contracts/events; no cross-area database access is
  introduced.

## 9. Failure Handling and Observability

Required state-transition evidence:

- data revision, serving generation, active release/source, and same-data fallback release;
- PostGIS projection generation and readiness;
- build input release/snapshot, duration, result, validation report, and supersession reason;
- immutable R2 object key, checksum, size, and upload precondition result;
- Martin source readiness, decoded feature count, source layers, identity samples, and Range behavior;
- CAS promotion/rollback result;
- manifest projection lag;
- client generation-poll lag, source-reload success/failure, and time to first tile at the new
  generation.

Readiness must fail when a dynamic unit is selected but its PostGIS projection is behind, or when a
static unit is selected but the exact Martin/R2 artifact is unreadable. A build failure leaves the
active dynamic source unchanged.

## 10. Mechanical Guards

Tests must make these regressions impossible:

1. A manifest artifact selects exactly one active source.
2. Every manifest layer maps to one Martin source and one expected MVT source layer; no undeclared
   source is exposed by the slice configuration.
3. Static and dynamic producers emit the same canonical feature identity.
4. An edit during build makes the stale build unpromotable.
5. A failed build or R2 upload cannot alter the active source.
6. A dynamic-to-static switch occurs only after decoding expected features through the Martin URL.
7. A static-to-dynamic edit cannot report public success while the dynamic projection is behind.
8. A browser revision change replaces the source and does not leave both versions registered.
9. Malformed/unready new manifests retain the last validated complete source.
10. The proof exercises add, modify, and delete for one sample unit and finds no duplicate, gap, or
    resurrected feature.

The guards run through `cargo xtask verify foundation` and `cargo xtask verify gongzzang`; workflows
must not introduce a second verification path.

## 11. Scale Evolution

The launch unit is a complete layer because it is the smallest architecture that provides immediate
correctness without custom tile composition. Before adding sharding, collect:

- Martin/PostGIS p95 and origin CPU;
- dynamic cache-miss rate and cost;
- PMTiles size and rebuild duration;
- edit frequency and percentage of each layer affected;
- superseded build frequency;
- active-map refresh success and latency.

Only when a measured threshold is exceeded may a follow-up ADR introduce fixed, non-overlapping
partition ownership. Each partition uses the same single-active-source invariant. An edit marks every
partition intersecting both the old and new geometry dynamic. Feature-level static/dynamic mixtures
remain prohibited.

Direct flat MVT objects, DuckDB/GeoParquet serving, and custom server-side MVT composition are not
launch paths. They may be reconsidered only with production evidence that the selected architecture
cannot meet its SLO or cost target.

## 12. Required ADR Reconciliation

Before production implementation is declared complete:

1. Amend the root object-storage-first ADR to replace feature-level suppression guidance with the
   single-complete-source publication invariant.
2. Add or amend the Foundation serving ADR so it owns the edit, publication, and active-source state.
3. Supersede the flat-only portions of Foundation ADR-0004 and Gongzzang ADR-0036 with manifest v2,
   while documenting the bounded v1 migration.
4. Keep the Foundation lakehouse ADR authoritative for R2 + Iceberg canonical data and PostGIS/PMTiles
   derived-serving roles.
5. Update the proof runbook so its local manifest is not mistaken for the production contract.

## 13. Delivery Boundary

The first implementation is one generic vertical slice using the `parcels` publication unit:

- one canonical revision and complete PostGIS mirror;
- one proven Naver mapbox-gl source-reload path before backend state-machine implementation;
- one isolated Iceberg WAP candidate whose failed activation cannot enter later public history;
- one immutable PMTiles candidate on the dedicated proof R2 path;
- one static-Martin remote-prefix discovery cycle that exposes the create-only R2 object without a
  restart;
- dynamic edit and 5-second open-map refresh;
- validated CAS promotion to static;
- a concurrent edit that mechanically blocks stale promotion;
- same-data static-to-dynamic serving rollback;
- generic contracts and tests capable of adding `complex`, `buildings`, and future Foundation polygon
  layers without copying publication logic.

It does not implement regional partitions, a custom tile compositor, feature tombstones, national
scale rollout, or the Dawneer admin UI.
