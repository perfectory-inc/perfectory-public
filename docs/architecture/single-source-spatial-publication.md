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
- `STATIC`: Martin serves one immutable, version-addressed PMTiles artifact from the dedicated
  private serving-derivative R2 bucket.

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
6. PMTiles, PostGIS, Martin caches, and the runtime manifest are projections, never independent
   authorities for canonical geometry or visibility.

These invariants eliminate double drawing, stale resurrection, feature-level suppression garbage
collection, and change loss during a static build.

## 3. Authority and SSOT

SSOT means one authority for each fact, not one physical storage system.

| Fact | Authority | Derived copies |
|---|---|---|
| Canonical geometry revision, feature identity, public lifecycle, and lineage | Foundation R2 + Apache Iceberg, mutated only through the Foundation Catalog contract | PostGIS mirror, PMTiles, MVT properties |
| Per-unit active release, serving generation, and serving rollback history | Foundation Catalog transaction/CAS state | Published manifest, Martin routes |
| Global runtime-manifest generation and ETag | Foundation Catalog global manifest pointer | create-only R2 manifest history, mutable R2 pointer projection, Gongzzang poll state |
| Tile bytes while dynamic | Complete PostGIS serving mirror through Martin | Martin/CDN caches |
| Tile bytes while static | Immutable PMTiles artifact selected by Catalog | Martin/CDN range cache |
| Listing visibility and marker state | Gongzzang listing write model | Gongzzang marker projections |

An approved public edit must be durably registered in the Foundation canonical revision and projected
into PostGIS before its serving generation becomes public. PostGIS remains reconstructible from the
canonical Foundation data and must not become the only copy of an edit. An unapproved preview may use
a staff-only workspace but is outside the public tile contract.

R2 carries two projections of Catalog manifest state: create-only
`gold/vector-tiles/manifests/{manifest_id}.json` history and the rebuildable no-cache
`gold/vector-tiles/runtime-manifest.json` active pointer. Neither is where operators manually choose
the active source. The mutable pointer is updated with R2 ETag compare-and-swap (`If-Match`, or
`If-None-Match: *` on bootstrap), never an unconditional overwrite after a database check.

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
  unit_selections[]              # immutable unit -> release + serving generation snapshot

runtime_manifest_pointer
  singleton                     # exactly one checked row
  active_manifest_id
  optimistic_version

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

The v2 state uses normalized publication-unit, release, release-layer, immutable-manifest,
manifest-unit-selection, singleton-pointer, build-job, and refresh-observation tables. The existing
schema-v1 `vector_tile_manifest` and `vector_tile_artifact` tables and their flat-MVT constraints are
not altered. This physical separation makes accidental v1 semantic reuse impossible and preserves
the legacy endpoint/event/object bytes during migration.

### 4.1 Public edit

1. Read the expected active release, Catalog-selected Iceberg snapshot, data revision, and optimistic
   version. New edits must branch from this selected snapshot, never from an unselected Iceberg
   `main` head.
2. Create a release-scoped Iceberg WAP branch from the selected snapshot, write the candidate there,
   and validate it. The branch has an explicit retention period and remains isolated from `main`.
3. In one Foundation database transaction, lock the singleton runtime-manifest pointer first, then
   lock the affected publication unit; verify the expected active release/version; apply the
   candidate revision to the complete PostGIS serving projection; record a new immutable dynamic
   release; select it; increment `serving_generation`; and record the outbox event for the R2
   manifest projection. The same transaction reads all unit selections while the pointer is locked,
   creates the new immutable global manifest, and increments `manifest_generation`.
4. The Catalog runtime-manifest endpoint reads the new complete release directly from that committed
   transaction.
5. The outbox publisher asynchronously projects the same manifest to R2 for boot/distribution use.
6. Return public success only after steps 1-4 are durable and the dynamic source is ready.

Every public edit creates a new release, even when the unit was already dynamic. The per-unit row
lock, global pointer lock, expected active release, and branch base snapshot serialize concurrent
edits. Every publication transaction acquires locks in the fixed order
`runtime_manifest_pointer -> publication_unit -> release rows`; no code path may acquire a unit lock
first. The selected WAP snapshot becomes the Catalog-authoritative canonical revision only when step
3 commits. A failed activation leaves an unselected branch; later edits still branch from the
Catalog-selected snapshot, so failed candidates cannot leak into later public revisions.

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

The dynamic tile URL carries `serving_generation` as a cache-busting query/path value and is
explicitly non-cacheable. The stable Martin source always reads the latest completely committed
PostGIS projection; the generation value is not a historical snapshot selector. Martin's mutable
tile cache and the CDN must not retain a deleted geometry beyond the 5-second SLO.

### 4.2 Scheduled or operator-requested static publication

1. Capture active dynamic release `R`, its `data_revision`, canonical Iceberg snapshot, and projection
   generation.
2. Materialize a build-scoped frozen PostGIS snapshot for exactly `R`; never run multiple
   `martin-cp` passes against a mutating live mirror.
3. Create a build job bound to `R` and the frozen snapshot; continue serving dynamic tiles.
4. Bulk render with `martin-cp` to MBTiles.
5. Validate source layers, stable identities, zoom coverage, feature counts, and expected omissions.
6. Convert and verify one immutable PMTiles artifact.
7. Upload it create-only to a versioned key in the dedicated private serving-derivative R2 bucket.
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
  private serving-derivative R2 bucket. A separate bucket-scoped, read-only R2 credential grants
  list/read access to that bucket. The configured prefix bounds source discovery; it is not an IAM
  boundary.

Every archive filename is exactly `{publication_unit}-{release_id}.pmtiles`, so Martin's discovered
source ID is the release-addressed filename stem. Discovery only adds immutable routes; files are
never overwritten in place. The publisher polls Martin's catalog until the expected source appears,
decodes representative tiles, and only then invokes the CAS promotion. The checked-in config fixes a
bounded reload interval and the proof mechanically verifies the pinned Martin source-ID convention.
Named `pmtiles.sources` URLs are not used because they are snapshotted at startup.

Martin supports Cloudflare R2 through its S3-compatible PMTiles object-store source, so the bucket
does not need a public `r2.dev` endpoint or custom domain. Cloudflare CDN fronts the public Martin
MVT route; Martin is the authenticated R2 origin client. Direct public/custom-domain PMTiles is an
explicitly authorized alternative, not the default.

This reuses Martin's documented
[S3-compatible PMTiles source and remote-prefix hot reload](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md)
and avoids a custom watcher, Docker socket, or service restart in the publication path. Local
fallback uses the same contract with a watched local directory. Named `pmtiles.sources` entries
remain startup snapshots; only `pmtiles.paths` provides prefix polling.

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

A Foundation-owned manifest v2 makes the serving transport explicit once per publication unit. It
does not duplicate release, source, or generation identities for every layer. The exact Rust DTO
remains the executable contract SSOT and generates OpenAPI/TypeScript consumers:

```json
{
  "schema_version": 2,
  "current_version": "0196e7e0-3c20-7000-8000-000000000052",
  "manifest_generation": 108,
  "refresh_after_seconds": 4,
  "published_at": "2026-07-24T00:00:00Z",
  "publication_units": {
    "parcels": {
      "data_revision": "0196e7e0-3c20-7000-8000-000000000061",
      "serving_generation": 42,
      "active_release_id": "0196e7e0-3c20-7000-8000-000000000062",
      "canonical_iceberg_snapshot_id": "70000000000000001",
      "source": {
        "kind": "dynamic_postgis",
        "martin_source_id": "parcels",
        "tiles_url_template": "https://tiles.example.com/parcels/{z}/{x}/{y}?generation=42",
        "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
        "cache_policy": "no_store"
      },
      "layers": {
        "parcels": {
          "source_layer": "parcels",
          "feature_id_property": "pnu",
          "tile_min_zoom": 11,
          "tile_max_zoom": 16,
          "render_min_zoom": 11,
          "render_max_zoom": 18,
          "feature_filter_properties": {
            "pnu": "pnu"
          }
        }
      },
      "lineage": {
        "source_record_id": "0196e7e0-3c20-7000-8000-000000000064",
        "source_file_asset_ids": [
          "0196e7e0-3c20-7000-8000-000000000065"
        ]
      }
    }
  }
}
```

`source` is a closed tagged union. `dynamic_postgis` carries the complete PostGIS projection
revision and cache policy. `static_pmtiles` instead carries the immutable PMTiles object key,
file-asset UUID, SHA-256, byte size, and release-addressed Martin source. Exactly one variant is
valid. V2 forbids v1 flat-object fields.

`current_version`, `data_revision`, `active_release_id`, projection revisions, and lineage IDs are
UUIDs. `manifest_generation` and `serving_generation` are positive integers no greater than
`9007199254740991`. `canonical_iceberg_snapshot_id` is a positive decimal **string**, never a JSON
number: production Iceberg snapshot IDs can exceed JavaScript's safe integer range.

`manifest_generation` is only a global poll/change token. A unit's `source` and
`serving_generation` select its runtime. `data_revision` changes when feature content changes;
`serving_generation` also changes for a dynamic-to-static switch of the same data.

Dynamic PostGIS source IDs are stable explicit Martin configuration names. Their URL must carry the
exact `serving_generation` in the query/path cache key and use `no_store`; creating an undeclared
per-generation Martin source ID is forbidden. Static source IDs are instead immutable
release-addressed PMTiles filename stems.

Each layer declares one canonical lowercase `feature_id_property`. The PostGIS view, PMTiles
producer, TileJSON `vector_layers[].id`, Martin source, Mapbox `promoteId`, and contract tests use
that same value. Proof-only aliases such as uppercase `PNU` are not production identities.

The first v2 publication unit is only `parcels`. During the bounded migration, the existing v1
manifest continues to supply `parcel_anchor_aggregate` and `parcel_anchor`; Gongzzang ignores its
v1 `parcels` artifact and loads the v2 parcel source instead. A parcel polygon transition therefore
does not retarget the two anchor sources, and no unit is registered twice. `complex`, both anchor
units, `admin`, and `buildings` migrate to v2 only after their own producer/consumer parity. The
model may allow multiple MVT layers within one future unit only when they are always built,
validated, and switched as one complete Martin source.

Manifest v1 remains supported during a bounded consumer migration. Catalog must never publish a v2
manifest until the pinned Gongzzang consumer contract accepts it. Both sides dispatch exactly on
`schema_version`; values other than `1` or `2` fail closed.

The two schemas also use distinct projections during that migration. The existing
`gold/manifest.json` stays frozen as v1; the Catalog live endpoint is
`GET /catalog/v1/vector-tiles/runtime-manifest`. Each v2 `current_version` is also the
`manifest_id` in create-only `gold/vector-tiles/manifests/{manifest_id}.json`, while the rebuildable
active v2 pointer is `gold/vector-tiles/runtime-manifest.json`. Overwriting the v1 key with v2 would
remove the anchor sources on a fresh page load and is forbidden.

## 6. Five-Second Active-Map Refresh

Gongzzang performs one non-overlapping conditional Catalog runtime-manifest check every 4 seconds
while the map is mounted and visible. Schema v2 fixes `refresh_after_seconds` to `4`. This reserves
one second of the 5-second SLO for manifest retrieval, source
replacement, and the first new tile:

1. Use `ETag`/`If-None-Match` or an equivalent revision response so unchanged polls are small.
2. On a changed global `manifest_generation`, validate the complete manifest from the same Catalog
   response; do not wait for the asynchronous R2 manifest projection. Diff per-unit
   `serving_generation` values to find the affected sources.
3. Replace or retarget only affected vector sources using the supported Naver internal mapbox-gl
   source API, then force tile reload.
4. Re-register dependent style layers in deterministic order if source replacement requires it.
5. Stop polling when the component is unmounted or the page is hidden; check immediately when it
   becomes visible.
6. Randomize only the initial polling phase to spread clients, never overlap requests, and use
   bounded exponential backoff after transport/server failures.

The steady-state budget is at most `0.25` conditional manifest requests/second per visible map.
Before v2 is enabled, Foundation must register the route as an anonymous read-only public contract,
bound its metric label and edge/CORS policy, and pass a load probe at twice the deployment's declared
concurrent-visible-map launch budget.

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

The client keeps its currently registered source descriptor if a new manifest is malformed or its
candidate source is not ready. Immutable static URLs continue to return the exact old release.
A retained dynamic URL is only an availability fallback: because its stable Martin source reads the
latest committed complete projection, it may return newer dynamic bytes and is not historical
rollback. Foundation must never publish a dynamic release before that projection is ready. A source
transition failure must be observable and must not combine old static tiles with new dynamic tiles.

The 5-second SLO starts when Foundation commits the active release and complete runtime manifest in
the Catalog transaction, not when an operator begins drawing or when a background canonicalization
job starts. The target is at least 99% of transitions within 5 seconds over a rolling 24-hour window;
before launch, a repeated browser integration probe must meet the limit on every run. The measurement
ends when the already-open map successfully loads a tile for each changed unit's new
`serving_generation`.

## 7. Cache Contract

- Static PMTiles object keys and Martin source URLs are immutable and version-addressed.
- Static tile and HTTP Range responses may use long-lived immutable caching.
- The lightweight Catalog runtime-manifest response uses `no-cache, must-revalidate` and supports conditional
  requests, so every poll observes the current revision while unchanged responses remain small.
  The schema-v2 polling interval is exactly four seconds, reserving one second for source reload and the first new
  tile; measurements must shorten the interval if that budget is missed.
- Dynamic tiles use `no-store` at launch, or a measured cache configuration whose total origin,
  Martin, CDN, browser, and polling delay remains within 5 seconds.
- A dynamic generation query changes cache identity only. It does not retain or address an old
  PostGIS projection; business rollback creates a new revision.
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

1. A manifest publication unit selects exactly one active source.
2. Every manifest layer maps to one Martin source and one expected MVT source layer; no undeclared
   source is exposed by the slice configuration.
3. Static and dynamic producers emit the same canonical feature identity.
4. An edit during build makes the stale build unpromotable.
5. A failed build or R2 upload cannot alter the active source.
6. A dynamic-to-static switch occurs only after decoding expected features through the Martin URL.
7. A static-to-dynamic edit cannot report public success while the dynamic projection is behind.
8. A browser revision change replaces the source and does not leave both versions registered.
9. Malformed/unready new manifests retain the current source descriptor; tests distinguish exact
   immutable-static retention from the non-historical latest-projection behavior of a dynamic route.
10. The proof exercises add, modify, and delete for one sample unit and finds no duplicate, gap, or
    resurrected feature.
11. Two outbox publishers interleaved as `A reads -> B publishes newer -> A writes` cannot regress
    the R2 runtime pointer; A's stale ETag fails and reconciliation selects Catalog's current manifest.
12. Two different publication units activated concurrently preserve both selections and produce an
    ordered global manifest sequence; a partial or lost unit selection is impossible.

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

## 12. ADR Reconciliation

The governing records now agree:

1. Root ADR 0006 defines object-storage-first serving and the single-complete-source invariant.
2. Foundation ADR 0004 owns edit publication, active releases, strict manifest v1/v2 semantics, and
   authenticated private-R2 Martin serving.
3. Foundation ADR 0006 remains authoritative for R2/Iceberg canonical data and reconstructible
   PostGIS/PMTiles serving projections.
4. Gongzzang ADR 0036 owns strict consumer dispatch and source replacement; historical ADRs 0016 and
   0021 are superseded.
5. The proof runbook distinguishes its local v1 adapter from the production v2 contract.

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
