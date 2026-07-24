# ADR 0004 - Foundation Vector Tile Publication Contract

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-12 |
| 상태 | Accepted |
| 최종 개정 | 2026-07-24 |
| 상속 | [`gongzzang ADR 0036`](../../../../products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md) |
| 범위 | `foundation-platform` Catalog, Martin/PostGIS/PMTiles publication, `gongzzang` map runtime |

## 결정

Foundation Catalog is the authority for the active schema-v2 vector-tile manifest and every
publication unit's active release. During the bounded migration, `gold/manifest.json` remains the
frozen schema-v1 pointer consumed by the existing parcel-anchor runtime. Schema v2 uses the
distinct rebuildable R2 projection `gold/vector-tiles/runtime-manifest.json`; neither object is
canonical state.

Gongzzang ADR 0036 의 static vector tile runtime contract 를 상속하되,
foundation-platform cutover 이후에는 필지, 산업단지, 행정구역, 건물 등 Catalog spatial
layer 의 vector tile manifest 를 `foundation-platform` 가 생성, 검증, publish 한다.

`gongzzang` 은 manifest consumer only 다. Gongzzang 은 manifest 를 읽어 지도 source 를
구성할 수 있지만, manifest version, artifact metadata, lineage, file asset 연결을
직접 write 하지 않는다.

The publication invariant is:

```text
(publication_unit, serving_generation)
  -> exactly one complete Martin source
  -> DynamicPostgis XOR StaticPmtiles
```

A complete source contains every currently visible feature for the unit. Static and dynamic
representations of the same unit are never composed in the browser or in Martin. There is no
feature tombstone/suppression transport. Different units may independently select different source
kinds.

This contract owns only Foundation public/reference units. Gongzzang listing markers, listing
visibility, `filter_hash`, and marker-delta/filter-mask behavior remain governed by Gongzzang ADR
0037/0038 and are not publication units in this manifest.

## Runtime Pointer

The legacy schema-v1 pointer remains:

```text
gold/manifest.json                                      # schema v1, frozen during migration
```

Schema v2 uses a distinct mutable runtime projection:

```text
gold/vector-tiles/runtime-manifest.json                 # schema v2, rebuildable
```

보존/rollback object key 는 immutable release id 규칙을 따른다.

```text
gold/vector-tiles/manifests/{manifest_id}.json
gold/vector-tiles/releases/{release_id}/{publication_unit}-{release_id}.pmtiles
gold/vector-tiles/releases/{release_id}/{publication_unit}-{release_id}.tilejson.json
```

`gold/manifest.json` must not be overwritten with schema-v2 bytes. It remains byte-compatible for
the bounded legacy anchor consumer until those sources have their own proven v2 producer/consumer
path. Every v2 Catalog manifest has the same UUID as `current_version`; its immutable R2 projection
uses that UUID as `{manifest_id}` and is written create-only. The mutable
`gold/vector-tiles/runtime-manifest.json` object is a no-cache pointer projection, and canonical
truth is the Catalog. Immutable manifest and release objects are never overwritten. Catalog records
active and retained release history; both v2 projections can be regenerated from that state.

Canonical Bronze/lakehouse/recovery objects and serving derivatives must use different buckets.
The derivative bucket is private by default. Martin receives a separate bucket-scoped read-only R2
credential and reads `s3://` PMTiles through the S3-compatible API. An object prefix is a discovery
boundary, not an IAM boundary. A public `r2.dev` URL or custom-domain origin requires an explicit
security decision and is not the default.

## Schema version dispatch

`schema_version` is an exact compatibility discriminator, not a minimum. Producer and consumer
must dispatch `1` and `2` to separate strict DTOs and reject every other value. The `/catalog/v1`
HTTP segment is independent of the manifest schema version.

Schema v1 remains a bounded legacy contract for already-published individual flat MVT objects.
Schema v2 is the production contract for Martin single-source publication. A v1 field is never
repurposed with v2 semantics.

The schema-v1 `catalog.vector_tile_manifest` and `catalog.vector_tile_artifact` tables remain
unchanged. Schema v2 uses separate normalized publication-unit, release, release-layer,
immutable-manifest, manifest-unit-selection, singleton-pointer, build-job, and refresh-observation
tables. This storage boundary mechanically prevents v2 from weakening the existing flat-MVT
constraints.

## Legacy manifest schema v1

foundation-platform 가 publish 하는 manifest 는 최소한 다음 필드를 포함한다.

```json
{
  "schema_version": 1,
  "current_version": "0196e7e0-3c20-7000-8000-000000000042",
  "previous_version": "0196e7e0-3c20-7000-8000-000000000041",
  "tiles_url_template": "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf",
  "published_at": "2026-05-12T00:00:00Z",
  "artifacts": {
    "parcels": {
      "source_layer": "parcels",
      "tile_min_zoom": 8,
      "tile_max_zoom": 16,
      "render_min_zoom": 10,
      "render_max_zoom": 22,
      "tilejson_object_key": "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels.json",
      "object_key_prefix": "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels/",
      "flat_tile_count": 123456,
      "flat_tile_total_bytes": 987654321,
      "feature_filter_properties": {
        "pnu": "pnu"
      },
      "lineage": {
        "source_record_id": "00000000-0000-0000-0000-000000000000",
        "manifest_file_asset_id": "00000000-0000-0000-0000-000000000000",
        "tilejson_file_asset_id": "00000000-0000-0000-0000-000000000000",
        "source_file_asset_ids": [
          "00000000-0000-0000-0000-000000000000"
        ]
      }
    }
  }
}
```

Required manifest fields:

- `current_version`
- `previous_version`
- `tiles_url_template`
- `artifacts`

Required `artifacts[layer]` fields:

- `source_layer`
- `tile_min_zoom`
- `tile_max_zoom`
- `render_min_zoom`
- `render_max_zoom`
- `tilejson_object_key`
- `object_key_prefix`
- `lineage.source_record_id`
- `lineage.manifest_file_asset_id`
- `lineage.tilejson_file_asset_id`
- `lineage.source_file_asset_ids`

Optional `artifacts[layer].feature_filter_properties` maps logical filter identities to concrete
feature property names inside the vector tile. foundation-platform publishes only public/reference
properties it owns. Product-owned properties such as listing price, listing status, exposure rules,
or product search filters must not appear in this manifest.

Current foundation-platform-owned reference mappings:

| Manifest artifact | Logical filter property | Vector tile feature property |
|---|---|---|
| `parcels` | `pnu` | `pnu` |
| `parcel_anchor` | `pnu` | `pnu` |
| `complex` | `official_complex_code` | `official_complex_code` |

Consumers must not assume a filter property exists unless it is present in
`feature_filter_properties`.

`tiles_url_template` must contain `{object_key_prefix}`, `{z}`, `{x}`, and `{y}` placeholders.
The runtime replaces `{object_key_prefix}` with `artifacts[layer].object_key_prefix`.

New v1 publication stops after the v2 producer/consumer cutover. Existing v1 manifests remain
readable only for the bounded migration period.

## Manifest schema v2

V2 models the unit that is switched, rather than pretending a PMTiles object is a directory of
flat tiles:

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
      "canonical_iceberg_snapshot_id": "2095444522288693696",
      "source": {
        "kind": "static_pmtiles",
        "martin_source_id": "parcels-0196e7e0-3c20-7000-8000-000000000062",
        "tiles_url_template": "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
        "pmtiles_object_key": "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels-0196e7e0-3c20-7000-8000-000000000062.pmtiles",
        "pmtiles_file_asset_id": "0196e7e0-3c20-7000-8000-000000000063",
        "pmtiles_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "pmtiles_bytes": 987654321
      },
      "layers": {
        "parcels": {
          "source_layer": "parcels",
          "feature_id_property": "pnu",
          "tile_min_zoom": 8,
          "tile_max_zoom": 16,
          "render_min_zoom": 10,
          "render_max_zoom": 22,
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

V2 scalar rules:

- `current_version`, `data_revision`, `active_release_id`, and every file/record ID are UUIDs.
- `manifest_generation` is a global poll token only. It does not select a source.
- `manifest_generation` and `serving_generation` are integers in
  `1..=9007199254740991`, so JavaScript cannot lose precision.
- `canonical_iceberg_snapshot_id` is a positive base-10 decimal **string**. Real Iceberg snapshot
  IDs exceed JavaScript's safe integer range and must never cross JSON as numbers.
- `refresh_after_seconds` is exactly `4` in schema v2. It is a schedule interval, never a source
  identity or permission to create overlapping polls.
- Every `source.tiles_url_template` is an absolute Martin URL containing `{z}`, `{x}`, and `{y}`
  exactly once. Production publication requires HTTPS. Parsing permits HTTP only for a loopback
  literal or `localhost` so the checked-in Docker proof needs no fake public TLS endpoint; the
  production publish gate rejects every HTTP URL, including loopback. The URL is
  generation-addressed for dynamic sources and release-addressed for static sources. The consumer
  neither appends `.pbf` nor rewrites the route.
- `publication_units` and every unit's `layers` map are non-empty.
- `data_revision` identifies the exact logical feature set. `serving_generation` changes whenever
  another complete release is selected, including dynamic-to-static for the same data revision.
- `active_release_id` identifies an immutable release descriptor. It is never reused for another
  source or generation.

The unit's `source` is a closed tagged union. Unknown kinds or fields fail validation.

`dynamic_postgis` contains exactly:

```json
{
  "kind": "dynamic_postgis",
  "martin_source_id": "parcels",
  "tiles_url_template": "https://tiles.example.com/parcels/{z}/{x}/{y}?generation=42",
  "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000071",
  "cache_policy": "no_store"
}
```

`dynamic_postgis` uses the stable explicit Martin source ID configured for the unit. Its tile URL,
not its source ID, includes the exact `serving_generation` as a cache-key query/path component.
Martin's in-process cache is disabled and the route is `no_store`. The generation component is not
a historical PostGIS snapshot selector: every dynamic URL reaches the latest completely committed
projection behind that stable source.

`static_pmtiles` contains the fields shown in the v2 example. Its object key, checksum, byte size,
and file-asset identity are required. The filename is
`{publication_unit}-{release_id}.pmtiles`; Martin's discovered source ID is exactly that
release-addressed filename stem, and the URL is immutable. Flat-object fields such as
`object_key_prefix`, `flat_tile_count`, and
`flat_tile_total_bytes` are forbidden in v2.

Every unit source must expose all `layers` declared under that unit, and no undeclared layer needed
by the consumer. The `source_layer` is the MVT layer ID, not a database table. Each layer declares
one canonical lowercase `feature_id_property`; PostGIS, PMTiles, TileJSON
`vector_layers[].id`, Martin, Mapbox `promoteId`, and decoded-tile tests use the same identity.

The first schema-v2 migration unit is deliberately only `parcels`:

| Initial v2 publication unit | Required MVT layers |
|---|---|
| `parcels` | `parcels` |

The following units may migrate later only after their own producer and consumer parity is proven:

| Publication unit | Required MVT layers |
|---|---|
| `complex` | `complex` |
| `parcel_anchor_aggregate` | `parcel_anchor_aggregate` |
| `parcel_anchor` | `parcel_anchor` |
| `admin` | `admin` when published |
| `buildings` | `buildings` when published |

Both parcel-anchor layers remain required by the current Gongzzang marker runtime. During the first
v2 slice, Gongzzang loads those two sources from the frozen v1 manifest and loads `parcels` from the
v2 manifest. It must not also register the v1 `parcels` artifact. Changing the `parcels` source must
not retarget or remove either legacy anchor source. This bounded dual-manifest migration never
registers two sources for the same publication unit.

## Catalog ownership

The manifest is Catalog data because it describes foundation-platform spatial facts and their derived
runtime artifacts.

| Resource | Owner | Catalog link |
|---|---|---|
| `gold/manifest.json` | `foundation-platform` Catalog | frozen legacy schema-v1 projection during migration |
| `gold/vector-tiles/runtime-manifest.json` | `foundation-platform` Catalog | rebuildable schema-v2 runtime projection |
| `gold/vector-tiles/manifests/{manifest_id}.json` | `foundation-platform` Catalog | create-only immutable manifest projection; ID equals `current_version` and links to its `catalog.file_asset` row |
| `gold/vector-tiles/releases/{release_id}/{publication_unit}-{release_id}.pmtiles` | `foundation-platform` Catalog | immutable release `pmtiles_file_asset_id` |
| `publication_units[unit]` | `foundation-platform` Catalog | active release plus layer and build metadata |
| `lineage.source_record_id` | `foundation-platform` Catalog | `catalog.source_record.id` |
| `lineage.*file_asset_id` | `foundation-platform` Catalog | `catalog.file_asset.id` |

Legacy v1 individual `.pbf` tiles do not require one `catalog.file_asset` row per object. V2 has one
file-asset row for each PMTiles archive and records release validation evidence separately.

## Spatial layer mapping

Each v1 artifact or v2 publication unit maps to a Foundation spatial layer. V2 unit names express
the independently switched serving boundary; MVT `source_layer` remains the renderer contract.

| Publication unit | Foundation Platform source |
|---|---|
| `parcels` | `catalog.parcel` + `catalog.spatial_layer(layer_kind = 'parcel_boundary')` |
| `complex` | `catalog.industrial_complex` + `catalog.spatial_layer(layer_kind = 'complex_boundary')` |
| `admin` | imported admin boundary `catalog.spatial_layer` |
| `buildings` | `catalog.building` + building footprint layer |

The manifest `source_layer` value is the vector tile layer name inside MVT, not a DB table name. It
must be stable for runtime style and click handling.

## Gongzzang Runtime Contract

Gongzzang runtime must:

1. Fetch schema v2 from `GET /catalog/v1/vector-tiles/runtime-manifest` and, during the bounded
   migration, fetch the frozen v1 anchor manifest through the existing v1 location. Dispatch each
   document exactly on `schema_version`.
2. Retain the existing v1 flat-object materialization rules only for
   `parcel_anchor_aggregate` and `parcel_anchor` while v2 `parcels` is active. Do not register the v1
   `parcels` artifact at the same time.
3. For v2, register exactly one vector source per publication unit from that unit's tagged
   `source.tiles_url_template`.
4. Treat v2 `parcels` plus the two legacy v1 anchor sources as core for the current map workflow;
   treat declared optional v2 units as skippable.
5. Poll the v2 endpoint every four seconds with one non-overlapping conditional request while the
   map is visible. A changed global
   `manifest_generation` triggers full validation, then only units whose `serving_generation`
   changed are replaced.
6. Retain the currently registered source descriptor if a new manifest or source is invalid/unready.
   Static retention returns the exact immutable release. Dynamic retention remains available but may
   read the latest committed projection; a generation query is not historical rollback. Never render
   the old and new source for one unit together.
7. Use manifest lineage for diagnostics, source disclosure, and support reports.

Gongzzang runtime must not:

- write either `gold/manifest.json` or `gold/vector-tiles/runtime-manifest.json`;
- rewrite `current_version`, generation values, or active releases;
- synthesize missing publication units, layers, lineage, or source metadata;
- use `manifest_generation` as a source selector;
- combine static and dynamic sources for one unit;
- use Naver internal tile URLs as domain data source;
- use build-time env vars as the production active version pointer.

## Publish Gate

foundation-platform promote must fail before changing Catalog active state or projecting
`gold/vector-tiles/runtime-manifest.json` unless all required checks pass. V2 promotion must never
change `gold/manifest.json`.

- the candidate manifest UUID and release ID are new and immutable;
- compare-and-swap matches the currently active release and optimistic version;
- every required unit selects exactly one tagged source and every declared layer decodes non-empty
  representative tiles from that exact Martin route;
- the complete candidate has stable `source_layer`, canonical feature identity, valid zoom ranges,
  and source/geometry lineage;
- a dynamic source's complete PostGIS projection revision is ready and reconstructible from the
  selected Iceberg snapshot plus audited publication inputs;
- a static source's PMTiles object exists create-only, checksum and size match, HTTP Range/S3 reads
  work through Martin, and the Martin source is release-addressed;
- a static build input still equals the active dynamic release/data revision; otherwise the build is
  `SUPERSEDED`;
- every production URL is HTTPS; the loopback-HTTP parser exception is proof-only and cannot pass
  this gate;
- manifest, PMTiles, and source inputs have the required Catalog `file_asset` /
  `source_record` rows; and
- only the small mutable manifest projection is expired or purged. Immutable release paths are not.

Add, modify, and delete are identical at this boundary: each creates a complete candidate feature
set. Feature-level overlays, subtraction, and tombstone garbage collection are forbidden.

## API Boundary

Foundation exposes the active manifest through the Catalog API. The browser uses this endpoint for
the live conditional-poll SLO. The R2 manifest is an asynchronous, rebuildable distribution/boot
projection and must not be newer than Catalog authority.

The runtime endpoint is an anonymous read-only public contract because browser JavaScript calls it
directly. It must appear in the Foundation traffic/auth registry with a bounded canonical metric
route, explicit edge policy, CORS allow-list, `If-None-Match` allowance, exposed `ETag`, and no
service-identity middleware. The launch client budget is one non-overlapping request per visible map
per four seconds. Deployment must prove the endpoint at twice its declared concurrent-visible-map
launch budget before enabling v2.

Recommended API surfaces:

```text
GET /catalog/v1/vector-tiles/runtime-manifest
GET /catalog/v1/vector-tiles/runtime-manifests/{version}
POST /catalog/v1/vector-tiles/publication-units/{unit}:activate-dynamic
POST /catalog/v1/vector-tiles/publication-units/{unit}:promote-static
POST /catalog/v1/vector-tiles/publication-units/{unit}:rollback-serving
```

The existing `/catalog/v1/vector-tiles/manifest`, `manifest:promote`, and `manifest:rollback`
surfaces remain schema-v1-only. Their route names, payloads, persistence tables, events, and object
key are not repurposed for v2.

The API response and projected R2 object use the same strict wire contract. HTTP `ETag` represents
`current_version`; `manifest_generation` is returned inside the validated document.

Promote is a Foundation Catalog admin operation. In one transaction it locks the singleton runtime
manifest pointer first and then the affected publication unit, registers the immutable release/lineage,
requires the expected active release and optimistic version, selects the candidate, increments that
unit's `serving_generation`, reads every unit selection while the pointer is locked, creates a new
immutable manifest UUID, increments global `manifest_generation`, and emits an outbox event for the
R2 projection. Every publication path uses the fixed lock order
`runtime_manifest_pointer -> publication_unit -> release rows`; duplicate release IDs, manifest IDs,
generations, or object keys fail closed. Concurrent activations of different units are serialized
under the same pointer lock so no unit selection is lost and no mixed global manifest is committed.

Manual serving rollback targets a retained, validated immutable release for the **same**
`data_revision`, requires an expected-active-release compare-and-swap, and creates a new manifest
that selects that release. It never edits or republishes an old manifest document. Reverting
business data creates a new canonical revision and follows the normal publication flow.

The rollback API must verify a staff Bearer token through foundation-platform Staff Identity before mutation.
Only `MASTER_ADMIN`, `CATALOG_ADMIN`, or `VECTOR_TILE_ADMIN` may roll back vector tile manifests.
The staff identity comes from Zitadel token verification, while the role set used for this decision
comes from foundation-platform Staff Identity DB roles. `operator_staff_id` is derived from the verified staff
session, never trusted from the request body. The event includes that verified
`operator_staff_id`, optional `request_id`, old/new release IDs, the expected active release, and the
new manifest/generation values for auditability and stale-operation diagnosis.

The outbox publisher is responsible for both external R2 projections. When it observes a versioned
published/rolled-back event, it loads that exact immutable Catalog manifest and first writes
`gold/vector-tiles/manifests/{manifest_id}.json` create-only. A retry that finds the key must verify
identical bytes/checksum or fail closed. It then reloads the active Catalog pointer and writes the
same schema-v2 bytes to `gold/vector-tiles/runtime-manifest.json` with
`Cache-Control: no-cache, max-age=0` only when the event manifest/generation is still active.

That mutable write is an R2 compare-and-swap, never a check followed by an unconditional overwrite.
The publisher reads the current pointer and ETag, then sends `If-Match: <observed-etag>`; bootstrap
uses `If-None-Match: *`. A failed precondition forces it to reload both Catalog authority and R2:
it retries the currently active manifest or skips a stale event. Thus an interleaving in which
publisher A checks, publisher B writes a newer pointer, and A writes last rejects A's stale ETag.
A stale event may safely finish its immutable object but cannot regress the mutable pointer. The
publisher never rewrites the frozen schema-v1 `gold/manifest.json`.

Before production, the publisher and Martin smoke tests must use dedicated derivative-bucket
configuration. The generic lakehouse `R2_BUCKET_NAME` adapter and its credentials are forbidden for
tile publication. Publisher credentials are bucket-scoped write credentials; Martin credentials
are separate bucket-scoped read-only credentials.

## Rejected

- Static base plus dynamic overlay/tombstone composition for one publication unit.
- A PostGIS-only runtime for units whose edit rate does not justify steady-state dynamic rendering.
- Naver internal vector/tile endpoints as domain data source.
- PMTiles direct browser runtime or a public R2 bucket as the production default.
- Gongzzang-owned manifest after foundation-platform Catalog cutover.
- Per-tile `file_asset` rows for every `.pbf` object.
- Reusing v1 flat-object fields for Martin/PMTiles.
- Mutating `previous_version` on an old manifest during rollback.

## Completion Definition

- The legacy `gold/manifest.json` remains schema-v1 byte-compatible until its anchor consumers are
  migrated; schema v2 is served from the Catalog runtime endpoint, projected create-only to
  `gold/vector-tiles/manifests/{manifest_id}.json`, and projected as the active no-cache pointer at
  `gold/vector-tiles/runtime-manifest.json`.
- Strict v1/v2 dispatch rejects unknown schema versions; v1 stays legacy-only.
- Every v2 publication unit carries one tagged source, UUID data/release identities,
  JavaScript-safe generations, a decimal-string Iceberg snapshot ID, complete layer metadata, and
  lineage.
- Every static release links to its immutable PMTiles `file_asset`; every dynamic release links to
  a ready, reconstructible PostGIS projection revision.
- Add/modify/delete, stale-build rejection, same-data serving rollback, and source readiness are
  mechanically tested.
- Gongzzang has no manifest write path and consumes the manifest only.
- Martin uses authenticated S3-compatible R2 access to a dedicated private derivative bucket; no
  canonical/lakehouse/recovery bucket or generic credential can be selected.
- The contract is referenced from the Catalog SSOT model, implementation plan, and Gongzzang ADR
  0036.
- R2 publish/read and decoded tiles are verified through dedicated smoke commands before live
  pointer writes are enabled.
- the R2 adapter and outbox tests prove fenced pointer compare-and-swap under two-publisher
  interleaving; unconditional overwrite of the v2 pointer is forbidden.

## References

- [Root ADR 0006 - Object-storage-first serving](../../../../docs/adr/0006-object-storage-first-serving.md)
- [Martin file sources and remote-prefix reload](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md)
- [Apache Iceberg branching and WAP](https://iceberg.apache.org/docs/latest/branching/)
- [Cloudflare R2 S3 conditional `PutObject`](https://developers.cloudflare.com/r2/api/s3/api/)
