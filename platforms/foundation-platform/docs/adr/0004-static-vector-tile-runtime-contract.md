# ADR 0004 - Static Vector Tile Runtime Contract

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-12 |
| 상태 | Accepted |
| 상속 | [`gongzzang ADR 0036`](../../../../products/gongzzang/docs/adr/0036-static-vector-tile-runtime-contract.md) |
| 범위 | `foundation-platform` Catalog, static vector tile manifest, `gongzzang` map runtime |

## 결정

`gold/manifest.json` 의 최종 owner 는 `foundation-platform` Catalog 다.

Gongzzang ADR 0036 의 static vector tile runtime contract 를 상속하되,
foundation-platform cutover 이후에는 필지, 산업단지, 행정구역, 건물 등 Catalog spatial
layer 의 정적 vector tile manifest 를 `foundation-platform` 가 생성, 검증, publish 한다.

`gongzzang` 은 manifest consumer only 다. Gongzzang 은 manifest 를 읽어 지도 source 를
구성할 수 있지만, manifest version, artifact metadata, lineage, file asset 연결을
직접 write 하지 않는다.

## Runtime Pointer

Canonical pointer 는 다음 R2 object key 다.

```text
gold/manifest.json
```

보존/rollback object key 는 immutable artifact id 규칙을 따른다.

```text
gold/vector-tiles/manifests/{manifest_file_asset_id}.json
gold/vector-tiles/artifacts/{artifact_id}/{layer}.json
gold/vector-tiles/artifacts/{artifact_id}/{layer}/{z}/{x}/{y}.pbf
```

`gold/manifest.json` 은 no-cache serving pointer 이고 canonical truth는 Catalog다.
`gold/vector-tiles/artifacts/{artifact_id}/...` 아래 flat tile 은 immutable cache 로
다룬다. Catalog가 active/previous artifact를 관리하며 manifest pointer는 그 상태에서
재생성할 수 있다.

## Manifest Schema

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

## Catalog Ownership

The manifest is Catalog data because it describes foundation-platform spatial facts and their derived
runtime artifacts.

| Resource | Owner | Catalog link |
|---|---|---|
| `gold/manifest.json` | `foundation-platform` Catalog | `catalog.file_asset.object_key = 'gold/manifest.json'` |
| `gold/vector-tiles/manifests/{manifest_id}.json` | `foundation-platform` Catalog | immutable manifest `catalog.file_asset` row |
| `gold/vector-tiles/artifacts/{artifact_id}/{layer}.json` | `foundation-platform` Catalog | `tilejson_file_asset_id` |
| `<object_key_prefix>/{z}/{x}/{y}.pbf` | `foundation-platform` Catalog | artifact `object_key_prefix`; individual tile rows are not required |
| `artifacts[layer]` | `foundation-platform` Catalog | derived from `catalog.spatial_layer` and tile build metadata |
| `lineage.source_record_id` | `foundation-platform` Catalog | `catalog.source_record.id` |
| `lineage.*file_asset_id` | `foundation-platform` Catalog | `catalog.file_asset.id` |

Individual `.pbf` tiles do not need one `catalog.file_asset` row per object. That would create
unnecessary catalog cardinality. The manifest and TileJSON are `file_asset` rows, while the tile
set is represented by `object_key_prefix`, count, bytes, and checksum/build metadata.

## SpatialLayer Mapping

Each manifest artifact key maps to a foundation-platform spatial layer or layer family.

| Manifest artifact | Foundation Platform source |
|---|---|
| `parcels` | `catalog.parcel` + `catalog.spatial_layer(layer_kind = 'parcel_boundary')` |
| `complex` | `catalog.industrial_complex` + `catalog.spatial_layer(layer_kind = 'complex_boundary')` |
| `admin` | imported admin boundary `catalog.spatial_layer` |
| `buildings` | `catalog.building` + building footprint layer |

The manifest `source_layer` value is the vector tile layer name inside `.pbf`, not a DB table name.
It must be stable for runtime style and click handling.

## Gongzzang Runtime Contract

Gongzzang runtime must:

1. Fetch `gold/manifest.json` from foundation-platform/R2 public URL.
2. Register only layers present in `artifacts`.
3. Build vector tile URLs from `tiles_url_template`.
4. Treat `parcels` as core if the map workflow requires parcel interaction.
5. Treat optional layers such as `admin` or `complex` as skippable when absent.
6. Use manifest lineage for diagnostics, footer/source disclosure, and support reports.

Gongzzang runtime must not:

- write `gold/manifest.json`;
- rewrite `current_version` or `previous_version`;
- synthesize missing `artifacts[layer]` metadata;
- use Naver internal tile URLs as domain data source;
- use build-time env vars as the production active version pointer.

## Publish Gate

foundation-platform promote must fail before changing `gold/manifest.json` unless all required checks pass.

- `current_version` is new and immutable.
- `previous_version` equals the current production manifest version.
- every required `artifacts[layer]` has non-empty tile output.
- `tiles_url_template` has all required placeholders.
- `source_layer` is stable and non-empty.
- `tile_min_zoom <= tile_max_zoom`.
- `render_min_zoom <= render_max_zoom`.
- manifest file, TileJSON file, and source inputs have `catalog.file_asset` rows.
- every artifact has `lineage.source_record_id`.
- `catalog.source_record` captures source URL/license/checksum or equivalent provenance.
- Cloudflare purge, if configured, targets manifest only. Immutable tile paths are not purged.

## API Boundary

foundation-platform may expose the active manifest through Catalog API for internal consumers, but the
browser runtime may fetch the public R2/CDN manifest directly.

Recommended API surfaces:

```text
GET /catalog/v1/vector-tiles/manifest
GET /catalog/v1/vector-tiles/versions/{version}
PUT /catalog/v1/vector-tiles/manifest:promote
POST /catalog/v1/vector-tiles/manifest:rollback
```

The API response must be the same manifest contract or a strict superset. `gongzzang` must not
depend on any field that is absent from `gold/manifest.json`.

Promote is a foundation-platform Catalog admin operation. It registers the source record, manifest
file asset, TileJSON file assets, source file assets, and every `artifacts[layer]` record in the
same database transaction that switches the active manifest. It must require
`expected_current_version` to match the currently active manifest, reject duplicate
`current_version` values and object key conflicts, set `previous_version` to the version that was
active immediately before promote, and emit `catalog.vector_tile_manifest.promoted.v1` to the
Catalog outbox.

Manual rollback is a foundation-platform Catalog admin operation. It must target an existing immutable
`current_version`, require `expected_current_version` to match the currently active manifest,
atomically switch the active manifest pointer, set the rolled-back manifest's `previous_version`
to the version that was active immediately before the rollback, and emit
`catalog.vector_tile_manifest.rolled_back.v1` to the Catalog outbox.

The rollback API must verify a staff Bearer token through foundation-platform Staff Identity before mutation.
Only `MASTER_ADMIN`, `CATALOG_ADMIN`, or `VECTOR_TILE_ADMIN` may roll back vector tile manifests.
The staff identity comes from Zitadel token verification, while the role set used for this decision
comes from foundation-platform Staff Identity DB roles. `operator_staff_id` is derived from the verified staff
session, never trusted from the request body. The event must include that verified
`operator_staff_id`, optional `request_id`, `previous_manifest_id`, and `expected_current_version`
for auditability and stale-operation diagnosis.

The outbox publisher is responsible for the external R2 pointer write. When it observes
`catalog.vector_tile_manifest.rolled_back.v1` or `catalog.vector_tile_manifest.promoted.v1`, it
reloads the currently active Catalog manifest and writes the runtime JSON to
`gold/manifest.json` with `Cache-Control: no-cache, max-age=0`. If the event payload's
`manifest_id` is no longer the active manifest, the publisher treats the event as stale and skips
the object write so delayed delivery cannot regress the public pointer.

Before enabling R2 publish in a real environment, operators must run the dedicated R2 smoke command:

```bash
cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- smoke-r2
```

The smoke command writes, reads, and deletes only `gold/_smoke/foundation-platform-r2-smoke.json` by
default. It refuses `gold/manifest.json` so verification cannot accidentally flip the public
runtime pointer.

## Rejected

- Runtime PostGIS vector tile server as the default path.
- Naver internal vector/tile endpoints as domain data source.
- PMTiles direct browser runtime as the production contract.
- Gongzzang-owned manifest after foundation-platform Catalog cutover.
- Per-tile `file_asset` rows for every `.pbf` object.

## Completion Definition

- `gold/manifest.json` is represented by a foundation-platform `catalog.file_asset` row.
- The active manifest carries `current_version`, `previous_version`, `tiles_url_template`,
  `artifacts[layer]`, zoom ranges, `source_layer`, and lineage links.
- Every artifact links to `catalog.source_record` and relevant `catalog.file_asset` rows.
- Gongzzang has no manifest write path and consumes the manifest only.
- The contract is referenced from the Catalog SSOT model and migration plan.
- R2 publish is verified through the dedicated smoke command before live pointer writes are enabled.
