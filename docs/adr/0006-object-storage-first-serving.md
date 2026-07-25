# ADR 0006: Object-storage-first serving for reference data; Postgres for personalized/transactional

- Status: Accepted
- Date: 2026-07-21
- Amended: 2026-07-24 (single-source spatial publication and manifest v2)

## Context

The foundation-platform pipeline collects raw public data (Bronze, ~257 GiB in
Cloudflare R2), processes it (Silver/Gold as Iceberg on R2), and serves an
industrial-real-estate catalog (parcels, buildings, complexes) that the gongzzang
product consumes. A 2026-07-21 audit found the pipeline is genuinely built end-to-end
in pieces, but the canonical Postgres catalog tables are essentially unpopulated — the
"last mile" from the selected Iceberg revision to a serving store was never run.

Rather than make Postgres canonical or use it as the steady-state origin for batch-stable tiles, we
adopt the modern
**object-storage-first** (a.k.a. zero-disk / diskless) paradigm: serve reads from object
storage + edge cache + pre-rendered artifacts, minimizing the traditional serving DB.
This is an established 2025–2026 pattern (Cloudflare Workers/KV, WarpStream, Turbopuffer,
Quickwit; Iceberg-on-object-storage convergence across AWS S3 Tables / Snowflake /
Databricks). It fits us especially well because (a) the catalog is read-mostly reference
data updated in batches, and (b) we are already on Cloudflare R2 (free egress).

Grounded in the actual code, not assumptions:

- The map renderer is **Naver Maps GL** (bundling mapbox-gl), driven URL-first. Foundation
  catalog vector tiles are **already** designed as static R2/CDN objects addressed by a
  manifest (GZ-ADR-0036, static vector tile runtime contract) — no renderer change
  is needed to serve them from R2.
- gongzzang consumes the catalog via two point-lookups only
  (`catalog/v1/parcels/by-pnu/{pnu}`, `.../buildings`) — tiny, PNU-keyed, immutable-ish.
- gongzzang **listing search** is authenticated, per-viewer (`is_bookmarked`), exact-count,
  paginated, sorted, over live mutable rows — structurally relational, NOT pre-renderable.

## Decision

**Serve reference/spatial reads object-storage-first; keep personalized/transactional
reads on Postgres.** Per access pattern:

| Access pattern | Store / engine |
|---|---|
| Canonical spatial feature history | Catalog-selected `silver.*` SCD2 Apache Iceberg snapshots on Cloudflare R2 |
| Curated consumer projections (Gold) | Rebuildable Iceberg tables/artifacts derived from the selected Silver snapshot |
| Map tiles — static basemap (parcels/complex/admin/buildings) | Immutable, versioned PMTiles serving derivatives in a dedicated private serving-derivative R2 bucket, read by Martin through the S3-compatible API and fronted by Cloudflare CDN |
| Map tiles — Foundation units with a newly approved edit | A complete, reconstructible PostGIS serving projection rendered by Martin |
| Map tiles — dynamic Gongzzang listing markers | Existing Gongzzang `ST_AsMVT`/PostGIS path remains in place; the Martin slice is additive, not its migration |
| Catalog point-lookups (parcel/building by PNU) | Pre-rendered JSON on R2/CDN, or Cloudflare KV, keyed by PNU |
| Heavy / ad-hoc analytics | Trino over Iceberg (existing); DuckDB for light/embedded |
| Bronze → Silver → Gold processing | Spark (existing) |
| gongzzang listing search + personalized card feed | Postgres + PostGIS (existing) |
| Sessions, tile cache, rate-limit, JTI denylist | Redis (existing) |

Rule of thumb: **"same for everyone, batch-updated" → R2; "per-user, real-time" → Postgres.**

**Tile serving engine — Martin (Rust, MapLibre).** For every
`(publication_unit, serving_generation)`, Foundation selects exactly one complete Martin source:
`DynamicPostgis` XOR `StaticPmtiles`. Martin serves the complete PostGIS projection while a unit has
newly approved content, then serves one immutable PMTiles release after scheduled publication.
The browser never composes a static base with an edit overlay, tombstone layer, or feature
suppression list. Different publication units may independently be static or dynamic.

Cloudflare CDN absorbs repeated static reads. PostGIS remains warm and complete so the next edit can
be exposed immediately, but it is a serving projection reconstructed from the selected R2/Iceberg
Silver SCD2 snapshot and the audited edit ledger. It is not the only copy of canonical geometry.
Gongzzang's existing listing `ST_AsMVT` endpoint and `filter_hash`/marker-delta contract are separate
product-owned runtime paths and are not removed by this decision.

Canonical/source geometry and serving derivatives are different private R2 security zones.
Canonical, Bronze, lakehouse, recovery, and backup data never share the static-tile derivative
bucket. Martin reads only that derivative bucket with a separate bucket-scoped, read-only
credential through the S3-compatible API. Standard R2 API tokens are bucket-scoped; an object-key
prefix is a discovery/create-only convention, not an IAM boundary. A public `r2.dev` URL or
bucket-bound custom domain is proof-only or an explicitly authorized alternative, not the
production default.

The supported static build chain is exactly:

`PostGIS snapshot → martin-cp → MBTiles → mbtiles validate → pmtiles convert → pmtiles verify → R2`

`martin-cp` writes MBTiles, not PMTiles. `mbtiles diff/apply-patch` may optimize or synchronize
an MBTiles build artifact; it does not incrementally mutate PostGIS, does not patch a remote
PMTiles object, and does not avoid publishing a new immutable PMTiles version.

**Publication lifecycle.** Foundation owns canonical geometry and tile publication. A public edit is
first committed through an Iceberg Write-Audit-Publish branch, projected into a complete
pointer-selected PostGIS source, decoded through Martin, and selected with compare-and-swap.
That dynamic source becomes visible immediately after the active release commits. Approval queues a
debounced static build; an administrator may request **Publish now** to bypass the wait. A scheduled
retry/reconciler repairs failed jobs.

The build freezes the selected projection generation, renders and validates one complete unit, and
uploads a new immutable PMTiles object create-only. Promotion waits until Martin can read and decode
that exact R2 object, then selects a new complete static release only if the input dynamic release is
still active. A concurrent edit therefore makes the candidate `SUPERSEDED`. Add, modify, and delete
all follow the same whole-unit switch; no stale static feature can remain behind an overlay.
Rollback selects another retained, validated immutable release for the same data revision. A
business-data revert creates a new canonical revision instead of mutating an old release.

**Manifest contract.** GZ-ADR-0036 schema v1 remains a bounded legacy flat-PBF contract:
`object_key_prefix` is a physical tile prefix and `flat_tile_count` /
`flat_tile_total_bytes` are counts for individual objects. Those fields are never repurposed for
PMTiles or Martin routes. Schema v2 groups layers under publication units and carries:

- an exact `schema_version: 2`;
- a global, JavaScript-safe `manifest_generation` used only as a poll/change token;
- an exact `refresh_after_seconds: 4` launch polling interval;
- a per-unit UUID `data_revision`, JavaScript-safe `serving_generation`, immutable
  `active_release_id`, and canonical Iceberg snapshot ID encoded as a positive decimal string;
- one tagged `source` value, `dynamic_postgis` or `static_pmtiles`, never both;
- one stable dynamic or release-addressed static Martin tile URL and the unit's complete MVT layer metadata; and
- transport-specific PostGIS projection or PMTiles object/checksum/size lineage.

The production client dispatches exactly on schema version, validates the full manifest, and
replaces only a unit whose `serving_generation` changed. `manifest_generation` never selects a tile
source. Static releases use a new immutable Martin route/cache identity. Dynamic Martin URLs are
stable and query-free: the `vector_tile_runtime_manifest_pointer` is the only source selector, and
the `serving_postgis.*_current` view joins that pointer to exactly one committed `data_revision`.
Parcel identity converges on canonical lowercase `pnu`; proof-only uppercase `PNU` is not a second
production contract. Exact rollback is a complete pointer switch, never a cache-busting query string.

The first v2 migration unit is only `parcels`. The existing schema-v1 endpoint, persistence model,
events, `NEXT_PUBLIC_TILES_MANIFEST_URL` meaning, and `gold/manifest.json` bytes remain frozen. Those
v1 bytes still contain parcels and both anchor artifacts; the v2-aware runtime registers only
`parcel_anchor_aggregate` and `parcel_anchor` from them. V2 uses the distinct Catalog endpoint
`/catalog/v1/vector-tiles/runtime-manifest` and R2 projection
`gold/vector-tiles/runtime-manifest.json`, plus create-only history at
`gold/vector-tiles/manifests/{manifest_id}.json`. During this bounded migration, Gongzzang ignores
the v1 parcel artifact and loads v2 parcels, so no publication unit has two active sources. The
anchor, complex, admin, and building units move only after separate producer/consumer parity.
Concurrent outbox workers update the mutable v2 R2 pointer only through ETag compare-and-swap;
check-then-unconditional-overwrite is forbidden.

**Deferred** until scale earns them — data stays in open formats on R2, so adding them is a
no-migration engine swap: ClickHouse / Apache Pinot for high-QPS analytics serving;
Meilisearch / OpenSearch-with-Nori for Korean full-text search (no free-text search exists
today).

This is **almost entirely the existing stack** — R2, Iceberg, Spark, Trino, Postgres, and
Redis are already in use. The one added component is **Martin** (a lightweight Rust tile
server), chosen because it serves both PostGIS MVT and local or remote PMTiles. The core
change is a serving *pattern*, not a pile of new infrastructure.

## Consequences

- **Cost**: R2/CDN carries steady-state static reads, so PostGIS need not serve the steady-state map
  traffic. A complete warm PostGIS projection remains for immediate source switches and validation;
  static publication reduces compute/load, not Foundation geometry storage to zero.
- **The "empty canonical tables" gap resolves differently**: the spatial pipeline's last mile becomes
  "select Silver snapshot → derive Gold/PMTiles/JSON", not "make Postgres the canonical copy".
  The existing client is URL-first; the accepted manifest v2 contract must be implemented and
  verified on both Foundation and Gongzzang before production publication.
- **Honest boundary**: authenticated listing search / personalized feeds stay on Postgres.
  Object-storage-first means no canonical Postgres geometry and no PostGIS origin load for
  steady-state static tiles. Foundation still operates one complete warm, derived PostGIS projection
  for immediate edits, validation, and static rebuilds.
- **Tile serving standardized on Martin for Foundation units**: the same open-source engine handles
  complete static PMTiles and complete dynamic PostGIS sources. Replacing Gongzzang listing tile
  serving is a separate future decision.
- **Reversibility**: because canonical data stays in open formats (Iceberg / PBF / JSON) on
  R2, any serving engine choice (KV vs static JSON; Trino vs DuckDB vs later ClickHouse/Pinot)
  is a swap with no data migration.
- **Proof-first rollout**: the existing Postgres catalog and listing tile path are NOT removed.
  One `industrial_complex` exercises PostGIS/Martin and PMTiles/Martin lanes before any
  production promotion. The proof's real-R2 branch is evidence only when it is actually run
  with the dedicated test credentials and reports `REAL R2`.

## References

- [Single-source spatial publication architecture](../architecture/single-source-spatial-publication.md)
- [Foundation ADR 0004 - Vector tile publication contract](../../platforms/foundation-platform/docs/adr/0004-static-vector-tile-runtime-contract.md)
- GZ-ADR-0036 — vector tile runtime contract (legacy flat-object v1 and single-source v2)
- [Martin file sources](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md)
  — Rust PostGIS / PMTiles / MBTiles serving, S3-compatible R2, and remote-prefix polling
- ADR-0004 — verification SSOT (same "one definition" discipline, applied to serving)
- Internal foundation pipeline audit, 2026-07-21
