# ADR 0006: Object-storage-first serving for reference data; Postgres for personalized/transactional

- Status: Accepted
- Date: 2026-07-21

## Context

The foundation-platform pipeline collects raw public data into Bronze objects,
processes it into Silver/Gold Iceberg tables on R2, and serves an
industrial-real-estate catalog (parcels, buildings, complexes) that the gongzzang
product consumes. The serving architecture must not require a second, always-on
Postgres copy of batch-oriented reference data as its canonical last mile.

Rather than fill an always-on Postgres serving copy, we adopt the modern
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
| Canonical processed dataset (Gold) | Apache Iceberg on Cloudflare R2 |
| Map tiles — static basemap (parcels/complex/admin) | Pre-rendered to PMTiles on R2 (`martin-cp`), fronted by Cloudflare CDN → Martin; manifest per GZ-ADR-0036 |
| Map tiles — dynamic (listing markers) | Martin serving MVT from PostGIS on the fly (Moka + CDN cached); incremental via PostGIS auto-reload / `mbtiles` diff |
| Catalog point-lookups (parcel/building by PNU) | Pre-rendered JSON on R2/CDN, or Cloudflare KV, keyed by PNU |
| Heavy / ad-hoc analytics | Trino over Iceberg (existing); DuckDB for light/embedded |
| Bronze → Silver → Gold processing | Spark (existing) |
| gongzzang listing search + personalized card feed | Postgres + PostGIS (existing) |
| Sessions, tile cache, rate-limit, JTI denylist | Redis (existing) |

Rule of thumb: **"same for everyone, batch-updated" → R2; "per-user, real-time" → Postgres.**

**Tile serving engine — Martin (Rust, MapLibre).** Both static basemap tiles (PMTiles on R2)
and dynamic listing tiles (MVT from PostGIS) are served through **Martin behind Cloudflare
CDN**. The goal is low-cost / high-efficiency, not zero servers: the CDN absorbs the bulk of
traffic (cache hits), so Martin — a lightweight Rust service — only handles cache misses, and
Postgres is touched only then. Martin also supplies bulk pre-render (`martin-cp`) and
incremental tileset diff (`mbtiles` diff/apply) out of the box — tooling we would otherwise
hand-build — and replaces gongzzang's hand-rolled `ST_AsMVT` serving with a maintained
standard. This refines, not contradicts, object-storage-first: giant static data is
pre-rendered to R2; a thin cached server fronts the dynamic remainder.

**Deferred** until scale earns them — data stays in open formats on R2, so adding them is a
no-migration engine swap: ClickHouse / Apache Pinot for high-QPS analytics serving;
Meilisearch / OpenSearch-with-Nori for Korean full-text search (no free-text search exists
today).

This is **almost entirely the existing stack** — R2, Iceberg, Spark, Trino, Postgres, and
Redis are already in use. The one added component is **Martin** (a lightweight Rust tile
server), chosen because it supplies dynamic serving, caching, bulk pre-render, and
incremental diff out of the box — replacing hand-rolled tile code. The core change is a
serving *pattern*, not a pile of new infrastructure.

## Consequences

- **Cost**: near-$0 serving at 0 users — no always-on serving DB for reference data, and R2
  egress is free. Cost scales with usage, not with an idle instance. Postgres remains only
  for gongzzang's own operational/personalized data.
- **The canonical serving gap resolves differently**: the pipeline's last mile becomes
  "render Gold → R2 tiles/JSON (+ optional KV)", not "populate a Postgres serving copy".
  Foundation catalog tiles are already contract-shaped for this (GZ-ADR-0036).
- **Honest boundary**: authenticated listing search / personalized feeds stay on Postgres.
  "No serving DB" applies to reference + spatial reads, not to gongzzang's transactional data.
- **Tile serving standardized on Martin**: gongzzang's hand-rolled `ST_AsMVT` serving is
  replaced by Martin behind the CDN — one maintained service for static PMTiles (R2) and
  dynamic PostGIS tiles, with built-in cache + `martin-cp` bulk render + `mbtiles` incremental
  diff. It is a server, but a thin one behind a cache, and we already run app + DB servers.
- **Reversibility**: because canonical data stays in open formats (Iceberg / PBF / JSON) on
  R2, any serving engine choice (KV vs static JSON; Trino vs DuckDB vs later ClickHouse/Pinot)
  is a swap with no data migration.
- **Proof-first rollout**: the existing Postgres catalog is NOT removed. A vertical slice —
  one `industrial_complex`: Gold → R2 tiles/JSON → consumed by the existing Naver renderer,
  no DB — proves the path before any migration.

## References

- GZ-ADR-0036 — static vector tile runtime contract (foundation tiles already R2-shaped)
- Martin (maplibre/martin) — Rust PostGIS / PMTiles / MBTiles tile server + `martin-cp` / `mbtiles` tooling
- ADR-0004 — verification SSOT (same "one definition" discipline, applied to serving)
