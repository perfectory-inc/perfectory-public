# ADR 0006: Object-storage-first serving for reference data; Postgres for personalized/transactional

- Status: Accepted
- Date: 2026-07-21
- Amended: 2026-07-21 (tile publication lifecycle and tool-chain clarification)

## Context

The foundation-platform pipeline collects raw public data (Bronze, ~257 GiB in
Cloudflare R2), processes it (Silver/Gold as Iceberg on R2), and serves an
industrial-real-estate catalog (parcels, buildings, complexes) that the gongzzang
product consumes. A 2026-07-21 audit found the pipeline is genuinely built end-to-end
in pieces, but the canonical Postgres catalog tables are essentially unpopulated — the
"last mile" from Gold to a serving store was never run.

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
| Map tiles — static basemap (parcels/complex/admin) | Immutable, versioned Gold PMTiles in a dedicated public static-tile R2 bucket, read by Martin with HTTP Range and fronted by Cloudflare CDN |
| Map tiles — Foundation edits awaiting static publication | A bounded PostGIS overlay served immediately by Martin |
| Map tiles — dynamic Gongzzang listing markers | Existing Gongzzang `ST_AsMVT`/PostGIS path remains in place; the Martin slice is additive, not its migration |
| Catalog point-lookups (parcel/building by PNU) | Pre-rendered JSON on R2/CDN, or Cloudflare KV, keyed by PNU |
| Heavy / ad-hoc analytics | Trino over Iceberg (existing); DuckDB for light/embedded |
| Bronze → Silver → Gold processing | Spark (existing) |
| gongzzang listing search + personalized card feed | Postgres + PostGIS (existing) |
| Sessions, tile cache, rate-limit, JTI denylist | Redis (existing) |

Rule of thumb: **"same for everyone, batch-updated" → R2; "per-user, real-time" → Postgres.**

**Tile serving engine — Martin (Rust, MapLibre).** Martin serves immutable PMTiles from R2
with HTTP Range reads and serves the bounded Foundation edit overlay from PostGIS. Cloudflare
CDN absorbs repeated static reads; PostGIS is used for editable or not-yet-published geometry,
not as the only copy of canonical geometry. Gongzzang's existing listing `ST_AsMVT` endpoint
is not removed by this decision or by the proof slice.

Private canonical/source geometry and public serving derivatives are different R2 security zones.
Canonical, Bronze, lakehouse, recovery, and backup data never share the public static-tile bucket
or its custom domain. Test proof objects use a third, dedicated bucket and bucket-scoped token; an
object-key prefix is a create-only defense, not a substitute for bucket-level credential isolation.

The supported static build chain is exactly:

`PostGIS snapshot → martin-cp → MBTiles → mbtiles validate → pmtiles convert → pmtiles verify → R2`

`martin-cp` writes MBTiles, not PMTiles. `mbtiles diff/apply-patch` may optimize or synchronize
an MBTiles build artifact; it does not incrementally mutate PostGIS, does not patch a remote
PMTiles object, and does not avoid publishing a new immutable PMTiles version.

**Publication lifecycle.** Foundation owns canonical geometry and tile publication. An edit is
visible immediately through the dynamic Martin/PostGIS overlay. Approval queues a debounced
static build; an administrator may request **Publish now** to bypass that wait. A nightly
retry/reconciliation job repairs missed or failed publications. The dynamic feature is retired
only after the new archive has passed MBTiles/PMTiles validation, R2 range-read and decoded-tile
checks, and the published pointer has been promoted. Rollback changes the pointer to the prior
verified immutable archive; it never overwrites that archive. Replacing or deleting an identity
already present in static tiles also requires a dynamic suppression/tombstone until promotion, so
the old static representation is not rendered alongside the overlay.

**GZ-ADR-0036 production gate.** Its schema v1 defines `object_key_prefix` as a physical flat
R2 PBF prefix and `flat_tile_count`/`flat_tile_total_bytes` as flat-object statistics. A Martin
route such as `/foundation_static/{z}/{x}/{y}` backed by one PMTiles object has different
semantics. The local slice manifest is therefore only a marked proof adapter. Production must
revise Foundation ADR-0004 and inherited GZ-ADR-0036/schema plus both producer/consumer contract
tests; v1 fields must not be silently repurposed. Each manifest version must resolve to an
immutable version-addressed Martin tile URL/cache key so promotion and rollback do not reuse a CDN
cache identity. The same revision must converge parcel identity on canonical lowercase `pnu` (or
an explicit manifest-selected identity field); the proof-only uppercase `PNU` renderer alias must
not become a second production contract.

**Deferred** until scale earns them — data stays in open formats on R2, so adding them is a
no-migration engine swap: ClickHouse / Apache Pinot for high-QPS analytics serving;
Meilisearch / OpenSearch-with-Nori for Korean full-text search (no free-text search exists
today).

This is **almost entirely the existing stack** — R2, Iceberg, Spark, Trino, Postgres, and
Redis are already in use. The one added component is **Martin** (a lightweight Rust tile
server), chosen because it serves both PostGIS MVT and local or remote PMTiles. The core
change is a serving *pattern*, not a pile of new infrastructure.

## Consequences

- **Cost**: the static reference basemap does not require a full always-on PostGIS serving
  copy. R2/CDN carries steady-state reads; a bounded PostGIS state remains for editable
  Foundation overlays and Gongzzang keeps its operational/personalized Postgres data.
- **The "empty canonical tables" gap resolves differently**: the pipeline's last mile becomes
  "render Gold → R2 tiles/JSON (+ optional KV)", not "populate a Postgres serving copy".
  The existing client is URL-first, but the PMTiles manifest fields still require the explicit
  GZ-ADR-0036 production contract revision described above.
- **Honest boundary**: authenticated listing search / personalized feeds stay on Postgres.
  "No serving DB" applies to reference + spatial reads, not to gongzzang's transactional data.
- **Tile serving standardized on Martin for this Foundation slice**: one maintained server
  handles static PMTiles and the editable PostGIS overlay. Replacing Gongzzang listing tile
  serving is a separate future decision.
- **Reversibility**: because canonical data stays in open formats (Iceberg / PBF / JSON) on
  R2, any serving engine choice (KV vs static JSON; Trino vs DuckDB vs later ClickHouse/Pinot)
  is a swap with no data migration.
- **Proof-first rollout**: the existing Postgres catalog and listing tile path are NOT removed.
  One `industrial_complex` exercises PostGIS/Martin and PMTiles/Martin lanes before any
  production promotion. The proof's real-R2 branch is evidence only when it is actually run
  with the dedicated test credentials and reports `REAL R2`.

## References

- GZ-ADR-0036 — static vector tile runtime contract (flat-object v1; revision required for production PMTiles)
- Martin (maplibre/martin) — Rust PostGIS / PMTiles / MBTiles tile server and `martin-cp`
- ADR-0004 — verification SSOT (same "one definition" discipline, applied to serving)
- Internal foundation pipeline audit, 2026-07-21
