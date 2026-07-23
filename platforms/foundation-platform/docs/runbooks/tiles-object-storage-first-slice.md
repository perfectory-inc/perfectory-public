# Object-storage-first tile slice

## Scope and evidence status

This runbook covers one prelaunch Foundation slice: three parcels in one industrial complex,
served as Mapbox Vector Tiles through two Martin lanes.

- **Dynamic:** explicit PostGIS views → Martin → MVT.
- **Static:** the same views → `martin-cp` → MBTiles → PMTiles → Martin, using local file reads or
  R2 HTTP Range reads → MVT.
- **Consumer:** the checked local manifest resolves to Martin URLs that the existing Gongzzang
  Naver Maps/mapbox-gl integration can fetch without renderer changes.

The proof checks representative z11 aggregate and z14 parcel/exact-anchor responses. The archive
itself is rendered for every advertised zoom from z0 through z16, then every unpacked zoom is
decoded to enforce aggregate-only z0-11, exact-anchor-only z12-13, and parcel-plus-exact-anchor
z14-16 coverage. It rejects missing or extra layers, incorrect feature counts, wrong PNU/complex
identities, non-renderable point or polygon geometry, and any dynamic/static identity or full MVT
byte mismatch for the representative tiles.
The parcel layer emits canonical lowercase `pnu` plus the uppercase `PNU` compatibility alias used
by the unchanged mapbox-gl `promoteId` configuration. Aggregate rendering ends at exclusive style
zoom 12, so it remains visible through z11 without a gap before exact anchors begin at z12.

The checked-in public snapshot has been verified through the local PMTiles fallback. No
credentialed R2 result is claimed here: a real-R2 run is evidence only when it is executed with the
dedicated test bucket and its fresh output is retained. The local run produced a 3,255-byte PMTiles
archive (SHA-256
`aa84be475cf46dc4194844347ab5f4cf8082a3ab1171022c96edd618aa3ad714`) and decoded seven matching
dynamic/static features, including PNU `9999900000000000001`. No existing R2 bucket is written,
reconfigured, or deleted by the local lane. This is still a correctness slice, not a production
rollout or a national-scale load test.

## Ownership and storage model

Foundation owns canonical parcel/building/complex geometry, lineage, approval, static tile builds,
publication, and rollback. Gongzzang consumes the published HTTP/manifest contract and does not
write Foundation objects.

R2 holds immutable bytes, but public serving and private source data are separate security zones.
The canonical/source geometry remains in separate private buckets with no public domain. A
dedicated public static-tile serving bucket contains only publishable, immutable Gold derivatives.
Each release includes the immutable PMTiles archive, TileJSON, and manifest. A PMTiles archive is a
serving derivative, not an editable geometry source. PostGIS is bounded editable and serving state;
it is not the sole source of truth.
Loading an R2 source geometry version into PostGIS for an edit is expected. Approval must persist
the new private canonical/source version and lineage before publishing its derived Gold artifacts.
Foundation Catalog metadata remains the authority for active/previous versions, lineage, approval,
and pointer state; R2 holds their immutable bytes.

The lifecycle is:

1. Load or create geometry in PostGIS and serve it immediately through the dynamic Martin overlay.
2. Review and approve the edit.
3. Queue a debounced static publication keyed by the affected tileset/complex. The debounce value
   must live in publisher configuration, not in UI code or this runbook.
4. Allow an administrator to choose **Publish now** to bypass the debounce.
5. Rebuild and verify an immutable archive, then promote its pointer.
6. Hide or retire the corresponding dynamic feature only after the promoted static route is decoded
   and verified. Retiring it earlier creates a gap; leaving it indefinitely can render it twice.
7. Run nightly retry/reconciliation for approved versions that are missing, failed, or not promoted.

An edit of an identity already present in the published archive also needs a small dynamic
suppression/tombstone contract: suppress the old static PNU/feature while drawing its replacement,
and retain a tombstone for a deletion. Remove that suppression only after the promoted archive
contains the replacement or omission. The slice proves tile transport and identities; it does not
implement this future admin/runtime filtering contract.

The slice does not install the scheduler or admin UI. The production scheduler must run the nightly
reconciliation once per `Asia/Seoul` calendar day and expose its last-success/lag state; its exact
hour and debounce duration belong to deployment configuration. Launch-time zero-downtime is not a
requirement, but validation, ordering, and rollback correctness remain mandatory.

## Prerequisites

- Run from the repository root in Bash on the Windows host (Git Bash or an equivalent standard
  shell); do not add a PowerShell harness.
- Docker Engine with Compose v2 must be available.
- The harness pulls only the digest-pinned PostGIS, Martin, Protomaps PMTiles, and Rust images
  checked by the repository contract test.
- Do not enable shell tracing (`set -x`) in an R2 run. The harness disables inherited xtrace before
  reading any R2 variable, passes curl credentials through stdin, and
  disables user curl configuration at the executable boundary. Callers must still keep credentials
  and presigned URLs out of surrounding job logs.

The harness creates a unique Compose project, uses disposable PostGIS storage, and cleans up its
containers on exit. It applies every checked-in migration through the
production `foundation-migrate` SQLx runner, then applies `scripts/tiles/fixture.sql`; it does not
modify a developer or production database. `sqlx::Migrator::run` is the migration SSOT: its embedded
migration set rejects a dirty ledger, missing versions, and checksum drift before it applies every
pending migration. The proof, disposable integration harness, and Foundation CI all invoke that same
runner and do not duplicate SQLx's private-ledger or migration-count logic. The API build script
watches the migrations directory itself, so a cached `foundation-migrate` is rebuilt when a migration
file is added or removed, not only when an already embedded file changes.

## Local PMTiles fallback

Ensure that no R2 proof variables are exported, then run the proof twice:

```bash
for name in \
  R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_TILES_TEST_BUCKET_NAME \
  R2_ENDPOINT R2_TILES_READ_BASE_URL R2_TILES_READ_URL R2_TILES_OBJECT_KEY; do
  unset "$name"
done

scripts/tiles/tiles-slice-proof.sh
scripts/tiles/tiles-slice-proof.sh
```

Both runs must exit zero. The significant output is:

```text
DYNAMIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 expected PNU=9999900000000000001
STATIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 MATCHING features (LOCAL PMTiles fallback)
tiles-slice-proof: artifacts retained at .../target/tiles-slice-proof/<run-id>
```

Each run retains its local evidence below `target/tiles-slice-proof/<run-id>/`: dynamic/static
PBFs and response headers, canonical identity dumps, unpacked logical tiles, and
`tiles-slice-proof/local/foundation-static.{mbtiles,pmtiles,tilejson.json}`. These generated files
are proof output, not source-controlled artifacts. The deterministic proof archive contains 17
logical MVT entries with 3,247 total logical tile-payload bytes; the checked proof manifest records
those compatibility statistics and fails if they drift.

After the proof, run the repository verification SSOT and the complete web suite:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc \
  cargo xtask verify foundation

docker run --rm -v "$PWD:/workspace" -w /workspace \
  rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc \
  cargo xtask verify gongzzang

pnpm -C products/gongzzang/apps/web test
```

The static build chain is deliberately explicit. Three zoom-bounded `martin-cp` passes append to
one MBTiles file so each layer exists only across its advertised tile zooms:

```text
PostGIS snapshot
  -> martin-cp aggregate z0-11 (new MBTiles)
  -> martin-cp exact anchors z12-13 (append)
  -> martin-cp parcels + exact anchors z14-16 (append)
  -> composite Martin TileJSON vector-layer metadata
  -> mbtiles validate
  -> pmtiles convert
  -> pmtiles verify
  -> Martin
```

`martin-cp` does not write PMTiles. `mbtiles diff/apply-patch` operates on MBTiles build/sync
artifacts only; it is never an in-place update of a local or remote PMTiles archive.

## Real R2 proof mode

Use a dedicated proof bucket whose name contains `tiles-slice-proof`, with a bucket-scoped token
that can access only that bucket. Standard R2 API-token scoping is bucket-level, so the
`tiles-slice-proof/` object prefix is a second create-only guard, not an IAM boundary. Never bind the
proof domain or token to a Bronze, canonical, lakehouse, recovery, backup, production serving, or
other production-data bucket. The harness also reads the repository's production/recovery bucket
SSOT and rejects those names, but the dedicated bucket is the primary isolation boundary.

Supply all values from the environment or secret manager; never put them in a file in this
repository:

```bash
export R2_ACCOUNT_ID='<Cloudflare account ID>'
export R2_ACCESS_KEY_ID='<R2 test access key ID>'
export R2_SECRET_ACCESS_KEY='<R2 test secret access key>'
export R2_TILES_TEST_BUCKET_NAME='<dedicated bucket containing tiles-slice-proof>'
export R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
export R2_TILES_READ_BASE_URL='<HTTPS r2.dev or bound test custom-domain bucket URL>'

unset R2_TILES_READ_URL R2_TILES_OBJECT_KEY

scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
scripts/tiles/tiles-slice-proof.sh
```

The preflight performs no Docker or R2 request. It fails closed if the repository's protected-bucket
SSOT is missing/empty, rejects every declared production/recovery bucket, and applies the same
3-63 character lowercase-letter/digit/hyphen rule as the Foundation lakehouse registry (including
the no-leading/trailing/double-hyphen constraint).

Partial R2 configuration is an error; unset all variables for the local lane or provide the full
set. An exported-but-empty R2 variable also counts as partial configuration and fails rather than
silently selecting local fallback. The endpoint must be the account's exact R2 S3 endpoint. With
`R2_TILES_READ_BASE_URL`, the harness creates
`tiles-slice-proof/<run-id>/foundation-static.pmtiles` and appends that key to the base URL. The
base URL must be HTTPS and contain no query or fragment.

For a presigned or otherwise exact read URL, use the mutually exclusive exact-URL mode:

```bash
unset R2_TILES_READ_BASE_URL
export R2_TILES_OBJECT_KEY='tiles-slice-proof/<unique-run-id>/foundation-static.pmtiles'
export R2_TILES_READ_URL='<exact HTTPS read URL for that key; a presigned query is allowed>'

scripts/tiles/tiles-slice-proof.sh
```

The path before any query string must end in the exact `R2_TILES_OBJECT_KEY`. Setting both read
modes, omitting both, or supplying a key outside `tiles-slice-proof/` fails before upload.

The harness uploads with `If-None-Match: *`. It must never overwrite or delete an object. Before
Martin starts, it performs a full public GET, requires the byte count and
full public readback SHA-256 to equal the local archive, and separately requires an HTTP `206 Partial
Content` Range response. Static Martin then reads that verified remote URL and repeats the decoded
feature comparison. Success contains:

```text
DYNAMIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 expected PNU=9999900000000000001
STATIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 MATCHING features (REAL R2)
tiles-slice-proof: artifacts retained at .../target/tiles-slice-proof/<run-id>
```

The unique proof archive is intentionally left in R2 as evidence. The harness retains only an
allowlist of non-secret response fields (status, ETag, content length/range, and checksum metadata),
the complete public readback after its size and SHA-256 match, the 512-byte Range body after it
matches that verified archive prefix, and `r2-evidence.txt`. The PutObject response body is
discarded instead of being written to disk. Raw response headers and unverified public-readback or
Range bodies are deleted by the EXIT cleanup on both success and failure; this prevents a redirect
or error response from retaining a presigned URL. The evidence file records the dedicated bucket,
exact key, local and public-readback SHA-256, both byte counts, ETag, and exact `Content-Range`.
Preserve those files with the proof timestamp and command result. The harness provides no R2 delete
path. Any later retention cleanup is a separate, explicitly approved operation against an exact
recorded test key; it must never target a broad prefix or any production bucket.

## What the proof adapter does not mean

GZ-ADR-0036 schema v1 describes individual PBF objects:

- `object_key_prefix` is a physical R2 flat-tile prefix.
- `flat_tile_count` is the number of flat tile objects.
- `flat_tile_total_bytes` is their total object payload size.

The slice instead has one PMTiles object and Martin exposes
`/foundation_static/{z}/{x}/{y}`. Its checked manifest is intentionally marked
`proof-adapter-not-adr-0036-production`: `object_key_prefix` is a Martin route source ID and the
compatibility `flat_*` values describe archive entries/payloads, not R2 object statistics.

That is sufficient to prove the existing client's URL-first behavior, but it is not a production
GZ-ADR-0036 manifest. Before production, revise Foundation ADR-0004 and its inherited Gongzzang
ADR-0036/schema together, name archive-versus-route fields honestly, and update producer,
consumer, TileJSON, and drift tests. Do not silently redefine schema v1.

To exercise the current renderer unchanged, the parcel proof view also emits a proof-only uppercase `PNU` compatibility alias
beside the canonical lowercase `pnu`. Production contract reconciliation
must make identity one source of truth: change Gongzzang's `promoteId` to canonical `pnu` (or derive
it from an explicit manifest field), then remove the uppercase alias. Do not normalize this proof
duplication into the Foundation serving contract.

The production contract must also make the Martin tile URL version-addressed (a versioned route or
cache-key parameter bound to the immutable archive). Reusing the proof URL
`/foundation_static/{z}/{x}/{y}` for different archives would let old manifests and CDN entries
resolve to new or stale content and would make pointer rollback non-atomic.

## Production promotion checklist

Promotion is pointer-based and uses a new immutable object. It never overwrites the currently
published archive.

1. **Pass the contract gate.** Accept matching PMTiles-aware revisions to Foundation ADR-0004 and
   GZ-ADR-0036, then deploy their producer/consumer contract tests before exposing a production
   manifest. The revision must bind each manifest version to an immutable Martin tile URL/cache key.
2. **Freeze an approved source version.** Persist its geometry and lineage in the private canonical
   R2 source records. Record the exact snapshot/version used to populate PostGIS.
3. **Build once.** Run
   `PostGIS -> martin-cp -> MBTiles -> mbtiles validate -> pmtiles convert -> pmtiles verify` for
   the intended bounded layers and zooms. Decode representative and boundary tiles.
4. **Create and register the complete immutable release.** Build the immutable PMTiles archive,
   TileJSON, and manifest under the accepted PMTiles-aware contract. Upload every object with a
   create-only precondition to UUID/version-addressed keys in a dedicated public static-tile
   serving bucket, for example
   `gold/vector-tiles/artifacts/<artifact-uuid>/foundation-static.pmtiles`. Persist the source
   lineage, file-asset rows, vector-tile artifact rows, immutable manifest row, and other required
   Catalog rows before any public pointer changes. Record each object's size, SHA-256/ETag, bounds,
   zooms, and layer IDs. Never put private canonical/source geometry in this bucket.
5. **Verify the R2 origin.** `HEAD` the exact object, full-GET the public read URL and match its byte
   count and SHA-256 to the local build, then require byte-range GETs to return `206` with the
   expected `Content-Range`. Verify the PMTiles archive and decoded identities again from the read
   URL, not from the local build file.
6. **Use production domains.** Bind the R2 custom domain only to the dedicated public static-tile
   serving bucket and put the public Martin tile hostname behind Cloudflare CDN. Never bind a
   custom domain to the private canonical/source bucket. `r2.dev` is rate-limited and proof-only;
   it is not the production origin. Configure cache rules deliberately for `.pmtiles`/Range
   traffic and immutable versioned tile URLs. Before relying on cache, compare the exact PMTiles
   size with the zone's current cacheable-object limit. Cloudflare currently documents 512 MB per
   file on Free/Pro/Business and 5 GB by default on Enterprise. If the archive is too large, stop
   and either shard by bounded layer/region, change plan, or explicitly approve and load-test
   uncached origin reads; do not describe an oversized object as CDN-cached.
7. **Prove CDN behavior, not only content.** Repeat identical archive Range requests and identical
   MVT requests through their production hostnames. Retain `CF-Cache-Status` and `Age` response
   headers and confirm they match the intended cache rule; if responses remain `BYPASS`, do not
   claim CDN caching is enabled. Keep the semantic decode checks as a separate requirement.
8. **Configure CORS deliberately.** The present browser reads MVT from Martin, so the Martin/CDN
   hostname must allow the Gongzzang origin. Server-side Martin range reads do not rely on browser
   CORS. If the archive is ever fetched from a browser, restrict R2 CORS to the actual origins and
   `GET`/`HEAD`, test with an `Origin` header, and refresh cached objects after a CORS change.
9. **Stage Martin.** Point a candidate Martin instance at the exact custom-domain PMTiles URL and
   verify its TileJSON `vector_layers`, z11 aggregate, z14 parcel/anchor identities, health, and
   metrics. A named remote PMTiles URL is snapshotted when Martin starts; deploy/restart it for the
   new URL rather than assuming an in-place file update will hot-reload.
10. **Install an isolated tile-public publication path first.**
     The current generic `R2_BUCKET_NAME` publisher cannot publish static-tile pointers safely.
     Its Catalog broadcaster constructs the same generic R2 adapter used by other Foundation
     object-storage work. Add a dedicated
     `TilePublicObjectStorage` port backed by `FOUNDATION_TILE_PUBLIC_R2_BUCKET` plus separate
     tile-public endpoint/access-key/secret configuration, or deploy a separate publisher with the
     same isolation. Scope those credentials to the dedicated public static-tile bucket and deny
     access to private canonical, Bronze, lakehouse, and recovery buckets. Add contract tests proving
     tile publication cannot resolve the generic adapter and must not retarget `R2_BUCKET_NAME` to
     the public bucket. This is a hard pre-promotion prerequisite, not functionality supplied by the
     current slice.
11. **Promote through Catalog and outbox.** Only after steps 1-10 pass, CAS-switch the Catalog active
     version and emit the promotion outbox event in one database transaction. Do not write the
     public pointer directly. The new tile-public outbox publisher writes the public R2 manifest pointer
     and rejects stale events. Wait until the public manifest exposes the expected version. Keep
     pointer metadata short-lived or explicitly purged; keep immutable version-addressed
     archive/tile responses long-lived. Never reuse a tile cache key, and never purge or overwrite
     the old archive merely to promote.
12. **Verify through the client route.** Fetch the published manifest, TileJSON, and representative
     tiles through the production CDN and confirm the expected source layers and identities.
13. **Retire the dynamic overlay last.** Hide only identities included in the verified static
     version. The listing `ST_AsMVT` path remains untouched.

For rollback, keep every rollback-eligible versioned route live when practical. Otherwise stage and
verify the previous Martin route before any pointer change. In one database transaction, CAS-switch
the Catalog active version and emit the rollback outbox event. Let the outbox publisher write the
previous public R2 manifest pointer through the isolated `TilePublicObjectStorage` path, wait until
that version is externally visible, verify the
client route, and reconcile the dynamic overlay last. Old private source versions and public Gold
releases remain for lineage and rollback.

Cloudflare documents that R2 caching requires a
[custom domain](https://developers.cloudflare.com/cache/interaction-cloudflare-products/r2/) and
that `r2.dev` lacks production cache/WAF behavior. Configure and test
[R2 CORS](https://developers.cloudflare.com/r2/buckets/cors/) against the real browser origin.
Martin documents remote PMTiles as an
[HTTP Range-capable tile source](https://maplibre.org/martin/sources-tiles/) and notes that named
[remote file sources are snapshotted at startup](https://maplibre.org/martin/sources-files/).

## Health and observability exception

The unmodified Martin image used by this proof exposes `/health` and `/_/metrics`. Those are
third-party native endpoints and are a proof-only exception to the monorepo convention.

Before production, place an adapter/proxy around Martin that exposes:

- `/healthz` for process liveness;
- `/readyz` for readiness after the configured PostGIS or PMTiles source can be read; and
- `/metrics` for the protected scrape path backed by Martin's `/_/metrics`.

Do not publish the metrics endpoint to unauthenticated internet traffic. CDN health checks must use
the adapter contract, not depend directly on Martin's private endpoint names.

## Troubleshooting and stop conditions

- **The script reports local fallback:** no R2 variables were visible. This is expected offline and
  is not real-R2 evidence.
- **It rejects partial credentials:** either supply the complete test set or unset every R2 proof
  variable. Do not weaken the check.
- **Upload returns precondition failure:** the key already exists. Use a new unique proof key; never
  overwrite it.
- **Range read returns `200` instead of `206`:** stop. The chosen URL/CDN path has not proved the
  random-access contract Martin needs.
- **Full public readback size or SHA-256 differs:** stop. The public URL is stale, misbound, or does
  not resolve to the object just uploaded; representative matching tiles are not sufficient.
- **The PMTiles object exceeds the zone's cacheable-object limit:** stop promotion or select and
  validate an explicit sharding/plan/uncached-origin strategy.
- **Static features differ:** do not promote. Check the frozen snapshot, source zooms, `count`, PNU
  strings, `official_complex_code`, identity content encoding, and archive conversion.
- **Manifest `flat_*` compatibility values differ from the rendered MBTiles:** do not replace the
  check with a sentinel or skip it. Record the deterministic logical tile count/payload bytes in
  the checked proof manifest, then rebuild and run the complete proof twice.
- **Martin keeps the previous named remote archive:** start it with the new immutable URL; named
  remote sources are not in-place PMTiles watchers.
- **Browser CORS fails:** test with the real `Origin` header. If CORS changed after objects were
  cached, refresh/purge only the affected cache entries; do not overwrite the archive.
- **A production or Bronze/lakehouse/recovery bucket is selected:** stop before any write.
