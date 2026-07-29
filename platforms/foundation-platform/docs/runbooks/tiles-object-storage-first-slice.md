---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Object-storage-first tile slice

## Scope and evidence status

This runbook covers one prelaunch Foundation slice: three parcels in one industrial complex,
served as Mapbox Vector Tiles through two Martin lanes.

- **Dynamic:** explicit PostGIS views → Martin → MVT.
- **Static:** the same views → `martin-cp` → MBTiles → PMTiles → Martin, using local file reads or
  proof-only R2 HTTP Range reads → MVT. Production uses Martin's authenticated S3-compatible R2
  source described below. Martin 1.12 `pmtiles.paths` is the production discovery point: it is
  configured with the private R2 S3 endpoint and read-only credentials, and it accepts only release
  PMTiles objects.
  The accepted real-R2 proof uses that private S3 path directly; the public HTTP Range path is only
  an optional legacy compatibility check.
- **Consumer:** the checked local manifest resolves to Martin URLs that the existing Gongzzang
  Naver Maps/mapbox-gl integration can fetch without renderer changes.

The proof checks representative z11 aggregate and z14 parcel/exact-anchor responses. The archive
itself is rendered for every advertised zoom from z0 through z16, then every unpacked zoom is
decoded to enforce aggregate-only z0-11, exact-anchor-only z12-13, and parcel-plus-exact-anchor
z14-16 coverage. It rejects missing or extra layers, incorrect feature counts, wrong pnu/complex
identities, non-renderable point or polygon geometry, and any dynamic/static identity or full MVT
byte mismatch for the representative tiles.
The v2 parcel layer emits only canonical lowercase `pnu`; the legacy fixture view retains an
uppercase `PNU` alias solely for the frozen v1 proof contract. Aggregate rendering ends at exclusive style
zoom 12, so it remains visible through z11 without a gap before exact anchors begin at z12.

The latest real-R2 run was verified against the canonical
`foundation-platform-tile-derivatives-prod` bucket using only the unique
`tiles-slice-proof/<run-id>/` prefix. Martin read the private PMTiles object through its
authenticated S3 origin and decoded seven matching dynamic/static features, including pnu
`9999900000000000001`. Upload uses `If-None-Match: *`; the harness never overwrites or deletes an
object. The local lane remains available for offline reproduction. This is still a correctness
slice, not a production rollout or a national-scale load test.

## Ownership and storage model

Foundation owns canonical parcel/building/complex geometry, lineage, approval, static tile builds,
publication, and rollback. Gongzzang consumes the published HTTP/manifest contract and does not
write Foundation objects.

R2 holds immutable bytes, but canonical data and serving derivatives are separate private security
zones. Canonical/source geometry remains in the lakehouse bucket. A dedicated private
serving-derivative bucket contains only publishable, immutable PMTiles serving releases.
Each release includes the immutable PMTiles archive, TileJSON, and manifest. A PMTiles archive is a
serving derivative, not an editable geometry source. PostGIS is a complete warm serving projection
reconstructible from the Catalog-selected R2/Iceberg snapshot and audited publication inputs; it is
not the sole source of truth. Static serving removes steady-state tile-rendering load from PostGIS,
not the warm projection itself.

Foundation Catalog metadata remains the authority for active releases, data and serving
generations, lineage, approval, and rollback history. R2 holds immutable bytes. Standard R2 tokens
are bucket-scoped: Martin gets a separate read-only credential for the derivative bucket, while the
publisher gets a separate write credential. The release prefix limits discovery and create-only
keys; it is not an IAM boundary.

The lifecycle is:

1. Branch from the Catalog-selected Iceberg snapshot, write and validate the approved edit through
   Iceberg WAP, and prepare a **complete** pointer-selected PostGIS projection for the unit.
2. Decode that exact dynamic Martin source, then atomically select it. The unit becomes visible
   immediately from one complete dynamic source.
3. Queue a debounced static publication keyed by the publication unit. The debounce value
   must live in publisher configuration, not in UI code or this runbook.
4. Allow an administrator to choose **Publish now** to bypass the debounce.
5. Freeze the selected PostGIS generation, rebuild and verify one complete immutable archive, then
   upload it create-only.
6. After Martin discovers and decodes that exact archive, compare-and-swap the whole unit from
   `DynamicPostgis` to `StaticPmtiles`. The old and new sources are never rendered together.
7. Run nightly retry/reconciliation for approved versions that are missing, failed, or not promoted.

Add, modify, and delete use the same complete-source switch. There is no Foundation overlay,
tombstone, or client feature-suppression contract. A static build that loses the active-release CAS
is `SUPERSEDED` and cannot resurrect stale geometry.

The slice does not install the scheduler or admin UI. The production scheduler must run the nightly
reconciliation once per `Asia/Seoul` calendar day and expose its last-success/lag state; its exact
hour and debounce duration belong to deployment configuration. Launch-time zero-downtime is not a
requirement, but validation, ordering, source completeness, and rollback correctness remain
mandatory.

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

The v2 local fixture is additive: `infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql` selects
one complete `parcels` dynamic release and never rewrites the frozen v1 seed. The stable dynamic
Martin URL is query-free; `serving_postgis.parcel_boundary_current` follows the one runtime-manifest
pointer to the selected `data_revision`. Martin's dynamic cache is disabled with the supported
`cache: disable` setting.

## Official administrative-boundary geometry path

The boundary producer is intentionally separate from the tile switch:

```text
official GeoJSON (EPSG:4326)
  -> write-official-administrative-boundary-source-snapshot
  -> write-administrative-spatial-scope-registry
  -> publish-administrative-boundary-postgis
  -> complete admin dynamic release + runtime-manifest CAS
  -> Martin /admin/{z}/{x}/{y}
```

The source writer retains Polygon/MultiPolygon geometry and its hash. The registry rejects a missing
or changed geometry hash. The PostGIS publisher requires an existing Catalog revision, source record,
and `status=ready` registry evidence; it appends only to
`serving_postgis.administrative_unit_boundary_publication`, never to an ad-hoc table. It creates
stable units using `scope:{scope-kind}:{canonical-code}` and appends official name/code facts when
the source supplies a name. It does not fabricate sigungu/sido names from a bbox-only row.

Required publisher inputs are supplied only through the environment:

```bash
export DATABASE_URL='postgres://...'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CONFIRM=1
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION='<revision UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID='<positive decimal>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID='iceberg:<snapshot-id>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID='<source-record UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY='gold/admin-boundaries/<snapshot>.geojson'

foundation-outbox-publisher publish-administrative-boundary-postgis
```

Martin is configured with the `admin` source, but the current view joins the runtime-manifest pointer.
Therefore publishing the projection alone cannot leak an unapproved revision: an operator must create a
complete `admin` dynamic release and promote a manifest containing every publication unit through the
existing CAS function. The checked-in publisher performs that complete operation after the projection
has been validated. Supply a fresh release/manifest UUID, the current manifest UUID (or leave it unset
only for the first-ever manifest), and the real local/CDN Martin URL:

```bash
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CONFIRM=1
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION='<revision UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID='<positive decimal>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID='<source-record UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID='<file-asset UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID='<current manifest UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID='<new release UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID='<new manifest UUID>'
export FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE='http://127.0.0.1:3110/admin/{z}/{x}/{y}'

foundation-outbox-publisher promote-administrative-boundary-runtime
```

The command builds a complete next manifest (including parcels and every other existing publication
unit), then calls the database CAS function. The browser can consume the v2 `admin` unit with the
existing MapLibre bridge; the legacy v1 `admin` artifact remains a fallback until that release is
promoted.

### Disposable end-to-end smoke proof

When the official government boundary snapshot is not available, run the checked-in proof with a
reserved-coordinate synthetic fixture:

```bash
bash scripts/tiles/administrative-boundary-slice-proof.sh
```

It starts disposable PostGIS and Martin containers, writes a synthetic legal-dong and sigungu
source snapshot, validates the registry, publishes PostGIS geometry, promotes the CAS runtime
manifest, and decodes the resulting Martin MVT. The fixture is deliberately non-official data and
must never be promoted to production; replace it with the verified official source snapshot before
any real release.

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
DYNAMIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 expected pnu=9999900000000000001
STATIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 MATCHING features (LOCAL PMTiles fallback)
tiles-slice-proof: artifacts retained at .../target/tiles-slice-proof/<run-id>
```

Each run retains its local evidence below `target/tiles-slice-proof/<run-id>/`: dynamic/static
PBFs and response headers, canonical identity dumps, unpacked logical tiles, and
`tiles-slice-proof/local/foundation-static.{mbtiles,pmtiles,tilejson.json}`. These generated files
are proof output, not source-controlled artifacts. The deterministic proof archive contains 17
logical MVT entries with 3,214 total logical tile-payload bytes; the checked proof manifest records
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

For the canonical production-shaped proof, use the Foundation tile-derivatives bucket and a
bucket-scoped publisher/read pair. The harness writes only a unique
`tiles-slice-proof/<run-id>/` prefix and rejects any other bucket through the checked-in R2
connection contract. Standard R2 API-token scoping is bucket-level, so the prefix is a second
create-only guard, not an IAM boundary. Lakehouse, Bronze, recovery, backup, and other data buckets
are never valid tile targets.

The legacy proof-only HTTP lane deliberately requires an HTTPS Range URL to prove public remote
PMTiles reads. It is optional. The production-shaped lane is the canonical path: Martin reads the
private derivative bucket through authenticated S3-compatible access, so no public R2 URL or R2 CORS
policy is required.

Supply all values from the environment or secret manager; never put them in a file in this
repository:

For repeatable canonical runs, the tile credentials come from the ignored
`platforms/foundation-platform/.env.local` profile (or the normal CI secret manager). The canonical
mode is explicit and opt-in:

```bash
TILES_SLICE_USE_CANONICAL_TILE_R2=1 scripts/tiles/tiles-slice-proof.sh --validate-r2-config-only
TILES_SLICE_USE_CANONICAL_TILE_R2=1 scripts/tiles/tiles-slice-proof.sh
```

The script reads only the `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_*` namespace, uses the publisher
key for the create-only upload, and injects the separate Martin read-only key into the production
config. It never prints credential values. The older `FOUNDATION_PLATFORM_R2_TILE_PROOF_*` tuple
remains available for the optional public HTTP readback lane.

```bash
export FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID='<Cloudflare account ID>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID='<R2 test access key ID>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY='<R2 test secret access key>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET='<dedicated bucket containing tiles-slice-proof>'
export FOUNDATION_PLATFORM_R2_TILE_PROOF_ENDPOINT="https://${FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID}.r2.cloudflarestorage.com"
export FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_BASE_URL='<HTTPS r2.dev or bound test custom-domain bucket URL>'

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

For an otherwise exact, query-free read URL, use the mutually exclusive exact-URL mode:

```bash
unset R2_TILES_READ_BASE_URL
export R2_TILES_OBJECT_KEY='tiles-slice-proof/<unique-run-id>/foundation-static.pmtiles'
export R2_TILES_READ_URL='<exact query-free HTTPS read URL for that key>'

scripts/tiles/tiles-slice-proof.sh
```

The path must end in the exact `R2_TILES_OBJECT_KEY`; query strings are rejected because Martin
1.12 must receive a query-free HTTP PMTiles source. Setting both read modes, omitting both, or
supplying a key outside `tiles-slice-proof/` fails before upload.

For the production publisher/serving boundary, do not reuse the generic `R2_*` environment. The
Rust preflight command is:

```bash
foundation-outbox-publisher validate-tile-derivative-r2
```

It requires `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ACCOUNT_ID`, `..._ENDPOINT`, `..._BUCKET`, separate
`..._PUBLISHER_ACCESS_KEY_ID`/`..._PUBLISHER_SECRET_ACCESS_KEY`, and separate Martin
`..._MARTIN_READ_ACCESS_KEY_ID`/`..._MARTIN_READ_SECRET_ACCESS_KEY`. The bucket must be a dedicated tile/derivative
bucket and the immutable prefix is fixed to `gold/vector-tiles/releases`. Release objects are
derived mechanically as `gold/vector-tiles/releases/{publication_unit}-{release_id}.pmtiles`;
callers cannot supply an arbitrary key. The publisher only uses create-only writes; Martin receives
read-only credentials. Never use a prefix as an IAM boundary or point either credential at a
lakehouse, Bronze, recovery, or backup bucket.

### Production bucket naming

Use the same three-part convention as the Lakehouse buckets—`<owner-service>-<purpose>-<environment>`—but
keep the serving derivative in its own bucket because its retention, credentials, and CDN exposure are
different from Bronze/Silver/Gold lakehouse data. The created production bucket is:

```text
foundation-platform-tile-derivatives-prod
```

The corresponding Lakehouse bucket remains `foundation-platform-lakehouse-prod`; do not substitute it
for the tile bucket. Future environments follow the same shape, for example
`foundation-platform-tile-derivatives-staging` and `foundation-platform-tile-derivatives-ci`.
Provisioning must be performed through the infrastructure/account automation and recorded here; an
operator must not silently create a differently named bucket and only change a secret.

The canonical harness uploads with `If-None-Match: *`, performs an authenticated HEAD, and requires
the ETag, content length, and checksum metadata to match the local archive. Static Martin then
discovers that private R2 prefix and repeats the decoded feature comparison. The optional public
HTTP lane additionally performs a full public readback SHA-256 comparison, a full GET, and a
`206 Partial Content` Range check. Success contains:

```text
DYNAMIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 expected pnu=9999900000000000001
STATIC tile OK bbox=127.1230,36.1230,127.1239,36.1239 decoded feature count=7 MATCHING features (REAL R2 via Martin S3 origin)
tiles-slice-proof: artifacts retained at .../target/tiles-slice-proof/<run-id>
```

The unique proof archive is intentionally left in R2 as evidence. The harness retains only an
allowlist of non-secret response fields (status, ETag, content length, and checksum metadata), plus
the optional public readback/Range evidence and `r2-evidence.txt`. The PutObject response body is
discarded instead of being written to disk. Raw response headers and unverified public-readback or
Range bodies are deleted by the EXIT cleanup on both success and failure. The evidence file records
the dedicated prefix, exact key, local checksum, byte count, and ETag.
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
GZ-ADR-0036 manifest. Foundation ADR-0004 and Gongzzang ADR-0036 now define strict manifest v2 with
publication units and a tagged `DynamicPostgis`/`StaticPmtiles` source. The producer, consumer, and
drift tests must implement that accepted v2 contract before production. Do not silently redefine
schema v1.

The frozen v1 fixture view retains a proof-only uppercase `PNU` compatibility alias beside the
canonical lowercase `pnu`. The v2 publication view and Gongzzang runtime use canonical `pnu`
directly; this alias is not part of the v2 Martin source or production identity contract.

Static manifest v2 routes are release-addressed. Reusing the proof URL
`/foundation_static/{z}/{x}/{y}` for different archives would let old manifests and CDN entries
resolve to new or stale content. Dynamic routes remain stable and query-free; their
`serving_postgis.*_current` view follows the Catalog runtime-manifest pointer.
The browser still owns one logical Mapbox source named `parcels`; a static promotion changes only
the validated Martin URL to the source ID derived from `{publication_unit}-{release_id}.pmtiles`.

## Production promotion checklist

Promotion selects a new immutable release descriptor and PMTiles object. It never overwrites the
current archive or mutates an old manifest.

1. **Deploy the v2 contract first.** Foundation and Gongzzang must both pass strict v1/v2 contract
   tests before Catalog may publish schema v2. Unknown schema versions fail closed. Keep the legacy
   v1 endpoint and `gold/manifest.json` bytes unchanged for the two anchor sources; publish v2 only
   from `/catalog/v1/vector-tiles/runtime-manifest` and
   `gold/vector-tiles/runtime-manifest.json`. Publish every v2 manifest create-only to
   `gold/vector-tiles/manifests/<manifest-uuid>.json` before moving that mutable pointer.
2. **Select canonical input.** Record the exact Catalog-selected Iceberg snapshot as a decimal
   string and the UUID `data_revision`. A build never follows an arbitrary Iceberg `main` head.
3. **Freeze the complete unit.** Materialize a build-scoped PostGIS snapshot for the active dynamic
   release. Never run separate `martin-cp` passes against a mutating live projection.
4. **Build and validate once.** Run
   `PostGIS -> martin-cp -> MBTiles -> mbtiles validate -> pmtiles convert -> pmtiles verify`.
   Decode representative and boundary tiles, stable identities, zoom coverage, expected omissions,
   and every required MVT source layer.
5. **Create the immutable release.** Upload with a create-only precondition to the dedicated private
   serving-derivative bucket, for example
   `gold/vector-tiles/releases/<publication-unit>-<release-uuid>.pmtiles`. Persist the immutable
   release, source lineage, file assets, checksum, byte size, bounds, zooms, and layer IDs in
   Catalog. Never put canonical source data in this bucket.
6. **Use isolated credentials.** The canonical Lakehouse `FOUNDATION_PLATFORM_R2_LAKEHOUSE_*` adapter is forbidden.
   The tile publisher has a bucket-scoped write credential; Martin has a different bucket-scoped
   read-only credential. Both are unable to access Bronze, lakehouse, or recovery buckets.
7. **Stage Martin from private R2.** Deploy the checked-in
    `scripts/tiles/martin-static-production.yaml`; inject `TILES_R2_PMTILES_PREFIX` as the
    derivative bucket's `s3://` release prefix, `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT`,
    `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION`, and the read-only
    `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID` /
     `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY` through environment/secrets. Do not
     use a named `pmtiles.sources` URL for scheduled discovery: named sources are startup snapshots.
8. **Verify the production-shaped route.** Wait for the expected release-addressed Martin source,
   then verify TileJSON layer IDs, authenticated R2 reads, health/readiness, and decoded MVT through
   the public Martin/CDN hostname. The R2 bucket itself needs no public domain.
9. **Prove CDN behavior.** Repeat identical MVT requests through the public hostname and retain
   `CF-Cache-Status`/`Age` evidence. CDN caches the immutable Martin MVT route; it does not need
   direct access to the PMTiles object. Keep semantic decode checks separate from cache checks.
10. **Configure browser policy.** Allow only the real Gongzzang origins on the Martin/CDN MVT
    endpoint. R2 CORS is irrelevant to the default server-side Martin S3 read.
11. **Compare and swap.** Only if the publication unit still selects the build's input dynamic
    release/data revision, atomically register/select `StaticPmtiles`, increment
    `serving_generation`, create a new immutable manifest UUID, increment global
    `manifest_generation`, and emit the outbox projection event. The Catalog transaction calls
    `catalog.promote_vector_tile_runtime_manifest(expected_manifest_id, next_manifest_id)`; this
    database CAS rejects stale writers and incomplete manifests before changing the one runtime
    pointer. The publisher writes the exact
    immutable manifest object create-only and verifies retry bytes before updating the active
    no-cache pointer. The pointer update uses the R2 ETag observed immediately before the write with
    `If-Match` (`If-None-Match: *` for bootstrap); `412` reloads Catalog and R2 instead of
    overwriting. A stale event never moves the pointer, even when two publisher workers interleave.
    Otherwise mark the build `SUPERSEDED`.
12. **Verify active-map replacement.** Fetch the Catalog v2 runtime manifest, frozen v1 anchor
    manifest, and representative tiles through the production client route. Confirm the v1 parcel
    artifact and old v2 parcel source are absent, the new complete v2 parcel source is loaded, and
    both parcel-anchor plus listing sources are unchanged.

For serving rollback, stage and verify a retained immutable release for the same `data_revision`,
then select it with the same expected-active-release CAS. Rollback creates a new immutable manifest;
it never edits a historical manifest or reconciles feature overlays. A business-data revert creates
a new Iceberg revision and follows the normal dynamic publication flow. Old canonical snapshots and
serving releases remain subject to explicit retention policy.

Martin documents Cloudflare R2 as a supported S3-compatible PMTiles store, remote-prefix polling
through `pmtiles.paths`, and startup snapshot behavior for named sources in
[Martin file sources](https://github.com/maplibre/martin/blob/martin-v1.12.0/docs/content/sources-files.md).

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
- **A direct-public PMTiles experiment exceeds the zone's cacheable-object limit:** that optional
  origin path is not proven. The default private-R2/Martin path caches MVT responses, not the whole
  PMTiles object at Cloudflare edge.
- **Static features differ:** do not promote. Check the frozen snapshot, source zooms, `count`, pnu
  strings, `official_complex_code`, identity content encoding, and archive conversion.
- **Manifest `flat_*` compatibility values differ from the rendered MBTiles:** do not replace the
  check with a sentinel or skip it. Record the deterministic logical tile count/payload bytes in
  the checked proof manifest, then rebuild and run the complete proof twice.
- **Martin does not discover the new archive:** verify `pmtiles.paths`, S3 endpoint/bucket
  credentials, object prefix, immutable filename-derived source ID, and `reload_interval`. A named
  `pmtiles.sources` entry does not poll.
- **Browser CORS fails:** test the Martin/CDN MVT hostname with the real `Origin` header. R2 CORS is
  not involved in Martin's authenticated server-side S3 read.
- **A production or Bronze/lakehouse/recovery bucket is selected:** stop before any write.
