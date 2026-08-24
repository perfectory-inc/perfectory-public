#!/usr/bin/env bash
# Reproducible disposable proof for the official boundary serving path, for every publication unit
# that travels it: source snapshot -> registry -> PostGIS -> CAS runtime manifest -> Martin MVT.
#
# Two units travel it today, `admin` and `complex`, and they share one Postgres, one Martin and one
# compiled publisher here rather than one proof script each. A second copy of the 250 lines below
# would be a second set of answers to questions this path only asks once — how the containers are
# built, how a tile is decoded, what a promotion's evidence is — and the units differ only in their
# own inputs. The `complex` half additionally proves the negative: the same layer, from the same
# Martin, is empty before the promotion and populated after it.
#
# `complex` then continues into the second serving state ADR-0006 defines. A unit that has finished
# publishing leaves PostGIS for one immutable PMTiles archive, and the chain that builds it is fixed:
#
#   PostGIS view -> martin-cp -> MBTiles -> mbtiles validate -> pmtiles convert -> pmtiles verify
#     -> Martin
#
# The archive is built here and verified here; it is never uploaded. What the last section proves is
# that the two lanes answer identically and that a unit cannot occupy both states at once.
set -euo pipefail
set +x
IFS=$'\n\t'

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
RUST_IMAGE="rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc"
MARTIN_IMAGE="ghcr.io/maplibre/martin:1.12.0@sha256:6cb9f6fbe3f3aa9d76841120ac02ba562037bd2d303f38a93e80764298a0d21f"
POSTGIS_IMAGE="postgis/postgis:17-3.5-alpine@sha256:fe9821935d163abca5611e3e0a6a7c73c8c547f3412ed2036ec0ed8f789390da"
PMTILES_IMAGE="protomaps/go-pmtiles:v1.31.1@sha256:057f8e5a6c77e89b46eebd40d62d295a0b69009371542bc0abfe1ecbc7ee6285"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}"
RUN_RELATIVE="target/boundary-slice-proof/$RUN_ID"
RUN_DIR="$REPO_ROOT/$RUN_RELATIVE"
DB="boundary-slice-db-$$"
MARTIN="boundary-slice-martin-$$"
STATIC_MARTIN="boundary-slice-static-martin-$$"
NET="boundary-slice-net-$$"
DB_PASSWORD="boundary-slice-proof-$RUN_ID"
MARTIN_PORT="${BOUNDARY_SLICE_MARTIN_PORT:-3112}"
STATIC_MARTIN_PORT="${BOUNDARY_SLICE_STATIC_MARTIN_PORT:-3113}"

fail() {
  printf 'boundary-slice-proof: ERROR: %s\n' "$*" >&2
  exit 1
}

if command -v docker.exe >/dev/null 2>&1 && docker.exe info >/dev/null 2>&1; then
  DOCKER_EXECUTABLE=docker.exe
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  DOCKER_EXECUTABLE=docker
else
  printf 'boundary-slice-proof: Docker is required\n' >&2
  DOCKER_EXECUTABLE=docker
  exit 1
fi
docker() { command "$DOCKER_EXECUTABLE" "$@"; }

for command in docker curl date mkdir find sort cmp wc sed grep sha256sum; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'boundary-slice-proof: missing command %s\n' "$command" >&2
    exit 1
  }
done

host_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -am "$1"
  elif [[ "$DOCKER_EXECUTABLE" == "docker.exe" && "$1" =~ ^/mnt/([[:alpha:]])/(.*)$ ]]; then
    printf '%s:/%s\n' "${BASH_REMATCH[1]^}" "${BASH_REMATCH[2]}"
  elif [[ "$DOCKER_EXECUTABLE" == "docker.exe" && "$1" =~ ^/([[:alpha:]])/(.*)$ ]]; then
    printf '%s:/%s\n' "${BASH_REMATCH[1]^}" "${BASH_REMATCH[2]}"
  else
    printf '%s\n' "$1"
  fi
}

REPO_HOST_PATH="$(host_path "$REPO_ROOT")"
export MSYS_NO_PATHCONV=1
mkdir -p "$RUN_DIR"

# `-v` removes the anonymous volume the postgres image declares for its data directory. Without it
# each proof run orphans one, and nothing collects them — see the same fix in
# `scripts/verify/integration.sh`.
cleanup() {
  docker rm -f -v "$STATIC_MARTIN" "$MARTIN" "$DB" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$NET" >/dev/null
docker run -d --name "$DB" --network "$NET" \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD="$DB_PASSWORD" \
  -e POSTGRES_DB=tiles_slice_proof \
  "$POSTGIS_IMAGE" >/dev/null

ready=false
for _ in $(seq 1 90); do
  if docker exec "$DB" pg_isready -h 127.0.0.1 -U postgres -d tiles_slice_proof >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
$ready || { printf 'PostGIS did not become ready\n' >&2; exit 1; }

psql_file() {
  docker exec -i "$DB" psql -X -h 127.0.0.1 -U postgres -d tiles_slice_proof \
    -v ON_ERROR_STOP=1 -q -f - < "$1"
}

# One scalar out of the proof database, for an assertion. A `psql | grep` would put a pipe in the
# judgment position and report the wrong exit code (root ADR-0012).
#
# `ON_ERROR_STOP` because this also runs the statements that build a static release: without it psql
# reports a rejected INSERT on stderr and still exits 0, which would turn a gate doing its job into
# a proof that silently continued past it.
psql_value() {
  docker exec "$DB" psql -X -At -F '|' -h 127.0.0.1 -U postgres -d tiles_slice_proof \
    -v ON_ERROR_STOP=1 -c "$1"
}

UUID_PATTERN='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

run_publisher() {
  local env_values=()
  while [[ "${1:-}" == "-e" ]]; do
    [[ "${2:-}" == *=* ]] || {
      printf 'run_publisher: -e requires KEY=value\n' >&2
      exit 2
    }
    env_values+=("$2")
    shift 2
  done
  [[ "${1:-}" == "$RUST_IMAGE" ]] && shift
  docker run --rm --network "$NET" \
    -v "$REPO_HOST_PATH:/work" \
    -v perfectory-cargo-registry:/usr/local/cargo/registry \
    -v perfectory-rustup:/usr/local/rustup \
    -v perfectory-target-foundation-platform:/work/platforms/foundation-platform/target \
    -w /work/platforms/foundation-platform \
    "$RUST_IMAGE" env "${env_values[@]}" "$@"
}

# Builds the decoder once and asserts one tile with it.
assert_tile() {
  local tile_relative="$1"
  shift
  docker run --rm -v "$REPO_HOST_PATH:/work" -w /work "$RUST_IMAGE" \
    "/work/$DECODER_RELATIVE" assert "/work/$tile_relative" \
    --content-encoding identity "$@"
}

# Writes the decoded feature identities of every named tile to `output`.
#
# The keys are named rather than assumed: a `complex` feature has no PNU, and a dump with no key at
# all is a list of layer names that two tiles sharing a layer would agree on while agreeing on
# nothing else. Every tile goes into one invocation because an unpacked archive is hundreds of them
# even for two polygons, and a container per tile is what makes decoding every zoom skippable.
dump_complex_identities() {
  local output="$1"
  shift
  # A zoom's tile list can outgrow one command line — Windows stops at 32 KB of argv, which is
  # about 250 of these paths — so the tiles are decoded in batches and appended in order.
  local batch=() tile_relative
  : > "$output"
  for tile_relative in "$@"; do
    batch+=("/work/$tile_relative")
    if [[ "${#batch[@]}" -ge 120 ]]; then
      dump_batch "$output" "${batch[@]}"
      batch=()
    fi
  done
  if [[ "${#batch[@]}" -gt 0 ]]; then
    dump_batch "$output" "${batch[@]}"
  fi
}

dump_batch() {
  local output="$1"
  shift
  docker run --rm -v "$REPO_HOST_PATH:/work" -w /work "$RUST_IMAGE" \
    "/work/$DECODER_RELATIVE" dump "$@" \
    --content-encoding identity "${COMPLEX_IDENTITY[@]}" >> "$output"
  printf '\n' >> "$output"
}

# Fetches one tile as identity-encoded bytes. The port defaults to the dynamic Martin; the static
# lane passes its own, so the two lanes are fetched by one function and can only differ in the URL.
fetch_tile() {
  local tile_relative="$1"
  local route="$2"
  local port="${3:-$MARTIN_PORT}"
  (cd "$REPO_ROOT" && curl --fail --silent --show-error -H 'Accept-Encoding: identity' \
    -o "$tile_relative" \
    "http://127.0.0.1:$port/$route")
}

# Build both binaries once, here, with cmake present. `rdkafka-sys` builds vendored librdkafka and
# its build script calls cmake, which the stock `rust` image does not carry —
# `tools/verify-image/Dockerfile` installs it for exactly this reason. Every later `cargo run` in
# this proof then reuses these artifacts and needs no toolchain beyond rustc, so the install happens
# in one container rather than in each of them.
#
# `perfectory-target-foundation-platform` is a named volume that survives runs, so on a machine that
# has built before, this step is a no-op and the missing cmake stays invisible. It is not invisible
# on a fresh volume, which is what CI always has.
docker run --rm \
  -v "$REPO_HOST_PATH:/work" \
  -v perfectory-cargo-registry:/usr/local/cargo/registry \
  -v perfectory-rustup:/usr/local/rustup \
  -v perfectory-target-foundation-platform:/work/platforms/foundation-platform/target \
  -w /work/platforms/foundation-platform \
  -e SQLX_OFFLINE=true \
  "$RUST_IMAGE" bash -euo pipefail -c '
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq >/dev/null 2>&1
    apt-get install -y --no-install-recommends cmake >/dev/null 2>&1
    command -v cmake >/dev/null || { printf "cmake install failed\n" >&2; exit 1; }
    cargo build --locked --quiet -p foundation-api --bin foundation-migrate
    cargo build --locked --quiet -p foundation-outbox-publisher
  '

docker run --rm --network "$NET" \
  -v "$REPO_HOST_PATH:/work" \
  -v perfectory-cargo-registry:/usr/local/cargo/registry \
  -v perfectory-rustup:/usr/local/rustup \
  -v perfectory-target-foundation-platform:/work/platforms/foundation-platform/target \
  -w /work/platforms/foundation-platform \
  -e SQLX_OFFLINE=true \
  -e FOUNDATION_MIGRATOR_DATABASE_URL="postgres://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
  "$RUST_IMAGE" cargo run --locked --quiet -p foundation-api --bin foundation-migrate

psql_file "$SCRIPT_DIR/fixture.sql"
psql_file "$REPO_ROOT/platforms/foundation-platform/infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql"
psql_file "$SCRIPT_DIR/administrative-boundary-fixture.sql"
psql_file "$SCRIPT_DIR/industrial-complex-boundary-fixture.sql"

run_publisher \
  -e FOUNDATION_PLATFORM_REPO_ROOT=/work \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_CONFIRM=true \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_INPUT_GEOJSON_PATH=/work/scripts/tiles/administrative-boundary-fixture.geojson \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_OUTPUT_PATH="/work/$RUN_RELATIVE/source.jsonl" \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_EVIDENCE_PATH="/work/$RUN_RELATIVE/source-evidence.json" \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_SNAPSHOT_ID=iceberg:administrative-boundary-fixture-v1 \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_PROVIDER=official-administrative-boundary-fixture \
  -e FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_VALID_FROM_UTC=2026-07-01T00:00:00Z \
  cargo run --locked --quiet -p foundation-outbox-publisher -- \
  write-official-administrative-boundary-source-snapshot

run_publisher \
  -e FOUNDATION_PLATFORM_REPO_ROOT=/work \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_CONFIRM=true \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_SOURCE_SNAPSHOT_ID=iceberg:administrative-boundary-fixture-v1 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_SOURCE_PATH="/work/$RUN_RELATIVE/source.jsonl" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_OUTPUT_PATH="/work/$RUN_RELATIVE/registry.jsonl" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_EVIDENCE_PATH="/work/$RUN_RELATIVE/registry-evidence.json" \
  cargo run --locked --quiet -p foundation-outbox-publisher -- \
  write-administrative-spatial-scope-registry

run_publisher \
  -e DATABASE_URL="postgres://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CONFIRM=1 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH="/work/$RUN_RELATIVE/source.jsonl" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_REGISTRY_EVIDENCE_PATH="/work/$RUN_RELATIVE/registry-evidence.json" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION=019d2b87-3fd1-7e3a-8d88-0b72c8743701 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID=841361364657368624 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID=iceberg:administrative-boundary-fixture-v1 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743702 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY=tiles-slice-proof/administrative-boundary/fixture.geojson \
  cargo run --locked --quiet -p foundation-outbox-publisher -- \
  publish-administrative-boundary-postgis

# The publish command opened and closed one projection load. Read its id from the ledger rather than
# from the command's stdout: a `$(... | grep ...)` would put a pipe in the judgment position and
# report the wrong exit code, and reading the row proves the ledger recorded the load at all.
#
# `ORDER BY started_at DESC LIMIT 1` is not decoration. (unit, revision) is exactly the pair this
# increment makes non-unique, so an unbounded query would return two ids the moment a revision is
# republished — and `psql -At` would hand back both, newline-separated, failing the UUID pattern
# below with a message about formatting rather than about ambiguity. The newest load is the one the
# publisher above just committed.
PROJECTION_LOAD_ID="$(psql_value "SELECT load.id
        FROM serving_postgis.spatial_projection_load AS load
        JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = load.publication_unit_id
       WHERE unit.unit_key = 'admin'
         AND load.data_revision = '019d2b87-3fd1-7e3a-8d88-0b72c8743701'
         AND load.status = 'succeeded'
       ORDER BY load.started_at DESC
       LIMIT 1;")"
[[ "$PROJECTION_LOAD_ID" =~ $UUID_PATTERN ]]

run_publisher \
  -e DATABASE_URL="postgres://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CONFIRM=1 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID="$PROJECTION_LOAD_ID" \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION=019d2b87-3fd1-7e3a-8d88-0b72c8743701 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID=841361364657368624 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743702 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743703 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743605 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743802 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743805 \
  -e FOUNDATION_PLATFORM_ADMINISTRATIVE_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE='http://127.0.0.1:3112/admin/{z}/{x}/{y}' \
  cargo run --locked --quiet -p foundation-outbox-publisher -- \
  promote-administrative-boundary-runtime

docker run -d --name "$MARTIN" --network "$NET" \
  -p "127.0.0.1:$MARTIN_PORT:3000" \
  -v "$(host_path "$SCRIPT_DIR/martin-dynamic.yaml"):/etc/martin/config.yaml:ro" \
  -e DATABASE_URL="postgresql://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
  "$MARTIN_IMAGE" --config /etc/martin/config.yaml >/dev/null

for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:$MARTIN_PORT/catalog" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:$MARTIN_PORT/catalog" | grep -q '"admin"'
# `complex` is a source before it is a publication: Martin publishes it from the view, which exists
# from the migration and returns nothing until a promotion selects a load. That the source is here
# now is what makes the empty tile below evidence about the promotion gate rather than about a
# missing layer.
curl --fail --silent "http://127.0.0.1:$MARTIN_PORT/catalog" | grep -q '"complex"'

DECODER_RELATIVE="$RUN_RELATIVE/mvt-assert"
docker run --rm -v "$REPO_HOST_PATH:/work" -w /work "$RUST_IMAGE" \
  rustc --edition=2021 -D warnings scripts/tiles/mvt_assert.rs \
  -o "/work/$DECODER_RELATIVE"

TILE_RELATIVE="$RUN_RELATIVE/admin-z14.pbf"
fetch_tile "$TILE_RELATIVE" 'admin/14/13977/6426'
assert_tile "$TILE_RELATIVE" \
  --expect-layer admin=1 \
  --expect-property canonical_code=9999900100 \
  --expect-property scope_kind=legal_dong

current_projection="$(psql_value "SELECT count(*) || '|' || string_agg(canonical_code, ',') FROM serving_postgis.administrative_unit_boundary_current;")"
[[ "$current_projection" == "1|9999900100" ]]
manifest_pointer="$(psql_value "SELECT pointer.manifest_id || '|' || manifest.manifest_generation FROM catalog.vector_tile_runtime_manifest_pointer pointer JOIN catalog.vector_tile_runtime_manifest manifest ON manifest.id=pointer.manifest_id;")"
[[ "$manifest_pointer" == "019d2b87-3fd1-7e3a-8d88-0b72c8743805|2" ]]

# ---------------------------------------------------------------------------
# The industrial-complex designation boundary, through the same Postgres and the same Martin.
#
# `019d2b87-3fd1-7e3a-8d88-0b72c8742005` / `...2006` are the release's source record and file asset,
# and `...2009` is the collected `catalog.bronze_object` the publish anchors to (root ADR-0046) —
# all three seeded by `industrial-complex-boundary-fixture.sql`. `841361364657368625` is this unit's
# canonical snapshot, distinct from the administrative one because it is a different table's version.
# ---------------------------------------------------------------------------

COMPLEX_TILE_ROUTE='complex/6/54/25'

# Nothing is published yet, so the layer is absent from the tile. The count is asserted rather than
# the response code, because a 204 proves only that Martin had nothing to send and cannot say
# whether that was the view's answer or a routing mistake.
COMPLEX_TILE_UNPUBLISHED="$RUN_RELATIVE/complex-z6-unpublished.pbf"
fetch_tile "$COMPLEX_TILE_UNPUBLISHED" "$COMPLEX_TILE_ROUTE"
assert_tile "$COMPLEX_TILE_UNPUBLISHED" --expect-layer complex=0

run_publisher \
  -e DATABASE_URL="postgres://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_CONFIRM=1 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_PATH=/work/scripts/tiles/industrial-complex-boundary-fixture.jsonl \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID=841361364657368625 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_SNAPSHOT_ID=vworldkr__sandan_boundary-synthetic-fixture \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_KEY=tiles-slice-proof/synthetic-industrial-complex-boundary/fixture.zip \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_OBJECT_CHECKSUM_SHA256=7777777777777777777777777777777777777777777777777777777777777777 \
  cargo run --locked --quiet -p foundation-outbox-publisher -- \
  publish-industrial-complex-boundary-postgis

# Both ids come out of the ledger. The revision is minted by the publisher rather than named by the
# operator — one canonical snapshot is one revision of one unit, so there is nothing for a caller to
# choose — which means the promotion below can only learn it from the row the publish committed.
COMPLEX_LOAD="$(psql_value "SELECT load.id, load.data_revision
        FROM serving_postgis.spatial_projection_load AS load
        JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = load.publication_unit_id
       WHERE unit.unit_key = 'complex'
         AND load.canonical_iceberg_snapshot_id = '841361364657368625'
         AND load.status = 'succeeded'
       ORDER BY load.started_at DESC
       LIMIT 1;")"
COMPLEX_LOAD_ID="${COMPLEX_LOAD%%|*}"
COMPLEX_DATA_REVISION="${COMPLEX_LOAD##*|}"
[[ "$COMPLEX_LOAD_ID" =~ $UUID_PATTERN ]]
[[ "$COMPLEX_DATA_REVISION" =~ $UUID_PATTERN ]]

# The disabling experiment, and the most valuable check in this file. Two boundaries are in the
# append-only table right now; the view Martin reads still returns none of them, and the tile Martin
# cuts still carries no `complex` layer, because no runtime manifest selects this load. If the
# manifest ever stops being the visibility switch, this assertion is what fails.
complex_written="$(psql_value "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_publication WHERE projection_load_id = '$COMPLEX_LOAD_ID';")"
[[ "$complex_written" == "2" ]]
complex_visible="$(psql_value "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_current;")"
[[ "$complex_visible" == "0" ]]
COMPLEX_TILE_UNPROMOTED="$RUN_RELATIVE/complex-z6-unpromoted.pbf"
fetch_tile "$COMPLEX_TILE_UNPROMOTED" "$COMPLEX_TILE_ROUTE"
assert_tile "$COMPLEX_TILE_UNPROMOTED" --expect-layer complex=0

run_publisher \
  -e DATABASE_URL="postgres://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_CONFIRM=1 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_PROJECTION_LOAD_ID="$COMPLEX_LOAD_ID" \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_DATA_REVISION="$COMPLEX_DATA_REVISION" \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_CANONICAL_ICEBERG_SNAPSHOT_ID=841361364657368625 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_SOURCE_RECORD_ID=019d2b87-3fd1-7e3a-8d88-0b72c8742005 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_SOURCE_FILE_ASSET_ID=019d2b87-3fd1-7e3a-8d88-0b72c8742006 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_BRONZE_OBJECT_ID=019d2b87-3fd1-7e3a-8d88-0b72c8742009 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_EXPECTED_MANIFEST_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743805 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_RELEASE_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743803 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_MANIFEST_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743806 \
  -e FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_RUNTIME_PROMOTE_TILES_URL_TEMPLATE='http://127.0.0.1:3112/complex/{z}/{x}/{y}' \
  cargo run --locked --quiet -p foundation-outbox-publisher -- \
  promote-industrial-complex-boundary-runtime

# The same route, the same Martin process, the same tile coordinates: only the manifest pointer
# moved. Both designation polygons are now features, each carrying the two properties the layer
# contract names — the lakehouse identity and the official code — and nothing else.
COMPLEX_TILE_PROMOTED="$RUN_RELATIVE/complex-z6-promoted.pbf"
fetch_tile "$COMPLEX_TILE_PROMOTED" "$COMPLEX_TILE_ROUTE"
assert_tile "$COMPLEX_TILE_PROMOTED" \
  --expect-layer complex=2 \
  --expect-property official_complex_code=999ZZ0 \
  --expect-property official_complex_code=999ZZ1 \
  --expect-property complex_id=00000000-0000-5000-8000-000000000001 \
  --expect-property complex_id=00000000-0000-5000-8000-000000000002

complex_projection="$(psql_value "SELECT count(*) || '|' || string_agg(official_complex_code, ',' ORDER BY official_complex_code) FROM serving_postgis.industrial_complex_boundary_current;")"
[[ "$complex_projection" == "2|999ZZ0,999ZZ1" ]]
# The administrative unit kept the release it was serving. A manifest is a complete publication, so
# promoting one unit re-states every other one, and getting that wrong would deselect `admin` here
# rather than anywhere a test would look.
current_projection="$(psql_value "SELECT count(*) || '|' || string_agg(canonical_code, ',') FROM serving_postgis.administrative_unit_boundary_current;")"
[[ "$current_projection" == "1|9999900100" ]]
manifest_pointer="$(psql_value "SELECT pointer.manifest_id || '|' || manifest.manifest_generation FROM catalog.vector_tile_runtime_manifest_pointer pointer JOIN catalog.vector_tile_runtime_manifest manifest ON manifest.id=pointer.manifest_id;")"
[[ "$manifest_pointer" == "019d2b87-3fd1-7e3a-8d88-0b72c8743806|3" ]]

# ---------------------------------------------------------------------------
# The same unit through the static lane, and the source XOR that keeps the two lanes apart.
#
# Every parameter of the bake is read out of the dynamic source's own TileJSON instead of being
# restated here. Zoom range, bounds and layer fields already have one definition — the `complex`
# entry in `scripts/tiles/martin-dynamic.yaml`, which the promoted release's layer row also carries
# — and a second copy in this script is how a static archive comes to disagree with the dynamic
# source it replaces. That disagreement has no symptom except a blank map at some zoom nobody
# checked, so it is spelled out of existence rather than asserted.
# ---------------------------------------------------------------------------

# `catalog_domain::static_release_martin_source_id` derives the source name from the unit key and
# the release, and `static_release_pmtiles_object_key` derives the object key from that name. Both
# are derived here too rather than written out, so a rename in the domain breaks this proof.
STATIC_RELEASE_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743804
STATIC_MANIFEST_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743807
STATIC_FILE_ASSET_ID=019d2b87-3fd1-7e3a-8d88-0b72c8743808
STATIC_SOURCE_ID="complex-$STATIC_RELEASE_ID"
STATIC_ARCHIVE_NAME="$STATIC_SOURCE_ID.pmtiles"
STATIC_OBJECT_KEY="gold/vector-tiles/releases/$STATIC_ARCHIVE_NAME"
COMPLEX_IDENTITY=(--identity-property complex_id --identity-property official_complex_code)

mkdir -p "$RUN_DIR/static" "$RUN_DIR/unpacked"

# Martin worked out this source's extent with `auto_bounds: calc` when it started, and that was
# before the unit was promoted: the view returned nothing, so it advertises no extent at all. It
# never recomputes. Measured on this Martin (1.12.0), a table that was empty at startup and then
# filled still reports the startup answer:
#
#   started against an empty table          -> TileJSON has no `bounds` key
#   row inserted, same process still up     -> still no `bounds` key
#   process restarted, table now populated  -> "bounds":[127.0,36.0,127.1,36.1]
#
# The middle line is the one that matters. Absent bounds stop the bake loudly, which is how CI found
# this; but a Martin that started with *some* rows keeps advertising that older, smaller extent after
# a load adds more, and a bake keyed on it would quietly cut a bbox that omits every complex outside
# it. Restarting is what makes the advertisement describe what the source now serves, and it is the
# state a deployed Martin is in anyway, because it is brought up against a projection that has
# already been published. The empty-before/populated-after proof above is finished by now, and
# `cache: disable` means no tile answer is carried across the restart.
docker restart "$MARTIN" >/dev/null
martin_restarted=false
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:$MARTIN_PORT/catalog" >/dev/null 2>&1; then
    martin_restarted=true
    break
  fi
  sleep 1
done
$martin_restarted || fail "dynamic Martin did not come back after the restart"

COMPLEX_TILEJSON_RELATIVE="$RUN_RELATIVE/complex-dynamic.tilejson.json"
(cd "$REPO_ROOT" && curl --fail --silent --show-error -H 'Accept: application/json' \
  -o "$COMPLEX_TILEJSON_RELATIVE" "http://127.0.0.1:$MARTIN_PORT/complex")
complex_tilejson="$(tr -d '\r\n\t' < "$RUN_DIR/complex-dynamic.tilejson.json")"

# The layer array is read on its own terms. Keying it to whatever Martin emits next — it used to
# require `,"bounds":` to follow — makes the parse fail for a reason that has nothing to do with
# layers, which is exactly the error CI reported. Its entries carry no brackets of their own, so the
# first `]` closes the array.
COMPLEX_VECTOR_LAYERS="$(printf '%s' "$complex_tilejson" \
  | sed -n 's/^.*\("vector_layers":\[[^]]*\]\).*$/{\1}/p')"
[[ -n "$COMPLEX_VECTOR_LAYERS" ]] \
  || fail "the dynamic complex TileJSON carries no parseable vector_layers metadata"

# The tileset's own keys, with the per-layer ones lifted out rather than skipped past, so nothing
# below depends on the order Martin happens to serialise in.
complex_tileset="$(printf '%s' "$complex_tilejson" | sed 's/"vector_layers":\[[^]]*\],*//')"
COMPLEX_BOUNDS="$(printf '%s' "$complex_tileset" \
  | sed -n 's/^.*"bounds":\[\([-0-9.,eE+]*\)\].*$/\1/p')"
COMPLEX_MIN_ZOOM="$(printf '%s' "$complex_tileset" \
  | sed -n 's/^.*"minzoom":\([0-9][0-9]*\).*$/\1/p')"
COMPLEX_MAX_ZOOM="$(printf '%s' "$complex_tileset" \
  | sed -n 's/^.*"maxzoom":\([0-9][0-9]*\).*$/\1/p')"
[[ "$COMPLEX_BOUNDS" =~ ^-?[0-9] ]] \
  || fail "the dynamic complex source advertises no bounds; its auto_bounds ran before the unit held rows"
[[ "$COMPLEX_MIN_ZOOM" =~ ^[0-9]+$ && "$COMPLEX_MAX_ZOOM" =~ ^[0-9]+$ ]] \
  || fail "could not read the tile zoom range from the complex TileJSON"

# And the extent it advertises has to contain the rows it serves. Absent and stale bounds are one
# defect wearing two faces: absent stops the bake above, stale would reach martin-cp and cut a bbox
# short of the data with nothing to show for it. PostGIS holds both numbers, so the comparison is
# made there. The expansion is 1e-9 degrees — about 0.1 mm — so a rounded advertisement is not read
# as a missing corner.
bounds_cover_rows="$(psql_value "SELECT ST_Covers(
        ST_Expand(ST_MakeEnvelope($COMPLEX_BOUNDS, 4326), 1e-9),
        ST_SetSRID(ST_Extent(geom)::geometry, 4326))
      FROM serving_postgis.industrial_complex_boundary_current;")"
[[ "$bounds_cover_rows" == "t" ]] \
  || fail "the complex source advertises $COMPLEX_BOUNDS, which does not cover the rows it serves"

# The zooms Martin serves and the zooms the promoted release says it serves are the same fact. If
# they ever differ, the archive built below would be correct about one of them and wrong about the
# thing a client reads, so this is checked before anything is baked rather than after.
published_zoom_range="$(psql_value "SELECT layer.tile_min_zoom || ',' || layer.tile_max_zoom
        FROM catalog.vector_tile_release_layer AS layer
        JOIN catalog.vector_tile_publication_unit AS unit ON unit.active_release_id = layer.release_id
       WHERE unit.unit_key = 'complex' AND layer.layer_id = 'complex';")"
[[ "$published_zoom_range" == "$COMPLEX_MIN_ZOOM,$COMPLEX_MAX_ZOOM" ]] \
  || fail "Martin serves complex over $COMPLEX_MIN_ZOOM-$COMPLEX_MAX_ZOOM but the promoted release says $published_zoom_range"

# The layer carries the two identities and nothing that moves. A status, a progress percentage or a
# tenancy figure baked into a tile goes stale behind every cache that holds it while the geometry
# stays right, and nothing in the client can tell. The count is asserted, not just the membership,
# because a third field is exactly what this forbids.
complex_fields="$(printf '%s' "$complex_tilejson" | grep -o '"fields":{[^}]*}')"
for identity_key in complex_id official_complex_code; do
  printf '%s' "$complex_fields" | grep -q "\"$identity_key\":" \
    || fail "the complex layer does not publish $identity_key"
done
complex_field_count="$(printf '%s' "$complex_fields" | grep -o '":"' | wc -l | tr -d '[:space:]')"
[[ "$complex_field_count" == 2 ]] \
  || fail "the complex layer publishes $complex_field_count fields; a tile carries identities only"

# martin-cp and mbtiles ship in the Martin image and read the same config the running server does,
# so the archive is cut from the view Martin serves rather than from a query written here.
martin_tool() {
  local entrypoint="$1"
  shift
  docker run --rm --network "$NET" --entrypoint "$entrypoint" \
    -v "$(host_path "$SCRIPT_DIR/martin-dynamic.yaml"):/etc/martin/config.yaml:ro" \
    -v "$(host_path "$RUN_DIR"):/artifacts" \
    -e DATABASE_URL="postgresql://postgres:$DB_PASSWORD@$DB:5432/tiles_slice_proof" \
    "$MARTIN_IMAGE" "$@"
}

pmtiles_tool() {
  docker run --rm -v "$(host_path "$RUN_DIR"):/artifacts" "$PMTILES_IMAGE" "$@"
}

bake_started_at="$(date -u +%s)"
martin_tool martin-cp \
  --config /etc/martin/config.yaml \
  --source complex \
  --output-file /artifacts/complex.mbtiles \
  --encoding identity \
  --bbox "$COMPLEX_BOUNDS" \
  --min-zoom "$COMPLEX_MIN_ZOOM" \
  --max-zoom "$COMPLEX_MAX_ZOOM" \
  --concurrency 2
bake_seconds=$(( $(date -u +%s) - bake_started_at ))

# martin-cp writes the tiles; the vector-layer metadata is what makes the archive self-describing,
# and it is copied from the dynamic source rather than composed, so the two TileJSONs can be
# compared for equality further down.
martin_tool mbtiles meta-set /artifacts/complex.mbtiles json "$COMPLEX_VECTOR_LAYERS"
martin_tool mbtiles meta-set /artifacts/complex.mbtiles name "$STATIC_SOURCE_ID"
martin_tool mbtiles validate /artifacts/complex.mbtiles

# `tile_count` appears once for the tileset and again for every entry of `zoom_info`, so the tail is
# cut off before matching. Reading the last one instead yields the highest zoom's count, which for
# this fixture is 50 against an archive of 86 — a number that looks like an answer and is not one.
mbtiles_summary="$(martin_tool mbtiles summary --format json /artifacts/complex.mbtiles)"
mbtiles_tile_count="$(printf '%s' "${mbtiles_summary%%\"zoom_info\"*}" | tr -d '\r\n' \
  | sed -n 's/^.*"tile_count"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*$/\1/p')"
[[ "$mbtiles_tile_count" =~ ^[0-9]+$ ]] || fail "mbtiles summary reported no tile_count"
martin_tool mbtiles unpack /artifacts/complex.mbtiles /artifacts/unpacked

# Every zoom the archive advertises, decoded again from the unpacked bytes. A zoom that renders
# nothing, a tile that carries some other layer, and a polygon whose command stream will not draw
# are all rejections, and none of them is visible in the representative tiles compared below.
mapfile -t unpacked_tiles < <(find "$RUN_DIR/unpacked" -type f -name '*.pbf' -print | sort)
[[ "${#unpacked_tiles[@]}" == "$mbtiles_tile_count" ]] \
  || fail "mbtiles counted $mbtiles_tile_count tiles; the unpacked archive holds ${#unpacked_tiles[@]}"
for zoom in $(seq "$COMPLEX_MIN_ZOOM" "$COMPLEX_MAX_ZOOM"); do
  mapfile -t zoom_tiles < <(find "$RUN_DIR/unpacked/$zoom" -type f -name '*.pbf' -print 2>/dev/null | sort)
  [[ "${#zoom_tiles[@]}" -gt 0 ]] || fail "the archive has no tile at advertised zoom $zoom"
  zoom_relative=()
  for zoom_tile in "${zoom_tiles[@]}"; do
    zoom_relative+=("${zoom_tile#"$REPO_ROOT/"}")
  done
  zoom_identities="$RUN_DIR/unpacked-z$zoom.identities"
  dump_complex_identities "$zoom_identities" "${zoom_relative[@]}"
  zoom_layers="$(sed -n 's/^layer="\([^"]*\)".*/\1/p' "$zoom_identities" \
    | sort -u | tr '\n' ',' | sed 's/,$//')"
  [[ "$zoom_layers" == "complex" ]] \
    || fail "archive zoom $zoom serves layers [$zoom_layers] instead of complex"
done

pmtiles_tool convert "/artifacts/complex.mbtiles" "/artifacts/static/$STATIC_ARCHIVE_NAME"
pmtiles_tool verify "/artifacts/static/$STATIC_ARCHIVE_NAME"
STATIC_ARCHIVE_PATH="$RUN_DIR/static/$STATIC_ARCHIVE_NAME"
[[ -s "$STATIC_ARCHIVE_PATH" ]] || fail "pmtiles convert produced an empty archive"
archive_bytes="$(wc -c < "$STATIC_ARCHIVE_PATH" | tr -d '[:space:]')"
archive_sha256="$(sha256sum "$STATIC_ARCHIVE_PATH" | sed 's/[[:space:]].*$//')"
[[ "$archive_bytes" -gt 512 && "$archive_sha256" =~ ^[0-9a-f]{64}$ ]] \
  || fail "the archive's size/checksum evidence is not usable"

# `pmtiles.paths` is production's discovery mode and it derives the source id from the filename, so
# serving the archive here proves the release-addressed route rather than a name this script chose.
docker run -d --name "$STATIC_MARTIN" --network "$NET" \
  -p "127.0.0.1:$STATIC_MARTIN_PORT:3000" \
  -v "$(host_path "$SCRIPT_DIR/martin-static-local-paths.yaml"):/etc/martin/config.yaml:ro" \
  -v "$(host_path "$RUN_DIR"):/artifacts:ro" \
  "$MARTIN_IMAGE" --config /etc/martin/config.yaml >/dev/null

static_ready=false
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:$STATIC_MARTIN_PORT/catalog" 2>/dev/null \
    | grep -q "\"$STATIC_SOURCE_ID\""; then
    static_ready=true
    break
  fi
  sleep 1
done
$static_ready || fail "static Martin did not discover $STATIC_SOURCE_ID"

# The comparison this whole section exists for. Same tile coordinates, two lanes.
COMPLEX_TILE_STATIC="$RUN_RELATIVE/complex-z6-static.pbf"
fetch_tile "$COMPLEX_TILE_STATIC" "$STATIC_SOURCE_ID/6/54/25" "$STATIC_MARTIN_PORT"
assert_tile "$COMPLEX_TILE_STATIC" \
  --expect-layer complex=2 \
  --expect-property official_complex_code=999ZZ0 \
  --expect-property official_complex_code=999ZZ1 \
  --expect-property complex_id=00000000-0000-5000-8000-000000000001 \
  --expect-property complex_id=00000000-0000-5000-8000-000000000002

dump_complex_identities "$RUN_DIR/complex-z6-dynamic.identities" "$COMPLEX_TILE_PROMOTED"
dump_complex_identities "$RUN_DIR/complex-z6-static.identities" "$COMPLEX_TILE_STATIC"
cmp --silent "$RUN_DIR/complex-z6-dynamic.identities" "$RUN_DIR/complex-z6-static.identities" \
  || fail "static complex identities differ from dynamic"
# The identities agree on what each feature is. The bytes agree on everything else — winding,
# property order, extent — which is what a renderer actually consumes.
cmp --silent "$RUN_DIR/complex-z6-promoted.pbf" "$RUN_DIR/complex-z6-static.pbf" \
  || fail "static complex MVT bytes differ from dynamic"

static_tilejson_relative="$RUN_RELATIVE/complex-static.tilejson.json"
(cd "$REPO_ROOT" && curl --fail --silent --show-error -H 'Accept: application/json' \
  -o "$static_tilejson_relative" "http://127.0.0.1:$STATIC_MARTIN_PORT/$STATIC_SOURCE_ID")
static_tilejson="$(tr -d '\r\n\t' < "$RUN_DIR/complex-static.tilejson.json")"
static_vector_layers="$(printf '%s' "$static_tilejson" \
  | sed -n 's/^.*\("vector_layers":\[.*\]\),"bounds":.*$/{\1}/p')"
[[ "$static_vector_layers" == "$COMPLEX_VECTOR_LAYERS" ]] \
  || fail "the static TileJSON's vector_layers differ from the dynamic source's"

# ---------------------------------------------------------------------------
# One unit, one source. ADR-0006 allows a publication unit to be dynamic PostGIS or static PMTiles
# and never both, and three separate things enforce it. This selects the static release through the
# CAS gate and then reads the dynamic route, which is the only place the enforcement is observable.
# ---------------------------------------------------------------------------

# The release describes the archive that was just built and verified. Its revision, snapshot and
# lineage are copied from the release it replaces, because a static build is the same data revision
# served differently — `promote_vector_tile_runtime_manifest` refuses it otherwise.
psql_value "INSERT INTO catalog.vector_tile_release (
        id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, source_record_id,
        source_file_asset_ids, source_kind, martin_source_id, tiles_url_template,
        pmtiles_object_key, pmtiles_file_asset_id, pmtiles_sha256, pmtiles_bytes,
        validated_at, validation_evidence_sha256)
      SELECT '$STATIC_RELEASE_ID', unit.id, unit.active_data_revision,
             active.canonical_iceberg_snapshot_id, active.source_record_id,
             active.source_file_asset_ids, 'static_pmtiles', '$STATIC_SOURCE_ID',
             'http://127.0.0.1:$STATIC_MARTIN_PORT/$STATIC_SOURCE_ID/{z}/{x}/{y}',
             '$STATIC_OBJECT_KEY', '$STATIC_FILE_ASSET_ID', '$archive_sha256', $archive_bytes,
             now(), '$archive_sha256'
        FROM catalog.vector_tile_publication_unit AS unit
        JOIN catalog.vector_tile_release AS active ON active.id = unit.active_release_id
       WHERE unit.unit_key = 'complex';" > /dev/null

psql_value "INSERT INTO catalog.vector_tile_release_layer (
        release_id, layer_id, source_layer, feature_id_property,
        tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom, feature_filter_properties)
      SELECT '$STATIC_RELEASE_ID', layer.layer_id, layer.source_layer, layer.feature_id_property,
             layer.tile_min_zoom, layer.tile_max_zoom, layer.render_min_zoom, layer.render_max_zoom,
             layer.feature_filter_properties
        FROM catalog.vector_tile_release_layer AS layer
        JOIN catalog.vector_tile_publication_unit AS unit ON unit.active_release_id = layer.release_id
       WHERE unit.unit_key = 'complex';" > /dev/null

# A manifest is a complete publication, so the next one restates every unit. Copying the current
# rows and overriding the one that changes is what keeps `admin` and `parcels` selected; composing
# a fresh list here would be a second definition of "every unit" that the CAS would then reject.
psql_value "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation)
      SELECT '$STATIC_MANIFEST_ID', manifest.manifest_generation + 1
        FROM catalog.vector_tile_runtime_manifest_pointer AS pointer
        JOIN catalog.vector_tile_runtime_manifest AS manifest ON manifest.id = pointer.manifest_id
       WHERE pointer.singleton;" > /dev/null
psql_value "INSERT INTO catalog.vector_tile_runtime_manifest_unit (
        manifest_id, publication_unit_id, release_id, serving_generation, data_revision,
        canonical_iceberg_snapshot_id)
      SELECT '$STATIC_MANIFEST_ID', manifest_unit.publication_unit_id, manifest_unit.release_id,
             manifest_unit.serving_generation, manifest_unit.data_revision,
             manifest_unit.canonical_iceberg_snapshot_id
        FROM catalog.vector_tile_runtime_manifest_unit AS manifest_unit
        JOIN catalog.vector_tile_runtime_manifest_pointer AS pointer
          ON pointer.manifest_id = manifest_unit.manifest_id
       WHERE pointer.singleton;" > /dev/null
psql_value "UPDATE catalog.vector_tile_runtime_manifest_unit AS manifest_unit
         SET release_id = '$STATIC_RELEASE_ID',
             serving_generation = manifest_unit.serving_generation + 1
        FROM catalog.vector_tile_publication_unit AS unit
       WHERE manifest_unit.manifest_id = '$STATIC_MANIFEST_ID'
         AND unit.id = manifest_unit.publication_unit_id
         AND unit.unit_key = 'complex';" > /dev/null

# The primary key is (manifest_id, publication_unit_id): one release per unit per manifest. This is
# the XOR at its narrowest — a manifest that named both a dynamic and a static release for `complex`
# cannot be written at all, so the "which one wins" question never reaches the runtime.
if docker exec "$DB" psql -X -q -h 127.0.0.1 -U postgres -d tiles_slice_proof -v ON_ERROR_STOP=1 \
  -c "INSERT INTO catalog.vector_tile_runtime_manifest_unit (
        manifest_id, publication_unit_id, release_id, serving_generation, data_revision,
        canonical_iceberg_snapshot_id)
      SELECT '$STATIC_MANIFEST_ID', unit.id, '019d2b87-3fd1-7e3a-8d88-0b72c8743803',
             manifest_unit.serving_generation, manifest_unit.data_revision,
             manifest_unit.canonical_iceberg_snapshot_id
        FROM catalog.vector_tile_publication_unit AS unit
        JOIN catalog.vector_tile_runtime_manifest_unit AS manifest_unit
          ON manifest_unit.publication_unit_id = unit.id
         AND manifest_unit.manifest_id = '$STATIC_MANIFEST_ID'
       WHERE unit.unit_key = 'complex';" > /dev/null 2>&1; then
  fail "a manifest accepted both a dynamic and a static release for one publication unit"
fi

static_generation="$(psql_value "SELECT catalog.promote_vector_tile_runtime_manifest(
      '019d2b87-3fd1-7e3a-8d88-0b72c8743806', '$STATIC_MANIFEST_ID');")"
[[ "$static_generation" == "4" ]] || fail "static promotion returned generation $static_generation"

# `serving_postgis.industrial_complex_boundary_current` joins the selected release and requires
# `source_kind = 'dynamic_postgis'`, so selecting the static release empties the dynamic projection.
# The dynamic route is still configured and still answers; what it no longer has is the layer.
complex_dynamic_after="$(psql_value "SELECT count(*) FROM serving_postgis.industrial_complex_boundary_current;")"
[[ "$complex_dynamic_after" == "0" ]] \
  || fail "the dynamic complex projection still serves $complex_dynamic_after rows after the static selection"
COMPLEX_TILE_AFTER_STATIC="$RUN_RELATIVE/complex-z6-after-static.pbf"
fetch_tile "$COMPLEX_TILE_AFTER_STATIC" "$COMPLEX_TILE_ROUTE"
assert_tile "$COMPLEX_TILE_AFTER_STATIC" --expect-layer complex=0
# And the static lane is unaffected by the switch: the archive is immutable and was never re-read.
assert_tile "$COMPLEX_TILE_STATIC" --expect-layer complex=2

# The other units kept their releases. A manifest restates every unit, so a promotion that got that
# wrong would deselect `admin` here rather than anywhere a unit test would look.
current_projection="$(psql_value "SELECT count(*) || '|' || string_agg(canonical_code, ',') FROM serving_postgis.administrative_unit_boundary_current;")"
[[ "$current_projection" == "1|9999900100" ]]

printf 'BOUNDARY SLICE E2E OK admin=%s complex=%s manifest=%s artifacts=%s\n' \
  "$current_projection" "$complex_projection" "$manifest_pointer" "$RUN_RELATIVE"
printf 'COMPLEX STATIC ARCHIVE OK zooms=%s-%s bbox=%s tiles=%s mbtiles_seconds=%s pmtiles_bytes=%s sha256=%s source=%s\n' \
  "$COMPLEX_MIN_ZOOM" "$COMPLEX_MAX_ZOOM" "$COMPLEX_BOUNDS" "$mbtiles_tile_count" \
  "$bake_seconds" "$archive_bytes" "$archive_sha256" "$STATIC_SOURCE_ID"
printf 'COMPLEX SOURCE XOR OK dynamic_rows_after_static=%s static_features=2 manifest_generation=%s\n' \
  "$complex_dynamic_after" "$static_generation"
