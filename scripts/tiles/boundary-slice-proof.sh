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
set -euo pipefail
set +x
IFS=$'\n\t'

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
RUST_IMAGE="rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc"
MARTIN_IMAGE="ghcr.io/maplibre/martin:1.12.0@sha256:6cb9f6fbe3f3aa9d76841120ac02ba562037bd2d303f38a93e80764298a0d21f"
POSTGIS_IMAGE="postgis/postgis:17-3.5-alpine@sha256:fe9821935d163abca5611e3e0a6a7c73c8c547f3412ed2036ec0ed8f789390da"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}"
RUN_RELATIVE="target/boundary-slice-proof/$RUN_ID"
RUN_DIR="$REPO_ROOT/$RUN_RELATIVE"
DB="boundary-slice-db-$$"
MARTIN="boundary-slice-martin-$$"
NET="boundary-slice-net-$$"
DB_PASSWORD="boundary-slice-proof-$RUN_ID"
MARTIN_PORT="${BOUNDARY_SLICE_MARTIN_PORT:-3112}"

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

for command in docker curl date mkdir; do
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
  docker rm -f -v "$MARTIN" "$DB" >/dev/null 2>&1 || true
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
psql_value() {
  docker exec "$DB" psql -X -At -F '|' -h 127.0.0.1 -U postgres -d tiles_slice_proof -c "$1"
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

# Fetches one tile from the running Martin as identity-encoded bytes.
fetch_tile() {
  local tile_relative="$1"
  local route="$2"
  (cd "$REPO_ROOT" && curl --fail --silent --show-error -H 'Accept-Encoding: identity' \
    -o "$tile_relative" \
    "http://127.0.0.1:$MARTIN_PORT/$route")
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

printf 'BOUNDARY SLICE E2E OK admin=%s complex=%s manifest=%s artifacts=%s\n' \
  "$current_projection" "$complex_projection" "$manifest_pointer" "$RUN_RELATIVE"
