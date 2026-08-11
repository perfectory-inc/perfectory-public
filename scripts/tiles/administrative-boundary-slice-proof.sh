#!/usr/bin/env bash
# Reproducible disposable proof for the official administrative-boundary path:
# source snapshot -> registry -> PostGIS -> CAS runtime manifest -> Martin MVT.
set -euo pipefail
set +x
IFS=$'\n\t'

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
RUST_IMAGE="rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc"
MARTIN_IMAGE="ghcr.io/maplibre/martin:1.12.0@sha256:6cb9f6fbe3f3aa9d76841120ac02ba562037bd2d303f38a93e80764298a0d21f"
POSTGIS_IMAGE="postgis/postgis:17-3.5-alpine@sha256:fe9821935d163abca5611e3e0a6a7c73c8c547f3412ed2036ec0ed8f789390da"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}"
RUN_RELATIVE="target/admin-boundary-slice-proof/$RUN_ID"
RUN_DIR="$REPO_ROOT/$RUN_RELATIVE"
DB="admin-boundary-slice-db-$$"
MARTIN="admin-boundary-slice-martin-$$"
NET="admin-boundary-slice-net-$$"
DB_PASSWORD="admin-boundary-proof-$RUN_ID"
MARTIN_PORT="${ADMIN_BOUNDARY_MARTIN_PORT:-3112}"

if command -v docker.exe >/dev/null 2>&1 && docker.exe info >/dev/null 2>&1; then
  DOCKER_EXECUTABLE=docker.exe
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  DOCKER_EXECUTABLE=docker
else
  printf 'administrative-boundary-slice-proof: Docker is required\n' >&2
  DOCKER_EXECUTABLE=docker
  exit 1
fi
docker() { command "$DOCKER_EXECUTABLE" "$@"; }

for command in docker curl date mkdir; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'administrative-boundary-slice-proof: missing command %s\n' "$command" >&2
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
PROJECTION_LOAD_ID="$(docker exec "$DB" psql -X -At -h 127.0.0.1 -U postgres -d tiles_slice_proof \
  -c "SELECT load.id
        FROM serving_postgis.spatial_projection_load AS load
        JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = load.publication_unit_id
       WHERE unit.unit_key = 'admin'
         AND load.data_revision = '019d2b87-3fd1-7e3a-8d88-0b72c8743701'
         AND load.status = 'succeeded'
       ORDER BY load.started_at DESC
       LIMIT 1;")"
[[ "$PROJECTION_LOAD_ID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]

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

TILE_RELATIVE="$RUN_RELATIVE/admin-z14.pbf"
(cd "$REPO_ROOT" && curl --fail --silent --show-error -H 'Accept-Encoding: identity' \
  -o "$TILE_RELATIVE" \
  "http://127.0.0.1:$MARTIN_PORT/admin/14/13977/6426")

DECODER_RELATIVE="$RUN_RELATIVE/mvt-assert"
docker run --rm -v "$REPO_HOST_PATH:/work" -w /work "$RUST_IMAGE" \
  rustc --edition=2021 -D warnings scripts/tiles/mvt_assert.rs \
  -o "/work/$DECODER_RELATIVE"
docker run --rm -v "$REPO_HOST_PATH:/work" -w /work "$RUST_IMAGE" \
  "/work/$DECODER_RELATIVE" assert "/work/$TILE_RELATIVE" \
  --content-encoding identity --expect-layer admin=1 \
  --expect-property canonical_code=9999900100 \
  --expect-property scope_kind=legal_dong

current_projection="$(docker exec "$DB" psql -X -At -h 127.0.0.1 -U postgres -d tiles_slice_proof \
  -c "SELECT count(*) || '|' || string_agg(canonical_code, ',') FROM serving_postgis.administrative_unit_boundary_current;")"
[[ "$current_projection" == "1|9999900100" ]]
manifest_pointer="$(docker exec "$DB" psql -X -At -h 127.0.0.1 -U postgres -d tiles_slice_proof \
  -c "SELECT pointer.manifest_id || '|' || manifest.manifest_generation FROM catalog.vector_tile_runtime_manifest_pointer pointer JOIN catalog.vector_tile_runtime_manifest manifest ON manifest.id=pointer.manifest_id;")"
[[ "$manifest_pointer" == "019d2b87-3fd1-7e3a-8d88-0b72c8743805|2" ]]

printf 'ADMINISTRATIVE BOUNDARY E2E OK current=%s manifest=%s artifact=%s\n' \
  "$current_projection" "$manifest_pointer" "$TILE_RELATIVE"
