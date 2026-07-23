#!/usr/bin/env bash
# Proves one bounded Foundation geometry slice through both Martin lanes:
# PostGIS -> dynamic MVT, then martin-cp -> MBTiles -> PMTiles -> Martin MVT.
set -euo pipefail
# A caller may invoke `bash -x`; disable inherited tracing before any secret-bearing
# environment variable can be inspected or expanded.
set +x
IFS=$'\n\t'

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/compose.yaml"
HTTP_EVIDENCE_HELPER="$SCRIPT_DIR/http-evidence.sh"
source "$HTTP_EVIDENCE_HELPER"

RUST_IMAGE="rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc"
PMTILES_IMAGE="protomaps/go-pmtiles:v1.31.1@sha256:057f8e5a6c77e89b46eebd40d62d295a0b69009371542bc0abfe1ecbc7ee6285"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}"
COMPOSE_PROJECT="tiles-slice-proof-$$-${RANDOM}"
export TILES_SLICE_POSTGRES_PASSWORD="tiles-slice-proof-${RUN_ID}"
RUN_RELATIVE="target/tiles-slice-proof/$RUN_ID"
ARTIFACT_DIR="$REPO_ROOT/$RUN_RELATIVE"
ARCHIVE_RELATIVE="tiles-slice-proof/local/foundation-static.pmtiles"
ARCHIVE_PATH="$ARTIFACT_DIR/$ARCHIVE_RELATIVE"
MBTILES_PATH="$ARTIFACT_DIR/tiles-slice-proof/local/foundation-static.mbtiles"
UNPACK_DIR="$ARTIFACT_DIR/unpacked"
DECODER_RELATIVE="$RUN_RELATIVE/mvt-assert"
MANIFEST_PATH="$SCRIPT_DIR/vector-tile-manifest.local.json"

BBOX="127.1230,36.1230,127.1239,36.1239"
TILESET_CENTER="127.12345,36.12345,14"
REQUEST_ORIGIN="http://127.0.0.1:3000"
COMPLEX_CODE="IC-SYNTHETIC-001"
PNUS=(
  "9999900000000000001"
  "9999900000000000002"
  "9999900000000000003"
)

fail() {
  printf 'tiles-slice-proof: ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

VALIDATE_R2_CONFIG_ONLY=false
if [[ "$#" -gt 0 ]]; then
  if [[ "$#" == 1 && "$1" == "--validate-r2-config-only" ]]; then
    VALIDATE_R2_CONFIG_ONLY=true
  else
    fail "usage: tiles-slice-proof.sh [--validate-r2-config-only]"
  fi
fi

host_path() {
  local path="$1"
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -am "$path"
  else
    printf '%s\n' "$path"
  fi
}

R2_MODE=false
R2_READ_OBJECT_URL=""
R2_UPLOAD_OBJECT_URL=""
R2_OBJECT_KEY=""
STATIC_MODE_LABEL="LOCAL PMTiles fallback"

validate_r2_key() {
  local key="$1"
  [[ "$key" =~ ^tiles-slice-proof/[A-Za-z0-9._/-]+\.pmtiles$ ]] \
    || fail "R2_TILES_OBJECT_KEY must be a .pmtiles key below tiles-slice-proof/"
  case "/$key/" in
    *//* | */./* | */../*) fail "R2_TILES_OBJECT_KEY contains an unsafe path segment" ;;
  esac
}

repository_protected_bucket_names() {
  local registry="$REPO_ROOT/platforms/foundation-platform/crates/lakehouse/lakehouse-domain/src/lakehouse_registry.rs"
  local recovery_env="$REPO_ROOT/platforms/foundation-platform/.env.example"
  local registry_names recovery_name
  [[ -f "$registry" && -f "$recovery_env" ]] || return 1

  registry_names="$(sed -n 's/.*=> "\([^"]*-prod\)".*/\1/p' "$registry")" || return 1
  recovery_name="$(sed -n 's/^FOUNDATION_RECOVERY_R2_BUCKET=\([^#[:space:]]*\).*/\1/p' "$recovery_env")" \
    || return 1
  [[ -n "$registry_names" && -n "$recovery_name" ]] || return 1
  printf '%s\n%s\n' "$registry_names" "$recovery_name"
}

validate_r2_test_bucket() {
  local bucket="$1" protected protected_names
  [[ "$bucket" =~ ^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$ && "$bucket" != *--* ]] \
    || fail "R2_TILES_TEST_BUCKET_NAME must use the repository's 3-63 lowercase/digit/hyphen rule"

  if ! protected_names="$(repository_protected_bucket_names)"; then
    fail "repository bucket SSOT files are missing or invalid"
  fi
  [[ -n "$protected_names" ]] || fail "repository protected bucket SSOT is empty"

  while IFS= read -r protected; do
    [[ -z "$protected" || "$bucket" != "$protected" ]] \
      || fail "R2_TILES_TEST_BUCKET_NAME is a repository-declared protected bucket"
  done <<< "$protected_names"

  [[ "$bucket" == *tiles-slice-proof* ]] \
    || fail "R2_TILES_TEST_BUCKET_NAME must contain tiles-slice-proof"
}

validate_curl_config_url() {
  local url="$1"
  case "$url" in
    *$'\n'* | *$'\r'* | *'"'* | *\\*) fail "R2 read URL contains a curl-config-unsafe character" ;;
  esac
}

configure_r2_mode() {
  local relevant=(
    R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_TILES_TEST_BUCKET_NAME R2_ENDPOINT
    R2_TILES_READ_BASE_URL R2_TILES_READ_URL R2_TILES_OBJECT_KEY
  )
  local any=false name
  for name in "${relevant[@]}"; do
    if declare -p "$name" >/dev/null 2>&1; then
      any=true
      break
    fi
  done

  if [[ "$any" == false ]]; then
    printf 'STATIC storage mode: LOCAL PMTiles fallback (R2 credentials absent)\n'
    export TILES_SLICE_PMTILES_URL="file:///artifacts/$ARCHIVE_RELATIVE"
    return
  fi

  for name in R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_TILES_TEST_BUCKET_NAME; do
    [[ -n "${!name:-}" ]] || fail "partial R2 configuration: $name is required"
  done
  [[ -n "${R2_ACCOUNT_ID:-}" || -n "${R2_ENDPOINT:-}" ]] \
    || fail "partial R2 configuration: R2_ACCOUNT_ID or R2_ENDPOINT is required"
  validate_r2_test_bucket "$R2_TILES_TEST_BUCKET_NAME"
  [[ "$R2_ACCESS_KEY_ID" =~ ^[A-Za-z0-9]+$ ]] \
    || fail "R2_ACCESS_KEY_ID contains an unsupported character"
  [[ "$R2_SECRET_ACCESS_KEY" =~ ^[A-Za-z0-9/+=]+$ ]] \
    || fail "R2_SECRET_ACCESS_KEY contains an unsupported character"

  local endpoint="${R2_ENDPOINT:-}"
  local account="${R2_ACCOUNT_ID:-}"
  endpoint="${endpoint%/}"
  if [[ -n "$endpoint" ]]; then
    [[ "$endpoint" =~ ^https://([[:xdigit:]]{32})\.r2\.cloudflarestorage\.com$ ]] \
      || fail "R2_ENDPOINT must be the exact standard Cloudflare R2 S3 endpoint"
    local endpoint_account
    endpoint_account="$(printf '%s' "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]')"
    if [[ -n "$account" ]]; then
      account="$(printf '%s' "$account" | tr '[:upper:]' '[:lower:]')"
      [[ "$account" == "$endpoint_account" ]] \
        || fail "R2_ACCOUNT_ID does not match R2_ENDPOINT"
    else
      account="$endpoint_account"
    fi
  else
    [[ "$account" =~ ^[[:xdigit:]]{32}$ ]] \
      || fail "R2_ACCOUNT_ID must be a 32-character hexadecimal account id"
    account="$(printf '%s' "$account" | tr '[:upper:]' '[:lower:]')"
    endpoint="https://$account.r2.cloudflarestorage.com"
  fi
  [[ "$endpoint" == "https://$account.r2.cloudflarestorage.com" ]] \
    || fail "R2 endpoint/account validation failed"

  if [[ -n "${R2_TILES_READ_BASE_URL:-}" && -n "${R2_TILES_READ_URL:-}" ]]; then
    fail "set exactly one read mode: R2_TILES_READ_BASE_URL or R2_TILES_READ_URL"
  fi

  if [[ -n "${R2_TILES_READ_BASE_URL:-}" ]]; then
    [[ "$R2_TILES_READ_BASE_URL" == https://* ]] \
      || fail "R2_TILES_READ_BASE_URL must use HTTPS"
    [[ "$R2_TILES_READ_BASE_URL" != *\?* && "$R2_TILES_READ_BASE_URL" != *\#* ]] \
      || fail "R2_TILES_READ_BASE_URL must not contain a query or fragment"
    R2_OBJECT_KEY="${R2_TILES_OBJECT_KEY:-tiles-slice-proof/$RUN_ID/foundation-static.pmtiles}"
    validate_r2_key "$R2_OBJECT_KEY"
    R2_READ_OBJECT_URL="${R2_TILES_READ_BASE_URL%/}/$R2_OBJECT_KEY"
  elif [[ -n "${R2_TILES_READ_URL:-}" ]]; then
    [[ -n "${R2_TILES_OBJECT_KEY:-}" ]] \
      || fail "R2_TILES_OBJECT_KEY is required with an exact R2_TILES_READ_URL"
    R2_OBJECT_KEY="$R2_TILES_OBJECT_KEY"
    validate_r2_key "$R2_OBJECT_KEY"
    [[ "$R2_TILES_READ_URL" == https://* && "$R2_TILES_READ_URL" != *\#* ]] \
      || fail "R2_TILES_READ_URL must be an HTTPS URL without a fragment"
    local read_path="${R2_TILES_READ_URL%%\?*}"
    case "$read_path" in
      *"/$R2_OBJECT_KEY") ;;
      *) fail "R2_TILES_READ_URL path must end in the exact R2_TILES_OBJECT_KEY" ;;
    esac
    R2_READ_OBJECT_URL="$R2_TILES_READ_URL"
  else
    fail "partial R2 configuration: set R2_TILES_READ_BASE_URL or R2_TILES_READ_URL"
  fi

  validate_curl_config_url "$R2_READ_OBJECT_URL"
  R2_UPLOAD_OBJECT_URL="$endpoint/$R2_TILES_TEST_BUCKET_NAME/$R2_OBJECT_KEY"
  R2_MODE=true
  STATIC_MODE_LABEL="REAL R2"
  printf 'STATIC storage mode: REAL R2 (dedicated tiles-slice-proof bucket + unique object; no delete or overwrite)\n'
}

configure_r2_mode

if [[ "$VALIDATE_R2_CONFIG_ONLY" == true ]]; then
  printf 'R2 configuration validation OK (%s)\n' "$STATIC_MODE_LABEL"
  exit 0
fi

for command in docker curl sed grep find sort wc cmp tr date mkdir rm sha256sum tail head; do
  require_command "$command"
done
docker info >/dev/null 2>&1 || fail "Docker must be running"
docker compose version >/dev/null 2>&1 || fail "Docker Compose is required"

mkdir -p "$(dirname -- "$ARCHIVE_PATH")" "$ARTIFACT_DIR/http"
REPO_HOST_PATH="$(host_path "$REPO_ROOT")"
COMPOSE_FILE_HOST="$(host_path "$COMPOSE_FILE")"
export TILES_SLICE_ARTIFACT_DIR
TILES_SLICE_ARTIFACT_DIR="$(host_path "$ARTIFACT_DIR")"

compose() {
  # Scope this to Docker: exporting it globally breaks Git Bash curl /dev/null.
  MSYS_NO_PATHCONV=1 docker compose --project-name "$COMPOSE_PROJECT" --file "$COMPOSE_FILE_HOST" "$@"
}

clean_curl() {
  # Curl loads a user's default config before ordinary options. Keep every proof
  # request deterministic and make --disable argv[1] at the executable boundary.
  command curl --disable "$@"
}

RAW_RESPONSE_HEADERS=()
UNVERIFIED_RESPONSE_BODIES=()
cleanup() {
  local status=$?
  trap - EXIT
  tiles_remove_http_artifacts "${RAW_RESPONSE_HEADERS[@]}" "${UNVERIFIED_RESPONSE_BODIES[@]}"
  compose --profile static down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

r2_signed_curl() {
  # `printf` is a Bash builtin. Curl reads credentials from stdin, so neither the
  # access key nor secret appears in the curl process argv.
  printf 'user = "%s:%s"\n' "$R2_ACCESS_KEY_ID" "$R2_SECRET_ACCESS_KEY" \
    | clean_curl --config - --aws-sigv4 'aws:amz:auto:s3' "$@"
}

r2_read_curl() {
  # The exact read URL may be presigned. Keep its query string out of argv/logs.
  printf 'url = "%s"\n' "$R2_READ_OBJECT_URL" | clean_curl --config - "$@"
}

response_header_value() {
  local file="$1" name="$2"
  sed -n "/^${name}:/I { s/^[^:]*:[[:space:]]*//; s/\r$//; p; }" "$file" | tail -n 1
}

wait_for_postgres() {
  local attempt
  for attempt in $(seq 1 90); do
    if compose exec -T postgis pg_isready -h 127.0.0.1 -U postgres -d tiles_slice_proof \
      >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  fail "PostGIS did not become ready"
}

wait_for_http() {
  local url="$1" label="$2" attempt
  for attempt in $(seq 1 90); do
    if clean_curl --silent --show-error --fail --connect-timeout 2 --max-time 3 \
      --output /dev/null "$url"; then
      return
    fi
    sleep 1
  done
  compose ps >&2 || true
  fail "$label did not become healthy"
}

psql_value() {
  compose exec -T postgis psql -X -h 127.0.0.1 -U postgres -d tiles_slice_proof \
    -v ON_ERROR_STOP=1 -A -t -c "$1" | tr -d '\r'
}

printf 'tiles-slice-proof: starting disposable PostGIS\n'
compose up -d postgis
wait_for_postgres

printf 'tiles-slice-proof: applying Foundation migrations through the production SQLx runner\n'
postgis_container="$(compose ps -q postgis)"
[[ -n "$postgis_container" ]] || fail "cannot resolve the disposable PostGIS container"
MSYS_NO_PATHCONV=1 docker run --rm --network "container:$postgis_container" \
  --volume "$REPO_HOST_PATH:/work:ro" \
  --volume perfectory-cargo-registry:/usr/local/cargo/registry \
  --volume perfectory-rustup:/usr/local/rustup \
  --volume perfectory-target-platforms-foundation-platform:/work/platforms/foundation-platform/target \
  --workdir /work/platforms/foundation-platform \
  --env SQLX_OFFLINE=true \
  --env CARGO_TERM_COLOR=always \
  --env RUSTUP_TOOLCHAIN=1.96.0-x86_64-unknown-linux-gnu \
  --env "FOUNDATION_MIGRATOR_DATABASE_URL=postgres://postgres:${TILES_SLICE_POSTGRES_PASSWORD}@127.0.0.1:5432/tiles_slice_proof" \
  "$RUST_IMAGE" cargo run --locked --quiet -p foundation-api --bin foundation-migrate
for _ in 1 2; do
  compose exec -T postgis psql -X -h 127.0.0.1 -U postgres -d tiles_slice_proof \
    -v ON_ERROR_STOP=1 -q -f - < "$SCRIPT_DIR/fixture.sql"
done

[[ "$(psql_value "SELECT concat_ws('|', (SELECT count(*) FROM catalog.industrial_complex WHERE id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101'), (SELECT count(*) FROM catalog.parcel WHERE complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101'), (SELECT count(*) FROM serving_postgis.parcel_boundary_mirror WHERE complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101'), (SELECT count(*) FROM catalog.parcel_marker_anchor AS anchor JOIN catalog.parcel AS parcel ON parcel.id = anchor.parcel_id WHERE parcel.complex_id = '019d2b87-3fd1-7e3a-8d88-0b72c8742101' AND anchor.is_active));")" == "1|3|3|3" ]] \
  || fail "fixture row counts drifted"
[[ "$(psql_value "SELECT concat_ws('|', count(*), min(ST_SRID(geom)), max(ST_SRID(geom)), bool_and(ST_IsValid(geom))) FROM serving_postgis.tiles_slice_parcels;")" == "3|5179|5179|t" ]] \
  || fail "parcel view geometry contract failed"
[[ "$(psql_value "SELECT concat_ws('|', count(*), min(ST_SRID(geom)), max(ST_SRID(geom)), bool_and(ST_IsValid(geom))) FROM serving_postgis.tiles_slice_parcel_anchor;")" == "3|4326|4326|t" ]] \
  || fail "anchor view geometry contract failed"
[[ "$(psql_value "SELECT concat_ws('|', count(*), min(aggregate.count), max(aggregate.count), min(ST_SRID(geom)), bool_and(ST_IsValid(geom))) FROM serving_postgis.tiles_slice_parcel_anchor_aggregate AS aggregate;")" == "1|3|3|4326|t" ]] \
  || fail "aggregate view contract failed"

printf 'tiles-slice-proof: starting dynamic Martin after schema + fixture\n'
compose up -d dynamic-martin
wait_for_http "http://127.0.0.1:3110/health" "dynamic Martin"

DYNAMIC_TILEJSON_PATH="$ARTIFACT_DIR/dynamic-composite.tilejson.json"
dynamic_tilejson_status="$(clean_curl --silent --show-error --connect-timeout 5 --max-time 30 \
  --header 'Accept: application/json' --output "$DYNAMIC_TILEJSON_PATH" \
  --write-out '%{http_code}' \
  "http://127.0.0.1:3110/parcels,parcel_anchor_aggregate,parcel_anchor")"
[[ "$dynamic_tilejson_status" == "200" && -s "$DYNAMIC_TILEJSON_PATH" ]] \
  || fail "dynamic Martin composite TileJSON request failed with HTTP $dynamic_tilejson_status"
dynamic_tilejson_compact="$(tr -d '\r\n\t' < "$DYNAMIC_TILEJSON_PATH")"
MBTILES_VECTOR_LAYERS="$(printf '%s' "$dynamic_tilejson_compact" \
  | sed -n 's/^.*\("vector_layers":\[.*\]\),"bounds":.*$/{\1}/p')"
[[ -n "$MBTILES_VECTOR_LAYERS" ]] \
  || fail "dynamic Martin TileJSON is missing parseable vector_layers metadata"

MSYS_NO_PATHCONV=1 docker run --rm \
  --env RUSTUP_TOOLCHAIN=1.96.0-x86_64-unknown-linux-gnu \
  --volume "$REPO_HOST_PATH:/workspace" --workdir /workspace \
  "$RUST_IMAGE" rustc --edition=2021 -D warnings scripts/tiles/mvt_assert.rs \
  -o "/workspace/$DECODER_RELATIVE"

mvt_assert() {
  MSYS_NO_PATHCONV=1 docker run --rm \
    --volume "$REPO_HOST_PATH:/workspace" --workdir /workspace \
    "$RUST_IMAGE" "/workspace/$DECODER_RELATIVE" "$@"
}

fetch_tile() {
  local url="$1" output="$2"
  local headers="$output.headers" status encoding content_type cors_origin
  status="$(clean_curl --silent --show-error --connect-timeout 5 --max-time 30 \
    --header 'Accept: application/vnd.mapbox-vector-tile, application/x-protobuf' \
    --header "Origin: $REQUEST_ORIGIN" \
    --header 'Accept-Encoding: identity' --dump-header "$headers" --output "$output" \
    --write-out '%{http_code}' "$url")"
  [[ "$status" == "200" ]] || fail "tile request returned HTTP $status"
  [[ -s "$output" ]] || fail "tile response was empty"
  encoding="$(tr -d '\r' < "$headers" | sed -n 's/^[Cc]ontent-[Ee]ncoding:[[:space:]]*//p' | tail -n 1)"
  [[ -z "$encoding" || "${encoding,,}" == "identity" ]] \
    || fail "tile response was compressed as $encoding instead of identity"
  content_type="$(tr -d '\r' < "$headers" | sed -n 's/^[Cc]ontent-[Tt]ype:[[:space:]]*//p' | tail -n 1)"
  case "$content_type" in
    application/x-protobuf* | application/vnd.mapbox-vector-tile* | application/octet-stream*) ;;
    *) fail "unexpected tile Content-Type: $content_type" ;;
  esac
  cors_origin="$(tr -d '\r' < "$headers" \
    | sed -n 's/^[Aa]ccess-[Cc]ontrol-[Aa]llow-[Oo]rigin:[[:space:]]*//p' | tail -n 1)"
  [[ "$cors_origin" == "$REQUEST_ORIGIN" || "$cors_origin" == "*" ]] \
    || fail "Martin tile response does not allow the proof Origin"
}

DYNAMIC_Z11="$ARTIFACT_DIR/dynamic-z11.pbf"
DYNAMIC_Z14="$ARTIFACT_DIR/dynamic-z14.pbf"
fetch_tile "http://127.0.0.1:3110/parcels,parcel_anchor_aggregate,parcel_anchor/11/1747/803" "$DYNAMIC_Z11"
fetch_tile "http://127.0.0.1:3110/parcels,parcel_anchor_aggregate,parcel_anchor/14/13977/6426" "$DYNAMIC_Z14"

mvt_assert assert "$RUN_RELATIVE/dynamic-z11.pbf" --content-encoding identity \
  --expect-layer parcel_anchor_aggregate=1 \
  --expect-identity "parcel_anchor_aggregate|${PNUS[0]}|$COMPLEX_CODE" \
  --expect-property count=3

z14_expectations=()
for pnu in "${PNUS[@]}"; do
  z14_expectations+=(--expect-identity "parcels|$pnu|$COMPLEX_CODE")
  z14_expectations+=(--expect-identity "parcel_anchor|$pnu|$COMPLEX_CODE")
  z14_expectations+=(--expect-property "PNU=$pnu")
done
mvt_assert assert "$RUN_RELATIVE/dynamic-z14.pbf" --content-encoding identity \
  --expect-layer parcels=3 --expect-layer parcel_anchor=3 "${z14_expectations[@]}"
mvt_assert dump "$RUN_RELATIVE/dynamic-z11.pbf" --content-encoding identity \
  > "$ARTIFACT_DIR/dynamic-z11.identities"
mvt_assert dump "$RUN_RELATIVE/dynamic-z14.pbf" --content-encoding identity \
  > "$ARTIFACT_DIR/dynamic-z14.identities"

MBTILES_CONTAINER="/artifacts/tiles-slice-proof/local/foundation-static.mbtiles"
ARCHIVE_CONTAINER="/artifacts/$ARCHIVE_RELATIVE"
compose run --rm --no-deps --entrypoint martin-cp dynamic-martin \
  --config /etc/martin/config.yaml \
  --source parcel_anchor_aggregate \
  --output-file "$MBTILES_CONTAINER" \
  --encoding identity --bbox "$BBOX" --min-zoom 0 --max-zoom 11 --concurrency 2
compose run --rm --no-deps --entrypoint martin-cp dynamic-martin \
  --config /etc/martin/config.yaml \
  --source parcel_anchor \
  --output-file "$MBTILES_CONTAINER" --on-duplicate abort \
  --encoding identity --bbox "$BBOX" --min-zoom 12 --max-zoom 13 --concurrency 2
compose run --rm --no-deps --entrypoint martin-cp dynamic-martin \
  --config /etc/martin/config.yaml \
  --source parcels,parcel_anchor \
  --output-file "$MBTILES_CONTAINER" --on-duplicate abort \
  --encoding identity --bbox "$BBOX" --min-zoom 14 --max-zoom 16 --concurrency 2
compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  meta-set "$MBTILES_CONTAINER" json "$MBTILES_VECTOR_LAYERS"
compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  meta-set "$MBTILES_CONTAINER" name foundation_static
compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  meta-set "$MBTILES_CONTAINER" bounds "$BBOX"
compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  meta-set "$MBTILES_CONTAINER" center "$TILESET_CENTER"
compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  validate "$MBTILES_CONTAINER"

summary_json="$(compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  summary --format json "$MBTILES_CONTAINER")"
summary_matches="$(printf '%s' "$summary_json" | tr -d '\r\n' \
  | grep -o '"tile_count"[[:space:]]*:[[:space:]]*[0-9][0-9]*')" \
  || fail "mbtiles summary did not contain tile_count"
summary_count="$(printf '%s\n' "$summary_matches" | sed -n '1s/.*:[[:space:]]*//p')"
[[ -n "$summary_count" ]] || fail "could not read tile_count from mbtiles summary"
compose run --rm --no-deps --entrypoint mbtiles dynamic-martin \
  unpack "$MBTILES_CONTAINER" /artifacts/unpacked

mapfile -t unpacked_tiles < <(find "$UNPACK_DIR" -type f -name '*.pbf' -print | sort)
flat_tile_count="${#unpacked_tiles[@]}"
[[ "$flat_tile_count" -gt 0 ]] || fail "MBTiles unpack produced no logical PBF tiles"
[[ "$flat_tile_count" == "$summary_count" ]] \
  || fail "MBTiles summary count $summary_count != unpacked count $flat_tile_count"

for zoom in $(seq 0 16); do
  mapfile -t zoom_tiles < <(find "$UNPACK_DIR/$zoom" -type f -name '*.pbf' -print | sort)
  [[ "${#zoom_tiles[@]}" -gt 0 ]] || fail "archive has no tile at manifest zoom $zoom"
  zoom_identities="$ARTIFACT_DIR/unpacked-z$zoom.identities"
  : > "$zoom_identities"
  for tile in "${zoom_tiles[@]}"; do
    relative_tile="${tile#"$REPO_ROOT/"}"
    mvt_assert dump "$relative_tile" --content-encoding identity >> "$zoom_identities"
  done
  actual_layers="$(sed -n 's/^layer="\([^"]*\)".*/\1/p' "$zoom_identities" \
    | sort -u | tr '\n' ',' | sed 's/,$//')"
  case "$zoom" in
    0|1|2|3|4|5|6|7|8|9|10|11) expected_layers="parcel_anchor_aggregate" ;;
    12|13) expected_layers="parcel_anchor" ;;
    14|15|16) expected_layers="parcel_anchor,parcels" ;;
  esac
  [[ "$actual_layers" == "$expected_layers" ]] \
    || fail "archive zoom $zoom layers $actual_layers != manifest layers $expected_layers"
done

flat_tile_total_bytes=0
for tile in "${unpacked_tiles[@]}"; do
  tile_bytes="$(wc -c < "$tile" | tr -d '[:space:]')"
  flat_tile_total_bytes=$((flat_tile_total_bytes + tile_bytes))
done
[[ "$flat_tile_total_bytes" -gt 0 ]] || fail "unpacked PBF payload bytes were zero"

mapfile -t manifest_counts < <(sed -n 's/.*"flat_tile_count":[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$MANIFEST_PATH")
mapfile -t manifest_bytes < <(sed -n 's/.*"flat_tile_total_bytes":[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$MANIFEST_PATH")
[[ "${#manifest_counts[@]}" == 3 && "${#manifest_bytes[@]}" == 3 ]] \
  || fail "local manifest must contain exactly three flat compatibility-stat pairs"
for value in "${manifest_counts[@]}"; do
  [[ "$value" == "$flat_tile_count" ]] \
    || fail "manifest flat_tile_count=$value; rendered composite archive has $flat_tile_count"
done
for value in "${manifest_bytes[@]}"; do
  [[ "$value" == "$flat_tile_total_bytes" ]] \
    || fail "manifest flat_tile_total_bytes=$value; rendered composite archive has $flat_tile_total_bytes"
done

MSYS_NO_PATHCONV=1 docker run --rm --volume "$TILES_SLICE_ARTIFACT_DIR:/artifacts" \
  "$PMTILES_IMAGE" convert "$MBTILES_CONTAINER" "$ARCHIVE_CONTAINER"
MSYS_NO_PATHCONV=1 docker run --rm --volume "$TILES_SLICE_ARTIFACT_DIR:/artifacts" \
  "$PMTILES_IMAGE" verify "$ARCHIVE_CONTAINER"
[[ -s "$ARCHIVE_PATH" ]] || fail "PMTiles conversion produced an empty archive"
archive_bytes="$(wc -c < "$ARCHIVE_PATH" | tr -d '[:space:]')"
archive_sha256="$(sha256sum "$ARCHIVE_PATH" | sed 's/[[:space:]].*$//')"
[[ "$archive_bytes" -gt 512 && "$archive_sha256" =~ ^[[:xdigit:]]{64}$ ]] \
  || fail "PMTiles archive size/checksum evidence is invalid"

if [[ "$R2_MODE" == true ]]; then
  put_headers_raw="$ARTIFACT_DIR/http/r2-put-headers.raw"
  put_headers="$ARTIFACT_DIR/r2-put-headers.redacted.txt"
  head_headers_raw="$ARTIFACT_DIR/http/r2-head-headers.raw"
  head_headers="$ARTIFACT_DIR/r2-head-headers.redacted.txt"
  readback_headers_raw="$ARTIFACT_DIR/http/r2-readback-headers.raw"
  readback_headers="$ARTIFACT_DIR/r2-readback-headers.redacted.txt"
  readback_path="$ARTIFACT_DIR/r2-public-readback.pmtiles"
  range_headers_raw="$ARTIFACT_DIR/http/r2-range-headers.raw"
  range_headers="$ARTIFACT_DIR/r2-range-headers.redacted.txt"
  range_path="$ARTIFACT_DIR/r2-range-proof.bin"
  RAW_RESPONSE_HEADERS=(
    "$put_headers_raw"
    "$head_headers_raw"
    "$readback_headers_raw"
    "$range_headers_raw"
  )
  # Public responses are untrusted until their status and bytes are verified.
  # Keep them cleanup-eligible so a redirect/error body cannot retain a signed URL.
  UNVERIFIED_RESPONSE_BODIES=("$readback_path" "$range_path")

  put_status="$(r2_signed_curl --silent --show-error --output /dev/null \
    --dump-header "$put_headers_raw" \
    --write-out '%{http_code}' --connect-timeout 10 --max-time 120 \
    --header 'If-None-Match: *' \
    --header 'Content-Type: application/vnd.pmtiles' \
    --header 'Cache-Control: public, max-age=31536000, immutable' \
    --header "x-amz-meta-sha256: $archive_sha256" \
    --upload-file "$ARCHIVE_PATH" "$R2_UPLOAD_OBJECT_URL")"
  tiles_redact_response_headers "$put_headers_raw" "$put_headers"
  [[ "$put_status" == "200" ]] \
    || fail "conditional R2 PutObject failed with HTTP $put_status (no overwrite attempted)"

  head_status="$(r2_signed_curl --silent --show-error --head --output /dev/null \
    --dump-header "$head_headers_raw" \
    --write-out '%{http_code}' --connect-timeout 10 --max-time 60 \
    "$R2_UPLOAD_OBJECT_URL")"
  tiles_redact_response_headers "$head_headers_raw" "$head_headers"
  [[ "$head_status" == "200" ]] || fail "authenticated R2 HEAD failed with HTTP $head_status"

  put_etag="$(response_header_value "$put_headers" ETag)"
  head_etag="$(response_header_value "$head_headers" ETag)"
  head_length="$(response_header_value "$head_headers" Content-Length)"
  head_sha256="$(response_header_value "$head_headers" x-amz-meta-sha256)"
  [[ -n "$put_etag" && "$put_etag" == "$head_etag" ]] \
    || fail "R2 PUT/HEAD ETag evidence is missing or differs"
  [[ "$head_length" == "$archive_bytes" ]] \
    || fail "R2 HEAD Content-Length $head_length != local archive bytes $archive_bytes"
  [[ "$head_sha256" == "$archive_sha256" ]] \
    || fail "R2 HEAD checksum metadata differs from the local SHA-256"

  readback_status="$(r2_read_curl --silent --show-error --output "$readback_path" \
    --dump-header "$readback_headers_raw" \
    --write-out '%{http_code}' --connect-timeout 10 --max-time 120 \
    --header 'Accept-Encoding: identity')"
  tiles_redact_response_headers "$readback_headers_raw" "$readback_headers"
  [[ "$readback_status" == "200" ]] \
    || fail "R2 public full-object readback failed with HTTP $readback_status"
  readback_bytes="$(wc -c < "$readback_path" | tr -d '[:space:]')"
  [[ "$readback_bytes" == "$archive_bytes" ]] \
    || fail "R2 public readback bytes $readback_bytes != local archive bytes $archive_bytes"
  readback_sha256="$(sha256sum "$readback_path" | sed 's/[[:space:]].*$//')"
  [[ "$readback_sha256" == "$archive_sha256" ]] \
    || fail "R2 public readback SHA-256 differs from the uploaded archive"
  unset 'UNVERIFIED_RESPONSE_BODIES[0]'

  range_status="$(r2_read_curl --silent --show-error --output "$range_path" \
    --dump-header "$range_headers_raw" \
    --write-out '%{http_code}' --connect-timeout 10 --max-time 60 \
    --header 'Range: bytes=0-511')"
  tiles_redact_response_headers "$range_headers_raw" "$range_headers"
  [[ "$range_status" == "206" ]] || fail "R2 read URL did not honor Range (HTTP $range_status)"
  range_bytes="$(wc -c < "$range_path" | tr -d '[:space:]')"
  range_content_range="$(response_header_value "$range_headers" Content-Range)"
  [[ "$range_bytes" == "512" ]] || fail "R2 range response was not exactly 512 bytes"
  [[ "$range_content_range" == "bytes 0-511/$archive_bytes" ]] \
    || fail "R2 Content-Range $range_content_range != bytes 0-511/$archive_bytes"
  head -c 512 "$readback_path" | cmp --silent - "$ARTIFACT_DIR/r2-range-proof.bin" \
    || fail "R2 range response bytes differ from the verified archive prefix"
  unset 'UNVERIFIED_RESPONSE_BODIES[1]'

  {
    printf 'mode=REAL_R2\n'
    printf 'bucket=%s\n' "$R2_TILES_TEST_BUCKET_NAME"
    printf 'object_key=%s\n' "$R2_OBJECT_KEY"
    printf 'archive_bytes=%s\n' "$archive_bytes"
    printf 'archive_sha256=%s\n' "$archive_sha256"
    printf 'public_readback_bytes=%s\n' "$readback_bytes"
    printf 'public_readback_sha256=%s\n' "$readback_sha256"
    printf 'etag=%s\n' "$head_etag"
    printf 'content_range=%s\n' "$range_content_range"
  } > "$ARTIFACT_DIR/r2-evidence.txt"
  export TILES_SLICE_PMTILES_URL="$R2_READ_OBJECT_URL"
fi

compose --profile static up -d static-martin
wait_for_http "http://127.0.0.1:3101/health" "static Martin"

STATIC_Z11="$ARTIFACT_DIR/static-z11.pbf"
STATIC_Z14="$ARTIFACT_DIR/static-z14.pbf"
fetch_tile "http://127.0.0.1:3101/foundation_static/11/1747/803" "$STATIC_Z11"
fetch_tile "http://127.0.0.1:3101/foundation_static/14/13977/6426" "$STATIC_Z14"
mvt_assert assert "$RUN_RELATIVE/static-z11.pbf" --content-encoding identity \
  --expect-layer parcel_anchor_aggregate=1 \
  --expect-identity "parcel_anchor_aggregate|${PNUS[0]}|$COMPLEX_CODE" \
  --expect-property count=3
mvt_assert assert "$RUN_RELATIVE/static-z14.pbf" --content-encoding identity \
  --expect-layer parcels=3 --expect-layer parcel_anchor=3 "${z14_expectations[@]}"
mvt_assert dump "$RUN_RELATIVE/static-z11.pbf" --content-encoding identity \
  > "$ARTIFACT_DIR/static-z11.identities"
mvt_assert dump "$RUN_RELATIVE/static-z14.pbf" --content-encoding identity \
  > "$ARTIFACT_DIR/static-z14.identities"
cmp --silent "$ARTIFACT_DIR/dynamic-z11.identities" "$ARTIFACT_DIR/static-z11.identities" \
  || fail "z11 static feature identities differ from dynamic"
cmp --silent "$ARTIFACT_DIR/dynamic-z14.identities" "$ARTIFACT_DIR/static-z14.identities" \
  || fail "z14 static feature identities differ from dynamic"
cmp --silent "$DYNAMIC_Z11" "$STATIC_Z11" \
  || fail "z11 static MVT bytes differ from dynamic"
cmp --silent "$DYNAMIC_Z14" "$STATIC_Z14" \
  || fail "z14 static MVT bytes differ from dynamic"

TILEJSON_PATH="$ARTIFACT_DIR/tiles-slice-proof/local/foundation-static.tilejson.json"
tilejson_status="$(clean_curl --silent --show-error --connect-timeout 5 --max-time 30 \
  --header 'Accept: application/json' --output "$TILEJSON_PATH" \
  --write-out '%{http_code}' "http://127.0.0.1:3101/foundation_static")"
[[ "$tilejson_status" == "200" && -s "$TILEJSON_PATH" ]] \
  || fail "static Martin TileJSON request failed with HTTP $tilejson_status"
tilejson_compact="$(tr -d '\r\n\t' < "$TILEJSON_PATH")"
[[ "$tilejson_compact" == *'"vector_layers":['* ]] \
  || fail "TileJSON is missing vector_layers"
static_vector_layers="$(printf '%s' "$tilejson_compact" \
  | sed -n 's/^.*\("vector_layers":\[.*\]\),"bounds":.*$/{\1}/p')"
[[ "$static_vector_layers" == "$MBTILES_VECTOR_LAYERS" ]] \
  || fail "static TileJSON vector_layers fields differ from dynamic Martin source metadata"
[[ "$tilejson_compact" == *'"minzoom":0'* && "$tilejson_compact" == *'"maxzoom":16'* ]] \
  || fail "static TileJSON zoom bounds do not cover manifest tiles 0 through 16"
[[ "$tilejson_compact" == *'"bounds":[127.123,36.123,127.1239,36.1239]'* ]] \
  || fail "static TileJSON bounds differ from the frozen build bbox"
[[ "$tilejson_compact" == *'"center":[127.12345,36.12345,14]'* ]] \
  || fail "static TileJSON center differs from the frozen build center"
for layer in parcels parcel_anchor_aggregate parcel_anchor; do
  occurrences="$(printf '%s' "$tilejson_compact" | grep -o "\"id\":\"$layer\"" | wc -l | tr -d '[:space:]')"
  [[ "$occurrences" == 1 ]] || fail "TileJSON must list vector layer $layer exactly once"
done
all_layer_ids="$(printf '%s' "$tilejson_compact" | grep -o '"id":"[^"]*"' | wc -l | tr -d '[:space:]')"
[[ "$all_layer_ids" == 3 ]] || fail "TileJSON contains unexpected vector layer IDs"

printf 'DYNAMIC tile OK bbox=%s decoded feature count=7 expected PNU=%s\n' "$BBOX" "${PNUS[0]}"
printf 'STATIC tile OK bbox=%s decoded feature count=7 MATCHING features (%s)\n' "$BBOX" "$STATIC_MODE_LABEL"
printf 'tiles-slice-proof: artifacts retained at %s\n' "$ARTIFACT_DIR"
