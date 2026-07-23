#!/usr/bin/env bash
# Local disposable-DB integration harness — the "Testcontainers" pattern.
#
# `cargo xtask verify` runs offline with no database, so it structurally SKIPS the
# live-DB / `#[ignore]` integration tests — leaving them provable only in CI. That
# blind spot is exactly how DB bugs (e.g. the 42501 owner error, the sqlx
# search_path break) reached CI green-locally-but-red-in-CI. This harness closes it:
# it spins a throwaway Postgres, migrates + seeds it, and runs the SAME tests CI
# runs — locally, on the host — so "local green" finally covers the DB tests too.
#
#   scripts/verify/integration.sh foundation
#
# Division of labour (ADR-0004): this script owns ONLY the disposable-DB
# orchestration. The test COMMAND is owned by `cargo xtask integration <area>`, the
# same single source CI uses, so it can never drift.
#
# Requires Docker. Reuses the exact image + cache volumes as cargo-verify.sh, so the
# area's build cache is shared (no cold recompile).
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO="$(pwd -W 2>/dev/null || pwd)"
source tools/container-images.env

AREA="${1:-}"
[ -n "$AREA" ] || { echo "usage: scripts/verify/integration.sh <foundation>" >&2; exit 2; }

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "integration: Docker is required — it provisions the disposable Postgres." >&2
  exit 1
fi

NET="perfectory-it-net-${AREA}-$$"
DB="perfectory-it-db-${AREA}-$$"

# Per-area provisioning: DB image + credentials + the SQL applied before the tests.
# (The test command itself lives in xtask, not here.) Image/creds mirror the area's
# *-ci.yml service container so local == CI.
case "$AREA" in
  foundation)
    AREA_DIR="platforms/foundation-platform"
    DB_IMAGE="postgis/postgis:17-3.5-alpine@sha256:fe9821935d163abca5611e3e0a6a7c73c8c547f3412ed2036ec0ed8f789390da"
    PG_USER="foundation_platform"
    PG_PASS="foundation_platform_dev_2026"   # disposable CI-grade throwaway, not a secret
    PG_DB="foundation_platform"
    URL_VAR="DATABASE_URL"
    PREPARE_SQL=(
      migrations/20260719000001_foundation_platform_schema.sql
      migrations/20260719000002_foundation_platform_constraints.sql
      migrations/20260719000003_foundation_platform_indexes.sql
      migrations/20260719000004_foundation_platform_foreign_keys.sql
      infra/db/seeds/local_vector_tile_manifest.sql
    )
    ;;
  *)
    echo "integration: area '$AREA' is not wired yet (foundation only for now)." >&2
    exit 2
    ;;
esac
SLUG="$(echo "$AREA_DIR" | tr '/' '-')"

cleanup() {
  docker rm -f "$DB" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "integration($AREA): network + disposable Postgres…"
docker network create "$NET" >/dev/null
docker run -d --name "$DB" --network "$NET" \
  -e POSTGRES_USER="$PG_USER" -e POSTGRES_PASSWORD="$PG_PASS" -e POSTGRES_DB="$PG_DB" \
  "$DB_IMAGE" >/dev/null

echo "integration($AREA): waiting for the REAL Postgres (post-init) to accept TCP…"
# The postgres/postgis entrypoint runs a TEMPORARY init server on the unix socket
# only, then stops it and starts the real server on TCP. A unix-socket pg_isready
# reports "ready" against that init server and we'd start migrating right as it
# restarts (FATAL: terminating connection). Forcing TCP (-h 127.0.0.1) only
# succeeds against the real, post-init server — so it is a stable-ready signal.
ready=0
for _ in $(seq 1 90); do
  if docker exec "$DB" pg_isready -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[ "$ready" = 1 ] || { echo "integration: Postgres never became ready" >&2; exit 1; }

echo "integration($AREA): applying migrations + seeds (superuser)…"
for sql in "${PREPARE_SQL[@]}"; do
  [ -f "$AREA_DIR/$sql" ] || { echo "integration: missing $AREA_DIR/$sql" >&2; exit 1; }
  docker exec -i "$DB" psql -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q -f - < "$AREA_DIR/$sql"
done

echo "integration($AREA): running DB tests via cargo xtask integration $AREA…"
# Same image + cache volumes as cargo-verify.sh (target/ is a named volume; Windows
# bind mounts are too slow for it). The rust container joins the DB's network and
# reaches it by container name — no host port mapping needed.
MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
  -v "$REPO":/work \
  -v perfectory-cargo-registry:/usr/local/cargo/registry \
  -v perfectory-rustup:/usr/local/rustup \
  -v "perfectory-target-$SLUG":/work/"$AREA_DIR"/target \
  -v perfectory-target-xtask:/work/tools/xtask/target \
  -w /work \
  -e SQLX_OFFLINE=true \
  -e CARGO_TERM_COLOR=always \
  -e "$URL_VAR=postgres://$PG_USER:$PG_PASS@$DB:5432/$PG_DB" \
  "$RUST_TOOLCHAIN_IMAGE" cargo xtask integration "$AREA"

echo "integration($AREA): PASS"
